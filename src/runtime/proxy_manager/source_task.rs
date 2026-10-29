//! Source-side bounded PRE/CROSS/POST tasks and their private chunk owners.

use std::sync::Arc;

use super::remote_stage_operation::{RemoteStageOperation, read_request};
use super::stage_registration::PinnedStage;
use super::{ProxyManager, StageBufferLease};
use crate::cuda::StagingCopy;
use crate::engine::Source;
use crate::memory::Allocation;
use crate::runtime::control_plane::{ControlChannel, Operation, Request};
use crate::transport::tcp::TcpTransport;
use crate::{Error, RegisteredMemory, Result, SourceEpoch, TransferOp, Transport};

struct Chunk {
    stage: PinnedStage,
    to_device: bool,
    pipeline: bool,
}

impl Chunk {
    fn new(
        lease: StageBufferLease,
        source: &Source,
        transport: Transport,
        operation: TransferOp,
        pipeline: bool,
    ) -> Result<Self> {
        Ok(Self {
            stage: PinnedStage::new(lease, source, transport)?,
            to_device: operation == TransferOp::Write,
            pipeline,
        })
    }

    fn delegate(
        &mut self,
        copy: &mut StagingCopy,
        channel: &mut ControlChannel<'_>,
        remote: &RemoteStageOperation,
        manager: &ProxyManager,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        remote.expect(channel, Operation::Delegate, offset, length)?;
        self.stage
            .copy(copy, offset, length, self.to_device, self.pipeline)?;
        if !self.pipeline {
            manager.completed_remote(Operation::Delegate);
        }
        // For a pipeline this receipt accepts PRE/POST; CROSS or final Unpin
        // supplies its completion boundary. The nonce/range still scopes both.
        remote.respond(channel, Operation::Delegate, offset, length)
    }

    fn drain(&mut self, copy: &mut StagingCopy, manager: &ProxyManager) -> Result<()> {
        if self.stage.complete(copy)? {
            manager.completed_remote(Operation::Delegate);
        }
        Ok(())
    }

    fn finish(self, result: Result<()>) -> Result<()> {
        self.stage.finish(result)
    }
}

impl ProxyManager {
    pub(crate) fn serve_remote(
        self: &Arc<Self>,
        channel: &mut ControlChannel<'_>,
        source: &Source,
        request: &Request,
        memory: &RegisteredMemory,
    ) -> Result<()> {
        let mut reply_epoch = source.epoch;
        let result = self.pin_remote(channel, source, request, memory, &mut reply_epoch);
        if let Err(error) = &result {
            if !matches!(error, Error::Io(_) | Error::Timeout | Error::SourceClosed) {
                // Preserve the native error if the error receipt itself is lost.
                let _ = channel.reply(
                    reply_epoch,
                    crate::runtime::control_plane::error_status(error),
                    0,
                );
            }
        }
        result
    }

    fn pin_remote(
        self: &Arc<Self>,
        channel: &mut ControlChannel<'_>,
        source: &Source,
        request: &Request,
        memory: &RegisteredMemory,
        reply_epoch: &mut SourceEpoch,
    ) -> Result<()> {
        if !source.remote_staging {
            return Err(Error::Unavailable("remote staging is not enabled"));
        }
        let mut hello = [0; 10];
        channel.read_exact(&mut hello)?;
        let transport = match hello[0] {
            0 => Transport::Tcp,
            1 => {
                #[cfg(feature = "rdma")]
                {
                    source
                        .rdma
                        .as_ref()
                        .ok_or(Error::Unavailable("RDMA is not configured"))?;
                }
                #[cfg(not(feature = "rdma"))]
                {
                    return Err(Error::Unavailable("build with --features rdma"));
                }
                #[cfg(feature = "rdma")]
                {
                    Transport::Rdma
                }
            }
            _ => return Err(Error::Protocol("Pin transport")),
        };
        if !(1..=16).contains(&hello[1]) {
            return Err(Error::Protocol("Pin pipeline depth"));
        }
        let cap = usize::try_from(crate::runtime::control_plane::u64_at(&hello, 2))
            .map_err(|_| Error::Protocol("Pin chunk cap"))?;
        if cap == 0 || cap > 64 << 20 {
            return Err(Error::Protocol("Pin chunk cap"));
        }
        let length = self.chunk_size.min(cap);
        let total = usize::try_from(request.length).map_err(|_| Error::InvalidRange)?;
        let depth = (hello[1] as usize)
            .min(source.staging_pipeline_depth)
            .min(total.div_ceil(length));
        if depth == 0 {
            return Err(Error::InvalidRange);
        }
        let (allocation, base) = memory.allocation_range(request.offset as usize, total)?;
        let Allocation::Cuda(gpu) = allocation else {
            return Err(Error::Unavailable(
                "remote staging requires CUDA source memory",
            ));
        };
        let operation = if request.operation == Operation::PinRead {
            TransferOp::Read
        } else {
            TransferOp::Write
        };
        let remote = RemoteStageOperation {
            nonce: SourceEpoch::generate()?,
            region: request.region,
        };
        gpu.with_staging(base, total, self, depth, |copy, leases| {
            let mut chunks = Vec::with_capacity(depth);
            let result = (|| {
                for lease in leases {
                    chunks.push(Chunk::new(lease, source, transport, operation, depth > 1)?);
                }
                channel.reply(source.epoch, 0, length as u64)?;
                channel.write_all(&remote.nonce.0)?;
                channel.write_all(&[depth as u8])?;
                *reply_epoch = remote.nonce;
                self.completed_remote(request.operation);
                if operation == TransferOp::Read {
                    for (slot, chunk) in chunks.iter_mut().enumerate() {
                        let offset = slot * length;
                        chunk.delegate(
                            copy,
                            channel,
                            &remote,
                            self,
                            offset,
                            length.min(total - offset),
                        )?;
                    }
                }
                let mut offset = 0;
                while offset < total {
                    *reply_epoch = source.epoch;
                    let slot = (offset / length) % depth;
                    let count = length.min(total - offset);
                    let chunk = &mut chunks[slot];
                    chunk.drain(copy, self)?;
                    remote.expect(channel, Operation::Cross, offset, count)?;
                    let cross = read_request(channel)?;
                    let expected = match (transport, operation) {
                        (Transport::Tcp, TransferOp::Read) => Operation::TcpRead,
                        (Transport::Tcp, TransferOp::Write) => Operation::TcpWrite,
                        (Transport::Rdma, TransferOp::Read) => Operation::RdmaReadSlices,
                        (Transport::Rdma, TransferOp::Write) => Operation::RdmaWriteSlices,
                        _ => return Err(Error::Protocol("Pin transport")),
                    };
                    if cross.operation != expected
                        || cross.epoch != source.epoch
                        || cross.region != remote.stage_id(slot)
                        || cross.offset != 0
                        || cross.length != count as u64
                    {
                        return Err(Error::Protocol("CROSS grant scope"));
                    }
                    match transport {
                        Transport::Tcp => TcpTransport::serve(
                            channel,
                            source.epoch,
                            &cross,
                            chunk.stage.registration.memory()?,
                            0,
                            count,
                            self,
                        )?,
                        Transport::Rdma => {
                            #[cfg(feature = "rdma")]
                            source
                                .rdma
                                .as_ref()
                                .ok_or(Error::Unavailable("RDMA is not configured"))?
                                .serve(
                                    channel,
                                    source.epoch,
                                    &cross,
                                    chunk.stage.registration.memory()?,
                                )?;
                        }
                        _ => return Err(Error::Protocol("Pin transport")),
                    }
                    *reply_epoch = remote.nonce;
                    self.completed_remote(Operation::Cross);
                    remote.respond(channel, Operation::Cross, offset, count)?;
                    if operation == TransferOp::Write {
                        chunk.delegate(copy, channel, &remote, self, offset, count)?;
                    } else if offset + length * depth < total {
                        let next = offset + length * depth;
                        chunk.delegate(
                            copy,
                            channel,
                            &remote,
                            self,
                            next,
                            length.min(total - next),
                        )?;
                    }
                    offset += count;
                }
                remote.expect(channel, Operation::Unpin, total, 0)
            })();
            let mut result = result;
            // A deadline/EOF stops new work, not native completion. Drain every
            // accepted copy once; one successful slot never clears another's failure.
            for chunk in &mut chunks {
                let drained = chunk.drain(copy, self);
                if matches!(drained, Err(Error::Unquiesced { .. })) || result.is_ok() {
                    result = drained;
                }
            }
            for chunk in chunks {
                result = chunk.finish(result);
            }
            result
        })?;
        self.completed_remote(Operation::Unpin);
        remote.respond(channel, Operation::Unpin, total, 0)
    }
}
