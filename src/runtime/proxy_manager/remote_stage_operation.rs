//! Serial PRE/CROSS/POST remote operations, following TENT's ProxyManager.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::ProxyManager;
use super::stage_registration::StageRegistration;
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::memory::Allocation;
use crate::runtime::control_plane::{
    ControlChannel, Operation, REQUEST_SIZE, Reply, Request, buffer,
};
use crate::transport::tcp::TcpTransport;
use crate::{
    Error, HostMemory, RegisteredMemory, RemoteEffect, RemoteRegion, RemoteSegment, Result,
    SourceEpoch, TransferOp, Transport,
};

fn read_request(channel: &mut ControlChannel<'_>) -> Result<Request> {
    let mut bytes = [0; REQUEST_SIZE];
    channel.read_exact(&mut bytes)?;
    Request::decode(&bytes)
}

/// Each frame is scoped to the original registration, operation, chunk and
/// freshly pinned session. The handler only accepts the next expected step.
struct RemoteStageOperation {
    nonce: SourceEpoch,
    region: u64,
}

impl RemoteStageOperation {
    fn receipt(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let reply = Reply::read(channel, self.nonce)?;
        if reply.status != 0 || reply.length != REQUEST_SIZE as u64 {
            return Err(Error::Protocol("staging operation receipt"));
        }
        self.expect(channel, operation, offset, length)
    }

    fn respond(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        channel.reply(self.nonce, 0, REQUEST_SIZE as u64)?;
        self.send(channel, operation, offset, length)
    }

    fn frame(&self, operation: Operation, offset: usize, length: usize) -> Request {
        Request {
            operation,
            epoch: self.nonce,
            region: self.region,
            offset: offset as u64,
            length: length as u64,
        }
    }

    fn expect(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let request = read_request(channel)?;
        if request.operation != operation
            || request.epoch != self.nonce
            || request.region != self.region
            || request.offset != offset as u64
            || request.length != length as u64
        {
            return Err(Error::Protocol("staging operation scope or order"));
        }
        remaining(channel.deadline)?;
        Ok(())
    }

    fn send(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        channel.write_all(&self.frame(operation, offset, length).encode())
    }

    fn stage_id(&self) -> u64 {
        crate::runtime::control_plane::u64_at(&self.nonce.0, 0)
    }
}

impl ProxyManager {
    fn completed_remote(&self, operation: Operation) {
        let mut state = self.buffers.lock().unwrap_or_else(|p| p.into_inner());
        let count = match operation {
            Operation::PinRead | Operation::PinWrite => &mut state.remote_pins,
            Operation::Delegate => &mut state.remote_delegates,
            Operation::Cross => &mut state.remote_crosses,
            Operation::Unpin => &mut state.remote_unpins,
            _ => return,
        };
        *count = count.saturating_add(1);
    }

    pub(crate) fn transfer_remote(
        self: &Arc<Self>,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        source: &Source,
        deadline: Instant,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut effect = RemoteEffect::None;
        let result = (|| {
            let mut payload = buffer(request.length)?;
            // Local GPU locks never span network I/O: WRITE snapshots before
            // Pin; READ commits only after confirmed remote Unpin. This also
            // makes self-peer and opposite-direction sessions deadlock-free.
            if operation == TransferOp::Write {
                self.read(&request.local, request.local_offset, &mut payload, deadline)?;
            }
            let host = Arc::new(HostMemory::new(payload.into_boxed_slice())?);
            let mut stage = StageRegistration::new(host, source, request.transport)?;
            let result = (|| {
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                channel.write_all(
                    &Request {
                        operation: if operation == TransferOp::Read {
                            Operation::PinRead
                        } else {
                            Operation::PinWrite
                        },
                        epoch: peer.epoch,
                        region: request.remote.id,
                        offset: request.remote_offset as u64,
                        length: request.length as u64,
                    }
                    .encode(),
                )?;
                channel.write_all(&[match request.transport {
                    Transport::Tcp => 0,
                    Transport::Rdma => 1,
                    _ => return Err(Error::Unavailable("remote staging requires TCP or RDMA")),
                }])?;
                let pin = Reply::read(&mut channel, peer.epoch)?;
                let chunk = usize::try_from(pin.length).map_err(|_| Error::InvalidRange)?;
                if pin.status != 0 || chunk == 0 || chunk > 64 << 20 {
                    return Err(Error::Protocol("Pin chunk size"));
                }
                let mut nonce = [0; 16];
                channel.read_exact(&mut nonce)?;
                let remote = RemoteStageOperation {
                    nonce: SourceEpoch(nonce),
                    region: request.remote.id,
                };
                let mut offset = 0;
                while offset < request.length {
                    let length = chunk.min(request.length - offset);
                    if operation == TransferOp::Read {
                        remote.send(&mut channel, Operation::Delegate, offset, length)?;
                        remote.receipt(&mut channel, Operation::Delegate, offset, length)?;
                    }
                    remote.send(&mut channel, Operation::Cross, offset, length)?;
                    let cross = PreparedTransfer {
                        transport: request.transport,
                        local: stage.memory()?.clone(),
                        local_offset: offset,
                        remote: RemoteRegion {
                            epoch: peer.epoch,
                            id: remote.stage_id(),
                            address: 0,
                            length: chunk,
                            permission: crate::Permission::RemoteReadWrite,
                        },
                        remote_offset: 0,
                        length,
                    };
                    let transfer = match request.transport {
                        Transport::Tcp => {
                            TcpTransport::transfer_on(operation, peer, &cross, &mut channel, self)
                        }
                        Transport::Rdma => {
                            #[cfg(feature = "rdma")]
                            {
                                source
                                    .rdma
                                    .as_ref()
                                    .ok_or(Error::Unavailable("RDMA is not configured"))?
                                    .transfer_on(
                                        operation,
                                        peer,
                                        &cross,
                                        source.epoch,
                                        &mut channel,
                                    )
                            }
                            #[cfg(not(feature = "rdma"))]
                            {
                                return Err(Error::Unavailable("build with --features rdma"));
                            }
                        }
                        _ => return Err(Error::Unavailable("remote staging requires TCP or RDMA")),
                    };
                    transfer.map_err(|(error, _)| error)?;
                    // The transport ACK is followed by a session receipt only
                    // after source grants/loans finish their native cleanup.
                    remote.receipt(&mut channel, Operation::Cross, offset, length)?;
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                        remote.send(&mut channel, Operation::Delegate, offset, length)?;
                        remote.receipt(&mut channel, Operation::Delegate, offset, length)?;
                    }
                    offset += length;
                }
                remote.send(&mut channel, Operation::Unpin, request.length, 0)?;
                remote.receipt(&mut channel, Operation::Unpin, request.length, 0)
            })();
            stage.finish(result)?;
            if operation == TransferOp::Read {
                stage.host.with_read_slice(|bytes| {
                    self.write(&request.local, request.local_offset, bytes, deadline)
                })?;
            }
            Ok(())
        })();
        result.map_err(|error| (error, effect))
    }

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
        let mut hello = [0];
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
        let (allocation, base) =
            memory.allocation_range(request.offset as usize, request.length as usize)?;
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
        gpu.with_staging(base, request.length as usize, self, |copy, lease| {
            let length = lease.buffer()?.length;
            let address = lease.buffer()?.address;
            let lease = Arc::new(Mutex::new(lease));
            // SAFETY: HostMemory retains the actual lease. All CPU, RDMA and
            // delegated CUDA access shares its access lock and DMA tracking.
            #[allow(unsafe_code)]
            let host =
                Arc::new(unsafe { HostMemory::from_raw_parts(address, length, lease.clone())? });
            let mut stage = StageRegistration::new(host, source, transport)?;
            let result = (|| {
                channel.reply(source.epoch, 0, length as u64)?;
                channel.write_all(&remote.nonce.0)?;
                *reply_epoch = remote.nonce;
                self.completed_remote(request.operation);
                let mut offset = 0;
                while offset < request.length as usize {
                    let count = length.min(request.length as usize - offset);
                    let delegate = |channel: &mut ControlChannel<'_>,
                                    copy: &mut crate::cuda::StagingCopy<'_>|
                     -> Result<()> {
                        remote.expect(channel, Operation::Delegate, offset, count)?;
                        stage.host.with_staging_access(count, || {
                            let mut lease = lease
                                .lock()
                                .map_err(|_| Error::Internal("staging lease lock poisoned"))?;
                            copy.copy(&mut lease, offset, count, operation == TransferOp::Write)
                        })?;
                        self.completed_remote(Operation::Delegate);
                        remote.respond(channel, Operation::Delegate, offset, count)
                    };
                    if operation == TransferOp::Read {
                        delegate(channel, copy)?;
                    }
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
                        || cross.region != remote.stage_id()
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
                            stage.memory()?,
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
                                .serve(channel, source.epoch, &cross, stage.memory()?)?;
                        }
                        _ => return Err(Error::Protocol("Pin transport")),
                    }
                    self.completed_remote(Operation::Cross);
                    remote.respond(channel, Operation::Cross, offset, count)?;
                    if operation == TransferOp::Write {
                        delegate(channel, copy)?;
                    }
                    offset += count;
                }
                remote.expect(channel, Operation::Unpin, request.length as usize, 0)
            })();
            let result = stage.finish(result);
            if matches!(result, Err(Error::Unquiesced { .. })) {
                lease.lock().unwrap_or_else(|p| p.into_inner()).quarantine();
                return result;
            }
            drop(stage);
            let lease = Arc::try_unwrap(lease).map_err(|lease| {
                lease.lock().unwrap_or_else(|p| p.into_inner()).quarantine();
                std::mem::forget(lease);
                Error::Unquiesced {
                    operation: "pinned stage still borrowed",
                    code: -1,
                }
            })?;
            lease
                .into_inner()
                .unwrap_or_else(|p| p.into_inner())
                .release()
                .and(result)
        })?;
        // No private MR, native loan, stage lease or GPU guard survives success.
        self.completed_remote(Operation::Unpin);
        remote.respond(channel, Operation::Unpin, request.length as usize, 0)
    }
}
