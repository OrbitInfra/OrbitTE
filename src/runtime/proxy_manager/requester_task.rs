//! Bounded requester PRE/CROSS/POST with independent local and remote rings.

use std::sync::Arc;
use std::time::Instant;

use super::ProxyManager;
use super::remote_stage_operation::RemoteStageOperation;
use super::stage_buffer_cache::StageBufferCache;
use super::stage_registration::{PinnedStage, StageRegistration};
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::memory::Allocation;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request, buffer};
use crate::transport::tcp::TcpTransport;
use crate::{
    Error, HostMemory, RemoteEffect, RemoteRegion, RemoteSegment, SourceEpoch, TransferOp,
    Transport,
};

impl ProxyManager {
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
            let mut transfer = |stages: &mut StageBufferCache<'_>| {
                let local_width = stages.width();
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
                channel.write_all(&[
                    match request.transport {
                        Transport::Tcp => 0,
                        Transport::Rdma => 1,
                        _ => {
                            return Err(Error::Unavailable("remote staging requires TCP or RDMA"));
                        }
                    },
                    source.staging_pipeline_depth as u8,
                ])?;
                channel.write_all(&(self.chunk_size as u64).to_be_bytes())?;
                let pin = Reply::read(&mut channel, peer.epoch)?;
                let chunk = usize::try_from(pin.length).map_err(|_| Error::InvalidRange)?;
                if pin.status != 0 || chunk == 0 || chunk > self.chunk_size {
                    return Err(Error::Protocol("Pin chunk size"));
                }
                let mut nonce = [0; 16];
                channel.read_exact(&mut nonce)?;
                let mut width = [0];
                channel.read_exact(&mut width)?;
                let depth = width[0] as usize;
                if depth == 0
                    || depth > source.staging_pipeline_depth
                    || depth > request.length.div_ceil(chunk)
                {
                    return Err(Error::Protocol("Pin pipeline depth"));
                }
                let remote = RemoteStageOperation {
                    nonce: SourceEpoch(nonce),
                    region: request.remote.id,
                };
                let delegate = |channel: &mut ControlChannel<'_>, offset: usize| {
                    let length = chunk.min(request.length - offset);
                    remote.send(channel, Operation::Delegate, offset, length)?;
                    remote.receipt(channel, Operation::Delegate, offset, length)
                };
                if operation == TransferOp::Read {
                    for slot in 0..depth {
                        delegate(&mut channel, slot * chunk)?;
                    }
                }
                if operation == TransferOp::Write {
                    for slot in 0..local_width {
                        remaining(deadline)?;
                        let offset = slot * chunk;
                        let length = chunk.min(request.length - offset);
                        stages.copy(slot, request, offset, length, operation)?;
                    }
                }
                let mut offset = 0;
                while offset < request.length {
                    remaining(deadline)?;
                    let length = chunk.min(request.length - offset);
                    let slot = (offset / chunk) % local_width;
                    // WRITE waits for PRE; READ waits for the prior POST in
                    // this local slot. Other slots may still be copying.
                    stages.complete(slot)?;
                    remote.send(&mut channel, Operation::Cross, offset, length)?;
                    let cross = PreparedTransfer {
                        transport: request.transport,
                        local: stages.registration(slot).memory()?.clone(),
                        local_offset: 0,
                        remote: RemoteRegion {
                            epoch: peer.epoch,
                            id: remote.stage_id((offset / chunk) % depth),
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
                        _ => {
                            return Err(Error::Unavailable("remote staging requires TCP or RDMA"));
                        }
                    };
                    transfer.map_err(|(error, _)| error)?;
                    // The transport ACK is followed by a session receipt only
                    // after source grants/loans finish their native cleanup.
                    remote.receipt(&mut channel, Operation::Cross, offset, length)?;
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                        delegate(&mut channel, offset)?;
                        let next = offset + chunk * local_width;
                        if next < request.length {
                            remaining(deadline)?;
                            stages.copy(
                                slot,
                                request,
                                next,
                                chunk.min(request.length - next),
                                operation,
                            )?;
                        }
                    } else {
                        // CROSS's native drain and scoped receipt precede POST.
                        // Completed chunks may remain visible if a later leg fails.
                        remaining(deadline)?;
                        stages.copy(slot, request, offset, length, operation)?;
                        if offset + chunk * depth < request.length {
                            delegate(&mut channel, offset + chunk * depth)?;
                        }
                    }
                    offset += length;
                }
                stages.drain()?;
                remote.send(&mut channel, Operation::Unpin, request.length, 0)?;
                remote.receipt(&mut channel, Operation::Unpin, request.length, 0)
            };
            let (allocation, base) = request
                .local
                .allocation_range(request.local_offset, request.length)?;
            match allocation {
                Allocation::Cuda(gpu) => {
                    let width = source
                        .staging_pipeline_depth
                        .min(request.length.div_ceil(self.chunk_size));
                    gpu.with_staging(base, request.length, self, width, |copy, leases| {
                        let mut slots = Vec::with_capacity(width);
                        let built = leases.into_iter().try_for_each(|lease| {
                            slots.push(PinnedStage::new(lease, source, request.transport)?);
                            Ok(())
                        });
                        let mut stages = StageBufferCache::Cuda { copy, slots };
                        let result = built.and_then(|()| transfer(&mut stages));
                        stages.finish(result)
                    })
                }
                Allocation::Host(_) => {
                    let payload = buffer(self.chunk_size.min(request.length))?;
                    let host = Arc::new(HostMemory::new(payload.into_boxed_slice())?);
                    let mut stages = StageBufferCache::Host(StageRegistration::new(
                        host,
                        source,
                        request.transport,
                    )?);
                    let result = transfer(&mut stages);
                    stages.finish(result)
                }
            }
        })();
        result.map_err(|error| (error, effect))
    }
}
