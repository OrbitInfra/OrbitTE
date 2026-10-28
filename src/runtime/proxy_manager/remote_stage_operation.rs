//! Scoped PRE/CROSS/POST protocol and requester scheduling.

use std::sync::Arc;
use std::time::Instant;

use super::ProxyManager;
use super::stage_registration::StageRegistration;
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::runtime::control_plane::{
    ControlChannel, Operation, REQUEST_SIZE, Reply, Request, buffer,
};
use crate::transport::tcp::TcpTransport;
use crate::{
    Error, HostMemory, RemoteEffect, RemoteRegion, RemoteSegment, Result, SourceEpoch, TransferOp,
    Transport,
};

pub(super) fn read_request(channel: &mut ControlChannel<'_>) -> Result<Request> {
    let mut bytes = [0; REQUEST_SIZE];
    channel.read_exact(&mut bytes)?;
    Request::decode(&bytes)
}

/// Each frame is scoped to the original registration, operation, chunk and
/// freshly pinned session. The handler only accepts the next expected step.
pub(super) struct RemoteStageOperation {
    pub(super) nonce: SourceEpoch,
    pub(super) region: u64,
}

impl RemoteStageOperation {
    pub(super) fn receipt(
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

    pub(super) fn respond(
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

    pub(super) fn expect(
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

    pub(super) fn send(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        channel.write_all(&self.frame(operation, offset, length).encode())
    }

    pub(super) fn stage_id(&self, slot: usize) -> u64 {
        crate::runtime::control_plane::u64_at(&self.nonce.0, 0).wrapping_add(slot as u64)
    }
}

impl ProxyManager {
    pub(super) fn completed_remote(&self, operation: Operation) {
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
                channel.write_all(&[
                    match request.transport {
                        Transport::Tcp => 0,
                        Transport::Rdma => 1,
                        _ => return Err(Error::Unavailable("remote staging requires TCP or RDMA")),
                    },
                    source.staging_pipeline_depth as u8,
                ])?;
                let pin = Reply::read(&mut channel, peer.epoch)?;
                let chunk = usize::try_from(pin.length).map_err(|_| Error::InvalidRange)?;
                if pin.status != 0 || chunk == 0 || chunk > 64 << 20 {
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
                let mut offset = 0;
                while offset < request.length {
                    let length = chunk.min(request.length - offset);
                    remote.send(&mut channel, Operation::Cross, offset, length)?;
                    let cross = PreparedTransfer {
                        transport: request.transport,
                        local: stage.memory()?.clone(),
                        local_offset: offset,
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
                        _ => return Err(Error::Unavailable("remote staging requires TCP or RDMA")),
                    };
                    transfer.map_err(|(error, _)| error)?;
                    // The transport ACK is followed by a session receipt only
                    // after source grants/loans finish their native cleanup.
                    remote.receipt(&mut channel, Operation::Cross, offset, length)?;
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                        delegate(&mut channel, offset)?;
                    } else if offset + chunk * depth < request.length {
                        delegate(&mut channel, offset + chunk * depth)?;
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
}
