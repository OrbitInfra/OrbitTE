use std::time::Instant;

use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request, buffer};
use crate::{
    Error, RegisteredMemory, RemoteEffect, RemoteSegment, Result, SourceEpoch, TransferOp,
};

/// Standard TCP READ/WRITE payload ownership, independent of native RDMA.
pub(crate) struct TcpTransport;

impl TcpTransport {
    pub(crate) fn transfer(
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        deadline: Instant,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut effect = RemoteEffect::None;
        let result = (|| {
            let mut bytes = buffer(request.length)?;
            if operation == TransferOp::Write {
                request.local.read(request.local_offset, &mut bytes)?;
            }
            let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
            let operation_code = if operation == TransferOp::Read {
                Operation::TcpRead
            } else {
                Operation::TcpWrite
            };
            channel.write_all(
                &Request {
                    operation: operation_code,
                    epoch: peer.epoch,
                    region: request.remote.id,
                    offset: request.remote_offset as u64,
                    length: request.length as u64,
                }
                .encode(),
            )?;
            let reply = Reply::read(&mut channel, peer.epoch)?;
            if operation == TransferOp::Read {
                if reply.status != 0 || reply.length != request.length as u64 {
                    return Err(Error::Protocol("READ payload length/status"));
                }
                channel.read_exact(&mut bytes)?;
                remaining(deadline)?;
                request.local.write(request.local_offset, &bytes)?;
            } else {
                if reply.status != 9 {
                    return Err(Error::Protocol("WRITE missing ready response"));
                }
                effect = RemoteEffect::Unknown;
                channel.write_all(&bytes)?;
                let reply = Reply::read(&mut channel, peer.epoch)?;
                if reply.status != 0 || reply.length != 0 {
                    return Err(Error::Protocol("WRITE acknowledgement length/status"));
                }
            }
            Ok(())
        })();
        result.map_err(|error| (error, effect))
    }

    pub(crate) fn serve(
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let mut payload = buffer(length)?;
        match request.operation {
            Operation::TcpRead => {
                memory.read(offset, &mut payload)?;
                channel.reply(epoch, 0, length as u64)?;
                channel.write_all(&payload)
            }
            Operation::TcpWrite => {
                channel.reply(epoch, 9, 0)?;
                channel.read_exact(&mut payload)?;
                remaining(channel.deadline)?;
                memory.write(offset, &payload)?;
                channel.reply(epoch, 0, 0)
            }
            _ => Err(Error::Protocol("unexpected TCP payload operation")),
        }
    }
}
