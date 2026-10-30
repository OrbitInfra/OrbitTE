mod task;

pub(crate) use task::TcpTask;

use std::time::Instant;

use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{ControlChannel, Operation, Request, buffer};
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
        #[cfg(feature = "cuda")] proxy: &std::sync::Arc<
            crate::runtime::proxy_manager::ProxyManager,
        >,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut task = TcpTask::new(
            operation,
            peer,
            request,
            deadline,
            #[cfg(feature = "cuda")]
            proxy,
        )
        .map_err(|error| (error, RemoteEffect::None))?;
        let mut channel = ControlChannel::connect(peer.endpoint, deadline)
            .map_err(|error| (error, RemoteEffect::None))?;
        let result = (|| {
            let mut idle = 0u32;
            loop {
                let mut advanced = false;
                if task.progress(
                    &mut channel,
                    &mut advanced,
                    #[cfg(feature = "cuda")]
                    proxy,
                )? {
                    return Ok(());
                }
                if advanced {
                    idle = 0;
                } else if idle < 128 {
                    std::thread::yield_now();
                } else {
                    std::thread::park_timeout(
                        std::time::Duration::from_micros(200).min(remaining(deadline)?),
                    );
                }
                idle = idle.saturating_add(1);
            }
        })();
        result.map_err(|error| (error, task.effect()))
    }

    fn payload(
        operation: TransferOp,
        request: &PreparedTransfer,
        deadline: Instant,
        #[cfg(feature = "cuda")] proxy: &std::sync::Arc<
            crate::runtime::proxy_manager::ProxyManager,
        >,
    ) -> Result<Vec<u8>> {
        let mut bytes = buffer(request.length)?;
        let _ = deadline;
        if operation == TransferOp::Write {
            #[cfg(feature = "cuda")]
            proxy.read(&request.local, request.local_offset, &mut bytes, deadline)?;
            #[cfg(not(feature = "cuda"))]
            request.local.read(request.local_offset, &mut bytes)?;
        }
        Ok(bytes)
    }

    pub(crate) fn serve(
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
        #[cfg(feature = "cuda")] proxy: &std::sync::Arc<
            crate::runtime::proxy_manager::ProxyManager,
        >,
    ) -> Result<()> {
        let mut payload = buffer(length)?;
        match request.operation {
            Operation::TcpRead => {
                #[cfg(feature = "cuda")]
                proxy.read(memory, offset, &mut payload, channel.deadline)?;
                #[cfg(not(feature = "cuda"))]
                memory.read(offset, &mut payload)?;
                channel.reply(epoch, 0, length as u64)?;
                channel.write_all(&payload)
            }
            Operation::TcpWrite => {
                channel.reply(epoch, 9, 0)?;
                channel.read_exact(&mut payload)?;
                remaining(channel.deadline)?;
                #[cfg(feature = "cuda")]
                proxy.write(memory, offset, &payload, channel.deadline)?;
                #[cfg(not(feature = "cuda"))]
                memory.write(offset, &payload)?;
                channel.reply(epoch, 0, 0)
            }
            _ => Err(Error::Protocol("unexpected TCP payload operation")),
        }
    }
}
