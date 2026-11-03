use super::{
    HpTcpConfig, buffer_registry,
    protocol::{REQUEST, Request, Response, Status},
    task::{self, Budget, STEP},
    workers::Workers,
};
use crate::{Error, Result, TransferOp, engine::Source};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

pub(super) fn start(
    workers: &Workers,
    listener: std::net::TcpListener,
    source: Arc<Source>,
    config: &HpTcpConfig,
    #[cfg(test)] retired_sessions: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<()> {
    let handles = workers.handles();
    let listener = {
        let _entered = handles[0].enter();
        TcpListener::from_std(listener)?
    };
    let max = config
        .max_outstanding_tasks
        .max(config.connections_per_peer as u64)
        .min(Semaphore::MAX_PERMITS as u64) as usize;
    let slots = Arc::new(Semaphore::new(max));
    let bytes = Budget::new(config.max_outstanding_tasks, config.max_outstanding_bytes);
    let limit = config.max_transfer_bytes;
    let timeout = Duration::from_millis(config.progress_timeout_ms);
    workers.execute(0, async move {
        let mut worker = 0;
        let mut warned = false;
        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => {
                    warned = false;
                    stream
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::Interrupted
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(e) => {
                    if !warned {
                        log::warn!("HP-TCP accept: {e}");
                        warned = true;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                }
            };
            let Ok(slot) = slots.clone().try_acquire_owned() else {
                continue;
            };
            if stream.set_nodelay(true).is_err() {
                continue;
            }
            let Ok(stream) = stream.into_std() else {
                continue;
            };
            let source = source.clone();
            let bytes = bytes.clone();
            #[cfg(test)]
            let retired = retired_sessions.clone();
            handles[worker].spawn(async move {
                if let Ok(stream) = TcpStream::from_std(stream) {
                    let _ = session(stream, source, bytes, limit, timeout).await;
                }
                drop(slot);
                #[cfg(test)]
                retired.fetch_add(1, std::sync::atomic::Ordering::Release);
            });
            worker = (worker + 1) % handles.len();
        }
    });
    Ok(())
}

async fn reply(
    socket: &mut TcpStream,
    timeout: Duration,
    id: u64,
    status: Status,
    committed: u64,
) -> Result<()> {
    task::write(
        socket,
        &Response {
            id,
            status,
            committed,
        }
        .encode(),
        timeout,
        &mut false,
    )
    .await
}

async fn session(
    mut s: TcpStream,
    source: Arc<Source>,
    budget: Arc<Budget>,
    limit: u64,
    timeout: Duration,
) -> Result<()> {
    let mut reused = false;
    loop {
        let mut header = [0; REQUEST];
        if reused {
            s.read_exact(&mut header[..1]).await?;
        }
        task::read(&mut s, &mut header[usize::from(reused)..], timeout).await?;
        let frame = match Request::decode(&header) {
            Ok(r) => r,
            Err(status) => return reply(&mut s, timeout, 0, status, 0).await,
        };
        let len = match usize::try_from(frame.length) {
            Ok(n) if frame.length <= limit => n,
            _ => return reply(&mut s, timeout, frame.id, Status::BadLength, 0).await,
        };
        let admitted = source
            .admit()
            .map_err(|_| Status::Closing)
            .and_then(|source_permit| {
                let charge = budget.reserve(len).map_err(|_| Status::Closing)?;
                let (mem, offset) = buffer_registry::lease(&source, frame, len)?;
                Ok((source_permit, charge, mem, offset))
            });
        let (_source, _charge, mem, base) = match admitted {
            Ok(owner) => owner,
            Err(status) => {
                if frame.op == TransferOp::Write {
                    let mut discard = [0; 4096];
                    let mut left = len;
                    while left != 0 {
                        let n = left.min(discard.len());
                        task::read(&mut s, &mut discard[..n], timeout).await?;
                        left -= n;
                    }
                }
                return reply(&mut s, timeout, frame.id, status, 0).await;
            }
        };
        let mut buf = crate::runtime::control_plane::buffer(len.min(STEP))?;
        if frame.op == TransferOp::Read {
            reply(&mut s, timeout, frame.id, Status::Ok, frame.length).await?;
        }
        let mut offset = 0;
        while offset < len {
            let n = buf.len().min(len - offset);
            match frame.op {
                TransferOp::Read => {
                    mem.read(base + offset, &mut buf[..n])?;
                    task::write(&mut s, &buf[..n], timeout, &mut false).await?;
                    offset += n;
                }
                TransferOp::Write => {
                    let got = tokio::time::timeout(timeout, s.read(&mut buf[..n]))
                        .await
                        .map_err(|_| Error::Timeout)??;
                    if got == 0 {
                        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
                    }
                    mem.write(base + offset, &buf[..got])?;
                    offset += got;
                }
            }
        }
        if frame.op == TransferOp::Write {
            reply(&mut s, timeout, frame.id, Status::Ok, frame.length).await?;
        }
        reused = true;
    }
}
