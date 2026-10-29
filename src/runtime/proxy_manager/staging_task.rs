//! One admitted task owns its bounded CROSS queue and control-connection worker.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Instant;

use super::ProxyManager;
use super::remote_stage_operation::RemoteStageOperation;
use super::stage_buffer_cache::StageBufferCache;
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
use crate::transport::tcp::TcpTransport;
use crate::{
    Error, Permission, RegisteredMemory, RemoteEffect, RemoteRegion, RemoteSegment, Result,
    SourceEpoch, TransferOp, Transport,
};

const WORKER_STOPPED: &str = "staging worker stopped before completion";

enum Command {
    Cross {
        local: RegisteredMemory,
        offset: usize,
    },
    Unpin,
}

pub(super) struct StagingTask<'a> {
    channel: ControlChannel<'a>,
    remote: RemoteStageOperation,
    operation: TransferOp,
    peer: &'a RemoteSegment,
    request: &'a PreparedTransfer,
    source: &'a Source,
    chunk: usize,
    remote_width: usize,
}

struct WorkerToken<'a>(&'a ProxyManager);

impl Drop for WorkerToken<'_> {
    fn drop(&mut self) {
        self.0.progress_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<'a> StagingTask<'a> {
    pub(super) fn new(
        operation: TransferOp,
        peer: &'a RemoteSegment,
        request: &'a PreparedTransfer,
        source: &'a Source,
        chunk_size: usize,
        deadline: Instant,
    ) -> Result<Self> {
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
        channel.write_all(&(chunk_size as u64).to_be_bytes())?;
        let pin = Reply::read(&mut channel, peer.epoch)?;
        let chunk = usize::try_from(pin.length).map_err(|_| Error::InvalidRange)?;
        if pin.status != 0 || chunk == 0 || chunk > chunk_size {
            return Err(Error::Protocol("Pin chunk size"));
        }
        let mut nonce = [0; 16];
        channel.read_exact(&mut nonce)?;
        let mut width = [0];
        channel.read_exact(&mut width)?;
        let remote_width = width[0] as usize;
        if remote_width == 0
            || remote_width > source.staging_pipeline_depth
            || remote_width > request.length.div_ceil(chunk)
        {
            return Err(Error::Protocol("Pin pipeline depth"));
        }
        Ok(Self {
            channel,
            remote: RemoteStageOperation {
                nonce: SourceEpoch(nonce),
                region: request.remote.id,
            },
            operation,
            peer,
            request,
            source,
            chunk,
            remote_width,
        })
    }

    pub(super) fn run(
        self,
        stages: &mut StageBufferCache<'_>,
        effect: &mut RemoteEffect,
    ) -> Result<()> {
        let width = stages.width();
        let (send, commands) = mpsc::sync_channel(width);
        let (complete, completed) = mpsc::sync_channel(width);
        let stopping = AtomicBool::new(false);
        let deadline = self.channel.deadline;
        let chunk = self.chunk;
        let request = self.request;
        let operation = self.operation;
        let manager = &self.source.proxy;
        thread::scope(|scope| {
            manager.progress_workers.fetch_add(1, Ordering::AcqRel);
            let _worker_token = WorkerToken(manager);
            let stopping = &stopping;
            let worker = thread::Builder::new()
                .name("orbitte-staging".into())
                .spawn_scoped(scope, move || {
                    let mut effect = RemoteEffect::None;
                    let result = self.progress(commands, complete, stopping, &mut effect);
                    (result, effect)
                })?;
            let result = (|| {
                if operation == TransferOp::Write {
                    for slot in 0..width {
                        remaining(deadline)?;
                        let offset = slot * chunk;
                        stages.copy(
                            slot,
                            request,
                            offset,
                            chunk.min(request.length - offset),
                            operation,
                        )?;
                    }
                }
                let count = request.length.div_ceil(chunk);
                for first in (0..count).step_by(width) {
                    let end = (first + width).min(count);
                    for index in first..end {
                        remaining(deadline)?;
                        let slot = index % width;
                        stages.complete(slot)?;
                        send.try_send(Command::Cross {
                            local: stages.registration(slot).memory()?.clone(),
                            offset: index * chunk,
                        })
                        .map_err(|error| match error {
                            mpsc::TrySendError::Full(_) => Error::ResourceExhausted,
                            mpsc::TrySendError::Disconnected(_) => Error::Internal(WORKER_STOPPED),
                        })?;
                    }
                    for index in first..end {
                        let offset = receive(&completed, deadline)?;
                        if offset != index * chunk {
                            return Err(Error::Internal("staging CROSS completion order"));
                        }
                        // Only the local slot is reusable here. The worker may
                        // still be waiting for the corresponding Delegate receipt.
                        let next = if operation == TransferOp::Read {
                            offset
                        } else {
                            offset + width * chunk
                        };
                        if next < request.length {
                            remaining(deadline)?;
                            stages.copy(
                                index % width,
                                request,
                                next,
                                chunk.min(request.length - next),
                                operation,
                            )?;
                        }
                    }
                }
                stages.drain()?;
                remaining(deadline)?;
                send.try_send(Command::Unpin)
                    .map_err(|_| Error::Internal(WORKER_STOPPED))?;
                Ok(())
            })();
            if result.is_err() {
                stopping.store(true, Ordering::Release);
                manager.cancelled_tasks.fetch_add(1, Ordering::AcqRel);
            }
            // Never strand a worker publishing a completion during cancellation.
            // Joining also drops every queued registration before MR retirement.
            drop(send);
            drop(completed);
            let terminal = match worker.join() {
                Ok((terminal, remote_effect)) => {
                    *effect = remote_effect;
                    terminal
                }
                Err(_) => {
                    if operation == TransferOp::Write {
                        *effect = RemoteEffect::Unknown;
                    }
                    Err(Error::Unquiesced {
                        operation: "staging worker panic",
                        code: -1,
                    })
                }
            };
            match (result, terminal) {
                (_, Err(error @ Error::Unquiesced { .. }))
                | (Err(Error::Internal(WORKER_STOPPED)), Err(error)) => Err(error),
                (Err(error), _) => Err(error),
                (Ok(()), terminal) => terminal,
            }
        })
    }

    fn progress(
        mut self,
        commands: Receiver<Command>,
        complete: SyncSender<usize>,
        stopping: &AtomicBool,
        effect: &mut RemoteEffect,
    ) -> Result<()> {
        if self.operation == TransferOp::Read {
            for slot in 0..self.remote_width {
                if stopping.load(Ordering::Acquire) {
                    return Err(Error::Cancelled);
                }
                self.delegate(slot * self.chunk)?;
            }
        }
        let mut expected = 0;
        loop {
            let command = receive(&commands, self.channel.deadline)?;
            if stopping.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            match command {
                Command::Cross { local, offset } => {
                    if offset != expected || offset >= self.request.length {
                        return Err(Error::Internal("staging CROSS submission order"));
                    }
                    let length = self.chunk.min(self.request.length - offset);
                    self.cross(local, offset, length)?;
                    if stopping.load(Ordering::Acquire) {
                        return Err(Error::Cancelled);
                    }
                    let next = if self.operation == TransferOp::Write {
                        Some(offset)
                    } else {
                        let next = offset + self.chunk * self.remote_width;
                        (next < self.request.length).then_some(next)
                    };
                    if let Some(next) = next {
                        if self.operation == TransferOp::Write {
                            *effect = RemoteEffect::Unknown;
                        }
                        self.remote.send(
                            &mut self.channel,
                            Operation::Delegate,
                            next,
                            self.chunk.min(self.request.length - next),
                        )?;
                    }
                    complete.send(offset).map_err(|_| Error::Cancelled)?;
                    if let Some(next) = next {
                        self.remote.receipt(
                            &mut self.channel,
                            Operation::Delegate,
                            next,
                            self.chunk.min(self.request.length - next),
                        )?;
                    }
                    expected += length;
                }
                Command::Unpin => {
                    if expected != self.request.length {
                        return Err(Error::Internal("staging Unpin before all CROSS"));
                    }
                    self.remote
                        .send(&mut self.channel, Operation::Unpin, expected, 0)?;
                    return self
                        .remote
                        .receipt(&mut self.channel, Operation::Unpin, expected, 0);
                }
            }
        }
    }

    fn delegate(&mut self, offset: usize) -> Result<()> {
        let length = self.chunk.min(self.request.length - offset);
        self.remote
            .send(&mut self.channel, Operation::Delegate, offset, length)?;
        self.remote
            .receipt(&mut self.channel, Operation::Delegate, offset, length)
    }

    fn cross(&mut self, local: RegisteredMemory, offset: usize, length: usize) -> Result<()> {
        self.remote
            .send(&mut self.channel, Operation::Cross, offset, length)?;
        let cross = PreparedTransfer {
            transport: self.request.transport,
            local,
            local_offset: 0,
            remote: RemoteRegion {
                epoch: self.peer.epoch,
                id: self
                    .remote
                    .stage_id((offset / self.chunk) % self.remote_width),
                address: 0,
                length: self.chunk,
                permission: Permission::RemoteReadWrite,
            },
            remote_offset: 0,
            length,
        };
        match cross.transport {
            Transport::Tcp => TcpTransport::transfer_on(
                self.operation,
                self.peer,
                &cross,
                &mut self.channel,
                &self.source.proxy,
            ),
            Transport::Rdma => {
                #[cfg(feature = "rdma")]
                {
                    self.source
                        .rdma
                        .as_ref()
                        .ok_or(Error::Unavailable("RDMA is not configured"))?
                        .transfer_on(
                            self.operation,
                            self.peer,
                            &cross,
                            self.source.epoch,
                            &mut self.channel,
                        )
                }
                #[cfg(not(feature = "rdma"))]
                {
                    return Err(Error::Unavailable("build with --features rdma"));
                }
            }
            _ => return Err(Error::Unavailable("remote staging requires TCP or RDMA")),
        }
        .map_err(|(error, _)| error)?;
        self.remote
            .receipt(&mut self.channel, Operation::Cross, offset, length)
    }
}

fn receive<T>(receiver: &Receiver<T>, deadline: Instant) -> Result<T> {
    receiver
        .recv_timeout(remaining(deadline)?)
        .map_err(|error| match error {
            mpsc::RecvTimeoutError::Timeout => Error::Timeout,
            mpsc::RecvTimeoutError::Disconnected => Error::Internal(WORKER_STOPPED),
        })
}
