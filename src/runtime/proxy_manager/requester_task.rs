//! Bounded requester PRE/CROSS/POST with independent local and remote rings.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use super::ProxyManager;
use super::stage_buffer_cache::StageBufferCache;
use super::stage_registration::{PinnedStage, StageRegistration};
use super::staging_task::{Command, StagingTask, WORKER_STOPPED, receive};
use super::worker_shard::{TaskHandle, Terminal};
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::memory::Allocation;
use crate::runtime::control_plane::buffer;
use crate::{Error, HostMemory, RemoteEffect, RemoteSegment, TransferOp};

impl ProxyManager {
    pub(crate) fn transfer_remote(
        self: &Arc<Self>,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        source: &Arc<Source>,
        deadline: Instant,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut effect = RemoteEffect::None;
        let result = (|| {
            let mut transfer = |stages: &mut StageBufferCache<'_>| {
                let width = stages.width();
                let (send, commands) = mpsc::sync_channel(width);
                let (complete, completed) = mpsc::sync_channel(width);
                let (pin, pinned) = mpsc::sync_channel(1);
                let stopping = Arc::new(AtomicBool::new(false));
                let stop = Arc::clone(&stopping);
                let task_peer = peer.clone();
                let task_request = request.clone();
                let task_source = Arc::clone(source);
                let chunk_size = self.chunk_size;
                let worker = self
                    .workers
                    .submit(operation, deadline, stopping, move || {
                        let mut effect = RemoteEffect::None;
                        let result = (|| {
                            let task = StagingTask::new(
                                operation,
                                &task_peer,
                                &task_request,
                                &task_source,
                                chunk_size,
                                deadline,
                                &stop,
                            )?;
                            pin.send(task.chunk).map_err(|_| Error::Cancelled)?;
                            task.progress(commands, complete, &stop, &mut effect)
                        })();
                        (result, effect)
                    })?;
                let session = Session {
                    send,
                    completed,
                    worker,
                };
                let result = (|| {
                    let chunk = receive(&pinned, deadline)?;
                    session.transfer_event_loop(stages, request, operation, chunk, deadline)?;
                    stages.drain()?;
                    remaining(deadline)?;
                    session
                        .send
                        .try_send(Command::Unpin)
                        .map_err(|_| Error::Internal(WORKER_STOPPED))?;
                    Ok(())
                })();
                if result.is_err() {
                    session.worker.cancel();
                    self.cancelled_tasks.fetch_add(1, Ordering::AcqRel);
                }
                let (terminal, remote_effect) = session.finish();
                effect = remote_effect;
                match (result, terminal) {
                    (_, Err(error @ Error::Unquiesced { .. }))
                    | (Err(Error::Internal(WORKER_STOPPED)), Err(error)) => Err(error),
                    (Err(error), _) => Err(error),
                    (Ok(()), terminal) => terminal,
                }
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

// Struct fields drop in declaration order. On unwind, disconnect both chunk
// queues before TaskHandle cancels/waits, so a worker publishing a full queue
// can exit before the caller's outer StageBufferCache releases any owner.
struct Session {
    send: mpsc::SyncSender<Command>,
    completed: mpsc::Receiver<usize>,
    worker: TaskHandle,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StageState {
    Pre,
    Cross,
    Post,
    InflightPre,
    InflightCross,
    InflightPost,
    Finish,
}

struct Chunk {
    offset: usize,
    state: StageState,
}

impl Session {
    fn transfer_event_loop(
        &self,
        stages: &mut StageBufferCache<'_>,
        request: &PreparedTransfer,
        operation: TransferOp,
        size: usize,
        deadline: Instant,
    ) -> crate::Result<()> {
        let width = stages.width();
        let first = if operation == TransferOp::Write {
            StageState::Pre
        } else {
            StageState::Cross
        };
        let mut chunks: Vec<_> = (0..width)
            .map(|slot| Chunk {
                offset: slot * size,
                state: first,
            })
            .collect();
        let mut submitted = 0;
        let mut finished = 0;
        let mut idle_sweeps = 0u32;
        while finished != width {
            remaining(deadline)?;
            let mut progress = false;
            loop {
                let offset = match self.completed.try_recv() {
                    Ok(offset) => offset,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        return Err(Error::Internal(WORKER_STOPPED));
                    }
                };
                let chunk = &mut chunks[(offset / size) % width];
                if offset != chunk.offset || chunk.state != StageState::InflightCross {
                    return Err(Error::Internal("staging CROSS completion order"));
                }
                if operation == TransferOp::Read {
                    chunk.state = StageState::Post;
                } else {
                    chunk.offset += width * size;
                    chunk.state = StageState::Pre;
                }
                progress = true;
            }
            for (slot, chunk) in chunks.iter_mut().enumerate() {
                if chunk.state == StageState::Finish {
                    continue;
                }
                if chunk.offset >= request.length {
                    chunk.state = StageState::Finish;
                    finished += 1;
                    progress = true;
                    continue;
                }
                let before = chunk.state;
                match chunk.state {
                    StageState::Pre | StageState::Post => {
                        stages.copy(
                            slot,
                            request,
                            chunk.offset,
                            size.min(request.length - chunk.offset),
                            operation,
                        )?;
                        chunk.state = if operation == TransferOp::Write {
                            StageState::InflightPre
                        } else {
                            StageState::InflightPost
                        };
                    }
                    StageState::InflightPre | StageState::InflightPost => {
                        if stages.query(slot)? {
                            chunk.state = StageState::Cross;
                            if operation == TransferOp::Read {
                                chunk.offset += width * size;
                            }
                        }
                    }
                    StageState::Cross if chunk.offset == submitted => {
                        let command = Command::Cross {
                            local: stages.registration(slot).memory()?.clone(),
                            offset: chunk.offset,
                        };
                        match self.send.try_send(command) {
                            Ok(()) => {
                                submitted += size.min(request.length - chunk.offset);
                                chunk.state = StageState::InflightCross;
                            }
                            Err(mpsc::TrySendError::Full(_)) => (),
                            Err(mpsc::TrySendError::Disconnected(_)) => {
                                return Err(Error::Internal(WORKER_STOPPED));
                            }
                        }
                    }
                    _ => (),
                }
                progress |= chunk.state != before;
            }
            if progress {
                idle_sweeps = 0;
            } else {
                if idle_sweeps < 128 {
                    std::thread::yield_now();
                } else {
                    let delay =
                        Duration::from_micros((1u64 << (idle_sweeps - 128).min(8)).min(200));
                    std::thread::park_timeout(delay.min(remaining(deadline)?));
                }
                idle_sweeps = idle_sweeps.saturating_add(1);
            }
        }
        Ok(())
    }

    fn finish(self) -> Terminal {
        let Self {
            send,
            completed,
            worker,
        } = self;
        drop(send);
        drop(completed);
        worker.finish(false)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/requester_task.rs"]
mod tests;
