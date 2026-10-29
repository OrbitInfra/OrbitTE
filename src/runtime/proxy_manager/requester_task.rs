//! Bounded requester PRE/CROSS/POST with independent local and remote rings.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Instant;

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
                            session
                                .send
                                .try_send(Command::Cross {
                                    local: stages.registration(slot).memory()?.clone(),
                                    offset: index * chunk,
                                })
                                .map_err(|error| match error {
                                    mpsc::TrySendError::Full(_) => Error::ResourceExhausted,
                                    mpsc::TrySendError::Disconnected(_) => {
                                        Error::Internal(WORKER_STOPPED)
                                    }
                                })?;
                        }
                        for index in first..end {
                            let offset = receive(&session.completed, deadline)?;
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

impl Session {
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
