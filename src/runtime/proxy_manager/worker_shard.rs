//! Shared session workers. Idle shards own queues, never engines or native memory.

use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use super::StagingStats;
use crate::engine::remaining;
use crate::{Error, RemoteEffect, Result, TransferOp};

const MAX_SHARDS: usize = 8;
const QUEUE_CAPACITY: usize = 64;

pub(super) type Terminal = (Result<()>, RemoteEffect);

#[derive(Default)]
pub(super) struct Workers {
    pool: Mutex<Pool>,
}

#[derive(Default)]
struct Pool {
    shards: Vec<(Arc<WorkerShard>, Option<JoinHandle<()>>)>,
    closed: bool,
    next: usize,
}

struct WorkerShard {
    state: Mutex<ShardState>,
    changed: Condvar,
}

struct ShardState {
    queue: VecDeque<QueuedTask>,
    active: Option<Arc<AtomicBool>>,
    open: bool,
    exited: bool,
}

struct QueuedTask {
    run: Box<dyn FnOnce() -> Terminal + Send>,
    operation: TransferOp,
    deadline: Instant,
    stopping: Arc<AtomicBool>,
    finished: mpsc::SyncSender<Terminal>,
}

pub(super) struct TaskHandle {
    shard: Arc<WorkerShard>,
    stopping: Arc<AtomicBool>,
    finished: Option<mpsc::Receiver<Terminal>>,
    operation: TransferOp,
}

impl Workers {
    pub(super) fn submit(
        &self,
        operation: TransferOp,
        deadline: Instant,
        stopping: Arc<AtomicBool>,
        run: impl FnOnce() -> Terminal + Send + 'static,
    ) -> Result<TaskHandle> {
        remaining(deadline)?;
        let mut pool = self
            .pool
            .lock()
            .map_err(|_| Error::Internal("staging workers lock"))?;
        if pool.closed {
            return Err(Error::RequesterClosed);
        }
        let mut selected = None;
        let mut minimum = usize::MAX;
        for offset in 0..pool.shards.len() {
            let id = (pool.next + offset) % pool.shards.len();
            let state = pool.shards[id]
                .0
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if !state.open || state.queue.len() == QUEUE_CAPACITY {
                continue;
            }
            let load = state.queue.len() + usize::from(state.active.is_some());
            if load < minimum {
                selected = Some(id);
                minimum = load;
            }
        }
        if minimum != 0 && pool.shards.len() < MAX_SHARDS {
            let id = pool.shards.len();
            let shard = Arc::new(WorkerShard {
                state: Mutex::new(ShardState {
                    queue: VecDeque::new(),
                    active: None,
                    open: true,
                    exited: false,
                }),
                changed: Condvar::new(),
            });
            let worker = Arc::clone(&shard);
            let handle = thread::Builder::new()
                .name(format!("orbitte-staging-{id}"))
                .spawn(move || worker.run())?;
            // Publication follows successful spawn. Existing shards survive an
            // allocation/thread failure and no task has been admitted yet.
            pool.shards.push((shard, Some(handle)));
            selected = Some(id);
        }
        let id = selected.ok_or(Error::ResourceExhausted)?;
        pool.next = (id + 1) % pool.shards.len();
        let shard = Arc::clone(&pool.shards[id].0);
        let (finished, completion) = mpsc::sync_channel(1);
        shard
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .push_back(QueuedTask {
                run: Box::new(run),
                operation,
                deadline,
                stopping: Arc::clone(&stopping),
                finished,
            });
        shard.changed.notify_one();
        Ok(TaskHandle {
            shard,
            stopping,
            finished: Some(completion),
            operation,
        })
    }

    pub(super) fn stats(&self, stats: &mut StagingStats) {
        let pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
        stats.opened_worker_shards = pool.shards.len() as u64;
        for (shard, _) in &pool.shards {
            let state = shard.state.lock().unwrap_or_else(|p| p.into_inner());
            stats.worker_shards += usize::from(!state.exited);
            stats.queued_tasks += state.queue.len();
            stats.active_tasks += usize::from(state.active.is_some());
        }
    }

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        let shards = {
            let mut pool = self
                .pool
                .lock()
                .map_err(|_| Error::Internal("staging workers lock"))?;
            pool.closed = true;
            pool.shards
                .iter()
                .map(|(shard, _)| Arc::clone(shard))
                .collect::<Vec<_>>()
        };
        for shard in &shards {
            shard.stop();
        }
        for shard in &shards {
            let mut state = shard.state.lock().unwrap_or_else(|p| p.into_inner());
            while !state.exited {
                state = shard
                    .changed
                    .wait_timeout(state, remaining(deadline)?)
                    .unwrap_or_else(|p| p.into_inner())
                    .0;
            }
        }
        let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
        for (_, handle) in &mut pool.shards {
            if let Some(handle) = handle.take() {
                handle.join().map_err(|_| Error::Unquiesced {
                    operation: "staging shard exit",
                    code: -1,
                })?;
            }
        }
        Ok(())
    }
}

impl WorkerShard {
    fn run(&self) {
        loop {
            let task = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                while state.open && state.queue.is_empty() {
                    state = self.changed.wait(state).unwrap_or_else(|p| p.into_inner());
                }
                if !state.open {
                    state.exited = true;
                    self.changed.notify_all();
                    return;
                }
                let Some(task) = state.queue.pop_front() else {
                    continue;
                };
                state.active = Some(Arc::clone(&task.stopping));
                task
            };
            let (finished, terminal) = task.execute();
            self.state.lock().unwrap_or_else(|p| p.into_inner()).active = None;
            self.changed.notify_all();
            // execute consumed the closure and all of its captures. No queued
            // registration, control channel or Source survives this receipt.
            let _ = finished.send(terminal);
        }
    }

    fn stop(&self) {
        let queued = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.open = false;
            if let Some(active) = &state.active {
                active.store(true, Ordering::Release);
            }
            self.changed.notify_all();
            std::mem::take(&mut state.queue)
        };
        for task in queued {
            task.stopping.store(true, Ordering::Release);
            let (finished, terminal) = task.execute();
            let _ = finished.send(terminal);
        }
    }
}

impl QueuedTask {
    fn execute(self) -> (mpsc::SyncSender<Terminal>, Terminal) {
        let Self {
            run,
            operation,
            deadline,
            stopping,
            finished,
        } = self;
        let ready = if stopping.load(Ordering::Acquire) {
            Err(Error::Cancelled)
        } else {
            remaining(deadline).map(|_| ())
        };
        let terminal = match ready {
            Ok(()) => catch_unwind(AssertUnwindSafe(run)).unwrap_or_else(|_| {
                (
                    Err(Error::Unquiesced {
                        operation: "staging worker panic",
                        code: -1,
                    }),
                    if operation == TransferOp::Write {
                        RemoteEffect::Unknown
                    } else {
                        RemoteEffect::None
                    },
                )
            }),
            Err(error) => {
                drop(run);
                (Err(error), RemoteEffect::None)
            }
        };
        (finished, terminal)
    }
}

impl TaskHandle {
    pub(super) fn cancel(&self) {
        self.stopping.store(true, Ordering::Release);
        let queued = {
            let mut state = self.shard.state.lock().unwrap_or_else(|p| p.into_inner());
            state
                .queue
                .iter()
                .position(|task| Arc::ptr_eq(&task.stopping, &self.stopping))
                .and_then(|index| state.queue.remove(index))
        };
        if let Some(task) = queued {
            let (finished, terminal) = task.execute();
            let _ = finished.send(terminal);
        }
    }

    fn receive(&mut self) -> Terminal {
        let terminal = self
            .finished
            .take()
            .and_then(|finished| finished.recv().ok());
        terminal.unwrap_or_else(|| {
            (
                Err(Error::Unquiesced {
                    operation: "staging task terminal lost",
                    code: -1,
                }),
                if self.operation == TransferOp::Write {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                },
            )
        })
    }

    pub(super) fn finish(mut self, cancel: bool) -> Terminal {
        if cancel {
            self.cancel();
        }
        self.receive()
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        if self.finished.is_some() {
            self.cancel();
            // A deadline/disconnect on the data queue never substitutes for
            // this task's native drain and destruction acknowledgement.
            let _ = self.receive();
        }
    }
}

impl Drop for Workers {
    fn drop(&mut self) {
        let pool = self.pool.get_mut().unwrap_or_else(|p| p.into_inner());
        for (shard, _) in &pool.shards {
            shard.stop();
        }
        for (_, handle) in &mut pool.shards {
            if let Some(handle) = handle.take() {
                if handle.thread().id() != thread::current().id() {
                    let _ = handle.join();
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/worker_shard.rs"]
mod tests;
