//! Transport-owned data threads. A context's SQ/RQ pair has one polling owner.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::RdmaStats;
use super::progress::{ContextProgress, ProgressState, Record, Schedule};
use crate::engine::remaining;
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct Workers {
    pool: Mutex<Pool>,
}

#[derive(Default)]
struct Pool {
    workers: Vec<(Arc<WorkerContext>, JoinHandle<()>)>,
    stopped: bool,
}

struct WorkerContext {
    pending: Mutex<Vec<Lane>>,
    thread: OnceLock<thread::Thread>,
    stopping: AtomicBool,
    exited: Mutex<bool>,
    changed: Condvar,
    usage: Arc<Mutex<RdmaStats>>,
}

struct Lane {
    state: Arc<ProgressState>,
    records: Vec<Arc<Record>>,
    cursor: usize,
}

impl Drop for Lane {
    fn drop(&mut self) {
        // Detachment is published only after this worker's request snapshot is gone.
        self.records.clear();
        self.state.detached();
    }
}

struct WorkerExit {
    context: Arc<WorkerContext>,
    lanes: Vec<Lane>,
}

impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.context.stopping.store(true, Ordering::Release);
        // On a panic Lane::drop retains each unfinished owner; no native retry.
        self.lanes.clear();
        self.context
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        self.context
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .progress_workers -= 1;
        *self
            .context
            .exited
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = true;
        self.context.changed.notify_all();
    }
}

impl Workers {
    pub(super) fn attach(&self, nic_id: usize, progress: &ContextProgress) -> Result<()> {
        if !std::ptr::eq(self, progress.workers.as_ref()) {
            return Err(Error::Internal(
                "RDMA progress belongs to another worker pool",
            ));
        }
        let mut pool = self
            .pool
            .lock()
            .map_err(|_| Error::Internal("RDMA workers lock poisoned"))?;
        if pool.stopped {
            return Err(Error::Unavailable("RDMA workers are stopped"));
        }
        let count = progress.shared.config.num_workers;
        if pool.workers.is_empty() {
            for id in 0..count {
                let context = Arc::new(WorkerContext {
                    pending: Mutex::default(),
                    thread: OnceLock::new(),
                    stopping: AtomicBool::new(false),
                    exited: Mutex::new(false),
                    changed: Condvar::new(),
                    usage: Arc::clone(&progress.shared.usage),
                });
                context
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .progress_workers += 1;
                let run = Arc::clone(&context);
                match thread::Builder::new()
                    .name(format!("orbitte-rdma-{id}"))
                    .spawn(move || run.run())
                {
                    Ok(handle) => {
                        let _ = context.thread.set(handle.thread().clone());
                        context
                            .usage
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .opened_progress_workers += 1;
                        pool.workers.push((context, handle));
                    }
                    Err(error) => {
                        context
                            .usage
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .progress_workers -= 1;
                        pool.stopped = true;
                        for (worker, _) in &pool.workers {
                            worker.stop();
                        }
                        for (_, worker) in pool.workers.drain(..) {
                            let _ = worker.join();
                        }
                        return Err(Error::Io(error));
                    }
                }
            }
        }
        let (worker, handle) = &pool.workers[nic_id % pool.workers.len()];
        let mut pending = worker
            .pending
            .lock()
            .map_err(|_| Error::Internal("RDMA worker queue poisoned"))?;
        if worker.stopping.load(Ordering::Acquire) {
            return Err(Error::Unquiesced {
                operation: "RDMA progress worker stopped",
                code: libc::EIO,
            });
        }
        progress.shared.attach(handle.thread().clone())?;
        pending.push(Lane {
            state: Arc::clone(&progress.shared),
            records: Vec::with_capacity(progress.shared.config.max_endpoints),
            cursor: 0,
        });
        handle.thread().unpark();
        Ok(())
    }

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        let mut pool = self
            .pool
            .lock()
            .map_err(|_| Error::Internal("RDMA workers lock poisoned"))?;
        pool.stopped = true;
        for (worker, _) in &pool.workers {
            worker.stop();
        }
        for (worker, _) in &pool.workers {
            let mut exited = worker
                .exited
                .lock()
                .map_err(|_| Error::Internal("RDMA worker exit lock poisoned"))?;
            while !*exited {
                exited = worker
                    .changed
                    .wait_timeout(exited, remaining(deadline)?)
                    .map_err(|_| Error::Internal("RDMA worker exit lock poisoned"))?
                    .0;
            }
        }
        let mut failure = None;
        for (_, worker) in pool.workers.drain(..) {
            if worker.join().is_err() {
                failure.get_or_insert(Error::Unquiesced {
                    operation: "RDMA progress worker",
                    code: libc::EIO,
                });
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for Workers {
    fn drop(&mut self) {
        let _ = self.close(Instant::now() + Duration::from_secs(300));
    }
}

impl WorkerContext {
    fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.thread.get() {
            worker.unpark();
        }
    }

    fn run(self: Arc<Self>) {
        let mut exit = WorkerExit {
            context: Arc::clone(&self),
            lanes: Vec::new(),
        };
        let mut cursor = 0usize;
        loop {
            exit.lanes
                .append(&mut self.pending.lock().unwrap_or_else(|p| p.into_inner()));
            let stopping = self.stopping.load(Ordering::Acquire);
            if stopping && exit.lanes.is_empty() {
                return;
            }
            let count = exit.lanes.len();
            let mut wait: Option<Duration> = None;
            let mut finished = Vec::new();
            for offset in 0..count {
                let index = (offset + cursor % count) % count;
                let lane = &mut exit.lanes[index];
                if stopping {
                    lane.state.stop();
                }
                #[cfg(test)]
                tests::before_tick(&lane.state);
                match lane.state.tick(&mut lane.records, &mut lane.cursor) {
                    Schedule::Finished => finished.push(index),
                    Schedule::Again => wait = Some(Duration::ZERO),
                    Schedule::Wait(Some(duration)) => {
                        wait = Some(wait.map_or(duration, |current| current.min(duration)))
                    }
                    Schedule::Wait(None) => {}
                }
            }
            // Rotate the first context so aggregate debt does not privilege a NIC.
            cursor = cursor.wrapping_add(1);
            finished.sort_unstable();
            for index in finished.into_iter().rev() {
                exit.lanes.remove(index);
            }
            if stopping {
                continue;
            }
            match wait {
                Some(duration) if duration.is_zero() => {}
                Some(duration) => thread::park_timeout(duration),
                None => thread::park(),
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/workers.rs"]
mod tests;
