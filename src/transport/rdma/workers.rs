//! Transport-owned workers, CQ-lane assignment and unfinished slice accounting.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::RdmaStats;
use super::cq_lane::{CqLane, ProgressState, Record, Schedule};
use crate::engine::remaining;
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct Workers {
    pool: Mutex<Pool>,
    pub(super) monitor: Arc<super::monitor::Monitor>,
}

#[derive(Default)]
struct Pool {
    workers: Vec<(Arc<WorkerContext>, JoinHandle<()>)>,
    stopped: bool,
    next_worker: usize,
}

struct WorkerContext {
    inflight_slices: AtomicUsize,
    affinity: crate::platform::cpu_probe::CpuAffinity,
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
    pub(super) fn attach(&self, worker_id: usize, progress: &CqLane) -> Result<()> {
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
        let count = progress.shared.config.num_lanes;
        if pool.workers.is_empty() {
            let affinities = crate::platform::cpu_probe::worker_affinities()?;
            for id in 0..count {
                let context = Arc::new(WorkerContext {
                    inflight_slices: AtomicUsize::new(0),
                    affinity: affinities[id % affinities.len()].clone(),
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
                let (started, ready) = std::sync::mpsc::sync_channel(1);
                match thread::Builder::new()
                    .name(format!("orbitte-rdma-{id}"))
                    .spawn(move || run.run(started))
                {
                    Ok(handle) => {
                        let _ = context.thread.set(handle.thread().clone());
                        context
                            .usage
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .opened_progress_workers += 1;
                        pool.workers.push((context, handle));
                        let initialized = ready
                            .recv()
                            .map_err(|_| Error::Unquiesced {
                                operation: "RDMA worker affinity startup",
                                code: libc::EIO,
                            })
                            .and_then(|result| result);
                        if let Err(error) = initialized {
                            pool.stopped = true;
                            for (worker, _) in &pool.workers {
                                worker.stop();
                            }
                            for (_, worker) in pool.workers.drain(..) {
                                let _ = worker.join();
                            }
                            return Err(error);
                        }
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
        let (worker, handle) = pool
            .workers
            .get(worker_id)
            .ok_or(Error::Internal("RDMA CQ lane has no worker"))?;
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
        progress
            .worker_id
            .set(worker_id)
            .map_err(|_| Error::Internal("RDMA CQ lane already assigned"))?;
        #[cfg(test)]
        super::tests::expect_cq_lane(progress, worker_id);
        progress.shared.attach(handle.thread().clone())?;
        pending.push(Lane {
            state: Arc::clone(&progress.shared),
            records: Vec::with_capacity(
                progress.shared.config.max_endpoints + progress.shared.config.max_grants,
            ),
            cursor: 0,
        });
        handle.thread().unpark();
        Ok(())
    }

    pub(super) fn order(&self, lanes: &mut [Arc<CqLane>]) -> Result<()> {
        let pool = self
            .pool
            .lock()
            .map_err(|_| Error::Internal("RDMA workers lock poisoned"))?;
        if pool.stopped {
            return Err(Error::Unavailable("RDMA workers are stopped"));
        }
        let count = pool.workers.len();
        lanes.sort_by_cached_key(|lane| {
            let id = *lane.worker_id.get().unwrap_or(&usize::MAX);
            pool.workers
                .get(id)
                .map_or((usize::MAX, usize::MAX), |(worker, _)| {
                    (
                        worker.inflight_slices.load(Ordering::Acquire),
                        (id + count - pool.next_worker % count) % count,
                    )
                })
        });
        Ok(())
    }

    pub(super) fn select(&self, lanes: &[Arc<CqLane>]) -> Result<(Arc<CqLane>, SliceLease)> {
        let mut pool = self
            .pool
            .lock()
            .map_err(|_| Error::Internal("RDMA workers lock poisoned"))?;
        if pool.stopped || pool.workers.is_empty() {
            return Err(Error::Unavailable("RDMA workers are stopped"));
        }
        let count = pool.workers.len();
        let lane = lanes
            .iter()
            .filter(|lane| std::ptr::eq(self, lane.workers.as_ref()))
            .filter_map(|lane| {
                lane.worker_id
                    .get()
                    .and_then(|&id| pool.workers.get(id).map(|(worker, _)| (lane, id, worker)))
            })
            .filter(|(_, _, worker)| !worker.stopping.load(Ordering::Acquire))
            .min_by_key(|(_, id, worker)| {
                (
                    worker.inflight_slices.load(Ordering::Acquire),
                    (id + count - pool.next_worker % count) % count,
                )
            })
            .map(|(lane, id, worker)| (Arc::clone(lane), id, Arc::clone(worker)))
            .ok_or(Error::Unavailable("RDMA context has no live CQ lane"))?;
        let lease = SliceLease::new(lane.2)?;
        pool.next_worker = (lane.1 + 1) % count;
        Ok((lane.0, lease))
    }

    pub(super) fn reserve(&self, lane: &CqLane) -> Result<SliceLease> {
        let pool = self
            .pool
            .lock()
            .map_err(|_| Error::Internal("RDMA workers lock poisoned"))?;
        if pool.stopped || !std::ptr::eq(self, lane.workers.as_ref()) {
            return Err(Error::Unavailable("RDMA workers are stopped or foreign"));
        }
        let worker = lane
            .worker_id
            .get()
            .and_then(|&id| pool.workers.get(id))
            .map(|(worker, _)| worker)
            .ok_or(Error::Internal("RDMA CQ lane has no worker"))?;
        if worker.stopping.load(Ordering::Acquire) {
            return Err(Error::Unavailable("RDMA progress worker stopped"));
        }
        SliceLease::new(Arc::clone(worker))
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
        self.monitor.close(deadline)?;
        failure.map_or(Ok(()), Err)
    }
}

/// One admitted wire slice, charged through grant drain or native quarantine.
pub(super) struct SliceLease {
    worker: Arc<WorkerContext>,
}

impl SliceLease {
    fn new(worker: Arc<WorkerContext>) -> Result<Self> {
        let mut load = worker.inflight_slices.load(Ordering::Acquire);
        loop {
            let next = load.checked_add(1).ok_or(Error::ResourceExhausted)?;
            match worker.inflight_slices.compare_exchange_weak(
                load,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => load = current,
            }
        }
        Ok(Self { worker })
    }
}

impl Drop for SliceLease {
    fn drop(&mut self) {
        let previous = self.worker.inflight_slices.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "RDMA slice load returned twice");
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

    fn run(self: Arc<Self>, started: std::sync::mpsc::SyncSender<Result<()>>) {
        let mut exit = WorkerExit {
            context: Arc::clone(&self),
            lanes: Vec::new(),
        };
        let bound = self.affinity.bind();
        #[cfg(test)]
        if bound.is_ok() {
            tests::observe_affinity(&self.affinity);
        }
        let success = bound.is_ok();
        if started.send(bound).is_err() || !success {
            return;
        }
        let mut cursor = 0usize;
        loop {
            exit.lanes
                .append(&mut self.pending.lock().unwrap_or_else(|p| p.into_inner()));
            let stopping = self.stopping.load(Ordering::Acquire);
            if stopping && exit.lanes.is_empty() {
                return;
            }
            let count = exit.lanes.len();
            let mut wait: Option<Instant> = None;
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
                    Schedule::Again => wait = Some(Instant::now()),
                    Schedule::Wait(Some(deadline)) => {
                        wait = Some(wait.map_or(deadline, |current| current.min(deadline)))
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
                Some(deadline) => {
                    if let Some(duration) = deadline.checked_duration_since(Instant::now()) {
                        thread::park_timeout(duration);
                    }
                }
                None => thread::park(),
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/workers.rs"]
mod tests;
