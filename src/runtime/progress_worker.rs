//! A TENT wake drives one batch turn. Payload I/O stays on the admitted batch worker.
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::batch::AllocationBudget;
use crate::runtime::admission_queue::LocalTransferAdmissionQueue;
use crate::{BatchId, Error, Result};

/// Delay before the next explicit poll. Short transfers stay hot.
pub fn next_poll_delay(poll_count: u64) -> Duration {
    if poll_count < 128 {
        Duration::ZERO
    } else {
        Duration::from_micros((1u64 << (poll_count - 128).min(8)).min(200))
    }
}

/// Yield during the hot phase and sleep for the bounded delay afterward.
pub fn wait_before_next_poll(poll_count: u64) {
    wait_for_poll(poll_count, Duration::MAX);
}

pub(crate) fn wait_for_poll(count: u64, remaining: Duration) {
    let delay = next_poll_delay(count).min(remaining);
    if delay.is_zero() {
        thread::yield_now();
    } else {
        thread::park_timeout(delay);
    }
}

#[derive(Default)]
struct Pending {
    batches: VecDeque<BatchId>,
    queue: bool,
    stopped: bool,
}

pub(crate) struct Wakes {
    pending: Mutex<Pending>,
    ready: Condvar,
    capacity: usize,
    #[cfg(test)]
    ticks: std::sync::atomic::AtomicUsize,
}

impl Wakes {
    pub(crate) fn batch(&self, id: BatchId) {
        let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if !p.stopped && p.batches.len() < self.capacity && !p.batches.contains(&id) {
            p.batches.push_back(id);
            self.ready.notify_one();
        }
    }

    pub(crate) fn discard(&self, id: BatchId) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .batches
            .retain(|&queued| queued != id);
    }

    pub(crate) fn queue(&self) {
        let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if !p.stopped && !p.queue {
            p.queue = true;
            self.ready.notify_one();
        }
    }

    fn run(
        &self,
        budget: &AllocationBudget,
        queue: Option<Arc<LocalTransferAdmissionQueue>>,
        interval: Duration,
    ) {
        loop {
            // Queue owners release their lock before signaling us. Never query the
            // queue, a batch, or a native owner under the wake-set lock.
            let active = !interval.is_zero() && queue.as_ref().is_some_and(|q| q.active());
            let mut p = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            let idle = |p: &mut Pending| !p.stopped && !p.queue && p.batches.is_empty();
            let mut tick = false;
            if active {
                let (state, timeout) = self
                    .ready
                    .wait_timeout_while(p, interval, idle)
                    .unwrap_or_else(|e| e.into_inner());
                p = state;
                tick = timeout.timed_out();
            } else {
                p = self
                    .ready
                    .wait_while(p, idle)
                    .unwrap_or_else(|e| e.into_inner());
            }
            if p.stopped {
                return;
            }
            let refill = std::mem::take(&mut p.queue) || tick;
            let batch = p.batches.pop_front();
            drop(p);
            #[cfg(test)]
            if tick {
                self.ticks
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            }
            if refill {
                if let Some(q) = &queue {
                    q.refill();
                }
            }
            if let Some(batch) = batch {
                if let Some(owner) = budget.lookup(batch) {
                    owner.step();
                }
            }
            budget.sweep_deferred();
        }
    }
}

pub(crate) struct ProgressWorker {
    pub(crate) wakes: Arc<Wakes>,
    thread: Option<JoinHandle<()>>,
}

impl ProgressWorker {
    pub(crate) fn start(
        budget: Arc<AllocationBudget>,
        queue: Option<Arc<LocalTransferAdmissionQueue>>,
        capacity: usize,
        interval: Duration,
    ) -> Result<Self> {
        let mut pending = Pending::default();
        pending
            .batches
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let wakes = Arc::new(Wakes {
            pending: Mutex::new(pending),
            ready: Condvar::new(),
            #[cfg(test)]
            ticks: Default::default(),
            capacity,
        });
        if let Some(q) = &queue {
            q.attach(Arc::downgrade(&wakes))?;
        }
        let runner = Arc::clone(&wakes);
        let thread = thread::Builder::new()
            .name("orbite-progress".into())
            .spawn(move || runner.run(&budget, queue, interval))?;
        Ok(Self {
            wakes,
            thread: Some(thread),
        })
    }

    pub(crate) fn stop(&mut self) {
        let Some(thread) = self.thread.take() else {
            return;
        };
        {
            let mut p = self.wakes.pending.lock().unwrap_or_else(|e| e.into_inner());
            p.stopped = true;
            p.batches.clear();
            p.queue = false;
        }
        self.wakes.ready.notify_all();
        let _ = thread.join();
    }
}

impl Drop for ProgressWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/progress_worker.rs"]
mod tests;
