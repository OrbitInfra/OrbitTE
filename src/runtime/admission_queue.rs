use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

use crate::engine::remaining;
use crate::{BatchId, Error, Result, RuntimeQueueConfig};

use super::qos_contract::QosReservation;
use super::receiver_credit::CreditReservation;

/// Reservations for whole submissions, not a native WR/byte counter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeQueueStats {
    /// Includes groups waiting for earlier submissions in the same Batch.
    pub queued_owners: usize,
    pub queued_bytes: usize,
    /// Includes window reservations waiting for their Batch worker to run.
    pub dispatching_owners: usize,
    pub dispatching_bytes: usize,
}

type QueueOwnerId = u64;
type DispatchKey = (Option<Instant>, QueueOwnerId);

struct QueueOwner {
    bytes: usize,
    deadline: Instant,
    batch: Option<BatchId>,
    key: DispatchKey,
    dispatching: bool,
    credit: Option<Arc<CreditReservation>>,
    qos: Option<Arc<QosReservation>>,
    rejected: bool,
}

#[derive(Default)]
struct QueueState {
    next_id: QueueOwnerId,
    owners: HashMap<QueueOwnerId, QueueOwner>,
    batches: HashMap<BatchId, VecDeque<QueueOwnerId>>,
    ready: BTreeSet<DispatchKey>,
    outstanding_bytes: usize,
    dispatching_owners: usize,
    dispatching_bytes: usize,
    credit_wakeup: Option<Instant>,
}

/// Runtime admission owns only scheduling metadata. QueuePermit follows the
/// actual PreparedSubmission, including quarantine after unproven DMA drain.
pub(crate) struct LocalTransferAdmissionQueue {
    config: RuntimeQueueConfig,
    state: Mutex<QueueState>,
    changed: Condvar,
    progress: std::sync::OnceLock<std::sync::Weak<crate::runtime::progress_worker::Wakes>>,
}

impl LocalTransferAdmissionQueue {
    pub(crate) fn new(config: RuntimeQueueConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: Mutex::default(),
            changed: Condvar::new(),
            progress: Default::default(),
        })
    }

    pub(crate) fn attach(
        &self,
        wakes: std::sync::Weak<crate::runtime::progress_worker::Wakes>,
    ) -> Result<()> {
        self.progress
            .set(wakes)
            .map_err(|_| Error::Internal("progress worker already attached"))
    }
    pub(crate) fn active(&self) -> bool {
        !self.lock().owners.is_empty()
    }
    fn signal(&self) {
        if let Some(wakes) = self.progress.get().and_then(std::sync::Weak::upgrade) {
            wakes.queue();
        }
    }
    pub(crate) fn refill(&self) {
        let mut state = self.lock();
        // Capacity, credit and QoS changes refill synchronously at their owner.
        // Only expiry can make another head eligible without a release. In
        // particular, do not reject a delayed head against a transient quota.
        if state.credit_wakeup.is_some_and(|at| Instant::now() >= at)
            && state.refill_dispatch_window(self.config)
        {
            self.changed.notify_all();
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub(crate) fn try_admit(
        self: &Arc<Self>,
        bytes: usize,
        deadline: Instant,
        batch: Option<BatchId>,
        credit: Option<Arc<CreditReservation>>,
        qos: Option<Arc<QosReservation>>,
    ) -> Result<QueuePermit> {
        if bytes > self.config.max_dispatch_bytes {
            return Err(Error::ResourceExhausted);
        }
        let mut state = self.lock();
        let id = state
            .next_id
            .checked_add(1)
            .ok_or(Error::ResourceExhausted)?;
        let outstanding_bytes = state
            .outstanding_bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceExhausted)?;
        let key = (self.config.deadline_aware.then_some(deadline), id);
        let ready = match batch {
            Some(batch) => {
                let owners = state.batches.entry(batch).or_default();
                owners.push_back(id);
                owners.len() == 1
            }
            None => true,
        };
        state.owners.insert(
            id,
            QueueOwner {
                bytes,
                deadline,
                batch,
                key,
                dispatching: false,
                credit,
                qos,
                rejected: false,
            },
        );
        state.next_id = id;
        state.outstanding_bytes = outstanding_bytes;
        if ready {
            state.ready.insert(key);
        }
        if state.refill_dispatch_window(self.config) {
            self.changed.notify_all();
        }
        if state.owners[&id].rejected {
            state.release(id);
            if state.refill_dispatch_window(self.config) {
                self.changed.notify_all();
            }
            return Err(Error::ResourceExhausted);
        }
        drop(state);
        self.signal();
        Ok(QueuePermit {
            queue: Arc::clone(self),
            id,
        })
    }

    pub(crate) fn stats(&self) -> RuntimeQueueStats {
        let state = self.lock();
        RuntimeQueueStats {
            queued_owners: state.owners.len() - state.dispatching_owners,
            queued_bytes: state.outstanding_bytes - state.dispatching_bytes,
            dispatching_owners: state.dispatching_owners,
            dispatching_bytes: state.dispatching_bytes,
        }
    }

    pub(crate) fn wake(&self) {
        // Cancellation publishes under BatchProgress, releases that lock, then
        // acquires this lock. A waiter cannot miss cancellation while parking.
        let mut state = self.lock();
        state.refill_dispatch_window(self.config);
        self.changed.notify_all();
        drop(state);
        self.signal();
    }
}

impl QueueState {
    fn refill_dispatch_window(&mut self, config: RuntimeQueueConfig) -> bool {
        self.credit_wakeup = None;
        let mut changed = false;
        let mut previous = None;
        while self.dispatching_owners < config.max_dispatch_owners {
            let key = match previous {
                Some(key) => self
                    .ready
                    .range((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded))
                    .next(),
                None => self.ready.first(),
            }
            .copied();
            let Some(key) = key else { break };
            previous = Some(key);
            let owner = self.owners.get_mut(&key.1).expect("ready owner exists");
            if owner.bytes > config.max_dispatch_bytes - self.dispatching_bytes {
                if let Some(qos) = &owner.qos {
                    match qos.availability() {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(_) => {
                            owner.rejected = true;
                            self.ready.remove(&key);
                            changed = true;
                            continue;
                        }
                    }
                }
                if let Some(credit) = &owner.credit {
                    let (eligible, expires) = credit.availability().unwrap_or((false, None));
                    if !eligible {
                        continue;
                    }
                    // Expiry removes this byte-blocking head's eligibility.
                    self.credit_wakeup = expires;
                }
                break;
            }
            let reserve_credit = || {
                owner
                    .credit
                    .as_ref()
                    .is_none_or(|credit| credit.try_reserve().unwrap_or(false))
            };
            let admitted = match &owner.qos {
                Some(qos) => qos.try_reserve(reserve_credit),
                None => Ok(reserve_credit()),
            };
            match admitted {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_) => {
                    owner.rejected = true;
                    self.ready.remove(&key);
                    changed = true;
                    continue;
                }
            }
            self.ready.remove(&key);
            owner.dispatching = true;
            self.dispatching_owners += 1;
            self.dispatching_bytes += owner.bytes;
            changed = true;
        }
        changed
    }

    fn release(&mut self, id: QueueOwnerId) {
        let Some(owner) = self.owners.remove(&id) else {
            return;
        };
        self.outstanding_bytes -= owner.bytes;
        if owner.dispatching {
            self.dispatching_owners -= 1;
            self.dispatching_bytes -= owner.bytes;
        } else {
            self.ready.remove(&owner.key);
        }
        if let Some(batch) = owner.batch {
            let chain = self.batches.get_mut(&batch).expect("batch chain exists");
            let was_head = chain.front() == Some(&id);
            if was_head {
                chain.pop_front();
            } else {
                // Failed enqueue/unwinding can release a not-yet-active group.
                chain.retain(|queued| *queued != id);
            }
            if chain.is_empty() {
                self.batches.remove(&batch);
            } else if was_head {
                let next = self.owners.get(&chain[0]).expect("successor exists");
                self.ready.insert(next.key);
            }
        }
    }
}

pub(crate) struct QueuePermit {
    queue: Arc<LocalTransferAdmissionQueue>,
    id: QueueOwnerId,
}

impl QueuePermit {
    /// The predicate only reads the first public task's cancellation flag. Lock
    /// order is queue -> QoS -> credit and queue -> BatchProgress. Cancellation
    /// releases BatchProgress before wake. Credit sync releases all locks before I/O;
    /// no backing drop or progress mutation runs while the lock is held.
    pub(crate) fn wait(&self, cancelled: impl Fn() -> bool + Sync) -> Result<()> {
        let mut state = self.queue.lock();
        loop {
            if state.credit_wakeup.is_some_and(|at| Instant::now() >= at)
                && state.refill_dispatch_window(self.queue.config)
            {
                self.queue.changed.notify_all();
            }
            if cancelled() {
                return Err(Error::Cancelled);
            }
            let owner = state
                .owners
                .get(&self.id)
                .ok_or(Error::Internal("runtime queue owner missing"))?;
            let mut timeout = remaining(owner.deadline)?;
            if owner.rejected {
                return Err(Error::ResourceExhausted);
            }
            if let Some(wakeup) = state.credit_wakeup {
                timeout = timeout.min(wakeup.saturating_duration_since(Instant::now()));
            }
            if let Some(credit) = &owner.credit {
                credit.check_active()?;
            }
            if owner.dispatching {
                return Ok(());
            }
            if let Some(credit) = &owner.credit {
                if credit.needs_sync()?
                    && owner
                        .qos
                        .as_ref()
                        .map(|qos| qos.availability())
                        .transpose()?
                        .unwrap_or(true)
                {
                    let credit = Arc::clone(credit);
                    let deadline = owner.deadline;
                    drop(state);
                    credit.synchronize(deadline, &cancelled)?;
                    state = self.queue.lock();
                    continue;
                }
            }
            state = self
                .queue
                .changed
                .wait_timeout(state, timeout)
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
    }
}

impl Drop for QueuePermit {
    fn drop(&mut self) {
        let mut state = self.queue.lock();
        state.release(self.id);
        if state.refill_dispatch_window(self.queue.config) {
            self.queue.changed.notify_all();
        }
        drop(state);
        self.queue.signal();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/admission_queue.rs"]
mod tests;
