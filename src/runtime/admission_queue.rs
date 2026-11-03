use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Instant;

use crate::engine::remaining;
use crate::{BatchId, Error, Result, RuntimeQueueConfig, Transport};

use super::qos_contract::QosReservation;
use super::receiver_credit::CreditReservation;

/// Logical admission and dispatch reservations for physical transfer owners.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeQueueStats {
    /// Includes physical owners waiting for earlier submissions in the same Batch.
    pub queued_owners: usize,
    pub queued_bytes: usize,
    /// Includes window reservations waiting for their Batch worker to run.
    pub dispatching_owners: usize,
    pub dispatching_bytes: usize,
    /// Physical owners dropped before payload dispatch.
    pub deadline_infeasible_drops: u64,
}

type QueueOwnerId = u64;
type DispatchKey = (Option<Instant>, QueueOwnerId);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueueOwnerKind {
    User,
    StagingInternal,
}

pub(crate) struct QueueInput {
    pub(crate) bytes: usize,
    pub(crate) length: usize,
    pub(crate) deadline: Instant,
    pub(crate) scheduling_deadline: Option<Instant>,
    pub(crate) kind: QueueOwnerKind,
    // Only transfers whose duration the selected provider estimates participate.
    pub(crate) rate: Option<(Transport, u64)>,
    pub(crate) credit: Option<Arc<CreditReservation>>,
    pub(crate) qos: Option<Arc<QosReservation>>,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
enum Chain {
    Batch(BatchId),
    Call(u64),
}

const LIVE: u8 = 0;
const DROPPED: u8 = 1;
const CANCELLED: u8 = 2;

#[derive(Clone, Debug)]
pub(crate) struct QueueTicket {
    id: QueueOwnerId,
    outcome: Arc<AtomicU8>,
}
impl QueueTicket {
    pub(crate) fn terminal(&self) -> Result<()> {
        match self.outcome.load(Ordering::Acquire) {
            DROPPED => Err(Error::DeadlineInfeasible),
            CANCELLED => Err(Error::Cancelled),
            _ => Ok(()),
        }
    }
}
struct QueueOwner {
    length: usize,
    bytes: usize,
    deadline: Instant,
    chain: Chain,
    group: u64,
    kind: QueueOwnerKind,
    rate: Option<(Transport, u64)>,
    ticket: QueueTicket,
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
    batches: HashMap<Chain, VecDeque<QueueOwnerId>>,
    ready: BTreeSet<DispatchKey>,
    outstanding_bytes: usize,
    dispatching_owners: usize,
    dispatching_bytes: usize,
    credit_wakeup: Option<Instant>,
    user_bytes: usize,
    user_owners: usize,
    eligible_bytes: [usize; Transport::Unspec as usize + 1],
    deadline_drops: u64,
}

/// Runtime admission owns only scheduling metadata. QueuePermit follows the
/// actual PreparedSubmission, including quarantine after unproven DMA drain.
type Bandwidth = dyn Fn(Transport, u64) -> f64 + Send + Sync;
pub(crate) struct LocalTransferAdmissionQueue {
    bandwidth: OnceLock<Box<Bandwidth>>,
    config: RuntimeQueueConfig,
    state: Mutex<QueueState>,
    changed: Condvar,
    progress: std::sync::OnceLock<std::sync::Weak<crate::runtime::progress_worker::Wakes>>,
}

impl LocalTransferAdmissionQueue {
    pub(crate) fn new(config: RuntimeQueueConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            bandwidth: OnceLock::new(),
            state: Mutex::default(),
            changed: Condvar::new(),
            progress: Default::default(),
        })
    }

    #[cfg(any(feature = "rdma", test))]
    pub(crate) fn set_bandwidth(&self, provider: Box<Bandwidth>) -> Result<()> {
        self.bandwidth
            .set(provider)
            .map_err(|_| Error::Internal("queue bandwidth provider already installed"))
    }

    fn dispatch(&self, state: &mut QueueState) -> bool {
        state.refill_dispatch_window(
            self.config,
            self.bandwidth.get().map(Box::as_ref),
            Instant::now(),
        )
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
        if state.credit_wakeup.is_some_and(|at| Instant::now() >= at) && self.dispatch(&mut state) {
            self.changed.notify_all();
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    #[cfg(test)]
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        bytes: usize,
        deadline: Instant,
        batch: Option<BatchId>,
        credit: Option<Arc<CreditReservation>>,
        qos: Option<Arc<QosReservation>>,
    ) -> Result<QueuePermit> {
        self.admit(
            vec![QueueInput {
                bytes,
                length: bytes,
                deadline,
                scheduling_deadline: Some(deadline),
                kind: QueueOwnerKind::User,
                rate: None,
                credit,
                qos,
            }],
            batch,
        )?
        .pop()
        .ok_or(Error::Internal("single queue permit missing"))
    }

    // Validate the entire submit before publishing IDs or taking any window.
    pub(crate) fn admit(
        self: &Arc<Self>,
        inputs: Vec<QueueInput>,
        batch: Option<BatchId>,
    ) -> Result<Vec<QueuePermit>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let mut state = self.lock();
        let mut bytes = 0usize;
        let mut user_bytes = state.user_bytes;
        let mut user_owners = state.user_owners;
        for input in &inputs {
            if input.bytes == 0 || input.bytes > self.config.max_dispatch_bytes {
                return Err(Error::ResourceExhausted);
            }
            bytes = bytes
                .checked_add(input.bytes)
                .ok_or(Error::ResourceExhausted)?;
            if input.kind == QueueOwnerKind::User {
                user_bytes = user_bytes
                    .checked_add(input.bytes)
                    .ok_or(Error::ResourceExhausted)?;
                user_owners = user_owners.checked_add(1).ok_or(Error::ResourceExhausted)?;
            }
        }
        let config = self.config;
        if inputs.len()
            > config
                .max_outstanding_owners
                .saturating_sub(state.owners.len())
            || bytes
                > config
                    .max_outstanding_bytes
                    .saturating_sub(state.outstanding_bytes)
            || user_owners
                > config
                    .max_outstanding_owners
                    .saturating_sub(config.staging_owner_reserve)
            || user_bytes
                > config
                    .max_outstanding_bytes
                    .saturating_sub(config.staging_byte_reserve)
        {
            return Err(Error::ResourceExhausted);
        }
        let mut permits = Vec::new();
        permits
            .try_reserve_exact(inputs.len())
            .map_err(|_| Error::ResourceExhausted)?;
        let last = state
            .next_id
            .checked_add(inputs.len() as u64)
            .ok_or(Error::ResourceExhausted)?;
        let group = last;
        let chain = batch.map_or(Chain::Call(group), Chain::Batch);
        state
            .owners
            .try_reserve(inputs.len())
            .map_err(|_| Error::ResourceExhausted)?;
        state.user_bytes = user_bytes;
        state.user_owners = user_owners;
        state.outstanding_bytes += bytes;
        for input in inputs {
            state.next_id += 1;
            let id = state.next_id;
            let key = (
                if config.deadline_aware {
                    input.scheduling_deadline
                } else {
                    None
                },
                id,
            );
            let ticket = QueueTicket {
                id,
                outcome: Arc::new(AtomicU8::new(LIVE)),
            };
            state.batches.entry(chain).or_default().push_back(id);
            state.owners.insert(
                id,
                QueueOwner {
                    length: input.length,
                    bytes: input.bytes,
                    deadline: input.deadline,
                    chain,
                    group,
                    kind: input.kind,
                    rate: input.rate,
                    ticket: ticket.clone(),
                    key,
                    dispatching: false,
                    rejected: false,
                    credit: input.credit,
                    qos: input.qos,
                },
            );
            permits.push(QueuePermit {
                queue: Arc::clone(self),
                id,
                ticket,
            });
        }
        state.activate(chain);
        let changed = self.dispatch(&mut state);
        let reject = permits
            .iter()
            .any(|p| state.owners.get(&p.id).is_some_and(|o| o.rejected));
        if reject {
            for p in &permits {
                state.release(p.id);
            }
            self.dispatch(&mut state);
        }
        drop(state);
        if changed || reject {
            self.changed.notify_all();
        }
        self.signal();
        if reject {
            return Err(Error::ResourceExhausted);
        }
        Ok(permits)
    }

    pub(crate) fn cancel(&self, ticket: &QueueTicket) -> bool {
        let mut state = self.lock();
        if state.owners.get(&ticket.id).is_some_and(|o| !o.dispatching) {
            ticket.outcome.store(CANCELLED, Ordering::Release);
            state.release(ticket.id);
            self.dispatch(&mut state);
            self.changed.notify_all();
            drop(state);
            self.signal();
            true
        } else {
            false
        }
    }

    pub(crate) fn stats(&self) -> RuntimeQueueStats {
        let state = self.lock();
        RuntimeQueueStats {
            queued_owners: state.owners.len() - state.dispatching_owners,
            queued_bytes: state.outstanding_bytes - state.dispatching_bytes,
            dispatching_owners: state.dispatching_owners,
            dispatching_bytes: state.dispatching_bytes,
            deadline_infeasible_drops: state.deadline_drops,
        }
    }

    pub(crate) fn wake(&self) {
        // Cancellation publishes under BatchProgress, releases that lock, then
        // acquires this lock. A waiter cannot miss cancellation while parking.
        let mut state = self.lock();
        self.dispatch(&mut state);
        self.changed.notify_all();
        drop(state);
        self.signal();
    }
}

impl QueueState {
    // Within a submit, release the next FIFO head as its predecessor enters
    // the window. A later submit waits for the prior submit to drain.
    fn activate(&mut self, chain: Chain) {
        let Some(ids) = self.batches.get(&chain) else {
            return;
        };
        let Some(first) = ids.front() else { return };
        let group = self.owners[first].group;
        for id in ids {
            let o = &self.owners[id];
            if o.group != group {
                break;
            }
            if !o.dispatching {
                if !o.rejected {
                    self.ready.insert(o.key);
                }
                break;
            }
        }
    }

    fn refill_dispatch_window(
        &mut self,
        config: RuntimeQueueConfig,
        bandwidth: Option<&Bandwidth>,
        now: Instant,
    ) -> bool {
        self.credit_wakeup = None;
        let mut changed = false;
        // Undeadlined entries follow timed EDF entries. Promotion moves only
        // future critical slack ahead of expired deadlines, preserving EDF ties.
        let mut waiting = Vec::new();
        let mut rates = HashMap::new();
        while self.dispatching_owners < config.max_dispatch_owners {
            let Some(key) = self.next_ready(config, now) else {
                break;
            };
            self.ready.remove(&key);
            let Some(owner) = self.owners.get_mut(&key.1) else {
                continue;
            };
            if config.deadline_aware && config.mlu_local_threshold > 0.0 {
                if let (Some(provider), Some((transport, mask)), Some(dl)) =
                    (bandwidth, owner.rate, key.0)
                {
                    let rate = *rates
                        .entry((transport as u8, mask))
                        .or_insert_with(|| provider(transport, mask));
                    if crate::runtime::deadline_mlu::deadline_mlu(
                        self.eligible_bytes[transport as usize] as u64,
                        owner.length,
                        Some(dl),
                        now,
                        rate,
                    ) >= config.mlu_local_threshold
                    {
                        owner.ticket.outcome.store(DROPPED, Ordering::Release);
                        self.deadline_drops = self.deadline_drops.saturating_add(1);
                        self.release(key.1);
                        changed = true;
                        continue;
                    }
                }
            }
            if owner.bytes > config.max_dispatch_bytes - self.dispatching_bytes {
                if let Some(qos) = &owner.qos {
                    match qos.availability() {
                        Ok(true) => {}
                        Ok(false) => {
                            waiting.push(key);
                            continue;
                        }
                        Err(_) => {
                            owner.rejected = true;
                            changed = true;
                            continue;
                        }
                    }
                }
                if let Some(credit) = &owner.credit {
                    let (eligible, expires) = credit.availability().unwrap_or((false, None));
                    if !eligible {
                        waiting.push(key);
                        continue;
                    }
                    // Expiry removes this byte-blocking head's eligibility.
                    self.credit_wakeup = expires;
                }
                waiting.push(key);
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
                Ok(false) => {
                    waiting.push(key);
                    continue;
                }
                Err(_) => {
                    owner.rejected = true;
                    changed = true;
                    continue;
                }
            }
            owner.dispatching = true;
            self.dispatching_owners += 1;
            self.dispatching_bytes += owner.bytes;
            if let Some((transport, _)) = owner.rate {
                self.eligible_bytes[transport as usize] += owner.length;
            }
            let chain = owner.chain;
            self.activate(chain);
            changed = true;
        }
        self.ready.extend(waiting);
        changed
    }

    fn next_ready(&self, config: RuntimeQueueConfig, now: Instant) -> Option<DispatchKey> {
        use std::ops::Bound::{Excluded, Unbounded};
        if config.deadline_aware {
            if config.promotion_slack_ns != 0 {
                if let Some(&key @ (Some(dl), _)) = self
                    .ready
                    .range((Excluded((Some(now), u64::MAX)), Unbounded))
                    .next()
                {
                    if dl.duration_since(now).as_nanos() < u128::from(config.promotion_slack_ns) {
                        return Some(key);
                    }
                }
            }
            if let Some(&key) = self
                .ready
                .range((Excluded((None, u64::MAX)), Unbounded))
                .next()
            {
                return Some(key);
            }
        }
        self.ready.first().copied()
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
        if owner.kind == QueueOwnerKind::User {
            self.user_bytes -= owner.bytes;
            self.user_owners -= 1;
        }
        if let Some((transport, _)) = owner.rate {
            if owner.dispatching {
                self.eligible_bytes[transport as usize] -= owner.length;
            }
        }
        if let Some(ids) = self.batches.get_mut(&owner.chain) {
            if ids.front() == Some(&id) {
                ids.pop_front();
            } else {
                ids.retain(|&old| old != id);
            }
            if ids.is_empty() {
                self.batches.remove(&owner.chain);
            } else {
                self.activate(owner.chain);
            }
        }
    }
}

pub(crate) struct QueuePermit {
    queue: Arc<LocalTransferAdmissionQueue>,
    id: QueueOwnerId,
    pub(crate) ticket: QueueTicket,
}

impl QueuePermit {
    pub(crate) fn reroute(&self, rate: Option<(Transport, u64)>) {
        let mut state = self.queue.lock();
        let Some(owner) = state.owners.get_mut(&self.id) else {
            return;
        };
        if !owner.dispatching || owner.rate == rate {
            return;
        }
        let previous = std::mem::replace(&mut owner.rate, rate);
        let length = owner.length;
        if let Some((t, _)) = previous {
            state.eligible_bytes[t as usize] -= length;
        }
        if let Some((t, _)) = rate {
            state.eligible_bytes[t as usize] += length;
        }
    }

    /// The predicate only reads the first public task's cancellation flag. Lock
    /// order is queue -> QoS -> credit and queue -> BatchProgress. Cancellation
    /// releases BatchProgress before wake. Credit sync releases all locks before I/O;
    /// no backing drop or progress mutation runs while the lock is held.
    pub(crate) fn wait(&self, cancelled: impl Fn() -> bool + Sync) -> Result<()> {
        let mut state = self.queue.lock();
        loop {
            if state.credit_wakeup.is_some_and(|at| Instant::now() >= at)
                && self.queue.dispatch(&mut state)
            {
                self.queue.changed.notify_all();
            }
            self.ticket.terminal()?;
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
        if self.queue.dispatch(&mut state) {
            self.queue.changed.notify_all();
        }
        drop(state);
        self.queue.signal();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/admission_queue.rs"]
mod tests;
