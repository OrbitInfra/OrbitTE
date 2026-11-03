mod issuer;
pub(crate) mod protocol;
mod sync;

pub(crate) use issuer::ReceiverCreditIssuer;
pub use issuer::{ReceiverCreditConfig, ReceiverCreditIssuerStats};
#[cfg(feature = "cuda")]
pub(crate) use issuer::{ReceiverResourceLease, StagingCreditReservation};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use crate::engine::remaining;
use crate::{Error, RemoteSegment, Result, SourceEpoch};

use super::admission_queue::LocalTransferAdmissionQueue;

/// Resource tags in TENT's receiver credit schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum CreditResource {
    DataBytes = 1,
    RequestSlots = 2,
    /// Automatic remote-Pin source leases; manual updates are not supported.
    StagingSlots = 3,
    /// Automatic remote-Pin sessions, retained through all native cleanup.
    ConsumerSlots = 4,
}

#[derive(Clone, Copy, Debug)]
pub struct CreditAmount {
    pub resource: CreditResource,
    pub grant_total: u64,
}

/// A cooperative receiver's application-delivered cumulative update.
/// Automatic bindings synchronize equivalent totals over OrbitTE's control wire;
/// neither API nor encoding is TENT wire-compatible.
#[derive(Clone, Debug)]
pub struct ReceiverCreditUpdateV1 {
    pub schema_version: u16,
    pub flags: u16,
    /// The authoritative RemoteSegment source epoch is the receiver session.
    pub receiver_session_id: SourceEpoch,
    pub qos_class: u32,
    pub epoch: u64,
    pub sequence: u64,
    /// Zero has no expiry. Freshness gates new reservations, not reserved work.
    pub freshness_ttl_ms: u32,
    /// Partial cumulative totals. Omitted resources retain their previous total.
    pub grants: Vec<CreditAmount>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreditUpdateDisposition {
    Applied,
    DuplicateOrOld,
    SequenceGap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiverCreditStats {
    pub automatic: bool,
    /// Successful data groups whose actual owners have been released.
    pub completed_bytes: u64,
    pub completed_requests: u64,
    pub epoch: u64,
    pub qos_class: u32,
    pub sequence: u64,
    pub open: bool,
    pub fresh: bool,
    pub active_owners: usize,
    pub reserved_owners: usize,
    /// Zero while fenced or stale, even if the cumulative balance is positive.
    pub available_bytes: u64,
    pub available_requests: u64,
    /// Includes reservations not yet handed to a transport.
    pub consumed_bytes: u64,
    pub consumed_requests: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CreditKey {
    endpoint: SocketAddr,
    receiver_session: SourceEpoch,
}

impl From<&RemoteSegment> for CreditKey {
    fn from(peer: &RemoteSegment) -> Self {
        Self {
            endpoint: peer.endpoint(),
            receiver_session: peer.source_epoch(),
        }
    }
}

struct Entry {
    epoch: u64,
    qos_class: u32,
    sequence: u64,
    open: bool,
    expires: Option<Instant>,
    grants: [u64; 2],
    consumed: [u64; 2],
    committed: [u64; 2],
    completed: [u64; 2],
    automatic: Option<sync::AutomaticCredit>,
    active: usize,
    reserved: usize,
}

impl Entry {
    fn new(epoch: u64, qos_class: u32) -> Self {
        Self {
            epoch,
            qos_class,
            sequence: 0,
            open: true,
            expires: None,
            grants: [0; 2],
            consumed: [0; 2],
            committed: [0; 2],
            completed: [0; 2],
            automatic: None,
            active: 0,
            reserved: 0,
        }
    }

    fn fresh(&self) -> bool {
        self.sequence != 0 && self.expires.is_none_or(|expires| Instant::now() < expires)
    }

    fn fits(&self, charge: [u64; 2]) -> bool {
        self.fresh()
            && charge
                .iter()
                .enumerate()
                .all(|(i, amount)| *amount <= self.grants[i] - self.consumed[i])
    }
}

#[derive(Clone, Copy)]
struct CreditOwner {
    key: CreditKey,
    epoch: u64,
    charge: [u64; 2],
    reserved: bool,
    committed: bool,
    completed: bool,
}

#[derive(Default)]
struct LedgerState {
    entries: HashMap<CreditKey, Entry>,
    owners: HashMap<u64, CreditOwner>,
    next_owner: u64,
}

impl LedgerState {
    fn owner(&self, id: u64) -> Result<(CreditOwner, &Entry)> {
        let owner = *self
            .owners
            .get(&id)
            .ok_or(Error::Internal("credit owner missing"))?;
        let entry = self
            .entries
            .get(&owner.key)
            .ok_or(Error::Internal("credit session missing"))?;
        if entry.epoch != owner.epoch {
            return Err(Error::EpochMismatch);
        }
        if !entry.open {
            return Err(Error::Cancelled);
        }
        Ok((owner, entry))
    }

    fn try_reserve(&mut self, id: u64) -> Result<bool> {
        let (owner, entry) = self.owner(id)?;
        if owner.reserved {
            return Ok(true);
        }
        if !entry.fits(owner.charge) {
            return Ok(false);
        }
        let entry = self
            .entries
            .get_mut(&owner.key)
            .ok_or(Error::Internal("credit session missing"))?;
        for (i, amount) in owner.charge.iter().enumerate() {
            entry.consumed[i] += amount;
        }
        entry.reserved += 1;
        self.owners
            .get_mut(&id)
            .ok_or(Error::Internal("credit owner missing"))?
            .reserved = true;
        Ok(true)
    }
}

pub(crate) struct SenderCreditLedger {
    max_entries: usize,
    state: Mutex<LedgerState>,
    changed: Condvar,
    queue: Weak<LocalTransferAdmissionQueue>,
}

impl SenderCreditLedger {
    pub(crate) fn new(
        max_entries: usize,
        queue: Option<&Arc<LocalTransferAdmissionQueue>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            max_entries,
            state: Mutex::default(),
            changed: Condvar::new(),
            queue: queue.map(Arc::downgrade).unwrap_or_default(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, LedgerState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub(crate) fn activate(&self, peer: &RemoteSegment, epoch: u64, qos_class: u32) -> Result<()> {
        if epoch == 0 {
            return Err(Error::InvalidConfig("zero receiver credit epoch"));
        }
        let key = CreditKey::from(peer);
        let mut state = self.lock();
        if state.entries.iter().any(|(existing, entry)| {
            existing.receiver_session == key.receiver_session && entry.automatic.is_some()
        }) {
            return Err(Error::InvalidConfig(
                "automatic receiver credit identity cannot be manually activated",
            ));
        }
        if let Some(entry) = state.entries.get(&key) {
            if epoch < entry.epoch {
                return Err(Error::EpochMismatch);
            }
            if epoch == entry.epoch {
                if qos_class != entry.qos_class {
                    return Err(Error::InvalidConfig(
                        "credit QoS class changed within epoch",
                    ));
                }
                return if entry.open {
                    Ok(())
                } else {
                    Err(Error::Cancelled)
                };
            }
            if entry.active != 0 {
                return Err(Error::BatchBusy);
            }
        } else if state.entries.len() >= self.max_entries {
            return Err(Error::ResourceExhausted);
        }
        state.entries.insert(key, Entry::new(epoch, qos_class));
        Ok(())
    }

    pub(crate) fn deactivate(&self, peer: &RemoteSegment, epoch: u64) -> Result<()> {
        if epoch == 0 {
            return Err(Error::InvalidConfig("zero receiver credit epoch"));
        }
        let mut state = self.lock();
        let key = CreditKey::from(peer);
        let Some(entry) = state.entries.get_mut(&key) else {
            return Ok(());
        };
        if entry.automatic.is_some() {
            return Err(Error::InvalidConfig(
                "use disconnect_receiver_credit for automatic bindings",
            ));
        }
        if entry.epoch != epoch {
            return Err(Error::EpochMismatch);
        }
        entry.open = false;
        let busy = entry.active != 0;
        if !busy {
            state.entries.remove(&key);
        }
        drop(state);
        self.notify();
        if busy { Err(Error::BatchBusy) } else { Ok(()) }
    }

    pub(crate) fn apply_update(
        &self,
        peer: &RemoteSegment,
        update: &ReceiverCreditUpdateV1,
    ) -> Result<CreditUpdateDisposition> {
        if update.schema_version != 1
            || update.flags != 0
            || update.epoch == 0
            || update.sequence == 0
        {
            return Err(Error::Protocol("invalid receiver credit header"));
        }
        if update.receiver_session_id != peer.source_epoch() {
            return Err(Error::EpochMismatch);
        }
        let mut totals = [None; 2];
        for amount in &update.grants {
            let index = match amount.resource {
                CreditResource::DataBytes => 0,
                CreditResource::RequestSlots => 1,
                _ => return Err(Error::Unavailable("manual staging/consumer credit updates")),
            };
            if totals[index].replace(amount.grant_total).is_some() {
                return Err(Error::Protocol("duplicate receiver credit resource"));
            }
        }
        let mut state = self.lock();
        let entry = state
            .entries
            .get_mut(&CreditKey::from(peer))
            .ok_or(Error::Unavailable("receiver credit session is not active"))?;
        if entry.automatic.is_some() {
            return Err(Error::InvalidConfig(
                "automatic receiver credit binding rejects manual updates",
            ));
        }
        if entry.epoch != update.epoch {
            return Err(Error::EpochMismatch);
        }
        if entry.qos_class != update.qos_class {
            return Err(Error::Protocol("receiver credit QoS class mismatch"));
        }
        if !entry.open {
            return Err(Error::Cancelled);
        }
        if update.sequence <= entry.sequence {
            return Ok(CreditUpdateDisposition::DuplicateOrOld);
        }
        for (i, proposed) in totals.iter().enumerate() {
            if proposed.is_some_and(|total| total < entry.grants[i] || total < entry.consumed[i]) {
                return Err(Error::Protocol("decreasing receiver credit grant"));
            }
        }
        let expires = if update.freshness_ttl_ms == 0 {
            None
        } else {
            Some(
                Instant::now()
                    .checked_add(Duration::from_millis(update.freshness_ttl_ms.into()))
                    .ok_or(Error::InvalidConfig(
                        "credit freshness overflows monotonic clock",
                    ))?,
            )
        };
        let gap = entry.sequence != 0 && update.sequence - entry.sequence > 1;
        for (i, total) in totals.iter().enumerate() {
            if let Some(total) = total {
                entry.grants[i] = *total;
            }
        }
        entry.sequence = update.sequence;
        entry.expires = expires;
        drop(state);
        self.notify();
        Ok(if gap {
            CreditUpdateDisposition::SequenceGap
        } else {
            CreditUpdateDisposition::Applied
        })
    }

    #[cfg(test)]
    pub(crate) fn prepare(
        self: &Arc<Self>,
        peer: &RemoteSegment,
        bytes: usize,
        requests: usize,
    ) -> Result<CreditPermit> {
        self.prepare_owners(peer, &[[bytes as u64, requests as u64]])?
            .pop()
            .ok_or(Error::Internal("credit permit missing"))
    }

    pub(crate) fn prepare_owners(
        self: &Arc<Self>,
        peer: &RemoteSegment,
        charges: &[[u64; 2]],
    ) -> Result<Vec<CreditPermit>> {
        let key = CreditKey::from(peer);
        let mut state = self.lock();
        let mut permits = Vec::new();
        permits
            .try_reserve_exact(charges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        let last = state
            .next_owner
            .checked_add(charges.len() as u64)
            .ok_or(Error::ResourceExhausted)?;
        state
            .owners
            .try_reserve(charges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        let entry = state
            .entries
            .get_mut(&key)
            .ok_or(Error::Unavailable("receiver credit session is not active"))?;
        if !entry.open {
            return Err(Error::Cancelled);
        }
        if entry.epoch == 0 {
            return Err(Error::Unavailable(
                "receiver credit connection is not ready",
            ));
        }
        if charges.iter().any(|&charge| entry.exceeds_window(charge)) {
            return Err(Error::ResourceExhausted);
        }
        entry.active = entry
            .active
            .checked_add(charges.len())
            .ok_or(Error::ResourceExhausted)?;
        let epoch = entry.epoch;
        for &charge in charges {
            state.next_owner += 1;
            let id = state.next_owner;
            state.owners.insert(
                id,
                CreditOwner {
                    key,
                    epoch,
                    charge,
                    reserved: false,
                    committed: false,
                    completed: false,
                },
            );
            permits.push(CreditPermit {
                reservation: Arc::new(CreditReservation {
                    ledger: self.clone(),
                    id,
                }),
            });
        }
        debug_assert_eq!(state.next_owner, last);
        Ok(permits)
    }

    pub(crate) fn stats(&self, peer: &RemoteSegment) -> Option<ReceiverCreditStats> {
        let state = self.lock();
        let entry = state.entries.get(&CreditKey::from(peer))?;
        let fresh = entry.fresh();
        let available = if entry.open && fresh {
            [
                entry.grants[0] - entry.consumed[0],
                entry.grants[1] - entry.consumed[1],
            ]
        } else {
            [0; 2]
        };
        Some(ReceiverCreditStats {
            automatic: entry.automatic.is_some(),
            completed_bytes: entry.completed[0],
            completed_requests: entry.completed[1],
            epoch: entry.epoch,
            qos_class: entry.qos_class,
            sequence: entry.sequence,
            open: entry.open,
            fresh,
            active_owners: entry.active,
            reserved_owners: entry.reserved,
            available_bytes: available[0],
            available_requests: available[1],
            consumed_bytes: entry.consumed[0],
            consumed_requests: entry.consumed[1],
        })
    }

    pub(crate) fn wake(&self) {
        let _state = self.lock();
        self.changed.notify_all();
    }

    // Call only after releasing the ledger lock: dispatch uses queue -> ledger.
    fn notify(&self) {
        self.wake();
        if let Some(queue) = self.queue.upgrade() {
            queue.wake();
        }
    }
}

/// Shared scheduling metadata only. The actual CreditPermit owns rollback/drain.
pub(crate) struct CreditReservation {
    ledger: Arc<SenderCreditLedger>,
    id: u64,
}

impl CreditReservation {
    pub(crate) fn check_active(&self) -> Result<()> {
        self.ledger.lock().owner(self.id).map(|_| ())
    }

    pub(crate) fn availability(&self) -> Result<(bool, Option<Instant>)> {
        let state = self.ledger.lock();
        let (owner, entry) = state.owner(self.id)?;
        Ok((owner.reserved || entry.fits(owner.charge), entry.expires))
    }

    pub(crate) fn try_reserve(&self) -> Result<bool> {
        self.ledger.lock().try_reserve(self.id)
    }

    pub(crate) fn wait(
        &self,
        deadline: Instant,
        cancelled: impl Fn() -> bool + Sync,
    ) -> Result<()> {
        let mut state = self.ledger.lock();
        loop {
            if cancelled() {
                return Err(Error::Cancelled);
            }
            let timeout = remaining(deadline)?;
            if state.try_reserve(self.id)? {
                return Ok(());
            }
            let (owner, entry) = state.owner(self.id)?;
            if entry.needs_sync(owner) {
                drop(state);
                self.synchronize(deadline, &cancelled)?;
                state = self.ledger.lock();
                continue;
            }
            state = self
                .ledger
                .changed
                .wait_timeout(state, timeout)
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
        }
    }

    pub(crate) fn commit(&self) -> Result<()> {
        let mut state = self.ledger.lock();
        let (owner, _) = state.owner(self.id)?;
        if !owner.reserved {
            return Err(Error::Internal("unreserved receiver credit handoff"));
        }
        if !owner.committed {
            let entry = state
                .entries
                .get_mut(&owner.key)
                .ok_or(Error::Internal("credit session missing"))?;
            for (i, charge) in owner.charge.iter().enumerate() {
                entry.committed[i] += charge;
            }
        }
        state
            .owners
            .get_mut(&self.id)
            .ok_or(Error::Internal("credit owner missing"))?
            .committed = true;
        Ok(())
    }

    pub(crate) fn complete(&self) {
        if let Some(owner) = self.ledger.lock().owners.get_mut(&self.id) {
            owner.completed = owner.committed;
        }
    }
}

pub(crate) struct CreditPermit {
    pub(crate) reservation: Arc<CreditReservation>,
}

impl Drop for CreditPermit {
    fn drop(&mut self) {
        let ledger = &self.reservation.ledger;
        let mut state = ledger.lock();
        let mut refunded = false;
        if let Some(owner) = state.owners.remove(&self.reservation.id) {
            if let Some(entry) = state
                .entries
                .get_mut(&owner.key)
                .filter(|entry| entry.epoch == owner.epoch)
            {
                entry.active -= 1;
                if owner.completed {
                    for (i, charge) in owner.charge.iter().enumerate() {
                        entry.completed[i] += charge;
                    }
                    refunded |= entry.automatic.is_some();
                }
                if owner.reserved {
                    entry.reserved -= 1;
                    if !owner.committed {
                        refunded = true;
                        for (i, amount) in owner.charge.iter().enumerate() {
                            entry.consumed[i] -= amount;
                        }
                    }
                }
            }
        }
        drop(state);
        if refunded {
            ledger.notify();
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/receiver_credit.rs"]
mod tests;
