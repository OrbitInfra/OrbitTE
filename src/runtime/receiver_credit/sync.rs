use super::protocol::{CreditReport, CreditSnapshot};
use super::*;

pub(super) struct AutomaticCredit {
    sender: SourceEpoch,
    window: [u64; 2],
    reported_completed: [u64; 2],
    syncing: bool,
    closed: bool,
}

impl Entry {
    pub(super) fn needs_sync(&self, owner: CreditOwner) -> bool {
        self.automatic.as_ref().is_some_and(|automatic| {
            !owner.reserved
                && !self.fits(owner.charge)
                && !automatic.syncing
                && self.completed != automatic.reported_completed
        })
    }

    pub(super) fn exceeds_window(&self, charge: [u64; 2]) -> bool {
        self.automatic.as_ref().is_some_and(|automatic| {
            charge
                .iter()
                .zip(automatic.window)
                .any(|(charge, window)| *charge > window)
        })
    }
}

impl SenderCreditLedger {
    pub(crate) fn connect_automatic(
        &self,
        peer: &RemoteSegment,
        sender: SourceEpoch,
        qos_class: u32,
        deadline: Instant,
    ) -> Result<()> {
        let key = CreditKey::from(peer);
        let mut state = self.lock();
        if !state.entries.contains_key(&key) {
            if state
                .entries
                .keys()
                .any(|existing| existing.receiver_session == key.receiver_session)
            {
                return Err(Error::InvalidConfig(
                    "receiver credit identity already bound to another endpoint",
                ));
            }
            if state.entries.len() >= self.max_entries {
                return Err(Error::ResourceExhausted);
            }
            let mut entry = Entry::new(0, qos_class);
            entry.automatic = Some(AutomaticCredit {
                sender,
                window: [0; 2],
                reported_completed: [0; 2],
                syncing: false,
                closed: false,
            });
            state.entries.insert(key, entry);
        }
        loop {
            let entry = state
                .entries
                .get(&key)
                .ok_or(Error::Internal("automatic credit entry missing"))?;
            let automatic = entry.automatic.as_ref().ok_or(Error::InvalidConfig(
                "manual receiver credit binding already exists",
            ))?;
            if automatic.sender != sender || entry.qos_class != qos_class {
                return Err(Error::InvalidConfig(
                    "automatic receiver credit identity changed",
                ));
            }
            if !entry.open {
                return Err(Error::Cancelled);
            }
            if entry.epoch != 0 {
                return Ok(());
            }
            let timeout = remaining(deadline)?;
            if automatic.syncing {
                state = self
                    .changed
                    .wait_timeout(state, timeout)
                    .unwrap_or_else(|p| p.into_inner())
                    .0;
                continue;
            }
            drop(state);
            self.synchronize(key, false, deadline, &|| false)?;
            state = self.lock();
        }
    }

    pub(crate) fn disconnect_automatic(
        &self,
        peer: &RemoteSegment,
        deadline: Instant,
    ) -> Result<()> {
        let key = CreditKey::from(peer);
        let mut state = self.lock();
        let entry = state.entries.get_mut(&key).ok_or(Error::Unavailable(
            "automatic receiver credit binding missing",
        ))?;
        let automatic = entry
            .automatic
            .as_ref()
            .ok_or(Error::InvalidConfig("manual receiver credit binding"))?;
        if automatic.closed {
            return Ok(());
        }
        if entry.epoch == 0 {
            return Err(Error::Unavailable(
                "retry receiver credit connect before close",
            ));
        }
        entry.open = false;
        let busy = entry.active != 0 || entry.committed != entry.completed || automatic.syncing;
        drop(state);
        self.notify();
        if busy {
            return Err(Error::BatchBusy);
        }
        if !self.synchronize(key, true, deadline, &|| false)? {
            return Err(Error::BatchBusy);
        }
        Ok(())
    }

    fn synchronize(
        &self,
        key: CreditKey,
        close: bool,
        deadline: Instant,
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<bool> {
        remaining(deadline)?;
        let report = {
            let mut state = self.lock();
            let entry = state
                .entries
                .get_mut(&key)
                .ok_or(Error::Internal("automatic credit entry missing"))?;
            let automatic = entry
                .automatic
                .as_mut()
                .ok_or(Error::Internal("automatic credit mode missing"))?;
            if automatic.closed {
                return Err(Error::Cancelled);
            }
            if !close && !entry.open {
                return Err(Error::Cancelled);
            }
            if close && (entry.open || entry.active != 0 || entry.committed != entry.completed) {
                return Err(Error::BatchBusy);
            }
            if automatic.syncing
                || (!close && entry.epoch != 0 && entry.completed == automatic.reported_completed)
            {
                return Ok(false);
            }
            automatic.syncing = true;
            CreditReport {
                sender: automatic.sender,
                epoch: entry.epoch,
                qos_class: entry.qos_class,
                committed: entry.committed,
                completed: entry.completed,
            }
        };
        // This is the only network boundary. Neither ledger nor queue lock is held.
        let flight = SyncFlight { ledger: self, key };
        let reply = protocol::synchronize(
            key.endpoint,
            key.receiver_session,
            report,
            close,
            deadline,
            cancelled,
        );
        let mut state = self.lock();
        let result = (|| {
            let entry = state
                .entries
                .get_mut(&key)
                .ok_or(Error::Internal("automatic credit entry missing"))?;
            let snapshot = reply?;
            entry.apply_snapshot(report, snapshot, close)?;
            Ok(true)
        })();
        drop(state);
        drop(flight);
        result
    }
}

struct SyncFlight<'a> {
    ledger: &'a SenderCreditLedger,
    key: CreditKey,
}

impl Drop for SyncFlight<'_> {
    fn drop(&mut self) {
        if let Some(automatic) = self
            .ledger
            .lock()
            .entries
            .get_mut(&self.key)
            .and_then(|entry| entry.automatic.as_mut())
        {
            automatic.syncing = false;
        }
        self.ledger.notify();
    }
}

impl Entry {
    fn apply_snapshot(
        &mut self,
        report: CreditReport,
        snapshot: CreditSnapshot,
        close: bool,
    ) -> Result<()> {
        let automatic = self
            .automatic
            .as_mut()
            .ok_or(Error::Internal("automatic credit mode missing"))?;
        if snapshot.qos_class != self.qos_class
            || (self.epoch != 0 && snapshot.epoch != self.epoch)
            || snapshot.sequence < self.sequence
            || (self.epoch != 0 && snapshot.window != automatic.window)
        {
            return Err(Error::Protocol(
                "automatic receiver credit identity or window changed",
            ));
        }
        for i in 0..2 {
            if snapshot.window[i].checked_add(report.completed[i]) != Some(snapshot.grants[i])
                || snapshot.grants[i] < self.grants[i]
                || snapshot.grants[i] < self.consumed[i]
            {
                return Err(Error::Protocol("invalid automatic receiver credit total"));
            }
        }
        self.epoch = snapshot.epoch;
        self.sequence = snapshot.sequence;
        self.grants = snapshot.grants;
        self.expires = None;
        automatic.window = snapshot.window;
        automatic.reported_completed = report.completed;
        automatic.closed = close;
        Ok(())
    }
}

impl CreditReservation {
    #[cfg(feature = "cuda")]
    pub(crate) fn pin_binding(&self) -> Result<Option<protocol::PinCredit>> {
        let state = self.ledger.lock();
        let owner = state
            .owners
            .get(&self.id)
            .ok_or(Error::Internal("credit owner missing"))?;
        let entry = state
            .entries
            .get(&owner.key)
            .ok_or(Error::Internal("credit session missing"))?;
        if entry.epoch != owner.epoch {
            return Err(Error::EpochMismatch);
        }
        // Fencing stops new groups; an already committed group must finish its
        // later Pin requests before the same binding can be retired.
        if !entry.open && !owner.committed {
            return Err(Error::Cancelled);
        }
        Ok(entry
            .automatic
            .as_ref()
            .map(|automatic| protocol::PinCredit {
                sender: automatic.sender,
                epoch: entry.epoch,
            }))
    }

    /// The caller checks this while holding its wait mutex, then performs sync
    /// outside that mutex and rechecks before parking. A completing owner wakes
    /// both wait domains only after releasing the ledger lock.
    pub(crate) fn needs_sync(&self) -> Result<bool> {
        let state = self.ledger.lock();
        let (owner, entry) = state.owner(self.id)?;
        Ok(entry.needs_sync(owner))
    }

    pub(crate) fn synchronize(
        &self,
        deadline: Instant,
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<()> {
        let key = {
            let state = self.ledger.lock();
            let (owner, entry) = state.owner(self.id)?;
            if !entry.needs_sync(owner) {
                return Ok(());
            }
            owner.key
        };
        self.ledger
            .synchronize(key, false, deadline, cancelled)
            .map(|_| ())
    }
}
