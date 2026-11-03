use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "cuda")]
mod resources;
#[cfg(feature = "cuda")]
pub(crate) use resources::{ReceiverResourceLease, StagingCreditReservation};

use crate::{Error, Result, SourceEpoch};

use super::protocol::{CreditReport, CreditSnapshot};

/// Receiver-owned cooperative windows. Counts cover public payload submissions,
/// not physical receive queues or authenticated tenants. Closed sessions retain
/// tombstones until the receiver engine exits.
#[derive(Clone, Copy, Debug)]
pub struct ReceiverCreditConfig {
    pub max_sessions: usize,
    pub max_bytes: u64,
    pub max_requests: u64,
    pub bytes_per_sender: u64,
    pub requests_per_sender: u64,
    /// Remote Pin owner windows. All four zero disables this resource gate.
    /// These authorize concurrent owners; the shared physical pool can still fill.
    pub max_staging_slots: u64,
    pub max_consumer_slots: u64,
    pub staging_slots_per_sender: u64,
    pub consumer_slots_per_sender: u64,
}

impl Default for ReceiverCreditConfig {
    fn default() -> Self {
        Self {
            max_sessions: 128,
            max_bytes: 64 << 20,
            max_requests: 1024,
            bytes_per_sender: 1 << 20,
            requests_per_sender: 16,
            max_staging_slots: 0,
            max_consumer_slots: 0,
            staging_slots_per_sender: 0,
            consumer_slots_per_sender: 0,
        }
    }
}

impl ReceiverCreditConfig {
    pub(crate) fn validate(self) -> Result<()> {
        let resources = [
            self.max_staging_slots,
            self.max_consumer_slots,
            self.staging_slots_per_sender,
            self.consumer_slots_per_sender,
        ];
        if resources != [0; 4]
            && (resources.contains(&0)
                || self.staging_slots_per_sender > self.max_staging_slots
                || self.consumer_slots_per_sender > self.max_consumer_slots)
        {
            return Err(Error::InvalidConfig(
                "staging credit windows must be all zero or positive and fit capacity",
            ));
        }
        #[cfg(not(feature = "cuda"))]
        if resources != [0; 4] {
            return Err(Error::InvalidConfig(
                "staging credit windows require --features cuda",
            ));
        }
        if !(1..=4096).contains(&self.max_sessions)
            || self.bytes_per_sender == 0
            || self.requests_per_sender == 0
            || self.bytes_per_sender > self.max_bytes
            || self.requests_per_sender > self.max_requests
        {
            return Err(Error::InvalidConfig(
                "receiver credit windows must be positive and fit bounded capacity",
            ));
        }
        Ok(())
    }

    fn resource_window(self) -> [u64; 2] {
        [
            self.staging_slots_per_sender,
            self.consumer_slots_per_sender,
        ]
    }

    fn window(self) -> [u64; 2] {
        [self.bytes_per_sender, self.requests_per_sender]
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReceiverCreditIssuerStats {
    pub active_senders: usize,
    pub retired_senders: usize,
    pub reserved_bytes: u64,
    pub reserved_requests: u64,
    pub reserved_staging_slots: u64,
    pub reserved_consumer_slots: u64,
    /// Borrowed slots, including construction and retained owners.
    pub active_staging_slots: u64,
    pub active_consumer_slots: u64,
    /// Subsets of active slots whose cleanup remains unproven.
    pub retained_staging_slots: u64,
    pub retained_consumer_slots: u64,
}

struct Session {
    epoch: u64,
    qos_class: u32,
    sequence: u64,
    committed: [u64; 2],
    completed: [u64; 2],
    open: bool,
    closing: bool,
    resource_consumed: [u64; 2],
    resource_released: [u64; 2],
}

#[derive(Default)]
struct IssuerState {
    sessions: HashMap<SourceEpoch, Session>,
    next_epoch: u64,
    stats: ReceiverCreditIssuerStats,
}

pub(crate) struct ReceiverCreditIssuer {
    config: ReceiverCreditConfig,
    state: Mutex<IssuerState>,
}

impl ReceiverCreditIssuer {
    pub(crate) fn new(config: ReceiverCreditConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            state: Mutex::default(),
        })
    }

    pub(crate) fn stats(&self) -> ReceiverCreditIssuerStats {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).stats
    }

    /// Reports and responses are cumulative, so a lost response can be retried
    /// without replaying payload or creating another window. No timer reclaims
    /// outstanding authority, including after sender exit or control EOF.
    pub(super) fn synchronize(
        &self,
        report: CreditReport,
        close: bool,
        allow_open: bool,
    ) -> Result<CreditSnapshot> {
        let window = self.config.window();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.sessions.contains_key(&report.sender) {
            if !allow_open {
                return Err(Error::SourceClosed);
            }
            if close
                || report.epoch != 0
                || report.committed != [0; 2]
                || report.completed != [0; 2]
            {
                return Err(Error::EpochMismatch);
            }
            let bytes = state
                .stats
                .reserved_bytes
                .checked_add(window[0])
                .ok_or(Error::ResourceExhausted)?;
            let requests = state
                .stats
                .reserved_requests
                .checked_add(window[1])
                .ok_or(Error::ResourceExhausted)?;
            let resource_window = self.config.resource_window();
            let staging = state
                .stats
                .reserved_staging_slots
                .checked_add(resource_window[0])
                .ok_or(Error::ResourceExhausted)?;
            let consumers = state
                .stats
                .reserved_consumer_slots
                .checked_add(resource_window[1])
                .ok_or(Error::ResourceExhausted)?;
            let epoch = state
                .next_epoch
                .checked_add(1)
                .ok_or(Error::ResourceExhausted)?;
            if state.sessions.len() >= self.config.max_sessions
                || bytes > self.config.max_bytes
                || requests > self.config.max_requests
                || staging > self.config.max_staging_slots
                || consumers > self.config.max_consumer_slots
            {
                return Err(Error::ResourceExhausted);
            }
            state.sessions.insert(
                report.sender,
                Session {
                    epoch,
                    qos_class: report.qos_class,
                    sequence: 1,
                    committed: [0; 2],
                    completed: [0; 2],
                    open: true,
                    closing: false,
                    resource_consumed: [0; 2],
                    resource_released: [0; 2],
                },
            );
            state.next_epoch = epoch;
            state.stats.active_senders += 1;
            state.stats.reserved_bytes = bytes;
            state.stats.reserved_requests = requests;
            state.stats.reserved_staging_slots = staging;
            state.stats.reserved_consumer_slots = consumers;
        }
        let session = state
            .sessions
            .get_mut(&report.sender)
            .ok_or(Error::Internal("credit issuer session missing"))?;
        if report.qos_class != session.qos_class {
            return Err(Error::Protocol("receiver credit class changed"));
        }
        if report.epoch == 0 {
            // An initial-open retry returns the existing authority, never resets it.
            if close || report.committed != [0; 2] || report.completed != [0; 2] {
                return Err(Error::Protocol("invalid initial receiver credit report"));
            }
        } else if report.epoch != session.epoch {
            return Err(Error::EpochMismatch);
        }
        if session.closing && !close {
            return Err(Error::Cancelled);
        }
        let old_grants = [
            window[0].checked_add(session.completed[0]),
            window[1].checked_add(session.completed[1]),
        ];
        let mut committed = session.committed;
        let mut completed = session.completed;
        for i in 0..2 {
            if report.completed[i] > report.committed[i]
                || report.committed[i] > old_grants[i].ok_or(Error::ResourceExhausted)?
            {
                return Err(Error::Protocol(
                    "receiver credit completion exceeds issued authority",
                ));
            }
            committed[i] = committed[i].max(report.committed[i]);
            completed[i] = completed[i].max(report.completed[i]);
        }
        if !session.open
            && (!close
                || report.committed != session.committed
                || report.completed != session.completed)
        {
            return Err(Error::Cancelled);
        }
        if close {
            // Fence resource admission even when actual cleanup is still busy.
            // Existing owner releases use this same ledger; only they drain it.
            session.closing = true;
            if report.epoch == 0
                || committed != completed
                || session.resource_consumed != session.resource_released
            {
                return Err(Error::BatchBusy);
            }
        }
        let grants = [
            window[0]
                .checked_add(completed[0])
                .ok_or(Error::ResourceExhausted)?,
            window[1]
                .checked_add(completed[1])
                .ok_or(Error::ResourceExhausted)?,
        ];
        let changed = committed != session.committed
            || completed != session.completed
            || (close && session.open);
        let sequence = if changed {
            session
                .sequence
                .checked_add(1)
                .ok_or(Error::ResourceExhausted)?
        } else {
            session.sequence
        };
        let retiring = close && session.open;
        session.committed = committed;
        session.completed = completed;
        session.sequence = sequence;
        session.open &= !close;
        let snapshot = CreditSnapshot {
            epoch: session.epoch,
            sequence,
            qos_class: session.qos_class,
            window,
            grants,
        };
        if retiring {
            state.stats.active_senders -= 1;
            state.stats.retired_senders += 1;
            state.stats.reserved_bytes -= window[0];
            state.stats.reserved_requests -= window[1];
            let resource_window = self.config.resource_window();
            state.stats.reserved_staging_slots -= resource_window[0];
            state.stats.reserved_consumer_slots -= resource_window[1];
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/receiver_credit/issuer.rs"]
mod tests;
