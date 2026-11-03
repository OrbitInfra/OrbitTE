//! Automatic remote-Pin capacity follows the source's actual cleanup owners.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::ReceiverCreditIssuer;
use crate::runtime::receiver_credit::CreditResource;
use crate::runtime::receiver_credit::protocol::PinCredit;
use crate::{Error, Result, SourceEpoch};

impl ReceiverCreditIssuer {
    pub(crate) fn reserve_staging(
        self: &Arc<Self>,
        binding: Option<PinCredit>,
        depth: usize,
    ) -> Result<Option<StagingCreditReservation>> {
        if self.config.staging_slots_per_sender == 0 {
            return Ok(None);
        }
        let binding = binding.ok_or(Error::Unavailable(
            "remote staging requires an automatic receiver credit binding",
        ))?;
        if !(1..=16).contains(&depth) {
            return Err(Error::InvalidRange);
        }
        let charge = [depth as u64, 1];
        let window = self.config.resource_window();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let session = state
            .sessions
            .get_mut(&binding.sender)
            .ok_or(Error::EpochMismatch)?;
        if session.epoch != binding.epoch {
            return Err(Error::EpochMismatch);
        }
        if !session.open || session.closing {
            return Err(Error::Cancelled);
        }
        let mut consumed = session.resource_consumed;
        for i in 0..2 {
            consumed[i] = consumed[i]
                .checked_add(charge[i])
                .ok_or(Error::ResourceExhausted)?;
            if consumed[i] - session.resource_released[i] > window[i] {
                return Err(Error::ResourceExhausted);
            }
        }
        session.resource_consumed = consumed;
        state.stats.active_staging_slots += charge[0];
        state.stats.active_consumer_slots += charge[1];
        Ok(Some(StagingCreditReservation {
            owner: Arc::new(PinCreditOwner {
                issuer: Arc::clone(self),
                sender: binding.sender,
                epoch: binding.epoch,
                staging: AtomicU64::new(charge[0]),
            }),
            remaining: charge,
        }))
    }

    fn release_resources(&self, sender: SourceEpoch, epoch: u64, charge: [u64; 2]) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = state.sessions.get_mut(&sender).filter(|s| s.epoch == epoch) {
            for (released, amount) in session.resource_released.iter_mut().zip(charge) {
                *released += amount;
            }
            state.stats.active_staging_slots -= charge[0];
            state.stats.active_consumer_slots -= charge[1];
        }
    }

    fn retain_resource(&self, charge: [u64; 2]) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.stats.retained_staging_slots += charge[0];
        state.stats.retained_consumer_slots += charge[1];
    }
}

/// The consumer cannot retire while any of this Pin's staging owners remain.
struct PinCreditOwner {
    issuer: Arc<ReceiverCreditIssuer>,
    sender: SourceEpoch,
    epoch: u64,
    staging: AtomicU64,
}

impl PinCreditOwner {
    fn release(&self, charge: [u64; 2]) {
        self.issuer
            .release_resources(self.sender, self.epoch, charge);
        self.staging.fetch_sub(charge[0], Ordering::Release);
    }
}

/// Unassigned capacity has never touched a physical owner and rolls back on Drop.
pub(crate) struct StagingCreditReservation {
    owner: Arc<PinCreditOwner>,
    remaining: [u64; 2],
}

impl StagingCreditReservation {
    pub(crate) fn take(&mut self, resource: CreditResource) -> Result<ReceiverResourceLease> {
        let index = match resource {
            CreditResource::StagingSlots => 0,
            CreditResource::ConsumerSlots => 1,
            _ => return Err(Error::Internal("not a source-owned staging resource")),
        };
        self.remaining[index] = self.remaining[index]
            .checked_sub(1)
            .ok_or(Error::Internal("staging credit already assigned"))?;
        Ok(ReceiverResourceLease {
            owner: Some(Arc::clone(&self.owner)),
            charge: if index == 0 { [1, 0] } else { [0, 1] },
            bound: false,
            retained: false,
        })
    }
}

impl Drop for StagingCreditReservation {
    fn drop(&mut self) {
        if self.remaining != [0; 2] {
            self.owner.release(self.remaining);
        }
    }
}

/// Once bound, only an explicit native-cleanup proof can release the charge.
pub(crate) struct ReceiverResourceLease {
    owner: Option<Arc<PinCreditOwner>>,
    charge: [u64; 2],
    bound: bool,
    retained: bool,
}

impl ReceiverResourceLease {
    pub(crate) fn bind(&mut self) {
        self.bound = true;
    }

    pub(crate) fn retain(&mut self) {
        self.bound = true;
        if !self.retained {
            if let Some(owner) = &self.owner {
                owner.issuer.retain_resource(self.charge);
            }
            self.retained = true;
        }
    }

    pub(crate) fn release(mut self) -> Result<()> {
        if self.retained
            || (self.charge[1] != 0
                && self
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.staging.load(Ordering::Acquire) != 0))
        {
            self.retain();
            return Err(Error::Unquiesced {
                operation: "remote staging credit owners",
                code: -1,
            });
        }
        self.bound = false;
        Ok(())
    }
}

impl Drop for ReceiverResourceLease {
    fn drop(&mut self) {
        if self.bound {
            self.retain();
            // Keep the authority ledger with the unresolved physical owner.
            std::mem::forget(self.owner.take());
        } else if let Some(owner) = self.owner.take() {
            owner.release(self.charge);
        }
    }
}
