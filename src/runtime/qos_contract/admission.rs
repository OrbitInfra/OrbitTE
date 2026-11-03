use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use super::{CompiledContract, QosPolicyFields, QosUsage};
use crate::runtime::admission_queue::LocalTransferAdmissionQueue;
use crate::{Error, Result};

#[derive(Clone, Copy)]
pub(crate) struct QosCharge {
    slot: usize,
    bytes: usize,
    requests: usize,
}

impl QosCharge {
    pub(crate) fn add(
        charges: &mut Vec<Self>,
        contract: &CompiledContract,
        bytes: usize,
    ) -> Result<()> {
        if !contract.fields.bounded() {
            return Ok(());
        }
        if let Some(last) = charges.last_mut().filter(|last| last.slot == contract.slot) {
            last.bytes = last
                .bytes
                .checked_add(bytes)
                .ok_or(Error::ResourceExhausted)?;
            last.requests = last
                .requests
                .checked_add(1)
                .ok_or(Error::ResourceExhausted)?;
        } else {
            charges
                .try_reserve(1)
                .map_err(|_| Error::ResourceExhausted)?;
            charges.push(Self {
                slot: contract.slot,
                bytes,
                requests: 1,
            });
        }
        Ok(())
    }

    fn coalesce(charges: &mut Vec<Self>) -> Result<()> {
        // Adjacent requests coalesce during preparation; sort remaining runs once.
        charges.sort_unstable_by_key(|charge| charge.slot);
        let mut count = 0;
        for index in 0..charges.len() {
            let charge = charges[index];
            if count > 0 && charges[count - 1].slot == charge.slot {
                let previous = &mut charges[count - 1];
                previous.bytes = previous
                    .bytes
                    .checked_add(charge.bytes)
                    .ok_or(Error::ResourceExhausted)?;
                previous.requests = previous
                    .requests
                    .checked_add(charge.requests)
                    .ok_or(Error::ResourceExhausted)?;
            } else {
                charges[count] = charge;
                count += 1;
            }
        }
        charges.truncate(count);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct QosQuota {
    limits: Vec<QosPolicyFields>,
    usage: Mutex<Vec<QosUsage>>,
    queue: Option<Weak<LocalTransferAdmissionQueue>>,
}

impl QosQuota {
    pub(super) fn new(
        limits: Vec<QosPolicyFields>,
        queue: Option<&Arc<LocalTransferAdmissionQueue>>,
    ) -> Self {
        Self {
            usage: Mutex::new(vec![QosUsage::default(); limits.len()]),
            limits,
            queue: queue.map(Arc::downgrade),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<QosUsage>> {
        self.usage.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn usage(&self, slot: usize) -> QosUsage {
        self.lock()[slot]
    }

    pub(super) fn admit(self: &Arc<Self>, mut charges: Vec<QosCharge>) -> Result<QosPermit> {
        QosCharge::coalesce(&mut charges)?;
        // An intrinsically oversized group cannot become eligible after drain.
        for charge in &charges {
            let limits = self.limits[charge.slot];
            if limits
                .max_inflight_bytes
                .is_some_and(|cap| charge.bytes > cap)
                || limits
                    .max_inflight_requests
                    .is_some_and(|cap| charge.requests > cap)
            {
                return Err(Error::ResourceExhausted);
            }
        }
        let mut usage = self.lock();
        self.available(&usage, &charges)?;
        if self.queue.is_some() {
            // All bounded contracts in a delay-enabled engine acquire in Batch
            // head order, including reject-only contracts. A later group cannot
            // reserve capacity needed by its own waiting predecessor.
            Ok(QosPermit::Queued(Arc::new(QosReservation {
                quota: Arc::clone(self),
                charges,
                charged: OnceLock::new(),
            })))
        } else {
            Self::charge(&mut usage, &charges);
            Ok(QosPermit::Admitted {
                quota: Arc::clone(self),
                charges,
            })
        }
    }

    fn available(&self, usage: &[QosUsage], charges: &[QosCharge]) -> Result<bool> {
        let mut available = true;
        for charge in charges {
            let current = usage[charge.slot];
            let limits = self.limits[charge.slot];
            let bytes = current
                .inflight_bytes
                .checked_add(charge.bytes)
                .ok_or(Error::ResourceExhausted)?;
            let requests = current
                .inflight_requests
                .checked_add(charge.requests)
                .ok_or(Error::ResourceExhausted)?;
            if current.owners == usize::MAX {
                return Err(Error::ResourceExhausted);
            }
            if limits.max_inflight_bytes.is_some_and(|cap| bytes > cap)
                || limits
                    .max_inflight_requests
                    .is_some_and(|cap| requests > cap)
            {
                if !limits
                    .allowed_degraded_actions
                    .is_some_and(|actions| actions.delay)
                {
                    return Err(Error::ResourceExhausted);
                }
                available = false;
            }
        }
        Ok(available)
    }

    fn charge(usage: &mut [QosUsage], charges: &[QosCharge]) {
        for charge in charges {
            let current = &mut usage[charge.slot];
            current.inflight_bytes += charge.bytes;
            current.inflight_requests += charge.requests;
            current.owners += 1;
        }
    }

    fn release(&self, charges: &[QosCharge]) {
        let mut usage = self.lock();
        for charge in charges {
            let current = &mut usage[charge.slot];
            current.inflight_bytes -= charge.bytes;
            current.inflight_requests -= charge.requests;
            current.owners -= 1;
        }
        // Scheduling takes queue -> QoS -> credit; never wake under this lock.
        drop(usage);
        if let Some(queue) = self.queue.as_ref().and_then(Weak::upgrade) {
            queue.wake();
        }
    }
}

/// Shared scheduling metadata. Only QosPermit returns the actual owner's quota.
pub(crate) struct QosReservation {
    quota: Arc<QosQuota>,
    charges: Vec<QosCharge>,
    charged: OnceLock<()>,
}

impl QosReservation {
    pub(crate) fn availability(&self) -> Result<bool> {
        let usage = self.quota.lock();
        if self.charged.get().is_some() {
            return Ok(true);
        }
        self.quota.available(&usage, &self.charges)
    }

    /// Reserve all QoS slots only if the queue can also acquire receiver credit.
    /// The callback must not take a queue lock or perform I/O.
    pub(crate) fn try_reserve(&self, credit_ready: impl FnOnce() -> bool) -> Result<bool> {
        let mut usage = self.quota.lock();
        if self.charged.get().is_some() {
            return Ok(credit_ready());
        }
        if !self.quota.available(&usage, &self.charges)? || !credit_ready() {
            return Ok(false);
        }
        QosQuota::charge(&mut usage, &self.charges);
        let _ = self.charged.set(());
        Ok(true)
    }
}

pub(crate) enum QosPermit {
    Admitted {
        quota: Arc<QosQuota>,
        charges: Vec<QosCharge>,
    },
    Queued(Arc<QosReservation>),
}

impl QosPermit {
    pub(crate) fn reservation(&self) -> Option<Arc<QosReservation>> {
        match self {
            Self::Admitted { .. } => None,
            Self::Queued(reservation) => Some(Arc::clone(reservation)),
        }
    }
}

impl Drop for QosPermit {
    fn drop(&mut self) {
        match self {
            Self::Admitted { quota, charges } => quota.release(charges),
            Self::Queued(reservation) if reservation.charged.get().is_some() => {
                reservation.quota.release(&reservation.charges);
            }
            Self::Queued(_) => {}
        }
    }
}
