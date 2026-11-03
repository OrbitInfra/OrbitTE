use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use super::{CompiledContract, QosPolicyFields, QosUsage};
use crate::runtime::admission_queue::LocalTransferAdmissionQueue;
use crate::{Error, Result};

#[derive(Clone, Copy)]
pub(crate) struct QosCharge {
    slot: usize,
    bytes: usize,
}

impl QosCharge {
    pub(crate) fn new(contract: &CompiledContract, bytes: usize) -> Option<Self> {
        contract.fields.bounded().then_some(Self {
            slot: contract.slot,
            bytes,
        })
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

    #[cfg(feature = "metrics")]
    pub(super) fn total_usage(&self) -> QosUsage {
        self.lock()
            .iter()
            .fold(QosUsage::default(), |mut total, u| {
                total.owners += u.owners;
                total.inflight_bytes += u.inflight_bytes;
                total.inflight_requests += u.inflight_requests;
                total
            })
    }
    pub(super) fn usage(&self, slot: usize) -> QosUsage {
        self.lock()[slot]
    }

    pub(super) fn admit(
        self: &Arc<Self>,
        charges: Vec<Option<QosCharge>>,
    ) -> Result<Vec<Option<QosPermit>>> {
        let queued = self.queue.is_some();
        let mut permits = Vec::new();
        permits
            .try_reserve_exact(charges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        let mut usage = self.lock();
        for (index, item) in charges.iter().enumerate() {
            let Some(c) = item else { continue };
            // Direct admission publishes the complete set or none under the
            // ledger lock. Queued owners take their individual slot at dispatch.
            let result = self.available(&usage, c);
            if let Err(error) = result {
                if !queued {
                    for old in charges[..index].iter().flatten() {
                        Self::uncharge(&mut usage, old);
                    }
                }
                return Err(error);
            }
            if !queued {
                Self::charge(&mut usage, c);
            }
        }
        drop(usage);
        permits.extend(charges.into_iter().map(|item| {
            item.map(|charge| {
                if queued {
                    QosPermit::Queued(Arc::new(QosReservation {
                        quota: self.clone(),
                        charge,
                        state: AtomicU8::new(PENDING),
                    }))
                } else {
                    QosPermit::Admitted {
                        quota: self.clone(),
                        charge,
                    }
                }
            })
        }));
        Ok(permits)
    }

    fn available(&self, usage: &[QosUsage], charge: &QosCharge) -> Result<bool> {
        let current = usage[charge.slot];
        let limits = self.limits[charge.slot];
        if limits
            .max_inflight_bytes
            .is_some_and(|cap| charge.bytes > cap)
            || limits.max_inflight_requests == Some(0)
        {
            return Err(Error::ResourceExhausted);
        }
        let bytes = current
            .inflight_bytes
            .checked_add(charge.bytes)
            .ok_or(Error::ResourceExhausted)?;
        let requests = current
            .inflight_requests
            .checked_add(1)
            .ok_or(Error::ResourceExhausted)?;
        if limits.max_inflight_bytes.is_some_and(|cap| bytes > cap)
            || limits
                .max_inflight_requests
                .is_some_and(|cap| requests > cap)
        {
            if self.queue.is_some() && limits.allowed_degraded_actions.is_some_and(|a| a.delay) {
                return Ok(false);
            }
            return Err(Error::ResourceExhausted);
        }
        Ok(true)
    }
    fn charge(usage: &mut [QosUsage], charge: &QosCharge) {
        let current = &mut usage[charge.slot];
        current.inflight_bytes += charge.bytes;
        current.inflight_requests += 1;
        current.owners += 1;
    }
    fn uncharge(usage: &mut [QosUsage], charge: &QosCharge) {
        let current = &mut usage[charge.slot];
        current.inflight_bytes -= charge.bytes;
        current.inflight_requests -= 1;
        current.owners -= 1;
    }
    fn release(&self, charge: &QosCharge) {
        Self::uncharge(&mut self.lock(), charge);
        self.wake_queue();
    }
    // Scheduling takes queue -> QoS -> credit; call only after unlocking quota.
    fn wake_queue(&self) {
        if let Some(queue) = self.queue.as_ref().and_then(Weak::upgrade) {
            queue.wake();
        }
    }
}

const PENDING: u8 = 0;
const CHARGED: u8 = 1;
const RETIRED: u8 = 2;

/// Shared scheduling metadata. Only QosPermit retires the actual owner's quota.
pub(crate) struct QosReservation {
    quota: Arc<QosQuota>,
    charge: QosCharge,
    // Every transition/read is serialized by quota.usage. Atomic storage lets
    // the queue share metadata without introducing a second admission lock.
    state: AtomicU8,
}

impl QosReservation {
    pub(crate) fn availability(&self) -> Result<bool> {
        let usage = self.quota.lock();
        match self.state.load(Ordering::Relaxed) {
            RETIRED => return Ok(false),
            CHARGED => return Ok(true),
            _ => {}
        }
        self.quota.available(&usage, &self.charge)
    }

    /// Reserve all QoS slots only if the queue can also acquire receiver credit.
    /// The callback must not take a queue lock or perform I/O.
    pub(crate) fn try_reserve(&self, credit_ready: impl FnOnce() -> bool) -> Result<bool> {
        let mut usage = self.quota.lock();
        match self.state.load(Ordering::Relaxed) {
            RETIRED => return Ok(false),
            CHARGED => return Ok(credit_ready()),
            _ => {}
        }
        if !self.quota.available(&usage, &self.charge)? || !credit_ready() {
            return Ok(false);
        }
        QosQuota::charge(&mut usage, &self.charge);
        self.state.store(CHARGED, Ordering::Relaxed);
        Ok(true)
    }

    fn retire(&self) {
        let mut usage = self.quota.lock();
        let charged = self.state.swap(RETIRED, Ordering::Relaxed) == CHARGED;
        if charged {
            QosQuota::uncharge(&mut usage, &self.charge);
        }
        // Queue metadata may outlive the real permit during Drop. Even an
        // uncharged owner must retire before another wake can reserve it.
        drop(usage);
        if charged {
            self.quota.wake_queue();
        }
    }
}

pub(crate) enum QosPermit {
    Admitted {
        quota: Arc<QosQuota>,
        charge: QosCharge,
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
            Self::Admitted { quota, charge } => quota.release(charge),
            Self::Queued(reservation) => reservation.retire(),
        }
    }
}
