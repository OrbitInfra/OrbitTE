//! Physical permits for a prepared submission. The executing batch worker
//! alone commits credit and returns each dispatch slot after physical drain.
use super::{PreparedSubmission, PreparedTarget, TransferEngine, remaining};
use crate::runtime::admission_queue::{QueueInput, QueueOwnerKind, QueuePermit, QueueTicket};
use crate::runtime::qos_contract::{CompiledContract, QosCharge, QosPermit};
use crate::runtime::receiver_credit::CreditPermit;
use crate::runtime::request_merge::TaskMap;
use crate::{BatchId, Error, Result, TransferRequest, Transport};
use std::sync::{Arc, Mutex};
use std::time::Instant;

impl TransferEngine {
    pub(super) fn admit_physical(
        &self,
        payload: &PreparedTarget,
        mapping: &TaskMap,
        requests: &[TransferRequest],
        contracts: &[Option<&CompiledContract>],
        (batch, deadline): (Option<BatchId>, Instant),
        notification_bytes: usize,
    ) -> Result<Vec<OwnerAdmission>> {
        let mut owners = Vec::with_capacity(mapping.owners.len());
        let mut inputs = Vec::new();
        let mut qos_permits = match &self.qos {
            Some(resolver) => resolver.admit(
                mapping
                    .owners
                    .iter()
                    .map(|o| contracts[o.representative].and_then(|c| QosCharge::new(c, o.length)))
                    .collect(),
            )?,
            None => Vec::new(),
        }
        .into_iter();
        let mut credits = match (payload, &self.receiver_credits) {
            (PreparedTarget::Memory { peer, .. }, Some(ledger)) if !mapping.owners.is_empty() => {
                let charges: Vec<_> = mapping
                    .owners
                    .iter()
                    .map(|o| [o.length as u64, 1])
                    .collect();
                ledger.prepare_owners(peer, &charges)?
            }
            _ => Vec::new(),
        }
        .into_iter();
        for (i, owner) in mapping.owners.iter().enumerate() {
            let first = owner.representative;
            let qos = qos_permits.next().flatten();
            let credit = credits.next();
            let owner_deadline = requests[first]
                .deadline
                .map_or(deadline, |d| d.min(deadline));
            if self.runtime_queue.is_some() {
                let (kind, rate) = match payload {
                    PreparedTarget::Memory { requests: ts, .. }
                        if requests[first].remote_staging =>
                    {
                        // The concrete ProxyManager session carries this owner's
                        // permit across PRE/CROSS/POST. A caller's intent alone
                        // cannot enter the internal reserve.
                        debug_assert!(matches!(
                            ts[i].selection.transport,
                            Transport::Tcp | Transport::Rdma
                        ));
                        (QueueOwnerKind::StagingInternal, None)
                    }
                    PreparedTarget::Memory { requests: ts, .. }
                        if ts[i].selection.transport == Transport::Rdma =>
                    {
                        (
                            QueueOwnerKind::User,
                            Some((Transport::Rdma, ts[i].selection.device_mask)),
                        )
                    }
                    _ => (QueueOwnerKind::User, None),
                };
                inputs.push(QueueInput {
                    length: owner.length,
                    bytes: owner.length + if i == 0 { notification_bytes } else { 0 },
                    deadline: owner_deadline,
                    scheduling_deadline: Some(owner_deadline),
                    kind,
                    rate,
                    credit: credit.as_ref().map(|c| c.reservation.clone()),
                    qos: qos.as_ref().and_then(QosPermit::reservation),
                });
            }
            owners.push(OwnerAdmission {
                deadline: owner_deadline,
                permits: Mutex::new(Some(OwnerPermits {
                    _qos: qos,
                    queue: None,
                    credit,
                })),
            });
        }
        if let Some(queue) = &self.runtime_queue {
            for (owner, permit) in owners.iter_mut().zip(queue.admit(inputs, batch)?) {
                if let Some(p) = owner.permits.get_mut().unwrap_or_else(|e| e.into_inner()) {
                    p.queue = Some(permit);
                }
            }
        }
        Ok(owners)
    }
}

// Drop QoS before advancing the queue, and retire queue metadata before
// receiver-credit rollback can wake another waiter.
struct OwnerPermits {
    _qos: Option<QosPermit>,
    queue: Option<QueuePermit>,
    credit: Option<CreditPermit>,
}
pub(super) struct OwnerAdmission {
    pub(super) deadline: Instant,
    permits: Mutex<Option<OwnerPermits>>,
}
impl OwnerAdmission {
    pub(super) fn start(&self, cancelled: impl Fn() -> bool + Sync) -> Result<()> {
        let permits = self.permits.lock().unwrap_or_else(|e| e.into_inner());
        let p = permits
            .as_ref()
            .ok_or(Error::Internal("physical owner already retired"))?;
        if let Some(q) = &p.queue {
            q.wait(cancelled)?;
        } else if let Some(c) = &p.credit {
            c.reservation.wait(self.deadline, cancelled)?;
        }
        remaining(self.deadline).map(|_| ())
    }
    pub(super) fn credit(&self) -> Option<Arc<crate::runtime::receiver_credit::CreditReservation>> {
        self.permits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()?
            .credit
            .as_ref()
            .map(|c| c.reservation.clone())
    }
}
impl PreparedSubmission {
    pub(super) fn retain_window(&self, range: std::ops::Range<usize>) {
        *self.uncertain.lock().unwrap_or_else(|e| e.into_inner()) = Some(range);
    }
    pub(super) fn retain_uncertain_owners(&mut self) {
        let Some(range) = self
            .uncertain
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        else {
            return;
        };
        // The entire native cohort stays pinned, including a CUDA/file prefix
        // that shares a stream, ring or cuFile batch with an uncertain suffix.
        #[allow(unused_mut)] // cuFile can also retain an uncertain deregistration.
        let mut keep: Vec<_> = (0..self.owners.len()).map(|i| range.contains(&i)).collect();
        match &mut self.payload {
            PreparedTarget::Memory { requests, .. } => {
                let mut i = 0;
                requests.retain(|_| {
                    let keep = keep[i];
                    i += 1;
                    keep
                });
            }
            #[cfg(any(feature = "gds", feature = "io-uring"))]
            PreparedTarget::File { requests, .. } => {
                let mut i = 0;
                requests.retain(|_r| {
                    #[cfg(feature = "gds")]
                    if !keep[i] && _r.buffer.as_ref().is_some_and(|b| b.release().is_err()) {
                        keep[i] = true;
                    }
                    let keep = keep[i];
                    i += 1;
                    keep
                });
            }
        }
        let mut bytes = 0;
        for (i, owned) in keep.into_iter().enumerate() {
            if owned {
                bytes += self.mapping.owners[i].length;
            } else {
                self.owners[i]
                    .permits
                    .get_mut()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
            }
        }
        if let Some(admission) = &mut self.admission {
            admission.retain_bytes(bytes);
        }
        self.notification = None;
    }
    pub(crate) fn running_cancel(&self, _i: usize) -> crate::batch::RunningCancel {
        use crate::batch::RunningCancel;
        match &self.payload {
            PreparedTarget::Memory {
                requests: _requests,
                ..
            } => {
                #[cfg(feature = "rdma")]
                if let Some(flag) = &_requests[_i].rdma_cancel {
                    return RunningCancel::Rdma(flag.clone());
                }
                RunningCancel::Unsupported
            }
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            PreparedTarget::File { .. } => RunningCancel::File,
        }
    }
    pub(crate) fn ticket(&self, i: usize) -> Option<QueueTicket> {
        self.owners[i]
            .permits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()?
            .queue
            .as_ref()
            .map(|q| q.ticket.clone())
    }
    #[cfg(feature = "cuda")]
    pub(super) fn native_group(&self, first: usize) -> usize {
        if self.ticket(first).is_some() || self.owners[first].credit().is_some() {
            return 1;
        }
        self.owners[first..]
            .iter()
            .take_while(|o| o.deadline == self.owners[first].deadline)
            .count()
    }
    pub(super) fn retire_owners(&self, first: usize, count: usize, completed: usize) {
        for (i, o) in self.owners[first..first + count].iter().enumerate() {
            let mut permits = o.permits.lock().unwrap_or_else(|e| e.into_inner());
            if i < completed {
                if let Some(c) = permits.as_ref().and_then(|p| p.credit.as_ref()) {
                    c.reservation.complete();
                }
            }
            // The final owner follows its PreparedSubmission through teardown.
            if first + i + 1 != self.owners.len() {
                drop(permits.take());
            }
        }
    }
}
