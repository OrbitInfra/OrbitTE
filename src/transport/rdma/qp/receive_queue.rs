//! Physical RQ buffers and dispatch to independently identified grant receipts.

use std::sync::{Arc, Mutex};

use super::super::receive::{MessageBuffer, ReceiveCredits, ReceiveWork};
use super::super::{COMPLETION_MESSAGE_SIZE, RdmaContext, RdmaStats, sys};
use crate::{Error, Result};

struct Slot {
    id: u64,
    donor: Arc<ReceiveWork>,
    message: MessageBuffer,
    credits: Option<ReceiveCredits>,
    pending: bool,
    posted: bool,
}

pub(in crate::transport::rdma) struct ReceiveQueue {
    slots: Vec<Slot>,
    receipts: Vec<Arc<ReceiveWork>>,
    limit: usize,
    usage: Arc<Mutex<RdmaStats>>,
    fenced: bool,
}

impl ReceiveQueue {
    pub(in crate::transport::rdma) fn new(
        limit: usize,
        usage: Arc<Mutex<RdmaStats>>,
    ) -> Result<Self> {
        if !(1..=64).contains(&limit) {
            return Err(Error::InvalidConfig(
                "RDMA QP receive depth must be in 1..=64",
            ));
        }
        let mut slots = Vec::new();
        let mut receipts = Vec::new();
        slots
            .try_reserve_exact(limit)
            .map_err(|_| Error::ResourceExhausted)?;
        receipts
            .try_reserve_exact(limit)
            .map_err(|_| Error::ResourceExhausted)?;
        Ok(Self {
            slots,
            receipts,
            limit,
            usage,
            fenced: false,
        })
    }

    pub(in crate::transport::rdma) fn prepare(
        &mut self,
        work: Arc<ReceiveWork>,
        credits: ReceiveCredits,
        device: &RdmaContext,
        message_capacity: usize,
    ) -> Result<()> {
        if self.fenced {
            return Err(Error::Unavailable("RDMA QP receive queue is fenced"));
        }
        if self.slots.len() >= self.limit || self.receipts.len() >= self.limit {
            return Err(Error::ResourceExhausted);
        }
        if work.id == 0
            || self.slots.iter().any(|slot| slot.id == work.id)
            || self.receipts.iter().any(|entry| {
                entry.id == work.id || entry.rkey == work.rkey || entry.expected == work.expected
            })
        {
            return Err(Error::Protocol(
                "RDMA duplicate receive identity or authority",
            ));
        }
        let message = MessageBuffer::new(Arc::clone(&self.usage), message_capacity)?;
        // Publish before registration: even a failing provider may create an MR.
        self.receipts.push(Arc::clone(&work));
        self.slots.push(Slot {
            id: work.id,
            donor: work,
            message,
            credits: Some(credits),
            pending: false,
            posted: false,
        });
        let slot = self
            .slots
            .last_mut()
            .ok_or(Error::Internal("missing RDMA receive slot"))?;
        let registered = device.register_message(
            &mut slot.message.handles,
            slot.message.memory.address,
            slot.message.memory.length,
        );
        slot.message.registered();
        if registered.is_err() {
            self.fence();
        }
        registered
    }

    pub(in crate::transport::rdma) fn post(
        &mut self,
        id: u64,
        capacity: usize,
        handles: &mut sys::ConnectionHandles,
        verbs: &sys::Verbs,
    ) -> Result<()> {
        if self.fenced {
            return Err(Error::Unavailable("RDMA QP receive queue is fenced"));
        }
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.id == id)
            .ok_or(Error::Internal("missing RDMA receive slot"))?;
        if slot.pending || slot.posted || slot.credits.is_none() {
            return Err(Error::Internal("RDMA receive state"));
        }
        {
            let mut usage = self
                .usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            if usage.receive_work_requests >= capacity {
                return Err(Error::ResourceExhausted);
            }
            usage.receive_work_requests += 1;
            usage.peak_receive_work_requests = usage
                .peak_receive_work_requests
                .max(usage.receive_work_requests);
            usage.receive_post_calls += 1;
        }
        slot.pending = true;
        let mut result = sys::PostResult::default();
        // SAFETY: this QP owns the registered slot until its CQE or native drain.
        let code = unsafe {
            (verbs.post_receive)(
                handles,
                &slot.message.handles,
                slot.id,
                slot.message.memory.address.as_ptr() as u64,
                slot.message.memory.length,
                &mut result,
            )
        };
        if result.uncertain || result.accepted > 1 || (code == 0 && result.accepted != 1) {
            self.fenced = true;
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .uncertain_posts += 1;
            return Err(Error::Backend {
                operation: "ibv_post_recv uncertain",
                code: if code == 0 { libc::EIO } else { code },
            });
        }
        if code != 0 {
            self.fenced = true;
            if result.accepted == 0 {
                Self::discharge(slot, &self.usage);
            }
            return Err(Error::Backend {
                operation: "ibv_post_recv",
                code,
            });
        }
        slot.posted = true;
        slot.donor.mark_posted();
        Ok(())
    }

    pub(in crate::transport::rdma) fn complete(
        &mut self,
        completion: sys::Completion,
    ) -> Result<Arc<ReceiveWork>> {
        let result = self.dispatch(completion);
        if result.is_err() {
            self.fence();
        }
        result
    }

    fn dispatch(&mut self, completion: sys::Completion) -> Result<Arc<ReceiveWork>> {
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| slot.id == completion.id && slot.pending && slot.posted)
            .ok_or(Error::Protocol("RDMA receive identity or duplicate"))?;
        if completion.status != 0 {
            Self::discharge(slot, &self.usage);
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .completion_errors += 1;
            return Err(Error::Backend {
                operation: "RDMA receive completion",
                code: completion.status as i32,
            });
        }
        if completion.opcode != 128
            || completion.length != COMPLETION_MESSAGE_SIZE as u32
            || completion.flags != sys::WITH_INVALIDATE
        {
            return Err(Error::Protocol("RDMA receive opcode/length/invalidation"));
        }
        // Only this actual successful CQE permits reading these bytes. The RQ
        // slot donor need not be the grant named by the received binding/key.
        let receipt = self
            .receipts
            .iter()
            .find(|work| {
                work.posted()
                    && !work.completed()
                    && work.rkey == completion.invalidated_rkey
                    && work.expected == slot.message.memory.bytes()
            })
            .ok_or(Error::Protocol(
                "RDMA received grant binding or key mismatch",
            ))?;
        Self::discharge(slot, &self.usage);
        receipt.mark_completed();
        Ok(Arc::clone(receipt))
    }

    fn discharge(slot: &mut Slot, usage: &Mutex<RdmaStats>) {
        if slot.pending {
            usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .receive_work_requests -= 1;
            slot.pending = false;
        }
        drop(slot.credits.take());
    }

    pub(in crate::transport::rdma) fn outstanding(&self) -> usize {
        self.slots.iter().filter(|slot| slot.pending).count()
    }

    pub(in crate::transport::rdma) fn memory_usage(&self) -> (usize, usize) {
        (
            self.slots
                .iter()
                .filter(|slot| !slot.message.handles.mr.is_null())
                .count(),
            self.slots.len() * COMPLETION_MESSAGE_SIZE,
        )
    }

    pub(in crate::transport::rdma) fn release(
        &mut self,
        work: &Arc<ReceiveWork>,
        verbs: &sys::Verbs,
    ) -> std::result::Result<(), (&'static str, i32)> {
        if !work.completed() || !self.receipts.iter().any(|entry| Arc::ptr_eq(entry, work)) {
            return Err(("RDMA receive lacks grant receipt", libc::EBUSY));
        }
        self.reap(verbs)?;
        self.receipts.retain(|entry| !Arc::ptr_eq(entry, work));
        Ok(())
    }

    fn reap(&mut self, verbs: &sys::Verbs) -> std::result::Result<(), (&'static str, i32)> {
        let mut index = 0;
        while index < self.slots.len() {
            if self.slots[index].pending || self.slots[index].credits.is_some() {
                index += 1;
                continue;
            }
            self.slots[index].message.close(verbs)?;
            self.slots.swap_remove(index);
        }
        Ok(())
    }

    pub(in crate::transport::rdma) fn fence(&mut self) {
        self.fenced = true;
    }

    // Called only after all associated QPs and both CQs are destroyed.
    pub(in crate::transport::rdma) fn quiesce(&mut self) {
        self.fence();
        for slot in &mut self.slots {
            Self::discharge(slot, &self.usage);
        }
    }

    pub(in crate::transport::rdma) fn close(
        &mut self,
        verbs: &sys::Verbs,
    ) -> std::result::Result<(), (&'static str, i32)> {
        self.fence();
        if self.outstanding() != 0 {
            return Err(("RDMA receive before native drain", libc::EBUSY));
        }
        // Unposted buffers have no DMA debt, but still hold parent admission.
        for slot in &mut self.slots {
            Self::discharge(slot, &self.usage);
        }
        self.reap(verbs)?;
        self.receipts.clear();
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/receive_queue.rs"]
mod tests;
