use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::{COMPLETION_MESSAGE_SIZE, RdmaStats, Staging, sys};
use crate::{Error, Result};

/// Separate local-only bytes: READ destinations must never alias SEND payloads.
pub(super) struct MessageBuffer {
    pub(super) memory: Staging,
    pub(super) handles: sys::MemoryHandles,
    usage: Arc<Mutex<RdmaStats>>,
}

impl MessageBuffer {
    pub(super) fn new(usage: Arc<Mutex<RdmaStats>>, capacity: usize) -> Result<Self> {
        {
            let mut stats = usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            let next = stats
                .message_bytes
                .checked_add(COMPLETION_MESSAGE_SIZE)
                .ok_or(Error::ResourceExhausted)?;
            let limit = capacity
                .checked_mul(COMPLETION_MESSAGE_SIZE)
                .ok_or(Error::ResourceExhausted)?;
            if next > limit {
                return Err(Error::ResourceExhausted);
            }
            stats.message_bytes = next;
        }
        let memory = match Staging::new(COMPLETION_MESSAGE_SIZE) {
            Ok(memory) => memory,
            Err(error) => {
                usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .message_bytes -= COMPLETION_MESSAGE_SIZE;
                return Err(error);
            }
        };
        Ok(Self {
            memory,
            handles: sys::MemoryHandles::default(),
            usage,
        })
    }

    pub(super) fn registered(&self) {
        if !self.handles.mr.is_null() {
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .active_message_mrs += 1;
        }
    }

    pub(super) fn close(
        &mut self,
        verbs: &sys::Verbs,
    ) -> std::result::Result<(), (&'static str, i32)> {
        if !self.handles.mr.is_null() {
            let code = unsafe { (verbs.deregister)(&mut self.handles) };
            if code != 0 {
                return Err(("ibv_dereg_mr completion message", code));
            }
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .active_message_mrs -= 1;
        }
        Ok(())
    }
}

impl Drop for MessageBuffer {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .message_bytes -= COMPLETION_MESSAGE_SIZE;
    }
}

/// Reserves a whole parent's completion RQ before any of its grants are exposed.
pub(super) struct ReceiveCredits {
    usage: Arc<Mutex<RdmaStats>>,
    remaining: usize,
}

impl ReceiveCredits {
    pub(super) fn acquire(
        usage: Arc<Mutex<RdmaStats>>,
        capacity: usize,
        count: usize,
    ) -> Result<Self> {
        if count == 0 || count > capacity {
            return Err(Error::InvalidRange);
        }
        {
            let mut stats = usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            let next = stats.receive_credits.checked_add(count);
            if next.is_none_or(|next| next > capacity) {
                stats.receive_admission_rejections += 1;
                return Err(Error::ResourceExhausted);
            }
            stats.receive_credits = next.ok_or(Error::ResourceExhausted)?;
            stats.peak_receive_credits = stats.peak_receive_credits.max(stats.receive_credits);
        }
        Ok(Self {
            usage,
            remaining: count,
        })
    }

    pub(super) fn take(&mut self) -> Result<Self> {
        if self.remaining == 0 {
            return Err(Error::Internal("RDMA parent exhausted receive credits"));
        }
        self.remaining -= 1;
        Ok(Self {
            usage: Arc::clone(&self.usage),
            remaining: 1,
        })
    }
}

impl Drop for ReceiveCredits {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .receive_credits -= self.remaining;
    }
}

pub(super) struct ReceiveQueue {
    pub(super) id: u64,
    pub(super) expected: [u8; COMPLETION_MESSAGE_SIZE],
    pub(super) rkey: u32,
    pub(super) deadline: Instant,
    pub(super) posted: bool,
    pub(super) completed: bool,
    pending: bool,
    pub(super) credits: Option<ReceiveCredits>,
    usage: Arc<Mutex<RdmaStats>>,
}

impl ReceiveQueue {
    pub(super) fn new(
        id: u64,
        expected: [u8; COMPLETION_MESSAGE_SIZE],
        rkey: u32,
        deadline: Instant,
        credits: ReceiveCredits,
    ) -> Self {
        Self {
            id,
            expected,
            rkey,
            deadline,
            posted: false,
            completed: false,
            pending: false,
            usage: Arc::clone(&credits.usage),
            credits: Some(credits),
        }
    }

    pub(super) fn reserve(&mut self, capacity: usize) -> Result<()> {
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        if self.posted || self.pending || self.completed || self.credits.is_none() {
            return Err(Error::Internal("RDMA receive state"));
        }
        if usage.receive_work_requests >= capacity {
            return Err(Error::ResourceExhausted);
        }
        self.pending = true;
        usage.receive_work_requests += 1;
        usage.peak_receive_work_requests = usage
            .peak_receive_work_requests
            .max(usage.receive_work_requests);
        Ok(())
    }

    pub(super) fn posted(&mut self, code: i32, result: sys::PostResult) -> Result<()> {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .receive_post_calls += 1;
        if result.uncertain || result.accepted > 1 || (code == 0 && result.accepted != 1) {
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
            if result.accepted == 0 {
                self.quiesce();
            }
            return Err(Error::Backend {
                operation: "ibv_post_recv",
                code,
            });
        }
        self.posted = true;
        Ok(())
    }

    pub(super) fn complete(&mut self, completion: sys::Completion) -> Result<()> {
        if !self.pending || !self.posted || completion.id != self.id {
            return Err(Error::Protocol("RDMA receive identity or duplicate"));
        }
        if completion.status != 0 {
            self.quiesce();
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
            || completion.invalidated_rkey != self.rkey
        {
            return Err(Error::Protocol("RDMA receive opcode/length/invalidation"));
        }
        self.quiesce();
        Ok(())
    }

    pub(super) fn pending(&self) -> bool {
        self.pending
    }
    pub(super) fn quiesce(&mut self) {
        if self.pending {
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .receive_work_requests -= 1;
            self.pending = false;
        }
        drop(self.credits.take());
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/receive.rs"]
mod tests;
