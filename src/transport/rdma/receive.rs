use std::sync::atomic::{AtomicU8, Ordering};
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

/// Grant receipt identity is independent of the physical receive-buffer WR.
pub(super) struct ReceiveWork {
    pub(super) id: u64,
    pub(super) expected: [u8; COMPLETION_MESSAGE_SIZE],
    pub(super) rkey: u32,
    pub(super) deadline: Instant,
    state: AtomicU8,
}

impl ReceiveWork {
    pub(super) fn new(
        id: u64,
        expected: [u8; COMPLETION_MESSAGE_SIZE],
        rkey: u32,
        deadline: Instant,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            expected,
            rkey,
            deadline,
            state: AtomicU8::new(0),
        })
    }
    pub(super) fn posted(&self) -> bool {
        self.state.load(Ordering::Acquire) & 1 != 0
    }
    pub(super) fn completed(&self) -> bool {
        self.state.load(Ordering::Acquire) & 2 != 0
    }
    pub(super) fn mark_posted(&self) {
        self.state.fetch_or(1, Ordering::Release);
    }
    pub(super) fn mark_completed(&self) {
        self.state.fetch_or(2, Ordering::Release);
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/receive.rs"]
mod tests;
