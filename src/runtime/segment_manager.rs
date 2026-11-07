//! Requester-side peer admission and reference-retained epoch fences.
//!
//! Unlike TENT's reversible handle invalidation, retirement is a membership
//! fence. A borrowed descriptor cannot reopen admission to a retired identity.

use crate::{Error, RemoteSegment, Result, SourceEpoch};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PeerKey {
    pub(crate) endpoint: SocketAddr,
    pub(crate) epoch: SourceEpoch,
}

impl From<&RemoteSegment> for PeerKey {
    fn from(peer: &RemoteSegment) -> Self {
        Self {
            endpoint: peer.endpoint,
            epoch: peer.epoch,
        }
    }
}

enum Cleanup {
    NotStarted,
    Running,
    Complete,
    Failed(Error),
}
struct State {
    open: bool,
    active: usize,
    cleanup: Cleanup,
    drain_uncertain: bool,
}
struct Entry {
    owner: Arc<()>,
    key: PeerKey,
    state: Mutex<State>,
    changed: Condvar,
}

pub(crate) struct SegmentManager {
    table: Mutex<Table>,
    owner: Arc<()>,
    capacity: usize,
    // Serializes native teardown only. Credit reconciliation remains callable
    // after global requester close; payload admission has its own fence.
    closing: AtomicBool,
}

#[derive(Default)]
struct Table {
    entries: HashMap<PeerKey, Arc<Entry>>,
    reclaimed: u64,
}
impl Table {
    fn reclaim(&mut self) {
        self.entries.retain(|_, entry| {
            // The table lock excludes open/close lookup. Admission can clone an
            // entry only through an already-live handle, which makes this count
            // greater than one. There is no Weak upgrade or key-only admission.
            if Arc::strong_count(entry) != 1 {
                return true;
            }
            let state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
            // A timed-out drain still owes native cleanup. Failed cleanup is
            // sticky even after every public handle and permit has disappeared.
            let reclaim =
                state.active == 0 && (state.open || matches!(state.cleanup, Cleanup::Complete));
            if reclaim {
                self.reclaimed = self.reclaimed.saturating_add(1);
            }
            !reclaim
        });
    }
}

#[derive(Clone)]
pub(crate) struct PeerHandle(Arc<Entry>);
impl std::fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PeerHandle").field(&self.0.key).finish()
    }
}

#[derive(Default, Debug, Eq, PartialEq)]
pub(crate) struct PeerStats {
    pub(crate) identities: usize,
    pub(crate) live: usize,
    pub(crate) retired: usize,
    pub(crate) unquiesced: usize,
    pub(crate) active: usize,
    pub(crate) reclaimed: u64,
}

/// Shared with notification retries; one charge lasts until the last owner drops.
pub(crate) struct PeerPermit(Arc<Entry>);

impl Drop for PeerPermit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        state.active -= 1;
        self.0.changed.notify_all();
    }
}

impl SegmentManager {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            table: Mutex::default(),
            owner: Arc::new(()),
            capacity,
            closing: AtomicBool::new(false),
        }
    }

    fn entry(&self, key: PeerKey) -> Result<Arc<Entry>> {
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        table.reclaim();
        if let Some(entry) = table.entries.get(&key) {
            return Ok(entry.clone());
        }
        if table.entries.len() >= self.capacity {
            return Err(Error::ResourceExhausted);
        }
        table
            .entries
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        let entry = Arc::new(Entry {
            owner: self.owner.clone(),
            key,
            state: Mutex::new(State {
                open: true,
                active: 0,
                cleanup: Cleanup::NotStarted,
                drain_uncertain: false,
            }),
            changed: Condvar::new(),
        });
        table.entries.insert(key, entry.clone());
        Ok(entry)
    }

    /// Only an authoritative open creates an admissible descriptor. Keeping
    /// this token (including through a clone) prevents tombstone reclamation.
    pub(crate) fn open(&self, key: PeerKey) -> Result<(PeerHandle, Arc<PeerPermit>)> {
        let handle = PeerHandle(self.entry(key)?);
        let permit = self.admit(&handle, key)?;
        Ok((handle, permit))
    }

    pub(crate) fn admit(&self, handle: &PeerHandle, key: PeerKey) -> Result<Arc<PeerPermit>> {
        let entry = &handle.0;
        if !Arc::ptr_eq(&self.owner, &entry.owner) || entry.key != key {
            return Err(Error::InvalidConfig(
                "open_segment on this requester with the authoritative endpoint and epoch",
            ));
        }
        {
            let mut state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.open {
                return Err(Error::PeerClosed);
            }
            state.active = state
                .active
                .checked_add(1)
                .ok_or(Error::ResourceExhausted)?;
        }
        Ok(Arc::new(PeerPermit(entry.clone())))
    }

    pub(crate) fn close(
        &self,
        key: PeerKey,
        deadline: Instant,
        cleanup: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let entry = self.entry(key)?;
        let mut state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
        state.open = false;
        entry.changed.notify_all();
        loop {
            match &state.cleanup {
                Cleanup::Complete => return Ok(()),
                Cleanup::Failed(e) => return Err(e.for_report()),
                Cleanup::NotStarted if state.active == 0 => break,
                _ => {}
            }
            let Some(wait) = deadline.checked_duration_since(Instant::now()) else {
                state.drain_uncertain = true;
                return Err(drain_timeout());
            };
            state = entry
                .changed
                .wait_timeout(state, wait)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        if self.closing.load(Ordering::Acquire) {
            return Err(Error::RequesterClosed);
        }
        state.cleanup = Cleanup::Running;
        drop(state);
        // No table or peer lock is held during native cleanup. Independent peers
        // keep making progress, and concurrent closers observe this one result.
        let mut completion = CleanupCompletion {
            entry,
            result: None,
        };
        let result = cleanup().map_err(|e| match e {
            Error::Unquiesced { .. } => e,
            Error::Timeout => drain_timeout(),
            _ => Error::Unquiesced {
                operation: "peer resource retirement",
                code: libc::EIO,
            },
        });
        completion.result = Some(result.as_ref().copied().map_err(Error::for_report));
        result
    }

    pub(crate) fn close_cleanup(&self, deadline: Instant) -> Result<()> {
        // Global requester close already fenced and drained payload admission.
        // Prevent a new selective cleanup from racing global transport teardown.
        self.closing.store(true, Ordering::Release);
        let entries: Vec<_> = self
            .table
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .values()
            .cloned()
            .collect();
        let mut failure = None;
        for entry in entries {
            let mut state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
            while matches!(state.cleanup, Cleanup::Running) {
                let wait = deadline
                    .checked_duration_since(Instant::now())
                    .ok_or_else(drain_timeout)?;
                state = entry
                    .changed
                    .wait_timeout(state, wait)
                    .unwrap_or_else(|p| p.into_inner())
                    .0;
            }
            if let Cleanup::Failed(e) = &state.cleanup {
                failure.get_or_insert_with(|| e.for_report());
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(crate) fn stats(&self) -> PeerStats {
        let mut table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        table.reclaim();
        let mut stats = PeerStats {
            identities: table.entries.len(),
            reclaimed: table.reclaimed,
            ..Default::default()
        };
        for entry in table.entries.values() {
            let state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
            stats.live += usize::from(state.open);
            stats.retired += usize::from(!state.open);
            stats.unquiesced +=
                usize::from(state.drain_uncertain || matches!(state.cleanup, Cleanup::Failed(_)));
            stats.active += state.active;
        }
        stats
    }
}

fn drain_timeout() -> Error {
    Error::Unquiesced {
        operation: "peer retirement drain",
        code: libc::ETIMEDOUT,
    }
}

struct CleanupCompletion {
    entry: Arc<Entry>,
    result: Option<Result<()>>,
}
impl Drop for CleanupCompletion {
    fn drop(&mut self) {
        let mut state = self.entry.state.lock().unwrap_or_else(|p| p.into_inner());
        state.cleanup = match self.result.take() {
            Some(Ok(())) => Cleanup::Complete,
            Some(Err(e)) => Cleanup::Failed(e),
            None => Cleanup::Failed(Error::Unquiesced {
                operation: "peer retirement interrupted",
                code: libc::EIO,
            }),
        };
        if matches!(state.cleanup, Cleanup::Complete) {
            state.drain_uncertain = false;
        }
        self.entry.changed.notify_all();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/segment_manager.rs"]
mod tests;
