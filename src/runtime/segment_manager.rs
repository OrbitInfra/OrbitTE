//! Requester-side peer admission and permanent epoch tombstones.
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
}
struct Entry {
    state: Mutex<State>,
    changed: Condvar,
}

pub(crate) struct SegmentManager {
    entries: Mutex<HashMap<PeerKey, Arc<Entry>>>,
    capacity: usize,
    closing: AtomicBool,
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
            entries: Mutex::default(),
            capacity,
            closing: AtomicBool::new(false),
        }
    }

    fn entry(&self, key: PeerKey) -> Result<Arc<Entry>> {
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = entries.get(&key) {
            return Ok(entry.clone());
        }
        // Tombstones are charged for the engine's lifetime. Eviction would
        // silently make a stale descriptor usable again.
        if entries.len() >= self.capacity {
            return Err(Error::ResourceExhausted);
        }
        entries
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        let entry = Arc::new(Entry {
            state: Mutex::new(State {
                open: true,
                active: 0,
                cleanup: Cleanup::NotStarted,
            }),
            changed: Condvar::new(),
        });
        entries.insert(key, entry.clone());
        Ok(entry)
    }

    pub(crate) fn admit(&self, key: PeerKey) -> Result<Arc<PeerPermit>> {
        let entry = self.entry(key)?;
        {
            let mut state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
            if self.closing.load(Ordering::Acquire) {
                return Err(Error::RequesterClosed);
            }
            if !state.open {
                return Err(Error::PeerClosed);
            }
            state.active = state
                .active
                .checked_add(1)
                .ok_or(Error::ResourceExhausted)?;
        }
        Ok(Arc::new(PeerPermit(entry)))
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
            let wait = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(drain_timeout)?;
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
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
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

    pub(crate) fn stats(&self) -> (usize, usize, usize) {
        let entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        let (mut retired, mut active) = (0, 0);
        for entry in entries.values() {
            let state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
            retired += usize::from(!state.open);
            active += state.active;
        }
        (entries.len(), retired, active)
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
        self.entry.changed.notify_all();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/segment_manager.rs"]
mod tests;
