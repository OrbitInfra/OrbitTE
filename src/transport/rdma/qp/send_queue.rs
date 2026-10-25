//! Physical QP send depth and the exact WRs still owned by the provider.

use std::sync::{Arc, Mutex};

use super::super::{RdmaStats, sys};
use crate::{Error, Result};

struct Entry {
    id: u64,
    posted: bool,
}

struct State {
    entries: Vec<Entry>,
    drained: bool,
    fenced: bool,
}

pub(in crate::transport::rdma) struct SendQueue {
    limit: usize,
    state: Mutex<State>,
    pub(in crate::transport::rdma) usage: Arc<Mutex<RdmaStats>>,
}

impl SendQueue {
    pub(in crate::transport::rdma) fn new(
        limit: usize,
        usage: Arc<Mutex<RdmaStats>>,
    ) -> Result<Arc<Self>> {
        if !(1..=64).contains(&limit) {
            return Err(Error::InvalidConfig("RDMA QP send depth must be in 1..=64"));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(limit)
            .map_err(|_| Error::ResourceExhausted)?;
        Ok(Arc::new(Self {
            limit,
            state: Mutex::new(State {
                entries,
                drained: false,
                fenced: false,
            }),
            usage,
        }))
    }

    pub(in crate::transport::rdma) fn reserve(
        &self,
        requests: &[sys::WorkRequest],
        capacity: usize,
    ) -> Result<usize> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA SQ lock poisoned"))?;
        if state.drained || state.fenced {
            return Err(Error::Unavailable("RDMA QP send queue is fenced"));
        }
        if requests.is_empty() {
            return Err(Error::Internal("RDMA empty SQ reservation"));
        }
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        let count = requests
            .len()
            .min(self.limit.saturating_sub(state.entries.len()))
            .min(capacity.saturating_sub(usage.work_requests));
        if count == 0 {
            return Err(Error::ResourceExhausted);
        }
        for (index, request) in requests[..count].iter().enumerate() {
            if request.id == 0
                || requests[..index].iter().any(|other| other.id == request.id)
                || state.entries.iter().any(|entry| entry.id == request.id)
            {
                return Err(Error::Protocol("RDMA duplicate SQ work identity"));
            }
        }
        state
            .entries
            .extend(requests[..count].iter().map(|request| Entry {
                id: request.id,
                posted: false,
            }));
        usage.work_requests += count;
        usage.peak_work_requests = usage.peak_work_requests.max(usage.work_requests);
        Ok(count)
    }

    pub(in crate::transport::rdma) fn posted(
        &self,
        requests: &[sys::WorkRequest],
        code: i32,
        result: sys::PostResult,
    ) -> Result<usize> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA SQ lock poisoned"))?;
        if requests.is_empty()
            || requests.iter().any(|request| {
                !state
                    .entries
                    .iter()
                    .any(|entry| entry.id == request.id && !entry.posted)
            })
        {
            return Err(Error::Protocol("RDMA SQ post receipt has no reservation"));
        }
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        usage.post_calls += 1;
        let uncertain = result.uncertain
            || result.accepted > requests.len()
            || (code == 0 && result.accepted != requests.len());
        let owed = if uncertain {
            requests.len()
        } else {
            result.accepted
        };
        for request in &requests[..owed] {
            // The whole reservation was checked before changing either budget.
            if let Some(entry) = state
                .entries
                .iter_mut()
                .find(|entry| entry.id == request.id)
            {
                entry.posted = true;
            }
        }
        state.entries.retain(|entry| {
            !requests[owed..]
                .iter()
                .any(|request| request.id == entry.id)
        });
        usage.work_requests -= requests.len() - owed;
        if uncertain {
            usage.uncertain_posts += 1;
        } else {
            usage.posted_work_requests += result.accepted as u64;
            usage.partial_posts += u64::from(code != 0 && result.accepted != 0);
        }
        state.fenced |= code != 0 || uncertain;
        Ok(owed)
    }

    pub(in crate::transport::rdma) fn complete(&self, id: u64, failed: bool) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA SQ lock poisoned"))?;
        let index = state
            .entries
            .iter()
            .position(|entry| entry.id == id && entry.posted)
            .ok_or(Error::Protocol("RDMA completion has no posted SQ owner"))?;
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        state.entries.swap_remove(index);
        state.fenced |= failed;
        usage.work_requests -= 1;
        Ok(())
    }

    pub(in crate::transport::rdma) fn outstanding(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entries
            .len()
    }

    pub(in crate::transport::rdma) fn fence(&self) {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).fenced = true;
    }

    // Called only after every associated QP and both CQs were destroyed.
    // Dropping a grant or observing a timeout never returns these credits.
    pub(in crate::transport::rdma) fn quiesce(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        usage.work_requests -= state.entries.len();
        state.entries.clear();
        state.drained = true;
    }

    pub(in crate::transport::rdma) fn drained(&self) -> bool {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).drained
    }
}
