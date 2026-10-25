use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::cq_lane::Record;
use super::endpoint::Peer;
use crate::{Error, Result};

struct Idle {
    endpoint: Arc<Record>,
    peer: Peer,
    claim: u64,
}

/// CQ-local grant lookup retains shared physical QPs through native retirement.
pub(super) struct QpStore {
    live: Vec<Arc<Record>>,
    retiring: Vec<Arc<Record>>,
    capacity: usize,
    idle: Vec<Idle>,
}

impl QpStore {
    pub(super) fn new(capacity: usize) -> Result<Self> {
        let mut live = Vec::new();
        let mut retiring = Vec::new();
        live.try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        retiring
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut idle = Vec::new();
        idle.try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        Ok(Self {
            live,
            retiring,
            capacity,
            idle,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.live.len() + self.retiring.len()
    }

    pub(super) fn publish(&mut self, endpoint: Arc<Record>) -> Result<()> {
        endpoint.require_live()?;
        if self.len() >= self.capacity
            || endpoint.terminal.load(Ordering::Acquire)
            || self
                .live
                .iter()
                .chain(&self.retiring)
                .any(|entry| entry.tag == endpoint.tag)
        {
            return Err(Error::ResourceExhausted);
        }
        self.live.push(endpoint);
        Ok(())
    }

    pub(super) fn detach(&mut self, endpoint: &Arc<Record>) -> bool {
        let was_idle = self.idle.len();
        self.idle
            .retain(|entry| !Arc::ptr_eq(&entry.endpoint, endpoint));
        endpoint.terminal.store(true, Ordering::Release);
        endpoint.connection.fence();
        if let Some(index) = self
            .live
            .iter()
            .position(|entry| Arc::ptr_eq(entry, endpoint))
        {
            self.retiring.push(self.live.swap_remove(index));
        }
        self.idle.len() != was_idle
    }

    pub(super) fn affiliates(&self, record: &Arc<Record>) -> Vec<Arc<Record>> {
        if !self
            .live
            .iter()
            .chain(&self.retiring)
            .any(|entry| Arc::ptr_eq(entry, record))
        {
            return Vec::new();
        }
        self.live
            .iter()
            .chain(&self.retiring)
            .filter(|entry| Arc::ptr_eq(&entry.connection, &record.connection))
            .cloned()
            .collect()
    }

    pub(super) fn detach_all(&mut self) -> usize {
        let idle = self.idle.len();
        self.idle.clear();
        for endpoint in &self.live {
            endpoint.terminal.store(true, Ordering::Release);
            endpoint.connection.fence();
        }
        self.retiring.append(&mut self.live);
        idle
    }

    pub(super) fn remove_retired(&mut self, endpoint: &Arc<Record>) {
        self.retiring.retain(|entry| !Arc::ptr_eq(entry, endpoint));
    }

    pub(super) fn ready(&mut self, endpoint: &Arc<Record>, peer: Peer, claim: u64) -> Result<bool> {
        endpoint.require_live()?;
        if !self.live.iter().any(|entry| Arc::ptr_eq(entry, endpoint))
            || self
                .idle
                .iter()
                .any(|entry| Arc::ptr_eq(&entry.endpoint, endpoint))
        {
            return Err(Error::Internal("RDMA endpoint store READY ownership"));
        }
        let previous = self
            .idle
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.endpoint.connection, &endpoint.connection));
        let inserted = previous.is_none();
        if let Some(index) = previous {
            let old = self.idle.remove(index).endpoint;
            old.terminal.store(true, Ordering::Release);
            self.live.retain(|entry| !Arc::ptr_eq(entry, &old));
        }
        self.idle.push(Idle {
            endpoint: Arc::clone(endpoint),
            peer,
            claim,
        });
        Ok(inserted)
    }

    pub(super) fn claim(&mut self, matches: impl Fn(&Peer) -> bool) -> Option<(Arc<Record>, Peer)> {
        let index = self.idle.iter().position(|entry| matches(&entry.peer))?;
        let idle = self.idle.remove(index);
        Some((idle.endpoint, idle.peer))
    }

    pub(super) fn evict(&mut self) -> Option<Arc<Record>> {
        let idle = self.idle.first()?;
        let record = Arc::clone(&idle.endpoint);
        self.detach(&record);
        Some(record)
    }

    pub(super) fn retire_idle(&mut self, endpoint: &Arc<Record>, claim: u64) -> bool {
        if self
            .idle
            .iter()
            .any(|entry| Arc::ptr_eq(&entry.endpoint, endpoint) && entry.claim == claim)
        {
            self.detach(endpoint);
            true
        } else {
            false
        }
    }

    pub(super) fn work_owner(&self, tag: u64, receive: bool) -> Option<Arc<Record>> {
        self.live
            .iter()
            .chain(&self.retiring)
            .find(|entry| {
                tag != 0
                    && (if receive {
                        &entry.receive_namespace
                    } else {
                        &entry.send_namespace
                    })
                    .load(Ordering::Acquire)
                        == tag
            })
            .cloned()
    }

    pub(super) fn snapshot(&self, records: &mut Vec<Arc<Record>>) {
        records.extend(self.live.iter().chain(&self.retiring).cloned());
    }

    pub(super) fn take_all(&mut self) -> Vec<Arc<Record>> {
        self.idle.clear();
        self.live.append(&mut self.retiring);
        std::mem::take(&mut self.live)
    }

    pub(super) fn clear_after_drain(&mut self) {
        self.idle.clear();
        self.live.clear();
        self.retiring.clear();
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/qp_store.rs"]
mod tests;
