use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::workers::Record;
use crate::{Error, Result};

/// Strong native owners remain charged after lookup detachment and through retirement.
pub(super) struct EndpointStore {
    live: Vec<Arc<Record>>,
    retiring: Vec<Arc<Record>>,
    capacity: usize,
}

impl EndpointStore {
    pub(super) fn new(capacity: usize) -> Result<Self> {
        let mut live = Vec::new();
        let mut retiring = Vec::new();
        live.try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        retiring
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        Ok(Self {
            live,
            retiring,
            capacity,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.live.len() + self.retiring.len()
    }

    pub(super) fn publish(&mut self, endpoint: Arc<Record>) -> Result<()> {
        if self.len() >= self.capacity || endpoint.terminal.load(Ordering::Acquire) {
            return Err(Error::ResourceExhausted);
        }
        self.live.push(endpoint);
        Ok(())
    }

    pub(super) fn detach(&mut self, endpoint: &Arc<Record>) {
        endpoint.terminal.store(true, Ordering::Release);
        if let Some(index) = self
            .live
            .iter()
            .position(|entry| Arc::ptr_eq(entry, endpoint))
        {
            self.retiring.push(self.live.swap_remove(index));
        }
    }

    pub(super) fn detach_all(&mut self) {
        for endpoint in &self.live {
            endpoint.terminal.store(true, Ordering::Release);
        }
        self.retiring.append(&mut self.live);
    }

    pub(super) fn remove_retired(&mut self, endpoint: &Arc<Record>) {
        self.retiring.retain(|entry| !Arc::ptr_eq(entry, endpoint));
    }

    pub(super) fn work_owner(&self, tag: u64) -> Option<Arc<Record>> {
        self.live
            .iter()
            .chain(&self.retiring)
            .find(|entry| entry.tag == tag)
            .cloned()
    }

    pub(super) fn snapshot(&self, records: &mut Vec<Arc<Record>>) {
        records.extend(self.live.iter().chain(&self.retiring).cloned());
    }

    pub(super) fn take_all(&mut self) -> Vec<Arc<Record>> {
        self.live.append(&mut self.retiring);
        std::mem::take(&mut self.live)
    }

    pub(super) fn clear_after_drain(&mut self) {
        self.live.clear();
        self.retiring.clear();
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/endpoint_store.rs"]
mod tests;
