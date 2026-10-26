use std::sync::{Arc, Mutex, Weak};

use crate::{Error, Result, Topology};

/// Wakes context progress when aggregate SQ/RQ completion debt returns capacity.
#[derive(Default)]
pub(super) struct CqQuota {
    progress: Mutex<Vec<Weak<super::cq_lane::ProgressState>>>,
}

impl CqQuota {
    pub(super) fn attach(&self, state: &Arc<super::cq_lane::ProgressState>) {
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        progress.retain(|state| state.strong_count() != 0);
        progress.push(Arc::downgrade(state));
    }

    pub(super) fn released(&self) {
        let mut progress = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        progress.retain(|state| {
            if let Some(state) = state.upgrade() {
                state.wake_quota();
                true
            } else {
                false
            }
        });
    }
}

/// TENT's DeviceSelector boundary: stable NIC IDs, topology tiers and byte charges.
pub(super) struct DeviceSelector {
    topology: Topology,
    state: Mutex<State>,
}

struct State {
    available: u64,
    inflight: Vec<usize>,
    cursor: usize,
}

/// Lives with the actual grant, including retained native resources after failure.
pub(super) struct DeviceLease {
    selector: Arc<DeviceSelector>,
    device_id: usize,
    length: usize,
}

impl DeviceLease {
    pub(super) fn device_id(&self) -> usize {
        self.device_id
    }
    pub(super) fn length(&self) -> usize {
        self.length
    }
}

impl Drop for DeviceLease {
    fn drop(&mut self) {
        let mut state = self
            .selector
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state.inflight[self.device_id] -= self.length;
    }
}

impl DeviceSelector {
    pub(super) fn new(topology: Topology, available: u64) -> Arc<Self> {
        let count = topology.nic_list.len();
        Arc::new(Self {
            topology,
            state: Mutex::new(State {
                available,
                inflight: vec![0; count],
                cursor: 0,
            }),
        })
    }

    pub(super) fn set_available(&self, id: usize, available: bool) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if available {
            state.available |= 1u64 << id;
        } else {
            state.available &= !(1u64 << id);
        }
    }

    pub(super) fn allocate(
        self: &Arc<Self>,
        length: usize,
        numa: Option<u32>,
        mask: u64,
    ) -> Result<DeviceLease> {
        if length == 0 {
            return Err(Error::InvalidRange);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA selector lock poisoned"))?;
        let count = state.inflight.len();
        let memory = numa
            .and_then(|node| {
                self.topology
                    .mem_list
                    .iter()
                    .find(|entry| entry.numa_node == Some(node))
            })
            .or_else(|| {
                self.topology
                    .mem_list
                    .iter()
                    .find(|entry| entry.name == "*")
            });
        let chosen = (0..count)
            .filter(|id| mask & state.available & (1u64 << id) != 0)
            .min_by_key(|id| {
                let rank = memory
                    .and_then(|entry| entry.device_list.iter().position(|tier| tier.contains(id)))
                    .unwrap_or(2);
                (
                    rank,
                    state.inflight[*id],
                    (id + count - state.cursor) % count,
                )
            })
            .ok_or(Error::Unavailable(
                "no enabled RDMA device matches rail mask",
            ))?;
        state.inflight[chosen] = state.inflight[chosen]
            .checked_add(length)
            .ok_or(Error::ResourceExhausted)?;
        state.cursor = (chosen + 1) % count;
        Ok(DeviceLease {
            selector: Arc::clone(self),
            device_id: chosen,
            length,
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/quota.rs"]
mod tests;
