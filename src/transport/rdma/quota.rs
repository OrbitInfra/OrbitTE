use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use crate::{Error, MemType, MemoryLocation, Result, Topology};

mod bandwidth;
mod scheduling;
pub(super) use bandwidth::DeviceInfo;
pub use bandwidth::NicLoadStats;
pub use scheduling::SchedulingParams;

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
    priorities: BTreeMap<MemoryLocation, DevicePriority>,
    fallback: DevicePriority,
    unlisted_cpu: DevicePriority,
    state: Mutex<State>,
    devices: Vec<Arc<DeviceInfo>>,
    names: Vec<String>,
    scheduling: SchedulingParams,
}

/// Compiled once per location; selection performs no topology walks or allocation.
#[derive(Clone)]
pub(super) struct DevicePriority {
    pub(super) ranks: [u8; 64],
    pub(super) eligible: u64,
}

fn location_key(location: MemoryLocation) -> MemoryLocation {
    match location {
        MemoryLocation::Cuda { device, .. } => MemoryLocation::Cuda { device, context: 0 },
        cpu => cpu,
    }
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
    pub(super) fn device_info(&self) -> Arc<DeviceInfo> {
        Arc::clone(&self.selector.devices[self.device_id])
    }
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
    pub(super) fn new(
        topology: &Topology,
        available: u64,
        config: &super::RdmaConfig,
    ) -> Result<Arc<Self>> {
        config.scheduling.validate()?;
        let strict_local_numa = config.strict_local_numa;
        let count = topology.nic_list.len();
        if count > 64 {
            return Err(Error::InvalidConfig(
                "RDMA device masks support at most 64 topology NICs",
            ));
        }
        let all = u64::MAX.checked_shr((64 - count) as u32).unwrap_or(0);
        let mut priorities = BTreeMap::new();
        for memory in &topology.mem_list {
            let location = match memory.kind {
                MemType::Host if memory.name == "*" => Some(MemoryLocation::Cpu(None)),
                MemType::Host => memory
                    .name
                    .strip_prefix("cpu:")
                    .and_then(|node| node.parse().ok())
                    .map(|node| MemoryLocation::Cpu(Some(node))),
                MemType::Cuda => memory
                    .name
                    .strip_prefix("cuda:")
                    .and_then(|device| device.parse::<i32>().ok())
                    .filter(|device| *device >= 0)
                    .map(|device| MemoryLocation::Cuda { device, context: 0 }),
            }
            .ok_or(Error::InvalidConfig(
                "invalid RDMA topology memory location",
            ))?;
            let mut priority = DevicePriority {
                ranks: [2; 64],
                eligible: 0,
            };
            let mut seen = 0;
            for (rank, devices) in memory.device_list.iter().enumerate() {
                for &id in devices {
                    if id >= count || seen & (1u64 << id) != 0 {
                        return Err(Error::InvalidConfig(
                            "invalid or duplicate RDMA topology NIC tier",
                        ));
                    }
                    seen |= 1u64 << id;
                    priority.eligible |= 1u64 << id;
                    priority.ranks[id] = rank as u8;
                }
            }
            let node = match location {
                MemoryLocation::Cpu(node) => node,
                MemoryLocation::Cuda { .. } => memory.numa_node,
            };
            if strict_local_numa {
                for (id, nic) in topology.nic_list.iter().enumerate() {
                    if node.zip(nic.numa_node).is_some_and(|(a, b)| a != b) {
                        priority.eligible &= !(1u64 << id);
                    }
                }
            }
            if priorities.insert(location, priority).is_some() {
                return Err(Error::InvalidConfig(
                    "duplicate RDMA topology memory location",
                ));
            }
        }
        let wildcard = priorities.get(&MemoryLocation::Cpu(None)).cloned();
        let fallback = wildcard.clone().unwrap_or(DevicePriority {
            ranks: [2; 64],
            eligible: all,
        });
        // CPU owners and explicit hints already carry a NUMA node, even when
        // CPU sysfs omitted its row. Compile every possible known-NIC match;
        // any other known CPU node can only use unknown NICs in strict mode.
        for node in topology.nic_list.iter().filter_map(|nic| nic.numa_node) {
            priorities
                .entry(MemoryLocation::Cpu(Some(node)))
                .or_insert_with(|| {
                    let mut priority = fallback.clone();
                    for (id, nic) in topology.nic_list.iter().enumerate() {
                        if wildcard.is_none() && nic.numa_node == Some(node) {
                            priority.ranks[id] = 0;
                        }
                        if strict_local_numa && nic.numa_node.is_some_and(|other| other != node) {
                            priority.eligible &= !(1u64 << id);
                        }
                    }
                    priority
                });
        }
        let unknown_nics = topology
            .nic_list
            .iter()
            .enumerate()
            .filter(|(_, nic)| nic.numa_node.is_none())
            .fold(0, |mask, (id, _)| mask | (1u64 << id));
        Ok(Arc::new(Self {
            devices: (0..count)
                .map(|_| Arc::new(DeviceInfo::new(config.scheduling.clone())))
                .collect(),
            names: topology
                .nic_list
                .iter()
                .map(|nic| nic.name.clone())
                .collect(),
            scheduling: config.scheduling.clone(),
            priorities,
            unlisted_cpu: DevicePriority {
                ranks: fallback.ranks,
                eligible: fallback.eligible & if strict_local_numa { unknown_nics } else { all },
            },
            fallback,
            state: Mutex::new(State {
                available: available & all,
                inflight: vec![0; count],
                cursor: 0,
            }),
        }))
    }

    pub(super) fn priority(&self, location: MemoryLocation) -> &DevicePriority {
        self.priorities
            .get(&location_key(location))
            .unwrap_or(match location {
                MemoryLocation::Cpu(Some(_)) => &self.unlisted_cpu,
                _ => &self.fallback,
            })
    }

    pub(super) fn set_available(&self, id: usize, available: bool) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if available {
            state.available |= 1u64 << id;
        } else {
            if state.available & (1u64 << id) != 0 {
                self.devices[id].invalidate();
            }
            state.available &= !(1u64 << id);
        }
    }

    pub(super) fn set_bandwidth(&self, id: usize, mbps: Option<u64>) {
        self.devices[id].set_bandwidth(mbps);
    }

    pub(super) fn stats(&self) -> Vec<NicLoadStats> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        self.devices
            .iter()
            .enumerate()
            .filter(|(id, _)| state.available & (1u64 << id) != 0)
            .map(|(id, info)| info.snapshot(id, &self.names[id], state.inflight[id]))
            .collect()
    }

    pub(super) fn allocate(
        self: &Arc<Self>,
        length: usize,
        location: MemoryLocation,
        mask: u64,
    ) -> Result<DeviceLease> {
        if length == 0 {
            return Err(Error::InvalidRange);
        }
        let priority = self.priority(location);
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA selector lock poisoned"))?;
        let count = state.inflight.len();
        let chosen = (0..count)
            .filter(|id| mask & state.available & priority.eligible & (1u64 << id) != 0)
            .min_by(|a, b| {
                let score = |id: usize| {
                    if self.scheduling.smart_selection {
                        (state.inflight[id] as f64 + length as f64) / self.devices[id].bandwidth()
                            * self.scheduling.numa_tier_weights[priority.ranks[id] as usize]
                    } else {
                        f64::from(priority.ranks[id])
                    }
                };
                score(*a).total_cmp(&score(*b)).then_with(|| {
                    ((a + count - state.cursor) % count).cmp(&((b + count - state.cursor) % count))
                })
            })
            .ok_or(Error::Unavailable(
                "no enabled RDMA device matches location and rail mask",
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
