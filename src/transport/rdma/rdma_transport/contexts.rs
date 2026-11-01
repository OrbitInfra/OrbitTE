//! Transport initialization, context selection, lane recovery and shutdown.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::RdmaTransport;
use crate::engine::remaining;
use crate::transport::rdma::context_set::{ContextSet, ContextSlot};
use crate::transport::rdma::path::NicPath;
use crate::transport::rdma::quota::{DeviceAllocation, DeviceLease, DeviceSelector};
use crate::transport::rdma::{RdmaContext, cq_lane};
use crate::{Error, MemoryLocation, Result};

impl RdmaTransport {
    pub(in crate::transport::rdma) fn initialize_contexts(
        &self,
        contexts: &mut ContextSet,
    ) -> Result<()> {
        if contexts.closed {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        if let Some((operation, code)) = contexts.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        if contexts.topology.is_some() {
            return Ok(());
        }
        let (mut topology, candidates) = (self.verbs.discover)(&self.config)?;
        if topology.nic_list.len() > 64 {
            return Err(Error::InvalidConfig(
                "RDMA device masks support at most 64 topology NICs",
            ));
        }
        topology.load_from_config(&self.config)?;
        contexts
            .slots
            .resize_with(topology.nic_list.len(), ContextSlot::default);
        let selector = DeviceSelector::new(&topology, 0, &self.config)?;
        for (id, slot) in contexts.slots.iter_mut().enumerate() {
            let name = topology.nic_list[id].name.clone();
            for address in candidates.iter().filter(|address| address.device == name) {
                match RdmaContext::open_selected(
                    &self.config,
                    self.verbs,
                    Arc::clone(&self.usage),
                    Arc::clone(&self.counters),
                    address.clone(),
                    Arc::clone(&self.workers.monitor),
                ) {
                    Ok(owner) => {
                        if owner.asynchronous.check().is_ok() {
                            let address = owner.address()?;
                            address.publish(&mut topology)?;
                            selector.set_bandwidth(id, address.link_speed_mbps);
                            selector.set_available(id, true);
                        }
                        slot.owner = Some(owner);
                        break;
                    }
                    Err(Error::Unquiesced { operation, code }) => {
                        contexts.failure = Some((operation, code));
                        return Err(Error::Unquiesced { operation, code });
                    }
                    Err(_) => continue,
                }
            }
        }
        contexts.selector = Some(selector);
        contexts.topology = Some(topology);
        self.workers
            .monitor
            .watch_contexts(&self.context_set, &self.usage)?;
        Ok(())
    }

    pub(in crate::transport::rdma) fn paths(
        &self,
        location: MemoryLocation,
    ) -> Result<Vec<NicPath>> {
        self.recover_progress()?;
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        self.initialize_contexts(&mut contexts)?;
        let topology = contexts
            .topology
            .as_ref()
            .ok_or(Error::Internal("missing RDMA topology"))?;
        let location = self
            .config
            .preferred_numa_node
            .map_or(location, |node| MemoryLocation::Cpu(Some(node)));
        let priority = contexts
            .selector
            .as_ref()
            .ok_or(Error::Internal("missing RDMA device selector"))?
            .priority(location);
        let mut paths = Vec::new();
        for (id, slot) in contexts.slots.iter().enumerate() {
            if self.config.device_mask & priority.eligible & (1u64 << id) == 0 {
                continue;
            }
            if let Some(owner) = &slot.owner {
                if owner.asynchronous.check().is_err() {
                    continue;
                }
                let address = owner.address()?;
                paths.push(NicPath {
                    nic_id: id,
                    name: topology.nic_list[id].name.clone(),
                    domain: owner.domain()?,
                    gid_type: address.gid_type,
                    descriptor: address.descriptor,
                });
            }
        }
        if paths.is_empty() {
            return Err(Error::Unavailable(
                "no enabled RDMA context matches location and device mask",
            ));
        }
        paths.sort_unstable_by_key(|path| (priority.ranks[path.nic_id], path.nic_id));
        Ok(paths)
    }

    pub(in crate::transport::rdma) fn allocate(
        &self,
        length: usize,
        location: MemoryLocation,
        mask: u64,
    ) -> Result<DeviceLease> {
        self.allocate_slices(length, length, 1, location, mask)?
            .next()
            .ok_or(Error::Internal("empty RDMA device allocation"))
    }

    pub(in crate::transport::rdma) fn allocate_slices(
        &self,
        length: usize,
        block: usize,
        count: usize,
        location: MemoryLocation,
        mask: u64,
    ) -> Result<DeviceAllocation> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        self.initialize_contexts(&mut contexts)?;
        let healthy = contexts
            .slots
            .iter()
            .enumerate()
            .fold(0, |mask, (id, slot)| {
                mask | if slot
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.asynchronous.check().is_ok())
                {
                    1u64 << id
                } else {
                    0
                }
            });
        contexts
            .selector
            .as_ref()
            .ok_or(Error::Internal("missing RDMA device selector"))?
            .allocate(
                length,
                block,
                count,
                self.config
                    .preferred_numa_node
                    .map_or(location, |node| MemoryLocation::Cpu(Some(node))),
                mask & self.config.device_mask & healthy,
            )
    }

    pub(in crate::transport::rdma) fn device_at(&self, id: usize) -> Result<Arc<RdmaContext>> {
        let contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        if contexts.closed {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        if let Some((operation, code)) = contexts.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        contexts
            .slots
            .get(id)
            .and_then(|slot| slot.owner.clone())
            .ok_or(Error::Unavailable("RDMA context slot is disabled"))
    }

    pub(in crate::transport::rdma) fn cq_list(
        &self,
        device: Arc<RdmaContext>,
    ) -> Result<Vec<Arc<cq_lane::CqLane>>> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        self.initialize_contexts(&mut contexts)?;
        let slot = contexts
            .slots
            .iter_mut()
            .find(|slot| {
                slot.owner
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &device))
            })
            .ok_or(Error::Internal(
                "RDMA CQ context is not owned by this transport",
            ))?;
        slot.cq_list.resize_with(self.config.num_lanes, || None);
        for (id, entry) in slot.cq_list.iter_mut().enumerate() {
            if entry.is_some() {
                continue;
            }
            let lane = cq_lane::CqLane::new(
                &self.config,
                Arc::clone(&self.workers),
                Arc::clone(&device),
                Arc::clone(&self.usage),
                Arc::clone(&self.cq_quota),
                self.verbs,
            )
            .and_then(|lane| {
                if let Err(error) = self.workers.attach(id, &lane) {
                    lane.close(Instant::now() + Duration::from_secs(30))?;
                    return Err(error);
                }
                Ok(lane)
            });
            match lane {
                Ok(lane) => *entry = Some(lane),
                Err(error) => {
                    if let Error::Unquiesced { operation, code } = error {
                        contexts.failure = Some((operation, code));
                    }
                    return Err(error);
                }
            }
        }
        Ok(slot.cq_list.iter().flatten().cloned().collect())
    }

    pub(crate) fn recover_progress(&self) -> Result<()> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        if contexts.closed {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        if let Some((operation, code)) = contexts.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        for slot in &mut contexts.slots {
            for entry in &mut slot.cq_list {
                let Some(progress) = entry else {
                    continue;
                };
                if !progress.recoverable()? {
                    continue;
                }
                let device = slot
                    .owner
                    .as_ref()
                    .ok_or(Error::Internal("RDMA lane has no context owner"))?;
                device.asynchronous.check()?;
                // A lane may be replaced only after both CQs, every QP and the
                // worker snapshot have drained. Sibling lanes retain this PD.
                progress.close(Instant::now())?;
                *entry = None;
                self.usage
                    .lock()
                    .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                    .progress_recoveries += 1;
            }
        }
        Ok(())
    }

    pub(in crate::transport::rdma) fn evict_idle(&self, deadline: Instant) -> Result<bool> {
        let workers: Vec<_> = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?
            .slots
            .iter()
            .flat_map(|slot| slot.cq_list.iter().flatten().cloned())
            .collect();
        for progress in workers {
            if progress.evict_idle(|_| true, deadline)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn close(&self, deadline: Instant) -> Result<()> {
        self.workers.monitor.unwatch_contexts();
        let (owners, failure) = {
            let mut contexts = self
                .context_set
                .lock()
                .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
            contexts.closed = true;
            (
                contexts
                    .slots
                    .iter()
                    .map(|slot| {
                        (
                            slot.owner.clone(),
                            slot.cq_list.iter().flatten().cloned().collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>(),
                contexts.failure,
            )
        };
        let mut error = None;
        for (_, lanes) in &owners {
            for progress in lanes {
                if let Err(fault) = progress.fence(deadline) {
                    error.get_or_insert(fault);
                }
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        // A partially constructed retained CQ can own a PD without a context-progress
        // handle in its slot. Fencing siblings cannot prove that PD quiescent.
        if let Some((operation, code)) = failure {
            return Err(Error::Unquiesced { operation, code });
        }
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        loop {
            if usage.retained_endpoints != 0 || usage.retained_grants != 0 {
                return Err(Error::Unquiesced {
                    operation: "RDMA endpoint resources retained",
                    code: libc::EIO,
                });
            }
            if usage.active_endpoints == 0 && usage.active_grants == 0 {
                break;
            }
            usage = self
                .drained
                .wait_timeout(usage, remaining(deadline)?)
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                .0;
        }
        drop(usage);
        self.registrations.close(deadline)?;
        for (owner, lanes) in owners {
            let mut drained = true;
            for progress in lanes {
                if let Err(fault) = progress.close(deadline) {
                    error.get_or_insert(fault);
                    drained = false;
                }
            }
            if !drained {
                continue;
            }
            if let Some(owner) = owner {
                if let Err(fault) = owner.close() {
                    error.get_or_insert(fault);
                }
            }
        }
        if let Err(fault) = self.workers.close(deadline) {
            error.get_or_insert(fault);
        }
        if let Some(error) = error {
            return Err(error);
        }
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        contexts.slots.clear();
        contexts.topology = None;
        contexts.selector = None;
        Ok(())
    }
}
