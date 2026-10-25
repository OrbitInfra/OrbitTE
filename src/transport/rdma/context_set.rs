//! Stable NIC slots, shared device/PD owners and per-context CQ-lane lifecycles.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::endpoint::Peer;
use super::quota::{DeviceLease, DeviceSelector};
use super::rail_monitor::NicPath;
use super::rdma_transport::RdmaTransport;
use super::{RdmaContext, RdmaGrant, cq_lane, quota};
use crate::engine::remaining;
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct ContextSet {
    pub(super) closed: bool,
    pub(super) topology: Option<crate::Topology>,
    pub(super) slots: Vec<ContextSlot>,
    pub(super) selector: Option<Arc<quota::DeviceSelector>>,
    pub(super) failure: Option<(&'static str, i32)>,
}

#[derive(Default)]
pub(super) struct ContextSlot {
    pub(super) owner: Option<Arc<RdmaContext>>,
    pub(super) cq_list: Vec<Option<Arc<cq_lane::CqLane>>>,
    pub(super) endpoints: super::endpoint_store::EndpointStore,
}

impl RdmaTransport {
    pub(super) fn initialize_contexts(&self, contexts: &mut ContextSet) -> Result<()> {
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
        contexts
            .slots
            .resize_with(topology.nic_list.len(), ContextSlot::default);
        let mut available = 0;
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
                        owner.address()?.publish(&mut topology)?;
                        slot.owner = Some(owner);
                        available |= 1u64 << id;
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
        contexts.selector = Some(DeviceSelector::new(topology.clone(), available));
        contexts.topology = Some(topology);
        Ok(())
    }

    pub(super) fn paths(&self) -> Result<Vec<NicPath>> {
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
        let mut paths = Vec::new();
        for (id, slot) in contexts.slots.iter().enumerate() {
            if self.config.device_mask & (1u64 << id) == 0 {
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
                "no enabled RDMA context matches device mask",
            ));
        }
        Ok(paths)
    }

    pub(super) fn allocate(&self, length: usize, mask: u64) -> Result<DeviceLease> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        self.initialize_contexts(&mut contexts)?;
        contexts
            .selector
            .as_ref()
            .ok_or(Error::Internal("missing RDMA device selector"))?
            .allocate(
                length,
                self.config.preferred_numa_node,
                mask & self.config.device_mask,
            )
    }

    pub(super) fn device_at(&self, id: usize) -> Result<Arc<RdmaContext>> {
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

    pub(super) fn cq_list(&self, device: Arc<RdmaContext>) -> Result<Vec<Arc<cq_lane::CqLane>>> {
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

    pub(super) fn claim(
        &self,
        device: Arc<RdmaContext>,
        matches: impl Fn(&Peer) -> bool,
        deadline: Instant,
    ) -> Result<Option<RdmaGrant>> {
        let mut lanes = self.cq_list(Arc::clone(&device))?;
        self.workers.order(&mut lanes)?;
        let endpoints = {
            let mut contexts = self
                .context_set
                .lock()
                .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
            let slot = contexts
                .slots
                .iter_mut()
                .find(|slot| {
                    slot.owner
                        .as_ref()
                        .is_some_and(|owner| Arc::ptr_eq(owner, &device))
                })
                .ok_or(Error::Internal("RDMA peer endpoint context missing"))?;
            slot.endpoints.snapshot()?
        };
        let mut candidates = Vec::new();
        for endpoint in endpoints {
            for (lane, peer) in endpoint.candidates(&matches)? {
                candidates.push((Arc::clone(&endpoint), lane, peer));
            }
        }
        for lane in lanes {
            for (owner, candidate, peer) in &candidates {
                if !Arc::ptr_eq(&lane, candidate) {
                    continue;
                }
                if let Some(mut grant) = lane.claim(
                    |entry| {
                        matches(entry)
                            && entry.same_pair(peer)
                            && entry.generation == peer.generation
                    },
                    deadline,
                )? {
                    grant.endpoint = Some(Arc::clone(owner));
                    return Ok(Some(grant));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn adopt_peer(&self, grant: &mut RdmaGrant, peer: Peer) -> Result<()> {
        let device = grant
            .record
            .lock()?
            .resources
            .as_ref()
            .map(|resources| Arc::clone(&resources.connection.device))
            .ok_or(Error::Internal("RDMA peer endpoint has no physical QP"))?;
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        if contexts.closed {
            return Err(Error::Unavailable("RDMA peer endpoint admission closed"));
        }
        let slot = contexts
            .slots
            .iter_mut()
            .find(|slot| {
                slot.owner
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &device))
            })
            .ok_or(Error::Internal("RDMA peer endpoint context missing"))?;
        let owner = slot.endpoints.adopt(grant, peer)?;
        if grant
            .endpoint
            .as_ref()
            .is_some_and(|old| !Arc::ptr_eq(old, &owner))
        {
            return Err(Error::Protocol(
                "RDMA reused QP moved to another peer endpoint",
            ));
        }
        grant.endpoint = Some(owner);
        Ok(())
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

    pub(super) fn evict_idle(&self, deadline: Instant) -> Result<bool> {
        let workers: Vec<_> = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?
            .slots
            .iter()
            .flat_map(|slot| slot.cq_list.iter().flatten().cloned())
            .collect();
        for progress in workers {
            if progress.evict_idle(deadline)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn close(&self, deadline: Instant) -> Result<()> {
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
