//! Fixed-width endpoint construction and claims on published physical pairs.

use std::sync::Arc;
use std::time::Instant;

use super::RdmaTransport;
use crate::transport::rdma::cq_lane::CqLane;
use crate::transport::rdma::endpoint::{Construction, EndpointKey, Peer, RdmaEndpoint};
use crate::transport::rdma::{ConnectionId, RdmaContext, RdmaGrant};
use crate::{Error, Result};

pub(in crate::transport::rdma) enum Connection {
    Fresh(Option<Construction>),
    Reuse(Box<RdmaGrant>),
}

/// Retains one endpoint array and its CQ lanes while a cold builder publishes.
pub(super) struct Connecting {
    endpoint: Arc<RdmaEndpoint>,
    lanes: Vec<Arc<CqLane>>,
    remote: Option<ConnectionId>,
}

impl Connecting {
    pub(super) fn poll(
        &mut self,
        transport: &RdmaTransport,
        deadline: Instant,
    ) -> Result<Option<Connection>> {
        crate::engine::remaining(deadline)?;
        if transport
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?
            .closed
        {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        transport.workers.order(&mut self.lanes)?;
        let candidates = self
            .endpoint
            .candidates(&|peer| self.remote.is_none_or(|id| peer.connection == id))?;
        for lane in &self.lanes {
            for (candidate, _, peer) in &candidates {
                if !Arc::ptr_eq(lane, candidate) {
                    continue;
                }
                if let Some(mut grant) = lane.claim(|entry| entry.same_pair(peer), deadline)? {
                    grant.cached_peer = Some(*peer);
                    grant.endpoint = Some(Arc::clone(&self.endpoint));
                    return Ok(Some(Connection::Reuse(Box::new(grant))));
                }
            }
        }
        let full = transport
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .active_endpoints
            >= transport.config.max_endpoints;
        if candidates.is_empty() || (!full && self.remote.is_none()) {
            if let Some(slot) = self.endpoint.reserve(self.remote)? {
                return Ok(Some(Connection::Fresh(Some(slot))));
            }
        }
        for lane in &self.lanes {
            for (candidate, record, peer) in &candidates {
                if Arc::ptr_eq(lane, candidate) {
                    let mut grant = lane.share(record, *peer, deadline)?;
                    grant.endpoint = Some(Arc::clone(&self.endpoint));
                    return Ok(Some(Connection::Reuse(Box::new(grant))));
                }
            }
        }
        if let Some(remote) = self.remote {
            if !self.endpoint.contains(remote)? {
                for (lane, _, peer) in self.endpoint.candidates(&|_| true)? {
                    if lane.evict_idle(|entry| entry.same_pair(&peer), deadline)? {
                        break;
                    }
                }
            }
        }
        #[cfg(test)]
        {
            crate::transport::rdma::tests::cold_connection_wait(transport);
            tests::observe_pending(&self.endpoint);
        }
        Ok(None)
    }
}

impl RdmaTransport {
    pub(super) fn connecting(
        &self,
        device: Arc<RdmaContext>,
        key: EndpointKey,
        width: usize,
        remote: Option<ConnectionId>,
    ) -> Result<Connecting> {
        let endpoint = {
            let mut contexts = self
                .context_set
                .lock()
                .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
            if contexts.closed {
                return Err(Error::Unavailable("RDMA runtime is closed"));
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
            slot.endpoints.get_or_insert(key, width)?
        };
        Ok(Connecting {
            endpoint,
            lanes: self.cq_list(device)?,
            remote,
        })
    }

    pub(in crate::transport::rdma) fn connection(
        &self,
        device: Arc<RdmaContext>,
        key: EndpointKey,
        width: usize,
        remote: Option<ConnectionId>,
        deadline: Instant,
    ) -> Result<Connection> {
        let mut connecting = self.connecting(device, key, width, remote)?;
        loop {
            if let Some(connection) = connecting.poll(self, deadline)? {
                return Ok(connection);
            }
            connecting.endpoint.wait(deadline)?;
        }
    }

    pub(in crate::transport::rdma) fn claim(
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
            for (lane, record, peer) in endpoint.candidates(&matches)? {
                candidates.push((Arc::clone(&endpoint), lane, record, peer));
            }
        }
        for lane in &lanes {
            for (owner, candidate, _, peer) in &candidates {
                if !Arc::ptr_eq(lane, candidate) {
                    continue;
                }
                if let Some(mut grant) =
                    lane.claim(|entry| matches(entry) && entry.same_pair(peer), deadline)?
                {
                    grant.cached_peer = Some(*peer);
                    grant.endpoint = Some(Arc::clone(owner));
                    return Ok(Some(grant));
                }
            }
        }
        for lane in &lanes {
            for (owner, candidate, record, peer) in &candidates {
                if Arc::ptr_eq(lane, candidate) {
                    let mut grant = lane.share(record, *peer, deadline)?;
                    grant.endpoint = Some(Arc::clone(owner));
                    return Ok(Some(grant));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/connection_progress.rs"]
pub(crate) mod tests;
