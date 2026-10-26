//! Fixed-width endpoint construction and claims on published physical pairs.

use std::sync::Arc;
use std::time::Instant;

use super::RdmaTransport;
use crate::transport::rdma::endpoint::{Construction, EndpointKey, Peer};
use crate::transport::rdma::{ConnectionId, RdmaContext, RdmaGrant};
use crate::{Error, Result};

pub(in crate::transport::rdma) enum Connection {
    Fresh(Option<Construction>),
    Reuse(Box<RdmaGrant>),
}

impl RdmaTransport {
    pub(in crate::transport::rdma) fn connection(
        &self,
        device: Arc<RdmaContext>,
        key: EndpointKey,
        width: usize,
        remote: Option<ConnectionId>,
        deadline: Instant,
    ) -> Result<Connection> {
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
        let mut lanes = self.cq_list(device)?;
        loop {
            crate::engine::remaining(deadline)?;
            if self
                .context_set
                .lock()
                .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?
                .closed
            {
                return Err(Error::Unavailable("RDMA runtime is closed"));
            }
            self.workers.order(&mut lanes)?;
            let candidates =
                endpoint.candidates(&|peer| remote.is_none_or(|id| peer.connection == id))?;
            for lane in &lanes {
                for (candidate, _, peer) in &candidates {
                    if !Arc::ptr_eq(lane, candidate) {
                        continue;
                    }
                    if let Some(mut grant) = lane.claim(|entry| entry.same_pair(peer), deadline)? {
                        grant.cached_peer = Some(*peer);
                        grant.endpoint = Some(Arc::clone(&endpoint));
                        return Ok(Connection::Reuse(Box::new(grant)));
                    }
                }
            }
            let full = self
                .usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                .active_endpoints
                >= self.config.max_endpoints;
            if candidates.is_empty() || (!full && remote.is_none()) {
                if let Some(slot) = endpoint.reserve(remote)? {
                    return Ok(Connection::Fresh(Some(slot)));
                }
            }
            for lane in &lanes {
                for (candidate, record, peer) in &candidates {
                    if Arc::ptr_eq(lane, candidate) {
                        let mut grant = lane.share(record, *peer, deadline)?;
                        grant.endpoint = Some(Arc::clone(&endpoint));
                        return Ok(Connection::Reuse(Box::new(grant)));
                    }
                }
            }
            if let Some(remote) = remote {
                if !endpoint.contains(remote)? {
                    for (lane, _, peer) in endpoint.candidates(&|_| true)? {
                        if lane.evict_idle(|entry| entry.same_pair(&peer), deadline)? {
                            break;
                        }
                    }
                }
            }
            // A cold builder publishes before payload execution. Waiting here
            // therefore never depends on the rest of our parent's slice list.
            endpoint.wait(deadline)?;
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
