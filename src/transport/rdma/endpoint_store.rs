//! Context-owned peer endpoints. CQ-local QP progress lives in `qp_store`.

use std::sync::Arc;

use super::endpoint::{EndpointKey, RdmaEndpoint};
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct EndpointStore {
    fenced: bool,
    endpoints: Vec<Arc<RdmaEndpoint>>,
}

impl EndpointStore {
    pub(super) fn peer(
        &self,
        peer: crate::runtime::segment_manager::PeerKey,
    ) -> Vec<Arc<RdmaEndpoint>> {
        self.endpoints
            .iter()
            .filter(|e| {
                !e.key.source && e.key.route == Some(peer.endpoint) && e.key.epoch == peer.epoch
            })
            .map(|e| {
                e.fence();
                e.clone()
            })
            .collect()
    }

    pub(super) fn remove_peer(
        &mut self,
        peer: crate::runtime::segment_manager::PeerKey,
    ) -> Result<()> {
        let mut index = 0;
        while index < self.endpoints.len() {
            let e = &self.endpoints[index];
            if !e.key.source
                && e.key.route == Some(peer.endpoint)
                && e.key.epoch == peer.epoch
                && e.prune()?
            {
                self.endpoints.swap_remove(index);
            } else {
                index += 1;
            }
        }
        Ok(())
    }

    pub(super) fn prune(&mut self) -> Result<()> {
        let mut index = 0;
        while index < self.endpoints.len() {
            let endpoint = &self.endpoints[index];
            // An unbound builder or waiter must keep the canonical array.
            if endpoint.prune()? && Arc::strong_count(endpoint) == 1 {
                self.endpoints.swap_remove(index);
            } else {
                index += 1;
            }
        }
        Ok(())
    }

    pub(super) fn quiescent(&self) -> Result<bool> {
        for endpoint in &self.endpoints {
            if !endpoint.prune()? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn get_or_insert(
        &mut self,
        key: EndpointKey,
        width: usize,
        limit: usize,
    ) -> Result<Arc<RdmaEndpoint>> {
        if self.fenced {
            return Err(Error::Unavailable("RDMA endpoint store closed"));
        }
        self.prune()?;
        for endpoint in &self.endpoints {
            if endpoint.key == key {
                if endpoint.width()? != width {
                    return Err(Error::Protocol("RDMA peer changed QP width"));
                }
                return Ok(Arc::clone(endpoint));
            }
            if !key.source
                && (EndpointKey {
                    domain: key.domain,
                    ..endpoint.key
                }) == key
            {
                return Err(Error::Protocol(
                    "RDMA path table replaced a cached protection domain",
                ));
            }
        }
        if self.endpoints.len() >= limit {
            return Err(Error::ResourceExhausted);
        }
        let endpoint = Arc::new(RdmaEndpoint::new(key, width)?);
        self.endpoints
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        self.endpoints.push(Arc::clone(&endpoint));
        Ok(endpoint)
    }

    pub(super) fn fence(&mut self) {
        self.fenced = true;
        for endpoint in &self.endpoints {
            endpoint.fence();
        }
    }
    pub(super) fn snapshot(&mut self) -> Result<Vec<Arc<RdmaEndpoint>>> {
        self.prune()?;
        Ok(self.endpoints.clone())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/endpoint_store.rs"]
mod tests;
