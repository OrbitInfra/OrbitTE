//! Context-owned peer endpoints. CQ-local QP progress lives in `qp_store`.

use std::sync::Arc;

use super::RdmaGrant;
use super::endpoint::{Peer, RdmaEndpoint};
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct EndpointStore {
    endpoints: Vec<Arc<RdmaEndpoint>>,
}

impl EndpointStore {
    pub(super) fn prune(&mut self) -> Result<()> {
        let mut index = 0;
        while index < self.endpoints.len() {
            if self.endpoints[index].prune()? {
                self.endpoints.swap_remove(index);
            } else {
                index += 1;
            }
        }
        Ok(())
    }

    pub(super) fn adopt(&mut self, grant: &RdmaGrant, peer: Peer) -> Result<Arc<RdmaEndpoint>> {
        if grant
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| !endpoint.matches(peer))
        {
            return Err(Error::Protocol(
                "RDMA QP cannot move to a different peer endpoint",
            ));
        }
        self.prune()?;
        let endpoint = match self
            .endpoints
            .iter()
            .find(|endpoint| endpoint.matches(peer))
        {
            Some(endpoint) => Arc::clone(endpoint),
            None => Arc::new(RdmaEndpoint::new(peer)),
        };
        endpoint.adopt(grant, peer)?;
        if !self
            .endpoints
            .iter()
            .any(|entry| Arc::ptr_eq(entry, &endpoint))
        {
            self.endpoints.push(Arc::clone(&endpoint));
        }
        Ok(endpoint)
    }

    pub(super) fn snapshot(&mut self) -> Result<Vec<Arc<RdmaEndpoint>>> {
        self.prune()?;
        Ok(self.endpoints.clone())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/endpoint_store.rs"]
mod tests;
