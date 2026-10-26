//! Context-owned peer endpoints. CQ-local QP progress lives in `qp_store`.

use std::sync::Arc;

use super::endpoint::{EndpointKey, RdmaEndpoint};
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct EndpointStore {
    endpoints: Vec<Arc<RdmaEndpoint>>,
}

impl EndpointStore {
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
    ) -> Result<Arc<RdmaEndpoint>> {
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
        let endpoint = Arc::new(RdmaEndpoint::new(key, width)?);
        self.endpoints
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        self.endpoints.push(Arc::clone(&endpoint));
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
