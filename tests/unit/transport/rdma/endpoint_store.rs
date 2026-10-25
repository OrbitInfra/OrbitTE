use super::*;

impl EndpointStore {
    pub(in crate::transport::rdma) fn native_endpoints(&self) -> Vec<Arc<RdmaEndpoint>> {
        self.endpoints.clone()
    }
}
