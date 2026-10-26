use super::*;

impl EndpointStore {
    pub(in crate::transport::rdma) fn native_endpoints(&self) -> Vec<Arc<RdmaEndpoint>> {
        self.endpoints.clone()
    }
}

#[test]
fn waiting_callers_keep_the_canonical_unbound_endpoint() {
    let mut store = EndpointStore::default();
    let key = EndpointKey {
        route: None,
        epoch: crate::SourceEpoch([1; 16]),
        domain: [2; 16],
        source: true,
        source_nic_id: 0,
        operation: crate::TransferOp::Read,
    };
    let owner = store.get_or_insert(key, 1).unwrap();
    store.prune().unwrap();
    assert!(Arc::ptr_eq(&owner, &store.get_or_insert(key, 1).unwrap()));
    assert!(store.get_or_insert(key, 3).is_err());
    drop(owner);
    assert!(store.snapshot().unwrap().is_empty());
}
