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
    let owner = store.get_or_insert(key, 1, 8).unwrap();
    store.prune().unwrap();
    assert!(Arc::ptr_eq(
        &owner,
        &store.get_or_insert(key, 1, 8).unwrap()
    ));
    assert!(store.get_or_insert(key, 3, 8).is_err());
    drop(owner);
    assert!(store.snapshot().unwrap().is_empty());
}

#[test]
fn bounded_store_fences_an_external_handle_and_never_revives_its_slots() {
    let mut store = EndpointStore::default();
    let mut key = EndpointKey {
        route: None,
        epoch: crate::SourceEpoch([1; 16]),
        domain: [2; 16],
        source: true,
        source_nic_id: 0,
        operation: crate::TransferOp::Read,
    };
    let original = store.get_or_insert(key, 1, 1).unwrap();
    let bound = original.reserve(None, None).unwrap().unwrap();
    assert!(!store.quiescent().unwrap());
    key.domain = [3; 16];
    assert!(matches!(
        store.get_or_insert(key, 1, 1),
        Err(Error::ResourceExhausted)
    ));
    store.fence();
    store.fence();
    assert!(matches!(
        original.reserve(None, None),
        Err(Error::Unavailable(_))
    ));
    drop(bound);
    assert!(store.quiescent().unwrap());
    store.prune().unwrap();
    assert_eq!(store.native_endpoints().len(), 1);
    drop(original);
    store.prune().unwrap();
    store.prune().unwrap();
    assert!(store.native_endpoints().is_empty());
    assert!(matches!(
        store.get_or_insert(key, 1, 8),
        Err(Error::Unavailable(_))
    ));
}

#[test]
fn reclaim_sweeps_a_quiescent_backlog_while_a_live_builder_stays_pinned() {
    let mut store = EndpointStore::default();
    let key = EndpointKey {
        route: None,
        epoch: crate::SourceEpoch([1; 16]),
        domain: [2; 16],
        source: true,
        source_nic_id: 0,
        operation: crate::TransferOp::Read,
    };
    let live = store.get_or_insert(key, 1, 1024).unwrap();
    let reservation = live.reserve(None, None).unwrap().unwrap();
    let mut holders = Vec::new();
    for id in 1..=1000usize {
        let mut peer = key;
        peer.epoch.0[..8].copy_from_slice(&(id as u64).to_be_bytes());
        holders.push(store.get_or_insert(peer, 1, 1024).unwrap());
    }
    let last = Arc::downgrade(holders.last().unwrap());
    drop(holders);
    store.prune().unwrap();
    assert!(last.upgrade().is_none());
    assert_eq!(store.snapshot().unwrap().len(), 1);
    assert!(!store.quiescent().unwrap());
    drop(reservation);
    drop(live);
    store.prune().unwrap();
    assert!(store.snapshot().unwrap().is_empty());
}
