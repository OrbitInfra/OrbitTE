use std::time::{Duration, Instant};

use super::*;
use crate::transport::rdma::tests::runtime;

#[test]
fn detachment_retains_work_owners_and_capacity_until_exact_retirement() {
    let mut transport = runtime("ok");
    transport.config.max_endpoints = 2;
    transport.config.max_staging_bytes = 24;
    let mut first = transport.staging_endpoint(8, 0).unwrap();
    let mut second = transport.staging_endpoint(8, 0).unwrap();
    let mut store = EndpointStore::new(2).unwrap();
    first.record.send_namespace.store(1, Ordering::Release);
    store.publish(Arc::clone(&first.record)).unwrap();
    store.publish(Arc::clone(&second.record)).unwrap();
    store.detach(&first.record);
    assert!(first.record.require_live().is_err());
    assert!(second.record.require_live().is_ok());
    assert_eq!(store.len(), 2);
    assert!(Arc::ptr_eq(
        &store.work_owner(1, false).unwrap(),
        &first.record
    ));
    let stats = transport.stats().unwrap();
    assert_eq!(
        (
            stats.active_endpoints,
            stats.active_grants,
            stats.staging_bytes
        ),
        (2, 2, 16)
    );
    assert!(transport.staging_endpoint(1, 0).is_err());
    store.remove_retired(&second.record);
    assert_eq!(
        store.len(),
        2,
        "a stale/nonretiring owner cannot remove a live endpoint"
    );

    first.close().unwrap();
    store.remove_retired(&first.record);
    assert_eq!(store.len(), 1);
    assert!(store.publish(Arc::clone(&first.record)).is_err());
    let mut replacement = transport.staging_endpoint(8, 0).unwrap();
    replacement
        .record
        .send_namespace
        .store(2, Ordering::Release);
    store.publish(Arc::clone(&replacement.record)).unwrap();
    store.detach(&replacement.record);
    store.remove_retired(&first.record);
    assert_eq!(store.len(), 2);
    assert!(Arc::ptr_eq(
        &store.work_owner(2, false).unwrap(),
        &replacement.record
    ));
    assert!(replacement.connect(&replacement.descriptor()).is_err());
    replacement.close().unwrap();
    second.close().unwrap();
    drop((first, second, replacement));
    transport
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn whole_store_detachment_fences_every_endpoint_without_repaying_native_debt() {
    let mut transport = runtime("ok");
    transport.config.max_endpoints = 2;
    transport.config.max_staging_bytes = 16;
    let mut endpoints = [
        transport.staging_endpoint(8, 1).unwrap(),
        transport.staging_endpoint(8, 2).unwrap(),
    ];
    let mut store = EndpointStore::new(2).unwrap();
    for endpoint in &endpoints {
        endpoint
            .record
            .send_namespace
            .store(endpoint.record.tag + 1, Ordering::Release);
        store.publish(Arc::clone(&endpoint.record)).unwrap();
    }
    let before = transport.stats().unwrap();
    store.detach_all();
    assert_eq!(store.live.len(), 0);
    assert_eq!(store.retiring.len(), 2);
    for endpoint in &endpoints {
        assert!(endpoint.record.require_live().is_err());
        assert!(store.work_owner(endpoint.record.tag + 1, false).is_some());
    }
    let after = transport.stats().unwrap();
    assert_eq!(
        (
            before.active_endpoints,
            before.active_grants,
            before.staging_bytes
        ),
        (
            after.active_endpoints,
            after.active_grants,
            after.staging_bytes
        )
    );
    assert_eq!(before.work_requests, after.work_requests);
    for endpoint in &mut endpoints {
        endpoint.close().unwrap();
    }
    drop(endpoints);
    transport
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}
