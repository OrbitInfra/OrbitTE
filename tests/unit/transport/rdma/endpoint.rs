use std::time::Duration;

use super::*;
use crate::Permission;
use crate::memory::MemoryRegistry;
use crate::transport::rdma::tests::{events, runtime};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[test]
fn grant_release_requires_its_own_physical_connection_and_outputs_keep_only_grant_capacity() {
    let mut transport = runtime("direct:ok");
    transport.config.max_endpoints = 2;
    transport.config.max_direct_bytes = 16;
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 1, 16);
    let memory = registry
        .register(
            vec![0x42; 16].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let mut first = transport.endpoint(&memory, 0, 8, 2, deadline()).unwrap();
    let mut other = transport.endpoint(&memory, 8, 8, 2, deadline()).unwrap();
    first.connect(&first.descriptor()).unwrap();
    first.bind_window().unwrap();
    first.invalidate_window().unwrap();
    let snapshot = Arc::clone(&first.record);
    {
        let mut request = first.record.lock().unwrap();
        let resources = request.resources.as_mut().unwrap();
        assert!(
            resources
                .grant
                .close(&resources.connection, transport.verbs)
                .is_err()
        );
        assert!(
            memory.read(0, &mut [0; 8]).is_err(),
            "INV CQE is not a physical connection drain"
        );
        let mut second = other.record.lock().unwrap();
        let second = second.resources.as_mut().unwrap();
        second.connection.close(transport.verbs).unwrap();
        assert!(
            resources
                .grant
                .close(&second.connection, transport.verbs)
                .is_err(),
            "another connection cannot authorize grant release"
        );
    }
    other.close().unwrap();
    first.close().unwrap();
    assert!(snapshot.lock().unwrap().resources.is_none());
    let stats = transport.stats().unwrap();
    assert_eq!(
        (
            stats.active_endpoints,
            stats.active_grants,
            stats.direct_bytes
        ),
        (0, 2, 16)
    );
    assert!(matches!(
        transport.close(Instant::now() + Duration::from_millis(10)),
        Err(Error::Timeout)
    ));
    memory.read(0, &mut [0; 16]).unwrap();
    drop((first, other));
    assert_eq!(transport.stats().unwrap().active_grants, 0);
    registry.unregister(&memory).unwrap();
    transport.close(deadline()).unwrap();
}

#[test]
fn partial_retirement_distinguishes_closed_qps_from_retained_grants_and_never_retries() {
    for (mode, qps, windows, deregistered) in [
        ("close0", 1, 1, false),
        ("window_close", 0, 1, false),
        ("mr_close", 0, 0, true),
    ] {
        let transport = runtime(&format!("direct:{mode}"));
        let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 1, 8);
        let memory = registry
            .register(
                vec![0x42; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let mut endpoint = transport.endpoint(&memory, 0, 8, 2, deadline()).unwrap();
        let log = events(&transport);
        endpoint.connect(&endpoint.descriptor()).unwrap();
        endpoint.bind_window().unwrap();
        endpoint.invalidate_window().unwrap();
        registry.unregister(&memory).unwrap();
        transport.registrations.retire(&memory).unwrap();
        assert!(
            matches!(endpoint.close(), Err(Error::Unquiesced { .. })),
            "{mode}"
        );
        let stats = transport.stats().unwrap();
        assert_eq!(
            (stats.active_endpoints, stats.retained_endpoints),
            (qps, qps),
            "{mode}"
        );
        assert_eq!(
            (
                stats.active_grants,
                stats.retained_grants,
                stats.direct_bytes,
                stats.active_windows
            ),
            (1, 1, 8, windows),
            "{mode}"
        );
        assert!(memory.read(0, &mut [0; 8]).is_err());
        let receipt = log.lock().unwrap().clone();
        assert_eq!(
            receipt
                .iter()
                .filter(|e| **e == "deregister_parent")
                .count(),
            usize::from(deregistered)
        );
        assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
        drop(endpoint);
        assert_eq!(
            *log.lock().unwrap(),
            receipt,
            "{mode}: a retained owner must not retry native teardown"
        );
        assert!(matches!(
            transport.close(deadline()),
            Err(Error::Unquiesced { .. })
        ));
    }
}
