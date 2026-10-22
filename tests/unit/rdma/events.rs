use std::time::{Duration, Instant};

use super::*;
use crate::rdma::{
    Runtime,
    tests::{events as native_events, runtime},
};

fn wait(runtime: &Runtime, predicate: impl Fn(RdmaStats) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate(runtime.stats().unwrap()) {
        assert!(
            Instant::now() < deadline,
            "async consumer did not make progress"
        );
        thread::yield_now();
    }
}

#[test]
fn context_consumer_acks_while_native_destroy_holds_request_lock() {
    let runtime = runtime("direct:async_destroy");
    let mut endpoint = runtime.staging_endpoint(8, 2).unwrap();
    let events = native_events(&runtime);
    assert_eq!(runtime.stats().unwrap().async_event_workers, 1);
    let closed = endpoint.close();
    assert!(
        !matches!(closed, Err(Error::Unquiesced { .. })),
        "{closed:?}"
    );
    drop(endpoint);
    assert!(
        runtime.staging_endpoint(8, 0).is_err(),
        "async fault must fence new admission"
    );
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.acknowledged_async_events,
            stats.async_event_workers,
            stats.active_endpoints,
            stats.active_windows,
            stats.active_cqs,
            stats.active_devices
        ),
        (1, 0, 0, 0, 0, 0)
    );
    let events = events.lock().unwrap();
    let ack = events.iter().position(|s| *s == "async_ack").unwrap();
    let destroyed = events
        .iter()
        .position(|s| *s == "destroy_after_async_ack")
        .unwrap();
    let context = events.iter().position(|s| *s == "close_device").unwrap();
    assert!(ack < destroyed && destroyed < context);
}

#[test]
fn context_consumer_acks_information_then_fences_unknown_events() {
    let runtime = runtime("async_ok");
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    for (index, kind) in [4, 5, 9].into_iter().enumerate() {
        crate::rdma::device::tests::emit_async(&runtime, kind);
        wait(&runtime, |s| {
            s.acknowledged_async_events == index as u64 + 1
        });
        assert!(runtime.device().unwrap().asynchronous.check().is_ok());
    }
    crate::rdma::device::tests::emit_async(&runtime, 254);
    wait(&runtime, |s| s.acknowledged_async_events == 4);
    assert!(matches!(
        runtime.device().unwrap().asynchronous.check(),
        Err(Error::Backend {
            operation: "RDMA asynchronous event",
            code: 254,
        })
    ));
    let closed = endpoint.close();
    assert!(!matches!(closed, Err(Error::Unquiesced { .. })));
    drop(endpoint);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().async_event_workers, 0);
    for kind in [
        0,
        1,
        2,
        3,
        6,
        7,
        8,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        u32::MAX,
    ] {
        let state = State::default();
        state.event(kind);
        assert!(
            state.check().is_err(),
            "event {kind} must not silently preserve health"
        );
    }
}

#[test]
fn lost_async_consumer_retains_actual_qp_mw_mr_and_direct_backing() {
    let runtime = runtime("direct:async_lost");
    let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([1; 16]), 1, 8);
    let memory = registry
        .register(
            vec![0x42; 8].into_boxed_slice(),
            crate::Permission::RemoteReadWrite,
        )
        .unwrap();
    let weak = Arc::downgrade(&memory.0);
    let mut endpoint = runtime
        .endpoint(&memory, 0, 8, 2, Instant::now() + Duration::from_secs(2))
        .unwrap();
    let events = native_events(&runtime);
    crate::rdma::device::tests::emit_async(&runtime, 255);
    wait(&runtime, |s| {
        s.retained_endpoints == 1 && s.async_event_workers == 0
    });
    assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
    registry.unregister(&memory).unwrap();
    assert!(memory.read(0, &mut [0; 8]).is_err());
    drop(memory);
    drop(endpoint);
    assert!(weak.upgrade().is_some());
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.retained_endpoints,
            stats.retained_windows,
            stats.retained_cqs,
            stats.active_devices,
            stats.direct_bytes,
            stats.acknowledged_async_events
        ),
        (1, 1, 1, 1, 8, 0)
    );
    let before = events.lock().unwrap().clone();
    assert!(!before.iter().any(|e| matches!(
        *e,
        "destroy_qp" | "deregister" | "destroy_cq" | "close_device"
    )));
    assert!(matches!(
        runtime.close(Instant::now() + Duration::from_secs(2)),
        Err(Error::Unquiesced { .. })
    ));
    drop(runtime);
    assert_eq!(
        *events.lock().unwrap(),
        before,
        "no native teardown retry without an ACK consumer"
    );
}
