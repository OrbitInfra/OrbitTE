use std::time::{Duration, Instant};

use ibverbs_sys as ffi;

use super::*;
use crate::transport::rdma::{
    RdmaTransport,
    tests::{events as native_events, runtime},
};

fn wait(runtime: &RdmaTransport, predicate: impl Fn(RdmaStats) -> bool) {
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
    for (index, kind) in [ffi::IBV_EVENT_COMM_EST, ffi::IBV_EVENT_SQ_DRAINED]
        .into_iter()
        .enumerate()
    {
        crate::transport::rdma::context::tests::emit_async(&runtime, kind as u8);
        wait(&runtime, |s| {
            s.acknowledged_async_events == index as u64 + 1
        });
        assert!(runtime.device().unwrap().asynchronous.check().is_ok());
    }
    crate::transport::rdma::context::tests::emit_async(&runtime, 254);
    wait(&runtime, |s| s.acknowledged_async_events == 3);
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
        ffi::IBV_EVENT_CQ_ERR,
        ffi::IBV_EVENT_QP_FATAL,
        ffi::IBV_EVENT_QP_REQ_ERR,
        ffi::IBV_EVENT_QP_ACCESS_ERR,
        ffi::IBV_EVENT_PATH_MIG,
        ffi::IBV_EVENT_PATH_MIG_ERR,
        ffi::IBV_EVENT_DEVICE_FATAL,
        ffi::IBV_EVENT_PORT_ACTIVE,
        ffi::IBV_EVENT_PORT_ERR,
        ffi::IBV_EVENT_LID_CHANGE,
        ffi::IBV_EVENT_PKEY_CHANGE,
        ffi::IBV_EVENT_SM_CHANGE,
        ffi::IBV_EVENT_SRQ_ERR,
        ffi::IBV_EVENT_SRQ_LIMIT_REACHED,
        ffi::IBV_EVENT_QP_LAST_WQE_REACHED,
        ffi::IBV_EVENT_CLIENT_REREGISTER,
        ffi::IBV_EVENT_GID_CHANGE,
        ffi::IBV_EVENT_WQ_FATAL,
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
        .endpoint(
            &memory,
            0,
            8,
            2,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    let events = native_events(&runtime);
    crate::transport::rdma::context::tests::emit_async(&runtime, 255);
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
        (1, 1, 2, 1, 8, 0)
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

#[test]
fn route_fault_is_permanent_but_lost_consumer_keeps_unquiesced_precedence() {
    let state = State::default();
    state.fence_route();
    assert!(matches!(
        state.check(),
        Err(Error::Backend {
            operation: "RDMA route changed",
            code: libc::ESTALE
        })
    ));
    state.lost(libc::EIO);
    state.fence_route();
    assert!(matches!(
        state.check(),
        Err(Error::Unquiesced {
            operation: "RDMA asynchronous event consumer",
            code: libc::EIO
        })
    ));
}

#[test]
fn lost_cq_consumer_dominates_route_and_device_errors_and_cannot_be_healed() {
    let state = State::default();
    state.fence_route();
    state.event(3);
    state.lost_completion(libc::EBADF);
    state.event(IBV_EVENT_COMM_EST);
    assert!(matches!(
        state.check(),
        Err(Error::Unquiesced {
            operation: "RDMA completion event consumer",
            code: libc::EBADF
        })
    ));
    state.lost(libc::ENODEV);
    assert!(matches!(
        state.check(),
        Err(Error::Unquiesced {
            operation: "RDMA asynchronous event consumer",
            code: libc::ENODEV
        })
    ));
}
