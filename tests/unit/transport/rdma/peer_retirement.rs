use super::*;
use crate::transport::rdma::tests::{events, pause};

fn fixture(mode: &str) -> TransferEngine {
    let mut config = crate::transport::rdma::tests::runtime(mode).config.clone();
    config.max_endpoints = 8;
    config.max_direct_bytes = 4096;
    config.max_staging_bytes = 4096;
    crate::engine::tests::rdma_fixture_with_runtime(
        crate::transport::rdma::tests::runtime_with_config(config),
    )
}
fn read(e: &TransferEngine, src: &TransferEngine, l: &RegisteredMemory, r: &RegisteredMemory) {
    let p = e
        .open_segment(src.endpoint(), src.source_epoch(), TIMEOUT)
        .unwrap();
    let q = TransferRequest::new(
        Transport::Rdma,
        l.base_address() + 1,
        r.base_address() + 1,
        16,
    );
    let result = e
        .submit_and_wait(TransferOp::Read, &p, &[q], TIMEOUT)
        .unwrap();
    assert!(result.is_complete(), "{result:?}");
}
fn bytes(m: &RegisteredMemory) -> Vec<u8> {
    let mut b = vec![0; m.len()];
    m.read(0, &mut b).unwrap();
    b
}

#[test]
fn peer_retirement_rdma_closes_only_matching_qps_after_real_grant_drain() {
    let src = fixture("direct:copy");
    let other = fixture("direct:copy");
    let dst = Arc::new(fixture("direct:pause_copy"));
    let r = src
        .register_memory(vec![0x29; 18].into(), Permission::RemoteReadWrite)
        .unwrap();
    let rr = other
        .register_memory(vec![0x36; 18].into(), Permission::RemoteReadWrite)
        .unwrap();
    let l = dst
        .register_memory(vec![0xcd; 18].into(), Permission::LocalOnly)
        .unwrap();
    let ll = dst
        .register_memory(vec![0xae; 18].into(), Permission::LocalOnly)
        .unwrap();
    let peer = dst
        .open_segment(src.endpoint(), src.source_epoch(), TIMEOUT)
        .unwrap();
    let q = TransferRequest::new(
        Transport::Rdma,
        l.base_address() + 1,
        r.base_address() + 1,
        16,
    );
    let held = pause(source_runtime(&dst));
    let active = dst
        .submit_async(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&q),
            None,
            TIMEOUT,
        )
        .unwrap();
    let (state, result) = held
        .1
        .wait_timeout_while(held.0.lock().unwrap(), TIMEOUT, |s| !s.0)
        .unwrap();
    assert!(!result.timed_out() && state.0);
    drop(state);
    assert!(matches!(
        dst.close_segment(peer.endpoint, peer.epoch, Duration::ZERO),
        Err(Error::Unquiesced { .. })
    ));
    assert_eq!(dst.stats().unwrap().inflight_bytes, 16);
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Read, &peer, &[q], TIMEOUT),
        Err(Error::PeerClosed)
    ));
    assert!(
        !events(source_runtime(&dst))
            .lock()
            .unwrap()
            .contains(&"destroy_qp")
    );
    held.0.lock().unwrap().1 = true;
    held.1.notify_all();
    assert_eq!(active.wait(TIMEOUT).unwrap().completed_tasks, 1);
    read(&dst, &other, &ll, &rr);
    let before = dst.stats().unwrap().rdma.unwrap();
    assert_eq!(before.active_endpoints, 2);
    dst.close_segment(peer.endpoint, peer.epoch, TIMEOUT)
        .unwrap();
    let after = dst.stats().unwrap().rdma.unwrap();
    assert_eq!(after.active_endpoints, 1);
    assert_eq!(after.persistent_mrs, before.persistent_mrs);
    read(&dst, &other, &ll, &rr);
    assert_eq!(
        dst.stats().unwrap().rdma.unwrap().opened_connections,
        before.opened_connections
    );
    let mut want = vec![0xcd; 18];
    want[1..17].fill(0x29);
    assert_eq!(bytes(&l), want);
    let mut want = vec![0xae; 18];
    want[1..17].fill(0x36);
    assert_eq!(bytes(&ll), want);
    assert_eq!(bytes(&r), vec![0x29; 18]);
    assert_eq!(bytes(&rr), vec![0x36; 18]);
}

#[test]
fn peer_retirement_rdma_failed_destroy_keeps_native_charge_without_retry() {
    let src = fixture("direct:copy");
    let dst = fixture("direct:copy_close0");
    let r = src
        .register_memory(vec![0x29; 18].into(), Permission::RemoteReadWrite)
        .unwrap();
    let l = dst
        .register_memory(vec![0xcd; 18].into(), Permission::LocalOnly)
        .unwrap();
    read(&dst, &src, &l, &r);
    let trace = events(source_runtime(&dst));
    for _ in 0..2 {
        assert!(matches!(
            dst.close_segment(src.endpoint(), src.source_epoch(), TIMEOUT),
            Err(Error::Unquiesced { .. })
        ));
    }
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "destroy_qp")
            .count(),
        1
    );
    let stats = dst.stats().unwrap().rdma.unwrap();
    assert_eq!(stats.retained_endpoints, 1);
    assert_eq!(stats.active_endpoints, 1);
    let mut want = vec![0xcd; 18];
    want[1..17].fill(0x29);
    assert_eq!(bytes(&l), want);
    assert!(matches!(
        dst.close_requester(TIMEOUT),
        Err(Error::Unquiesced { .. })
    ));
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "destroy_qp")
            .count(),
        1
    );
}
