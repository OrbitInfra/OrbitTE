use super::shim::{self, Faults};
use crate::test_support::EVENTUAL_PROGRESS as WAIT;
use crate::*;
use std::{
    sync::{Arc, atomic::Ordering::SeqCst},
    thread,
    time::{Duration, Instant},
};
pub(super) fn cfg() -> EngineConfig {
    EngineConfig {
        max_failover_attempts: 0,
        fabric: Some(FabricConfig {
            provider: FabricProvider::Tcp,
            devices: vec!["lo".into()],
            max_mr_size: 4096,
            slice_size: 1024,
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn wait(f: impl Fn() -> bool) {
    let end = Instant::now() + WAIT;
    while !f() {
        assert!(
            Instant::now() < end,
            "fabric owner boundary was not reached"
        );
        thread::sleep(Duration::from_millis(1));
    }
}
struct Pair {
    source: Arc<TransferEngine>,
    engine: Arc<TransferEngine>,
    local: RegisteredMemory,
    remote: RegisteredMemory,
    peer: RemoteSegment,
}
impl Pair {
    fn new(f: Arc<Faults>, mutate: impl FnOnce(&mut EngineConfig)) -> Self {
        let source = Arc::new(TransferEngine::new(cfg()).unwrap());
        let mut c = cfg();
        mutate(&mut c);
        let engine = Arc::new(shim::configured(f, || TransferEngine::new(c)).unwrap());
        let local = engine
            .register_memory(vec![0xee; 8194].into(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                (0..8194)
                    .map(|i| (i % 251) as u8)
                    .collect::<Vec<_>>()
                    .into(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let peer = engine
            .open_segment(source.endpoint(), source.source_epoch(), WAIT)
            .unwrap();
        Self {
            source,
            engine,
            local,
            remote,
            peer,
        }
    }
    fn request(&self, off: u64, length: usize) -> TransferRequest {
        TransferRequest::new(
            Transport::Fabric,
            self.local.base_address() + off,
            self.remote.base_address() + off,
            length,
        )
    }
    fn submit(&self, operation: TransferOp, r: &[TransferRequest]) -> Batch {
        self.engine
            .submit_async(operation, &self.peer, r, None, WAIT)
            .unwrap()
    }
    fn check(&self, offset: usize, len: usize) {
        let mut got = vec![0; 8194];
        self.local.read(0, &mut got).unwrap();
        let mut expected = vec![0xee; 8194];
        for (i, b) in expected.iter_mut().enumerate().skip(offset).take(len) {
            *b = (i % 251) as u8;
        }
        assert_eq!(got, expected);
        self.remote.read(0, &mut got).unwrap();
        assert_eq!(got, (0..8194).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    }
}
#[test]
fn held_completion_cancel_and_peer_retirement_keep_owners_until_drain() {
    let f = Arc::new(Faults::default());
    f.hold.store(true, SeqCst);
    let p = Pair::new(f.clone(), |_| {});
    let b = p.submit(TransferOp::Read, &[p.request(1, 4097)]);
    wait(|| f.intercepted.load(SeqCst) > 0);
    assert!(matches!(b.cancel(0), Err(Error::Unavailable(_))));
    assert!(matches!(
        b.wait(Duration::from_millis(2)),
        Err(Error::Timeout)
    ));
    assert!(p.local.read(1, &mut [0; 1]).is_err());
    assert!(matches!(
        p.engine.close_segment(
            p.source.endpoint(),
            p.source.source_epoch(),
            Duration::from_millis(2)
        ),
        Err(Error::Unquiesced { .. })
    ));
    let s = p.engine.stats().unwrap();
    assert_eq!(s.active_peer_operations, 1);
    assert!(s.fabric.unwrap().memory_regions > 0);
    f.hold.store(false, SeqCst);
    assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Completed);
    p.check(1, 4097);
    p.engine
        .close_segment(p.source.endpoint(), p.source.source_epoch(), WAIT)
        .unwrap();
    assert!(
        p.engine
            .submit_and_wait(TransferOp::Read, &p.peer, &[p.request(1, 1)], WAIT)
            .is_err()
    );
    let s = p.engine.stats().unwrap();
    assert_eq!(s.active_peer_operations, 0);
    assert_eq!(s.fabric.unwrap().peer_addresses, 0);
}
#[test]
fn merged_aliases_use_one_physical_owner_and_one_post() {
    let f = Arc::new(Faults::default());
    let p = Pair::new(f.clone(), |_| {});
    let b = p.submit(TransferOp::Read, &[p.request(1, 4), p.request(5, 4)]);
    assert_eq!(b.wait(WAIT).unwrap().completed_tasks, 2);
    let tasks = b.task_statuses().unwrap();
    assert!(
        tasks
            .iter()
            .all(|t| t.physical_owner == 0 && t.transport == Transport::Fabric)
    );
    // EAGAIN does not count as a posted request. Each real CQE corresponds to a slice.
    assert_eq!(f.intercepted.load(SeqCst), 1);
    p.check(1, 8);
}
#[test]
fn partial_post_cq_error_and_poll_error_never_replay_a_write() {
    for kind in ["partial", "cqe", "poll"] {
        let f = Arc::new(Faults::default());
        if kind == "partial" {
            f.partial_post.store(1, SeqCst);
        }
        if kind == "cqe" {
            f.cq_error.store(true, SeqCst);
        }
        if kind == "poll" {
            f.hold.store(true, SeqCst);
        }
        let p = Pair::new(f.clone(), |_| {});
        let b = p.submit(TransferOp::Write, &[p.request(1, 4097)]);
        if kind == "poll" {
            wait(|| f.intercepted.load(SeqCst) > 0);
            f.poll_error.store(true, SeqCst);
            f.drain_expired.store(true, SeqCst);
        }
        let status = b.wait(WAIT).unwrap();
        assert_eq!(status.state, BatchState::Failed);
        let task = b.task_status(0).unwrap();
        assert_eq!(task.failover_count, 0);
        assert_eq!(
            task.failure.as_ref().unwrap().remote_effect,
            RemoteEffect::Unknown
        );
        assert!(
            matches!(
                task.failure.as_ref().map(|f| &f.error),
                Some(Error::Unquiesced { .. })
            ),
            "{task:?}"
        );
        assert!(p.engine.stats().unwrap().fabric.unwrap().memory_regions > 0);
        assert!(p.local.write(1, &[1]).is_err());
        assert!(matches!(
            p.engine.close(Duration::from_millis(2)),
            Err(Error::Timeout)
        ));
    }
}
#[test]
fn expired_drain_quarantines_posted_slices_and_source_grant() {
    let f = Arc::new(Faults::default());
    f.hold.store(true, SeqCst);
    let p = Pair::new(f.clone(), |_| {});
    let b = p.submit(TransferOp::Read, &[p.request(1, 8)]);
    wait(|| f.intercepted.load(SeqCst) > 0);
    // Advance only the test drain clock after a real provider CQE is withheld.
    f.drain_expired.store(true, SeqCst);
    assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Failed);
    let s = p.engine.stats().unwrap();
    assert!(s.fabric.unwrap().posted_slices > 0);
    assert!(s.inflight_bytes > 0);
    assert!(p.local.read(1, &mut [0; 1]).is_err());
    assert!(p.remote.write(1, &[1]).is_err());
    assert!(matches!(
        p.engine.close(Duration::from_millis(1)),
        Err(Error::Timeout)
    ));
}
#[test]
fn mr_destroy_failure_is_sticky_and_keeps_backing_and_admission() {
    let f = Arc::new(Faults::default());
    f.close_kind.store(6, SeqCst);
    let p = Pair::new(f.clone(), |_| {});
    let b = p.submit(TransferOp::Read, &[p.request(1, 8)]);
    assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Failed);
    assert!(matches!(
        b.task_status(0).unwrap().failure.as_ref().map(|f| &f.error),
        Some(Error::Unquiesced { .. })
    ));
    let attempts = f.closes.load(SeqCst);
    assert_eq!(attempts, 1);
    assert!(p.local.read(1, &mut [0; 1]).is_err());
    assert_eq!(p.engine.stats().unwrap().fabric.unwrap().memory_regions, 1);
    drop(b);
    let _ = p.engine.close(Duration::from_millis(2));
    drop(p);
    assert_eq!(f.closes.load(SeqCst), attempts);
}
#[test]
fn endpoint_and_address_destroy_failures_are_never_retried() {
    for kind in [5, 4, 3, 2, 1, 0] {
        let f = Arc::new(Faults::default());
        let p = Pair::new(f.clone(), |_| {});
        let b = p.submit(TransferOp::Read, &[p.request(1, 8)]);
        b.wait(WAIT).unwrap();
        p.check(1, 8);
        drop(b);
        if kind == 0 {
            f.remove_error.store(true, SeqCst);
        } else {
            f.close_kind.store(kind, SeqCst);
        }
        let r = if kind == 0 {
            p.engine
                .close_segment(p.source.endpoint(), p.source.source_epoch(), WAIT)
        } else {
            p.engine.close(WAIT)
        };
        assert!(matches!(r, Err(Error::Unquiesced { .. })), "{kind}: {r:?}");
        let attempts = f.closes.load(SeqCst);
        assert_eq!(attempts, 1);
        let _ = p.engine.close(Duration::from_millis(2));
        drop(p);
        assert_eq!(f.closes.load(SeqCst), attempts);
    }
}
#[test]
fn registration_rejection_is_unposted_and_never_touches_bytes() {
    let f = Arc::new(Faults::default());
    f.register_error.store(true, SeqCst);
    let p = Pair::new(f.clone(), |_| {});
    let b = p.submit(TransferOp::Read, &[p.request(1, 8)]);
    assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Failed);
    assert_eq!(f.posts.load(SeqCst), 0);
    p.check(0, 0);
    assert_eq!(p.engine.stats().unwrap().fabric.unwrap().memory_regions, 0);
}

#[test]
fn queued_cancellation_and_capacity_exhaustion_post_no_payload() {
    let f = Arc::new(Faults::default());
    f.hold.store(true, SeqCst);
    let p = Pair::new(f.clone(), |c| {
        c.runtime_queue = Some(RuntimeQueueConfig {
            max_dispatch_owners: 1,
            ..Default::default()
        });
    });
    let first = p.submit(TransferOp::Read, &[p.request(1, 8)]);
    wait(|| f.intercepted.load(SeqCst) == 1);
    let queued = p.submit(TransferOp::Read, &[p.request(20, 8)]);
    assert!(queued.cancel(0).unwrap());
    assert_eq!(queued.wait(WAIT).unwrap().state, BatchState::Cancelled);
    assert_eq!(f.posts.load(SeqCst), 1);
    f.hold.store(false, SeqCst);
    first.wait(WAIT).unwrap();
    p.check(1, 8);
    let f = Arc::new(Faults::default());
    let p = Pair::new(f.clone(), |c| {
        c.fabric.as_mut().unwrap().max_pending_ops = 1;
    });
    let b = p.submit(TransferOp::Read, &[p.request(1, 2000)]);
    assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Failed);
    assert_eq!(f.posts.load(SeqCst), 0);
    p.check(0, 0);
}
#[test]
fn policies_and_unposted_fallback_reach_the_executing_owner() {
    for reject in [false, true] {
        let f = Arc::new(Faults::default());
        f.register_error.store(reject, SeqCst);
        let p = Pair::new(f.clone(), |c| {
            c.max_failover_attempts = 3;
            c.selector = Some(
                TransportSelectorConfig::from_json(
                    r#"{"policy":[{"name":"network","segment_type":"memory","transports":["fabric","tcp"]}]}"#,
                )
                .unwrap(),
            );
        });
        let mut r = p.request(1, 8);
        r.transport = Transport::Unspec;
        let b = p.submit(TransferOp::Read, &[r.with_policy_name("network").unwrap()]);
        assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Completed);
        let task = b.task_status(0).unwrap();
        assert_eq!(
            task.transport,
            if reject {
                Transport::Tcp
            } else {
                Transport::Fabric
            }
        );
        assert_eq!(task.failover_count, usize::from(reject));
        p.check(1, 8);
    }
}
#[test]
fn loopback_permissions_peer_retirement_and_new_epoch_are_scoped() {
    let f = Arc::new(Faults::default());
    let p = Pair::new(f, |_| {});
    let self_peer = p
        .source
        .open_segment(p.source.endpoint(), p.source.source_epoch(), WAIT)
        .unwrap();
    let local = p
        .source
        .register_memory(vec![0xee; 8194].into(), Permission::LocalOnly)
        .unwrap();
    let r = TransferRequest::new(
        Transport::Fabric,
        local.base_address() + 1,
        p.remote.base_address() + 1,
        8192,
    );
    assert!(
        p.source
            .submit_and_wait(TransferOp::Read, &self_peer, &[r], WAIT)
            .unwrap()
            .is_complete()
    );
    let mut got = vec![0; 8194];
    local.read(0, &mut got).unwrap();
    assert_eq!(got[0], 0xee);
    assert_eq!(got[8193], 0xee);
    assert_eq!(
        &got[1..8193],
        &(1..8193).map(|i| (i % 251) as u8).collect::<Vec<_>>()
    );
    let private = p
        .source
        .register_memory(vec![0x77; 8].into(), Permission::LocalOnly)
        .unwrap();
    let r = TransferRequest::new(
        Transport::Fabric,
        p.local.base_address() + 1,
        private.base_address(),
        8,
    );
    assert!(
        p.engine
            .submit_and_wait(TransferOp::Read, &p.peer, &[r], WAIT)
            .is_err()
    );
    let ro = p
        .source
        .register_memory(vec![0x77; 8].into(), Permission::RemoteRead)
        .unwrap();
    let updated = p
        .engine
        .open_segment(p.source.endpoint(), p.source.source_epoch(), WAIT)
        .unwrap();
    let r = TransferRequest::new(
        Transport::Fabric,
        p.local.base_address() + 1,
        ro.base_address(),
        8,
    );
    assert!(
        p.engine
            .submit_and_wait(TransferOp::Write, &updated, &[r], WAIT)
            .is_err()
    );
    let other = Arc::new(TransferEngine::new(cfg()).unwrap());
    let remote = other
        .register_memory(vec![0x42; 8].into(), Permission::RemoteRead)
        .unwrap();
    let peer = p
        .engine
        .open_segment(other.endpoint(), other.source_epoch(), WAIT)
        .unwrap();
    p.engine
        .close_segment(p.source.endpoint(), p.source.source_epoch(), WAIT)
        .unwrap();
    let r = TransferRequest::new(
        Transport::Fabric,
        p.local.base_address() + 1,
        remote.base_address(),
        8,
    );
    assert!(
        p.engine
            .submit_and_wait(TransferOp::Read, &peer, &[r], WAIT)
            .unwrap()
            .is_complete()
    );
    p.source.close(WAIT).unwrap();
    let endpoint = p.source.endpoint();
    drop(self_peer);
    drop(updated);
    drop(local);
    drop(p.source);
    let mut c = cfg();
    c.bind_addr = endpoint;
    let restarted = TransferEngine::new(c).unwrap();
    let remote = restarted
        .register_memory(vec![9; 8].into(), Permission::RemoteRead)
        .unwrap();
    let fresh = p
        .engine
        .open_segment(endpoint, restarted.source_epoch(), WAIT)
        .unwrap();
    assert!(
        p.engine
            .open_segment(endpoint, p.peer.source_epoch(), WAIT)
            .is_err()
    );
    let r = TransferRequest::new(
        Transport::Fabric,
        p.local.base_address() + 1,
        remote.base_address(),
        8,
    );
    assert!(
        p.engine
            .submit_and_wait(TransferOp::Read, &fresh, &[r], WAIT)
            .unwrap()
            .is_complete()
    );
}

#[test]
fn queued_provider_work_is_rejected_when_its_peer_is_overdue() {
    let f = Arc::new(Faults::default());
    f.hold.store(true, SeqCst);
    let p = Pair::new(f.clone(), |c| {
        let f = c.fabric.as_mut().unwrap();
        f.max_posted_ops = 1;
        f.op_timeout_ms = 1;
    });
    let b = p.submit(TransferOp::Read, &[p.request(1, 2048)]);
    wait(|| f.intercepted.load(SeqCst) == 1);
    wait(|| p.engine.stats().unwrap().fabric.unwrap().post_calls >= 1);
    // The queued second slice is failed after the first slice becomes overdue.
    // Waiting for terminal state would require the deliberately held CQE.
    wait(|| p.engine.stats().unwrap().fabric.unwrap().overdue_rejections > 0);
    f.hold.store(false, SeqCst);
    assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Failed);
    assert_eq!(f.posts.load(SeqCst), 1);
    assert!(matches!(
        b.task_status(0).unwrap().failure.as_ref().map(|f| &f.error),
        Some(Error::Unquiesced { .. })
    ));
    let mut guards = [0; 1];
    p.local.read(0, &mut guards).unwrap();
    assert_eq!(guards, [0xee]);
    p.local.read(8193, &mut guards).unwrap();
    assert_eq!(guards, [0xee]);
}

#[test]
fn runtime_loading_attributes_and_configuration_are_explicit() {
    use super::super::{fabric_transport::FabricTransport, sys::Api};
    assert!(matches!(
        Api::load_from("/orbite-missing-provider/libfabric.so.1"),
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        Api::load_from("libc.so.6"),
        Err(Error::Unavailable(_))
    ));
    let env = std::env::var_os("FI_HMEM");
    let f = FabricTransport::new(cfg().fabric.unwrap()).unwrap();
    assert!(
        f.contexts
            .iter()
            .all(|c| c.info.delivery_complete != 0 && c.info.tx_size > 0 && c.info.max_message > 0)
    );
    assert!(f.peer.nics.iter().all(|n| n.name == "lo"));
    assert_eq!(std::env::var_os("FI_HMEM"), env);
    f.close().unwrap();
    let c=EngineConfig::from_tent_json(r#"{"transports":{"fabric":{"enable":true,"provider":"tcp","devices":["lo"],"slice_size":1024}}}"#).unwrap();
    assert_eq!(c.fabric.unwrap().slice_size, 1024);
    let mut c = cfg();
    c.fabric.as_mut().unwrap().max_pending_ops = 0;
    assert!(matches!(
        TransferEngine::new(c),
        Err(Error::InvalidConfig(_))
    ));
}

#[test]
fn malformed_finish_cannot_release_a_published_grant() {
    use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
    let f = Arc::new(Faults::default());
    let p = Pair::new(f, |_| {});
    let mut channel = ControlChannel::connect(p.source.endpoint(), Instant::now() + WAIT).unwrap();
    channel
        .write_all(
            &Request {
                operation: Operation::FabricWrite,
                epoch: p.source.source_epoch(),
                region: p.remote.id(),
                offset: 1,
                length: 8,
            }
            .encode(),
        )
        .unwrap();
    let reply = Reply::read(&mut channel, p.source.source_epoch()).unwrap();
    let mut payload = vec![0; reply.length as usize];
    channel.read_exact(&mut payload).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert!(value["nonce"].as_u64().unwrap() > 0);
    channel.write_all(&[0; 24]).unwrap();
    assert!(Reply::read(&mut channel, p.source.source_epoch()).is_err());
    assert!(p.remote.write(1, &[1]).is_err());
    assert!(p.source.stats().unwrap().fabric.unwrap().memory_regions > 0);
    let mut byte = [0];
    p.remote.read(0, &mut byte).unwrap();
    assert_eq!(byte, [0]);
    p.remote.read(9, &mut byte).unwrap();
    assert_eq!(byte, [9]);
}
