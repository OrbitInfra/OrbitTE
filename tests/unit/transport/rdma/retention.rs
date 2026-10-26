//! Retained native resources keep completion debt and DMA loans owned.
use super::*;

#[path = "retention/native.rs"]
mod native;
pub(super) use native::Retirement;

pub(in super::super) fn retained(
    engine: Arc<TransferEngine>,
    memory: crate::RegisteredMemory,
    backing: Arc<HostMemory>,
    weak: std::sync::Weak<HostMemory>,
    kind: &str,
) -> RdmaStats {
    let debt = kind != "pd";
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let rdma = engine.stats().unwrap().rdma.unwrap();
        if match kind {
            "qp" => rdma.retained_endpoints > 0,
            "cq" => rdma.retained_cqs > 0,
            "channel" => rdma.retained_completion_channels > 0,
            "pd" => rdma.retained_devices > 0,
            _ => panic!("unsupported cleanup owner"),
        } {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "native destruction did not report retention"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let mut offset = None;
    let requester = {
        let trace = TRACE.get().unwrap().lock().unwrap();
        let retained = trace.retirement.as_ref().unwrap();
        retained.check();
        assert_eq!(retained.kind, kind);
        if debt {
            assert!(trace.registered > trace.deregistered && !trace.active_mrs.is_empty());
            let request = trace.injected_record.as_ref().unwrap().lock().unwrap();
            assert!(request.phase == Phase::Retained);
            let resources = request.resources.as_ref().unwrap();
            let connection = resources.connection.lock().unwrap();
            assert_eq!(connection.closed, kind != "qp");
            assert_eq!(connection.lease.is_none(), kind != "qp");
            if kind == "qp" {
                assert_eq!(connection.handles.qp as usize, retained.qp);
            }
            drop(connection);
            let grant = resources.grant().unwrap();
            assert!(trace.active_mrs.contains(&(grant.handles.mr as usize)));
            offset =
                Some((grant.buffer.address().as_ptr() as u64 - memory.base_address()) as usize);
            assert!(
                request.work.as_ref().is_some_and(|w| w.queue.pending())
                    || request.resources.as_ref().is_some_and(|r| r
                        .connection
                        .lock()
                        .unwrap()
                        .receive_queue
                        .outstanding()
                        != 0)
            );
        } else {
            assert_eq!(trace.registered, trace.deregistered);
            assert!(trace.active_mrs.is_empty());
        }
        trace.requester
    };
    if let Some(offset) = offset {
        assert!(
            matches!(memory.write(offset, &[0xff]), Err(Error::Unavailable(_))),
            "retained native debt lost its DMA loan"
        );
    }
    if debt {
        engine.unregister_memory(&memory).unwrap();
    }
    if debt {
        let before = engine.stats().unwrap().rdma.unwrap();
        source_runtime(&engine).recover_progress().unwrap();
        let after = engine.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                after.retained_endpoints,
                after.retained_cqs,
                after.retained_completion_channels,
                after.retained_grants
            ),
            (
                before.retained_endpoints,
                before.retained_cqs,
                before.retained_completion_channels,
                before.retained_grants
            )
        );
    }
    for _ in 0..2 {
        assert!(
            matches!(
                engine.close(Duration::from_millis(50)),
                Err(Error::Timeout | Error::Unquiesced { .. })
            ),
            "retained engine falsely reported drain"
        );
    }
    let outer = engine.stats().unwrap();
    let stats = outer.rdma.unwrap();
    if debt {
        assert!(stats.retained_grants > 0);
        assert_eq!(stats.retained_cqs > 0, kind != "channel");
        if kind == "channel" {
            assert!(stats.retained_completion_channels > 0);
        }
        assert!(stats.retained_work_requests + stats.retained_receive_work_requests > 0);
        assert!(stats.persistent_mrs > 0 && stats.direct_bytes > 0);
        assert!(if requester {
            outer.inflight_batches > 0
        } else {
            outer.active_source_operations > 0
        });
        assert_eq!(stats.retained_endpoints > 0, kind == "qp");
        assert_eq!(
            stats.opened_connections - stats.closed_connections,
            stats.active_endpoints as u64
        );
    } else {
        assert!(stats.retained_devices > 0 && stats.active_devices > 0);
        assert_eq!(
            (outer.inflight_batches, outer.active_source_operations),
            (0, 0)
        );
        assert_eq!(
            (
                stats.active_cqs,
                stats.active_grants,
                stats.persistent_mrs,
                stats.work_requests,
                stats.receive_work_requests,
                stats.receive_credits
            ),
            (0, 0, 0, 0, 0, 0)
        );
    }
    {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        trace.owners.clear();
        trace.injected_record.take();
        trace.memory.take();
    }
    drop(memory);
    drop(backing);
    drop(engine);
    let backing_retained = weak.upgrade().is_some();
    assert_eq!(
        backing_retained, debt,
        "observer references masked backing lifetime"
    );
    let trace = TRACE.get().unwrap().lock().unwrap();
    let retained = trace.retirement.as_ref().unwrap();
    retained.check();
    println!(
        "ORBITTE_CLEANUP_PROOF {{\"kind\":\"{kind}\",\"code\":{},\"attempts\":{},\"retained_endpoints\":{},\"retained_channels\":{},\"backing_retained\":{backing_retained},\"native_probe_after_drop\":true,\"syscall_fault_injection\":{}}}",
        retained.error,
        retained.attempts,
        stats.retained_endpoints,
        stats.retained_completion_channels,
        kind == "qp"
    );
    println!(
        "ORBITTE_NATIVE_RETENTION {{\"kind\":\"{kind}\",\"requester\":{},\"native_error\":{},\"native_attempts\":{},\"native_blocker_qp\":{},\"native_blocker_mw\":{},\"native_pd\":{},\"native_cq\":{},\"registered_mrs\":{},\"deregistered_mrs\":{},\"retained_cqs\":{},\"retained_devices\":{},\"retained_endpoints\":{},\"retained_grants\":{},\"sq_debt\":{},\"rq_debt\":{},\"rq_credits\":{},\"inflight_batches\":{},\"source_operations\":{},\"backing_retained\":{backing_retained},\"observer_owners_removed\":true,\"post_drop_native_blocker_alive\":true,\"drained\":false}}",
        trace.requester,
        retained.error,
        retained.attempts,
        retained.qp,
        retained.mw,
        retained.pd,
        retained.cq,
        trace.registered,
        trace.deregistered,
        stats.retained_cqs,
        stats.retained_devices,
        stats.retained_endpoints,
        stats.retained_grants,
        stats.work_requests,
        stats.receive_work_requests,
        stats.receive_credits,
        outer.inflight_batches,
        outer.active_source_operations
    );
    stats
}
