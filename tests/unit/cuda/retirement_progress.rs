use std::time::Instant;

use super::*;
use crate::RemoteEffect;
use crate::engine::tests::{rdma_fixture_with_config, source_runtime};
use crate::runtime::proxy_manager::ProxyManager;
use crate::transport::rdma::tests::{pause, retirement_progress::RetirementHold, two_rail_runtime};

type PayloadHold = Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>;

fn engine(mode: &str, depth: usize) -> (TransferEngine, PayloadHold) {
    let mut runtime = two_rail_runtime(mode);
    runtime.proxy = ProxyManager::new(7, depth, crate::StagingLocation::Any).unwrap();
    let hold = pause(&runtime);
    (
        rdma_fixture_with_config(
            runtime,
            EngineConfig {
                remote_staging: true,
                staging_chunk_size: 7,
                staging_chunk_count: depth,
                staging_pipeline_depth: depth,
                handler_threads: 1,
                max_inflight_batches: 1,
                ..Default::default()
            },
        ),
        hold,
    )
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(3);
    while !ready() {
        assert!(
            Instant::now() < end,
            "native retirement boundary was not reached"
        );
        std::thread::yield_now();
    }
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Error,
    Retain,
    Stop,
    Deadline,
    Complete,
}

#[test]
fn pending_native_retirement_observes_other_cross_error_stop_and_deadline() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        for outcome in [
            Outcome::Error,
            Outcome::Retain,
            Outcome::Stop,
            Outcome::Deadline,
            Outcome::Complete,
        ] {
            pending_retirement(operation, outcome);
        }
    }
}

fn pending_retirement(operation: TransferOp, outcome: Outcome) {
    let (source, _) = engine("copy", 2);
    let mode = match outcome {
        Outcome::Error => "retire_error",
        Outcome::Retain => "retire_failure",
        _ => "retire",
    };
    let (requester, payload) = engine(mode, 3);
    let requester = Arc::new(requester);
    let a_bytes: [u8; 64] = std::array::from_fn(|i| (i * 37 + 11) as u8);
    let b_bytes: [u8; 64] = std::array::from_fn(|i| (i * 19 + 53) as u8);
    let a = allocation(&a_bytes);
    let b = allocation(&b_bytes);
    let weak = Arc::downgrade(&b);
    let source_gpu = memory(&a);
    let local_gpu = memory(&b);
    let remote = source
        .register_cuda_memory_batch(&[(source_gpu.clone(), 0, 64)], Permission::RemoteReadWrite)
        .unwrap()
        .remove(0);
    let local = requester
        .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 64)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(3),
        )
        .unwrap();
    let requests = [TransferRequest {
        transport: Transport::Rdma,
        local_address: local.base_address() + 2,
        remote_address: remote.base_address() + 1,
        length: 41,
    }];
    let mut batch = requester
        .submit_async(
            operation,
            &peer,
            &requests,
            None,
            if matches!(outcome, Outcome::Deadline) {
                Duration::from_millis(750)
            } else {
                Duration::from_secs(10)
            },
        )
        .unwrap();
    // Drop releases the provider before a failed assertion drops the batch owner.
    let native = RetirementHold::new(source_runtime(&requester));
    wait_until(|| payload.0.lock().unwrap().0 && native.pause.0.lock().unwrap().0);
    native.assert_independent_workers(source_runtime(&requester));
    let stats = requester.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (
            stats.active_grants,
            stats.grant_credits,
            stats.staging_bytes
        ),
        (2, 2, 14)
    );
    assert_eq!(
        (
            stats.opened_connections,
            stats.active_endpoints,
            stats.closed_connections
        ),
        (2, 2, 0)
    );
    assert_eq!(stats.completed_sends, 1);
    assert_eq!(stats.work_requests, 1);
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..43]);
    assert_eq!(source.stats().unwrap().staging.remote_crosses, 0);
    assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
    let mut stop = None;
    if matches!(outcome, Outcome::Stop) {
        let requester = Arc::clone(&requester);
        stop = Some(std::thread::spawn(move || {
            source_runtime(&requester)
                .proxy
                .close(Instant::now() + Duration::from_secs(5))
        }));
    } else if !matches!(outcome, Outcome::Deadline) {
        payload.0.lock().unwrap().1 = true;
        payload.1.notify_all();
    }
    if !matches!(outcome, Outcome::Complete) {
        let observed = native
            .observed
            .recv_timeout(Duration::from_secs(2))
            .expect("native retirement hid another CROSS error, task stop or original deadline");
        assert!(
            match outcome {
                Outcome::Error | Outcome::Retain =>
                    observed.contains("Backend") || observed.contains("Unavailable"),
                Outcome::Stop => observed.contains("Cancelled"),
                Outcome::Deadline => observed.contains("Timeout") || observed.contains("Cancelled"),
                Outcome::Complete => unreachable!(),
            },
            "{operation:?}/{outcome:?}: {observed}"
        );
        assert!(!native.pause.0.lock().unwrap().1);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..43]);
        assert_eq!(requester.stats().unwrap().staging.active_tasks, 1);
        if matches!(outcome, Outcome::Error | Outcome::Retain) {
            assert!(requester.stats().unwrap().rdma.unwrap().completion_errors > 0);
        }
        // No DONE/copy-back/POST or Unpin may outrun native QP/MR retirement.
        assert_eq!(source.stats().unwrap().staging.remote_crosses, 0);
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &a_bytes);
        assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &b_bytes);
    }
    native.release();
    if let Some(stop) = stop {
        stop.join().unwrap().unwrap();
    }
    let report = batch.wait(Duration::from_secs(3)).unwrap();
    let retained = matches!(outcome, Outcome::Retain);
    assert_eq!(
        report.locally_drained, !retained,
        "{operation:?}/{outcome:?}: {report:?}"
    );
    if matches!(outcome, Outcome::Complete) {
        assert_eq!(report.state, BatchState::Completed, "{report:?}");
        batch.free().unwrap();
        let again = requester
            .submit_and_wait(operation, &peer, &requests, Duration::from_secs(3))
            .unwrap();
        assert!(again.is_complete() && again.locally_drained, "{again:?}");
        let mut expected_a = a_bytes;
        let mut expected_b = b_bytes;
        if operation == TransferOp::Read {
            expected_b[2..43].copy_from_slice(&a_bytes[1..42]);
        } else {
            expected_a[1..42].copy_from_slice(&b_bytes[2..43]);
        }
        assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &expected_a);
        assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &expected_b);
        for engine in [&*requester, &source] {
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    stats.opened_connections,
                    stats.closed_connections,
                    stats.active_endpoints
                ),
                (12, 12, 0)
            );
            assert_eq!(stats.reused_connections, 0);
        }
    } else {
        assert!(matches!(
            report.state,
            BatchState::Failed | BatchState::Cancelled
        ));
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert_eq!(
            matches!(failure.error, Error::Unquiesced { .. }),
            retained,
            "{failure:?}"
        );
        assert_eq!(failure.remote_effect, RemoteEffect::None);
        assert_eq!(report.transferred_bytes, 0);
        assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &a_bytes);
        assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &b_bytes);
    }
    source.close(Duration::from_secs(3)).unwrap();
    let stats = requester.stats().unwrap().rdma.unwrap();
    assert_eq!(stats.active_grants, usize::from(retained));
    assert_eq!(stats.active_endpoints, usize::from(retained));
    assert_eq!(stats.staging_bytes, if retained { 7 } else { 0 });
    if retained {
        assert_eq!(requester.stats().unwrap().staging.retained_chunks, 3);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    } else {
        assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
        if !matches!(outcome, Outcome::Complete) {
            batch.free().unwrap();
        }
        requester.close(Duration::from_secs(3)).unwrap();
        assert!(b.pins.lock().unwrap().is_empty());
    }
    drop(native);
    drop(batch);
    drop(local);
    drop(local_gpu);
    drop(requester);
    drop(b);
    assert_eq!(
        weak.upgrade().is_some(),
        retained,
        "actual allocation retention"
    );
}
