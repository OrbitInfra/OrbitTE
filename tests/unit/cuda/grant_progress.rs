use std::time::Instant;

use super::*;
use crate::RemoteEffect;
use crate::engine::tests::{rdma_fixture_with_config, source_runtime};
use crate::runtime::proxy_manager::ProxyManager;
use crate::transport::rdma::tests::{
    HeldConnection, HeldPublication, hold_second_connection, pause, two_rail_runtime,
};

type PayloadHold = Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>;

fn engine(mode: &str, depth: usize) -> (TransferEngine, PayloadHold) {
    let mut runtime = two_rail_runtime(mode);
    runtime.proxy = ProxyManager::new(7, depth, crate::StagingLocation::Any).unwrap();
    let hold = pause(&runtime);
    let engine = rdma_fixture_with_config(
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
    );
    (engine, hold)
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(3);
    while !ready() {
        assert!(
            Instant::now() < until,
            "did not reach the actual cold acquisition boundary"
        );
        std::thread::yield_now();
    }
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Complete,
    Error,
    Retain,
    Stop,
    Deadline,
}

enum Pending {
    Connection(HeldConnection),
    Registration(HeldPublication),
}

impl Pending {
    fn polls(&self) -> usize {
        match self {
            Self::Connection(held) => held.polls(),
            Self::Registration(held) => held.polls(),
        }
    }
    fn assert_held(&self) {
        match self {
            Self::Connection(held) => held.assert_held(),
            Self::Registration(held) => held.assert_held(),
        }
    }
}

fn pending_cross(operation: TransferOp, outcome: Outcome, registration: bool) {
    let mode = match outcome {
        Outcome::Error => "error",
        Outcome::Retain => "retain",
        _ => "hold",
    };
    let (source, _) = engine("copy", 2);
    let (requester, hold) = engine(mode, 3);
    let requester = Arc::new(requester);
    let source_bytes: [u8; 64] = std::array::from_fn(|i| (i * 37 + 11) as u8);
    let local_bytes: [u8; 64] = std::array::from_fn(|i| (i * 19 + 53) as u8);
    let a = allocation(&source_bytes);
    let b = allocation(&local_bytes);
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
    let mut cold = Some(if registration {
        Pending::Registration(HeldPublication::arm(source_runtime(&requester), 2))
    } else {
        Pending::Connection(hold_second_connection(&requester, &source, operation))
    });
    let retained = matches!(outcome, Outcome::Retain)
        || (registration && !matches!(outcome, Outcome::Complete));
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
    wait_until(|| hold.0.lock().unwrap().0);
    wait_until(|| cold.as_ref().unwrap().polls() >= 3);
    cold.as_ref().unwrap().assert_held();
    let stats = requester.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (stats.active_grants, stats.grant_credits, stats.direct_bytes),
        (2, 2, 14),
        "one lease per CROSS across repeated polls: {stats:?}"
    );
    assert_eq!(
        (stats.opened_connections, stats.active_endpoints),
        (1, 1 + usize::from(registration))
    );
    if registration {
        assert_eq!(
            (
                stats.opened_persistent_mrs,
                stats.persistent_mrs,
                stats.reused_persistent_mrs
            ),
            (2, 2, 0)
        );
    }
    assert_eq!(stats.posted_work_requests, 1);
    assert_eq!(stats.work_requests, 1);
    let source_stats = source.stats().unwrap().rdma.unwrap();
    assert_eq!(
        source_stats.opened_connections, 1,
        "no second HELLO/QP before acquisition"
    );
    assert_eq!(source_stats.completed_window_binds, 1);
    let gpu_loans = local_gpu.allocation.lock().unwrap().dma.clone();
    assert_eq!(gpu_loans, vec![2..43]);
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    hold.0.lock().unwrap().1 = true;
    hold.1.notify_all();
    if matches!(outcome, Outcome::Complete) {
        drop(cold.take());
    } else if matches!(outcome, Outcome::Stop) {
        wait_until(|| requester.stats().unwrap().rdma.unwrap().work_requests == 0);
        cold.as_ref().unwrap().assert_held();
        let stopped = source_runtime(&requester)
            .proxy
            .close(Instant::now() + Duration::from_secs(2));
        if registration {
            assert!(matches!(
                stopped,
                Err(Error::Unquiesced {
                    operation: "staging chunk retention",
                    ..
                })
            ));
        } else {
            stopped.expect("worker stop was hidden by cold acquisition");
        }
    }
    let report = batch.wait(Duration::from_secs(2)).expect(
        "pending construction hid another CROSS failure/stop until its builder was released",
    );
    if matches!(outcome, Outcome::Complete) {
        assert!(
            report.state == BatchState::Completed && report.locally_drained,
            "{report:?}"
        );
        batch.free().unwrap();
        let again = requester
            .submit_and_wait(operation, &peer, &requests, Duration::from_secs(3))
            .unwrap();
        assert!(again.is_complete() && again.locally_drained, "{again:?}");
        let mut expected_a = source_bytes;
        let mut expected_b = local_bytes;
        if operation == TransferOp::Read {
            expected_b[2..43].copy_from_slice(&source_bytes[1..42]);
        } else {
            expected_a[1..42].copy_from_slice(&local_bytes[2..43]);
        }
        assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &expected_a);
        assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &expected_b);
        for engine in [&*requester, &source] {
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!((stats.opened_connections, stats.active_endpoints), (2, 2));
            assert!(stats.reused_connections > 0);
            assert_eq!(
                (stats.active_grants, stats.grant_credits, stats.direct_bytes),
                (0, 0, 0)
            );
        }
    } else {
        cold.as_ref().unwrap().assert_held();
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert!(
            if retained {
                matches!(failure.error, Error::Unquiesced { .. })
            } else {
                match outcome {
                    Outcome::Error => {
                        matches!(failure.error, Error::Backend { .. } | Error::Unavailable(_))
                    }
                    Outcome::Retain => matches!(failure.error, Error::Unquiesced { .. }),
                    Outcome::Stop => matches!(failure.error, Error::Cancelled),
                    Outcome::Deadline => matches!(failure.error, Error::Timeout),
                    Outcome::Complete => unreachable!(),
                }
            },
            "{operation:?}/{outcome:?}: {failure:?}"
        );
        assert_eq!(
            report.state,
            if matches!(outcome, Outcome::Stop) && !retained {
                BatchState::Cancelled
            } else {
                BatchState::Failed
            }
        );
        assert_eq!(report.locally_drained, !retained);
        assert_eq!(report.transferred_bytes, 0);
        assert_eq!(failure.remote_effect, RemoteEffect::None);
        assert_eq!(requester.stats().unwrap().staging.active_tasks, 0);
        let stats = requester.stats().unwrap().rdma.unwrap();
        assert_eq!(stats.opened_connections, 1);
        if matches!(outcome, Outcome::Error | Outcome::Retain) {
            assert!(
                stats.completion_errors > 0,
                "the other CROSS error must actually be consumed"
            );
        }
        assert_eq!(
            stats.active_grants,
            usize::from(matches!(outcome, Outcome::Retain)),
            "pending lease must return even when its sibling is retained"
        );
        assert_eq!(
            stats.direct_bytes,
            if matches!(outcome, Outcome::Retain) {
                7
            } else {
                0
            }
        );
        assert_eq!(
            stats.grant_credits,
            usize::from(matches!(outcome, Outcome::Retain))
        );
        assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &source_bytes);
        assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &local_bytes);
        drop(cold.take());
        if retained {
            let retained_chunks = if registration && !matches!(outcome, Outcome::Retain) {
                2
            } else {
                3
            };
            assert_eq!(
                requester.stats().unwrap().staging.retained_chunks,
                retained_chunks
            );
            // CUDA copies drained; RDMA still owns pinned host backing.
            assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
            assert!((retained_chunks..=3).contains(&b.pins.lock().unwrap().len()));
            assert!(matches!(
                requester.close_requester(Duration::from_millis(5)),
                Err(Error::Timeout)
            ));
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        } else {
            assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
            batch.free().unwrap();
        }
    }
    source.close(Duration::from_secs(3)).unwrap();
    if !retained {
        requester.close(Duration::from_secs(3)).unwrap();
        assert!(b.pins.lock().unwrap().is_empty());
    }
    assert!(a.pins.lock().unwrap().is_empty());
    drop(batch);
    drop(local);
    drop(local_gpu);
    drop(requester);
    drop(b);
    assert_eq!(weak.upgrade().is_some(), retained);
}

#[test]
fn cold_acquisition_keeps_other_cross_errors_and_native_retention_observable() {
    for outcome in [Outcome::Error, Outcome::Retain] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            pending_cross(operation, outcome, false);
        }
    }
}

#[test]
fn cold_acquisition_releases_only_its_lease_on_deadline_or_worker_stop() {
    for outcome in [Outcome::Deadline, Outcome::Stop] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            pending_cross(operation, outcome, false);
        }
    }
}

#[test]
fn cold_acquisition_resumes_and_reuses_the_same_two_pairs_after_builder_rollback() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        pending_cross(operation, Outcome::Complete, false);
    }
}

#[test]
fn mr_publication_keeps_other_cross_errors_and_native_retention_observable() {
    for outcome in [Outcome::Error, Outcome::Retain] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            pending_cross(operation, outcome, true);
        }
    }
}

#[test]
fn mr_publication_returns_waiter_lease_but_retains_creator_backing_on_stop_or_deadline() {
    for outcome in [Outcome::Deadline, Outcome::Stop] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            pending_cross(operation, outcome, true);
        }
    }
}

#[test]
fn mr_publication_resumes_without_duplicate_mrs_or_connection_pairs() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        pending_cross(operation, Outcome::Complete, true);
    }
}
