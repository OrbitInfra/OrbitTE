use std::sync::mpsc;

use super::*;
use crate::engine::tests::{rdma_fixture_with_config, source_runtime};
use crate::runtime::proxy_manager::ProxyManager;
use crate::transport::rdma::tests::gpu_runtime;
use crate::{Notification, NotificationStatus, RemoteEffect, TaskState};

fn engine() -> TransferEngine {
    let mut runtime = gpu_runtime("copy");
    runtime.proxy = ProxyManager::new(7, 2, crate::StagingLocation::Any).unwrap();
    rdma_fixture_with_config(
        runtime,
        EngineConfig {
            staging_chunk_size: 7,
            staging_chunk_count: 2,
            max_inflight_batches: 1,
            ..Default::default()
        },
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Outcome {
    Complete,
    CopyError,
    Uncertain,
}

#[test]
fn mixed_gpu_batches_preserve_completion_handoff_and_failure_prefix() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        for outcome in [Outcome::Complete, Outcome::CopyError, Outcome::Uncertain] {
            mixed_batch(operation, outcome);
        }
    }
}

fn mixed_batch(operation: TransferOp, outcome: Outcome) {
    let source = engine();
    let requester = Arc::new(engine());
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
    let spans = [(2, 4, 5), (20, 23, 13), (44, 47, 9)];
    let requests = spans
        .iter()
        .enumerate()
        .map(
            |(i, &(local_offset, remote_offset, length))| TransferRequest {
                remote_staging: false,
                transport: if i == 1 {
                    Transport::Tcp
                } else {
                    Transport::Rdma
                },
                local_address: local.base_address() + local_offset,
                remote_address: remote.base_address() + remote_offset,
                length,
            },
        )
        .collect::<Vec<_>>();
    let notification = Notification {
        name: "mixed".into(),
        message: "done".into(),
    };
    let (entered, pending) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    if outcome == Outcome::CopyError {
        b.copy_error.store(1, Ordering::Release);
        *b.copy_gate.lock().unwrap() = Some((entered, gate));
    } else {
        if outcome == Outcome::Uncertain {
            b.event_wait_fail_at.store(1, Ordering::Release);
        }
        *b.event_gate.lock().unwrap() = Some((entered, gate));
    }
    let mut batch = requester
        .submit_async(
            operation,
            &peer,
            &requests,
            Some(&notification),
            Duration::from_secs(10),
        )
        .unwrap();
    pending
        .recv_timeout(Duration::from_secs(3))
        .expect("middle TCP GPU copy was not reached");
    assert_eq!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .map(|t| t.state)
            .collect::<Vec<_>>(),
        [TaskState::Completed, TaskState::Running, TaskState::Waiting]
    );
    let pending = batch.status().unwrap();
    assert_eq!(
        (
            pending.completed_tasks,
            pending.transferred_bytes,
            pending.locally_drained
        ),
        (1, 5, false)
    );
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    assert!(source.receive_notifications().unwrap().is_empty());
    assert_eq!(
        source_runtime(&requester)
            .stats()
            .unwrap()
            .opened_connections,
        1
    );
    assert_eq!(requester.stats().unwrap().inflight_batches, 1);
    let mut expected_a = a_bytes;
    let mut expected_b = b_bytes;
    let apply = |a: &mut [u8; 64], b: &mut [u8; 64], local: usize, remote: usize, length: usize| {
        if operation == TransferOp::Read {
            b[local..local + length].copy_from_slice(&a_bytes[remote..remote + length]);
        } else {
            a[remote..remote + length].copy_from_slice(&b_bytes[local..local + length]);
        }
    };
    apply(&mut expected_a, &mut expected_b, 2, 4, 5);
    assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &expected_a);
    assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &expected_b);
    resume.send(()).unwrap();
    let status = batch.wait(Duration::from_secs(3)).unwrap();
    let submission = batch.submissions().unwrap().remove(0);
    let retained = outcome == Outcome::Uncertain;
    assert_eq!(status.locally_drained, !retained);
    if outcome == Outcome::Complete {
        assert_eq!(
            (
                status.state,
                status.completed_tasks,
                status.transferred_bytes
            ),
            (BatchState::Completed, 3, 27)
        );
        assert_eq!(submission.completed_requests, 3);
        assert!(matches!(
            *submission.notification,
            NotificationStatus::Delivered
        ));
        assert_eq!(source.receive_notifications().unwrap(), [notification]);
        apply(&mut expected_a, &mut expected_b, 20, 23, 13);
        apply(&mut expected_a, &mut expected_b, 44, 47, 9);
        assert_eq!(
            source_runtime(&requester)
                .stats()
                .unwrap()
                .opened_connections,
            2
        );
        batch.free().unwrap();
        let repeated = requester
            .submit_and_wait(operation, &peer, &requests, Duration::from_secs(3))
            .unwrap();
        assert!(repeated.is_complete() && repeated.locally_drained);
        // Each GPU reuses one pooled pinned chunk and one copy stream/event.
        for engine in [&source, requester.as_ref()] {
            let stats = engine.stats().unwrap().staging;
            assert_eq!(
                (
                    stats.allocations,
                    stats.active_chunks,
                    stats.retained_chunks
                ),
                (1, 0, 0)
            );
            assert!(stats.reuses >= 1);
        }
    } else {
        assert_eq!(
            (
                status.state,
                status.completed_tasks,
                status.transferred_bytes
            ),
            (BatchState::Failed, 1, 5)
        );
        let failure = submission.failure.as_ref().unwrap();
        assert_eq!(
            (
                failure.request_index,
                failure.affected_requests,
                failure.remote_effect
            ),
            (1, 1, RemoteEffect::None)
        );
        assert_eq!(matches!(failure.error, Error::Unquiesced { .. }), retained);
        assert_eq!(
            batch
                .task_statuses()
                .unwrap()
                .iter()
                .map(|t| t.state)
                .collect::<Vec<_>>(),
            [TaskState::Completed, TaskState::Failed, TaskState::Skipped]
        );
        assert!(matches!(
            *submission.notification,
            NotificationStatus::Skipped
        ));
        assert!(source.receive_notifications().unwrap().is_empty());
        assert_eq!(
            source_runtime(&requester)
                .stats()
                .unwrap()
                .opened_connections,
            1
        );
        if outcome == Outcome::CopyError && operation == TransferOp::Read {
            // A failed H2D enqueue can leave one byte copied after successful drain.
            expected_b[20] = a_bytes[23];
        }
        if retained {
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            assert_eq!(requester.stats().unwrap().inflight_batches, 1);
            assert_eq!(requester.stats().unwrap().staging.retained_chunks, 1);
        } else {
            batch.free().unwrap();
        }
    }
    assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &expected_a);
    assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &expected_b);
    if !retained {
        requester.close(Duration::from_secs(3)).unwrap();
    }
    source.close(Duration::from_secs(3)).unwrap();
    drop((batch, requester, local, local_gpu, b));
    assert_eq!(weak.upgrade().is_some(), retained);
}

#[test]
fn per_request_staging_mixes_with_gpu_direct_and_reuses_bounded_resources() {
    for (transport, mode) in [
        (Transport::Tcp, "copy"),
        (Transport::Rdma, "copy"),
        (Transport::Rdma, "direct:copy"),
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let build = |allow_pin| {
                let mut runtime = gpu_runtime(mode);
                runtime.proxy = ProxyManager::new(7, 2, crate::StagingLocation::Any).unwrap();
                rdma_fixture_with_config(
                    runtime,
                    EngineConfig {
                        remote_staging: allow_pin,
                        staging_chunk_size: 7,
                        staging_chunk_count: 2,
                        staging_pipeline_depth: 2,
                        handler_threads: 1,
                        max_inflight_batches: 1,
                        ..Default::default()
                    },
                )
            };
            let source = build(true);
            // Outgoing selection does not require accepting incoming Pin.
            let requester = Arc::new(build(false));
            let source_bytes: [u8; 96] = std::array::from_fn(|i| (i * 37 + 11) as u8);
            let local_bytes: [u8; 96] = std::array::from_fn(|i| (i * 19 + 53) as u8);
            let a = allocation(&source_bytes);
            let b = allocation(&local_bytes);
            let source_gpu = memory(&a);
            let local_gpu = memory(&b);
            let remote = source
                .register_cuda_memory_batch(
                    &[(source_gpu.clone(), 0, 96)],
                    Permission::RemoteReadWrite,
                )
                .unwrap()
                .remove(0);
            let local = requester
                .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 96)], Permission::LocalOnly)
                .unwrap()
                .remove(0);
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(3),
                )
                .unwrap();
            let before_source = source.stats().unwrap().rdma.unwrap();
            let before_requester = requester.stats().unwrap().rdma.unwrap();
            let spans = [(2, 4, 5), (20, 23, 41), (74, 77, 9)];
            let requests: Vec<_> = spans
                .iter()
                .enumerate()
                .map(
                    |(i, &(local_offset, remote_offset, length))| TransferRequest {
                        remote_staging: i == 1,
                        transport: if i == 1 { transport } else { Transport::Rdma },
                        local_address: local.base_address() + local_offset,
                        remote_address: remote.base_address() + remote_offset,
                        length,
                    },
                )
                .collect();
            let notification = Notification {
                name: "mixed-remote".into(),
                message: "done".into(),
            };
            b.query_hold.store(true, Ordering::Release);
            let mut batch = requester
                .submit_async(
                    operation,
                    &peer,
                    &requests,
                    Some(&notification),
                    Duration::from_secs(8),
                )
                .unwrap();
            let until = std::time::Instant::now() + Duration::from_secs(3);
            while b.event_queries.load(Ordering::Acquire) == 0 {
                assert!(
                    std::time::Instant::now() < until,
                    "{transport:?}/{mode}/{operation:?}: middle remote staging was not reached: {:?}",
                    batch.status()
                );
                std::thread::yield_now();
            }
            assert_eq!(
                batch
                    .task_statuses()
                    .unwrap()
                    .iter()
                    .map(|t| t.state)
                    .collect::<Vec<_>>(),
                [TaskState::Completed, TaskState::Running, TaskState::Waiting]
            );
            let status = batch.status().unwrap();
            assert_eq!((status.completed_tasks, status.transferred_bytes), (1, 5));
            assert!(!status.locally_drained);
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            assert!(source.receive_notifications().unwrap().is_empty());
            b.query_hold.store(false, Ordering::Release);
            let status = batch.wait(Duration::from_secs(3)).unwrap();
            assert_eq!(
                status.state,
                BatchState::Completed,
                "{transport:?}/{mode}/{operation:?}: {:?}",
                batch.submissions().unwrap()
            );
            assert_eq!(
                (
                    status.completed_tasks,
                    status.transferred_bytes,
                    status.locally_drained
                ),
                (3, 55, true)
            );
            assert_eq!(source.receive_notifications().unwrap(), [notification]);
            batch.free().unwrap();
            let repeated = requester
                .submit_and_wait(operation, &peer, &requests, Duration::from_secs(3))
                .unwrap();
            assert!(
                repeated.is_complete() && repeated.locally_drained,
                "{transport:?}/{mode}/{operation:?}: {repeated:?}"
            );
            let mut expected_a = source_bytes;
            let mut expected_b = local_bytes;
            for &(local, remote, length) in &spans {
                let (local, remote) = (local as usize, remote as usize);
                if operation == TransferOp::Read {
                    expected_b[local..local + length]
                        .copy_from_slice(&source_bytes[remote..remote + length]);
                } else {
                    expected_a[remote..remote + length]
                        .copy_from_slice(&local_bytes[local..local + length]);
                }
            }
            let mut observed = [0; 96];
            source_gpu.read(0, &mut observed).unwrap();
            assert_eq!(observed, expected_a);
            local_gpu.read(0, &mut observed).unwrap();
            assert_eq!(observed, expected_b);
            let stats = source.stats().unwrap().staging;
            assert_eq!(
                (stats.remote_pins, stats.remote_unpins, stats.remote_crosses),
                (2, 2, 12)
            );
            assert_eq!(requester.stats().unwrap().staging.remote_pins, 0);
            for (engine, before) in [
                (&source, before_source),
                (requester.as_ref(), before_requester),
            ] {
                let stats = engine.stats().unwrap();
                assert_eq!(
                    (
                        stats.staging.allocations,
                        stats.staging.active_chunks,
                        stats.staging.retained_chunks
                    ),
                    (2, 0, 0)
                );
                assert!(stats.staging.reuses >= 2);
                let rdma = stats.rdma.unwrap();
                assert_eq!(
                    rdma.persistent_mrs, before.persistent_mrs,
                    "private host MRs must retire while GPU MRs stay cached"
                );
                assert_eq!(rdma.retained_persistent_mrs, 0);
                assert_eq!(
                    rdma.opened_persistent_mrs > before.opened_persistent_mrs,
                    transport == Transport::Rdma && mode.starts_with("direct:")
                );
                assert_eq!(stats.retained_regions, 1);
                engine.close(Duration::from_secs(3)).unwrap();
            }
            assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
        }
    }
}
