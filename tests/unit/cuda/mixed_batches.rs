use crate::test_support::EVENTUAL_PROGRESS;
use std::sync::mpsc;

use super::*;
use crate::engine::tests::{rdma_fixture_with_config, source_runtime};
use crate::runtime::proxy_manager::ProxyManager;
use crate::transport::rdma::tests::gpu_runtime;
use crate::{Notification, NotificationStatus, RemoteEffect, TaskState};

fn engine(selector: Option<crate::TransportSelectorConfig>, retries: usize) -> TransferEngine {
    let mut runtime = gpu_runtime("copy");
    runtime.proxy = ProxyManager::new(7, 2, crate::StagingLocation::Any).unwrap();
    rdma_fixture_with_config(
        runtime,
        EngineConfig {
            selector,
            max_failover_attempts: retries,
            staging_chunk_size: 7,
            staging_chunk_count: 2,
            max_inflight_batches: 1,
            runtime_queue: Some(crate::RuntimeQueueConfig {
                max_dispatch_owners: 1,
                max_dispatch_bytes: 1024,
                deadline_aware: true,
                ..Default::default()
            }),
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
            mixed_batch(operation, outcome, false);
        }
        mixed_batch(operation, Outcome::Complete, true);
    }
}

fn mixed_batch(operation: TransferOp, outcome: Outcome, named: bool) {
    let source = engine(None, 0);
    let selector = named.then(|| crate::TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"direct","segment_type":"memory","transports":["rdma"]},{"name":"stage","segment_type":"memory","transports":["tcp"]}]}"#
    ).unwrap());
    // Leave a real alternate RDMA candidate for the uncertain staged attempt.
    let requester = Arc::new(engine(
        selector,
        if outcome == Outcome::Uncertain { 3 } else { 0 },
    ));
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
    // The two direct ranges are address-adjacent but straddle a staged
    // owner. Only the two adjacent public aliases of that owner may merge.
    let spans = [(2, 4, 5), (20, 23, 6), (26, 29, 7), (7, 9, 9)];
    let requests = spans
        .iter()
        .enumerate()
        .map(|(i, &(local_offset, remote_offset, length))| {
            let staged = i == 1 || i == 2;
            let hint = if named {
                Transport::Unspec
            } else if staged {
                Transport::Tcp
            } else {
                Transport::Rdma
            };
            let r = TransferRequest::new(
                hint,
                local.base_address() + local_offset,
                remote.base_address() + remote_offset,
                length,
            );
            if named {
                r.with_policy_name(if staged { "stage" } else { "direct" })
                    .unwrap()
            } else {
                r
            }
        })
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
        .recv_timeout(EVENTUAL_PROGRESS)
        .expect("middle TCP GPU copy was not reached");
    assert_eq!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .map(|t| t.state)
            .collect::<Vec<_>>(),
        [
            TaskState::Completed,
            TaskState::Running,
            TaskState::Running,
            TaskState::Waiting
        ]
    );
    assert_eq!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .map(|s| s.physical_owner)
            .collect::<Vec<_>>(),
        [0, 1, 1, 3]
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
    // SAFETY: The pending chunk is held at the test gate; the completed prefix is the only
    // writer so far.
    assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &expected_a);
    // SAFETY: The pending chunk is held at the test gate; the completed prefix is the only
    // writer so far.
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
            (BatchState::Completed, 4, 27)
        );
        assert_eq!(submission.completed_requests, 4);
        assert!(matches!(
            *submission.notification,
            NotificationStatus::Delivered
        ));
        assert_eq!(source.receive_notifications().unwrap(), [notification]);
        apply(&mut expected_a, &mut expected_b, 20, 23, 13);
        apply(&mut expected_a, &mut expected_b, 7, 9, 9);
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
            assert_eq!(stats.copied_bytes, 26);
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
            (1, 2, RemoteEffect::None)
        );
        assert_eq!(matches!(failure.error, Error::Unquiesced { .. }), retained);
        for id in [1, 2] {
            let alias = batch.task_status(id).unwrap();
            assert_eq!(
                (alias.failover_count, alias.locally_drained),
                (0, !retained)
            );
            let failed = alias.failure.unwrap();
            assert_eq!(
                (
                    failed.request_index,
                    failed.affected_requests,
                    failed.remote_effect
                ),
                (1, 2, RemoteEffect::None)
            );
        }
        assert_eq!(
            batch
                .task_statuses()
                .unwrap()
                .iter()
                .map(|t| t.state)
                .collect::<Vec<_>>(),
            [
                TaskState::Completed,
                TaskState::Failed,
                TaskState::Failed,
                TaskState::Skipped
            ]
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
            let queue = requester.stats().unwrap().runtime_queue.unwrap();
            assert_eq!((queue.dispatching_owners, queue.dispatching_bytes), (1, 13));
            assert_eq!(queue.queued_owners, 0);
            assert_eq!(requester.stats().unwrap().inflight_bytes, 13);
            assert_eq!(requester.stats().unwrap().staging.retained_chunks, 1);
        } else {
            batch.free().unwrap();
        }
    }
    // SAFETY: The shim has finished the submitted copies and the failed batch cannot post its
    // tail.
    assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &expected_a);
    // SAFETY: The shim has finished the submitted copies and the failed batch cannot post its
    // tail.
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
                        runtime_queue: Some(crate::RuntimeQueueConfig {
                            max_dispatch_owners: 1,
                            max_dispatch_bytes: 1024,
                            deadline_aware: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
            };
            let source = build(true);
            // Outgoing selection does not require accepting incoming Pin.
            let requester = Arc::new(build(false));
            let mut source_bytes: [u8; 96] = std::array::from_fn(|i| (i * 37 + 11) as u8);
            let mut local_bytes: [u8; 96] = std::array::from_fn(|i| (i * 19 + 53) as u8);
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
            // Enqueue a producer on the READ source or WRITE requester. Its
            // generation must be visible to the first Direct access and the
            // later staged copy, including bytes in the destination guards.
            let (origin, image, offset) = if operation == TransferOp::Read {
                (&source_gpu, &mut source_bytes, 3)
            } else {
                (&local_gpu, &mut local_bytes, 1)
            };
            let producer = super::readiness::Producer::new(origin, 0x63, true);
            producer.enqueue(origin.base_address() + offset, 27);
            producer.record(origin).unwrap();
            let producer_owner = Arc::downgrade(&producer);
            drop(producer);
            image[offset as usize..offset as usize + 27].fill(0x63);
            let before_source = source.stats().unwrap().rdma.unwrap();
            let before_requester = requester.stats().unwrap().rdma.unwrap();
            let spans = [(2, 4, 5), (20, 23, 41), (7, 9, 9)];
            let requests: Vec<_> = spans
                .iter()
                .enumerate()
                .map(|(i, &(local_offset, remote_offset, length))| {
                    TransferRequest::new(
                        if i == 1 { transport } else { Transport::Rdma },
                        local.base_address() + local_offset,
                        remote.base_address() + remote_offset,
                        length,
                    )
                    .with_remote_staging(i == 1)
                })
                .collect();
            let notification = Notification {
                name: "mixed-remote".into(),
                message: "done".into(),
            };
            b.query_hold_address.store(
                (local_gpu.base_address() + spans[1].0) as usize,
                Ordering::Release,
            );
            let mut batch = requester
                .submit_async(
                    operation,
                    &peer,
                    &requests,
                    Some(&notification),
                    Duration::from_secs(8),
                )
                .unwrap();
            let until = std::time::Instant::now() + EVENTUAL_PROGRESS;
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
            assert!(producer_owner.upgrade().is_none());
            let status = batch.status().unwrap();
            assert_eq!((status.completed_tasks, status.transferred_bytes), (1, 5));
            assert!(!status.locally_drained);
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            assert!(source.receive_notifications().unwrap().is_empty());
            b.query_hold_address.store(0, Ordering::Release);
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
            let native = requester.stats().unwrap().rdma.unwrap();
            let bytes = 2 * (14 + if transport == Transport::Rdma { 41 } else { 0 });
            assert_eq!(
                (native.completed_read_bytes, native.completed_write_bytes),
                if operation == TransferOp::Read {
                    (bytes, 0)
                } else {
                    (0, bytes)
                }
            );
            let grants = source.stats().unwrap().rdma.unwrap();
            assert_eq!((grants.opened_grant_pds, grants.closed_grant_pds), (4, 4));
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
                assert_eq!(stats.staging.copied_bytes, 2 * 41);
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

#[test]
fn cancelling_a_later_staged_alias_preserves_the_running_stage_and_direct_sibling() -> Result<()> {
    for op in [TransferOp::Read, TransferOp::Write] {
        let src = engine(None, 0);
        let req = Arc::new(engine(None, 0));
        let mut source: [u8; 64] = std::array::from_fn(|i| (3 * i + 1) as u8);
        let mut target: [u8; 64] = std::array::from_fn(|i| (5 * i + 9) as u8);
        let a = allocation(&source);
        let b = allocation(&target);
        let remote = src
            .register_cuda_memory_batch(&[(memory(&a), 0, 64)], Permission::RemoteReadWrite)?
            .remove(0);
        let local = req
            .register_cuda_memory_batch(&[(memory(&b), 0, 64)], Permission::LocalOnly)?
            .remove(0);
        let peer = req.open_segment(src.endpoint(), src.source_epoch(), EVENTUAL_PROGRESS)?;
        let requests = [
            (Transport::Tcp, 1, 2, 4),
            (Transport::Rdma, 25, 30, 5),
            (Transport::Tcp, 5, 6, 3),
            (Transport::Tcp, 8, 9, 5),
        ]
        .map(|(t, l, r, n)| {
            TransferRequest::new(t, local.base_address() + l, remote.base_address() + r, n)
        });
        let note = Notification {
            name: "mixed".into(),
            message: "cancel".into(),
        };
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, paused) = mpsc::sync_channel(1);
        *b.event_gate.lock().unwrap() = Some((entered, paused));
        let mut batch = req.submit_async(op, &peer, &requests, Some(&note), EVENTUAL_PROGRESS)?;
        waiting.recv_timeout(EVENTUAL_PROGRESS).unwrap();
        assert!(matches!(batch.cancel(0), Err(Error::Unavailable(_))));
        assert!(batch.cancel(3)?);
        assert_eq!(batch.task_status(3)?.physical_owner, 2);
        assert_eq!(batch.task_status(0)?.state, TaskState::Running);
        assert_eq!(batch.task_status(1)?.state, TaskState::Waiting);
        assert!(!batch.status()?.locally_drained);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        assert_eq!(source_runtime(&req).stats()?.posted_work_requests, 0);
        assert!(src.receive_notifications()?.is_empty());
        resume.send(()).unwrap();
        let done = batch.wait(EVENTUAL_PROGRESS)?;
        assert!(done.locally_drained);
        assert_eq!((done.completed_tasks, done.transferred_bytes), (2, 9));
        for (i, length) in [4, 5].into_iter().enumerate() {
            let task = batch.task_status(i)?;
            assert_eq!(
                (task.state, task.transferred_bytes),
                (TaskState::Completed, length)
            );
        }
        for i in [2, 3] {
            let task = batch.task_status(i)?;
            assert_eq!((task.physical_owner, task.transferred_bytes), (2, 0));
            assert!(matches!(task.failure.unwrap().error, Error::Cancelled));
        }
        let receipt = batch.submissions()?.remove(0);
        let failure = receipt.failure.as_ref().unwrap();
        assert_eq!(
            (receipt.completed_requests, receipt.transferred_bytes),
            (2, 9)
        );
        assert_eq!(
            (
                failure.request_index,
                failure.affected_requests,
                failure.remote_effect
            ),
            (2, 2, RemoteEffect::None)
        );
        assert!(matches!(*receipt.notification, NotificationStatus::Skipped));
        assert!(src.receive_notifications()?.is_empty());
        for (l, r, n) in [(1, 2, 4), (25, 30, 5)] {
            match op {
                TransferOp::Read => target[l..l + n].copy_from_slice(&source[r..r + n]),
                TransferOp::Write => source[r..r + n].copy_from_slice(&target[l..l + n]),
            }
        }
        for (view, want) in [(&remote, &source), (&local, &target)] {
            let mut got = [0; 64];
            view.read(0, &mut got)?;
            assert_eq!(&got, want);
        }
        batch.free()?;
        assert_eq!(req.stats()?.inflight_bytes, 0);
        assert_eq!(req.stats()?.staging.active_chunks, 0);
        req.close(EVENTUAL_PROGRESS)?;
        src.close(EVENTUAL_PROGRESS)?;
    }
    Ok(())
}
