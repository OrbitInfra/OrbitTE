use super::requester_staging::engine;
use super::*;

fn wait_until(mut ready: impl FnMut() -> bool) {
    let until = std::time::Instant::now() + Duration::from_secs(2);
    while !ready() {
        assert!(
            std::time::Instant::now() < until,
            "independent staging progress stopped"
        );
        std::thread::yield_now();
    }
}

#[test]
fn local_slot_advances_while_remote_delegate_completion_is_pending() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let source = engine(transport, 3, 1);
            let requester = Arc::new(engine(transport, 3, 3));
            let a = allocation(&[0x57; 32]);
            let b = allocation(&[0xcc; 32]);
            let source_gpu = memory(&a);
            let local_gpu = memory(&b);
            let remote = source
                .register_cuda_memory_batch(
                    &[(source_gpu.clone(), 0, 32)],
                    Permission::RemoteReadWrite,
                )
                .unwrap()
                .remove(0);
            let local = requester
                .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 32)], Permission::LocalOnly)
                .unwrap()
                .remove(0);
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(3),
                )
                .unwrap();
            a.query_hold_address.store(
                remote.base_address() as usize
                    + 1
                    + if operation == TransferOp::Read { 3 } else { 0 },
                Ordering::Release,
            );
            b.query_hold_address.store(
                local.base_address() as usize
                    + 2
                    + if operation == TransferOp::Read { 0 } else { 9 },
                Ordering::Release,
            );
            // Keep one remote generation pending while the local ring advances.
            // READ holds its next PRE; WRITE holds its first POST.
            let mut batch = requester
                .submit_async(
                    operation,
                    &peer,
                    &[TransferRequest {
                        priority: Default::default(),
                        remote_staging: true,
                        transport,
                        local_address: local.base_address() + 2,
                        remote_address: remote.base_address() + 1,
                        length: 17,
                    }],
                    None,
                    Duration::from_secs(5),
                )
                .unwrap();
            wait_until(|| a.event_not_ready.load(Ordering::Acquire) != 0);
            let address = local.base_address() as usize
                + 2
                + if operation == TransferOp::Read { 0 } else { 9 };
            wait_until(|| {
                b.streams.lock().unwrap().values().flatten().any(|copy| {
                    if operation == TransferOp::Read {
                        copy.destination == address
                    } else {
                        copy.source == address
                    }
                })
            });
            assert_eq!(source.stats().unwrap().staging.remote_crosses, 1);
            assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
            assert_eq!(requester.stats().unwrap().staging.active_chunks, 3);
            assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..19]);
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            assert!(matches!(
                requester.close_requester(Duration::from_millis(5)),
                Err(Error::Timeout)
            ));
            b.query_hold_address.store(0, Ordering::Release);
            a.query_hold_address.store(0, Ordering::Release);
            let report = batch.wait(Duration::from_secs(5)).unwrap();
            assert_eq!(
                report.state,
                BatchState::Completed,
                "{transport:?}/{operation:?}: {report:?}"
            );
            assert!(report.locally_drained);
            batch.free().unwrap();
            let mut expected_a = [0x57; 32];
            let mut expected_b = [0xcc; 32];
            if operation == TransferOp::Read {
                expected_b[2..19].fill(0x57);
            } else {
                expected_a[1..18].fill(0xcc);
            }
            let mut bytes = [0; 32];
            source_gpu.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, expected_a);
            local_gpu.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, expected_b);
            requester.close(Duration::from_secs(3)).unwrap();
            source.close(Duration::from_secs(3)).unwrap();
            assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
        }
    }
}

#[test]
#[cfg_attr(not(feature = "rdma"), allow(clippy::single_element_loop))]
fn last_cross_ready_does_not_succeed_or_unpin_without_last_delegate_completion() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        let source = engine(transport, 3, 1);
        let requester = Arc::new(engine(transport, 3, 3));
        let a = allocation(&[0x57; 32]);
        let b = allocation(&[0xcc; 32]);
        let source_gpu = memory(&a);
        let local_gpu = memory(&b);
        let remote = source
            .register_cuda_memory_batch(&[(source_gpu.clone(), 0, 32)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 32)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        a.query_hold_address
            .store(remote.base_address() as usize + 7, Ordering::Release);
        let mut batch = requester
            .submit_async(
                TransferOp::Write,
                &peer,
                &[TransferRequest {
                    priority: Default::default(),
                    remote_staging: true,
                    transport,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 8,
                }],
                None,
                Duration::from_secs(1),
            )
            .unwrap();
        wait_until(|| a.event_not_ready.load(Ordering::Acquire) != 0);
        assert_eq!(source.stats().unwrap().staging.remote_crosses, 3);
        assert_eq!(source.stats().unwrap().staging.remote_delegates, 2);
        let report = batch.wait(Duration::from_secs(2)).unwrap();
        assert!(
            report.state == BatchState::Failed && report.locally_drained,
            "{transport:?}: {report:?}"
        );
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert!(matches!(failure.error, Error::Timeout), "{failure:?}");
        assert_eq!(failure.remote_effect, crate::RemoteEffect::Unknown);
        assert_eq!(report.transferred_bytes, 0);
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        assert_eq!(requester.stats().unwrap().staging.idle_chunks, 3);
        assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
        // A remote timeout is not a GPU drain proof. The source still owns POST.
        assert_eq!(
            a.streams
                .lock()
                .unwrap()
                .values()
                .filter(|c| !c.is_empty())
                .count(),
            1
        );
        assert_eq!(source_gpu.allocation.lock().unwrap().dma, vec![1..9]);
        a.query_hold_address.store(0, Ordering::Release);
        batch.free().unwrap();
        requester.close(Duration::from_secs(3)).unwrap();
        source.close(Duration::from_secs(3)).unwrap();
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        let mut bytes = [0; 32];
        source_gpu.read(0, &mut bytes).unwrap();
        let mut expected = [0x57; 32];
        expected[1..9].fill(0xcc);
        assert_eq!(bytes, expected);
        assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
    }
}

#[test]
#[cfg(feature = "rdma")]
fn cancellation_joins_active_cross_discards_queue_and_preserves_native_uncertainty() {
    for cleanup_failure in [false, true] {
        let transport = Transport::Rdma;
        let source = engine(transport, 3, 2);
        let mut runtime = crate::transport::rdma::tests::runtime(if cleanup_failure {
            "direct:copy_cross_hold_close0"
        } else {
            "direct:copy_cross_hold"
        });
        runtime.proxy =
            crate::runtime::proxy_manager::ProxyManager::new(3, 3, crate::StagingLocation::Any)
                .unwrap();
        let hold = crate::transport::rdma::tests::pause(&runtime);
        let requester = crate::engine::tests::rdma_fixture_with_config(
            runtime,
            EngineConfig {
                remote_staging: true,
                staging_chunk_size: 3,
                staging_chunk_count: 3,
                staging_pipeline_depth: 3,
                handler_threads: 1,
                max_inflight_batches: 1,
                ..EngineConfig::default()
            },
        );
        let requester = Arc::new(requester);
        let a = allocation(&[0x57; 32]);
        let b = allocation(&[0xcc; 32]);
        let weak = Arc::downgrade(&b);
        let local_gpu = memory(&b);
        let remote = source
            .register_cuda_memory_batch(&[(memory(&a), 0, 32)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 32)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        let (local_entered, local_waiting) = std::sync::mpsc::sync_channel(0);
        let (local_resume, local_blocked) = std::sync::mpsc::sync_channel(0);
        *b.copy_gate.lock().unwrap() = Some((local_entered, local_blocked));
        let mut batch = requester
            .submit_async(
                TransferOp::Read,
                &peer,
                &[TransferRequest {
                    priority: Default::default(),
                    remote_staging: true,
                    transport,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 17,
                }],
                None,
                Duration::from_secs(5),
            )
            .unwrap();
        local_waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        wait_until(|| {
            hold.0.lock().unwrap().0 && requester.stats().unwrap().rdma.unwrap().work_requests > 0
        });
        // The first CROSS owns an accepted WR while the second reaches POST.
        // Its local copy failure must cancel queued work and drain that real WR.
        b.copy_error.store(1, Ordering::Release);
        local_resume.send(()).unwrap();
        let report = batch.wait(Duration::from_secs(5)).unwrap();
        assert_eq!(report.state, BatchState::Failed);
        assert_eq!(report.locally_drained, !cleanup_failure, "{report:?}");
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert!(
            if cleanup_failure {
                matches!(failure.error, Error::Unquiesced { .. })
            } else {
                matches!(failure.error, Error::Backend { code: 1, .. })
            },
            "{failure:?}"
        );
        assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
        assert_eq!(requester.stats().unwrap().staging.active_tasks, 0);
        assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
        b.copy_error.store(0, Ordering::Release);
        let mut bytes = [0; 32];
        local_gpu.read(0, &mut bytes).unwrap();
        let mut expected = [0xcc; 32];
        expected[5] = 0x57; // POST of the later CROSS may copy a prefix.
        assert_eq!(bytes, expected);
        source.close(Duration::from_secs(5)).unwrap();
        let stats = source.stats().unwrap().staging;
        assert!(
            stats.remote_crosses <= 2,
            "queued CROSS 2 must never start: {stats:?}"
        );
        assert!(stats.remote_crosses >= 1);
        assert_eq!(stats.remote_unpins, 0);
        if cleanup_failure {
            assert_eq!(requester.stats().unwrap().staging.retained_chunks, 3);
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            assert!(matches!(
                requester.close_requester(Duration::from_millis(5)),
                Err(Error::Timeout)
            ));
        } else {
            batch.free().unwrap();
            requester.close(Duration::from_secs(3)).unwrap();
            assert!(b.pins.lock().unwrap().is_empty());
        }
        drop(batch);
        drop(local);
        drop(local_gpu);
        drop(requester);
        drop(b);
        assert_eq!(
            weak.upgrade().is_some(),
            cleanup_failure,
            "retain the actual pin/context owner on unproven DMA cleanup"
        );
    }
}

#[cfg(feature = "rdma")]
fn panic_on_close(remaining: usize, seen: Arc<AtomicUsize>) {
    crate::transport::rdma::tests::on_requester_close(move |grant, result| {
        assert!(
            result.is_ok(),
            "the injected panic follows native completion"
        );
        assert!(grant.has_resources());
        assert!(
            std::thread::current()
                .name()
                .unwrap()
                .starts_with("orbitte-staging-")
        );
        seen.fetch_add(1, Ordering::AcqRel);
        if remaining == 1 {
            panic!("injected worker panic before private grant cleanup");
        }
        panic_on_close(remaining - 1, seen);
    });
}

#[test]
#[cfg(feature = "rdma")]
fn worker_panic_retains_actual_owners_and_possible_write_effect() {
    use crate::engine::tests::source_runtime;
    use crate::transport::rdma::tests::after_open;
    for (operation, panic_after) in [
        (TransferOp::Read, 1),
        (TransferOp::Write, 1),
        (TransferOp::Write, 2),
    ] {
        let source = engine(Transport::Rdma, 3, 2);
        let requester = Arc::new(engine(Transport::Rdma, 3, 3));
        let a = allocation(&[0x57; 32]);
        let b = allocation(&[0xcc; 32]);
        let source_gpu = memory(&a);
        let local_gpu = memory(&b);
        let weak = Arc::downgrade(&local_gpu);
        let remote = source
            .register_cuda_memory_batch(&[(source_gpu.clone(), 0, 32)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 32)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let hook_seen = Arc::clone(&seen);
        // The native open callback only installs a TLS hook. The panic itself
        // occurs later in close_slices, entirely in Rust, outside an extern C ABI.
        after_open(source_runtime(&requester), move || {
            panic_on_close(panic_after, hook_seen)
        });
        let mut batch = requester
            .submit_async(
                operation,
                &peer,
                &[TransferRequest {
                    priority: Default::default(),
                    remote_staging: true,
                    transport: Transport::Rdma,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 8,
                }],
                None,
                Duration::from_secs(5),
            )
            .unwrap();
        let report = batch.wait(Duration::from_secs(5)).unwrap();
        assert_eq!(
            report.state,
            BatchState::Failed,
            "{operation:?}/{panic_after}: {report:?}"
        );
        assert!(
            !report.locally_drained,
            "panic must not escape into Batch WorkerExit"
        );
        assert_eq!(report.transferred_bytes, 0);
        let submission = batch.submissions().unwrap().remove(0);
        assert_eq!(submission.completed_requests, 0);
        let failure = submission.failure.unwrap();
        assert!(matches!(
            failure.error,
            Error::Unquiesced {
                operation: "staging worker panic",
                code: -1
            }
        ));
        assert_eq!(
            failure.remote_effect,
            if operation == TransferOp::Write {
                crate::RemoteEffect::Unknown
            } else {
                crate::RemoteEffect::None
            }
        );
        assert_eq!(seen.load(Ordering::Acquire), panic_after);
        let stats = requester.stats().unwrap();
        assert_eq!(
            (
                stats.staging.active_tasks,
                stats.staging.retained_chunks,
                stats.inflight_batches
            ),
            (0, 3, 1)
        );
        assert_eq!(stats.staging.cancelled_tasks, 1);
        assert!(b.streams.lock().unwrap().values().all(Vec::is_empty));
        assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
        assert_eq!(b.pins.lock().unwrap().len(), 3);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        assert!(matches!(
            requester.close_requester(Duration::from_millis(10)),
            Err(Error::Timeout)
        ));
        source.close(Duration::from_secs(3)).unwrap();
        let stats = source.stats().unwrap().staging;
        assert_eq!(stats.remote_unpins, 0);
        if operation == TransferOp::Write {
            assert_eq!(stats.remote_delegates, (panic_after - 1) as u64);
        }
        let mut bytes = [0; 32];
        local_gpu.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0xcc; 32]);
        let mut expected = [0x57; 32];
        if operation == TransferOp::Write {
            expected[1..1 + (panic_after - 1) * 3].fill(0xcc);
        }
        source_gpu.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, expected);
        assert!(a.pins.lock().unwrap().is_empty());
        drop(batch);
        drop(local);
        drop(local_gpu);
        drop(requester);
        assert!(
            weak.upgrade().is_some(),
            "unproven worker exit retains actual requester backing"
        );
        assert_eq!(b.pins.lock().unwrap().len(), 3);
    }
}
