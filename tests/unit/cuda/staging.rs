use super::*;
use crate::runtime::proxy_manager::ProxyManager;
use std::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[test]
fn tcp_and_rdma_share_bounded_chunks_and_preserve_partial_ranges() {
    let routes = [
        [Transport::Tcp, Transport::Tcp],
        #[cfg(feature = "rdma")]
        [Transport::Rdma, Transport::Rdma],
        #[cfg(feature = "rdma")]
        [Transport::Tcp, Transport::Rdma],
        #[cfg(feature = "rdma")]
        [Transport::Rdma, Transport::Tcp],
    ];
    for route in routes {
        let config = EngineConfig {
            staging_chunk_size: 3,
            staging_chunk_count: 1,
            ..EngineConfig::default()
        };
        let engine = || {
            #[cfg(feature = "rdma")]
            if route.contains(&Transport::Rdma) {
                let mut runtime = crate::transport::rdma::tests::runtime("copy");
                runtime.proxy = ProxyManager::new(3, 1).unwrap();
                return crate::engine::tests::rdma_fixture_with_config(runtime, config.clone());
            }
            TransferEngine::new(config.clone()).unwrap()
        };
        let source = engine();
        let requester = engine();
        let a = allocation(&[0x57; 16]);
        let b = allocation(&[0xcc; 16]);
        let source_gpu = memory(&a);
        let requester_gpu = memory(&b);
        let remote = source
            .register_cuda_memory_batch(&[(source_gpu.clone(), 2, 12)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(requester_gpu.clone(), 1, 14)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(2),
            )
            .unwrap();
        let mut request = TransferRequest {
            transport: route[0],
            local_address: local.base_address() + 2,
            remote_address: remote.base_address() + 1,
            length: 8,
        };
        let pattern = [3, 19, 21, 29, 37, 43, 51, 61];
        for (op, transport) in [TransferOp::Read, TransferOp::Write].into_iter().zip(route) {
            request.transport = transport;
            if op == TransferOp::Write {
                requester_gpu.write(3, &pattern).unwrap();
            }
            let report = requester
                .submit_and_wait(
                    op,
                    &peer,
                    std::slice::from_ref(&request),
                    Duration::from_secs(2),
                )
                .unwrap();
            assert!(
                report.is_complete() && report.locally_drained,
                "{transport:?}: {report:?}"
            );
            assert_eq!(report.transferred_bytes, 8);
            if op == TransferOp::Read {
                let mut observed = [0; 16];
                requester_gpu.read(0, &mut observed).unwrap();
                let mut expected = [0xcc; 16];
                expected[3..11].fill(0x57);
                assert_eq!(observed, expected);
            }
        }
        for engine in [&source, &requester] {
            engine.close_requester(Duration::from_secs(2)).unwrap();
            engine.close_source(Duration::from_secs(2)).unwrap();
            let stats = engine.stats().unwrap().staging;
            assert_eq!(
                (
                    stats.allocations,
                    stats.reuses,
                    stats.completed_chunks,
                    stats.copied_bytes
                ),
                (1, 1, 6, 16)
            );
            assert_eq!(
                (stats.reserved_bytes, stats.idle_chunks, stats.active_chunks),
                (3, 1, 0)
            );
            engine.close(Duration::from_secs(2)).unwrap();
            assert_eq!(engine.stats().unwrap().staging.reserved_chunks, 0);
        }
        let mut bytes = [0; 16];
        requester_gpu.read(0, &mut bytes).unwrap();
        let mut expected = [0xcc; 16];
        expected[3..11].copy_from_slice(&pattern);
        assert_eq!(bytes, expected);
        source_gpu.read(0, &mut bytes).unwrap();
        let mut expected = [0x57; 16];
        expected[3..11].copy_from_slice(&pattern);
        assert_eq!(bytes, expected);
        assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
    }
}

#[test]
fn native_context_key_reuses_wrappers_and_replaces_idle_locations_only_after_free() {
    let pool = ProxyManager::new(3, 1).unwrap();
    let a = allocation(&[1; 8]);
    let b = allocation(&[2; 8]);
    let first = memory(&a);
    let alias = memory(&a);
    let other = memory(&b);
    for gpu in [&first, &alias, &other] {
        gpu.copy_staged(0, &mut [0; 8], None, &pool, deadline())
            .unwrap();
    }
    let stats = pool.stats().unwrap();
    assert_eq!(
        (stats.allocations, stats.reuses, stats.reserved_bytes),
        (2, 1, 3)
    );
    assert!(a.pins.lock().unwrap().is_empty());
    assert_eq!(b.pins.lock().unwrap().len(), 1);
    pool.close(deadline()).unwrap();
    assert!(b.pins.lock().unwrap().is_empty());
}

#[test]
fn active_chunk_keeps_capacity_and_close_waits_for_native_completion() {
    let pool = ProxyManager::new(3, 1).unwrap();
    let a = allocation(&[5; 8]);
    let b = allocation(&[6; 8]);
    let gpu = memory(&a);
    let other = memory(&b);
    let (entered, wait) = std::sync::mpsc::sync_channel(0);
    let (resume, blocked) = std::sync::mpsc::sync_channel(0);
    *a.event_gate.lock().unwrap() = Some((entered, blocked));
    let manager = pool.clone();
    let worker =
        std::thread::spawn(move || gpu.copy_staged(0, &mut [0; 8], None, &manager, deadline()));
    wait.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(pool.stats().unwrap().active_chunks, 1);
    assert!(matches!(
        other.copy_staged(0, &mut [0; 8], None, &pool, deadline()),
        Err(Error::ResourceExhausted)
    ));
    assert!(!b.events.lock().unwrap().contains(&"pin"));
    assert!(matches!(
        pool.close(Instant::now() + Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert_eq!(a.pins.lock().unwrap().len(), 1);
    resume.send(()).unwrap();
    worker.join().unwrap().unwrap();
    pool.close(deadline()).unwrap();
    assert!(a.pins.lock().unwrap().is_empty());
    assert!(matches!(
        other.copy_staged(0, &mut [0; 8], None, &pool, deadline()),
        Err(Error::RequesterClosed)
    ));
}

#[test]
fn chunk_deadline_reports_partial_effect_and_reuses_only_drained_bytes() {
    let pool = ProxyManager::new(3, 1).unwrap();
    let a = allocation(&[0xcc; 8]);
    let gpu = memory(&a);
    let (entered, wait) = std::sync::mpsc::sync_channel(0);
    let (resume, blocked) = std::sync::mpsc::sync_channel(0);
    *a.event_gate.lock().unwrap() = Some((entered, blocked));
    let manager = pool.clone();
    let end = Instant::now() + Duration::from_millis(100);
    let worker_gpu = gpu.clone();
    let worker = std::thread::spawn(move || {
        worker_gpu.copy_staged(0, &mut [], Some(&[7; 8]), &manager, end)
    });
    wait.recv_timeout(Duration::from_secs(1)).unwrap();
    std::thread::sleep(end.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
    resume.send(()).unwrap();
    assert!(matches!(worker.join().unwrap(), Err(Error::Timeout)));
    assert_eq!(
        (
            pool.stats().unwrap().copied_bytes,
            pool.stats().unwrap().idle_chunks
        ),
        (3, 1)
    );
    let mut bytes = [0; 8];
    gpu.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7, 7, 7, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc]);
    pool.close(deadline()).unwrap();
}

#[test]
fn chunk_errors_require_drain_and_context_restore_before_reuse() {
    for fault in ["submit", "record", "wait", "pop", "stream-create"] {
        let pool = ProxyManager::new(3, 1).unwrap();
        let a = allocation(&[7; 8]);
        let weak = Arc::downgrade(&a);
        let gpu = memory(&a);
        match fault {
            "submit" => a.copy_error.store(1, Ordering::Release),
            "record" => a.record_error.store(1, Ordering::Release),
            "wait" => a.sync_error.store(700, Ordering::Release),
            "pop" => a.pop_after_completion.store(true, Ordering::Release),
            _ => a.stream_create_error.store(1, Ordering::Release),
        }
        let result = gpu.copy_staged(0, &mut [0; 8], None, &pool, deadline());
        assert!(result.is_err(), "{fault}");
        let retained = matches!(fault, "wait" | "pop");
        let stats = pool.stats().unwrap();
        assert_eq!(
            (
                stats.completed_chunks,
                stats.retained_chunks,
                stats.idle_chunks
            ),
            (0, usize::from(retained), usize::from(!retained)),
            "{fault}"
        );
        assert_eq!(pool.close(deadline()).is_err(), retained, "{fault}");
        STACK.with(|s| s.borrow_mut().clear());
        drop(gpu);
        drop(a);
        drop(pool);
        assert_eq!(weak.upgrade().is_some(), retained, "{fault}");
    }
}

#[test]
fn partial_pin_and_cleanup_failures_keep_quota_without_retrying_native_free() {
    for fault in ["partial-pin", "pin-pop", "free", "free-pop"] {
        let pool = ProxyManager::new(3, 1).unwrap();
        let a = allocation(&[7; 8]);
        let weak = Arc::downgrade(&a);
        let gpu = memory(&a);
        match fault {
            "partial-pin" => a.pin_error.store(1, Ordering::Release),
            "pin-pop" => a.pop_after_pin.store(true, Ordering::Release),
            _ => (),
        }
        let copied = gpu.copy_staged(0, &mut [0; 8], None, &pool, deadline());
        if fault.starts_with("free") {
            copied.unwrap();
            if fault == "free" {
                a.free_error.store(1, Ordering::Release);
            } else {
                a.pop_after_unpin.store(true, Ordering::Release);
            }
        } else {
            assert!(matches!(copied, Err(Error::Unquiesced { .. })));
        }
        for _ in 0..2 {
            assert!(matches!(
                pool.close(deadline()),
                Err(Error::Unquiesced { .. })
            ));
        }
        assert_eq!(pool.stats().unwrap().retained_chunks, 1);
        assert_eq!(
            a.events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "unpin")
                .count(),
            usize::from(fault.starts_with("free"))
        );
        assert_eq!(a.pins.lock().unwrap().is_empty(), fault == "free-pop");
        STACK.with(|s| s.borrow_mut().clear());
        drop(gpu);
        drop(a);
        drop(pool);
        assert!(weak.upgrade().is_some(), "{fault}");
    }
}

#[test]
fn queued_cancellation_does_not_release_the_running_chunk_or_report_partial_success() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let remote = source
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            staging_chunk_size: 3,
            staging_chunk_count: 1,
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let a = allocation(&[0xcc; 16]);
    let gpu = memory(&a);
    let local = requester
        .register_cuda_memory_batch(&[(gpu.clone(), 0, 16)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(2),
        )
        .unwrap();
    let requests: Vec<_> = [0, 8]
        .into_iter()
        .map(|offset| TransferRequest {
            transport: Transport::Tcp,
            local_address: local.base_address() + offset,
            remote_address: remote.base_address(),
            length: 8,
        })
        .collect();
    let (entered, wait) = std::sync::mpsc::sync_channel(0);
    let (resume, blocked) = std::sync::mpsc::sync_channel(0);
    *a.event_gate.lock().unwrap() = Some((entered, blocked));
    let mut batch = requester
        .submit_async(
            TransferOp::Read,
            &peer,
            &requests,
            None,
            Duration::from_secs(2),
        )
        .unwrap();
    wait.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(batch.cancel(1).unwrap());
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    assert!(matches!(
        requester.close(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert_eq!(requester.stats().unwrap().staging.active_chunks, 1);
    resume.send(()).unwrap();
    let status = batch.wait(Duration::from_secs(2)).unwrap();
    assert_eq!(status.transferred_bytes, 8);
    assert!(status.locally_drained);
    assert_eq!(requester.stats().unwrap().staging.completed_chunks, 3);
    batch.free().unwrap();
    requester.close(Duration::from_secs(2)).unwrap();
    let mut bytes = [0; 16];
    gpu.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[..8], &[7; 8]);
    assert_eq!(&bytes[8..], &[0xcc; 8]);
}

#[cfg(feature = "rdma")]
#[test]
fn staged_rdma_cuda_failure_survives_a_drained_cq_cleanup_error() {
    use crate::engine::tests::rdma_fixture;
    for fault_source in [false, true] {
        let source = Arc::new(rdma_fixture(if fault_source {
            "copy_staging_close_fault"
        } else {
            "copy"
        }));
        let requester = Arc::new(rdma_fixture(if fault_source {
            "copy"
        } else {
            "copy_staging_close_fault"
        }));
        let owner = allocation(&[7; 8]);
        let weak = Arc::downgrade(&owner);
        let gpu = memory(&owner);
        let failed = if fault_source { &source } else { &requester };
        let cuda = failed
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 8)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let (remote, local) = if fault_source {
            (
                cuda.clone(),
                requester
                    .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
                    .unwrap(),
            )
        } else {
            (
                source
                    .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::RemoteWrite)
                    .unwrap(),
                cuda.clone(),
            )
        };
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(2),
            )
            .unwrap();
        owner.sync_error.store(700, Ordering::Release);
        let mut batch = requester
            .submit_async(
                if fault_source {
                    TransferOp::Read
                } else {
                    TransferOp::Write
                },
                &peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: local.base_address(),
                    remote_address: remote.base_address(),
                    length: 8,
                }],
                None,
                Duration::from_secs(2),
            )
            .unwrap();
        let status = batch.wait(Duration::from_secs(2)).unwrap();
        assert_eq!(status.state, BatchState::Failed);
        assert_eq!(status.transferred_bytes, 0);
        assert_eq!(status.locally_drained, fault_source);
        if fault_source {
            batch.free().unwrap();
        } else {
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        }
        assert!(matches!(
            failed.close(Duration::from_millis(5)),
            Err(Error::Timeout)
        ));
        let stats = failed.stats().unwrap();
        assert_eq!(stats.staging.retained_chunks, 1);
        assert_eq!(stats.retained_regions, 1);
        if fault_source {
            assert_eq!(stats.active_source_operations, 1);
        } else {
            assert_eq!(stats.inflight_batches, 1);
        }
        failed.unregister_memory(&cuda).unwrap();
        drop(local);
        drop(remote);
        drop(cuda);
        drop(gpu);
        drop(owner);
        drop(batch);
        assert!(weak.upgrade().is_some());
    }
}
