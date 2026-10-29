use super::requester_staging::engine;
use super::*;

fn pending(device: &Device) -> usize {
    device
        .streams
        .lock()
        .unwrap()
        .values()
        .filter(|v| v.is_some())
        .count()
}

#[test]
fn requester_refill_errors_drain_other_slots_and_preserve_effects() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            for fault in ["submit", "record"] {
                let source = engine(transport, 3, 2);
                let requester = Arc::new(engine(transport, 3, 3));
                let a = allocation(&[0x57; 64]);
                let b = allocation(&[0xcc; 64]);
                let source_gpu = memory(&a);
                let local_gpu = memory(&b);
                let remote = source
                    .register_cuda_memory_batch(
                        &[(source_gpu.clone(), 0, 64)],
                        Permission::RemoteReadWrite,
                    )
                    .unwrap()
                    .remove(0);
                let local = requester
                    .register_cuda_memory_batch(
                        &[(local_gpu.clone(), 0, 64)],
                        Permission::LocalOnly,
                    )
                    .unwrap()
                    .remove(0);
                let peer = requester
                    .open_segment(
                        source.endpoint(),
                        source.source_epoch(),
                        Duration::from_secs(5),
                    )
                    .unwrap();
                b.query_hold.store(true, Ordering::Release);
                let mut batch = requester
                    .submit_async(
                        operation,
                        &peer,
                        &[TransferRequest {
                            transport,
                            local_address: local.base_address() + 2,
                            remote_address: remote.base_address() + 1,
                            length: 32,
                        }],
                        None,
                        Duration::from_secs(5),
                    )
                    .unwrap();
                wait_pending(&b, 3);
                // A queued copy precedes its event record. Hold all initial
                // generations until the refill fault can no longer hit setup.
                let until = std::time::Instant::now() + Duration::from_secs(3);
                while b.completions.lock().unwrap().values().flatten().count() != 3 {
                    assert!(
                        std::time::Instant::now() < until,
                        "initial events not recorded"
                    );
                    std::thread::yield_now();
                }
                assert_eq!(pending(&b), 3);
                if fault == "submit" {
                    b.copy_error.store(1, Ordering::Release);
                } else {
                    b.record_error.store(1, Ordering::Release);
                }
                b.query_hold.store(false, Ordering::Release);
                let report = batch.wait(Duration::from_secs(5)).unwrap();
                let submission = batch.submissions().unwrap().remove(0);
                assert_eq!(
                    report.state,
                    BatchState::Failed,
                    "{transport:?}/{operation:?}/{fault}: {report:?}"
                );
                assert!(report.locally_drained);
                assert_eq!(
                    (submission.completed_requests, report.transferred_bytes),
                    (0, 0)
                );
                let failure = submission.failure.unwrap();
                assert!(
                    matches!(failure.error, Error::Backend { code: 1, .. }),
                    "{failure:?}"
                );
                assert_eq!(
                    failure.remote_effect,
                    if operation == TransferOp::Write {
                        crate::RemoteEffect::Unknown
                    } else {
                        crate::RemoteEffect::None
                    }
                );
                assert_eq!(pending(&b), 0);
                assert_eq!(
                    b.event_waits.load(Ordering::Acquire)
                        + b.event_query_results.load(Ordering::Acquire),
                    3
                );
                assert_eq!(
                    b.events
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|e| **e == "stream-sync")
                        .count(),
                    1
                );
                assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
                let stats = requester.stats().unwrap().staging;
                assert_eq!(
                    (
                        stats.completed_chunks,
                        stats.idle_chunks,
                        stats.retained_chunks
                    ),
                    (3, 3, 0)
                );
                source.close(Duration::from_secs(5)).unwrap();
                let source_stats = source.stats().unwrap().staging;
                let crossed = source_stats.remote_crosses;
                assert!(
                    if operation == TransferOp::Read {
                        (4..=6).contains(&crossed)
                    } else {
                        (1..=3).contains(&crossed)
                    },
                    "{source_stats:?}"
                );
                assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
                b.copy_error.store(0, Ordering::Release);
                b.record_error.store(0, Ordering::Release);
                let mut observed = [0; 64];
                let mut expected = [0xcc; 64];
                if operation == TransferOp::Read {
                    let copies = b.submitted_copies.lock().unwrap();
                    assert_eq!(copies.len(), 4);
                    for (index, &(_, address, length)) in copies.iter().enumerate() {
                        let offset = address - local.base_address() as usize;
                        assert!(offset >= 2 && offset + length <= 34 && (offset - 2) % 3 == 0);
                        assert_eq!(
                            length,
                            if index == 3 && fault == "submit" {
                                1
                            } else {
                                3
                            }
                        );
                        expected[offset..offset + length].fill(0x57);
                    }
                }
                local_gpu.read(0, &mut observed).unwrap();
                assert_eq!(observed, expected, "{transport:?}/{operation:?}/{fault}");
                let mut expected = [0x57; 64];
                if operation == TransferOp::Write {
                    let copies = a.submitted_copies.lock().unwrap();
                    assert_eq!(copies.len(), source_stats.remote_delegates as usize);
                    for &(_, address, length) in copies.iter() {
                        let offset = address - remote.base_address() as usize;
                        assert!(offset >= 1 && offset + length <= 33 && (offset - 1) % 3 == 0);
                        assert_eq!(length, 3);
                        expected[offset..offset + length].fill(0xcc);
                    }
                }
                source_gpu.read(0, &mut observed).unwrap();
                assert_eq!(observed, expected);
                batch.free().unwrap();
                requester.close(Duration::from_secs(5)).unwrap();
                assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn requester_context_restore_failure_retains_all_accepted_slots() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let source = engine(Transport::Tcp, 3, 2);
        let requester = Arc::new(engine(Transport::Tcp, 3, 3));
        let a = allocation(&[0x57; 64]);
        let b = allocation(&[0xcc; 64]);
        let local_gpu = memory(&b);
        let weak = Arc::downgrade(&local_gpu);
        let remote = source
            .register_cuda_memory_batch(&[(memory(&a), 0, 64)], Permission::RemoteReadWrite)
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
                Duration::from_secs(5),
            )
            .unwrap();
        b.query_hold.store(true, Ordering::Release);
        let batch = requester
            .submit_async(
                operation,
                &peer,
                &[TransferRequest {
                    transport: Transport::Tcp,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 8,
                }],
                None,
                Duration::from_secs(5),
            )
            .unwrap();
        wait_pending(&b, 3);
        assert_eq!(pending(&b), 3);
        b.pop_after_completion.store(true, Ordering::Release);
        b.query_hold.store(false, Ordering::Release);
        let report = batch.wait(Duration::from_secs(5)).unwrap();
        assert_eq!(report.state, BatchState::Failed);
        assert!(!report.locally_drained);
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert!(matches!(failure.error, Error::Unquiesced { .. }));
        assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
        assert_eq!(b.event_query_successes.load(Ordering::Acquire), 1);
        assert_eq!(b.event_waits.load(Ordering::Acquire), 0);
        assert_eq!(
            pending(&b),
            2,
            "poisoned context must not retry the remaining native waits"
        );
        assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..10]);
        assert!(matches!(
            local_gpu.read(0, &mut [0; 1]),
            Err(Error::Unquiesced { .. })
        ));
        let stats = requester.stats().unwrap();
        assert_eq!(
            (
                stats.staging.retained_chunks,
                stats.staging.completed_chunks,
                stats.inflight_batches
            ),
            (3, 0, 1)
        );
        assert!(matches!(
            requester.close_requester(Duration::from_millis(10)),
            Err(Error::Timeout)
        ));
        source.close(Duration::from_secs(5)).unwrap();
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        assert!(a.pins.lock().unwrap().is_empty());
        drop(batch);
        drop(local);
        drop(local_gpu);
        drop(requester);
        assert!(
            weak.upgrade().is_some(),
            "actual GPU owner remains retained"
        );
        assert_eq!(b.pins.lock().unwrap().len(), 3);
    }
}

#[test]
fn requester_native_copy_failure_drains_siblings_and_retains_range_and_admission() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            for fail_at in 1..=3 {
                let source = engine(transport, 3, 2);
                let requester = engine(transport, 3, 3);
                let a = allocation(&[0x57; 64]);
                let b = allocation(&[0xcc; 64]);
                let gpu = memory(&b);
                let remote = source
                    .register_cuda_memory_batch(&[(memory(&a), 0, 64)], Permission::RemoteReadWrite)
                    .unwrap()
                    .remove(0);
                let local = requester
                    .register_cuda_memory_batch(&[(gpu.clone(), 0, 64)], Permission::LocalOnly)
                    .unwrap()
                    .remove(0);
                let peer = requester
                    .open_segment(
                        source.endpoint(),
                        source.source_epoch(),
                        Duration::from_secs(3),
                    )
                    .unwrap();
                b.query_hold.store(true, Ordering::Release);
                b.event_query_fail_at.store(fail_at, Ordering::Release);
                let report = std::thread::scope(|scope| {
                    let transfer = scope.spawn(|| {
                        requester.submit_and_wait(
                            operation,
                            &peer,
                            &[TransferRequest {
                                transport,
                                local_address: local.base_address() + 2,
                                remote_address: remote.base_address() + 1,
                                length: 8,
                            }],
                            Duration::from_secs(3),
                        )
                    });
                    wait_pending(&b, 3);
                    b.query_hold.store(false, Ordering::Release);
                    transfer.join().unwrap().unwrap()
                });
                assert!(
                    !report.is_complete() && !report.locally_drained,
                    "{transport:?}/{operation:?}/{fail_at}: {report:?}"
                );
                assert_eq!(
                    (report.completed_requests, report.transferred_bytes),
                    (0, 0)
                );
                let failure = report.failure.unwrap();
                assert!(matches!(failure.error, Error::Unquiesced { .. }));
                if operation == TransferOp::Read || fail_at == 1 {
                    assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
                }
                assert_eq!(gpu.allocation.lock().unwrap().dma, vec![2..10]);
                let stats = requester.stats().unwrap().staging;
                assert_eq!((stats.retained_chunks, stats.completed_chunks), (3, 2));
                assert_eq!(
                    b.event_waits.load(Ordering::Acquire)
                        + b.event_query_results.load(Ordering::Acquire),
                    3
                );
                assert_eq!(
                    b.streams
                        .lock()
                        .unwrap()
                        .values()
                        .filter(|v| v.is_some())
                        .count(),
                    1
                );

                assert!(matches!(
                    requester.close_requester(Duration::from_millis(10)),
                    Err(Error::Timeout)
                ));
                source.close(Duration::from_secs(3)).unwrap();
                let stats = source.stats().unwrap().staging;
                if operation == TransferOp::Read {
                    assert_eq!(stats.remote_crosses, 3);
                } else {
                    assert!(
                        stats.remote_crosses < fail_at as u64,
                        "only ready PREs can cross: {stats:?}"
                    );
                    assert!(stats.remote_delegates <= stats.remote_crosses);
                    if stats.remote_delegates != 0 {
                        assert_eq!(failure.remote_effect, crate::RemoteEffect::Unknown);
                    }
                    let mut expected = [0x57; 64];
                    expected[1..1 + stats.remote_delegates as usize * 3].fill(0xcc);
                    let mut bytes = [0; 64];
                    memory(&a).read(0, &mut bytes).unwrap();
                    assert_eq!(bytes, expected);
                }
                assert_eq!(
                    source.stats().unwrap().staging.remote_unpins,
                    0,
                    "local completion must precede Unpin"
                );
                assert!(a.pins.lock().unwrap().is_empty());
                assert_eq!(b.pins.lock().unwrap().len(), 3);
            }
        }
    }
}
