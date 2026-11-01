use super::requester_staging::engine;
use super::*;

#[test]
fn completed_slot_refills_while_another_generation_stays_not_ready() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            for held in [1, 3] {
                let source = engine(transport, 3, 2);
                let requester = Arc::new(engine(transport, 3, 2));
                let original_a: [u8; 32] = std::array::from_fn(|i| (i * 7 + 13) as u8);
                let original_b: [u8; 32] = std::array::from_fn(|i| (i * 11 + 23) as u8);
                let a = allocation(&original_a);
                let b = allocation(&original_b);
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
                    .register_cuda_memory_batch(
                        &[(local_gpu.clone(), 0, 32)],
                        Permission::LocalOnly,
                    )
                    .unwrap()
                    .remove(0);
                let peer = requester
                    .open_segment(
                        source.endpoint(),
                        source.source_epoch(),
                        Duration::from_secs(3),
                    )
                    .unwrap();
                let base = local.base_address() as usize + 2;
                b.query_hold_address
                    .store(base + held * 3, Ordering::Release);
                let mut batch = requester
                    .submit_async(
                        operation,
                        &peer,
                        &[TransferRequest {
                            priority: Default::default(),
                            remote_staging: true,
                            transport,
                            local_address: base as u64,
                            remote_address: remote.base_address() + 1,
                            length: 14,
                        }],
                        None,
                        Duration::from_secs(5),
                    )
                    .unwrap();
                let until = std::time::Instant::now() + Duration::from_secs(2);
                loop {
                    let advanced = b.submitted_copies.lock().unwrap().iter().any(
                        |&(source, destination, _)| {
                            if operation == TransferOp::Read {
                                destination == base + (held + 1) * 3
                            } else {
                                source == base + (held + 1) * 3
                            }
                        },
                    );
                    let pending = b.streams.lock().unwrap().values().flatten().any(|copy| {
                        if operation == TransferOp::Read {
                            copy.destination == base + held * 3
                        } else {
                            copy.source == base + held * 3
                        }
                    });
                    if advanced && pending && b.event_not_ready.load(Ordering::Acquire) != 0 {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < until,
                        "chunk after {held} stalled behind the old window barrier: {transport:?}/{operation:?}"
                    );
                    std::thread::yield_now();
                }
                assert_eq!(
                    b.event_waits.load(Ordering::Acquire),
                    0,
                    "healthy progress must never synchronize an event"
                );
                assert!(
                    b.event_queries.load(Ordering::Acquire)
                        > b.event_query_results.load(Ordering::Acquire)
                );
                assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..16]);
                assert_eq!(requester.stats().unwrap().staging.active_chunks, 2);
                assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
                assert!(matches!(batch.free(), Err(Error::BatchBusy)));
                b.query_hold_address.store(0, Ordering::Release);
                let report = batch.wait(Duration::from_secs(5)).unwrap();
                assert!(
                    report.state == BatchState::Completed && report.locally_drained,
                    "{transport:?}/{operation:?}: {report:?}"
                );
                assert_eq!(report.transferred_bytes, 14);
                assert_eq!(b.event_waits.load(Ordering::Acquire), 0);
                assert_eq!(b.event_query_successes.load(Ordering::Acquire), 5);
                assert_eq!(requester.stats().unwrap().staging.completed_chunks, 5);
                assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
                batch.free().unwrap();
                let mut expected_a = original_a;
                let mut expected_b = original_b;
                if operation == TransferOp::Read {
                    expected_b[2..16].copy_from_slice(&original_a[1..15]);
                } else {
                    expected_a[1..15].copy_from_slice(&original_b[2..16]);
                }
                let mut bytes = [0; 32];
                source_gpu.read(0, &mut bytes).unwrap();
                assert_eq!(bytes, expected_a);
                local_gpu.read(0, &mut bytes).unwrap();
                assert_eq!(bytes, expected_b);
                requester.close(Duration::from_secs(3)).unwrap();
                source.close(Duration::from_secs(3)).unwrap();
                assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
                // Byte verification owns a separate allocation copy stream.
                drop(local);
                drop(local_gpu);
                drop(requester);
                assert!(
                    b.streams.lock().unwrap().is_empty()
                        && b.completions.lock().unwrap().is_empty()
                );
            }
        }
    }
}

#[test]
fn not_ready_followed_by_context_restore_failure_retains_actual_owners() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let source = engine(Transport::Tcp, 3, 2);
        let requester = Arc::new(engine(Transport::Tcp, 3, 2));
        let a = allocation(&[0x57; 32]);
        let b = allocation(&[0xcc; 32]);
        let gpu = memory(&b);
        let weak = Arc::downgrade(&gpu);
        let remote = source
            .register_cuda_memory_batch(&[(memory(&a), 0, 32)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 32)], Permission::LocalOnly)
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
        b.pop_after_not_ready.store(true, Ordering::Release);
        let mut batch = requester
            .submit_async(
                operation,
                &peer,
                &[TransferRequest {
                    priority: Default::default(),
                    remote_staging: true,
                    transport: Transport::Tcp,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 8,
                }],
                None,
                Duration::from_secs(5),
            )
            .unwrap();
        let report = batch.wait(Duration::from_secs(5)).unwrap();
        assert!(
            report.state == BatchState::Failed && !report.locally_drained,
            "{report:?}"
        );
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert!(matches!(failure.error, Error::Unquiesced { .. }));
        assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
        assert_eq!(b.event_queries.load(Ordering::Acquire), 1);
        assert_eq!(b.event_query_results.load(Ordering::Acquire), 0);
        assert_eq!(b.event_waits.load(Ordering::Acquire), 0);
        assert_eq!(gpu.allocation.lock().unwrap().dma, vec![2..10]);
        assert_eq!(requester.stats().unwrap().staging.retained_chunks, 2);
        assert_eq!(requester.stats().unwrap().staging.completed_chunks, 0);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        assert!(matches!(
            requester.close_requester(Duration::from_millis(5)),
            Err(Error::Timeout)
        ));
        source.close(Duration::from_secs(3)).unwrap();
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        drop(batch);
        drop(local);
        drop(gpu);
        drop(requester);
        assert!(weak.upgrade().is_some());
        assert_eq!(b.pins.lock().unwrap().len(), 2);
    }
}
