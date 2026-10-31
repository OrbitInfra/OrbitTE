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
fn requester_pipeline_keeps_independent_rings_pending_until_query_completion() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for (source_cap, local_cap, physical, length) in [
            (3, 3, 64usize, 32usize), // L=1, R=3
            (1, 3, 3, 32),            // L=3, R=1
            (3, 3, 17, 32),           // L=2, R=3
            (2, 3, 3, 32),            // L=3, R=2
            (16, 16, 3, 149),         // Three ring turns and a tail.
        ] {
            let source = engine(transport, 3, source_cap);
            let requester = Arc::new(engine(transport, physical, local_cap));
            let original: [u8; 256] = std::array::from_fn(|i| (i * 37 + 11) as u8);
            let a = allocation(&original);
            let b = allocation(&[0xcc; 256]);
            let source_gpu = memory(&a);
            let local_gpu = memory(&b);
            let remote = source
                .register_cuda_memory_batch(
                    &[(source_gpu.clone(), 2, 250)],
                    Permission::RemoteReadWrite,
                )
                .unwrap()
                .remove(0);
            let local = requester
                .register_cuda_memory_batch(&[(local_gpu.clone(), 3, 248)], Permission::LocalOnly)
                .unwrap()
                .remove(0);
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(5),
                )
                .unwrap();
            let width = local_cap.min(length.div_ceil(physical));
            let chunks = length.div_ceil(3);
            let write: Vec<u8> = (0..length).map(|i| (i * 19 + 53) as u8).collect();
            for (round, operation) in [TransferOp::Read, TransferOp::Write]
                .into_iter()
                .enumerate()
            {
                if operation == TransferOp::Write {
                    local.write(5, &write).unwrap();
                }
                b.query_hold.store(true, Ordering::Release);
                let completed = 0;
                let mut batch = requester
                    .submit_async(
                        operation,
                        &peer,
                        &[TransferRequest {
                            remote_staging: true,
                            transport,
                            local_address: local.base_address() + 5,
                            remote_address: remote.base_address() + 7,
                            length,
                        }],
                        None,
                        Duration::from_secs(5),
                    )
                    .unwrap();
                wait_pending(&b, width);
                assert_eq!(
                    pending(&b),
                    width - completed,
                    "{transport:?}/{operation:?}/L={width}"
                );
                let mut pending_addresses: Vec<_> = b
                    .streams
                    .lock()
                    .unwrap()
                    .values()
                    .filter_map(|copy| copy.as_ref())
                    .map(|copy| {
                        if operation == TransferOp::Read {
                            copy.destination
                        } else {
                            copy.source
                        }
                    })
                    .collect();
                pending_addresses.sort_unstable();
                assert_eq!(
                    pending_addresses,
                    (completed..width)
                        .map(|i| local.base_address() as usize + 5 + i * 3)
                        .collect::<Vec<_>>()
                );
                let allocation = local_gpu
                    .allocation
                    .try_lock()
                    .expect("event wait must not hold GPU metadata lock");
                assert_eq!(allocation.dma, vec![8..8 + length]);
                drop(allocation);
                let stats = requester.stats().unwrap().staging;
                assert_eq!(
                    (stats.active_chunks, stats.reserved_bytes),
                    (width, physical * width)
                );
                assert_eq!(stats.completed_chunks, (round * chunks + completed) as u64);
                let expected_crosses = (round * chunks
                    + if operation == TransferOp::Read {
                        width
                    } else {
                        completed
                    }) as u64;
                let until = std::time::Instant::now() + Duration::from_secs(3);
                let stats = loop {
                    let stats = source.stats().unwrap().staging;
                    if stats.remote_crosses == expected_crosses {
                        break stats;
                    }
                    assert!(
                        std::time::Instant::now() < until,
                        "CROSS must progress while local CUDA waits: {stats:?}"
                    );
                    std::thread::yield_now();
                };
                assert_eq!(stats.remote_unpins, round as u64);
                b.query_hold.store(false, Ordering::Release);
                let report = batch.wait(Duration::from_secs(5)).unwrap();
                assert!(
                    report.state == BatchState::Completed && report.locally_drained,
                    "{transport:?}/{operation:?}/L={width}: {report:?}"
                );
                batch.free().unwrap();
                assert_eq!(pending(&b), 0);
                let mut expected = [0xcc; 256];
                expected[8..8 + length].copy_from_slice(if operation == TransferOp::Read {
                    &original[9..9 + length]
                } else {
                    &write
                });
                let mut bytes = [0; 256];
                local_gpu.read(0, &mut bytes).unwrap();
                assert_eq!(bytes, expected);
                let mut source_expected = original;
                if operation == TransferOp::Write {
                    source_expected[9..9 + length].copy_from_slice(&write);
                }
                source_gpu.read(0, &mut bytes).unwrap();
                assert_eq!(bytes, source_expected);
            }
            let stats = requester.stats().unwrap().staging;
            assert_eq!(
                (stats.allocations, stats.reuses, stats.idle_chunks),
                (width as u64, width as u64, width)
            );
            assert_eq!(stats.completed_chunks, (2 * chunks) as u64);
            assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
            requester.close(Duration::from_secs(5)).unwrap();
            source.close(Duration::from_secs(5)).unwrap();
            assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn self_peer_pipeline_requires_both_ring_budgets_before_any_payload_copy() {
    for capacity in [5, 6] {
        let engine = TransferEngine::new(EngineConfig {
            remote_staging: true,
            staging_chunk_size: 3,
            staging_chunk_count: capacity,
            staging_pipeline_depth: 3,
            handler_threads: 1,
            max_inflight_batches: 1,
            ..EngineConfig::default()
        })
        .unwrap();
        let original: [u8; 64] = std::array::from_fn(|i| (i * 7 + 5) as u8);
        let owner = allocation(&original);
        let gpu = memory(&owner);
        let registered = engine
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 64)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let peer = engine
            .open_segment(
                engine.endpoint(),
                engine.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        let report = engine
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[TransferRequest {
                    remote_staging: true,
                    transport: Transport::Tcp,
                    local_address: registered.base_address() + 32,
                    remote_address: registered.base_address(),
                    length: 17,
                }],
                Duration::from_secs(3),
            )
            .unwrap();
        assert_eq!(report.is_complete(), capacity == 6, "{report:?}");
        assert!(report.locally_drained);
        if capacity == 5 {
            assert_eq!(
                report.failure.unwrap().remote_effect,
                crate::RemoteEffect::None
            );
            assert_eq!(owner.event_waits.load(Ordering::Acquire), 0);
            assert_eq!(engine.stats().unwrap().staging.remote_pins, 0);
        }
        let mut expected = original;
        if capacity == 6 {
            expected[32..49].copy_from_slice(&original[..17]);
        }
        let mut observed = [0; 64];
        gpu.read(0, &mut observed).unwrap();
        assert_eq!(observed, expected);
        assert!(gpu.allocation.lock().unwrap().dma.is_empty());
        engine.close(Duration::from_secs(3)).unwrap();
        assert!(owner.pins.lock().unwrap().is_empty());
    }
}

#[test]
fn expired_final_post_drains_every_slot_before_releasing_local_owners() {
    let source = engine(Transport::Tcp, 3, 2);
    let requester = Arc::new(engine(Transport::Tcp, 3, 3));
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
    let (entered, waiting) = std::sync::mpsc::sync_channel(0);
    let (resume, blocked) = std::sync::mpsc::sync_channel(0);
    *b.event_gate.lock().unwrap() = Some((entered, blocked));
    b.query_hold.store(true, Ordering::Release);
    let mut batch = requester
        .submit_async(
            TransferOp::Read,
            &peer,
            &[TransferRequest {
                remote_staging: true,
                transport: Transport::Tcp,
                local_address: local.base_address() + 2,
                remote_address: remote.base_address() + 1,
                length: 8,
            }],
            None,
            Duration::from_secs(1),
        )
        .unwrap();
    wait_pending(&b, 3);
    assert_eq!(pending(&b), 3);
    assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
    assert!(matches!(
        requester.close_requester(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    waiting.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    assert_eq!(gpu.allocation.lock().unwrap().dma, vec![2..10]);
    b.query_hold.store(false, Ordering::Release);
    resume.send(()).unwrap();
    let report = batch.wait(Duration::from_secs(3)).unwrap();
    assert!(
        report.state == BatchState::Failed && report.locally_drained,
        "{report:?}"
    );
    assert_eq!(report.transferred_bytes, 0);
    assert_eq!(batch.submissions().unwrap()[0].completed_requests, 0);
    assert_eq!(pending(&b), 0);
    assert_eq!(b.event_waits.load(Ordering::Acquire), 3);
    assert_eq!(requester.stats().unwrap().staging.completed_chunks, 3);
    assert!(gpu.allocation.lock().unwrap().dma.is_empty());
    let mut bytes = [0; 64];
    gpu.read(0, &mut bytes).unwrap();
    let mut expected = [0xcc; 64];
    expected[2..10].fill(0x57);
    assert_eq!(bytes, expected);
    batch.free().unwrap();
    requester.close(Duration::from_secs(3)).unwrap();
    source.close(Duration::from_secs(3)).unwrap();
    assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
    assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
}
