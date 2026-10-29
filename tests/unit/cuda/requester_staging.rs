use super::*;
use crate::cuda::ipc::IpcExport;
use crate::runtime::proxy_manager::ProxyManager;
use std::time::Instant;

fn engine(transport: Transport, chunk: usize, depth: usize) -> TransferEngine {
    let config = EngineConfig {
        remote_staging: true,
        staging_chunk_size: chunk,
        staging_chunk_count: depth,
        staging_pipeline_depth: depth,
        handler_threads: 1,
        max_inflight_batches: 1,
        ..EngineConfig::default()
    };
    #[cfg(feature = "rdma")]
    if transport == Transport::Rdma {
        let mut runtime = crate::transport::rdma::tests::runtime("direct:copy");
        runtime.proxy = ProxyManager::new(chunk, depth).unwrap();
        return crate::engine::tests::rdma_fixture_with_config(runtime, config);
    }
    let _ = transport;
    TransferEngine::new(config).unwrap()
}

#[test]
fn requester_reuses_one_slot_at_negotiated_chunk_size_including_tails() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for (source_chunk, local_chunk, length) in
            [(7usize, 3usize, 23usize), (3, 7, 23), (9, 7, 1)]
        {
            for cuda in [false, true] {
                let source = engine(transport, source_chunk, 3);
                let requester = engine(transport, local_chunk, 3);
                let source_bytes: [u8; 64] = std::array::from_fn(|i| (i * 37 + 11) as u8);
                let a = allocation(&source_bytes);
                let b = allocation(&[0xcc; 64]);
                let remote = source
                    .register_cuda_memory_batch(&[(memory(&a), 1, 62)], Permission::RemoteReadWrite)
                    .unwrap()
                    .remove(0);
                let local_gpu = memory(&b);
                let local = if cuda {
                    requester
                        .register_cuda_memory_batch(
                            &[(local_gpu.clone(), 2, 60)],
                            Permission::LocalOnly,
                        )
                        .unwrap()
                        .remove(0)
                } else {
                    requester
                        .register_memory(vec![0xcc; 60].into_boxed_slice(), Permission::LocalOnly)
                        .unwrap()
                };
                let peer = requester
                    .open_segment(
                        source.endpoint(),
                        source.source_epoch(),
                        Duration::from_secs(3),
                    )
                    .unwrap();
                let request = TransferRequest {
                    transport,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length,
                };
                let write: Vec<u8> = (0..length).map(|i| (i * 19 + 53) as u8).collect();
                for operation in [TransferOp::Read, TransferOp::Write] {
                    if operation == TransferOp::Write {
                        local.write(2, &write).unwrap();
                    }
                    let report = requester
                        .submit_and_wait(
                            operation,
                            &peer,
                            std::slice::from_ref(&request),
                            Duration::from_secs(3),
                        )
                        .unwrap();
                    assert!(
                        report.is_complete() && report.locally_drained,
                        "{transport:?}/{source_chunk}/{local_chunk}/{cuda}: {report:?}"
                    );
                    let mut expected = [0xcc; 60];
                    expected[2..2 + length].copy_from_slice(if operation == TransferOp::Read {
                        &source_bytes[2..2 + length]
                    } else {
                        &write
                    });
                    let mut bytes = [0; 60];
                    local.read(0, &mut bytes).unwrap();
                    assert_eq!(bytes, expected);
                }
                let chunk = source_chunk.min(local_chunk);
                let chunks = length.div_ceil(chunk);
                let source_width = 3.min(chunks);
                let stats = requester.stats().unwrap().staging;
                assert_eq!(
                    (stats.allocations, stats.reuses, stats.reserved_bytes),
                    if cuda { (1, 1, local_chunk) } else { (0, 0, 0) }
                );
                assert_eq!(
                    stats.completed_chunks,
                    if cuda { (2 * chunks) as u64 } else { 0 }
                );
                assert_eq!(
                    stats.copied_bytes,
                    if cuda { (2 * length) as u64 } else { 0 }
                );
                assert_eq!(
                    source.stats().unwrap().staging.reserved_bytes,
                    source_chunk * source_width
                );
                assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
                requester.close(Duration::from_secs(3)).unwrap();
                source.close(Duration::from_secs(3)).unwrap();
                assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn staging_range_loans_interlock_with_cpu_rdma_and_whole_allocation_ipc() {
    let a = allocation(&[0x57; 64]);
    let gpu = super::ipc::owned(&a, true);
    let manager = ProxyManager::new(3, 2).unwrap();
    gpu.with_staging(4, 8, &manager, 1, |copy, mut stages| {
        assert!(gpu.allocation.try_lock().is_ok());
        assert!(matches!(
            gpu.read(5, &mut [0; 1]),
            Err(Error::Unavailable(_))
        ));
        assert!(matches!(gpu.write(11, &[1]), Err(Error::Unavailable(_))));
        assert!(matches!(
            IpcExport::new(gpu.clone()),
            Err(Error::Unavailable(_))
        ));
        assert!(matches!(
            gpu.with_staging(0, 5, &manager, 1, |_, _| Ok(())),
            Err(Error::Unavailable(_))
        ));
        gpu.with_staging(12, 3, &manager, 1, |_, _| Ok(())).unwrap();
        #[cfg(feature = "rdma")]
        {
            assert!(matches!(
                gpu.acquire_dma(11..13),
                Err(Error::Unavailable(_))
            ));
            gpu.acquire_dma(16..20).unwrap();
            copy.begin(&mut stages[0], 0, 3, false).unwrap();
            gpu.complete_dma().unwrap();
            gpu.release_dma(&(16..20));
            assert!(gpu.allocation.lock().unwrap().dma.contains(&(4..12)));
            copy.complete(&mut stages[0], 3).unwrap();
        }
        #[cfg(not(feature = "rdma"))]
        copy.copy(&mut stages[0], 0, 3, false).unwrap();
        assert!(!gpu.allocation.lock().unwrap().uncertain);
        Ok(())
    })
    .unwrap();
    assert!(gpu.allocation.lock().unwrap().dma.is_empty());
    let export = IpcExport::new(gpu.clone()).unwrap();
    assert!(matches!(
        gpu.with_staging(40, 3, &manager, 1, |_, _| Ok(())),
        Err(Error::Unavailable(_))
    ));
    export.finish();
    #[cfg(feature = "rdma")]
    {
        gpu.acquire_dma(4..12).unwrap();
        assert!(matches!(
            gpu.with_staging(11, 3, &manager, 1, |_, _| Ok(())),
            Err(Error::Unavailable(_))
        ));
        gpu.complete_dma().unwrap();
        gpu.release_dma(&(4..12));
    }
    manager
        .close(Instant::now() + Duration::from_secs(3))
        .unwrap();
    assert!(a.pins.lock().unwrap().is_empty());
}

#[test]
fn successful_sibling_drain_cannot_clear_failed_range_or_release_actual_owner() {
    let a = allocation(&[0x57; 64]);
    let gpu = memory(&a);
    let weak = Arc::downgrade(&gpu);
    let manager = ProxyManager::new(3, 2).unwrap();
    gpu.with_staging(0, 3, &manager, 1, |first, mut first_slots| {
        gpu.with_staging(6, 3, &manager, 1, |second, mut second_slots| {
            first.begin(&mut first_slots[0], 0, 3, false).unwrap();
            second.begin(&mut second_slots[0], 0, 3, false).unwrap();
            assert!(!gpu.allocation.lock().unwrap().uncertain);
            a.event_wait_fail_at.store(1, Ordering::Release);
            assert!(matches!(
                first.complete(&mut first_slots[0], 3),
                Err(Error::Unquiesced { .. })
            ));
            first.publish_failure();
            assert!(matches!(
                second.check_access(),
                Err(Error::Unquiesced { .. })
            ));
            second.complete(&mut second_slots[0], 3).unwrap();
            second.publish_failure();
            assert!(gpu.allocation.lock().unwrap().uncertain);
            Ok(())
        })
        .unwrap();
        assert_eq!(gpu.allocation.lock().unwrap().dma, vec![0..3]);
        Ok(())
    })
    .unwrap();
    assert_eq!(gpu.allocation.lock().unwrap().dma, vec![0..3]);
    assert_eq!(a.event_waits.load(Ordering::Acquire), 2);
    assert_eq!(
        a.streams
            .lock()
            .unwrap()
            .values()
            .filter(|v| v.is_some())
            .count(),
        1
    );
    assert!(matches!(
        gpu.read(0, &mut [0; 1]),
        Err(Error::Unquiesced { .. })
    ));
    assert!(matches!(
        manager.close(Instant::now() + Duration::from_secs(3)),
        Err(Error::Unquiesced { .. })
    ));
    drop(gpu);
    assert!(
        weak.upgrade().is_some(),
        "failed copy must retain the actual CudaMemory, not just its address"
    );
}

#[test]
fn requester_native_copy_failure_retains_one_slot_range_and_parent_admission() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let source = engine(Transport::Tcp, 3, 2);
        let requester = engine(Transport::Tcp, 3, 2);
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
        b.event_wait_fail_at.store(1, Ordering::Release);
        let report = requester
            .submit_and_wait(
                operation,
                &peer,
                &[TransferRequest {
                    transport: Transport::Tcp,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 8,
                }],
                Duration::from_secs(3),
            )
            .unwrap();
        assert!(
            !report.is_complete() && !report.locally_drained,
            "{report:?}"
        );
        let failure = report.failure.unwrap();
        assert!(matches!(failure.error, Error::Unquiesced { .. }));
        assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
        assert_eq!(gpu.allocation.lock().unwrap().dma, vec![2..10]);
        assert_eq!(requester.stats().unwrap().staging.retained_chunks, 1);
        assert!(matches!(
            requester.close_requester(Duration::from_millis(10)),
            Err(Error::Timeout)
        ));
        source.close(Duration::from_secs(3)).unwrap();
        assert!(a.pins.lock().unwrap().is_empty());
        assert_eq!(b.pins.lock().unwrap().len(), 1);
    }
}
