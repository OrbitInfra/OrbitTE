use super::*;
use crate::cuda::ipc::IpcExport;
use crate::runtime::proxy_manager::ProxyManager;
use std::time::Instant;

pub(super) fn engine(transport: Transport, chunk: usize, depth: usize) -> TransferEngine {
    let config = EngineConfig {
        max_failover_attempts: 0, // isolate the staging/native fault under test
        remote_staging: true,
        staging_chunk_size: chunk,
        staging_chunk_count: depth,
        staging_pipeline_depth: depth,
        handler_threads: 1,
        handler_timeout: crate::test_support::EVENTUAL_PROGRESS,
        max_inflight_batches: 1,
        ..EngineConfig::default()
    };
    #[cfg(feature = "rdma")]
    if transport == Transport::Rdma {
        let mut runtime = crate::transport::rdma::tests::runtime("direct:copy");
        runtime.proxy = ProxyManager::new(chunk, depth, crate::StagingLocation::Any).unwrap();
        return crate::engine::tests::rdma_fixture_with_config(runtime, config);
    }
    let _ = transport;
    TransferEngine::new(config).unwrap()
}

#[test]
fn requester_reuses_bounded_slots_at_negotiated_chunk_size_including_tails() {
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
                let source_gpu = memory(&a);
                let remote = source
                    .register_cuda_memory_batch(
                        &[(source_gpu.clone(), 1, 62)],
                        Permission::RemoteReadWrite,
                    )
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
                let request = TransferRequest::new(
                    transport,
                    local.base_address() + 2,
                    remote.base_address() + 1,
                    length,
                )
                .with_remote_staging(true);
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
                    let mut source_observed = [0; 64];
                    source_gpu.read(0, &mut source_observed).unwrap();
                    let mut source_expected = source_bytes;
                    if operation == TransferOp::Write {
                        source_expected[2..2 + length].copy_from_slice(&write);
                    }
                    assert_eq!(source_observed, source_expected);
                }
                let chunk = source_chunk.min(local_chunk);
                let chunks = length.div_ceil(chunk);
                let source_width = 3.min(chunks);
                let local_width = 3.min(length.div_ceil(local_chunk));
                let stats = requester.stats().unwrap().staging;
                assert_eq!(
                    (stats.allocations, stats.reuses, stats.reserved_bytes),
                    if cuda {
                        (
                            local_width as u64,
                            local_width as u64,
                            local_chunk * local_width,
                        )
                    } else {
                        (0, 0, 0)
                    }
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
    let manager = ProxyManager::new(3, 2, crate::StagingLocation::Any).unwrap();
    gpu.with_staging(4, 8, &manager, 1, None, |copy, mut stages| {
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
            gpu.with_staging(0, 5, &manager, 1, None, |_, _| Ok(())),
            Err(Error::Unavailable(_))
        ));
        gpu.with_staging(12, 3, &manager, 1, None, |_, _| Ok(()))
            .unwrap();
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
        gpu.with_staging(40, 3, &manager, 1, None, |_, _| Ok(())),
        Err(Error::Unavailable(_))
    ));
    export.finish();
    #[cfg(feature = "rdma")]
    {
        gpu.acquire_dma(4..12).unwrap();
        assert!(matches!(
            gpu.with_staging(11, 3, &manager, 1, None, |_, _| Ok(())),
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
    let manager = ProxyManager::new(3, 2, crate::StagingLocation::Any).unwrap();
    gpu.with_staging(0, 3, &manager, 1, None, |first, mut first_slots| {
        gpu.with_staging(6, 3, &manager, 1, None, |second, mut second_slots| {
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
            .filter(|v| !v.is_empty())
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
fn r1c_merged_staging_submits_one_cross_and_reports_each_alias() -> Result<()> {
    let t = crate::test_support::EVENTUAL_PROGRESS;
    for op in [TransferOp::Read, TransferOp::Write] {
        let source = engine(Transport::Tcp, 32, 2);
        let requester = Arc::new(engine(Transport::Tcp, 32, 2));
        let dev = allocation(&[0x71; 32]);
        let gpu = memory(&dev);
        let remote = source
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 32)], Permission::RemoteReadWrite)?
            .remove(0);
        let local = requester.register_memory(vec![0xb3; 32].into(), Permission::LocalOnly)?;
        let peer = requester.open_segment(source.endpoint(), source.source_epoch(), t)?;
        let reqs = [(4, 6), (10, 7)].map(|(off, len)| {
            TransferRequest::new(
                Transport::Tcp,
                local.base_address() + off,
                remote.base_address() + off,
                len,
            )
            .with_remote_staging(true)
        });
        let batch = requester.submit_async(op, &peer, &reqs, None, t)?;
        batch.wait(t)?;
        for (i, n) in [(1, 7), (0, 6)] {
            let status = batch.task_status(i)?;
            assert_eq!(
                (
                    status.physical_owner,
                    status.state,
                    status.transferred_bytes,
                    status.locally_drained
                ),
                (0, crate::TaskState::Completed, n, true)
            );
        }
        let s = source.stats()?.staging;
        assert_eq!(
            (
                s.remote_pins,
                s.remote_crosses,
                s.remote_delegates,
                s.remote_unpins
            ),
            (1, 1, 1, 1)
        );
        for (memory, background, data) in [
            (
                &local,
                0xb3,
                if op == TransferOp::Read { 0x71 } else { 0xb3 },
            ),
            (
                &remote,
                0x71,
                if op == TransferOp::Write { 0xb3 } else { 0x71 },
            ),
        ] {
            let mut expected = [background; 32];
            expected[4..17].fill(data);
            let mut bytes = [0; 32];
            memory.read(0, &mut bytes)?;
            assert_eq!(bytes, expected);
        }
        requester.close(t)?;
        source.close(t)?;
    }
    Ok(())
}

#[test]
fn r1c_failed_merged_staging_write_does_not_resubmit_an_alias() -> Result<()> {
    let t = crate::test_support::EVENTUAL_PROGRESS;
    let source = engine(Transport::Tcp, 32, 2);
    let requester = Arc::new(TransferEngine::new(EngineConfig {
        max_failover_attempts: 0,
        remote_staging: true,
        staging_chunk_size: 32,
        staging_chunk_count: 2,
        ..Default::default()
    })?);
    let dev = allocation(&[0x71; 32]);
    let gpu = memory(&dev);
    let remote = source
        .register_cuda_memory_batch(&[(gpu.clone(), 0, 32)], Permission::RemoteReadWrite)?
        .remove(0);
    let local = requester.register_memory(vec![0x3b; 32].into(), Permission::LocalOnly)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), t)?;
    let reqs = [(2, 4), (6, 5)].map(|(off, n)| {
        TransferRequest::new(
            Transport::Tcp,
            local.base_address() + off,
            remote.base_address() + off,
            n,
        )
        .with_remote_staging(true)
    });
    dev.copy_error.store(700, Ordering::Release);
    let batch = requester.submit_async(TransferOp::Write, &peer, &reqs, None, t)?;
    batch.wait(t)?;
    for i in [1, 0] {
        let status = batch.task_status(i)?;
        assert_eq!(
            (
                status.physical_owner,
                status.state,
                status.transferred_bytes
            ),
            (0, crate::TaskState::Failed, 0)
        );
        assert_eq!(
            status.failure.unwrap().remote_effect,
            crate::RemoteEffect::Unknown
        );
    }
    let s = source.stats()?.staging;
    assert_eq!((s.remote_pins, s.remote_crosses), (1, 1));
    dev.copy_error.store(0, Ordering::Release);
    let mut bytes = [0; 32];
    gpu.read(0, &mut bytes)?;
    // The injected HtoD error writes one byte before returning. The following
    // alias must neither replay that grant nor reach any other byte.
    let mut expected = [0x71; 32];
    expected[2] = 0x3b;
    assert_eq!(bytes, expected);
    requester.close(t)?;
    source.close(t)?;
    Ok(())
}
