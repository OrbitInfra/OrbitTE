use super::*;

#[test]
fn cuda_dma_ranges_are_shared_across_registration_views_and_engines() {
    let owner = allocation(&[0x5a; 32]);
    let allocation = memory(&owner);
    let first = TransferEngine::new(EngineConfig::default()).unwrap();
    let second = TransferEngine::new(EngineConfig::default()).unwrap();
    let a = first
        .register_cuda_memory_batch(&[(allocation.clone(), 4, 20)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let b = second
        .register_cuda_memory_batch(&[(allocation.clone(), 8, 20)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let loan = a.dma_lease(4, 8).unwrap();
    assert_eq!(loan.address().as_ptr() as u64, b.base_address());
    assert!(matches!(b.dma_lease(7, 2), Err(Error::Unavailable(_))));
    assert!(matches!(
        allocation.read(8, &mut [0; 1]),
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(b.write(0, &[1]), Err(Error::Unavailable(_))));
    let adjacent = b.dma_lease(8, 4).unwrap();
    allocation.write(0, &[1; 4]).unwrap();
    loan.complete().unwrap();
    assert!(
        b.read(0, &mut [0; 1]).is_err(),
        "visibility does not itself release ownership"
    );
    drop(loan);
    b.read(0, &mut [0; 8]).unwrap();
    adjacent.complete().unwrap();
    drop(adjacent);
    let events = owner.events.lock().unwrap();
    assert_eq!(events.iter().filter(|e| **e == "sync_memops").count(), 1);
    assert_eq!(events.iter().filter(|e| **e == "flush_rdma").count(), 2);
    assert!(STACK.with(|s| s.borrow().is_empty()));
}

#[test]
fn gpu_registration_requires_ordering_and_sync_memops_before_native_publication() {
    for case in ["ordered", "flush", "unsupported", "sync_error"] {
        let owner = allocation(&[7; 16]);
        owner
            .rdma_ordering
            .store(if case == "ordered" { 100 } else { 0 }, Ordering::Release);
        owner.rdma_flush_options.store(
            if matches!(case, "ordered" | "unsupported") {
                0
            } else {
                1
            },
            Ordering::Release,
        );
        owner
            .rdma_set_error
            .store(if case == "sync_error" { 1 } else { 0 }, Ordering::Release);
        let memory = memory(&owner);
        let called = AtomicBool::new(false);
        let result = memory.with_rdma_context(|| {
            called.store(true, Ordering::Release);
            Ok(())
        });
        let accepted = matches!(case, "ordered" | "flush");
        assert_eq!(result.is_ok(), accepted, "{case}");
        assert_eq!(called.load(Ordering::Acquire), accepted, "{case}");
        if accepted {
            memory.acquire_dma(0..8).unwrap();
            memory.complete_dma().unwrap();
            memory.release_dma(&(0..8));
            assert_eq!(
                owner.events.lock().unwrap().contains(&"flush_rdma"),
                case == "flush"
            );
        }
        assert!(STACK.with(|s| s.borrow().is_empty()));
    }
}

#[test]
fn failed_gpu_visibility_or_context_restore_retains_actual_allocation_and_blocks_reuse() {
    for case in ["flush", "pop"] {
        let owner = allocation(&[9; 16]);
        let weak = Arc::downgrade(&owner);
        let memory = memory(&owner);
        memory.acquire_dma(4..12).unwrap();
        if case == "flush" {
            owner.rdma_flush_error.store(700, Ordering::Release);
        } else {
            owner.pop_error.store(201, Ordering::Release);
        }
        assert!(matches!(
            memory.complete_dma(),
            Err(Error::Unquiesced { .. })
        ));
        assert!(matches!(
            memory.acquire_dma(0..4),
            Err(Error::Unquiesced { .. })
        ));
        assert!(matches!(
            memory.read(0, &mut [0; 4]),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(memory.allocation.lock().unwrap().dma, vec![4..12]);
        if case == "pop" {
            assert_eq!(STACK.with(|s| s.borrow().len()), 1);
            STACK.with(|s| s.borrow_mut().clear());
        }
        drop(memory);
        drop(owner);
        assert!(
            weak.upgrade().is_some(),
            "{case}: external owner must remain retained"
        );
    }
}

#[test]
fn failed_cuda_batch_publication_exposes_no_registration_and_preserves_old_views() {
    let owner = allocation(&[3; 32]);
    let memory = memory(&owner);
    let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([1; 16]), 4, 64);
    let first = registry
        .register_cuda_batch(&[(memory.clone(), 0, 8)], Permission::LocalOnly, |_| Ok(()))
        .unwrap();
    let result = registry.register_cuda_batch(
        &[(memory.clone(), 8, 8), (memory.clone(), 16, 8)],
        Permission::LocalOnly,
        |pending| {
            assert_eq!(pending.len(), 2);
            Err(Error::Unavailable("native GPU registration failed"))
        },
    );
    assert!(result.is_err());
    let retried = registry
        .register_cuda_batch(
            &[(memory.clone(), 8, 8), (memory, 16, 8)],
            Permission::LocalOnly,
            |_| Ok(()),
        )
        .unwrap();
    first[0].read(0, &mut [0; 8]).unwrap();
    assert_eq!(retried.len(), 2);
}

#[test]
fn private_gpu_grants_drain_in_order_or_keep_actual_owner_and_admission() {
    use crate::engine::tests::{rdma_fixture_with_runtime, source_runtime};
    use crate::transport::rdma::tests::{after_open, events, gpu_runtime, runtime};
    for mode in [
        "copy",
        "gpu_partial_pd",
        "gpu_partial_mr",
        "gpu_payload_close",
        "gpu_rq_close",
        "gpu_pd_close",
        "gpu_flush",
        "gpu_pop",
    ] {
        let source = rdma_fixture_with_runtime(gpu_runtime(mode));
        let requester = rdma_fixture_with_runtime(runtime("direct:copy"));
        let owner = allocation(&[0x7b; 16]);
        let weak = Arc::downgrade(&owner);
        let allocation = memory(&owner);
        let remote = source
            .register_cuda_memory_batch(&[(allocation.clone(), 0, 16)], Permission::RemoteRead)
            .unwrap()
            .remove(0);
        let native_events = events(source_runtime(&source));
        if mode == "gpu_pop" {
            let captured = owner.clone();
            after_open(source_runtime(&source), move || {
                captured.pop_error.store(201, Ordering::Release)
            });
        }
        if mode == "gpu_flush" {
            owner.rdma_flush_error.store(700, Ordering::Release);
        }
        let local = requester
            .register_memory(vec![0; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(2),
            )
            .unwrap();
        let report = requester
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[TransferRequest {
                    remote_staging: false,
                    transport: Transport::Rdma,
                    local_address: local.base_address(),
                    remote_address: remote.base_address() + 4,
                    length: 8,
                }],
                Duration::from_secs(2),
            )
            .unwrap();
        assert_eq!(report.is_complete(), mode == "copy", "{mode}: {report:?}");
        assert!(
            report.locally_drained,
            "requester is independently drained: {mode}"
        );
        requester.close(Duration::from_secs(2)).unwrap();
        source.unregister_memory(&remote).unwrap();
        let retained = !matches!(mode, "copy" | "gpu_partial_pd" | "gpu_partial_mr");
        if retained {
            assert!(
                source.close_source(Duration::from_millis(5)).is_err(),
                "{mode}"
            );
            let stats = source.stats().unwrap();
            let rdma = stats.rdma.unwrap();
            assert_eq!(
                (rdma.active_grants, rdma.retained_grants, rdma.direct_bytes),
                (1, 1, 8),
                "{mode}"
            );
            assert_eq!(stats.retained_regions, 1, "{mode}");
            if matches!(mode, "gpu_payload_close" | "gpu_rq_close" | "gpu_pd_close") {
                assert_eq!(
                    (rdma.active_grant_pds, rdma.active_endpoints),
                    (1, 1),
                    "{mode}: PD debt must retain the actual connection admission"
                );
            }
            if mode == "gpu_pop" {
                assert_eq!(
                    (
                        rdma.opened_connections,
                        rdma.closed_connections,
                        rdma.active_endpoints
                    ),
                    (1, 1, 0)
                );
                assert_eq!(
                    (
                        rdma.opened_grant_pds,
                        rdma.closed_grant_pds,
                        rdma.active_grant_pds
                    ),
                    (1, 1, 0)
                );
                assert_eq!(
                    rdma.completed_receives, 0,
                    "failed context restore cannot publish a grant"
                );
            }
            assert!(
                allocation.read(4, &mut [0; 8]).is_err(),
                "{mode}: DMA loan must remain"
            );
        } else {
            source.close(Duration::from_secs(2)).unwrap();
            let rdma = source.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    rdma.active_grant_pds,
                    rdma.opened_grant_pds,
                    rdma.closed_grant_pds
                ),
                (0, 1, 1),
                "{mode}"
            );
        }
        let log = native_events.lock().unwrap();
        assert!(
            !log.contains(&"alloc_window"),
            "PeerDirect must never use MW"
        );
        if matches!(mode, "copy" | "gpu_pd_close" | "gpu_flush" | "gpu_pop") {
            let pd = log.iter().position(|e| *e == "dealloc_grant_pd").unwrap();
            let qp = log.iter().position(|e| *e == "destroy_qp").unwrap();
            let payload = log.iter().position(|e| *e == "deregister").unwrap();
            assert!(qp < payload && payload < pd, "{mode}: {log:?}");
        }
        drop(log);
        drop(remote);
        drop(allocation);
        drop(owner);
        drop(source);
        assert_eq!(
            weak.upgrade().is_some(),
            retained,
            "{mode}: actual external owner"
        );
    }
}

#[test]
fn direct_mode_requires_provider_scatter_disabled_before_engine_construction() {
    use crate::transport::rdma::{RdmaConfig, RdmaCudaMemory};
    if let Ok(expected) = std::env::var("ORBITTE_SCATTER_TEST") {
        let config = RdmaConfig {
            cuda_memory: RdmaCudaMemory::Direct,
            ..Default::default()
        };
        assert_eq!(config.validate().is_ok(), expected == "0");
        return;
    }
    for value in ["missing", "1", "0"] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child.args(["--exact", "cuda::tests::rdma::direct_mode_requires_provider_scatter_disabled_before_engine_construction"]);
        child
            .env("ORBITTE_SCATTER_TEST", value)
            .env_remove("MLX5_SCATTER_TO_CQE");
        if value != "missing" {
            child.env("MLX5_SCATTER_TO_CQE", value);
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "{value}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn successful_native_registration_followed_by_failed_pop_never_publishes_gpu_memory() {
    use crate::engine::tests::{rdma_fixture_with_runtime, source_runtime};
    use crate::transport::rdma::tests::{after_register, gpu_runtime};
    let engine = rdma_fixture_with_runtime(gpu_runtime("copy"));
    let owner = allocation(&[1; 16]);
    let weak = Arc::downgrade(&owner);
    let memory = memory(&owner);
    let captured = owner.clone();
    after_register(move || captured.pop_error.store(201, Ordering::Release));
    assert!(matches!(
        engine.register_cuda_memory_batch(&[(memory.clone(), 0, 16)], Permission::RemoteRead),
        Err(Error::Unquiesced { .. })
    ));
    let stats = source_runtime(&engine).stats().unwrap();
    assert_eq!(
        (stats.persistent_mrs, stats.retained_persistent_mrs),
        (0, 0)
    );
    assert_eq!(STACK.with(|s| s.borrow().len()), 1);
    STACK.with(|s| s.borrow_mut().clear());
    engine.close(Duration::from_secs(2)).unwrap();
    drop(engine);
    drop(memory);
    drop(owner);
    assert!(
        weak.upgrade().is_some(),
        "failed restoration keeps the real CUDA owner"
    );
}

#[test]
fn private_gpu_source_rendezvous_keeps_route_and_cache_ownership_separate() {
    use crate::engine::tests::{rdma_fixture_with_runtime, source_runtime};
    use crate::transport::rdma::tests::{gpu_handshake, gpu_runtime};
    let engine = rdma_fixture_with_runtime(gpu_runtime("copy"));
    let owner = allocation(&[1; 16]);
    let memory = memory(&owner);
    let registered = engine
        .register_cuda_memory_batch(&[(memory, 0, 16)], Permission::RemoteRead)
        .unwrap()
        .remove(0);
    gpu_handshake::check_source(source_runtime(&engine), engine.source_epoch(), &registered);
    engine.close(Duration::from_secs(2)).unwrap();
}
