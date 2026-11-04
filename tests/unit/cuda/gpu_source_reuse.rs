use super::*;
use crate::engine::tests::{rdma_fixture_with_runtime, source_runtime};
use crate::test_support::EVENTUAL_PROGRESS;
use crate::transport::rdma::tests::{events, gpu_runtime, gpu_runtime_with_key_reuse, runtime};

#[test]
fn gpu_source_reuse_confines_each_remote_key_to_its_qp_range_and_direction() {
    for mode in [
        "gpu_key_copy",
        "gpu_key_unsupported",
        "gpu_key_busy",
        "gpu_key_probe_query",
        "disabled",
    ] {
        let capable = mode == "gpu_key_copy";
        let rdma = gpu_runtime_with_key_reuse(
            if mode == "disabled" {
                "gpu_key_copy"
            } else {
                mode
            },
            mode != "disabled",
        );
        let source = rdma_fixture_with_runtime(rdma);
        let requester = rdma_fixture_with_runtime(runtime(if capable {
            "direct:copy_check_gpu_keys"
        } else {
            "direct:copy"
        }));
        let owner = allocation(&[0x5a; 24]);
        let backing = memory(&owner);
        let remote = source
            .register_cuda_memory_batch(&[(backing.clone(), 2, 20)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_memory(vec![0xac; 18].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
            .unwrap();
        let mut gpu = [0x5a; 24];
        let mut cpu = [0xac; 18];
        for (i, op) in [
            TransferOp::Read,
            TransferOp::Read,
            TransferOp::Write,
            TransferOp::Write,
        ]
        .into_iter()
        .enumerate()
        {
            let offset = 3 + i;
            if op == TransferOp::Write {
                cpu[3..11].fill(0x40 + i as u8);
                local.write(3, &cpu[3..11]).unwrap();
                gpu[2 + offset..10 + offset].copy_from_slice(&cpu[3..11]);
            } else {
                cpu[3..11].copy_from_slice(&gpu[2 + offset..10 + offset]);
            }
            let req = TransferRequest::new(
                Transport::Rdma,
                local.base_address() + 3,
                remote.base_address() + offset as u64,
                8,
            );
            let report = requester
                .submit_and_wait(op, &peer, &[req], EVENTUAL_PROGRESS)
                .unwrap();
            assert!(
                report.is_complete() && report.locally_drained,
                "{mode}: {report:?}"
            );
            let mut bytes = [0; 24];
            backing.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, gpu);
            local.read(0, &mut bytes[..18]).unwrap();
            assert_eq!(bytes[..18], cpu);
        }
        let stats = source.stats().unwrap().rdma.unwrap();
        let log = events(source_runtime(&source));
        let log = log.lock().unwrap();
        assert!(!log.contains(&"alloc_window"));
        assert_eq!(stats.opened_grant_pds, if capable { 0 } else { 4 });
        if capable {
            assert_eq!(
                (
                    stats.opened_gpu_grant_keys,
                    stats.invalidated_gpu_grant_keys,
                    stats.reused_persistent_mrs
                ),
                (4, 4, 4)
            );
            assert!(stats.reused_connections >= 2);
            assert_eq!(stats.opened_persistent_mrs, 1);
            assert_eq!(log.iter().filter(|&&e| e == "gpu_key_destroy").count(), 4);
        } else {
            assert_eq!(stats.opened_gpu_grant_keys, 0);
            assert_eq!(
                log.contains(&"gpu_key_probe_destroy"),
                mode == "gpu_key_probe_query"
            );
        }
        drop(log);
        source.unregister_memory(&remote).unwrap();
        requester.close(EVENTUAL_PROGRESS).unwrap();
        source.close(EVENTUAL_PROGRESS).unwrap();
        assert_eq!(source.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
    }
}

#[test]
fn gpu_source_reuse_uncertain_destroy_or_visibility_keeps_the_parent_and_loan() {
    for mode in ["gpu_key_destroy", "gpu_key_flush"] {
        let source = rdma_fixture_with_runtime(gpu_runtime(mode));
        let requester = rdma_fixture_with_runtime(runtime("direct:copy_check_gpu_keys"));
        let owner = allocation(&[0x5a; 24]);
        let weak = Arc::downgrade(&owner);
        let backing = memory(&owner);
        let remote = source
            .register_cuda_memory_batch(&[(backing.clone(), 0, 24)], Permission::RemoteWrite)
            .unwrap()
            .remove(0);
        let log = events(source_runtime(&source));
        if mode == "gpu_key_flush" {
            owner.rdma_flush_error.store(700, Ordering::Release);
        }
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
            .unwrap();
        let local = requester
            .register_memory(vec![0xac; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let request = TransferRequest::new(
            Transport::Rdma,
            local.base_address(),
            remote.base_address() + 5,
            8,
        );
        let report = requester
            .submit_and_wait(TransferOp::Write, &peer, &[request], EVENTUAL_PROGRESS)
            .unwrap();
        assert_eq!(report.completed_requests, 0);
        assert_eq!(
            report.failure.unwrap().remote_effect,
            crate::RemoteEffect::Unknown
        );
        source.unregister_memory(&remote).unwrap();
        assert!(source.close_source(Duration::from_millis(5)).is_err());
        let held = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                held.opened_gpu_grant_keys,
                held.invalidated_gpu_grant_keys,
                held.retained_grants,
                held.direct_bytes,
                held.persistent_mrs
            ),
            (1, 1, 1, 8, 1)
        );
        assert!(backing.read(5, &mut [0; 8]).is_err());
        assert_eq!(
            log.lock()
                .unwrap()
                .iter()
                .filter(|&&e| e == "gpu_key_destroy")
                .count(),
            1
        );
        requester.close(EVENTUAL_PROGRESS).unwrap();
        drop((source, remote, backing, owner));
        assert!(weak.upgrade().is_some());
    }
}

#[test]
fn gpu_source_reuse_unreleased_probe_retains_unpublished_parent() {
    for mode in ["gpu_key_probe_destroy", "gpu_key_probe_unknown"] {
        let source = rdma_fixture_with_runtime(gpu_runtime(mode));
        let owner = allocation(&[0x5a; 24]);
        let weak = Arc::downgrade(&owner);
        let result =
            source.register_cuda_memory_batch(&[(memory(&owner), 0, 24)], Permission::RemoteRead);
        assert!(matches!(result, Err(Error::Unquiesced { .. })));
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.persistent_mrs,
                stats.retained_persistent_mrs,
                stats.opened_gpu_grant_keys
            ),
            (1, 1, 0)
        );
        assert_eq!(
            events(source_runtime(&source))
                .lock()
                .unwrap()
                .iter()
                .filter(|&&e| e == "gpu_key_probe_destroy")
                .count(),
            usize::from(mode == "gpu_key_probe_destroy")
        );
        drop((source, owner));
        assert!(weak.upgrade().is_some());
    }
}

#[test]
fn gpu_source_reuse_unregister_waits_for_the_exact_invalidation() {
    let source = rdma_fixture_with_runtime(gpu_runtime("gpu_key_copy"));
    let requester = rdma_fixture_with_runtime(runtime("direct:pause_copy_check_gpu_keys"));
    let pause = crate::transport::rdma::tests::pause(source_runtime(&requester));
    let owner = allocation(&[0x5a; 24]);
    let memory = memory(&owner);
    let remote = source
        .register_cuda_memory_batch(&[(memory.clone(), 0, 24)], Permission::RemoteWrite)
        .unwrap()
        .remove(0);
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
        .unwrap();
    let local = requester
        .register_memory(vec![0x82; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest::new(
        Transport::Rdma,
        local.base_address(),
        remote.base_address() + 5,
        8,
    );
    std::thread::scope(|scope| {
        let transfer = scope.spawn(|| {
            requester
                .submit_and_wait(TransferOp::Write, &peer, &[request], EVENTUAL_PROGRESS)
                .unwrap()
        });
        let guard = pause.0.lock().unwrap();
        let (mut gate, _) = pause
            .1
            .wait_timeout_while(guard, EVENTUAL_PROGRESS, |s| !s.0)
            .unwrap();
        assert!(gate.0);
        source.unregister_memory(&remote).unwrap();
        let held = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                held.opened_gpu_grant_keys,
                held.invalidated_gpu_grant_keys,
                held.active_grants,
                held.persistent_mrs
            ),
            (1, 0, 1, 1)
        );
        assert!(memory.read(5, &mut [0; 8]).is_err());
        gate.1 = true;
        pause.1.notify_all();
        drop(gate);
        assert!(transfer.join().unwrap().is_complete());
    });
    let mut bytes = [0; 24];
    memory.read(0, &mut bytes).unwrap();
    let mut expected = [0x5a; 24];
    expected[5..13].fill(0x82);
    assert_eq!(bytes, expected);
    let idle = source.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (
            idle.invalidated_gpu_grant_keys,
            idle.active_grants,
            idle.persistent_mrs
        ),
        (1, 0, 0)
    );
    requester.close(EVENTUAL_PROGRESS).unwrap();
    source.close(EVENTUAL_PROGRESS).unwrap();
}

#[test]
fn gpu_source_reuse_query_failure_retires_the_unpublished_key_before_payload() {
    let source = rdma_fixture_with_runtime(gpu_runtime("gpu_key_query"));
    let requester = rdma_fixture_with_runtime(runtime("direct:copy_check_gpu_keys"));
    let owner = allocation(&[0x5a; 24]);
    let memory = memory(&owner);
    let remote = source
        .register_cuda_memory_batch(&[(memory.clone(), 0, 24)], Permission::RemoteWrite)
        .unwrap()
        .remove(0);
    let log = events(source_runtime(&source));
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
        .unwrap();
    let local = requester
        .register_memory(vec![0x82; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest::new(
        Transport::Rdma,
        local.base_address(),
        remote.base_address() + 5,
        8,
    );
    let report = requester
        .submit_and_wait(TransferOp::Write, &peer, &[request], EVENTUAL_PROGRESS)
        .unwrap();
    assert!(!report.is_complete());
    assert_eq!(
        requester
            .stats()
            .unwrap()
            .rdma
            .unwrap()
            .posted_work_requests,
        0
    );
    source.close(EVENTUAL_PROGRESS).unwrap();
    let mut bytes = [0; 24];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0x5a; 24]);
    let events = log.lock().unwrap();
    let closed = events.iter().position(|&e| e == "destroy_qp").unwrap();
    assert!(closed < events.iter().position(|&e| e == "gpu_key_destroy").unwrap());
    requester.close(EVENTUAL_PROGRESS).unwrap();
}
