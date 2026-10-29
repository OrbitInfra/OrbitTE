use super::*;
use crate::runtime::proxy_manager::ProxyManager;
use crate::transport::rdma::{RdmaConfig, tests as verbs};

type PayloadHold = Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>;

fn engine(chunk: usize, depth: usize, mode: &str, grants: usize) -> (TransferEngine, PayloadHold) {
    let mut config = RdmaConfig::new(mode);
    config.max_endpoints = 1;
    config.max_grants = grants;
    config.max_cqes = grants;
    config.max_direct_bytes = chunk * 2;
    config.block_size = 1;
    let mut runtime = verbs::runtime_with_config(config);
    runtime.proxy = ProxyManager::new(chunk, depth).unwrap();
    let hold = verbs::pause(&runtime);
    (
        crate::engine::tests::rdma_fixture_with_config(
            runtime,
            EngineConfig {
                remote_staging: true,
                staging_chunk_size: chunk,
                staging_chunk_count: depth,
                staging_pipeline_depth: depth,
                handler_threads: 1,
                max_inflight_batches: 1,
                ..EngineConfig::default()
            },
        ),
        hold,
    )
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let until = std::time::Instant::now() + Duration::from_secs(3);
    while !ready() {
        assert!(
            std::time::Instant::now() < until,
            "independent CROSS progress stalled"
        );
        std::thread::yield_now();
    }
}

#[test]
fn second_cross_finishes_on_shared_qp_while_first_payload_completion_is_held() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        // Two 7-byte CROSS legs share four grants/RQ credits: each must adapt
        // to two slices, rather than exhaust all four on its first prepare.
        let (source, _) = engine(7, 2, "copy", 4);
        let (requester, hold) = engine(7, 3, "copy_cross_hold", 4);
        let requester = Arc::new(requester);
        let source_bytes: [u8; 64] = std::array::from_fn(|i| (i * 37 + 11) as u8);
        let local_bytes: [u8; 64] = std::array::from_fn(|i| (i * 19 + 53) as u8);
        let a = allocation(&source_bytes);
        let b = allocation(&local_bytes);
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
        let mut batch = requester
            .submit_async(
                operation,
                &peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 41,
                }],
                None,
                Duration::from_secs(8),
            )
            .unwrap();
        wait_until(|| hold.0.lock().unwrap().0);
        wait_until(|| source.stats().unwrap().staging.remote_crosses == 1);
        let local_stats = requester.stats().unwrap();
        let remote_stats = source.stats().unwrap();
        let local_rdma = local_stats.rdma.unwrap();
        let remote_rdma = remote_stats.rdma.unwrap();
        assert_eq!(
            (
                local_rdma.opened_connections,
                remote_rdma.opened_connections
            ),
            (1, 1)
        );
        assert!(
            local_rdma.work_requests > 0,
            "first payload still owes a CQE"
        );
        assert_eq!(remote_rdma.peak_receive_credits, 4);
        assert!(remote_rdma.receive_credits > 0 && remote_rdma.active_grants > 0);
        assert!(remote_rdma.receive_work_requests > 0);
        assert_eq!(
            (
                local_rdma.receive_admission_rejections,
                remote_rdma.receive_admission_rejections
            ),
            (0, 0)
        );
        assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..43]);
        assert_eq!(source_gpu.allocation.lock().unwrap().dma, vec![1..42]);
        assert_eq!(local_stats.staging.active_chunks, 3);
        assert_eq!(remote_stats.staging.remote_unpins, 0);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        if operation == TransferOp::Read {
            wait_until(|| b.event_query_successes.load(Ordering::Acquire) == 1);
            let observed = unsafe { &*b.bytes.get() };
            assert_eq!(&observed[9..16], &source_bytes[8..15]);
            assert_eq!(&observed[2..9], &local_bytes[2..9]);
        } else {
            assert_eq!(unsafe { &*a.bytes.get() }[1..8], source_bytes[1..8]);
        }
        hold.0.lock().unwrap().1 = true;
        hold.1.notify_all();
        let report = batch.wait(Duration::from_secs(8)).unwrap();
        assert!(
            report.state == BatchState::Completed && report.locally_drained,
            "{operation:?}: {report:?}"
        );
        batch.free().unwrap();
        let mut expected_a = source_bytes;
        let mut expected_b = local_bytes;
        if operation == TransferOp::Read {
            expected_b[2..43].copy_from_slice(&source_bytes[1..42]);
        } else {
            expected_a[1..42].copy_from_slice(&local_bytes[2..43]);
        }
        let mut bytes = [0; 64];
        source_gpu.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, expected_a);
        local_gpu.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, expected_b);
        requester.close(Duration::from_secs(3)).unwrap();
        source.close(Duration::from_secs(3)).unwrap();
        assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
    }
}

#[test]
fn failed_second_cross_drains_first_or_retains_every_uncertain_owner() {
    for cleanup_failure in [false, true] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let (source, _) = engine(7, 2, "copy", 4);
            let (requester, hold) = engine(
                7,
                3,
                if cleanup_failure {
                    "copy_cross_hold_error_close0"
                } else {
                    "copy_cross_hold_error"
                },
                4,
            );
            let requester = Arc::new(requester);
            let a = allocation(&[0x57; 64]);
            let b = allocation(&[0xcc; 64]);
            let local_gpu = memory(&b);
            let weak = Arc::downgrade(&local_gpu);
            let source_gpu = memory(&a);
            let remote = source
                .register_cuda_memory_batch(
                    &[(source_gpu.clone(), 0, 64)],
                    Permission::RemoteReadWrite,
                )
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
            let mut batch = requester
                .submit_async(
                    operation,
                    &peer,
                    &[TransferRequest {
                        transport: Transport::Rdma,
                        local_address: local.base_address() + 2,
                        remote_address: remote.base_address() + 1,
                        length: 41,
                    }],
                    None,
                    Duration::from_secs(5),
                )
                .unwrap();
            let report = batch.wait(Duration::from_secs(8)).unwrap();
            assert!(
                hold.0.lock().unwrap().0,
                "first native payload was accepted"
            );
            assert_eq!(report.state, BatchState::Failed);
            assert_eq!(report.locally_drained, !cleanup_failure, "{report:?}");
            let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
            assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
            assert!(
                if cleanup_failure {
                    matches!(failure.error, Error::Unquiesced { .. })
                } else {
                    matches!(failure.error, Error::Backend { .. })
                },
                "{failure:?}"
            );
            source.close(Duration::from_secs(3)).unwrap();
            assert_eq!(source.stats().unwrap().staging.remote_crosses, 0);
            assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
            let mut observed = [0; 64];
            source_gpu.read(0, &mut observed).unwrap();
            assert_eq!(observed, [0x57; 64]);
            assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &[0xcc; 64]);
            let stats = requester.stats().unwrap();
            if cleanup_failure {
                assert_eq!(stats.staging.retained_chunks, 3);
                assert!(stats.rdma.unwrap().retained_grants >= 2);
                assert_eq!(b.pins.lock().unwrap().len(), 3);
                assert!(matches!(batch.free(), Err(Error::BatchBusy)));
                assert!(matches!(
                    requester.close_requester(Duration::from_millis(5)),
                    Err(Error::Timeout)
                ));
            } else {
                assert_eq!(stats.rdma.unwrap().active_grants, 0);
                assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
                batch.free().unwrap();
                requester.close(Duration::from_secs(3)).unwrap();
                assert!(b.pins.lock().unwrap().is_empty());
            }
            drop(batch);
            drop(local);
            drop(local_gpu);
            drop(requester);
            drop(b);
            assert_eq!(weak.upgrade().is_some(), cleanup_failure);
        }
    }
}
