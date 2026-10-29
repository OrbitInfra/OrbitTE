use super::*;
#[cfg(feature = "rdma")]
use crate::runtime::proxy_manager::ProxyManager;

fn engine(transport: Transport) -> TransferEngine {
    let config = EngineConfig {
        remote_staging: true,
        staging_chunk_size: 3,
        staging_chunk_count: 16,
        staging_pipeline_depth: 1,
        handler_threads: 16,
        max_inflight_batches: 16,
        ..EngineConfig::default()
    };
    #[cfg(feature = "rdma")]
    if transport == Transport::Rdma {
        let mut runtime = crate::transport::rdma::tests::runtime_with_config(
            crate::transport::rdma::RdmaConfig::new("copy"),
        );
        runtime.proxy = ProxyManager::new(3, 16).unwrap();
        return crate::engine::tests::rdma_fixture_with_config(runtime, config);
    }
    let _ = transport;
    TransferEngine::new(config).unwrap()
}

#[test]
fn eight_active_sessions_defer_pin_and_expired_ninth_does_not_block_on_native_work() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let source = engine(transport);
            let requester = Arc::new(engine(transport));
            let devices: Vec<_> = (0..9).map(|_| allocation(&[0x57; 16])).collect();
            let memories: Vec<_> = devices.iter().map(memory).collect();
            let remote = source
                .register_cuda_memory_batch(
                    &memories
                        .iter()
                        .map(|gpu| (Arc::clone(gpu), 0, 16))
                        .collect::<Vec<_>>(),
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let queued_device = allocation(&[0xcc; 16]);
            let queued_gpu = memory(&queued_device);
            let mut local: Vec<_> = (0..8)
                .map(|_| {
                    requester
                        .register_memory(vec![0xcc; 16].into_boxed_slice(), Permission::LocalOnly)
                        .unwrap()
                })
                .collect();
            local.push(
                requester
                    .register_cuda_memory_batch(
                        &[(Arc::clone(&queued_gpu), 0, 16)],
                        Permission::LocalOnly,
                    )
                    .unwrap()
                    .remove(0),
            );
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(3),
                )
                .unwrap();
            let request = |index: usize| TransferRequest {
                transport,
                local_address: local[index].base_address() + 2,
                remote_address: remote[index].base_address() + 1,
                length: 8,
            };
            let mut active = Vec::new();
            let mut releases = Vec::new();
            for (index, device) in devices.iter().take(8).enumerate() {
                let (entered, waiting) = std::sync::mpsc::sync_channel(0);
                let (resume, blocked) = std::sync::mpsc::sync_channel(0);
                *device.event_gate.lock().unwrap() = Some((entered, blocked));
                active.push(
                    requester
                        .submit_async(
                            operation,
                            &peer,
                            &[request(index)],
                            None,
                            Duration::from_secs(5),
                        )
                        .unwrap(),
                );
                waiting.recv_timeout(Duration::from_secs(2)).unwrap();
                releases.push(resume);
            }
            let stats = requester.stats().unwrap().staging;
            assert_eq!(
                (stats.worker_shards, stats.active_tasks, stats.queued_tasks),
                (8, 8, 0)
            );
            assert_eq!(source.stats().unwrap().staging.remote_pins, 8);
            // Directional source close must not stop this engine's outgoing pool.
            requester.close_source(Duration::from_secs(1)).unwrap();
            let mut queued = requester
                .submit_async(
                    operation,
                    &peer,
                    &[request(8)],
                    None,
                    Duration::from_millis(500),
                )
                .unwrap();
            let until = std::time::Instant::now() + Duration::from_secs(1);
            while requester.stats().unwrap().staging.queued_tasks == 0 {
                assert!(std::time::Instant::now() < until);
                assert_eq!(queued.status().unwrap().state, BatchState::Pending);
                std::thread::yield_now();
            }
            assert_eq!(queued_gpu.allocation.lock().unwrap().dma, vec![2..10]);
            assert_eq!(queued_device.pins.lock().unwrap().len(), 1);
            let report = queued.wait(Duration::from_secs(1)).unwrap();
            assert!(
                report.state == BatchState::Failed && report.locally_drained,
                "{transport:?}/{operation:?}: {report:?}"
            );
            let failure = queued.submissions().unwrap().remove(0).failure.unwrap();
            assert!(matches!(failure.error, Error::Timeout), "{failure:?}");
            assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
            queued.free().unwrap();
            assert!(queued_gpu.allocation.lock().unwrap().dma.is_empty());
            assert_eq!(queued_device.event_waits.load(Ordering::Acquire), 0);
            let stats = requester.stats().unwrap().staging;
            assert_eq!(
                (stats.worker_shards, stats.active_tasks, stats.queued_tasks),
                (8, 8, 0)
            );
            assert_eq!(stats.cancelled_tasks, 1);
            assert_eq!(source.stats().unwrap().staging.remote_pins, 8);
            assert_eq!(devices[8].event_waits.load(Ordering::Acquire), 0);
            assert!(devices[8].pins.lock().unwrap().is_empty());
            let mut untouched = [0; 16];
            local[8].read(0, &mut untouched).unwrap();
            assert_eq!(untouched, [0xcc; 16]);
            for resume in releases {
                resume.send(()).unwrap();
            }
            for mut batch in active {
                let report = batch.wait(Duration::from_secs(3)).unwrap();
                assert!(
                    report.state == BatchState::Completed && report.locally_drained,
                    "{transport:?}/{operation:?}: {report:?}; {:?}",
                    batch.submissions().unwrap()
                );
                batch.free().unwrap();
            }
            let report = requester
                .submit_and_wait(operation, &peer, &[request(8)], Duration::from_secs(3))
                .unwrap();
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            for (local, gpu) in local.iter().zip(&memories) {
                let mut actual = [0; 16];
                local.read(0, &mut actual).unwrap();
                let mut expected = [0xcc; 16];
                if operation == TransferOp::Read {
                    expected[2..10].fill(0x57);
                }
                assert_eq!(actual, expected);
                gpu.read(0, &mut actual).unwrap();
                let mut expected = [0x57; 16];
                if operation == TransferOp::Write {
                    expected[1..9].fill(0xcc);
                }
                assert_eq!(actual, expected);
            }
            requester.close_requester(Duration::from_secs(3)).unwrap();
            let stats = requester.stats().unwrap().staging;
            assert_eq!(
                (
                    stats.worker_shards,
                    stats.opened_worker_shards,
                    stats.active_tasks,
                    stats.queued_tasks
                ),
                (8, 8, 0, 0)
            );
            requester.close(Duration::from_secs(3)).unwrap();
            source.close(Duration::from_secs(3)).unwrap();
            assert_eq!(requester.stats().unwrap().staging.worker_shards, 0);
            assert!(queued_device.pins.lock().unwrap().is_empty());
            assert!(
                devices
                    .iter()
                    .all(|device| device.pins.lock().unwrap().is_empty())
            );
        }
    }
}
