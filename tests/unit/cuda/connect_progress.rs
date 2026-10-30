use std::io::{Read, Write};
use std::net::TcpListener;
#[cfg(feature = "rdma")]
use std::time::Instant;

use super::requester_staging::engine;
use super::socket_gate::{Boundary, SocketGate};
use super::*;
use crate::RemoteEffect;
use crate::runtime::control_plane::Reply;

fn partial_pin(operation: TransferOp, stop: bool) {
    let source = engine(Transport::Tcp, 3, 2);
    // The combined profile exposes the injected runtime's actual ProxyManager
    // so this test can stop its workers without changing public Batch.cancel.
    let requester = engine(
        if cfg!(feature = "rdma") {
            Transport::Rdma
        } else {
            Transport::Tcp
        },
        3,
        3,
    );
    let requester = Arc::new(requester);
    let a = allocation(&[0x57; 32]);
    let b = allocation(&[0xcc; 32]);
    let weak = Arc::downgrade(&b);
    let source_gpu = memory(&a);
    let local_gpu = memory(&b);
    let remote = source
        .register_cuda_memory_batch(&[(source_gpu.clone(), 0, 32)], Permission::RemoteReadWrite)
        .unwrap()
        .remove(0);
    let local = requester
        .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 32)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let mut peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(3),
        )
        .unwrap();
    let gate = SocketGate::new(
        source.endpoint(),
        Boundary::Pin(if operation == TransferOp::Read {
            13
        } else {
            53
        }),
    );
    peer.endpoint = gate.endpoint;
    let mut batch = requester
        .submit_async(
            operation,
            &peer,
            &[TransferRequest {
                transport: Transport::Tcp,
                local_address: local.base_address() + 2,
                remote_address: remote.base_address() + 1,
                length: 17,
            }],
            None,
            if stop {
                Duration::from_secs(10)
            } else {
                Duration::from_millis(750)
            },
        )
        .unwrap();
    gate.reached.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(source_gpu.allocation.lock().unwrap().dma, vec![1..18]);
    assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..19]);
    assert!(b.submitted_copies.lock().unwrap().is_empty());
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    #[cfg(feature = "rdma")]
    if stop {
        crate::engine::tests::source_runtime(&requester)
            .proxy
            .close(Instant::now() + Duration::from_secs(2))
            .expect("worker stop did not interrupt partial Pin before its original deadline");
    }
    let report = batch.wait(Duration::from_secs(2)).unwrap();
    assert_eq!(
        report.state,
        if stop {
            BatchState::Cancelled
        } else {
            BatchState::Failed
        }
    );
    assert!(report.locally_drained, "{report:?}");
    assert_eq!(report.transferred_bytes, 0);
    let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
    assert!(
        if stop {
            matches!(failure.error, Error::Cancelled)
        } else {
            matches!(failure.error, Error::Timeout)
        },
        "{failure:?}"
    );
    assert_eq!(failure.remote_effect, RemoteEffect::None);
    assert_eq!(requester.stats().unwrap().staging.active_tasks, 0);
    assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
    assert!(b.submitted_copies.lock().unwrap().is_empty());
    gate.closed
        .recv_timeout(Duration::from_secs(1))
        .expect("task terminal retained its control socket");
    let stats = source.stats().unwrap();
    assert_eq!(
        stats.active_source_operations, 1,
        "source gate is still held"
    );
    assert_eq!(
        (
            stats.staging.remote_delegates,
            stats.staging.remote_crosses,
            stats.staging.remote_unpins
        ),
        (0, 0, 0)
    );
    assert_eq!(source_gpu.allocation.lock().unwrap().dma, vec![1..18]);
    drop(gate);
    source.close(Duration::from_secs(3)).unwrap();
    requester.close(Duration::from_secs(3)).unwrap();
    assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
    let mut bytes = [0; 32];
    source_gpu.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0x57; 32]);
    local_gpu.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0xcc; 32]);
    batch.free().unwrap();
    drop(batch);
    drop(local);
    drop(local_gpu);
    drop(requester);
    drop(b);
    assert!(weak.upgrade().is_none());
}

#[test]
fn public_deadline_retires_partial_pin_without_publishing_a_chunk() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        partial_pin(operation, false);
    }
}

#[cfg(feature = "rdma")]
#[test]
fn worker_stop_retires_partial_pin_before_peer_release() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        partial_pin(operation, true);
    }
}

#[test]
fn pin_error_and_invalid_negotiation_never_wait_for_a_success_tail_or_start_cross() {
    for (status, chunk, widths, expected) in [
        (6, 0, None, "capacity"),
        (0, 0, None, "chunk"),
        (0, 4, None, "chunk"),
        (0, 3, Some([0, 1]), "depth"),
        (0, 3, Some([1, 0]), "cross"),
    ] {
        let source = engine(Transport::Tcp, 3, 1);
        let requester = engine(Transport::Tcp, 3, 1);
        let requester = Arc::new(requester);
        let a = allocation(&[0x57; 8]);
        let remote = source
            .register_cuda_memory_batch(&[(memory(&a), 0, 8)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let mut peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        peer.endpoint = listener.local_addr().unwrap();
        let mut batch = requester
            .submit_async(
                TransferOp::Write,
                &peer,
                &[TransferRequest {
                    transport: Transport::Tcp,
                    local_address: local.base_address(),
                    remote_address: remote.base_address(),
                    length: 8,
                }],
                None,
                Duration::from_secs(10),
            )
            .unwrap();
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut pin = [0; 63];
        socket.read_exact(&mut pin).unwrap();
        socket
            .write_all(
                &Reply {
                    status,
                    epoch: peer.epoch,
                    length: chunk,
                }
                .encode(),
            )
            .unwrap();
        if let Some(widths) = widths {
            socket.write_all(&[0x71; 16]).unwrap();
            socket.write_all(&widths).unwrap();
        }
        let report = batch
            .wait(Duration::from_secs(2))
            .expect("invalid Pin waited for nonexistent success bytes");
        assert!(
            report.state == BatchState::Failed && report.locally_drained,
            "{report:?}"
        );
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert!(
            match expected {
                "capacity" => matches!(failure.error, Error::ResourceExhausted),
                "chunk" => matches!(failure.error, Error::Protocol("Pin chunk size")),
                "depth" => matches!(failure.error, Error::Protocol("Pin pipeline depth")),
                _ => matches!(failure.error, Error::Protocol("Pin CROSS depth")),
            },
            "{failure:?}"
        );
        assert_eq!(failure.remote_effect, RemoteEffect::None);
        assert_eq!(
            socket.read(&mut [0]).unwrap(),
            0,
            "invalid Pin sent follow-up work"
        );
        batch.free().unwrap();
        requester.close(Duration::from_secs(3)).unwrap();
        source.close(Duration::from_secs(3)).unwrap();
    }
}

#[cfg(feature = "rdma")]
#[test]
fn native_socket_stop_drains_accepted_cross_and_preserves_cleanup_uncertainty() {
    use crate::engine::tests::source_runtime;
    use crate::runtime::proxy_manager::ProxyManager;
    use crate::transport::rdma::tests as verbs;
    for setup in [true, false] {
        for cleanup_failure in [false, true] {
            let source = engine(Transport::Rdma, 3, 2);
            let mut runtime = verbs::runtime(match (setup, cleanup_failure) {
                (true, true) => "direct:copy_cross_hold_close0",
                (true, false) => "direct:copy_cross_hold",
                (false, true) => "direct:copy_close0",
                (false, false) => "direct:copy",
            });
            runtime.proxy = ProxyManager::new(3, 3).unwrap();
            let hold = verbs::pause(&runtime);
            let requester = crate::engine::tests::rdma_fixture_with_config(
                runtime,
                EngineConfig {
                    remote_staging: true,
                    staging_chunk_size: 3,
                    staging_chunk_count: 3,
                    staging_pipeline_depth: 3,
                    handler_threads: 1,
                    max_inflight_batches: 1,
                    ..EngineConfig::default()
                },
            );
            let requester = Arc::new(requester);
            let a = allocation(&[0x57; 32]);
            let b = allocation(&[0xcc; 32]);
            let weak = Arc::downgrade(&b);
            let local_gpu = memory(&b);
            let remote = source
                .register_cuda_memory_batch(&[(memory(&a), 0, 32)], Permission::RemoteReadWrite)
                .unwrap()
                .remove(0);
            let local = requester
                .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 32)], Permission::LocalOnly)
                .unwrap()
                .remove(0);
            let mut peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(3),
                )
                .unwrap();
            let gate = SocketGate::new(
                source.endpoint(),
                Boundary::Native(
                    Reply {
                        status: 0,
                        epoch: peer.epoch,
                        length: if setup { 89 } else { 0 },
                    }
                    .encode(),
                    if setup { 2 } else { 1 },
                ),
            );
            peer.endpoint = gate.endpoint;
            let mut batch = requester
                .submit_async(
                    TransferOp::Read,
                    &peer,
                    &[TransferRequest {
                        transport: Transport::Rdma,
                        local_address: local.base_address() + 2,
                        remote_address: remote.base_address() + 1,
                        length: 17,
                    }],
                    None,
                    Duration::from_secs(10),
                )
                .unwrap();
            gate.reached.recv_timeout(Duration::from_secs(3)).unwrap();
            if setup {
                let until = Instant::now() + Duration::from_secs(2);
                while !hold.0.lock().unwrap().0
                    || requester.stats().unwrap().rdma.unwrap().work_requests == 0
                {
                    assert!(
                        Instant::now() < until,
                        "first CROSS must have an accepted held WR"
                    );
                    std::thread::yield_now();
                }
            }
            if !setup {
                assert!(requester.stats().unwrap().rdma.unwrap().completed_sends > 0);
                assert!(source.stats().unwrap().rdma.unwrap().completed_receives > 0);
            }
            assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..19]);
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            let closed = source_runtime(&requester)
                .proxy
                .close(Instant::now() + Duration::from_secs(2));
            assert!(
                if cleanup_failure {
                    matches!(closed, Err(Error::Unquiesced { .. }))
                } else {
                    closed.is_ok()
                },
                "{setup}/{cleanup_failure}: {closed:?}"
            );
            let report = batch.wait(Duration::from_secs(2)).unwrap();
            assert_eq!(
                report.state,
                if !setup && !cleanup_failure {
                    BatchState::Cancelled
                } else {
                    BatchState::Failed
                }
            );
            assert_eq!(report.locally_drained, !cleanup_failure, "{report:?}");
            let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
            assert!(
                if cleanup_failure {
                    matches!(failure.error, Error::Unquiesced { .. })
                } else if setup {
                    // Closing the partially prepared grant drains its CQ lane,
                    // including the earlier CROSS's unresolved native work.
                    matches!(
                        failure.error,
                        Error::Internal("RDMA close with unresolved work")
                    )
                } else {
                    matches!(failure.error, Error::Cancelled)
                },
                "{setup}/{cleanup_failure}: {failure:?}"
            );
            assert_eq!(failure.remote_effect, RemoteEffect::None);
            assert_eq!(requester.stats().unwrap().staging.active_tasks, 0);
            gate.closed
                .recv_timeout(Duration::from_secs(1))
                .expect("native task terminal retained socket");
            assert_eq!(source.stats().unwrap().active_source_operations, 1);
            assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
            drop(gate);
            source.close(Duration::from_secs(3)).unwrap();
            if cleanup_failure {
                assert_eq!(requester.stats().unwrap().staging.retained_chunks, 3);
                assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            } else {
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
            assert_eq!(
                weak.upgrade().is_some(),
                cleanup_failure,
                "retain actual owner until DMA safety is proven"
            );
        }
    }
}
