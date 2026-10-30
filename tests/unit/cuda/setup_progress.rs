use std::time::Instant;

use super::socket_gate::{Boundary, SocketGate};
use super::staging_cross::engine;
use super::*;
use crate::RemoteEffect;
use crate::runtime::control_plane::Reply;

fn wait_until(mut ready: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(3);
    while !ready() {
        assert!(
            Instant::now() < until,
            "native setup did not reach the held boundary"
        );
        std::thread::yield_now();
    }
}

#[test]
fn partial_native_setup_keeps_other_cross_errors_observable() {
    for (status, length, occurrence) in [(0, 188, 2), (9, 0, 4), (0, 89, 3), (9, 0, 6)] {
        for cleanup_failure in [false, true] {
            for operation in [TransferOp::Read, TransferOp::Write] {
                let (source, _) = engine(7, 2, "copy", 4);
                let mode = if cleanup_failure {
                    "copy_cross_hold_first_error_close0"
                } else {
                    "copy_cross_hold_first_error"
                };
                let (requester, hold) = engine(7, 3, mode, 4);
                let requester = Arc::new(requester);
                let a = allocation(&[0x57; 64]);
                let b = allocation(&[0xcc; 64]);
                let weak = Arc::downgrade(&b);
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
                            status,
                            epoch: peer.epoch,
                            length,
                        }
                        .encode(),
                        occurrence,
                    ),
                );
                peer.endpoint = gate.endpoint;
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
                        Duration::from_secs(10),
                    )
                    .unwrap();
                wait_until(|| hold.0.lock().unwrap().0);
                gate.reached.recv_timeout(Duration::from_secs(3)).unwrap();
                assert_eq!(requester.stats().unwrap().rdma.unwrap().completed_sends, 0);
                assert!(requester.stats().unwrap().rdma.unwrap().work_requests > 0);
                assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..43]);
                assert!(matches!(batch.free(), Err(Error::BatchBusy)));
                // The first accepted payload CQE becomes an error while the
                // second parent's setup response remains withheld until terminal.
                hold.0.lock().unwrap().1 = true;
                hold.1.notify_all();
                let report = batch
                    .wait(Duration::from_secs(2))
                    .expect("setup wait hid another CROSS failure until peer release");
                assert_eq!(report.locally_drained, !cleanup_failure, "{report:?}");
                let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
                assert!(
                    if cleanup_failure {
                        matches!(failure.error, Error::Unquiesced { .. })
                    } else {
                        matches!(failure.error, Error::Backend { .. } | Error::Unavailable(_))
                    },
                    "{status}/{length}/{occurrence}/{cleanup_failure}/{operation:?}: {failure:?}"
                );
                assert_eq!(
                    report.state,
                    if matches!(failure.error, Error::Cancelled) {
                        BatchState::Cancelled
                    } else {
                        BatchState::Failed
                    }
                );
                // CROSS only touches pinned staging; no WRITE Delegate/POST
                // may reach the exported GPU before the finish receipt.
                assert_eq!(failure.remote_effect, RemoteEffect::None);
                assert!(requester.stats().unwrap().rdma.unwrap().completion_errors > 0);
                assert_eq!(requester.stats().unwrap().staging.active_tasks, 0);
                assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
                gate.closed.recv_timeout(Duration::from_secs(1)).unwrap();
                assert_eq!(source.stats().unwrap().active_source_operations, 1);
                assert_eq!(source_gpu.allocation.lock().unwrap().dma, vec![1..42]);
                drop(gate);
                source.close(Duration::from_secs(3)).unwrap();
                assert_eq!(unsafe { &*a.bytes.get() }.as_ref(), &[0x57; 64]);
                assert_eq!(unsafe { &*b.bytes.get() }.as_ref(), &[0xcc; 64]);
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
                    "actual owner retention"
                );
            }
        }
    }
}

#[test]
fn fragmented_native_setup_validates_all_grants_before_payload() {
    // A two-slice parent: admission is status-9 occurrence 1, and
    // each grant contributes another status-9 reply. Stop before its last GRANT.
    for (status, length, occurrence, payload, prefix, corrupt) in [
        (0, 188, 1, 188, 13, None),
        (0, 188, 1, 188, 36 + 187, None),
        (9, 0, 1, 0, 13, None),
        (0, 89, 2, 89, 13, None),
        (0, 89, 2, 89, 36 + 88, None),
        (9, 0, 3, 182, 13, None),
        (9, 0, 3, 182, 36 + 141, None),
        (9, 0, 3, 182, 36 + 142 + 39, None),
        (9, 0, 3, 182, 36 + 141, Some(36 + 141)),
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let (source, _) = engine(7, 2, "copy", 4);
            let (requester, _) = engine(7, 3, "copy", 4);
            let requester = Arc::new(requester);
            let a_bytes: [u8; 64] = std::array::from_fn(|i| (i * 37 + 11) as u8);
            let b_bytes: [u8; 64] = std::array::from_fn(|i| (i * 19 + 53) as u8);
            let a = allocation(&a_bytes);
            let b = allocation(&b_bytes);
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
                .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 64)], Permission::LocalOnly)
                .unwrap()
                .remove(0);
            let mut peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(3),
                )
                .unwrap();
            let mut gate = SocketGate::new(
                source.endpoint(),
                Boundary::Setup {
                    reply: Reply {
                        status,
                        epoch: peer.epoch,
                        length,
                    }
                    .encode(),
                    occurrence,
                    payload,
                    prefix,
                    corrupt,
                },
            );
            peer.endpoint = gate.endpoint;
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
                    Duration::from_secs(10),
                )
                .unwrap();
            gate.reached.recv_timeout(Duration::from_secs(3)).unwrap();
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..43]);
            assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
            assert_eq!(requester.stats().unwrap().rdma.unwrap().completed_sends, 0);
            let rdma = requester.stats().unwrap().rdma.unwrap();
            assert_eq!(
                rdma.posted_work_requests, 0,
                "partial parent cannot submit payload"
            );
            assert_eq!(rdma.work_requests, 0);
            gate.resume();
            let report = batch.wait(Duration::from_secs(3)).unwrap();
            assert!(report.locally_drained, "{report:?}");
            if corrupt.is_some() {
                assert_eq!(report.state, BatchState::Failed);
                let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
                assert!(
                    matches!(
                        failure.error,
                        Error::Protocol("RDMA phase or grant binding mismatch")
                    ),
                    "{failure:?}"
                );
                assert_eq!(source.stats().unwrap().staging.remote_crosses, 0);
                assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
                assert_eq!(requester.stats().unwrap().rdma.unwrap().completed_sends, 0);
            } else {
                assert_eq!(report.state, BatchState::Completed, "{report:?}");
                let mut expected_a = a_bytes;
                let mut expected_b = b_bytes;
                if operation == TransferOp::Read {
                    expected_b[2..43].copy_from_slice(&a_bytes[1..42]);
                } else {
                    expected_a[1..42].copy_from_slice(&b_bytes[2..43]);
                }
                let mut observed = [0; 64];
                source_gpu.read(0, &mut observed).unwrap();
                assert_eq!(observed, expected_a);
                local_gpu.read(0, &mut observed).unwrap();
                assert_eq!(observed, expected_b);
                let stats = requester.stats().unwrap().rdma.unwrap();
                assert_eq!(
                    stats.completed_sends, 12,
                    "one SEND per slice, including tails"
                );
                assert_eq!(stats.opened_connections, 1);
                assert!(stats.reused_connections > 0);
                assert_eq!(source.stats().unwrap().rdma.unwrap().completed_receives, 12);
                assert_eq!(source.stats().unwrap().staging.remote_unpins, 1);
            }
            batch.free().unwrap();
            drop(gate);
            requester.close(Duration::from_secs(3)).unwrap();
            source.close(Duration::from_secs(3)).unwrap();
            assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
        }
    }
}
