use std::time::Instant;

use super::socket_gate::{Boundary, SocketGate};
use super::staging_cross::engine;
use super::*;
use crate::RemoteEffect;
use crate::engine::tests::source_runtime;
use crate::runtime::control_plane::Reply;

fn wait_until(mut ready: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(3);
    while !ready() {
        assert!(
            Instant::now() < until,
            "native finish did not reach the held boundary"
        );
        std::thread::yield_now();
    }
}

#[test]
fn partial_native_finish_keeps_other_cross_errors_observable() {
    pending_native_finish(false);
}

#[test]
fn pending_native_send_observes_worker_stop_before_deadline() {
    pending_native_finish(true);
}

fn pending_native_finish(send_pending: bool) {
    for cleanup_failure in [false, true] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let (source, _) = engine(7, 2, "copy", 4);
            let mode = match (send_pending, cleanup_failure) {
                (false, false) => "copy_cross_hold_first_error",
                (false, true) => "copy_cross_hold_first_error_close0",
                (true, false) => "copy_send_hold",
                (true, true) => "copy_send_hold_close0",
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
            let gate = (!send_pending).then(|| {
                SocketGate::new(
                    source.endpoint(),
                    Boundary::Native(
                        Reply {
                            status: 0,
                            epoch: peer.epoch,
                            length: 0,
                        }
                        .encode(),
                        1,
                    ),
                )
            });
            if let Some(gate) = &gate {
                peer.endpoint = gate.endpoint;
            }
            let mut batch = requester
                .submit_async(
                    operation,
                    &peer,
                    &[TransferRequest {
                        priority: Default::default(),
                        remote_staging: true,
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
            if let Some(gate) = &gate {
                gate.reached.recv_timeout(Duration::from_secs(3)).unwrap();
                assert!(requester.stats().unwrap().rdma.unwrap().completed_sends > 0);
            } else {
                assert_eq!(requester.stats().unwrap().rdma.unwrap().completed_sends, 0);
            }
            if send_pending {
                let events = crate::transport::rdma::tests::events(source_runtime(&requester));
                assert_eq!(
                    events
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|event| **event == "send_completion")
                        .count(),
                    1
                );
            }
            assert!(requester.stats().unwrap().rdma.unwrap().work_requests > 0);
            assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..43]);
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            if send_pending {
                // Stop the actual worker while its accepted SEND owes a CQE.
                // A blocking wait would hide stop until the ten-second deadline.
                let closed = source_runtime(&requester)
                    .proxy
                    .close(Instant::now() + Duration::from_secs(2));
                assert!(
                    if cleanup_failure {
                        matches!(closed, Err(Error::Unquiesced { .. }))
                    } else {
                        closed.is_ok()
                    },
                    "{closed:?}"
                );
            } else {
                // Only the first payload CQE becomes visible. The later
                // CROSS's partial Reply remains withheld until after terminal.
                hold.0.lock().unwrap().1 = true;
                hold.1.notify_all();
            }
            let report = batch
                .wait(Duration::from_secs(2))
                .expect("finish wait hid native failure or worker stop until peer release");
            assert_eq!(report.locally_drained, !cleanup_failure, "{report:?}");
            let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
            assert!(
                if cleanup_failure {
                    matches!(failure.error, Error::Unquiesced { .. })
                } else if send_pending {
                    matches!(
                        failure.error,
                        Error::Cancelled | Error::Internal("RDMA close with unresolved work")
                    )
                } else {
                    matches!(failure.error, Error::Backend { .. } | Error::Unavailable(_))
                },
                "{send_pending}/{cleanup_failure}/{operation:?}: {failure:?}"
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
            if !send_pending {
                assert!(requester.stats().unwrap().rdma.unwrap().completion_errors > 0);
            }
            assert_eq!(requester.stats().unwrap().staging.active_tasks, 0);
            assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
            if let Some(gate) = &gate {
                gate.closed.recv_timeout(Duration::from_secs(1)).unwrap();
                assert_eq!(source.stats().unwrap().active_source_operations, 1);
                assert_eq!(source_gpu.allocation.lock().unwrap().dma, vec![1..42]);
            }
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

#[test]
fn fragmented_native_receipts_resume_once_and_validate_the_binding() {
    for (prefix, corrupt) in [
        (13, false),
        (36 + 141, false),
        (36 + 142 + 141, false),
        (36 + 141, true),
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
                Boundary::Finish(
                    Reply {
                        status: 0,
                        epoch: peer.epoch,
                        length: 0,
                    }
                    .encode(),
                    prefix,
                    corrupt,
                ),
            );
            peer.endpoint = gate.endpoint;
            let mut batch = requester
                .submit_async(
                    operation,
                    &peer,
                    &[TransferRequest {
                        priority: Default::default(),
                        remote_staging: true,
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
            assert_eq!(requester.stats().unwrap().rdma.unwrap().completed_sends, 1);
            gate.resume();
            let report = batch.wait(Duration::from_secs(3)).unwrap();
            assert!(report.locally_drained, "{report:?}");
            if corrupt {
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
                assert_eq!(requester.stats().unwrap().rdma.unwrap().completed_sends, 1);
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
