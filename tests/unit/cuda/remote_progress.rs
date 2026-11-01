use super::*;
use crate::SourceEpoch;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};

#[cfg(feature = "rdma")]
fn wait_until(mut ready: impl FnMut() -> bool) {
    let until = std::time::Instant::now() + Duration::from_secs(3);
    while !ready() {
        assert!(
            std::time::Instant::now() < until,
            "remote operation progress stalled"
        );
        std::thread::yield_now();
    }
}

#[test]
#[cfg(feature = "rdma")]
fn pending_remote_copy_does_not_block_another_submitted_cross() {
    use super::staging_cross::engine;
    for operation in [TransferOp::Read, TransferOp::Write] {
        let (source, _) = engine(7, 2, "copy", 4);
        let (requester, _) = engine(7, 3, "copy", 4);
        let requester = Arc::new(requester);
        let original_a: [u8; 64] = std::array::from_fn(|i| (i * 37 + 11) as u8);
        let original_b: [u8; 64] = std::array::from_fn(|i| (i * 19 + 53) as u8);
        let a = allocation(&original_a);
        let b = allocation(&original_b);
        let gpu_a = memory(&a);
        let gpu_b = memory(&b);
        let remote = source
            .register_cuda_memory_batch(&[(gpu_a.clone(), 0, 64)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(gpu_b.clone(), 0, 64)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        // READ holds PRE 1 after PRE 0 is ready. WRITE holds POST 0 after
        // its CROSS is finished. The other remote slot can still make progress.
        a.query_hold_address.store(
            remote.base_address() as usize + 1 + if operation == TransferOp::Read { 7 } else { 0 },
            Ordering::Release,
        );
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
                Duration::from_secs(8),
            )
            .unwrap();
        let crossed = if operation == TransferOp::Read { 1 } else { 2 };
        wait_until(|| source.stats().unwrap().staging.remote_crosses == crossed);
        wait_until(|| a.event_not_ready.load(Ordering::Acquire) != 0);
        if operation == TransferOp::Read {
            wait_until(|| b.event_query_successes.load(Ordering::Acquire) == 1);
        } else {
            wait_until(|| a.event_query_successes.load(Ordering::Acquire) == 1);
        }
        let stats = source.stats().unwrap();
        assert_eq!(stats.staging.remote_unpins, 0);
        assert_eq!(stats.staging.active_chunks, 2);
        assert_eq!(stats.active_source_operations, 1);
        assert_eq!(a.event_waits.load(Ordering::Acquire), 0);
        assert_eq!(stats.rdma.unwrap().active_grants, 0);
        assert_eq!(gpu_a.allocation.lock().unwrap().dma, vec![1..42]);
        assert_eq!(gpu_b.allocation.lock().unwrap().dma, vec![2..43]);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        let mut expected_a = original_a;
        let mut expected_b = original_b;
        if operation == TransferOp::Read {
            expected_b[2..9].copy_from_slice(&original_a[1..8]);
        } else {
            expected_a[8..15].copy_from_slice(&original_b[9..16]);
        }
        assert_eq!(unsafe { &**a.bytes.get() }, &expected_a);
        assert_eq!(unsafe { &**b.bytes.get() }, &expected_b);
        a.query_hold_address.store(0, Ordering::Release);
        let report = batch.wait(Duration::from_secs(8)).unwrap();
        assert!(
            report.state == BatchState::Completed && report.locally_drained,
            "{operation:?}: {report:?}"
        );
        batch.free().unwrap();
        if operation == TransferOp::Read {
            expected_b[2..43].copy_from_slice(&original_a[1..42]);
        } else {
            expected_a[1..42].copy_from_slice(&original_b[2..43]);
        }
        let mut bytes = [0; 64];
        gpu_a.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, expected_a);
        gpu_b.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, expected_b);
        assert_eq!(source.stats().unwrap().staging.remote_delegates, 6);
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 1);
        requester.close(Duration::from_secs(3)).unwrap();
        source.close(Duration::from_secs(3)).unwrap();
        assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
    }
}

fn frame(channel: &mut ControlChannel<'_>, nonce: SourceEpoch, region: u64, operation: Operation) {
    channel
        .write_all(
            &Request {
                operation,
                epoch: nonce,
                region,
                offset: 0,
                length: 3,
            }
            .encode(),
        )
        .unwrap();
}

fn receipt(
    channel: &mut ControlChannel<'_>,
    nonce: SourceEpoch,
    operation: Operation,
    region: u64,
) -> Option<bool> {
    let progress = operation == Operation::QueryDelegate;
    assert_eq!(
        Reply::read(channel, nonce).unwrap().length,
        52 + u64::from(progress)
    );
    let mut bytes = [0; 52];
    channel.read_exact(&mut bytes).unwrap();
    let reply = Request::decode(&bytes).unwrap();
    assert!(reply.operation == operation && reply.epoch == nonce);
    assert_eq!((reply.region, reply.offset, reply.length), (region, 0, 3));
    progress.then(|| {
        let mut ready = [0];
        channel.read_exact(&mut ready).unwrap();
        assert!(ready[0] <= 1);
        ready[0] == 1
    })
}

#[test]
fn delegate_query_requires_current_pending_generation_and_full_scope() {
    for fault in [
        "early",
        "nonce",
        "region",
        "offset",
        "length",
        "cross",
        "unpin",
        "duplicate",
        "next-generation",
    ] {
        let source = super::requester_staging::engine(Transport::Tcp, 3, 1);
        let a = allocation(&[0x57; 12]);
        let gpu = memory(&a);
        let remote = source
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 12)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let (mut channel, nonce) = super::remote_staging::pin(&source, &remote, false);
        a.query_hold.store(true, Ordering::Release);
        if fault != "early" {
            frame(&mut channel, nonce, remote.id(), Operation::Delegate);
            assert_eq!(
                receipt(&mut channel, nonce, Operation::Delegate, remote.id()),
                None
            );
            assert_eq!(a.event_queries.load(Ordering::Acquire), 0);
            for _ in 0..2 {
                frame(&mut channel, nonce, remote.id(), Operation::QueryDelegate);
                assert_eq!(
                    receipt(&mut channel, nonce, Operation::QueryDelegate, remote.id()),
                    Some(false)
                );
            }
            assert_eq!(source.stats().unwrap().staging.remote_delegates, 0);
        }
        let mut query = Request {
            operation: Operation::QueryDelegate,
            epoch: nonce,
            region: remote.id(),
            offset: 0,
            length: 3,
        };
        match fault {
            "nonce" => query.epoch = SourceEpoch::generate().unwrap(),
            "region" => query.region += 1,
            "offset" => query.offset = 3,
            "length" => query.length = 2,
            "cross" => query.operation = Operation::Cross,
            "unpin" => {
                query.operation = Operation::Unpin;
                query.offset = 8;
                query.length = 0;
            }
            "duplicate" | "next-generation" => {
                a.query_hold.store(false, Ordering::Release);
                frame(&mut channel, nonce, remote.id(), Operation::QueryDelegate);
                assert_eq!(
                    receipt(&mut channel, nonce, Operation::QueryDelegate, remote.id()),
                    Some(true)
                );
                assert_eq!(source.stats().unwrap().staging.remote_delegates, 1);
                if fault == "next-generation" {
                    query.offset = 3;
                }
            }
            _ => (),
        }
        let queries = a.event_queries.load(Ordering::Acquire);
        channel.write_all(&query.encode()).unwrap();
        a.query_hold.store(false, Ordering::Release);
        assert!(
            matches!(
                Reply::read(
                    &mut channel,
                    if fault == "cross" {
                        source.source_epoch()
                    } else {
                        nonce
                    }
                ),
                Err(Error::Protocol(_))
            ),
            "{fault}"
        );
        assert_eq!(
            a.event_queries.load(Ordering::Acquire),
            queries,
            "invalid query cannot touch CUDA: {fault}"
        );
        drop(channel);
        source.close(Duration::from_secs(3)).unwrap();
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        assert!(a.pins.lock().unwrap().is_empty());
        let mut bytes = [0; 12];
        gpu.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0x57; 12]);
    }
}

#[test]
#[cfg(feature = "rdma")]
fn remote_query_failure_closes_active_cross_and_retains_uncertain_cuda_owners() {
    use super::staging_cross::engine;
    for operation in [TransferOp::Read, TransferOp::Write] {
        for cleanup_failure in [false, true] {
            for pop_failure in [false, true] {
                let (source, _) = engine(7, 2, "copy", 4);
                let (requester, hold) = engine(
                    7,
                    3,
                    if cleanup_failure {
                        "copy_cross_hold_close0"
                    } else {
                        "copy_cross_hold"
                    },
                    4,
                );
                let requester = Arc::new(requester);
                let a = allocation(&[0x57; 64]);
                let b = allocation(&[0xcc; 64]);
                let gpu_a = memory(&a);
                let gpu_b = memory(&b);
                let weak_a = Arc::downgrade(&gpu_a);
                let weak_b = Arc::downgrade(&gpu_b);
                let remote = source
                    .register_cuda_memory_batch(
                        &[(gpu_a.clone(), 0, 64)],
                        Permission::RemoteReadWrite,
                    )
                    .unwrap()
                    .remove(0);
                let local = requester
                    .register_cuda_memory_batch(&[(gpu_b.clone(), 0, 64)], Permission::LocalOnly)
                    .unwrap()
                    .remove(0);
                let peer = requester
                    .open_segment(
                        source.endpoint(),
                        source.source_epoch(),
                        Duration::from_secs(3),
                    )
                    .unwrap();
                a.query_hold_address
                    .store(remote.base_address() as usize + 8, Ordering::Release);
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
                        Duration::from_secs(8),
                    )
                    .unwrap();
                wait_until(|| {
                    hold.0.lock().unwrap().0 && a.event_not_ready.load(Ordering::Acquire) != 0
                });
                assert!(requester.stats().unwrap().rdma.unwrap().work_requests > 0);
                if pop_failure {
                    a.pop_after_not_ready.store(true, Ordering::Release);
                } else {
                    a.event_query_fail_at.store(
                        a.event_query_results.load(Ordering::Acquire) + 1,
                        Ordering::Release,
                    );
                    a.query_hold_address.store(0, Ordering::Release);
                }
                let report = batch.wait(Duration::from_secs(8)).unwrap();
                assert_eq!(
                    report.state,
                    BatchState::Failed,
                    "{operation:?}/{cleanup_failure}/{pop_failure}: {report:?}"
                );
                assert_eq!(report.locally_drained, !cleanup_failure, "{report:?}");
                let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
                assert_eq!(
                    failure.remote_effect,
                    if operation == TransferOp::Write {
                        crate::RemoteEffect::Unknown
                    } else {
                        crate::RemoteEffect::None
                    }
                );
                if cleanup_failure {
                    assert!(
                        matches!(failure.error, Error::Unquiesced { .. }),
                        "{failure:?}"
                    );
                }
                assert_eq!(report.transferred_bytes, 0);
                assert!(gpu_a.allocation.lock().unwrap().uncertain);
                assert_eq!(gpu_a.allocation.lock().unwrap().dma, vec![1..42]);
                assert_eq!(source.stats().unwrap().staging.retained_chunks, 2);
                assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
                assert!(matches!(
                    source.close_source(Duration::from_millis(5)),
                    Err(Error::Timeout)
                ));
                assert!(b.streams.lock().unwrap().values().all(Vec::is_empty));
                assert!(gpu_b.allocation.lock().unwrap().dma.is_empty());
                if !cleanup_failure {
                    batch.free().unwrap();
                    requester.close(Duration::from_secs(3)).unwrap();
                    assert!(b.pins.lock().unwrap().is_empty());
                } else {
                    assert_eq!(requester.stats().unwrap().staging.retained_chunks, 3);
                    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
                }
                drop((batch, remote, local, gpu_a, gpu_b, source, requester));
                assert!(
                    weak_a.upgrade().is_some(),
                    "source admission retains actual uncertain GPU/pin owners"
                );
                assert_eq!(weak_b.upgrade().is_some(), cleanup_failure);
            }
        }
    }
}
