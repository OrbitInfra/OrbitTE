use super::*;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
use crate::runtime::proxy_manager::ProxyManager;
use crate::{RegisteredMemory, SourceEpoch};
use std::time::Instant;

fn config(depth: usize) -> EngineConfig {
    EngineConfig {
        remote_staging: true,
        staging_chunk_size: 3,
        staging_chunk_count: depth,
        staging_pipeline_depth: depth,
        handler_threads: 1,
        max_inflight_batches: 1,
        ..EngineConfig::default()
    }
}

pub(super) fn engine(transport: Transport, depth: usize) -> TransferEngine {
    #[cfg(feature = "rdma")]
    if transport == Transport::Rdma {
        let mut runtime = crate::transport::rdma::tests::runtime("direct:copy");
        runtime.proxy = ProxyManager::new(3, depth).unwrap();
        return crate::engine::tests::rdma_fixture_with_config(runtime, config(depth));
    }
    let _ = transport;
    TransferEngine::new(config(depth)).unwrap()
}

fn register(source: &TransferEngine, gpu: Arc<CudaMemory>) -> RegisteredMemory {
    source
        .register_cuda_memory_batch(&[(gpu, 0, 64)], Permission::RemoteReadWrite)
        .unwrap()
        .remove(0)
}

fn pending(device: &Device) -> usize {
    device
        .streams
        .lock()
        .unwrap()
        .values()
        .filter(|copy| copy.is_some())
        .count()
}

#[test]
fn remote_pipeline_negotiates_depth_overlaps_copies_and_reuses_slot_streams() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for (source_depth, requested, length) in [
            (2, 3, 14usize),
            (3, 2, 8),
            (3, 3, 23),
            (16, 16, 50),
            (2, 3, 1),
        ] {
            for local_gpu in [false, true] {
                let source = engine(transport, source_depth);
                let requester = Arc::new(engine(transport, requested));
                let source_bytes: [u8; 64] =
                    std::array::from_fn(|i| (i as u8).wrapping_mul(37).wrapping_add(0x57));
                let write_bytes: Vec<u8> = (0..length)
                    .map(|i| (i as u8).wrapping_mul(19).wrapping_add(0x29))
                    .collect();
                let a = allocation(&source_bytes);
                let b = allocation(&[0xcc; 64]);
                let gpu = memory(&a);
                let remote = register(&source, gpu.clone());
                let local = if local_gpu {
                    requester
                        .register_cuda_memory_batch(&[(memory(&b), 0, 64)], Permission::LocalOnly)
                        .unwrap()
                        .remove(0)
                } else {
                    requester
                        .register_memory(vec![0xcc; 64].into_boxed_slice(), Permission::LocalOnly)
                        .unwrap()
                };
                let peer = requester
                    .open_segment(
                        source.endpoint(),
                        source.source_epoch(),
                        Duration::from_secs(10),
                    )
                    .unwrap();
                let depth = source_depth.min(requested).min(length.div_ceil(3));
                let chunks = length.div_ceil(3);
                for (round, op) in [TransferOp::Read, TransferOp::Write]
                    .into_iter()
                    .enumerate()
                {
                    if op == TransferOp::Write {
                        local.write(1, &write_bytes).unwrap();
                    }
                    let (entered, waiting) = std::sync::mpsc::sync_channel(0);
                    let (resume, blocked) = std::sync::mpsc::sync_channel(0);
                    *a.event_gate.lock().unwrap() = Some((entered, blocked));
                    let mut batch = requester
                        .submit_async(
                            op,
                            &peer,
                            &[TransferRequest {
                                transport,
                                local_address: local.base_address() + 1,
                                remote_address: remote.base_address() + 2,
                                length,
                            }],
                            None,
                            Duration::from_secs(10),
                        )
                        .unwrap();
                    waiting.recv_timeout(Duration::from_secs(10)).unwrap();
                    // The shim keeps real source/destination pointers pending
                    // until the event wait. A serial implementation has only one.
                    assert_eq!(pending(&a), depth, "{transport:?}/{op:?}/{length}");
                    let stats = source.stats().unwrap();
                    assert_eq!(stats.active_source_operations, 1);
                    assert_eq!(stats.staging.active_chunks, depth);
                    assert_eq!(stats.staging.remote_delegates, (round * chunks) as u64);
                    assert_eq!(
                        stats.staging.remote_crosses,
                        (round * chunks + if op == TransferOp::Write { depth } else { 0 }) as u64
                    );
                    resume.send(()).unwrap();
                    let report = batch.wait(Duration::from_secs(10)).unwrap();
                    assert_eq!(
                        report.state,
                        BatchState::Completed,
                        "{transport:?}/{op:?}/gpu={local_gpu}/depth={depth}/length={length}: {report:?} {:?}",
                        batch.submissions().unwrap()
                    );
                    assert!(report.locally_drained);
                    batch.free().unwrap();
                    assert_eq!(pending(&a), 0);
                    let mut observed = [0; 64];
                    local.read(0, &mut observed).unwrap();
                    let mut expected = [0xcc; 64];
                    expected[1..1 + length].copy_from_slice(if op == TransferOp::Read {
                        &source_bytes[2..2 + length]
                    } else {
                        &write_bytes
                    });
                    assert_eq!(observed, expected);
                }
                let stats = source.stats().unwrap().staging;
                assert_eq!(
                    (stats.allocations, stats.reuses, stats.idle_chunks),
                    (depth as u64, depth as u64, depth)
                );
                assert_eq!(
                    (
                        stats.remote_delegates,
                        stats.remote_crosses,
                        stats.remote_unpins
                    ),
                    (2 * chunks as u64, 2 * chunks as u64, 2)
                );
                assert_eq!(a.streams.lock().unwrap().len(), depth);
                let mut observed = [0; 64];
                gpu.read(0, &mut observed).unwrap();
                let mut expected = source_bytes;
                expected[2..2 + length].copy_from_slice(&write_bytes);
                assert_eq!(observed, expected);
                requester.close(Duration::from_secs(10)).unwrap();
                source.close(Duration::from_secs(10)).unwrap();
                assert!(a.pins.lock().unwrap().is_empty());
                drop(remote);
                drop(gpu);
                drop(source);
                assert!(a.streams.lock().unwrap().is_empty());
                assert!(a.completions.lock().unwrap().is_empty());
            }
        }
    }
}

fn pin(
    source: &TransferEngine,
    memory: &RegisteredMemory,
    depth: u8,
) -> (ControlChannel<'static>, SourceEpoch) {
    let mut channel =
        ControlChannel::connect(source.endpoint(), Instant::now() + Duration::from_secs(3))
            .unwrap();
    channel
        .write_all(
            &Request {
                operation: Operation::PinRead,
                epoch: source.source_epoch(),
                region: memory.id(),
                offset: 0,
                length: 8,
            }
            .encode(),
        )
        .unwrap();
    channel.write_all(&[0, depth]).unwrap();
    Reply::read(&mut channel, source.source_epoch()).unwrap();
    let mut nonce = [0; 16];
    channel.read_exact(&mut nonce).unwrap();
    let mut negotiated = [0];
    channel.read_exact(&mut negotiated).unwrap();
    assert_eq!(negotiated, [depth]);
    (channel, SourceEpoch(nonce))
}

fn frame(
    channel: &mut ControlChannel<'_>,
    nonce: SourceEpoch,
    memory: &RegisteredMemory,
    operation: Operation,
    offset: u64,
) {
    channel
        .write_all(
            &Request {
                operation,
                epoch: nonce,
                region: memory.id(),
                offset,
                length: 3,
            }
            .encode(),
        )
        .unwrap();
}

fn receipt(channel: &mut ControlChannel<'_>, nonce: SourceEpoch) {
    assert_eq!(Reply::read(channel, nonce).unwrap().length, 52);
    let mut body = [0; 52];
    channel.read_exact(&mut body).unwrap();
    Request::decode(&body).unwrap();
}

#[test]
fn pipeline_cross_and_abort_drain_other_slots_without_reusing_the_wrong_generation() {
    for fault in ["eof", "scope", "slot", "healthy-cross"] {
        let source = engine(Transport::Tcp, 2);
        let a = allocation(&[0x57; 64]);
        let gpu = memory(&a);
        let remote = register(&source, gpu.clone());
        let (mut channel, nonce) = pin(&source, &remote, 2);
        for offset in [0, 3] {
            frame(&mut channel, nonce, &remote, Operation::Delegate, offset);
            receipt(&mut channel, nonce);
        }
        // All PRE operations have been accepted; the source may already be
        // waiting on the first event, so only the second is guaranteed pending.
        if fault != "eof" {
            frame(
                &mut channel,
                nonce,
                &remote,
                Operation::Cross,
                if fault == "scope" { 3 } else { 0 },
            );
            if fault != "scope" {
                channel
                    .write_all(
                        &Request {
                            operation: Operation::TcpRead,
                            epoch: source.source_epoch(),
                            region: crate::runtime::control_plane::u64_at(&nonce.0, 0)
                                .wrapping_add(u64::from(fault == "slot")),
                            offset: 0,
                            length: 3,
                        }
                        .encode(),
                    )
                    .unwrap();
            }
            if fault == "healthy-cross" {
                Reply::read(&mut channel, source.source_epoch()).unwrap();
                let mut bytes = [0; 3];
                channel.read_exact(&mut bytes).unwrap();
                assert_eq!(bytes, [0x57; 3]);
                receipt(&mut channel, nonce);
                assert_eq!(
                    pending(&a),
                    1,
                    "CROSS must proceed while the next PRE remains pending"
                );
                assert_eq!(source.stats().unwrap().staging.remote_delegates, 1);
            } else {
                assert!(matches!(
                    Reply::read(&mut channel, source.source_epoch()),
                    Err(Error::Protocol(_))
                ));
            }
        }
        drop(channel);
        source.close_source(Duration::from_secs(3)).unwrap();
        assert_eq!(pending(&a), 0);
        assert_eq!(a.event_waits.load(Ordering::Acquire), 2);
        assert_eq!(source.stats().unwrap().staging.idle_chunks, 2);
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        source.close(Duration::from_secs(3)).unwrap();
        assert!(a.pins.lock().unwrap().is_empty());
    }
}

#[test]
fn pipeline_failed_wait_is_sticky_but_every_other_slot_is_drained() {
    for op in [TransferOp::Read, TransferOp::Write] {
        let source = engine(Transport::Tcp, 2);
        let requester = engine(Transport::Tcp, 2);
        let a = allocation(&[0x57; 64]);
        let gpu = memory(&a);
        let remote = register(&source, gpu.clone());
        let local = requester
            .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        a.event_wait_fail_at.store(1, Ordering::Release);
        let report = requester
            .submit_and_wait(
                op,
                &peer,
                &[TransferRequest {
                    transport: Transport::Tcp,
                    local_address: local.base_address(),
                    remote_address: remote.base_address(),
                    length: 8,
                }],
                Duration::from_secs(3),
            )
            .unwrap();
        assert!(
            !report.is_complete() && report.locally_drained,
            "{report:?}"
        );
        // PRE may fail before CROSS is read; socket closure can win the race
        // with the error receipt. Both outcomes must retain the same DMA owners.
        assert!(
            matches!(
                report.failure.as_ref().unwrap().error,
                Error::Backend { .. } | Error::Io(_)
            ),
            "{report:?}"
        );
        assert_eq!(pending(&a), 1);
        assert_eq!(a.event_waits.load(Ordering::Acquire), 2);
        assert!(gpu.allocation.lock().unwrap().uncertain);
        assert!(matches!(
            gpu.read(0, &mut [0; 1]),
            Err(Error::Unquiesced { .. })
        ));
        let stats = source.stats().unwrap();
        assert_eq!(
            (
                stats.staging.retained_chunks,
                stats.active_source_operations
            ),
            (2, 1)
        );
        assert_eq!(stats.staging.remote_delegates, 1);
        assert_eq!(
            report.failure.as_ref().unwrap().remote_effect,
            if op == TransferOp::Write {
                crate::RemoteEffect::Unknown
            } else {
                crate::RemoteEffect::None
            }
        );
        assert!(matches!(
            source.close_source(Duration::from_millis(10)),
            Err(Error::Timeout)
        ));
        requester.close(Duration::from_secs(3)).unwrap();
    }
}

#[test]
fn pipeline_stream_construction_and_destroy_failures_keep_physical_slot_budget() {
    for fault in [
        "partial-stream",
        "partial-event",
        "event-destroy",
        "stream-destroy",
    ] {
        let a = allocation(&[0x57; 64]);
        let gpu = memory(&a);
        let manager = ProxyManager::new(3, 2).unwrap();
        if fault == "partial-stream" {
            a.stream_create_error.store(1, Ordering::Release);
            a.stream_partial_create.store(true, Ordering::Release);
        }
        if fault == "partial-event" {
            a.event_create_error.store(1, Ordering::Release);
        }
        if fault == "event-destroy" {
            a.event_destroy_error.store(1, Ordering::Release);
        } else {
            a.stream_destroy_error.store(1, Ordering::Release);
        }
        let result = gpu.with_staging(0, 3, &manager, 2, |copy, mut stages| {
            copy.begin(&mut stages[0], 0, 3, false)?;
            copy.complete(&mut stages[0], 3)
        });
        if fault.starts_with("partial") {
            assert!(
                matches!(result, Err(Error::Unquiesced { .. })),
                "{fault}: {result:?}"
            );
            assert_eq!(manager.stats().unwrap().retained_chunks, 1);
        } else {
            result.unwrap();
        }
        assert!(matches!(
            manager.close(Instant::now() + Duration::from_secs(3)),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(
            manager.stats().unwrap().retained_chunks,
            if fault.starts_with("partial") { 1 } else { 2 }
        );
        let destroys = a
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "event-destroy" || **e == "stream-destroy")
            .count();
        assert!(matches!(
            manager.close(Instant::now() + Duration::from_secs(3)),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(
            a.events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "event-destroy" || **e == "stream-destroy")
                .count(),
            destroys
        );
        assert!(!a.pins.lock().unwrap().is_empty());
    }
}

#[test]
fn pipeline_slot_restore_failure_fences_a_new_gpu_context_wrapper() {
    let a = allocation(&[0x57; 64]);
    let first = memory(&a);
    let second = memory(&a);
    let manager = ProxyManager::new(3, 2).unwrap();
    first
        .with_staging(0, 3, &manager, 2, |copy, mut stages| {
            copy.begin(&mut stages[0], 0, 3, false)?;
            copy.complete(&mut stages[0], 3)
        })
        .unwrap();
    a.pop_after_completion.store(true, Ordering::Release);
    let result = std::thread::spawn(move || {
        let result = second.with_staging(0, 3, &manager, 2, |copy, mut stages| {
            copy.begin(&mut stages[0], 0, 3, false)?;
            copy.complete(&mut stages[0], 3)
        });
        assert!(second.allocation.lock().unwrap().uncertain);
        assert_eq!(manager.stats().unwrap().retained_chunks, 2);
        result
    })
    .join()
    .unwrap();
    assert!(matches!(result, Err(Error::Unquiesced { .. })));
}

#[test]
fn pipeline_depth_and_partial_pool_admission_remain_bounded() {
    for depth in [0, 3, 17] {
        let mut invalid = config(2);
        invalid.staging_pipeline_depth = depth;
        assert!(matches!(
            TransferEngine::new(invalid),
            Err(Error::InvalidConfig(_))
        ));
    }
    let a = allocation(&[0x57; 64]);
    let gpu = memory(&a);
    let context = gpu.allocation.lock().unwrap().context.clone();
    let manager = ProxyManager::new(3, 2).unwrap();
    let held = manager
        .pin_stage_buffer(context.raw.as_ptr() as usize, |size| {
            PinnedAllocation::new(&context, size)
        })
        .unwrap();
    assert!(matches!(
        gpu.with_staging::<()>(0, 8, &manager, 2, |_, _| panic!("partial Pin accepted")),
        Err(Error::ResourceExhausted)
    ));
    let stats = manager.stats().unwrap();
    assert_eq!(
        (
            stats.reserved_chunks,
            stats.active_chunks,
            stats.idle_chunks
        ),
        (2, 1, 1)
    );
    held.release().unwrap();
    gpu.with_staging(0, 8, &manager, 2, |_, stages| {
        assert_eq!(stages.len(), 2);
        Ok(())
    })
    .unwrap();
    assert_eq!(manager.stats().unwrap().idle_chunks, 2);
    manager
        .close(Instant::now() + Duration::from_secs(3))
        .unwrap();
    assert!(a.pins.lock().unwrap().is_empty());
}

#[test]
fn pipeline_recovered_submit_failure_drains_accepted_pre_and_releases_admission() {
    let source = engine(Transport::Tcp, 2);
    let a = allocation(&[0x57; 64]);
    let gpu = memory(&a);
    let remote = register(&source, gpu.clone());
    let (mut channel, nonce) = pin(&source, &remote, 2);
    frame(&mut channel, nonce, &remote, Operation::Delegate, 0);
    receipt(&mut channel, nonce);
    assert_eq!(pending(&a), 1);
    a.copy_error.store(1, Ordering::Release);
    frame(&mut channel, nonce, &remote, Operation::Delegate, 3);
    assert!(matches!(
        Reply::read(&mut channel, nonce),
        Err(Error::Backend { .. })
    ));
    assert_eq!(pending(&a), 0);
    assert!(!gpu.allocation.lock().unwrap().uncertain);
    assert_eq!(source.stats().unwrap().staging.idle_chunks, 2);
    a.copy_error.store(0, Ordering::Release);
    drop(channel);
    let (next, _) = pin(&source, &remote, 2);
    drop(next);
    source.close(Duration::from_secs(3)).unwrap();
    assert!(a.pins.lock().unwrap().is_empty());
}

#[cfg(feature = "rdma")]
#[test]
fn nested_rdma_exhaustion_preserves_error_scope_and_prior_write_effect() {
    for op in [TransferOp::Read, TransferOp::Write] {
        let mut runtime = crate::transport::rdma::tests::runtime("direct:copy");
        runtime.proxy = ProxyManager::new(3, 2).unwrap();
        for _ in 0..254 {
            runtime.staging_endpoint(3, 1).unwrap().close().unwrap();
        }
        let source = crate::engine::tests::rdma_fixture_with_config(runtime, config(2));
        let requester = engine(Transport::Rdma, 2);
        let a = allocation(&[0x57; 64]);
        let remote = register(&source, memory(&a));
        let local = requester
            .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        let report = requester
            .submit_and_wait(
                op,
                &peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: local.base_address(),
                    remote_address: remote.base_address(),
                    length: 8,
                }],
                Duration::from_secs(3),
            )
            .unwrap();
        assert!(
            !report.is_complete() && report.locally_drained,
            "{report:?}"
        );
        let failure = report.failure.unwrap();
        assert!(
            matches!(failure.error, Error::ResourceExhausted),
            "{failure:?}"
        );
        assert_eq!(
            failure.remote_effect,
            if op == TransferOp::Write {
                crate::RemoteEffect::Unknown
            } else {
                crate::RemoteEffect::None
            }
        );
        source.close_source(Duration::from_secs(3)).unwrap();
        assert_eq!(pending(&a), 0);
        assert_eq!(source.stats().unwrap().staging.retained_chunks, 0);
        assert_eq!(
            source.stats().unwrap().rdma.unwrap().window_key_exhaustions,
            1
        );
        let mut bytes = [0; 8];
        local.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0xcc; 8]);
        requester.close(Duration::from_secs(3)).unwrap();
        source.close(Duration::from_secs(3)).unwrap();
        assert_eq!(source.stats().unwrap().rdma.unwrap().active_windows, 0);
        assert!(a.pins.lock().unwrap().is_empty());
    }
}
