use super::*;
#[cfg(feature = "rdma")]
use crate::runtime::proxy_manager::ProxyManager;

fn config() -> EngineConfig {
    EngineConfig {
        remote_staging: true,
        staging_chunk_size: 3,
        staging_chunk_count: 1,
        handler_threads: 1,
        max_inflight_batches: 1,
        ..EngineConfig::default()
    }
}

fn engine(transport: Transport) -> TransferEngine {
    #[cfg(feature = "rdma")]
    if transport == Transport::Rdma {
        let mut runtime = crate::transport::rdma::tests::runtime("direct:copy");
        runtime.proxy = ProxyManager::new(3, 1, crate::StagingLocation::Any).unwrap();
        return crate::engine::tests::rdma_fixture_with_config(runtime, config());
    }
    let _ = transport;
    TransferEngine::new(config()).unwrap()
}

#[test]
fn remote_chunks_cross_real_transports_with_one_handler_and_one_parent_slot() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for local_gpu in [false, true] {
            let source = engine(transport);
            let requester = engine(transport);
            let a = allocation(&[0x57; 16]);
            let b = allocation(&[0xcc; 16]);
            let source_gpu = memory(&a);
            let requester_gpu = memory(&b);
            let remote = source
                .register_cuda_memory_batch(
                    &[(source_gpu.clone(), 2, 12)],
                    Permission::RemoteReadWrite,
                )
                .unwrap()
                .remove(0);
            let local = if local_gpu {
                requester
                    .register_cuda_memory_batch(
                        &[(requester_gpu.clone(), 1, 14)],
                        Permission::LocalOnly,
                    )
                    .unwrap()
                    .remove(0)
            } else {
                requester
                    .register_memory(vec![0xcc; 14].into_boxed_slice(), Permission::LocalOnly)
                    .unwrap()
            };
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(2),
                )
                .unwrap();
            let request = TransferRequest {
                transport,
                local_address: local.base_address() + 2,
                remote_address: remote.base_address() + 1,
                length: 8,
            };
            let pattern = [3, 19, 21, 29, 37, 43, 51, 61];
            for op in [TransferOp::Read, TransferOp::Write] {
                if op == TransferOp::Write {
                    local.write(2, &pattern).unwrap();
                }
                let report = requester
                    .submit_and_wait(
                        op,
                        &peer,
                        std::slice::from_ref(&request),
                        Duration::from_secs(2),
                    )
                    .unwrap();
                assert!(
                    report.is_complete() && report.locally_drained,
                    "{transport:?}/{local_gpu}/{op:?}: {report:?}"
                );
                assert_eq!(report.transferred_bytes, 8);
                let mut bytes = [0; 14];
                local.read(0, &mut bytes).unwrap();
                let mut expected = [0xcc; 14];
                expected[2..10].copy_from_slice(if op == TransferOp::Read {
                    &[0x57; 8]
                } else {
                    &pattern
                });
                assert_eq!(bytes, expected);
            }
            let mut bytes = [0; 16];
            source_gpu.read(0, &mut bytes).unwrap();
            let mut expected = [0x57; 16];
            expected[3..11].copy_from_slice(&pattern);
            assert_eq!(bytes, expected);
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(2),
                )
                .unwrap();
            assert_eq!(peer.regions().len(), 1);
            assert_eq!(peer.regions()[0].id(), remote.id());
            let stats = source.stats().unwrap().staging;
            assert_eq!(
                (
                    stats.allocations,
                    stats.reuses,
                    stats.completed_chunks,
                    stats.copied_bytes
                ),
                (1, 1, 6, 16)
            );
            assert_eq!(
                (
                    stats.idle_chunks,
                    stats.active_chunks,
                    stats.retained_chunks
                ),
                (1, 0, 0)
            );
            for engine in [&requester, &source] {
                engine.close(Duration::from_secs(2)).unwrap();
            }
            assert!(a.pins.lock().unwrap().is_empty() && b.pins.lock().unwrap().is_empty());
        }
    }
}

use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
use crate::{RemoteEffect, SourceEpoch};
use std::time::Instant;

pub(super) fn pin(
    source: &TransferEngine,
    memory: &crate::RegisteredMemory,
    write: bool,
) -> (ControlChannel<'static>, SourceEpoch) {
    let mut channel =
        ControlChannel::connect(source.endpoint(), Instant::now() + Duration::from_secs(2))
            .unwrap();
    channel
        .write_all(
            &Request {
                operation: if write {
                    Operation::PinWrite
                } else {
                    Operation::PinRead
                },
                epoch: source.source_epoch(),
                region: memory.id(),
                offset: 1,
                length: 8,
            }
            .encode(),
        )
        .unwrap();
    channel.write_all(&[0, 1]).unwrap();
    channel.write_all(&3u64.to_be_bytes()).unwrap();
    channel.write_all(&[1]).unwrap();
    let reply = Reply::read(&mut channel, source.source_epoch()).unwrap();
    assert_eq!((reply.status, reply.length), (0, 3));
    let mut nonce = [0; 16];
    channel.read_exact(&mut nonce).unwrap();
    let mut depth = [0; 2];
    channel.read_exact(&mut depth).unwrap();
    assert_eq!(depth, [1, 1]);
    (channel, SourceEpoch(nonce))
}

#[test]
fn pin_scope_order_and_abandoned_host_write_cannot_modify_gpu() {
    for fault in [
        "nonce",
        "registration",
        "offset",
        "length",
        "order",
        "cross",
        "abandon",
        "early-finish",
        "early-unpin",
        "repeat-finish",
        "stale-finish-nonce",
    ] {
        let source = engine(Transport::Tcp);
        let a = allocation(&[0x57; 12]);
        let gpu = memory(&a);
        let memory = source
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 12)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let (mut channel, nonce) = pin(&source, &memory, true);
        let mut frame = Request {
            operation: Operation::Cross,
            epoch: nonce,
            region: memory.id(),
            offset: 0,
            length: 3,
        };
        match fault {
            "nonce" => frame.epoch = SourceEpoch::generate().unwrap(),
            "registration" => frame.region += 1,
            "offset" => frame.offset = 1,
            "length" => frame.length = 4,
            "order" => frame.operation = Operation::Delegate,
            "early-finish" => frame.operation = Operation::FinishCross,
            "early-unpin" => {
                frame.operation = Operation::Unpin;
                frame.offset = 8;
                frame.length = 0;
            }
            _ => (),
        }
        channel.write_all(&frame.encode()).unwrap();
        if matches!(
            fault,
            "cross" | "abandon" | "repeat-finish" | "stale-finish-nonce"
        ) {
            let id = crate::runtime::control_plane::u64_at(&nonce.0, 0);
            channel
                .write_all(
                    &Request {
                        operation: Operation::TcpWrite,
                        epoch: source.source_epoch(),
                        region: if fault == "cross" {
                            id.wrapping_add(1)
                        } else {
                            id
                        },
                        offset: 0,
                        length: 3,
                    }
                    .encode(),
                )
                .unwrap();
        }
        if matches!(fault, "abandon" | "repeat-finish" | "stale-finish-nonce") {
            assert_eq!(
                Reply::read(&mut channel, source.source_epoch())
                    .unwrap()
                    .status,
                9
            );
            channel.write_all(&[1, 2, 3]).unwrap();
            assert_eq!(
                Reply::read(&mut channel, source.source_epoch())
                    .unwrap()
                    .status,
                0
            );
            assert_eq!(Reply::read(&mut channel, nonce).unwrap().status, 0);
            let mut receipt = [0; 52];
            channel.read_exact(&mut receipt).unwrap();
            assert!(Request::decode(&receipt).unwrap().operation == Operation::Cross);
            if fault != "abandon" {
                frame.operation = Operation::FinishCross;
                if fault == "stale-finish-nonce" {
                    frame.epoch = SourceEpoch::generate().unwrap();
                }
                channel.write_all(&frame.encode()).unwrap();
                if fault == "repeat-finish" {
                    Reply::read(&mut channel, nonce).unwrap();
                    channel.read_exact(&mut receipt).unwrap();
                    assert!(Request::decode(&receipt).unwrap().operation == Operation::FinishCross);
                    channel.write_all(&frame.encode()).unwrap();
                }
                assert!(matches!(
                    Reply::read(&mut channel, source.source_epoch()),
                    Err(Error::Protocol(_))
                ));
            }
            // Neither CROSS receipt authorizes a delegated GPU write.
        } else {
            assert!(
                matches!(
                    Reply::read(
                        &mut channel,
                        if matches!(fault, "order" | "early-unpin") {
                            nonce
                        } else {
                            source.source_epoch()
                        }
                    ),
                    Err(Error::Protocol(_))
                ),
                "{fault}"
            );
        }
        drop(channel);
        source.close_source(Duration::from_secs(2)).unwrap();
        let mut bytes = [0; 12];
        gpu.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0x57; 12], "{fault}");
        assert_eq!(source.stats().unwrap().staging.idle_chunks, 1);
        source.close(Duration::from_secs(2)).unwrap();
        assert!(a.pins.lock().unwrap().is_empty());
    }
}

#[test]
fn remote_cuda_failures_keep_confirmed_drain_separate_from_success() {
    for write in [false, true] {
        for unquiesced in [false, true] {
            let source = engine(Transport::Tcp);
            let requester = engine(Transport::Tcp);
            let a = allocation(&[0x57; 12]);
            let weak = Arc::downgrade(&a);
            let gpu = memory(&a);
            let remote = source
                .register_cuda_memory_batch(&[(gpu.clone(), 0, 12)], Permission::RemoteReadWrite)
                .unwrap()
                .remove(0);
            let local = requester
                .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            if unquiesced {
                a.sync_error.store(700, Ordering::Release);
            } else {
                a.copy_error.store(1, Ordering::Release);
            }
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(2),
                )
                .unwrap();
            let report = requester
                .submit_and_wait(
                    if write {
                        TransferOp::Write
                    } else {
                        TransferOp::Read
                    },
                    &peer,
                    &[TransferRequest {
                        transport: Transport::Tcp,
                        local_address: local.base_address(),
                        remote_address: remote.base_address() + 1,
                        length: 8,
                    }],
                    Duration::from_secs(2),
                )
                .unwrap();
            assert!(
                !report.is_complete() && report.locally_drained,
                "{write}/{unquiesced}: {report:?}"
            );
            let failure = report.failure.unwrap();
            assert!(
                matches!(failure.error, Error::Backend { .. }),
                "{failure:?}"
            );
            assert_eq!(
                failure.remote_effect,
                if write {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                }
            );
            let mut bytes = [0; 8];
            local.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [0xcc; 8]);
            requester.close(Duration::from_secs(2)).unwrap();
            if unquiesced {
                assert!(matches!(
                    source.close_source(Duration::from_millis(10)),
                    Err(Error::Timeout)
                ));
                let stats = source.stats().unwrap();
                assert_eq!(
                    (
                        stats.active_source_operations,
                        stats.staging.retained_chunks
                    ),
                    (1, 1)
                );
                source.unregister_memory(&remote).unwrap();
                drop(remote);
                drop(gpu);
                drop(a);
                assert!(weak.upgrade().is_some());
            } else {
                source.close(Duration::from_secs(2)).unwrap();
                assert!(a.pins.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn self_peer_gpu_ranges_require_two_slots_and_reject_overlap_before_copy() {
    for capacity in [1, 2] {
        let engine = TransferEngine::new(EngineConfig {
            staging_chunk_count: capacity,
            ..config()
        })
        .unwrap();
        let original: [u8; 24] = std::array::from_fn(|i| i as u8 + 1);
        let a = allocation(&original);
        let gpu = memory(&a);
        let memory = engine
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 24)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let peer = engine
            .open_segment(
                engine.endpoint(),
                engine.source_epoch(),
                Duration::from_secs(2),
            )
            .unwrap();
        for operation in [TransferOp::Read, TransferOp::Write] {
            for overlap in [true, false] {
                memory.write(0, &original).unwrap();
                let report = engine
                    .submit_and_wait(
                        operation,
                        &peer,
                        &[TransferRequest {
                            transport: Transport::Tcp,
                            local_address: memory.base_address() + if overlap { 4 } else { 12 },
                            remote_address: memory.base_address(),
                            length: 8,
                        }],
                        Duration::from_secs(2),
                    )
                    .unwrap();
                let mut expected = original;
                if overlap || capacity == 1 {
                    assert!(
                        report.locally_drained && !report.is_complete(),
                        "{report:?}"
                    );
                    let failure = report.failure.unwrap();
                    assert!(
                        matches!(failure.error, Error::Unavailable(_)) && overlap
                            || matches!(failure.error, Error::ResourceExhausted) && !overlap,
                        "{failure:?}"
                    );
                    assert_eq!(failure.remote_effect, RemoteEffect::None);
                } else {
                    assert!(report.is_complete() && report.locally_drained, "{report:?}");
                    if operation == TransferOp::Read {
                        expected[12..20].copy_from_slice(&original[..8]);
                    } else {
                        expected[..8].copy_from_slice(&original[12..20]);
                    }
                }
                let mut bytes = [0; 24];
                memory.read(0, &mut bytes).unwrap();
                assert_eq!(bytes, expected);
                assert!(gpu.allocation.lock().unwrap().dma.is_empty());
                assert_eq!(engine.stats().unwrap().staging.active_chunks, 0);
            }
        }
        let guard = gpu.allocation.lock().unwrap();
        let mut channel =
            ControlChannel::connect(engine.endpoint(), Instant::now() + Duration::from_secs(2))
                .unwrap();
        channel
            .write_all(
                &Request {
                    operation: Operation::PinRead,
                    epoch: engine.source_epoch(),
                    region: memory.id(),
                    offset: 0,
                    length: 8,
                }
                .encode(),
            )
            .unwrap();
        channel.write_all(&[0, 1]).unwrap();
        channel.write_all(&3u64.to_be_bytes()).unwrap();
        channel.write_all(&[1]).unwrap();
        assert!(matches!(
            Reply::read(&mut channel, engine.source_epoch()),
            Err(Error::ResourceExhausted)
        ));
        drop(guard);
        engine.close(Duration::from_secs(2)).unwrap();
        assert!(a.pins.lock().unwrap().is_empty());
    }
}

#[cfg(feature = "rdma")]
#[test]
fn private_mr_cleanup_failure_retains_actual_stage_and_responsible_admission() {
    let _serial = super::staging_location::SERIAL.lock().unwrap();
    super::staging_location::reset();
    for mapped in [false, true] {
        for depth in [1, 3] {
            for source_fault in [false, true] {
                for operation in [TransferOp::Read, TransferOp::Write] {
                    let make = |fault| {
                        let mut runtime = crate::transport::rdma::tests::runtime(if fault {
                            "direct:copy_stage_close"
                        } else {
                            "direct:copy"
                        });
                        runtime.proxy = if mapped {
                            super::staging_location::pool(3, crate::StagingLocation::Auto, depth)
                        } else {
                            crate::runtime::proxy_manager::ProxyManager::new(
                                3,
                                depth,
                                crate::StagingLocation::Any,
                            )
                            .unwrap()
                        };
                        crate::engine::tests::rdma_fixture_with_config(
                            runtime,
                            EngineConfig {
                                remote_staging: true,
                                staging_chunk_size: 3,
                                staging_chunk_count: depth,
                                staging_pipeline_depth: depth,
                                handler_threads: 1,
                                max_inflight_batches: 1,
                                ..EngineConfig::default()
                            },
                        )
                    };
                    let source = make(source_fault);
                    let requester = make(!source_fault);
                    let a = allocation(&[0x57; 64]);
                    let b = allocation(&[0xcc; 64]);
                    let source_gpu = memory(&a);
                    let local_gpu = memory(&b);
                    let weak_bad = Arc::downgrade(if source_fault {
                        &source_gpu
                    } else {
                        &local_gpu
                    });
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
                    let peer = requester
                        .open_segment(
                            source.endpoint(),
                            source.source_epoch(),
                            Duration::from_secs(5),
                        )
                        .unwrap();
                    let report = requester
                        .submit_and_wait(
                            operation,
                            &peer,
                            &[TransferRequest {
                                transport: Transport::Rdma,
                                local_address: local.base_address() + 2,
                                remote_address: remote.base_address() + 1,
                                length: 8,
                            }],
                            Duration::from_secs(5),
                        )
                        .unwrap();
                    assert!(
                        !report.is_complete(),
                        "{source_fault}/{operation:?}: {report:?}"
                    );
                    assert_eq!(report.locally_drained, source_fault);
                    assert_eq!(
                        (report.completed_requests, report.transferred_bytes),
                        (0, 0)
                    );
                    assert_eq!(
                        report.failure.unwrap().remote_effect,
                        if operation == TransferOp::Write {
                            crate::RemoteEffect::Unknown
                        } else {
                            crate::RemoteEffect::None
                        }
                    );
                    assert!(a.streams.lock().unwrap().values().all(Option::is_none));
                    assert!(b.streams.lock().unwrap().values().all(Option::is_none));
                    assert!(source_gpu.allocation.lock().unwrap().dma.is_empty());
                    assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
                    let bad = if source_fault { &source } else { &requester };
                    let stats = bad.stats().unwrap();
                    assert_eq!(stats.rdma.unwrap().retained_persistent_mrs, 1);
                    assert_eq!(stats.staging.retained_chunks, depth);
                    if mapped {
                        let bad_owner = if source_fault { &a } else { &b };
                        assert_eq!(bad_owner.host_registrations.lock().unwrap().len(), depth);
                        assert!(
                            !bad_owner
                                .events
                                .lock()
                                .unwrap()
                                .contains(&"host-unregister")
                        );
                        for (&address, &length) in
                            bad_owner.host_registrations.lock().unwrap().iter()
                        {
                            let mut resident =
                                vec![0u8; length / crate::platform::numa::page_round(1).unwrap()];
                            assert_eq!(
                                unsafe {
                                    libc::mincore(address as *mut _, length, resident.as_mut_ptr())
                                },
                                0
                            );
                        }
                        assert_eq!(
                            stats.staging.reserved_bytes,
                            depth * crate::platform::numa::page_round(3).unwrap()
                        );
                    }
                    if source_fault {
                        assert!(matches!(
                            source.close_source(Duration::from_millis(10)),
                            Err(Error::Timeout)
                        ));
                        requester.close(Duration::from_secs(5)).unwrap();
                        assert_eq!(source.stats().unwrap().active_source_operations, 1);
                        assert!(b.pins.lock().unwrap().is_empty());
                    } else {
                        assert!(matches!(
                            requester.close_requester(Duration::from_millis(10)),
                            Err(Error::Timeout)
                        ));
                        source.close(Duration::from_secs(5)).unwrap();
                        assert_eq!(requester.stats().unwrap().inflight_batches, 1);
                        assert!(a.pins.lock().unwrap().is_empty());
                    }
                    assert_eq!(
                        source.stats().unwrap().staging.remote_unpins,
                        u64::from(!source_fault)
                    );
                    let mut observed = [0; 64];
                    let mut expected = [0xcc; 64];
                    if operation == TransferOp::Read {
                        expected[2..10].fill(0x57);
                    }
                    local_gpu.read(0, &mut observed).unwrap();
                    assert_eq!(observed, expected);
                    let mut expected = [0x57; 64];
                    if operation == TransferOp::Write {
                        expected[1..9].fill(0xcc);
                    }
                    source_gpu.read(0, &mut observed).unwrap();
                    assert_eq!(observed, expected);
                    drop(remote);
                    drop(local);
                    drop(source_gpu);
                    drop(local_gpu);
                    drop(source);
                    drop(requester);
                    assert!(
                        weak_bad.upgrade().is_some(),
                        "native MR failure retains the actual GPU backing through its admission owner"
                    );
                }
            }
        }
    }
}

#[test]
fn lost_remote_receipts_keep_actual_effects_but_cannot_report_success() {
    use std::io::{Read, Write, copy};
    use std::net::{Shutdown, TcpListener, TcpStream};
    for lost_query in [false, true] {
        for (operation, depth, device_requester) in [
            (TransferOp::Read, 1, false),
            (TransferOp::Write, 1, false),
            (TransferOp::Read, 2, false),
            (TransferOp::Write, 2, false),
            (TransferOp::Read, 3, true),
            (TransferOp::Write, 3, true),
        ] {
            let source = super::staging_pipeline::engine(Transport::Tcp, depth);
            let requester = super::staging_pipeline::engine(Transport::Tcp, depth);
            let a = allocation(&[0x57; 12]);
            let gpu = memory(&a);
            let remote = source
                .register_cuda_memory_batch(&[(gpu.clone(), 0, 12)], Permission::RemoteReadWrite)
                .unwrap()
                .remove(0);
            let b = allocation(&[0xcc; 10]);
            let local = if device_requester {
                requester
                    .register_cuda_memory_batch(&[(memory(&b), 0, 10)], Permission::LocalOnly)
                    .unwrap()
                    .remove(0)
            } else {
                requester
                    .register_memory(vec![0xcc; 10].into_boxed_slice(), Permission::LocalOnly)
                    .unwrap()
            };
            let mut peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(2),
                )
                .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            peer.endpoint = listener.local_addr().unwrap();
            let target = source.endpoint();
            let relay = std::thread::spawn(move || {
                let (mut client, _) = listener.accept().unwrap();
                let mut server = TcpStream::connect(target).unwrap();
                server
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut input = client.try_clone().unwrap();
                let mut output = server.try_clone().unwrap();
                let upstream = std::thread::spawn(move || {
                    let _ = copy(&mut input, &mut output);
                });
                // Pin has a fixed nonce/depth extension. Subsequent replies carry
                // their own payload length, including any number of pending queries.
                let mut pin = [0; 54];
                server.read_exact(&mut pin).unwrap();
                client.write_all(&pin).unwrap();
                loop {
                    let mut header = [0; 36];
                    server.read_exact(&mut header).unwrap();
                    let length = crate::runtime::control_plane::u64_at(&header, 28) as usize;
                    assert!(length <= 53);
                    let mut payload = vec![0; length];
                    server.read_exact(&mut payload).unwrap();
                    if length == 52 || length == 53 {
                        let frame = Request::decode(payload[..52].try_into().unwrap()).unwrap();
                        if lost_query
                            && frame.operation == Operation::QueryDelegate
                            && frame.offset == 0
                            && payload[52] == 1
                        {
                            break;
                        }
                        if frame.operation == Operation::Unpin {
                            assert_eq!(header[10], 0);
                            assert_eq!((frame.offset, frame.length), (8, 0));
                            break;
                        }
                    }
                    client.write_all(&header).unwrap();
                    client.write_all(&payload).unwrap();
                }
                client.shutdown(Shutdown::Both).unwrap();
                let _ = server.shutdown(Shutdown::Both);
                upstream.join().unwrap();
            });
            let report = requester
                .submit_and_wait(
                    operation,
                    &peer,
                    &[TransferRequest {
                        transport: Transport::Tcp,
                        local_address: local.base_address() + 1,
                        remote_address: remote.base_address() + 1,
                        length: 8,
                    }],
                    Duration::from_secs(2),
                )
                .unwrap();
            relay.join().unwrap();
            assert!(
                !report.is_complete() && report.locally_drained,
                "{report:?}"
            );
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (0, 0)
            );
            assert_eq!(
                report.failure.unwrap().remote_effect,
                if operation == TransferOp::Write {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                }
            );
            if device_requester {
                let local_copies = if lost_query && operation == TransferOp::Read {
                    0
                } else {
                    3
                };
                assert_eq!(
                    b.event_query_successes.load(Ordering::Acquire),
                    local_copies
                );
                assert_eq!(b.event_waits.load(Ordering::Acquire), 0);
                assert!(b.streams.lock().unwrap().values().all(Option::is_none));
                assert_eq!(
                    requester.stats().unwrap().staging.completed_chunks,
                    local_copies as u64
                );
            }
            let mut observed = [0; 10];
            local.read(0, &mut observed).unwrap();
            let mut expected = [0xcc; 10];
            if operation == TransferOp::Read && !lost_query {
                expected[1..9].fill(0x57);
            }
            assert_eq!(observed, expected);
            let mut bytes = [0; 8];
            source.close(Duration::from_secs(2)).unwrap();
            let stats = source.stats().unwrap().staging;
            assert_eq!(
                (
                    stats.remote_pins,
                    stats.remote_delegates,
                    stats.remote_crosses,
                    stats.remote_unpins
                ),
                if lost_query {
                    if operation == TransferOp::Read {
                        (1, depth as u64, 0, 0)
                    } else {
                        (1, 1, 1, 0)
                    }
                } else {
                    (1, 3, 3, 1)
                }
            );
            gpu.read(1, &mut bytes).unwrap();
            let mut expected = [0x57; 8];
            if operation == TransferOp::Write {
                expected[..if lost_query { 3 } else { 8 }].fill(0xcc);
            }
            assert_eq!(bytes, expected);
            for engine in [&requester, &source] {
                engine.close(Duration::from_secs(2)).unwrap();
            }
            assert!(b.pins.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn opposite_direction_sessions_hold_disjoint_gpu_ranges_without_network_locks() {
    let make = || {
        TransferEngine::new(EngineConfig {
            staging_chunk_count: 6,
            staging_pipeline_depth: 3,
            ..config()
        })
        .unwrap()
    };
    let a = make();
    let b = make();
    let a_owner = allocation(&[0x31; 24]);
    let b_owner = allocation(&[0x42; 24]);
    let a_gpu = memory(&a_owner);
    let b_gpu = memory(&b_owner);
    let a_memory = a
        .register_cuda_memory_batch(&[(a_gpu.clone(), 0, 24)], Permission::RemoteReadWrite)
        .unwrap()
        .remove(0);
    let b_memory = b
        .register_cuda_memory_batch(&[(b_gpu.clone(), 0, 24)], Permission::RemoteReadWrite)
        .unwrap()
        .remove(0);
    let a_peer = a
        .open_segment(b.endpoint(), b.source_epoch(), Duration::from_secs(2))
        .unwrap();
    let b_peer = b
        .open_segment(a.endpoint(), a.source_epoch(), Duration::from_secs(2))
        .unwrap();
    for owner in [&a_owner, &b_owner] {
        owner.query_hold.store(true, Ordering::Release);
    }
    std::thread::scope(|scope| {
        fn run(
            engine: &TransferEngine,
            peer: &crate::RemoteSegment,
            local: &crate::RegisteredMemory,
            remote: &crate::RegisteredMemory,
        ) {
            let report = engine
                .submit_and_wait(
                    TransferOp::Read,
                    peer,
                    &[TransferRequest {
                        transport: Transport::Tcp,
                        local_address: local.base_address() + 12,
                        remote_address: remote.base_address(),
                        length: 8,
                    }],
                    Duration::from_secs(2),
                )
                .unwrap();
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
        }
        let left = scope.spawn(|| run(&a, &a_peer, &a_memory, &b_memory));
        let right = scope.spawn(|| run(&b, &b_peer, &b_memory, &a_memory));
        for owner in [&a_owner, &b_owner] {
            wait_pending(owner, 3);
        }
        for owner in [&a_owner, &b_owner] {
            assert_eq!(owner.pins.lock().unwrap().len(), 6);
            assert_eq!(
                owner
                    .streams
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|v| v.is_some())
                    .count(),
                3
            );
        }
        for gpu in [&a_gpu, &b_gpu] {
            let allocation = gpu
                .allocation
                .try_lock()
                .expect("no GPU lock across pending copies/network");
            assert!(!allocation.uncertain);
            assert_eq!(allocation.dma.len(), 2);
            assert!(allocation.dma.contains(&(0..8)) && allocation.dma.contains(&(12..20)));
        }
        for owner in [&a_owner, &b_owner] {
            owner.query_hold.store(false, Ordering::Release);
        }
        left.join().unwrap();
        right.join().unwrap();
    });
    for (engine, memory, value) in [(&a, &a_memory, 0x42), (&b, &b_memory, 0x31)] {
        let mut bytes = [0; 8];
        memory.read(12, &mut bytes).unwrap();
        assert_eq!(bytes, [value; 8]);
        engine.close(Duration::from_secs(2)).unwrap();
    }
}

#[test]
fn pin_rejects_non_cuda_opt_out_and_source_permission_before_allocation() {
    for scenario in ["host", "disabled", "permission", "epoch", "registration"] {
        let mut config = config();
        config.remote_staging = scenario != "disabled";
        let source = TransferEngine::new(config).unwrap();
        let a = allocation(&[0x57; 12]);
        let gpu = memory(&a);
        let memory = if scenario == "host" {
            source
                .register_memory(
                    vec![0x57; 12].into_boxed_slice(),
                    Permission::RemoteReadWrite,
                )
                .unwrap()
        } else {
            source
                .register_cuda_memory_batch(
                    &[(gpu, 0, 12)],
                    if scenario == "permission" {
                        Permission::RemoteRead
                    } else {
                        Permission::RemoteReadWrite
                    },
                )
                .unwrap()
                .remove(0)
        };
        let mut channel =
            ControlChannel::connect(source.endpoint(), Instant::now() + Duration::from_secs(2))
                .unwrap();
        channel
            .write_all(
                &Request {
                    operation: Operation::PinWrite,
                    epoch: if scenario == "epoch" {
                        SourceEpoch::generate().unwrap()
                    } else {
                        source.source_epoch()
                    },
                    region: memory.id() + u64::from(scenario == "registration"),
                    offset: 1,
                    length: 8,
                }
                .encode(),
            )
            .unwrap();
        channel.write_all(&[0, 1]).unwrap();
        channel.write_all(&3u64.to_be_bytes()).unwrap();
        channel.write_all(&[1]).unwrap();
        let error = match Reply::read(&mut channel, source.source_epoch()) {
            Err(error) => error,
            Ok(_) => panic!("unexpected Pin grant: {scenario}"),
        };
        assert!(
            match scenario {
                "host" | "disabled" => matches!(error, Error::Unavailable(_)),
                "permission" => matches!(error, Error::PermissionDenied),
                "epoch" => matches!(error, Error::EpochMismatch),
                _ => matches!(error, Error::RegionNotFound),
            },
            "{scenario}: {error:?}"
        );
        assert_eq!(source.stats().unwrap().staging.allocations, 0);
        source.close(Duration::from_secs(2)).unwrap();
    }
}
