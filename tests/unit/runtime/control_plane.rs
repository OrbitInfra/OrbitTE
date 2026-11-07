#[path = "control_plane/native_boundary.rs"]
pub(crate) mod native_boundary;

use std::net::Shutdown;

use super::*;
use crate::{RemoteEffect, TransferEngine, TransferOp, TransferRequest};

use crate::test_support as common;

const TIMEOUT: Duration = Duration::from_secs(3);

fn wire_request(
    engine: &TransferEngine,
    operation: Operation,
    id: u64,
    offset: u64,
    length: u64,
) -> ControlChannel<'static> {
    let mut channel =
        ControlChannel::connect(engine.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    channel
        .write_all(
            &Request {
                operation,
                epoch: engine.source_epoch(),
                region: id,
                offset,
                length,
            }
            .encode(),
        )
        .unwrap();
    channel
}

#[test]
fn pin_requires_the_last_hello_byte_before_replying_to_an_invalid_region() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    for operation in [Operation::PinRead, Operation::PinWrite] {
        let mut hello = [0; PIN_HELLO_SIZE];
        hello[1] = 1;
        hello[2..10].copy_from_slice(&1u64.to_be_bytes());
        hello[10] = 1;
        let mut partial = wire_request(&source, operation, 99, 0, 1);
        partial.write_all(&hello[..PIN_HELLO_SIZE - 1]).unwrap();
        partial.stream.shutdown(Shutdown::Write).unwrap();
        assert!(
            matches!(partial.read_exact(&mut [0]), Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof)
        );

        let mut complete = wire_request(&source, operation, 99, 0, 1);
        complete.write_all(&hello).unwrap();
        assert!(matches!(
            Reply::read(&mut complete, source.source_epoch()),
            Err(Error::RegionNotFound)
        ));
        #[cfg(feature = "cuda")]
        assert_eq!(source.stats().unwrap().staging.allocations, 0);
    }
    source.close(TIMEOUT).unwrap();
}

#[test]
fn request_fixture_and_decoder_reject_protocol_drift() {
    let request = Request {
        operation: Operation::TcpRead,
        epoch: SourceEpoch([0x11; 16]),
        region: 1,
        offset: 2,
        length: 3,
    };
    let mut expected = Vec::from(&b"ORBITTE\0\0\x01\x01\0"[..]);
    expected.extend([0x11; 16]);
    expected.extend([0, 0, 0, 0, 0, 0, 0, 1]);
    expected.extend([0, 0, 0, 0, 0, 0, 0, 2]);
    expected.extend([0, 0, 0, 0, 0, 0, 0, 3]);
    assert_eq!(request.encode().as_slice(), expected);
    for (index, value) in [(0, 0), (10, 255), (11, 1)] {
        let mut malformed = request.encode();
        malformed[index] = value;
        assert!(matches!(
            Request::decode(&malformed),
            Err(Error::Protocol(_))
        ));
    }

    let mut request_bytes = request.encode();
    let mut reply_bytes = Reply {
        status: 0,
        epoch: request.epoch,
        length: 3,
    }
    .encode();
    for version in (0..=u16::MAX).filter(|&v| v != VERSION) {
        request_bytes[8..10].copy_from_slice(&version.to_be_bytes());
        reply_bytes[8..10].copy_from_slice(&version.to_be_bytes());
        assert!(
            matches!(Request::decode(&request_bytes), Err(Error::Protocol(_))),
            "request version {version}"
        );
        assert!(
            matches!(
                Reply::decode(&reply_bytes, request.epoch),
                Err(Error::Protocol(_))
            ),
            "reply version {version}"
        );
    }
}

#[test]
fn source_rejects_a_legacy_header_and_accepts_a_same_build_peer() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let memory = source
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let epoch = source.source_epoch();
    let mut legacy = Request {
        operation: Operation::Describe,
        epoch,
        region: 0,
        offset: 0,
        length: 0,
    }
    .encode();
    legacy[8..10].copy_from_slice(&23u16.to_be_bytes());
    let mut channel =
        ControlChannel::connect(source.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    channel.write_all(&legacy).unwrap();
    assert!(matches!(
        Reply::read(&mut channel, epoch),
        Err(Error::Protocol(_))
    ));

    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let peer = requester
        .open_segment(source.endpoint(), epoch, TIMEOUT)
        .unwrap();
    assert_eq!(peer.regions().len(), 1);
    assert_eq!(peer.regions()[0].id(), memory.id());
}

#[test]
fn source_validates_untrusted_ranges_permissions_epochs_and_budgets() {
    let engine = TransferEngine::new(EngineConfig {
        max_transfer_bytes: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    let memory = engine
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    for (operation, id, offset, length, expected) in [
        (Operation::TcpWrite, memory.id(), 0, 4, 3),
        (Operation::TcpRead, memory.id(), 7, 2, 4),
        (Operation::TcpRead, memory.id(), u64::MAX, 2, 4),
        (Operation::TcpRead, memory.id(), 0, 0, 4),
        (Operation::TcpRead, memory.id(), 0, 9, 6),
        (Operation::TcpRead, 999, 0, 4, 2),
        (Operation::RdmaRead, memory.id(), 0, 4, 10),
        (Operation::RdmaWrite, memory.id(), 0, 4, 3),
        (Operation::RdmaReadSlices, memory.id(), 0, 4, 10),
        (Operation::RdmaWriteSlices, memory.id(), 0, 4, 3),
        (Operation::RdmaReadSlices, memory.id(), 7, 2, 4),
        (Operation::RdmaReadSlices, memory.id(), 0, 9, 6),
        (Operation::NvlinkRead, memory.id(), 0, 4, 10),
        (Operation::NvlinkWrite, memory.id(), 0, 4, 3),
    ] {
        let mut channel = wire_request(&engine, operation, id, offset, length);
        let mut bytes = [0; REPLY_SIZE];
        channel.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes[10], expected);
    }
    let mut channel =
        ControlChannel::connect(engine.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    channel
        .write_all(
            &Request {
                operation: Operation::TcpRead,
                epoch: SourceEpoch([0; 16]),
                region: memory.id(),
                offset: 0,
                length: 4,
            }
            .encode(),
        )
        .unwrap();
    assert!(matches!(
        Reply::read(&mut channel, SourceEpoch([0; 16])),
        Err(Error::EpochMismatch)
    ));
}

#[test]
fn fragmented_write_keeps_backing_and_close_waits_for_actual_handler_exit() {
    let engine = TransferEngine::new(EngineConfig {
        max_registered_regions: 1,
        max_registered_bytes: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    let memory = engine
        .register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let mut channel =
        ControlChannel::connect(engine.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    let request = Request {
        operation: Operation::TcpWrite,
        epoch: engine.source_epoch(),
        region: memory.id(),
        offset: 0,
        length: 8,
    }
    .encode();
    for fragment in request.chunks(3) {
        channel.write_all(fragment).unwrap();
    }
    assert_eq!(
        Reply::read(&mut channel, engine.source_epoch())
            .unwrap()
            .status,
        9
    );
    engine.unregister_memory(&memory).unwrap();
    assert_eq!(engine.stats().unwrap().retained_bytes, 8);
    assert!(matches!(
        engine.close_source(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    channel.write_all(&[1, 2, 3]).unwrap();
    channel.write_all(&[4, 5, 6, 7, 8]).unwrap();
    assert_eq!(
        Reply::read(&mut channel, engine.source_epoch())
            .unwrap()
            .status,
        0
    );
    engine.close_source(TIMEOUT).unwrap();
    let mut bytes = [0; 8];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(engine.stats().unwrap().active_source_operations, 0);
    drop(memory);
    assert_eq!(engine.stats().unwrap().retained_bytes, 0);
}

#[test]
fn incomplete_write_never_commits_and_engine_drop_interrupts_idle_connections() {
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    let memory = engine
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let mut channel = wire_request(&engine, Operation::TcpWrite, memory.id(), 0, 8);
    assert_eq!(
        Reply::read(&mut channel, engine.source_epoch())
            .unwrap()
            .status,
        9
    );
    channel.write_all(&[1, 2, 3]).unwrap();
    channel.stream.shutdown(Shutdown::Both).unwrap();
    engine.close_source(TIMEOUT).unwrap();
    let mut bytes = [0; 8];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7; 8]);
    drop(engine);
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    let memory = engine
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let mut _idle = wire_request(&engine, Operation::TcpWrite, memory.id(), 0, 8);
    assert_eq!(
        Reply::read(&mut _idle, engine.source_epoch())
            .unwrap()
            .status,
        9
    );
    let start = Instant::now();
    drop(engine);
    assert!(start.elapsed() < Duration::from_secs(1));
    memory.read(0, &mut bytes).unwrap();
}

fn fault_peer() -> (TcpListener, RemoteSegment) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let epoch = SourceEpoch::generate().unwrap();
    let peer = RemoteSegment {
        identity: None,
        metadata: Default::default(),
        endpoint: listener.local_addr().unwrap(),
        epoch,
        regions: vec![RemoteRegion {
            memory_type: crate::MemoryType::Cpu,
            fabric: None,
            hp_registered: false,
            profile: Default::default(),
            ipc_exportable: false,
            epoch,
            id: 1,
            address: 0x1000,
            length: 8,
            permission: Permission::RemoteReadWrite,
        }],
    };
    (listener, peer)
}

fn fault_request(engine: &TransferEngine, peer: &RemoteSegment) -> TransferRequest {
    TransferRequest::new(
        crate::Transport::Tcp,
        engine
            .register_memory(vec![7; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap()
            .base_address(),
        peer.regions[0].base_address(),
        8,
    )
}

#[test]
fn partial_read_preserves_destination_and_lost_write_ack_is_ambiguous() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let (listener, mut peer) = fault_peer();
        let epoch = peer.epoch;
        let handler = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
                nonblocking: false,
            };
            channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
            if operation == TransferOp::Read {
                channel
                    .write_all(
                        &Reply {
                            status: 0,
                            epoch,
                            length: 8,
                        }
                        .encode(),
                    )
                    .unwrap();
                channel.write_all(&[1, 2, 3]).unwrap();
            } else {
                channel
                    .write_all(
                        &Reply {
                            status: 9,
                            epoch,
                            length: 0,
                        }
                        .encode(),
                    )
                    .unwrap();
                let mut body = [0; 8];
                channel.read_exact(&mut body).unwrap();
                assert_eq!(body, [7; 8]);
                // Source applied the body, then lost its final acknowledgement.
            }
            listener
        });
        let engine = TransferEngine::new(EngineConfig::default()).unwrap();

        engine.bind_test_peer(&mut peer).unwrap();
        let request = fault_request(&engine, &peer);
        let report = engine
            .submit_and_wait(
                operation,
                &peer,
                &[request.clone(), request.clone()],
                TIMEOUT,
            )
            .unwrap();
        let failure = report.failure.unwrap();
        assert_eq!(failure.request_index, 0);
        assert_eq!(report.completed_requests, 0);
        assert_eq!(
            failure.remote_effect,
            if operation == TransferOp::Write {
                RemoteEffect::Unknown
            } else {
                RemoteEffect::None
            }
        );
        let mut bytes = [0; 8];
        engine
            .resolve_local_address(request.local_address, 8)
            .unwrap()
            .0
            .read(0, &mut bytes)
            .unwrap();
        assert_eq!(bytes, [7; 8]);
        let listener = handler.join().unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "failed request was replayed or a later request was attempted"
        );
        assert_eq!(engine.stats().unwrap().inflight_bytes, 0);
    }
}

#[test]
fn a_trickling_response_cannot_extend_the_absolute_deadline() {
    let (listener, mut peer) = fault_peer();
    let epoch = peer.epoch;
    let handler = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut channel = ControlChannel {
            stream,
            deadline: deadline(TIMEOUT).unwrap(),
            stopping: None,
            nonblocking: false,
        };
        channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
        channel
            .write_all(
                &Reply {
                    status: 0,
                    epoch,
                    length: 8,
                }
                .encode(),
            )
            .unwrap();
        for _ in 0..8 {
            thread::sleep(Duration::from_millis(60));
            if channel.write_all(&[1]).is_err() {
                break;
            }
        }
    });
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();

    engine.bind_test_peer(&mut peer).unwrap();
    let request = fault_request(&engine, &peer);
    let report = engine
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&request),
            Duration::from_millis(220),
        )
        .unwrap();
    assert!(matches!(report.failure.unwrap().error, Error::Timeout));
    let mut bytes = [0; 8];
    engine
        .resolve_local_address(request.local_address, 8)
        .unwrap()
        .0
        .read(0, &mut bytes)
        .unwrap();
    assert_eq!(bytes, [7; 8]);
    handler.join().unwrap();
}

#[test]
fn external_owner_survives_unregister_until_the_handler_really_exits() {
    use std::sync::atomic::Ordering;

    let fixture = common::external_memory(&[0; 8]);
    let engine = TransferEngine::new(EngineConfig {
        max_registered_regions: 1,
        max_registered_bytes: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    let memory = engine
        .register_host_memory(
            Arc::clone(&fixture.memory),
            0,
            8,
            Permission::RemoteReadWrite,
        )
        .unwrap();
    drop(fixture.memory);
    let mut channel = wire_request(&engine, Operation::TcpWrite, memory.id(), 0, 8);
    assert_eq!(
        Reply::read(&mut channel, engine.source_epoch())
            .unwrap()
            .status,
        9
    );
    engine.unregister_memory(&memory).unwrap();
    drop(memory);
    assert!(matches!(
        engine.close_source(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert!(fixture.owner.upgrade().is_some());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    assert_eq!(engine.stats().unwrap().retained_bytes, 8);
    channel.write_all(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    assert_eq!(
        Reply::read(&mut channel, engine.source_epoch())
            .unwrap()
            .status,
        0
    );
    engine.close_source(TIMEOUT).unwrap();
    assert!(fixture.owner.upgrade().is_none());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(engine.stats().unwrap().retained_bytes, 0);
}

#[test]
fn descriptor_decoder_rejects_bad_addresses_versions_and_ambiguous_ids() {
    for case in 0..13 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let epoch = SourceEpoch::generate().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
                nonblocking: false,
            };
            channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
            let count = if case == 5 { 2 } else { 1 };
            let mut payload = vec![0; DESCRIPTOR_HEADER + count * REGION_SIZE];
            payload[..4].copy_from_slice(&(count as u32).to_be_bytes());
            for entry in payload[DESCRIPTOR_HEADER..].chunks_exact_mut(REGION_SIZE) {
                entry[..8].copy_from_slice(&1u64.to_be_bytes());
                entry[8..16].copy_from_slice(&0x1000u64.to_be_bytes());
                entry[16..24].copy_from_slice(&8u64.to_be_bytes());
                entry[24] = 3;
            }
            let entry = &mut payload[DESCRIPTOR_HEADER..];
            match case {
                0 => entry[8..16].copy_from_slice(&0u64.to_be_bytes()),
                1 => entry[8..16].copy_from_slice(&(u64::MAX - 2).to_be_bytes()),
                2 => entry[16..24].copy_from_slice(&0u64.to_be_bytes()),
                3 => entry[24] = 7,
                4 => entry[27] = 1,
                7 => entry[25] = 2,
                8 => entry[26] = 1, // Host memory cannot authorize CUDA IPC export.
                9 => entry[26] = 2,
                10 => payload[4] = 8,
                11 => payload[8] = 1, // Boot bytes require an advertised placement identity.
                // A pre-selector peer can send the same header with the old Describe layout.
                12 => drop(payload.drain(4..DESCRIPTOR_HEADER)),
                _ => {}
            }
            let mut reply = Reply {
                status: 0,
                epoch,
                length: payload.len() as u64,
            }
            .encode();
            if case == 6 {
                reply[8..10].copy_from_slice(&(VERSION + 1).to_be_bytes());
            }
            channel.write_all(&reply).unwrap();
            let _ = channel.write_all(&payload);
        });
        assert!(
            matches!(
                open_segment(endpoint, epoch, deadline(TIMEOUT).unwrap()),
                Err(Error::Protocol(_))
            ),
            "malformed descriptor case {case}"
        );
        server.join().unwrap();
    }
}

#[test]
fn notification_reservations_survive_drains_and_source_close() {
    let engine = TransferEngine::new(EngineConfig {
        max_pending_notifications: 2,
        max_pending_notification_bytes: 8,
        max_notification_bytes: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    let first = Notification {
        name: "a".into(),
        message: "b".into(),
    };
    engine.send_local_notification(&first).unwrap();
    for (id, name, message, status) in [(1, 0, 0, 7), (0, u64::MAX, 1, 6), (0, 0, 9, 6)] {
        let mut rejected = wire_request(&engine, Operation::Notify, id, name, message);
        let mut reply = [0; REPLY_SIZE];
        rejected.read_exact(&mut reply).unwrap();
        assert_eq!(reply[10], status);
    }
    let mut pending = wire_request(&engine, Operation::Notify, 0, 2, 4);
    assert_eq!(
        Reply::read(&mut pending, engine.source_epoch())
            .unwrap()
            .status,
        9
    );
    assert_eq!(engine.stats().unwrap().retained_notification_bytes, 8);
    let mut rejected = wire_request(&engine, Operation::Notify, 0, 0, 0);
    assert!(matches!(
        Reply::read(&mut rejected, engine.source_epoch()),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(engine.receive_notifications().unwrap(), [first]);
    let usage = engine.stats().unwrap();
    assert_eq!(
        (
            usage.queued_notifications,
            usage.retained_notifications,
            usage.retained_notification_bytes
        ),
        (0, 1, 6)
    );
    assert!(matches!(
        engine.close_source(Duration::ZERO),
        Err(Error::Timeout)
    ));
    pending.write_all(b"na12").unwrap();
    assert!(engine.receive_notifications().unwrap().is_empty());
    pending.write_all(b"34\0").unwrap();
    assert_eq!(
        Reply::read(&mut pending, engine.source_epoch())
            .unwrap()
            .status,
        0
    );
    engine.close_source(TIMEOUT).unwrap();
    assert_eq!(
        engine.receive_notifications().unwrap(),
        [Notification {
            name: "na".into(),
            message: "1234".into()
        }]
    );
    assert_eq!(engine.stats().unwrap().retained_notifications, 0);
    let mut closed = wire_request(&engine, Operation::Notify, 0, 0, 0);
    assert!(matches!(
        Reply::read(&mut closed, engine.source_epoch()),
        Err(Error::SourceClosed)
    ));
}

#[test]
fn malformed_and_incomplete_notifications_never_enter_the_inbox() {
    for (name, message, body, rejected) in [
        (1, 0, vec![0xff, 0], true),
        (0, 1, vec![0xff, 0], true),
        (0, 0, vec![1], true),
        (1, 2, vec![b'a', b'b'], false),
        (0, 0, vec![], false),
    ] {
        let engine = TransferEngine::new(EngineConfig::default()).unwrap();
        let mut pending = wire_request(&engine, Operation::Notify, 0, name, message);
        assert_eq!(
            Reply::read(&mut pending, engine.source_epoch())
                .unwrap()
                .status,
            9
        );
        pending.write_all(&body).unwrap();
        if rejected {
            assert!(matches!(
                Reply::read(&mut pending, engine.source_epoch()),
                Err(Error::Protocol(_))
            ));
        } else {
            pending.stream.shutdown(Shutdown::Both).unwrap();
        }
        engine.close_source(TIMEOUT).unwrap();
        assert!(engine.receive_notifications().unwrap().is_empty());
        let usage = engine.stats().unwrap();
        assert_eq!(
            (
                usage.retained_notifications,
                usage.retained_notification_bytes
            ),
            (0, 0)
        );
    }
}

#[test]
fn lost_notification_receipt_never_replays_or_erases_completed_data() {
    for attached in [false, true] {
        let (listener, mut peer) = fault_peer();
        let epoch = peer.epoch;
        let handler = thread::spawn(move || {
            if attached {
                let (stream, _) = listener.accept().unwrap();
                let mut channel = ControlChannel {
                    stream,
                    deadline: deadline(TIMEOUT).unwrap(),
                    stopping: None,
                    nonblocking: false,
                };
                let mut header = [0; REQUEST_SIZE];
                channel.read_exact(&mut header).unwrap();
                assert!(Request::decode(&header).unwrap().operation == Operation::TcpRead);
                channel
                    .write_all(
                        &Reply {
                            status: 0,
                            epoch,
                            length: 8,
                        }
                        .encode(),
                    )
                    .unwrap();
                channel.write_all(&[0xa7; 8]).unwrap();
            }
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
                nonblocking: false,
            };
            let mut header = [0; REQUEST_SIZE];
            channel.read_exact(&mut header).unwrap();
            let request = Request::decode(&header).unwrap();
            assert!(request.operation == Operation::Notify);
            assert_eq!((request.offset, request.length), (0, 0));
            channel
                .write_all(
                    &Reply {
                        status: 9,
                        epoch,
                        length: 0,
                    }
                    .encode(),
                )
                .unwrap();
            let mut marker = [1];
            channel.read_exact(&mut marker).unwrap();
            assert_eq!(marker, [0]);
            // Enqueue succeeded at the source, but its final receipt is lost.
            listener
        });
        let engine = TransferEngine::new(EngineConfig::default()).unwrap();

        engine.bind_test_peer(&mut peer).unwrap();
        let notification = Notification {
            name: String::new(),
            message: String::new(),
        };
        let failure = if attached {
            let request = fault_request(&engine, &peer);
            let report = engine
                .submit_and_notify(
                    TransferOp::Read,
                    &peer,
                    std::slice::from_ref(&request),
                    &notification,
                    TIMEOUT,
                )
                .unwrap();
            assert!(report.is_complete());
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (1, 8)
            );
            let mut bytes = [0; 8];
            engine
                .resolve_local_address(request.local_address, 8)
                .unwrap()
                .0
                .read(0, &mut bytes)
                .unwrap();
            assert_eq!(bytes, [0xa7; 8]);
            match report.notification {
                crate::NotificationStatus::Failed(failure) => failure,
                other => panic!("unexpected notification outcome: {other:?}"),
            }
        } else {
            engine
                .send_notification(&peer, &notification, TIMEOUT)
                .unwrap_err()
        };
        assert!(matches!(failure.error, Error::Io(_)));
        assert_eq!(failure.effect, NotificationEffect::Unknown);
        let listener = handler.join().unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
        assert_eq!(engine.stats().unwrap().inflight_batches, 0);
    }
}

#[test]
fn address_batch_retains_requester_allocation_through_concurrent_unregister_and_close() {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;

    let (listener, mut peer) = fault_peer();
    let epoch = peer.epoch;
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handler = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut channel = ControlChannel {
            stream,
            deadline: deadline(TIMEOUT).unwrap(),
            stopping: None,
            nonblocking: false,
        };
        channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
        admitted_tx.send(()).unwrap();
        release_rx.recv_timeout(common::EVENTUAL_PROGRESS).unwrap();
        channel
            .write_all(
                &Reply {
                    status: 0,
                    epoch,
                    length: 8,
                }
                .encode(),
            )
            .unwrap();
        channel.write_all(&[0xa7; 8]).unwrap();
    });
    let engine = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());

    engine.bind_test_peer(&mut peer).unwrap();
    let fixture = common::external_memory(&[0; 8]);
    let local = engine
        .register_host_memory(Arc::clone(&fixture.memory), 0, 8, Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest::new(
        crate::Transport::Tcp,
        local.base_address(),
        peer.regions[0].base_address(),
        8,
    );
    let requester = {
        let engine = Arc::clone(&engine);
        let peer = peer.clone();
        let request = request.clone();
        thread::spawn(move || {
            engine
                .submit_and_wait(TransferOp::Read, &peer, &[request], TIMEOUT)
                .unwrap()
        })
    };
    admitted_rx.recv_timeout(common::EVENTUAL_PROGRESS).unwrap();
    engine.unregister_memory(&local).unwrap();
    drop(local);
    drop(fixture.memory);
    engine.close_source(Duration::ZERO).unwrap();
    assert!(fixture.owner.upgrade().is_some());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    assert_eq!(engine.stats().unwrap().retained_bytes, 8);
    assert!(matches!(
        engine.submit_and_wait(TransferOp::Read, &peer, &[request], TIMEOUT),
        Err(Error::RegionNotFound)
    ));
    release_tx.send(()).unwrap();
    let report = requester.join().unwrap();
    assert!(report.is_complete());
    assert_eq!(report.transferred_bytes, 8);
    handler.join().unwrap();
    assert!(fixture.owner.upgrade().is_none());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    let stats = engine.stats().unwrap();
    assert_eq!(
        (
            stats.retained_regions,
            stats.retained_bytes,
            stats.inflight_batches,
            stats.inflight_bytes
        ),
        (0, 0, 0, 0)
    );
}

#[test]
fn async_cancel_wait_timeout_and_requester_drain_preserve_physical_owners() {
    use crate::{BatchState, TaskState};
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;

    for queued_cancel in [true, false] {
        let (listener, mut peer) = fault_peer();
        let epoch = peer.epoch;
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let handler = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
                nonblocking: false,
            };
            channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
            admitted_tx.send(()).unwrap();
            release_rx.recv_timeout(common::EVENTUAL_PROGRESS).unwrap();
            channel
                .write_all(
                    &Reply {
                        status: 0,
                        epoch,
                        length: 8,
                    }
                    .encode(),
                )
                .unwrap();
            channel.write_all(&[0xa7; 8]).unwrap();
            listener
        });
        let engine = Arc::new(
            TransferEngine::new(EngineConfig {
                max_allocated_batches: 1,
                ..EngineConfig::default()
            })
            .unwrap(),
        );

        engine.bind_test_peer(&mut peer).unwrap();
        let fixture = common::external_memory(&[0; 8]);
        let local = engine
            .register_host_memory(Arc::clone(&fixture.memory), 0, 8, Permission::LocalOnly)
            .unwrap();
        let request = TransferRequest::new(
            crate::Transport::Tcp,
            local.base_address(),
            peer.regions[0].base_address(),
            8,
        );
        let mut batch = engine.allocate_batch(2).unwrap();
        batch
            .submit(
                TransferOp::Read,
                &peer,
                std::slice::from_ref(&request),
                None,
                TIMEOUT,
            )
            .unwrap();
        admitted_rx.recv_timeout(common::EVENTUAL_PROGRESS).unwrap();
        let queued_timeout = if queued_cancel {
            TIMEOUT
        } else {
            Duration::from_millis(20)
        };
        batch
            .submit(TransferOp::Read, &peer, &[request], None, queued_timeout)
            .unwrap();
        if queued_cancel {
            assert!(matches!(batch.cancel(0), Err(Error::Unavailable(_))));
            assert!(batch.cancel(1).unwrap());
        }
        batch.seal().unwrap();
        assert!(matches!(
            batch.wait(Duration::from_millis(25)),
            Err(Error::Timeout)
        ));
        assert_eq!(batch.task_status(0).unwrap().state, TaskState::Running);
        assert!(!batch.status().unwrap().locally_drained);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        assert!(matches!(
            engine.close_requester(Duration::ZERO),
            Err(Error::Timeout)
        ));
        assert!(matches!(
            engine.allocate_batch(1),
            Err(Error::RequesterClosed)
        ));
        engine.unregister_memory(&local).unwrap();
        drop(local);
        drop(fixture.memory);
        assert!(fixture.owner.upgrade().is_some());
        assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
        release_tx.send(()).unwrap();
        let status = batch.wait(TIMEOUT).unwrap();
        assert_eq!(
            status.state,
            if queued_cancel {
                BatchState::Cancelled
            } else {
                BatchState::Failed
            }
        );
        assert_eq!(
            (
                status.completed_tasks,
                status.transferred_bytes,
                status.locally_drained
            ),
            (1, 8, true)
        );
        let tasks = batch.task_statuses().unwrap();
        assert_eq!(tasks[0].state, TaskState::Completed);
        assert!(!tasks[0].cancel_requested);
        let failure = tasks[1].failure.as_ref().unwrap();
        assert!(if queued_cancel {
            matches!(failure.error, Error::Cancelled)
        } else {
            matches!(failure.error, Error::Timeout)
        });
        assert_eq!(failure.remote_effect, RemoteEffect::None);
        assert_eq!(failure.request_index, 1);
        assert!(fixture.owner.upgrade().is_none());
        assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
        engine.close_requester(TIMEOUT).unwrap();
        let stats = engine.stats().unwrap();
        assert_eq!(
            (
                stats.retained_bytes,
                stats.inflight_batches,
                stats.inflight_bytes
            ),
            (0, 0, 0)
        );
        let listener = handler.join().unwrap();
        listener.set_nonblocking(true).unwrap();
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "queued work reached the network after cancellation/deadline"
        );
        batch.free().unwrap();
        assert_eq!(engine.stats().unwrap().allocated_batches, 0);
    }
}

#[test]
fn dropping_async_handle_retains_engine_allocator_and_capacity_until_worker_exit() {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;

    let (listener, mut peer) = fault_peer();
    let epoch = peer.epoch;
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handler = thread::spawn(move || {
        for index in 0..2 {
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
                nonblocking: false,
            };
            channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
            if index == 0 {
                admitted_tx.send(()).unwrap();
                release_rx.recv_timeout(common::EVENTUAL_PROGRESS).unwrap();
            }
            channel
                .write_all(
                    &Reply {
                        status: 0,
                        epoch,
                        length: 8,
                    }
                    .encode(),
                )
                .unwrap();
            channel.write_all(&[0xa7; 8]).unwrap();
        }
    });
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            max_allocated_batches: 1,
            ..EngineConfig::default()
        })
        .unwrap(),
    );

    engine.bind_test_peer(&mut peer).unwrap();
    let weak_engine = Arc::downgrade(&engine);
    let fixture = common::external_memory(&[0; 8]);
    let local = engine
        .register_host_memory(Arc::clone(&fixture.memory), 0, 8, Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest::new(
        crate::Transport::Tcp,
        local.base_address(),
        peer.regions[0].base_address(),
        8,
    );
    let mut batch = engine.allocate_batch(2).unwrap();
    batch
        .submit(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&request),
            None,
            TIMEOUT,
        )
        .unwrap();
    batch
        .submit(TransferOp::Read, &peer, &[request], None, TIMEOUT)
        .unwrap();
    admitted_rx.recv_timeout(common::EVENTUAL_PROGRESS).unwrap();
    engine.unregister_memory(&local).unwrap();
    drop(local);
    drop(fixture.memory);
    drop(batch);
    assert_eq!(engine.stats().unwrap().allocated_batches, 1);
    assert!(matches!(
        engine.allocate_batch(1),
        Err(Error::ResourceExhausted)
    ));
    assert!(fixture.owner.upgrade().is_some());
    drop(engine);
    assert!(weak_engine.upgrade().is_some());
    release_tx.send(()).unwrap();
    handler.join().unwrap();
    let end = Instant::now() + common::EVENTUAL_PROGRESS;
    while weak_engine.upgrade().is_some() || fixture.owner.upgrade().is_some() {
        assert!(
            Instant::now() < end,
            "detached batch did not drain and release its actual owners"
        );
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "rdma")]
pub(crate) fn read_request(channel: &mut ControlChannel<'_>) -> Request {
    let mut bytes = [0; REQUEST_SIZE];
    channel.read_exact(&mut bytes).unwrap();
    Request::decode(&bytes).unwrap()
}

#[test]
fn notify_ack_requires_a_registered_accepting_callback_and_well_formed_body() {
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    // Both real wire handlers dispatch through the same callback registration.
    let source = engine.notification_inbox_for_test();
    for operation in [Operation::Notify, Operation::NotifyRpc] {
        source.set_receiver(None);
        for behavior in [
            "before",
            "accept",
            "cleared",
            "throw",
            "reject",
            "malformed",
        ] {
            match behavior {
                "accept" | "malformed" => source.set_receiver(Some(|p, n| p.commit(n))),
                "cleared" => source.set_receiver(None),
                "throw" => source.set_receiver(Some(|_, _| panic!("callback failed"))),
                "reject" => source.set_receiver(Some(|_, _| Err(Error::PermissionDenied))),
                _ => {}
            }
            let mut channel = wire_request(&engine, operation, 0, 1, 0);
            assert_eq!(
                Reply::read(&mut channel, engine.source_epoch())
                    .unwrap()
                    .status,
                9
            );
            channel
                .write_all(&[b'x', u8::from(behavior == "malformed")])
                .unwrap();
            let reply = Reply::read(&mut channel, engine.source_epoch());
            match behavior {
                "accept" => assert_eq!(reply.unwrap().status, 0),
                "before" | "cleared" => assert!(matches!(reply, Err(Error::Unavailable(_)))),
                "throw" => assert!(matches!(reply, Err(Error::Internal(_)))),
                "reject" => assert!(matches!(reply, Err(Error::PermissionDenied))),
                _ => assert!(matches!(reply, Err(Error::Protocol(_)))),
            }
            assert_eq!(
                engine.receive_notifications().unwrap().len(),
                usize::from(behavior == "accept")
            );
            assert_eq!(engine.stats().unwrap().retained_notifications, 0);
        }
    }
}

#[test]
fn deferred_free_races_progress_and_allocation_until_real_tcp_drain() {
    use std::sync::atomic::Ordering;
    let t = common::EVENTUAL_PROGRESS;
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            enable_progress_worker: true,
            runtime_queue: Some(Default::default()),
            max_allocated_batches: 3,
            ..Default::default()
        })
        .unwrap(),
    );
    let (listener, mut peer) = fault_peer();
    engine.bind_test_peer(&mut peer).unwrap();

    let epoch = peer.epoch;
    let fi = common::external_memory(&[0; 8]);
    let local = engine
        .register_host_memory(fi.memory.clone(), 0, 8, Permission::LocalOnly)
        .unwrap();
    let req = [TransferRequest::new(
        crate::Transport::Tcp,
        local.base_address(),
        peer.regions[0].base_address(),
        8,
    )];
    let mut batch = engine
        .submit_async(TransferOp::Read, &peer, &req, None, t)
        .unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    stream.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
    let id = batch.id().unwrap();
    engine.unregister_memory(&local).unwrap();
    drop((local, fi.memory));
    // Two explicit pollers observe the same worker-owned request. Free on a
    // third thread fences the ID without waiting for this peer to reply.
    let barrier = std::sync::Barrier::new(4);
    let releases = thread::scope(|scope| {
        let poll = || {
            assert_eq!(engine.progress_batch(id).unwrap().completed_tasks, 0);
            barrier.wait();
            let until = Instant::now() + t;
            loop {
                match engine.progress_batch(id) {
                    Err(Error::BatchClosed) => break,
                    Ok(status) => assert!(!status.locally_drained),
                    Err(other) => panic!("{other}"),
                }
                assert!(Instant::now() < until);
                thread::yield_now();
            }
        };
        scope.spawn(poll);
        scope.spawn(poll);
        let allocator = scope.spawn(|| {
            barrier.wait();
            let mut releases = Vec::new();
            for _ in 0..20 {
                let mut other = engine.allocate_batch(1).unwrap();
                releases.push(other.allocation_release_receipt());
                other.free().unwrap();
            }
            releases
        });
        scope.spawn(|| {
            barrier.wait();
            batch.free().unwrap();
        });
        allocator.join().unwrap()
    });
    // A worker may still hold a lookup for an empty, now-fenced batch. Wait
    // for each distinct allocation debit to leave the ledger. The TCP reply
    // is still withheld, so its deferred batch must remain fully charged.
    for release in releases {
        release.recv_timeout(t).unwrap();
    }
    let held = engine.stats().unwrap();
    assert_eq!(
        (
            held.allocated_batches,
            held.deferred_batches,
            held.inflight_batches,
            held.inflight_bytes
        ),
        (1, 1, 1, 8)
    );
    assert_eq!(held.runtime_queue.unwrap().dispatching_owners, 1);
    assert!(fi.owner.upgrade().is_some());
    stream
        .write_all(
            &Reply {
                status: 0,
                epoch,
                length: 8,
            }
            .encode(),
        )
        .unwrap();
    stream.write_all(&[37; 8]).unwrap();
    let until = Instant::now() + t;
    while engine.stats().unwrap().allocated_batches != 0 {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    assert_eq!(fi.drops.load(Ordering::SeqCst), 1);
    assert_eq!(engine.stats().unwrap().deferred_batches, 0);
    assert_eq!(engine.stats().unwrap().quarantined_batches, 0);
    assert_eq!(engine.stats().unwrap().inflight_bytes, 0);
}

#[test]
fn background_progress_publishes_a_lost_write_receipt_without_replay() {
    let timeout = common::EVENTUAL_PROGRESS;
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            enable_progress_worker: true,
            max_receiver_credit_sessions: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    let (listener, mut peer) = fault_peer();
    engine.bind_test_peer(&mut peer).unwrap();

    engine.activate_receiver_credit(&peer, 1, 0).unwrap();
    engine
        .apply_receiver_credit(
            &peer,
            &crate::ReceiverCreditUpdateV1 {
                schema_version: 1,
                flags: 0,
                receiver_session_id: peer.epoch,
                qos_class: 0,
                epoch: 1,
                sequence: 1,
                freshness_ttl_ms: 0,
                grants: vec![
                    crate::CreditAmount {
                        resource: crate::CreditResource::DataBytes,
                        grant_total: 8,
                    },
                    crate::CreditAmount {
                        resource: crate::CreditResource::RequestSlots,
                        grant_total: 1,
                    },
                ],
            },
        )
        .unwrap();
    let local = engine
        .register_memory(
            vec![0xec, 1, 2, 3, 4, 5, 6, 7, 8, 0xed].into(),
            Permission::LocalOnly,
        )
        .unwrap();
    let request = TransferRequest::new(
        crate::Transport::Tcp,
        local.base_address() + 1,
        peer.regions[0].base_address(),
        8,
    );
    let mut b = engine
        .submit_async(TransferOp::Write, &peer, &[request], None, timeout)
        .unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(timeout)).unwrap();
    let mut header = [0; REQUEST_SIZE];
    stream.read_exact(&mut header).unwrap();
    assert!(Request::decode(&header).unwrap().operation == Operation::TcpWrite);
    stream
        .write_all(
            &Reply {
                status: 9,
                epoch: peer.epoch,
                length: 0,
            }
            .encode(),
        )
        .unwrap();
    let mut dest = [0xce; 10];
    stream.read_exact(&mut dest[1..9]).unwrap();
    assert_eq!(dest, [0xce, 1, 2, 3, 4, 5, 6, 7, 8, 0xce]);
    drop(stream);
    // No status or progress call is needed to publish the physical failure.
    let status = b.wait(timeout).unwrap();
    assert_eq!(status.state, crate::BatchState::Failed);
    assert_eq!(
        b.submissions().unwrap()[0]
            .failure
            .as_ref()
            .unwrap()
            .remote_effect,
        RemoteEffect::Unknown
    );
    for _ in 0..4 {
        assert_eq!(engine.progress_batch(b.id().unwrap()).unwrap(), status);
    }
    listener.set_nonblocking(true).unwrap();
    assert!(matches!(listener.accept(),Err(e) if e.kind()==io::ErrorKind::WouldBlock));
    b.free().unwrap();
    let stats = engine.receiver_credit_stats(&peer).unwrap();
    assert_eq!(
        (
            stats.active_owners,
            stats.completed_bytes,
            stats.consumed_bytes
        ),
        (0, 0, 8)
    );
    assert!(matches!(
        engine.deactivate_receiver_credit(&peer, 1),
        Err(Error::BatchBusy)
    ));
    assert!(matches!(
        engine.activate_receiver_credit(&peer, 2, 0),
        Err(Error::BatchBusy)
    ));
    assert_eq!(
        engine
            .receiver_credit_stats(&peer)
            .unwrap()
            .available_requests,
        0
    );
    let mut source = [0; 10];
    local.read(0, &mut source).unwrap();
    assert_eq!(source, [0xec, 1, 2, 3, 4, 5, 6, 7, 8, 0xed]);
}

#[test]
fn free_fences_hook_retries_without_joining_a_peer_that_has_not_replied() {
    use crate::{BatchState, NotificationStatus, Transport};
    let timeout = common::EVENTUAL_PROGRESS;
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            enable_progress_worker: true,
            ..Default::default()
        })
        .unwrap(),
    );
    let target = engine
        .register_memory(vec![11; 8].into(), Permission::RemoteRead)
        .unwrap();
    let local = engine
        .register_memory(vec![0; 8].into(), Permission::LocalOnly)
        .unwrap();
    let own = engine
        .open_segment(engine.endpoint(), engine.source_epoch(), timeout)
        .unwrap();
    let (listener, mut peer) = fault_peer();
    engine.bind_test_peer(&mut peer).unwrap();

    let req = [TransferRequest::new(
        Transport::Tcp,
        local.base_address(),
        target.base_address(),
        8,
    )];
    let mut b = engine.allocate_batch(1).unwrap();
    b.submit_with_notification_targets(
        TransferOp::Read,
        &own,
        &req,
        &Notification {
            name: "n".into(),
            message: "m".into(),
        },
        std::slice::from_ref(&peer),
        timeout,
    )
    .unwrap();
    b.seal().unwrap();
    let accept = || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(timeout)).unwrap();
        let mut request = [0; REQUEST_SIZE];
        stream.read_exact(&mut request).unwrap();
        assert!(Request::decode(&request).unwrap().operation == Operation::Notify);
        stream
    };
    let reply = |status| {
        Reply {
            status,
            epoch: peer.epoch,
            length: 0,
        }
        .encode()
    };
    accept().write_all(&reply(6)).unwrap();
    let done = b.wait(timeout).unwrap();
    assert_eq!(done.state, BatchState::Completed);
    assert!(matches!(
        *b.submissions().unwrap()[0].notification,
        NotificationStatus::Failed(_)
    ));
    let id = b.id().unwrap();
    engine.progress_batch(id).unwrap();
    let mut held = accept();
    // A pending retry has no right to change the already published data status.
    assert_eq!(b.status().unwrap(), done);
    b.free().unwrap();
    assert!(matches!(engine.progress_batch(id), Err(Error::BatchClosed)));
    assert_eq!(engine.stats().unwrap().deferred_batches, 1);
    assert_eq!(engine.stats().unwrap().inflight_batches, 1);
    assert!(matches!(
        engine.close_requester(Duration::ZERO),
        Err(Error::Timeout)
    ));
    held.write_all(&reply(9)).unwrap();
    let mut body = [0; 3];
    held.read_exact(&mut body).unwrap();
    assert_eq!(&body, b"nm\0");
    held.write_all(&reply(0)).unwrap();
    engine.close_requester(timeout).unwrap();
    let end = Instant::now() + timeout;
    while engine.stats().unwrap().allocated_batches != 0 {
        assert!(Instant::now() < end);
        thread::yield_now();
    }
    listener.set_nonblocking(true).unwrap();
    assert!(matches!(listener.accept(),Err(error) if error.kind()==io::ErrorKind::WouldBlock));
}

#[test]
fn one_tcp_transfer_carries_shuffled_aliases_and_owns_exact_qos_and_credit() {
    use crate::{QosConfig, QosPolicyFields, QosUsage, RuntimeQueueConfig, TaskState, Transport};
    let t = common::EVENTUAL_PROGRESS;
    let (listener, mut peer) = fault_peer();
    peer.metadata.max_transfer_bytes = 8;
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            runtime_queue: Some(RuntimeQueueConfig {
                max_outstanding_owners: 1,
                max_dispatch_owners: 1,
                ..Default::default()
            }),
            max_receiver_credit_sessions: 1,
            qos: Some(QosConfig {
                defaults: QosPolicyFields {
                    max_inflight_requests: Some(1),
                    max_inflight_bytes: Some(4),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap(),
    );
    engine.bind_test_peer(&mut peer).unwrap();
    let local = engine
        .register_memory(vec![0x91, 1, 2, 3, 4, 0x92].into(), Permission::LocalOnly)
        .unwrap();
    engine.activate_receiver_credit(&peer, 1, 0).unwrap();
    engine
        .apply_receiver_credit(
            &peer,
            &crate::ReceiverCreditUpdateV1 {
                schema_version: 1,
                flags: 0,
                receiver_session_id: peer.epoch,
                qos_class: 0,
                epoch: 1,
                sequence: 1,
                freshness_ttl_ms: 0,
                grants: vec![
                    crate::CreditAmount {
                        resource: crate::CreditResource::DataBytes,
                        grant_total: 4,
                    },
                    crate::CreditAmount {
                        resource: crate::CreditResource::RequestSlots,
                        grant_total: 1,
                    },
                ],
            },
        )
        .unwrap();
    let q = engine.resolve_qos(&Default::default()).unwrap();
    let req = [3, 1].map(|i| {
        TransferRequest::new(
            Transport::Tcp,
            local.base_address() + i,
            peer.regions[0].base_address() + i,
            2,
        )
        .with_qos(q.clone())
    });
    let mut batch = engine
        .submit_async(TransferOp::Write, &peer, &req, None, t)
        .unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(t)).unwrap();
    let mut header = [0; REQUEST_SIZE];
    stream.read_exact(&mut header).unwrap();
    let header = Request::decode(&header).unwrap();
    assert!(header.operation == Operation::TcpWrite);
    assert_eq!((header.region, header.offset, header.length), (1, 1, 4));
    assert_eq!(
        q.usage().unwrap(),
        QosUsage {
            owners: 1,
            inflight_requests: 1,
            inflight_bytes: 4
        }
    );
    let usage = engine.stats().unwrap();
    assert_eq!(usage.inflight_bytes, 4);
    let queue = usage.runtime_queue.unwrap();
    assert_eq!((queue.dispatching_owners, queue.dispatching_bytes), (1, 4));
    let credits = engine.receiver_credit_stats(&peer).unwrap();
    #[cfg(feature = "metrics")]
    {
        let j = engine.metrics().unwrap().json();
        assert_eq!(j["orbite_qos_owners"], 1);
        assert_eq!(j["orbite_qos_inflight_requests"], 1);
        assert_eq!(j["orbite_qos_inflight_bytes"], 4);
        assert_eq!(j["orbite_dispatching_owners"], 1);
        assert_eq!(
            j["orbite_receiver_credit_active_owners"],
            credits.active_owners
        );
        assert_eq!(
            j["orbite_receiver_credit_reserved_owners"],
            credits.reserved_owners
        );
        assert_eq!(j["tent_inflight_attempts"], 1);
    }
    assert_eq!(
        (
            credits.active_owners,
            credits.consumed_requests,
            credits.consumed_bytes
        ),
        (1, 1, 4)
    );
    assert!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .all(|t| t.physical_owner == 0 && t.state == TaskState::Running)
    );
    assert!(matches!(batch.cancel(1), Err(Error::Unavailable(_))));
    assert!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .all(|t| !t.cancel_requested)
    );
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    stream
        .write_all(
            &Reply {
                status: 9,
                epoch: peer.epoch,
                length: 0,
            }
            .encode(),
        )
        .unwrap();
    let mut bytes = [0xaa; 8];
    stream.read_exact(&mut bytes[1..5]).unwrap();
    assert_eq!(bytes, [0xaa, 1, 2, 3, 4, 0xaa, 0xaa, 0xaa]);
    stream
        .write_all(
            &Reply {
                status: 0,
                epoch: peer.epoch,
                length: 0,
            }
            .encode(),
        )
        .unwrap();
    assert_eq!(batch.wait(t).unwrap().completed_tasks, 2);
    assert!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .all(|t| t.transferred_bytes == 2
                && t.state == TaskState::Completed
                && t.locally_drained)
    );
    assert_eq!(batch.submissions().unwrap()[0].completed_requests, 2);
    assert_eq!(q.usage().unwrap(), QosUsage::default());
    batch.free().unwrap();
    listener.set_nonblocking(true).unwrap();
    assert!(matches!(listener.accept(),Err(e) if e.kind()==io::ErrorKind::WouldBlock));
}

#[test]
fn shuffled_owners_keep_the_public_failure_prefix_and_do_not_replay_a_write() {
    let timeout = common::EVENTUAL_PROGRESS;
    for asynchronous in [false, true] {
        let (listener, mut peer) = fault_peer();
        peer.metadata.max_transfer_bytes = 8;
        let engine = Arc::new(TransferEngine::new(Default::default()).unwrap());
        engine.bind_test_peer(&mut peer).unwrap();
        let local = engine
            .register_memory(vec![1, 2, 3, 4, 5, 6, 7, 8].into(), Permission::LocalOnly)
            .unwrap();
        let req = [0, 5, 2].map(|i| {
            TransferRequest::new(
                crate::Transport::Tcp,
                local.base_address() + i,
                peer.regions[0].base_address() + i,
                2,
            )
        });
        let epoch = peer.epoch;
        let handle = thread::spawn(move || {
            for (off, len) in [(0, 4), (5, 2)] {
                let (mut s, _) = listener.accept().unwrap();
                s.set_read_timeout(Some(timeout)).unwrap();
                let mut h = [0; REQUEST_SIZE];
                s.read_exact(&mut h).unwrap();
                let r = Request::decode(&h).unwrap();
                assert_eq!((r.offset, r.length), (off, len));
                s.write_all(
                    &Reply {
                        status: 9,
                        epoch,
                        length: 0,
                    }
                    .encode(),
                )
                .unwrap();
                let mut bytes = vec![0; len as usize];
                s.read_exact(&mut bytes).unwrap();
                if off == 0 {
                    assert_eq!(bytes, [1, 2, 3, 4]);
                    s.write_all(
                        &Reply {
                            status: 0,
                            epoch,
                            length: 0,
                        }
                        .encode(),
                    )
                    .unwrap();
                } else {
                    assert_eq!(bytes, [6, 7]);
                } // second WRITE loses its final receipt
            }
            listener
        });
        let (completed, bytes, index, span, effect) = if asynchronous {
            let mut b = engine
                .submit_async(TransferOp::Write, &peer, &req, None, timeout)
                .unwrap();
            assert_eq!(b.wait(timeout).unwrap().completed_tasks, 2);
            let tasks = b.task_statuses().unwrap();
            assert_eq!(
                (
                    tasks[0].physical_owner,
                    tasks[1].physical_owner,
                    tasks[2].physical_owner
                ),
                (0, 1, 0)
            );
            assert_eq!(tasks[2].transferred_bytes, 2);
            let s = b.submissions().unwrap().remove(0);
            let f = s.failure.unwrap();
            b.free().unwrap();
            (
                s.completed_requests,
                s.transferred_bytes,
                f.request_index,
                f.affected_requests,
                f.remote_effect,
            )
        } else {
            let r = engine
                .submit_and_wait(TransferOp::Write, &peer, &req, timeout)
                .unwrap();
            let f = r.failure.unwrap();
            (
                r.completed_requests,
                r.transferred_bytes,
                f.request_index,
                f.affected_requests,
                f.remote_effect,
            )
        };
        assert_eq!(
            (completed, bytes, index, span, effect),
            (1, 2, 1, 2, RemoteEffect::Unknown)
        );
        let l = handle.join().unwrap();
        l.set_nonblocking(true).unwrap();
        assert!(matches!(l.accept(),Err(e) if e.kind()==io::ErrorKind::WouldBlock));
    }
}

#[test]
fn describe_retries_only_read_only_io_on_a_fresh_connection_with_the_same_epoch() {
    for mode in ["healthy", "reset", "twice", "malformed", "wrong_epoch"] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let epoch = SourceEpoch::generate().unwrap();
        let expected = if matches!(mode, "reset" | "twice") {
            2
        } else {
            1
        };
        let worker = thread::spawn(move || {
            for i in 0..expected {
                let (mut c, _) = listener.accept().unwrap();
                let mut header = [0; REQUEST_SIZE];
                c.read_exact(&mut header).unwrap();
                let r = Request::decode(&header).unwrap();
                assert!(r.operation == Operation::Describe && r.epoch == epoch);
                if mode == "twice" || (mode == "reset" && i == 0) {
                    continue;
                }
                let reply = Reply {
                    status: 0,
                    epoch: if mode == "wrong_epoch" {
                        SourceEpoch::generate().unwrap()
                    } else {
                        epoch
                    },
                    length: if mode == "malformed" {
                        1
                    } else {
                        DESCRIPTOR_HEADER as u64
                    },
                };
                c.write_all(&reply.encode()).unwrap();
                if mode != "malformed" && mode != "wrong_epoch" {
                    c.write_all(&[0; DESCRIPTOR_HEADER]).unwrap();
                }
            }
            listener.set_nonblocking(true).unwrap();
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        });
        let result = open_segment(endpoint, epoch, deadline(TIMEOUT).unwrap());
        match mode {
            "wrong_epoch" => assert!(matches!(result, Err(Error::EpochMismatch))),
            "malformed" => assert!(matches!(result, Err(Error::Protocol(_)))),
            "twice" => assert!(matches!(result, Err(Error::Io(_)))),
            _ => assert_eq!(result.unwrap().source_epoch(), epoch),
        }
        worker.join().unwrap();
    }
}

#[test]
fn accept_readiness_preserves_bounded_handoff_and_serves_the_next_connection() {
    let timeout = common::EVENTUAL_PROGRESS;
    let engine = TransferEngine::new(EngineConfig {
        handler_threads: 1,
        handler_timeout: timeout,
        ..Default::default()
    })
    .unwrap();
    let memory = engine
        .register_memory(vec![0xea, 1, 2, 0xeb].into(), Permission::RemoteReadWrite)
        .unwrap();
    let epoch = engine.source_epoch();

    // An acknowledged but incomplete WRITE holds the sole handler. One READ
    // fits in the handoff queue and the next connection must be dropped.
    let mut write = wire_request(&engine, Operation::TcpWrite, memory.id(), 1, 2);
    write.deadline = deadline(timeout).unwrap();
    assert_eq!(Reply::read(&mut write, epoch).unwrap().status, 9);
    let mut queued = wire_request(&engine, Operation::TcpRead, memory.id(), 0, 4);
    queued.deadline = deadline(timeout).unwrap();
    let mut excess = TcpStream::connect_timeout(&engine.endpoint(), timeout).unwrap();
    excess.set_read_timeout(Some(timeout)).unwrap();
    match excess.read(&mut [0]) {
        Ok(0) => {}
        Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {}
        other => panic!("full control queue did not close the excess socket: {other:?}"),
    }
    write.write_all(&[3, 4]).unwrap();
    assert_eq!(Reply::read(&mut write, epoch).unwrap().status, 0);
    let expected = [0xea, 3, 4, 0xeb];
    let reply = Reply::read(&mut queued, epoch).unwrap();
    assert_eq!((reply.status, reply.length), (0, 4));
    let mut got = [0; 4];
    queued.read_exact(&mut got).unwrap();
    assert_eq!(got, expected);
    let mut next = wire_request(&engine, Operation::TcpRead, memory.id(), 0, 4);
    let reply = Reply::read(&mut next, epoch).unwrap();
    assert_eq!((reply.status, reply.length), (0, 4));
    next.read_exact(&mut got).unwrap();
    assert_eq!(got, expected);
    memory.read(0, &mut got).unwrap();
    assert_eq!(got, expected);
}

#[path = "control_plane/peer_retirement.rs"]
mod peer_retirement;

#[cfg(target_os = "linux")]
#[test]
fn p10_killed_tcp_requester_retains_partial_write_until_socket_drain() {
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    let memory = engine
        .register_memory(vec![0xa5; 18].into(), Permission::RemoteReadWrite)
        .unwrap();
    let frame = Request {
        operation: Operation::TcpWrite,
        epoch: engine.source_epoch(),
        region: memory.id(),
        offset: 2,
        length: 8,
    }
    .encode();
    let mut peer = killed_peer::KilledPeer::start(engine.endpoint(), &frame, REPLY_SIZE);
    engine.unregister_memory(&memory).unwrap();
    assert!(matches!(
        engine.close_source(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(engine.stats().unwrap().retained_bytes, 18);
    peer.kill();
    engine.close_source(common::EVENTUAL_PROGRESS).unwrap();
    let mut got = [0; 18];
    memory.read(0, &mut got).unwrap();
    assert_eq!(got, [0xa5; 18]);
    assert_eq!(engine.stats().unwrap().active_source_operations, 0);
    drop(memory);
    assert_eq!(engine.stats().unwrap().retained_bytes, 0);
}

#[cfg(target_os = "linux")]
#[path = "control_plane/killed_peer.rs"]
pub(crate) mod killed_peer;
