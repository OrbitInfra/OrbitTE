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
fn request_fixture_and_decoder_reject_protocol_drift() {
    let request = Request {
        operation: Operation::TcpRead,
        epoch: SourceEpoch([0x11; 16]),
        region: 1,
        offset: 2,
        length: 3,
    };
    let mut expected = Vec::from(&b"ORBITTE\0\0\x0b\x01\0"[..]);
    expected.extend([0x11; 16]);
    expected.extend([0, 0, 0, 0, 0, 0, 0, 1]);
    expected.extend([0, 0, 0, 0, 0, 0, 0, 2]);
    expected.extend([0, 0, 0, 0, 0, 0, 0, 3]);
    assert_eq!(request.encode().as_slice(), expected);
    for (index, value) in [
        (0, 0),
        (9, 1),
        (9, 2),
        (9, 3),
        (9, 4),
        (9, 5),
        (9, 6),
        (9, 7),
        (9, 8),
        (9, 9),
        (9, 10),
        (10, 255),
        (11, 1),
    ] {
        let mut malformed = request.encode();
        malformed[index] = value;
        assert!(Request::decode(&malformed).is_err());
    }
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
        endpoint: listener.local_addr().unwrap(),
        epoch,
        regions: vec![RemoteRegion {
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
    TransferRequest {
        transport: crate::Transport::Tcp,
        local_address: engine
            .register_memory(vec![7; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap()
            .base_address(),
        remote_address: peer.regions[0].base_address(),
        length: 8,
    }
}

#[test]
fn partial_read_preserves_destination_and_lost_write_ack_is_ambiguous() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let (listener, peer) = fault_peer();
        let epoch = peer.epoch;
        let handler = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
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
    let (listener, peer) = fault_peer();
    let epoch = peer.epoch;
    let handler = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut channel = ControlChannel {
            stream,
            deadline: deadline(TIMEOUT).unwrap(),
            stopping: None,
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
    for case in 0..7 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let epoch = SourceEpoch::generate().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
            };
            channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
            let count = if case == 5 { 2 } else { 1 };
            let mut payload = vec![0; 4 + count * REGION_SIZE];
            payload[..4].copy_from_slice(&(count as u32).to_be_bytes());
            for entry in payload[4..].chunks_exact_mut(REGION_SIZE) {
                entry[..8].copy_from_slice(&1u64.to_be_bytes());
                entry[8..16].copy_from_slice(&0x1000u64.to_be_bytes());
                entry[16..24].copy_from_slice(&8u64.to_be_bytes());
                entry[24] = 3;
            }
            match case {
                0 => payload[12..20].copy_from_slice(&0u64.to_be_bytes()),
                1 => payload[12..20].copy_from_slice(&(u64::MAX - 2).to_be_bytes()),
                2 => payload[20..28].copy_from_slice(&0u64.to_be_bytes()),
                3 => payload[28] = 7,
                4 => payload[29] = 1,
                _ => {}
            }
            let mut reply = Reply {
                status: 0,
                epoch,
                length: payload.len() as u64,
            }
            .encode();
            if case == 6 {
                reply[8..10].copy_from_slice(&1u16.to_be_bytes());
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
        let (listener, peer) = fault_peer();
        let epoch = peer.epoch;
        let handler = thread::spawn(move || {
            if attached {
                let (stream, _) = listener.accept().unwrap();
                let mut channel = ControlChannel {
                    stream,
                    deadline: deadline(TIMEOUT).unwrap(),
                    stopping: None,
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

    let (listener, peer) = fault_peer();
    let epoch = peer.epoch;
    let (admitted_tx, admitted_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handler = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut channel = ControlChannel {
            stream,
            deadline: deadline(TIMEOUT).unwrap(),
            stopping: None,
        };
        channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
        admitted_tx.send(()).unwrap();
        release_rx.recv_timeout(TIMEOUT).unwrap();
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
    let fixture = common::external_memory(&[0; 8]);
    let local = engine
        .register_host_memory(Arc::clone(&fixture.memory), 0, 8, Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest {
        transport: crate::Transport::Tcp,
        local_address: local.base_address(),
        remote_address: peer.regions[0].base_address(),
        length: 8,
    };
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
    admitted_rx.recv_timeout(TIMEOUT).unwrap();
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
        let (listener, peer) = fault_peer();
        let epoch = peer.epoch;
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let handler = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut channel = ControlChannel {
                stream,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
            };
            channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
            admitted_tx.send(()).unwrap();
            release_rx.recv_timeout(TIMEOUT).unwrap();
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
        let fixture = common::external_memory(&[0; 8]);
        let local = engine
            .register_host_memory(Arc::clone(&fixture.memory), 0, 8, Permission::LocalOnly)
            .unwrap();
        let request = TransferRequest {
            transport: crate::Transport::Tcp,
            local_address: local.base_address(),
            remote_address: peer.regions[0].base_address(),
            length: 8,
        };
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
        admitted_rx.recv_timeout(TIMEOUT).unwrap();
        let queued_timeout = if queued_cancel {
            TIMEOUT
        } else {
            Duration::from_millis(20)
        };
        batch
            .submit(TransferOp::Read, &peer, &[request], None, queued_timeout)
            .unwrap();
        if queued_cancel {
            assert!(batch.cancel(0).unwrap());
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
        assert_eq!(tasks[0].cancel_requested, queued_cancel);
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

    let (listener, peer) = fault_peer();
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
            };
            channel.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
            if index == 0 {
                admitted_tx.send(()).unwrap();
                release_rx.recv_timeout(TIMEOUT).unwrap();
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
    let weak_engine = Arc::downgrade(&engine);
    let fixture = common::external_memory(&[0; 8]);
    let local = engine
        .register_host_memory(Arc::clone(&fixture.memory), 0, 8, Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest {
        transport: crate::Transport::Tcp,
        local_address: local.base_address(),
        remote_address: peer.regions[0].base_address(),
        length: 8,
    };
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
    admitted_rx.recv_timeout(TIMEOUT).unwrap();
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
    let end = Instant::now() + TIMEOUT;
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
