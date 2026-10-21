use std::net::Shutdown;

use super::*;
use crate::TransferEngine;

const TIMEOUT: Duration = Duration::from_secs(3);

fn wire_request(
    engine: &TransferEngine,
    operation: Operation,
    id: u64,
    offset: u64,
    length: u64,
) -> Channel<'static> {
    let mut channel = Channel::connect(engine.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
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
        operation: Operation::Read,
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
    for (index, value) in [(0, 0), (9, 2), (10, 255), (11, 1)] {
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
        (Operation::Write, memory.id(), 0, 4, 3),
        (Operation::Read, memory.id(), 7, 2, 4),
        (Operation::Read, memory.id(), u64::MAX, 2, 4),
        (Operation::Read, memory.id(), 0, 0, 4),
        (Operation::Read, memory.id(), 0, 9, 6),
        (Operation::Read, 999, 0, 4, 2),
    ] {
        let mut channel = wire_request(&engine, operation, id, offset, length);
        let mut bytes = [0; REPLY_SIZE];
        channel.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes[10], expected);
    }
    let mut channel = Channel::connect(engine.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    channel
        .write_all(
            &Request {
                operation: Operation::Read,
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
    let mut channel = Channel::connect(engine.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
    let request = Request {
        operation: Operation::Write,
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
    let mut channel = wire_request(&engine, Operation::Write, memory.id(), 0, 8);
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
    let mut _idle = wire_request(&engine, Operation::Write, memory.id(), 0, 8);
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
            length: 8,
            permission: Permission::RemoteReadWrite,
        }],
    };
    (listener, peer)
}

fn fault_request(engine: &TransferEngine, peer: &RemoteSegment) -> TransferRequest {
    TransferRequest {
        local: engine
            .register_memory(vec![7; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap(),
        local_offset: 0,
        remote: peer.regions[0],
        remote_offset: 0,
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
            let mut channel = Channel {
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
        request.local.read(0, &mut bytes).unwrap();
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
        let mut channel = Channel {
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
    request.local.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7; 8]);
    handler.join().unwrap();
}
