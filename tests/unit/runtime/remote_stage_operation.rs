use super::*;
use std::net::TcpListener;
use std::time::{Duration, Instant};

#[test]
fn pending_and_terminal_query_results_require_the_entire_scope() {
    for fault in [
        "valid",
        "reply-nonce",
        "reply-length",
        "phase",
        "nonce",
        "region",
        "offset",
        "length",
        "result",
    ] {
        for ready in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = listener.local_addr().unwrap();
            let nonce = SourceEpoch([1; 16]);
            let server = std::thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                let mut channel = ControlChannel {
                    stream,
                    deadline: Instant::now() + Duration::from_secs(3),
                    stopping: None,
                    nonblocking: false,
                };
                let mut frame = read_request(&mut channel).unwrap();
                assert!(frame.operation == Operation::QueryDelegate && frame.epoch == nonce);
                assert_eq!((frame.region, frame.offset, frame.length), (3, 7, 2));
                match fault {
                    "phase" => frame.operation = Operation::Delegate,
                    "nonce" => frame.epoch = SourceEpoch([2; 16]),
                    "region" => frame.region += 1,
                    "offset" => frame.offset += 7,
                    "length" => frame.length += 1,
                    _ => (),
                }
                channel
                    .reply(
                        if fault == "reply-nonce" {
                            SourceEpoch([2; 16])
                        } else {
                            nonce
                        },
                        0,
                        if fault == "reply-length" { 52 } else { 53 },
                    )
                    .unwrap();
                let _ = channel.write_all(&frame.encode());
                let _ = channel.write_all(&[if fault == "result" {
                    2
                } else {
                    u8::from(ready)
                }]);
            });
            let mut channel =
                ControlChannel::connect(endpoint, Instant::now() + Duration::from_secs(3)).unwrap();
            let remote = RemoteStageOperation { nonce, region: 3 };
            let mut operation = remote.submit(Operation::QueryDelegate, 7, 2);
            let result = loop {
                let mut advanced = false;
                match operation.progress(&mut channel, &mut advanced) {
                    Ok(None) => std::thread::yield_now(),
                    Ok(Some(ready)) => break Ok(ready),
                    Err(error) => break Err(error),
                }
            };
            if fault == "valid" {
                assert_eq!(result.unwrap(), ready);
            } else {
                assert!(
                    matches!(result, Err(Error::Protocol(_) | Error::EpochMismatch)),
                    "{fault}/{ready}: {result:?}"
                );
            }
            server.join().unwrap();
        }
    }
}

#[test]
fn partial_scoped_receipt_never_completes_before_its_last_byte() {
    use std::io::{Read, Write};
    for operation in [
        Operation::Delegate,
        Operation::QueryDelegate,
        Operation::FinishCross,
        Operation::Unpin,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut channel = ControlChannel::connect(
            listener.local_addr().unwrap(),
            Instant::now() + Duration::from_secs(3),
        )
        .unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server.set_nodelay(true).unwrap();
        let remote = RemoteStageOperation {
            nonce: SourceEpoch([3; 16]),
            region: 7,
        };
        let mut pending = remote.submit(operation, 21, 3);
        assert_eq!(pending.progress(&mut channel, &mut false).unwrap(), None);
        assert!(pending.sent());
        let mut frame = [0; REQUEST_SIZE];
        server.read_exact(&mut frame).unwrap();
        let mut bytes = Reply {
            status: 0,
            epoch: remote.nonce,
            length: (REQUEST_SIZE + usize::from(operation == Operation::QueryDelegate)) as u64,
        }
        .encode()
        .to_vec();
        bytes.extend(frame);
        if operation == Operation::QueryDelegate {
            bytes.push(0);
        }
        for (index, byte) in bytes.iter().enumerate() {
            server.write_all(&[*byte]).unwrap();
            let result = pending.progress(&mut channel, &mut false).unwrap();
            if index + 1 != bytes.len() {
                assert_eq!(result, None);
            } else {
                let result = loop {
                    if let Some(result) = pending.progress(&mut channel, &mut false).unwrap() {
                        break result;
                    }
                };
                assert_eq!(result, operation != Operation::QueryDelegate);
            }
        }
    }
}
