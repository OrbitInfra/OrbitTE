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
            let result = remote.query_delegate(&mut channel, 7, 2);
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
