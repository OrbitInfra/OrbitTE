use super::*;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[test]
fn connect_consumes_one_socket_and_preserves_typed_stop_during_pin_io() {
    for source_stop in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stop = AtomicBool::new(false);
        let reason = if source_stop {
            Stop::Source(&stop)
        } else {
            Stop::Task(&stop)
        };
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut connecting =
            Connecting::new(listener.local_addr().unwrap(), deadline, reason).unwrap();
        let mut channel = loop {
            if let Some(channel) = connecting.poll().unwrap() {
                break channel;
            }
            std::thread::yield_now();
        };
        assert!(connecting.socket.is_none());
        assert!(connecting.poll().is_err());
        assert!(channel.nonblocking);
        assert_eq!(channel.deadline, deadline);
        let (mut peer, _) = listener.accept().unwrap();
        std::thread::scope(|scope| {
            let (sent, reached) = std::sync::mpsc::channel();
            let worker = scope.spawn(move || {
                channel.write_all(&[0x53]).unwrap();
                sent.send(()).unwrap();
                let result = channel.read_exact(&mut [0; 18]);
                assert!(!channel.nonblocking);
                result
            });
            reached.recv_timeout(Duration::from_secs(1)).unwrap();
            let mut byte = [0];
            std::io::Read::read_exact(&mut peer, &mut byte).unwrap();
            assert_eq!(byte, [0x53]);
            stop.store(true, Ordering::Release);
            let error = worker.join().unwrap().unwrap_err();
            assert!(if source_stop {
                matches!(error, Error::SourceClosed)
            } else {
                matches!(error, Error::Cancelled)
            });
        });
        peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        assert_eq!(std::io::Read::read(&mut peer, &mut [0]).unwrap(), 0);
    }
}

#[test]
fn refused_connect_is_terminal_and_pre_cancel_never_connects() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap();
    let stop = AtomicBool::new(true);
    assert!(matches!(
        Connecting::new(
            endpoint,
            Instant::now() + Duration::from_secs(1),
            Stop::Task(&stop)
        ),
        Err(Error::Cancelled)
    ));
    listener.set_nonblocking(true).unwrap();
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(listener);
    stop.store(false, Ordering::Release);
    let error = match Connecting::new(
        endpoint,
        Instant::now() + Duration::from_secs(1),
        Stop::Task(&stop),
    ) {
        Err(error) => error,
        Ok(mut connecting) => loop {
            match connecting.poll() {
                Ok(None) => std::thread::yield_now(),
                Ok(Some(_)) => panic!("refused endpoint connected"),
                Err(error) => {
                    assert!(connecting.socket.is_none());
                    break error;
                }
            }
        },
    };
    assert!(
        matches!(error, Error::Io(ref error) if error.kind() == io::ErrorKind::ConnectionRefused),
        "{error:?}"
    );
}

// A loopback listen queue with no remaining slot provides a real pending SYN
// without a remote host, network rule change or assumptions about routed IPs.
#[cfg(target_os = "linux")]
#[test]
fn saturated_listen_queue_keeps_one_pending_connect_until_cancel_or_deadline() {
    for cancel in [false, true] {
        let listener = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
        listener
            .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
            .unwrap();
        listener.listen(0).unwrap();
        let endpoint = listener.local_addr().unwrap().as_socket().unwrap();
        let occupied = TcpStream::connect_timeout(&endpoint, Duration::from_secs(1)).unwrap();
        let stop = AtomicBool::new(false);
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut connecting = Connecting::new(endpoint, deadline, Stop::Task(&stop)).unwrap();
        let address = connecting.socket.as_ref().unwrap().local_addr().unwrap();
        for _ in 0..16 {
            assert!(
                connecting.poll().unwrap().is_none(),
                "full accept queue unexpectedly connected"
            );
            assert_eq!(
                connecting.socket.as_ref().unwrap().local_addr().unwrap(),
                address
            );
        }
        if cancel {
            stop.store(true, Ordering::Release);
        }
        let error = loop {
            match connecting.poll() {
                Ok(None) => std::thread::park_timeout(Duration::from_millis(1)),
                Ok(Some(_)) => panic!("full accept queue unexpectedly connected"),
                Err(error) => break error,
            }
        };
        assert!(
            if cancel {
                matches!(error, Error::Cancelled)
            } else {
                matches!(error, Error::Timeout)
            },
            "{error:?}"
        );
        assert!(
            connecting.socket.is_none(),
            "terminal connection still owns its socket"
        );
        assert_eq!(connecting.deadline, deadline);
        drop(occupied);
    }
}
