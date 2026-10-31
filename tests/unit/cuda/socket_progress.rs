use std::io::{Read, Write, copy};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::Instant;

use super::requester_staging::engine;
use super::*;
use crate::RemoteEffect;
use crate::runtime::control_plane::{Operation, Request, u64_at};

struct SocketGate {
    endpoint: SocketAddr,
    reached: mpsc::Receiver<()>,
    closed: mpsc::Receiver<()>,
    release: Option<mpsc::Sender<()>>,
    relay: Option<std::thread::JoinHandle<()>>,
}

impl SocketGate {
    fn new(target: SocketAddr, operation: TransferOp, payload: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (entered, reached) = mpsc::channel();
        let (gone, closed) = mpsc::channel();
        let (release, resumed) = mpsc::channel();
        let relay = std::thread::spawn(move || {
            let (mut client, _) = listener.accept().unwrap();
            let mut server = TcpStream::connect(target).unwrap();
            client.set_nodelay(true).unwrap();
            server.set_nodelay(true).unwrap();
            server
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut input = client.try_clone().unwrap();
            let mut output = server.try_clone().unwrap();
            let upstream = std::thread::spawn(move || {
                let _ = copy(&mut input, &mut output);
                let _ = gone.send(());
            });
            let mut pin = [0; 54];
            server.read_exact(&mut pin).unwrap();
            client.write_all(&pin).unwrap();
            let mut payloads = 0;
            loop {
                let mut header = [0; 36];
                server.read_exact(&mut header).unwrap();
                let length = u64_at(&header, 28) as usize;
                assert!(length <= 53);
                let mut body = vec![0; length];
                server.read_exact(&mut body).unwrap();
                let selected = if payload && length == 3 {
                    payloads += 1;
                    payloads == 2
                } else if !payload && length == 52 {
                    let frame = Request::decode(body[..52].try_into().unwrap()).unwrap();
                    frame.operation == Operation::Delegate
                        && frame.offset == if operation == TransferOp::Read { 6 } else { 0 }
                } else {
                    false
                };
                if selected {
                    if payload {
                        client.write_all(&header).unwrap();
                        client.write_all(&body[..1]).unwrap();
                    } else {
                        client.write_all(&header[..13]).unwrap();
                    }
                    entered.send(()).unwrap();
                    let _ = resumed.recv();
                    break;
                }
                client.write_all(&header).unwrap();
                client.write_all(&body).unwrap();
            }
            let _ = client.shutdown(Shutdown::Both);
            let _ = server.shutdown(Shutdown::Both);
            upstream.join().unwrap();
        });
        Self {
            endpoint,
            reached,
            closed,
            release: Some(release),
            relay: Some(relay),
        }
    }
}

impl Drop for SocketGate {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(relay) = self.relay.take() {
            let _ = relay.join();
        }
    }
}

#[test]
fn local_failure_joins_partial_tcp_or_control_io_before_peer_release() {
    for (operation, payload) in [
        (TransferOp::Read, false),
        (TransferOp::Read, true),
        (TransferOp::Write, false),
    ] {
        let source = engine(Transport::Tcp, 3, 2);
        let requester = Arc::new(engine(Transport::Tcp, 3, 3));
        let a = allocation(&[0x57; 32]);
        let b = allocation(&[0xcc; 32]);
        let weak = Arc::downgrade(&b);
        let source_gpu = memory(&a);
        let local_gpu = memory(&b);
        let remote = source
            .register_cuda_memory_batch(&[(source_gpu.clone(), 0, 32)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 32)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let mut peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        let gate = SocketGate::new(source.endpoint(), operation, payload);
        peer.endpoint = gate.endpoint;
        let (entered, waiting) = mpsc::sync_channel(0);
        let (resume, blocked) = mpsc::sync_channel(0);
        if operation == TransferOp::Read {
            *b.copy_gate.lock().unwrap() = Some((entered, blocked));
        } else {
            b.query_hold_address
                .store(local.base_address() as usize + 5, Ordering::Release);
        }
        let mut batch = requester
            .submit_async(
                operation,
                &peer,
                &[TransferRequest {
                    remote_staging: true,
                    transport: Transport::Tcp,
                    local_address: local.base_address() + 2,
                    remote_address: remote.base_address() + 1,
                    length: 17,
                }],
                None,
                Duration::from_secs(10),
            )
            .unwrap();
        gate.reached.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        assert_eq!(requester.stats().unwrap().staging.active_tasks, 1);
        assert_eq!(source_gpu.allocation.lock().unwrap().dma, vec![1..18]);
        assert_eq!(local_gpu.allocation.lock().unwrap().dma, vec![2..19]);
        assert!(matches!(batch.free(), Err(Error::BatchBusy)));
        if operation == TransferOp::Read {
            waiting.recv_timeout(Duration::from_secs(3)).unwrap();
            b.copy_error.store(1, Ordering::Release);
            resume.send(()).unwrap();
        } else {
            let until = Instant::now() + Duration::from_secs(3);
            while b.event_not_ready.load(Ordering::Acquire) == 0 {
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
            b.pop_error.store(1, Ordering::Release);
        }
        // The peer still refuses the rest of its frame. Only cancellation of
        // the partial I/O can produce the worker's destruction acknowledgement.
        let report = batch
            .wait(Duration::from_secs(2))
            .expect("partial socket I/O did not yield to staging cancellation");
        assert_eq!(report.state, BatchState::Failed);
        assert_eq!(report.transferred_bytes, 0);
        assert_eq!(
            report.locally_drained,
            operation == TransferOp::Read,
            "{report:?}"
        );
        let failure = batch.submissions().unwrap().remove(0).failure.unwrap();
        assert!(!matches!(failure.error, Error::Timeout), "{failure:?}");
        assert_eq!(
            failure.remote_effect,
            if operation == TransferOp::Write {
                RemoteEffect::Unknown
            } else {
                RemoteEffect::None
            }
        );
        assert_eq!(requester.stats().unwrap().staging.active_tasks, 0);
        assert_eq!(requester.stats().unwrap().staging.cancelled_tasks, 1);
        gate.closed
            .recv_timeout(Duration::from_secs(1))
            .expect("worker retained its socket after terminal publication");
        assert_eq!(
            source.stats().unwrap().active_source_operations,
            1,
            "peer cleanup has not been released"
        );
        drop(gate);
        source.close(Duration::from_secs(3)).unwrap();
        let mut observed = [0; 32];
        source_gpu.read(0, &mut observed).unwrap();
        let mut expected = [0x57; 32];
        if operation == TransferOp::Write {
            expected[1..4].fill(0xcc);
        }
        assert_eq!(observed, expected);
        assert_eq!(source.stats().unwrap().staging.remote_unpins, 0);
        if operation == TransferOp::Read {
            b.copy_error.store(0, Ordering::Release);
            local_gpu.read(0, &mut observed).unwrap();
            let mut expected = [0xcc; 32];
            expected[2] = 0x57;
            assert_eq!(
                observed, expected,
                "the partially received next chunk cannot reach local POST"
            );
            assert!(local_gpu.allocation.lock().unwrap().dma.is_empty());
            batch.free().unwrap();
            requester.close(Duration::from_secs(3)).unwrap();
            assert!(b.pins.lock().unwrap().is_empty());
        } else {
            assert!(requester.stats().unwrap().staging.retained_chunks > 0);
            assert!(matches!(batch.free(), Err(Error::BatchBusy)));
            assert_eq!(unsafe { &**b.bytes.get() }, &[0xcc; 32]);
        }
        drop(batch);
        drop(local);
        drop(local_gpu);
        drop(requester);
        drop(b);
        assert_eq!(weak.upgrade().is_some(), operation == TransferOp::Write);
    }
}
