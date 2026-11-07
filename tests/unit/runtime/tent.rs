use super::*;
use crate::{
    EngineConfig, Permission, TransferEngine, TransferRequest,
    test_support::EVENTUAL_PROGRESS as WAIT,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::thread;

fn engine() -> TransferEngine {
    TransferEngine::new(EngineConfig {
        tent_bind_addr: Some("127.0.0.1:0".parse().unwrap()),
        ..Default::default()
    })
    .unwrap()
}
fn call(endpoint: SocketAddr, function: i32, payload: &[u8]) -> Result<Vec<u8>> {
    let mut c = ControlChannel::connect(endpoint, Instant::now() + WAIT)?;
    // Exercise fragmented headers and struct_pack body on the real listener.
    for byte in wire::request(function, payload.len())? {
        c.write_all(&[byte])?;
    }
    c.write_all(payload)?;
    response(&mut c, metadata::LIMIT)
}
fn data_bytes(address: u64, size: u64, data: &[u8]) -> Vec<u8> {
    let mut b = address.to_le_bytes().to_vec();
    b.extend(size.to_le_bytes());
    b.extend(data);
    b
}
fn contents(m: &crate::RegisteredMemory) -> Vec<u8> {
    let mut b = vec![0; m.len()];
    m.read(0, &mut b).unwrap();
    b
}

#[test]
fn explicit_listener_keeps_native_default_and_never_selects_tent_between_orbite_peers() {
    let source = engine();
    let requester = TransferEngine::new(Default::default()).unwrap();
    assert!(requester.tent_endpoint().is_none());
    let remote = source
        .register_memory(
            vec![0xd1, 1, 2, 3, 0xd2].into(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let local = requester
        .register_memory(vec![0xa1; 5].into(), Permission::LocalOnly)
        .unwrap();
    let native = requester
        .open_segment(source.endpoint(), source.source_epoch(), WAIT)
        .unwrap();
    assert!(native.metadata.tent.is_none());
    let req = TransferRequest::new(
        Transport::Tcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        3,
    );
    assert!(
        requester
            .submit_and_wait(TransferOp::Read, &native, &[req], WAIT)
            .unwrap()
            .is_complete()
    );
    assert_eq!(contents(&local), [0xa1, 1, 2, 3, 0xa1]);
    assert!(matches!(
        requester.open_tent_segment(source.tent_endpoint().unwrap(), source.source_epoch(), WAIT),
        Err(Error::Unavailable(_))
    ));
    // No auto-detect on either listener, and no change to the native epoch check.
    assert!(
        requester
            .open_segment(source.tent_endpoint().unwrap(), source.source_epoch(), WAIT)
            .is_err()
    );
    assert!(
        requester
            .open_tent_segment(source.endpoint(), source.source_epoch(), WAIT)
            .is_err()
    );
    assert!(matches!(
        requester.open_segment(source.endpoint(), SourceEpoch::generate().unwrap(), WAIT),
        Err(Error::EpochMismatch)
    ));
}

#[test]
fn tent_source_checks_bytes_permissions_bounds_notifications_and_unsupported_rpc() {
    let source = engine();
    let endpoint = source.tent_endpoint().unwrap();
    let rw = source
        .register_memory(
            vec![0xf0, 1, 2, 3, 0xf1].into(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let ro = source
        .register_memory(vec![0xee; 5].into(), Permission::RemoteRead)
        .unwrap();
    let desc: serde_json::Value =
        serde_json::from_slice(&call(endpoint, DESCRIBE, &[]).unwrap()).unwrap();
    assert_eq!(desc["rpc_server_addr"], endpoint.to_string());
    assert_eq!(desc["detail"]["buffers"].as_array().unwrap().len(), 2);
    assert_eq!(
        call(endpoint, READ, &data_bytes(rw.base_address() + 1, 3, &[])).unwrap(),
        [1, 2, 3]
    );
    assert!(
        call(
            endpoint,
            WRITE,
            &data_bytes(rw.base_address() + 1, 3, &[9, 8, 7])
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(contents(&rw), [0xf0, 9, 8, 7, 0xf1]);
    assert!(
        !call(
            endpoint,
            WRITE,
            &data_bytes(ro.base_address() + 1, 3, &[0; 3])
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        !call(endpoint, READ, &data_bytes(u64::MAX, 3, &[]))
            .unwrap()
            .is_empty()
    );
    assert_eq!(contents(&ro), [0xee; 5]);
    assert!(
        call(endpoint, NOTIFY, br#"{"name":"x","msg":"a\"b"}"#)
            .unwrap()
            .is_empty()
    );
    let n = source.receive_notifications().unwrap();
    assert_eq!(n[0].message, "a\"b");
    for f in [7, 8, 9, 10, 11, 12, 99] {
        assert!(call(endpoint, f, &[]).is_err());
    }
    let b = serde_json::to_vec(&metadata::Bootstrap::default()).unwrap();
    let b: metadata::Bootstrap =
        serde_json::from_slice(&call(endpoint, BOOTSTRAP, &b).unwrap()).unwrap();
    assert!(!b.reply_msg.is_empty() && b.qp_num.is_empty());
}

#[test]
fn listener_rejects_bad_magic_version_and_oversized_attachment_without_touching_bytes() {
    let source = engine();
    let endpoint = source.tent_endpoint().unwrap();
    let m = source
        .register_memory(vec![0xaa; 8].into(), Permission::RemoteReadWrite)
        .unwrap();
    for at in [0, 1, 2, 8, 12] {
        let mut b = wire::request(WRITE, 0).unwrap();
        b[at] ^= 1;
        let mut c = ControlChannel::connect(endpoint, Instant::now() + WAIT).unwrap();
        c.write_all(&b).unwrap();
        assert!(response(&mut c, 64).is_err());
    }
    let mut c = ControlChannel::connect(endpoint, Instant::now() + WAIT).unwrap();
    c.write_all(&wire::request(READ, u32::MAX as usize).unwrap())
        .unwrap();
    assert!(response(&mut c, 64).is_err());
    assert_eq!(contents(&m), [0xaa; 8]);
    assert!(call(endpoint, PROBE, &[]).unwrap().is_empty());
}

// Fault-injection peer for requester owners, not a native interoperability claim.
// The native C++ engine in tests/reference/interop provides that separate proof.
struct State {
    bytes: Mutex<Vec<u8>>,
    generation: AtomicUsize,
    writes: AtomicUsize,
    lose_ack: AtomicBool,
    bad_response: AtomicUsize,
    short_read: AtomicBool,
    hold_read: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}
struct Peer {
    endpoint: SocketAddr,
    state: Arc<State>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Peer {
    fn new() -> Self {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = l.local_addr().unwrap();
        l.set_nonblocking(true).unwrap();
        let state = Arc::new(State {
            bytes: Mutex::new(vec![0xf1; 18]),
            generation: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            lose_ack: AtomicBool::new(false),
            bad_response: AtomicUsize::new(0),
            short_read: AtomicBool::new(false),
            hold_read: Mutex::new(None),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let st = state.clone();
        let quit = stop.clone();
        let thread = thread::spawn(move || {
            while !quit.load(Ordering::Acquire) {
                let (mut s, _) = match l.accept() {
                    Ok(s) => s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => panic!("{e}"),
                };
                s.set_nonblocking(false).unwrap();
                s.set_read_timeout(Some(WAIT)).unwrap();
                s.set_write_timeout(Some(WAIT)).unwrap();
                s.set_nodelay(true).unwrap();
                loop {
                    let mut h = [0; wire::REQUEST];
                    if s.read_exact(&mut h).is_err() {
                        break;
                    }
                    let (seq, n) = wire::request_header(&h, 1 << 20).unwrap();
                    let mut b = [0; wire::ARGUMENT];
                    s.read_exact(&mut b).unwrap();
                    let mut p = vec![0; n];
                    s.read_exact(&mut p).unwrap();
                    let reply=match wire::argument(&b).unwrap() {
                        DESCRIBE=>serde_json::to_vec(&serde_json::json!({"name":"frozen-peer","type":"memory","machine_id":format!("host-{}",st.generation.load(Ordering::Acquire)),"rpc_server_addr":endpoint.to_string(),"detail":{"devices":[],"device_attrs":{},"buffers":[{"addr":65536,"length":18,"location":"cpu:0","transports":[7],"regions":[],"rkey":[],"shm_path":"","mnnvl_handle":"","internal":false}]}})).unwrap(),
                        f @ (READ|WRITE)=>{
                            let address=u64::from_le_bytes(p[..8].try_into().unwrap()) as usize-65536;
                            let len=u64::from_le_bytes(p[8..16].try_into().unwrap()) as usize;
                            if f==WRITE { st.writes.fetch_add(1,Ordering::AcqRel);st.bytes.lock().unwrap()[address..address+len].copy_from_slice(&p[16..]); if st.lose_ack.load(Ordering::Acquire){break} Vec::new() }
                            else { if let Some((reached,release))=st.hold_read.lock().unwrap().take(){reached.send(()).unwrap();release.recv_timeout(WAIT).unwrap();} st.bytes.lock().unwrap()[address..address+len].to_vec() }
                        }, _=>panic!("unexpected request")
                    };
                    let mut h = wire::response(seq, reply.len()).unwrap();
                    let bad = st.bad_response.load(Ordering::Acquire);
                    if bad != 0 {
                        h[bad - 1] ^= 1;
                    }
                    if wire::argument(&b).unwrap() == READ && st.short_read.load(Ordering::Acquire)
                    {
                        s.write_all(&h).unwrap();
                        s.write_all(&wire::RETURN).unwrap();
                        s.write_all(&reply[..reply.len() / 2]).unwrap();
                        break;
                    }
                    if s.write_all(&h)
                        .and_then(|()| s.write_all(&wire::RETURN))
                        .and_then(|()| s.write_all(&reply))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });
        Self {
            endpoint,
            state,
            stop,
            thread: Some(thread),
        }
    }
    fn open(&self, e: &TransferEngine) -> RemoteSegment {
        e.open_tent_segment(self.endpoint, SourceEpoch::generate().unwrap(), WAIT)
            .unwrap()
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.thread.take().unwrap().join().unwrap();
    }
}
fn request(local: &crate::RegisteredMemory) -> TransferRequest {
    TransferRequest::new(Transport::Tcp, local.base_address() + 1, 65537, 16)
}

#[test]
fn changed_metadata_fences_writes_and_consumer_epoch_change_retires_old_identity() {
    let source = Peer::new();
    let e = TransferEngine::new(Default::default()).unwrap();
    let p = source.open(&e);
    let local = e
        .register_memory(vec![0xa2; 18].into(), Permission::LocalOnly)
        .unwrap();
    let first = e
        .submit_and_wait(TransferOp::Write, &p, &[request(&local)], WAIT)
        .unwrap();
    assert!(first.is_complete(), "{first:?}");
    source.state.generation.fetch_add(1, Ordering::AcqRel);
    let r = e
        .submit_and_wait(TransferOp::Write, &p, &[request(&local)], WAIT)
        .unwrap();
    assert!(matches!(r.failure.unwrap().error, Error::EpochMismatch));
    assert_eq!(source.state.writes.load(Ordering::Acquire), 1);
    assert!(matches!(
        e.submit_and_wait(TransferOp::Read, &p, &[request(&local)], WAIT),
        Err(Error::PeerClosed)
    ));
    e.close_segment(p.endpoint, p.epoch, WAIT).unwrap();
    let next = source.open(&e);
    assert_ne!(p.epoch, next.epoch);
    assert!(
        e.submit_and_wait(TransferOp::Read, &next, &[request(&local)], WAIT)
            .unwrap()
            .is_complete()
    );
    // Consumer membership change must fence even when the wire descriptor is identical.
    e.close_segment(next.endpoint, next.epoch, WAIT).unwrap();
    let newest = source.open(&e);
    assert!(matches!(
        e.submit_and_wait(TransferOp::Write, &next, &[request(&local)], WAIT),
        Err(Error::PeerClosed)
    ));
    assert!(
        e.submit_and_wait(TransferOp::Write, &newest, &[request(&local)], WAIT)
            .unwrap()
            .is_complete()
    );
    assert_eq!(contents(&local), [0xa2; 18]);
    let mut expected = vec![0xa2; 18];
    expected[0] = 0xf1;
    expected[17] = 0xf1;
    assert_eq!(*source.state.bytes.lock().unwrap(), expected);
}

#[test]
fn lost_write_ack_is_unknown_and_never_replayed_and_bad_responses_are_rejected() {
    let s = Peer::new();
    let e = TransferEngine::new(Default::default()).unwrap();
    let p = s.open(&e);
    let l = e
        .register_memory(vec![0xaa; 18].into(), Permission::LocalOnly)
        .unwrap();
    s.state.lose_ack.store(true, Ordering::Release);
    let r = e
        .submit_and_wait(TransferOp::Write, &p, &[request(&l)], WAIT)
        .unwrap();
    assert_eq!(r.failure.unwrap().remote_effect, RemoteEffect::Unknown);
    assert_eq!(s.state.writes.load(Ordering::Acquire), 1);
    let mut expected = vec![0xaa; 18];
    expected[0] = 0xf1;
    expected[17] = 0xf1;
    assert_eq!(*s.state.bytes.lock().unwrap(), expected);
    for at in [1, 2] {
        s.state.bad_response.store(at, Ordering::Release);
        assert!(matches!(
            e.open_tent_segment(s.endpoint, SourceEpoch::generate().unwrap(), WAIT),
            Err(Error::Protocol(_))
        ));
    }
    assert_eq!(contents(&l), [0xaa; 18]);
}

#[test]
fn retirement_timeout_retains_a_running_tent_read_until_drain() {
    let s = Peer::new();
    let e = TransferEngine::new(Default::default()).unwrap();
    let p = s.open(&e);
    let l = e
        .register_memory(vec![0xaa; 18].into(), Permission::LocalOnly)
        .unwrap();
    let (entered, seen) = mpsc::sync_channel(1);
    let (release, gate) = mpsc::sync_channel(1);
    *s.state.hold_read.lock().unwrap() = Some((entered, gate));
    thread::scope(|scope| {
        let t = scope.spawn(|| {
            e.submit_and_wait(TransferOp::Read, &p, &[request(&l)], WAIT)
                .unwrap()
        });
        seen.recv_timeout(WAIT).unwrap();
        assert!(matches!(
            e.close_segment(p.endpoint, p.epoch, Duration::ZERO),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(e.stats().unwrap().inflight_bytes, 16);
        assert_eq!(contents(&l), [0xaa; 18]);
        assert!(matches!(
            e.submit_and_wait(TransferOp::Read, &p, &[request(&l)], WAIT),
            Err(Error::PeerClosed)
        ));
        release.send(()).unwrap();
        assert!(t.join().unwrap().is_complete());
    });
    e.close_segment(p.endpoint, p.epoch, WAIT).unwrap();
    assert_eq!(e.stats().unwrap().inflight_bytes, 0);
    let mut expected = vec![0xf1; 18];
    expected[0] = 0xaa;
    expected[17] = 0xaa;
    assert_eq!(contents(&l), expected);
}

#[test]
fn truncated_read_response_and_partial_write_frame_leave_destinations_unchanged() {
    let s = Peer::new();
    let e = TransferEngine::new(Default::default()).unwrap();
    let p = s.open(&e);
    let l = e
        .register_memory(vec![0xab; 18].into(), Permission::LocalOnly)
        .unwrap();
    s.state.short_read.store(true, Ordering::Release);
    let r = e
        .submit_and_wait(TransferOp::Read, &p, &[request(&l)], WAIT)
        .unwrap();
    assert_eq!((r.completed_requests, r.transferred_bytes), (0, 0));
    assert!(r.failure.is_some());
    assert_eq!(contents(&l), [0xab; 18]);
    assert_eq!(*s.state.bytes.lock().unwrap(), [0xf1; 18]);
    let src = engine();
    let m = src
        .register_memory(vec![0xdd; 18].into(), Permission::RemoteReadWrite)
        .unwrap();
    let mut c =
        ControlChannel::connect(src.tent_endpoint().unwrap(), Instant::now() + WAIT).unwrap();
    c.write_all(&wire::request(WRITE, 32).unwrap()).unwrap();
    c.write_all(&data_bytes(m.base_address() + 1, 16, &[1; 3]))
        .unwrap();
    c.stream.shutdown(std::net::Shutdown::Write).unwrap();
    assert!(response(&mut c, MAX_ERROR).is_err());
    assert_eq!(contents(&m), [0xdd; 18]);
}
