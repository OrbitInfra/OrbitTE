use super::*;
use crate::RemoteEffect;
use crate::test_support::EVENTUAL_PROGRESS as WAIT;
use crate::{EngineConfig, Permission, TransferEngine, TransferRequest, Transport};
use protocol::{REQUEST, RESPONSE, Request, Response, Status};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

mod batch;
mod rails;
mod sockets;

fn cfg() -> EngineConfig {
    EngineConfig {
        tcp: false,
        hp_tcp: HpTcpConfig {
            enable: true,
            worker_count: 2,
            connections_per_peer: 1,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn setup(
    c: EngineConfig,
    len: usize,
) -> (
    Arc<TransferEngine>,
    Arc<TransferEngine>,
    crate::RegisteredMemory,
    crate::RegisteredMemory,
    crate::RemoteSegment,
) {
    let src = Arc::new(TransferEngine::new(c.clone()).unwrap());
    let dst = Arc::new(TransferEngine::new(c).unwrap());
    let remote = src
        .register_memory(vec![0xa5; len].into(), Permission::RemoteReadWrite)
        .unwrap();
    let local = dst
        .register_memory(vec![0xee; len].into(), Permission::LocalOnly)
        .unwrap();
    let peer = dst
        .open_segment(src.endpoint(), src.source_epoch(), WAIT)
        .unwrap();
    (src, dst, remote, local, peer)
}
fn address(peer: &crate::RemoteSegment) -> SocketAddrV4 {
    let hp = peer.metadata.hp.as_ref().unwrap();
    SocketAddrV4::new(hp.rails()[0], hp.port)
}
fn accept(listener: &TcpListener) -> (TcpStream, std::net::SocketAddr) {
    listener.set_nonblocking(true).unwrap();
    let until = Instant::now() + WAIT;
    loop {
        match listener.accept() {
            Ok((socket, address)) => {
                socket.set_nonblocking(false).unwrap();
                socket.set_read_timeout(Some(WAIT)).unwrap();
                socket.set_write_timeout(Some(WAIT)).unwrap();
                return (socket, address);
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < until, "mock peer never accepted its lane");
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("accept mock HP-TCP lane: {e}"),
        }
    }
}
fn connect(addr: SocketAddrV4) -> TcpStream {
    let s = TcpStream::connect_timeout(&addr.into(), WAIT).unwrap();
    s.set_read_timeout(Some(WAIT)).unwrap();
    s.set_write_timeout(Some(WAIT)).unwrap();
    s.set_nodelay(true).unwrap();
    s
}
fn frame(peer: &crate::RemoteSegment, op: TransferOp, offset: usize, length: usize) -> Request {
    let remote = peer.regions()[0];
    Request {
        op,
        id: 19,
        registration: buffer_registry::registration(peer.epoch, remote.id()),
        address: remote.base_address() + offset as u64,
        length: length as u64,
    }
}
fn response(socket: &mut TcpStream) -> Response {
    let mut buf = [0; RESPONSE];
    socket.read_exact(&mut buf).unwrap();
    Response::decode(&buf).unwrap()
}
fn request(socket: &mut TcpStream) -> Request {
    let mut buf = [0; REQUEST];
    socket.read_exact(&mut buf).unwrap();
    Request::decode(&buf).unwrap()
}
fn bytes(mem: &crate::RegisteredMemory) -> Vec<u8> {
    let mut b = vec![0; mem.len()];
    mem.read(0, &mut b).unwrap();
    b
}

#[test]
fn frozen_frames_match_literal_bytes_and_reject_malformed_fields() {
    const REQ: [u8; 48] = *include_bytes!("../../../reference/hp_tcp/write.bin");
    const ACK: [u8; 32] = *include_bytes!("../../../reference/hp_tcp/ack.bin");
    let req = Request {
        op: TransferOp::Write,
        id: 0x0102030405060708,
        registration: 9,
        address: 256,
        length: 3,
    };
    assert_eq!(req.encode(), REQ);
    assert_eq!(
        (Request {
            op: TransferOp::Read,
            ..req
        })
        .encode(),
        *include_bytes!("../../../reference/hp_tcp/read.bin")
    );
    assert_eq!(Request::decode(&REQ), Ok(req));
    let ack = Response {
        id: req.id,
        status: Status::Ok,
        committed: 3,
    };
    assert_eq!(ack.encode(), ACK);
    assert_eq!(Response::decode(&ACK).unwrap(), ack);
    for (i, v, e) in [
        (0, 0, Status::Internal),
        (5, 2, Status::BadVersion),
        (6, 3, Status::BadOpcode),
        (7, 1, Status::Internal),
        (39, 0, Status::BadLength),
        (47, 1, Status::Internal),
    ] {
        let mut bad = REQ;
        bad[i] = v;
        assert_eq!(Request::decode(&bad), Err(e));
    }
    let mut overflow = REQ;
    overflow[24..32].fill(255);
    assert_eq!(Request::decode(&overflow), Err(Status::BadLength));
    assert!(Request::decode(&REQ[..47]).is_err());
    for i in [0, 5, 7, 31] {
        let mut bad = ACK;
        bad[i] = 255;
        assert!(Response::decode(&bad).is_err());
    }
}
#[test]
fn config_defaults_rails_limits_and_excluded_modes() {
    let base = HpTcpConfig::default();
    assert!(!base.enable);
    assert_eq!((base.worker_count, base.connections_per_peer), (16, 4));
    assert_eq!(
        (
            base.max_outstanding_tasks,
            base.max_outstanding_bytes,
            base.max_transfer_bytes
        ),
        (4096, 1 << 32, 1 << 30)
    );
    assert_eq!(
        (
            base.connect_timeout_ms,
            base.progress_timeout_ms,
            base.idle_connection_timeout_ms
        ),
        (2000, 30000, 60000)
    );
    let full:HpTcpConfig=serde_json::from_str(r#"{"enable":true,"max_outstanding_bytes":18446744073709551615,"rail_addresses":["127.0.0.1","127.0.0.2"],"idle_connection_timeout_ms":31}"#).unwrap();
    assert_eq!(full.max_outstanding_bytes, u64::MAX);
    assert_eq!(full.idle_connection_timeout_ms, 31);
    assert!(full.validate(false).is_ok());
    assert!(full.validate(true).is_err());
    for json in [
        r#"{"enable":"true"}"#,
        r#"{"port":65536}"#,
        r#"{"worker_count":-1}"#,
        r#"{"rail_addresses":["::1"]}"#,
        r#"{"tls":true}"#,
        r#"{"stripe_writes":true}"#,
        r#"{"rail_failover":true}"#,
    ] {
        assert!(serde_json::from_str::<HpTcpConfig>(json).is_err(), "{json}");
    }
    for (enable, succeeds) in [(false, true), (true, false)] {
        assert_eq!(
            HpTcpConfig {
                enable,
                worker_count: 0,
                ..base.clone()
            }
            .validate(false)
            .is_ok(),
            succeeds
        );
    }
    for bad in [
        HpTcpConfig {
            connections_per_peer: 1,
            ..full.clone()
        },
        HpTcpConfig {
            bind_address: "127.0.0.1".into(),
            rail_addresses: vec![Ipv4Addr::new(127, 0, 0, 2)],
            ..full.clone()
        },
        HpTcpConfig {
            bind_address: "127.0.0.1".into(),
            ..full.clone()
        },
        HpTcpConfig {
            rail_addresses: vec![Ipv4Addr::LOCALHOST; 2],
            ..full.clone()
        },
        HpTcpConfig {
            rail_addresses: vec![Ipv4Addr::UNSPECIFIED],
            ..full.clone()
        },
        HpTcpConfig {
            worker_count: 257,
            ..full
        },
    ] {
        assert!(bad.validate(false).is_err());
    }
}
#[test]
fn describe_and_registration_are_scoped_to_the_source_incarnation() {
    let (_, dst, _remote, _local, peer) = setup(cfg(), 8);
    let first = frame(&peer, TransferOp::Read, 0, 8).registration;
    let (s2, d2, _m2, _l2, p2) = setup(cfg(), 8);
    let private = s2
        .register_memory(vec![0x42; 8].into(), Permission::LocalOnly)
        .unwrap();
    let readonly = d2
        .open_segment(s2.endpoint(), s2.source_epoch(), WAIT)
        .unwrap();
    assert!(matches!(
        readonly.resolve_address(private.base_address(), 1),
        Err(Error::RegionNotFound)
    ));
    let denied = Request {
        address: private.base_address(),
        registration: buffer_registry::registration(p2.epoch, private.id()),
        ..frame(&p2, TransferOp::Read, 0, 8)
    };
    let mut raw = connect(address(&p2));
    raw.write_all(&denied.encode()).unwrap();
    assert_eq!(response(&mut raw).status, Status::Permission);
    assert_eq!(bytes(&private), [0x42; 8]);
    assert_ne!(first, frame(&p2, TransferOp::Read, 0, 8).registration);
    let mut s = connect(address(&p2));
    let mut stale = frame(&p2, TransferOp::Read, 0, 8);
    stale.registration = first;
    s.write_all(&stale.encode()).unwrap();
    assert_eq!(response(&mut s).status, Status::Stale);
    let ordinary = TransferEngine::new(Default::default()).unwrap();
    let m = ordinary
        .register_memory(vec![7; 8].into(), Permission::RemoteRead)
        .unwrap();
    let ordinary_peer = dst
        .open_segment(ordinary.endpoint(), ordinary.source_epoch(), WAIT)
        .unwrap();
    assert!(ordinary_peer.metadata.hp.is_none());
    let explicit =
        TransferRequest::new(Transport::HpTcp, _local.base_address(), m.base_address(), 4);
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Read, &ordinary_peer, &[explicit], WAIT),
        Err(Error::Unavailable(_))
    ));
}
#[test]
fn batch_capacity_is_atomic_and_aliases_charge_one_physical_task() {
    let mut c = cfg();
    c.hp_tcp.max_outstanding_tasks = 1;
    c.hp_tcp.max_outstanding_bytes = 8;
    c.hp_tcp.max_transfer_bytes = 8;
    let (_src, dst, remote, local, peer) = setup(c, 20);
    let req = |i| {
        TransferRequest::new(
            Transport::Unspec,
            local.base_address() + i,
            remote.base_address() + i,
            4,
        )
    };
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[req(1), req(10)], WAIT),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(bytes(&remote), [0xa5; 20]);
    let batch = dst
        .submit_async(TransferOp::Write, &peer, &[req(1), req(5)], None, WAIT)
        .unwrap();
    batch.wait(WAIT).unwrap();
    assert_eq!(
        batch.task_status(0).unwrap().physical_owner,
        batch.task_status(1).unwrap().physical_owner
    );
    let mut expected = [0xa5; 20];
    expected[1..9].fill(0xee);
    assert_eq!(bytes(&remote), expected);
    assert_eq!(bytes(&local), [0xee; 20]);
}

#[test]
fn worker_affinity_stop_and_permit_release_are_owned() {
    let workers = workers::Workers::start(2).unwrap();
    let (send, recv) = mpsc::channel();
    for i in [0, 1, 2] {
        let send = send.clone();
        workers.execute(i, async move {
            send.send((i, thread::current().id())).unwrap();
        });
    }
    let mut seen = (0..3)
        .map(|_| recv.recv_timeout(WAIT).unwrap())
        .collect::<Vec<_>>();
    seen.sort_by_key(|p| p.0);
    assert_eq!(seen[0].1, seen[2].1);
    assert_ne!(seen[0].1, seen[1].1);
    workers.stop().unwrap();
    workers.stop().unwrap();
    let budget = task::Budget::new(2, 8);
    let a = budget.reserve(6).unwrap();
    assert!(matches!(budget.reserve(3), Err(Error::ResourceExhausted)));
    let b = budget.reserve(2).unwrap();
    assert!(matches!(budget.reserve(1), Err(Error::ResourceExhausted)));
    drop((a, b));
    assert!(budget.reserve(8).is_ok());
}

#[test]
fn endpoint_bootstrap_rejects_invalid_lengths_duplicates_and_incompatible_rail_counts() {
    let one = Ipv4Addr::LOCALHOST;
    let two = Ipv4Addr::new(127, 0, 0, 2);
    let end = Endpoint::new(24821, 1 << 20, &[one, two]).unwrap();
    let mut wire = vec![0; end.wire_len()];
    end.encode(&mut wire);
    assert_eq!(Endpoint::decode(&wire).unwrap(), end);
    for i in [2, 3] {
        let mut bad = wire.clone();
        bad[i] = 255;
        assert!(Endpoint::decode(&bad).is_err());
    }
    let mut bad = wire.clone();
    bad[16..20].copy_from_slice(&one.octets());
    assert!(Endpoint::decode(&bad).is_err());
    assert!(Endpoint::decode(&wire[..19]).is_err());
    let (_src, dst, remote, local, mut peer) = setup(cfg(), 10);
    peer.metadata.hp = Some(Arc::new(end));
    let req = TransferRequest::new(
        Transport::HpTcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        4,
    );
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[req], WAIT),
        Err(Error::Unavailable(_))
    ));
    assert_eq!(bytes(&remote), [0xa5; 10]);
    assert_eq!(bytes(&local), [0xee; 10]);
}

#[test]
fn a_failed_worker_fences_new_admission_and_teardown_still_joins() {
    let workers = workers::Workers::start(1).unwrap();
    let (done, result) = mpsc::channel();
    workers.execute(0, async move {
        done.send(17).unwrap();
    });
    assert_eq!(result.recv_timeout(WAIT).unwrap(), 17);
    workers.execute(0, async { panic!("injected HP-TCP command failure") });
    let until = Instant::now() + WAIT;
    while workers.ready().is_ok() {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    assert!(matches!(workers.ready(), Err(Error::Internal(_))));
    assert!(matches!(workers.stop(), Err(Error::Internal(_))));
    workers.stop().unwrap();
}

#[test]
fn peer_frame_limit_keeps_adjacent_aliases_separate_before_payload() {
    let mut c = cfg();
    c.hp_tcp.max_outstanding_tasks = 1;
    c.hp_tcp.max_transfer_bytes = 6;
    let (_src, dst, remote, local, peer) = setup(c, 16);
    let requests = [1, 5].map(|off| {
        TransferRequest::new(
            Transport::Unspec,
            local.base_address() + off,
            remote.base_address() + off,
            4,
        )
    });
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, &requests, WAIT),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(bytes(&remote), [0xa5; 16]);
    assert_eq!(bytes(&local), [0xee; 16]);
}

impl HpTcpTransport {
    pub(crate) fn outstanding_on_lanes(&self) -> usize {
        self.client.outstanding()
    }
    pub(crate) fn fail_worker_for_test(&self) {
        self.workers.execute(0, async {
            panic!("injected worker failure after terminal status")
        });
        let until = Instant::now() + WAIT;
        while self.workers.ready().is_ok() {
            assert!(Instant::now() < until);
            thread::yield_now();
        }
    }
}
