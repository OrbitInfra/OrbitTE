use super::*;

#[test]
fn fragmented_frames_reuse_one_connection_and_rejected_body_is_never_a_header() {
    let (src, dst, remote, _local, peer) = setup(cfg(), 19);
    let mut socket = connect(address(&peer));
    let write = frame(&peer, TransferOp::Write, 3, 9);
    for b in write.encode().chunks(1) {
        socket.write_all(b).unwrap();
    }
    for b in b"fragment!".chunks(2) {
        socket.write_all(b).unwrap();
    }
    let ack = response(&mut socket);
    assert_eq!(
        (ack.id, ack.status, ack.committed),
        (write.id, Status::Ok, 9)
    );
    let mut expected = vec![0xa5; 19];
    expected[3..12].copy_from_slice(b"fragment!");
    assert_eq!(bytes(&remote), expected);
    let read = frame(&peer, TransferOp::Read, 3, 9);
    socket.write_all(&read.encode()).unwrap();
    assert_eq!(response(&mut socket).committed, 9);
    let mut got = [0; 9];
    socket.read_exact(&mut got).unwrap();
    assert_eq!(&got, b"fragment!");
    let readonly = src
        .register_memory(vec![0x91; 64].into(), Permission::RemoteRead)
        .unwrap();
    let peer2 = dst
        .open_segment(src.endpoint(), src.source_epoch(), WAIT)
        .unwrap();
    let denied = Request {
        address: readonly.base_address(),
        registration: buffer_registry::registration(peer2.epoch, readonly.id()),
        length: REQUEST as u64,
        ..write
    };
    socket.write_all(&denied.encode()).unwrap();
    socket.write_all(&write.encode()).unwrap();
    let reply = response(&mut socket);
    assert_eq!((reply.status, reply.committed), (Status::Permission, 0));
    assert_eq!(
        socket.read_exact(&mut [0; 1]).unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert_eq!(bytes(&readonly), [0x91; 64]);
    assert_eq!(bytes(&remote), expected);
}

#[test]
fn partial_server_write_retains_the_registered_backing_until_source_drain() {
    let (src, _dst, remote, _local, peer) = setup(cfg(), 18);
    let mut s = connect(address(&peer));
    let header = frame(&peer, TransferOp::Write, 2, 8);
    s.write_all(&header.encode()).unwrap();
    s.write_all(b"part").unwrap();
    let until = Instant::now() + WAIT;
    while &bytes(&remote)[2..6] != b"part" {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    src.unregister_memory(&remote).unwrap();
    assert!(matches!(
        src.close_source(Duration::from_millis(10)),
        Err(Error::Timeout)
    ));
    s.write_all(b"tail").unwrap();
    assert_eq!(response(&mut s).status, Status::Ok);
    src.close_source(WAIT).unwrap();
    let mut expected = [0xa5; 18];
    expected[2..10].copy_from_slice(b"parttail");
    assert_eq!(bytes(&remote), expected);
    let mut next = connect(address(&peer));
    next.write_all(
        &Request {
            op: TransferOp::Read,
            ..header
        }
        .encode(),
    )
    .unwrap();
    assert_eq!(response(&mut next).status, Status::Closing);
}

#[test]
fn empty_partial_headers_and_partial_rejected_writes_release_the_only_session() {
    let mut c = cfg();
    c.hp_tcp.max_outstanding_tasks = 1;
    c.hp_tcp.progress_timeout_ms = 180;
    let (src, _dst, remote, _local, peer) = setup(c, 10);
    for len in [0, 1, REQUEST] {
        let retired = src.retired_hp_sessions_for_test();
        let mut s = connect(address(&peer));
        let mut req = frame(&peer, TransferOp::Write, 2, 4);
        req.registration ^= 1;
        s.write_all(&req.encode()[..len]).unwrap();
        if len == REQUEST {
            s.write_all(&[0xd1]).unwrap();
        }
        let until = Instant::now() + WAIT;
        while src.retired_hp_sessions_for_test() == retired {
            assert!(
                Instant::now() < until,
                "the accepted partial frame still owns its slot"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(src.retired_hp_sessions_for_test(), retired + 1);
        // An incomplete frame gets no reply. Both FIN and RST are terminal for
        // this client; the source's receipt above proves retirement of this exact session.
        assert!(matches!(
            s.read_exact(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
        ));
    }
    let mut next = connect(address(&peer));
    next.write_all(&frame(&peer, TransferOp::Read, 1, 4).encode())
        .unwrap();
    assert_eq!(response(&mut next).committed, 4);
    let mut data = [0; 4];
    next.read_exact(&mut data).unwrap();
    assert_eq!(data, [0xa5; 4]);
    assert_eq!(bytes(&remote), [0xa5; 10]);
    src.close_source(WAIT).unwrap();
}

// Use an actual socket in place of only the advertised data endpoint. The engine still
// resolves the real registered region, selector, worker, admission and local memory.
pub(super) fn redirect(peer: &mut crate::RemoteSegment, listeners: &[&TcpListener]) {
    let addrs: Vec<_> = listeners
        .iter()
        .map(|l| match l.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => *a.ip(),
            _ => panic!("IPv4 fixture"),
        })
        .collect();
    peer.metadata.hp = Some(Arc::new(
        config::Endpoint::new(listeners[0].local_addr().unwrap().port(), 1 << 28, &addrs).unwrap(),
    ));
}

#[test]
fn fragmented_read_with_tail_truncation_and_stall_preserves_guards() {
    let n = task::STEP + 7;
    for fault in [0, 1, 2] {
        let mut c = cfg();
        c.hp_tcp.progress_timeout_ms = if fault == 2 { 250 } else { 30_000 };
        let (_src, dst, remote, local, mut peer) = setup(c, n + 2);
        let l = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        redirect(&mut peer, &[&l]);
        let (release, held) = mpsc::channel();
        let h = thread::spawn(move || {
            let (mut s, _) = accept(&l);
            let req = request(&mut s);
            for b in (Response {
                status: Status::Ok,
                id: req.id,
                committed: req.length,
            })
            .encode()
            .chunks(3)
            {
                s.write_all(b).unwrap();
            }
            let payload: Vec<_> = (0..n).map(|i| (i % 233) as u8).collect();
            let sent = if fault == 0 { n } else { n - 1 };
            for part in payload[..sent].chunks(5243) {
                s.write_all(part).unwrap();
            }
            if fault == 2 {
                held.recv_timeout(WAIT).unwrap();
            }
        });
        let req = TransferRequest::new(
            Transport::HpTcp,
            local.base_address() + 1,
            remote.base_address() + 1,
            n,
        );
        let report = dst
            .submit_and_wait(TransferOp::Read, &peer, &[req], WAIT)
            .unwrap();
        if fault == 2 {
            release.send(()).unwrap();
        }
        h.join().unwrap();
        assert_eq!(report.is_complete(), fault == 0);
        let got = bytes(&local);
        assert_eq!((got[0], got[n + 1]), (0xee, 0xee));
        // Only complete chunks are published to the registered destination.
        let used = if fault == 0 { n } else { task::STEP };
        assert!(
            got[1..=used]
                .iter()
                .enumerate()
                .all(|(i, b)| *b == (i % 233) as u8)
        );
        assert!(got[used + 1..].iter().all(|b| *b == 0xee));
        assert_eq!(bytes(&remote), vec![0xa5; n + 2]);
    }
}

#[test]
fn lost_write_ack_and_running_cancel_never_replay_or_release_early() {
    for cancel in [false, true] {
        let (_src, dst, remote, local, mut peer) = setup(cfg(), 18);
        let l = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        redirect(&mut peer, &[&l]);
        let (posted, on_post) = mpsc::channel();
        let (release, on_release) = mpsc::channel();
        let h = thread::spawn(move || {
            let (mut s, _) = accept(&l);
            let req = request(&mut s);
            assert_eq!(req.op, TransferOp::Write);
            let mut target = [0xa5; 18];
            s.read_exact(&mut target[2..10]).unwrap();
            assert_eq!(
                target,
                [
                    0xa5, 0xa5, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xa5, 0xa5, 0xa5,
                    0xa5, 0xa5, 0xa5, 0xa5, 0xa5
                ]
            );
            posted.send(()).unwrap();
            on_release.recv_timeout(WAIT).unwrap();
            drop(s);
            l.set_nonblocking(true).unwrap();
            assert!(matches!(l.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
        });
        let req = TransferRequest::new(
            Transport::HpTcp,
            local.base_address() + 2,
            remote.base_address() + 2,
            8,
        );
        let batch = dst
            .submit_async(TransferOp::Write, &peer, &[req], None, WAIT)
            .unwrap();
        on_post.recv_timeout(WAIT).unwrap();
        assert_eq!(dst.stats().unwrap().inflight_bytes, 8);
        if cancel {
            assert!(batch.cancel(0).unwrap());
        }
        release.send(()).unwrap();
        batch.wait(WAIT).unwrap();
        let status = batch.task_status(0).unwrap();
        assert_eq!(
            status.failure.as_ref().unwrap().remote_effect,
            RemoteEffect::Unknown
        );
        assert_ne!(status.state, crate::TaskState::Completed);
        h.join().unwrap();
        assert_eq!(bytes(&local), [0xee; 18]);
        assert_eq!(bytes(&remote), [0xa5; 18]);
    }
}

#[test]
fn idle_lane_expires_only_after_active_and_queued_requests_complete() {
    let mut c = cfg();
    c.hp_tcp.idle_connection_timeout_ms = 35;
    let (_src, dst, remote, local, mut peer) = setup(c, 14);
    let l = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    redirect(&mut peer, &[&l]);
    let (active, observed) = mpsc::channel();
    let (release, held) = mpsc::channel();
    let (idle, idle_seen) = mpsc::channel();
    let h = thread::spawn(move || {
        let (mut s, _) = accept(&l);
        s.set_read_timeout(Some(WAIT)).unwrap();
        for i in 0..2 {
            let req = request(&mut s);
            if i == 0 {
                active.send(()).unwrap();
                held.recv_timeout(WAIT).unwrap();
            }
            s.write_all(
                &(Response {
                    id: req.id,
                    status: Status::Ok,
                    committed: 3,
                })
                .encode(),
            )
            .unwrap();
            s.write_all(&[0x41 + i; 3]).unwrap();
        }
        assert_eq!(
            s.read_exact(&mut [0; 1]).unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        idle.send(()).unwrap();
        let (mut next, _) = accept(&l);
        let req = request(&mut next);
        next.write_all(
            &(Response {
                id: req.id,
                status: Status::Ok,
                committed: 3,
            })
            .encode(),
        )
        .unwrap();
        next.write_all(&[0x43; 3]).unwrap();
    });
    let run = |i: u64| {
        dst.submit_async(
            TransferOp::Read,
            &peer,
            &[TransferRequest::new(
                Transport::HpTcp,
                local.base_address() + i,
                remote.base_address() + i,
                3,
            )],
            None,
            WAIT,
        )
        .unwrap()
    };
    let a = run(1);
    observed.recv_timeout(WAIT).unwrap();
    let b = run(5);
    thread::sleep(Duration::from_millis(100));
    release.send(()).unwrap();
    assert_eq!(a.wait(WAIT).unwrap().completed_tasks, 1);
    assert_eq!(b.wait(WAIT).unwrap().completed_tasks, 1);
    // Observe idle retirement, rather than racing a timer against the reconnect.
    idle_seen.recv_timeout(WAIT).unwrap();
    assert_eq!(run(9).wait(WAIT).unwrap().completed_tasks, 1);
    h.join().unwrap();
    assert_eq!(
        bytes(&local),
        [
            0xee, 0x41, 0x41, 0x41, 0xee, 0x42, 0x42, 0x42, 0xee, 0x43, 0x43, 0x43, 0xee, 0xee
        ]
    );
    assert_eq!(bytes(&remote), [0xa5; 14]);
}

#[test]
fn backpressured_write_keeps_its_only_task_and_byte_charge_until_ack() {
    let len = 6 << 20;
    let mut c = cfg();
    c.hp_tcp.max_outstanding_tasks = 1;
    c.hp_tcp.max_outstanding_bytes = len as u64;
    let (_src, dst, remote, local, mut peer) = setup(c, len + 2);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let listener = {
        let _enter = runtime.enter();
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(64 << 10).unwrap();
        socket.bind((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
        socket.listen(1).unwrap().into_std().unwrap()
    };
    listener.set_nonblocking(false).unwrap();
    redirect(&mut peer, &[&listener]);
    let (ready, header) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let h = thread::spawn(move || {
        let (mut s, _) = accept(&listener);
        s.set_read_timeout(Some(WAIT)).unwrap();
        let req = request(&mut s);
        ready.send(()).unwrap();
        wait.recv_timeout(WAIT).unwrap();
        let mut target = vec![0xa5; len + 2];
        s.read_exact(&mut target[1..=len]).unwrap();
        assert_eq!((target[0], target[len + 1]), (0xa5, 0xa5));
        assert!(target[1..=len].iter().all(|b| *b == 0xee));
        s.write_all(
            &(Response {
                id: req.id,
                status: Status::Ok,
                committed: req.length,
            })
            .encode(),
        )
        .unwrap();
    });
    let req = TransferRequest::new(
        Transport::HpTcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        len,
    );
    let batch = dst
        .submit_async(
            TransferOp::Write,
            &peer,
            std::slice::from_ref(&req),
            None,
            WAIT,
        )
        .unwrap();
    header.recv_timeout(WAIT).unwrap();
    assert_eq!(dst.stats().unwrap().inflight_bytes, len);
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[req], WAIT),
        Err(Error::ResourceExhausted)
    ));
    release.send(()).unwrap();
    assert_eq!(batch.wait(WAIT).unwrap().completed_tasks, 1);
    h.join().unwrap();
    assert_eq!(bytes(&remote), vec![0xa5; len + 2]);
    assert_eq!(bytes(&local), vec![0xee; len + 2]);
    assert_eq!(dst.stats().unwrap().inflight_bytes, 0);
}

#[test]
fn restart_at_the_same_ports_and_address_rejects_the_old_write_id() {
    let mut c = cfg();
    let dst = TransferEngine::new(c.clone()).unwrap();
    let (control, _control_guard) = crate::test_ports::guarded_port();
    let (data, _data_guard) = crate::test_ports::guarded_port();
    c.bind_addr = control.local_addr().unwrap();
    c.hp_tcp.bind_address = "127.0.0.1".into();
    c.hp_tcp.port = data.local_addr().unwrap().port();
    drop((control, data));
    let backing = Arc::new(crate::HostMemory::new(vec![0xa5; 12].into()).unwrap());
    let (old_addr, old_epoch, old_frame) = {
        let engine = TransferEngine::new(c.clone()).unwrap();
        let reg = engine
            .register_host_memory(backing.clone(), 0, 12, Permission::RemoteReadWrite)
            .unwrap();
        let peer = dst
            .open_segment(engine.endpoint(), engine.source_epoch(), WAIT)
            .unwrap();
        assert_eq!(c.bind_addr, engine.endpoint());
        assert_eq!(c.hp_tcp.port, address(&peer).port());
        (
            reg.base_address(),
            peer.epoch,
            frame(&peer, TransferOp::Write, 1, 4),
        )
    };
    let engine = TransferEngine::new(c).unwrap();
    let reg = engine
        .register_host_memory(backing, 0, 12, Permission::RemoteReadWrite)
        .unwrap();
    assert_eq!(reg.base_address(), old_addr);
    assert_ne!(engine.source_epoch(), old_epoch);
    assert!(matches!(
        dst.open_segment(engine.endpoint(), old_epoch, WAIT),
        Err(Error::EpochMismatch)
    ));
    let peer = dst
        .open_segment(engine.endpoint(), engine.source_epoch(), WAIT)
        .unwrap();
    let mut s = connect(address(&peer));
    s.write_all(&old_frame.encode()).unwrap();
    s.write_all(b"lost").unwrap();
    assert_eq!(response(&mut s).status, Status::Stale);
    assert_eq!(bytes(&reg), [0xa5; 12]);
    let mut s = connect(address(&peer));
    s.write_all(&frame(&peer, TransferOp::Write, 1, 4).encode())
        .unwrap();
    s.write_all(b"live").unwrap();
    assert_eq!(response(&mut s).committed, 4);
    let mut expected = [0xa5; 12];
    expected[1..5].copy_from_slice(b"live");
    assert_eq!(bytes(&reg), expected);
}

#[cfg(target_os = "linux")]
#[test]
fn p10_killed_hp_tcp_requester_releases_only_after_partial_write_drain() {
    let (src, _dst, remote, _local, peer) = setup(cfg(), 18);
    let header = frame(&peer, TransferOp::Write, 2, 8).encode();
    let mut child = crate::runtime::control_plane::tests::killed_peer::KilledPeer::start(
        address(&peer).into(),
        &header,
        0,
    );
    let until = Instant::now() + WAIT;
    while &bytes(&remote)[2..6] != b"part" {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    src.unregister_memory(&remote).unwrap();
    assert!(matches!(
        src.close_source(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(src.stats().unwrap().retained_bytes, 18);
    child.kill();
    src.close_source(WAIT).unwrap();
    let mut expected = [0xa5; 18];
    expected[2..6].copy_from_slice(b"part");
    assert_eq!(bytes(&remote), expected);
    drop(remote);
    assert_eq!(src.stats().unwrap().retained_bytes, 0);
}
