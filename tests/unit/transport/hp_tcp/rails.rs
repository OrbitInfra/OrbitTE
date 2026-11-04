use super::*;

#[test]
fn concurrent_public_transfers_keep_all_sixteen_payloads_and_guards() {
    let mut c = cfg();
    c.hp_tcp.connections_per_peer = 4;
    let (_source, dst, remote, local, peer) = setup(c, 16 * 12);
    let want: Vec<u8> = (0..16 * 12).map(|i| (i * 7 % 251) as u8).collect();
    local.write(0, &want).unwrap();
    let run = |op| {
        thread::scope(|scope| {
            let mut joins = Vec::new();
            for i in 0..16 {
                let d = &dst;
                let p = &peer;
                let r = TransferRequest::new(
                    Transport::Unspec,
                    local.base_address() + 12 * i + 1,
                    remote.base_address() + 12 * i + 1,
                    10,
                );
                joins.push(scope.spawn(move || d.submit_and_wait(op, p, &[r], WAIT).unwrap()));
            }
            for h in joins {
                let rep = h.join().unwrap();
                assert!(rep.is_complete(), "{rep:?}");
                assert_eq!(rep.transferred_bytes, 10);
            }
        })
    };
    run(TransferOp::Write);
    let mut target = vec![0xa5; 192];
    for i in 0..16 {
        target[12 * i + 1..12 * i + 11].copy_from_slice(&want[12 * i + 1..12 * i + 11]);
    }
    assert_eq!(bytes(&remote), target);
    local.write(0, &[0xee; 192]).unwrap();
    run(TransferOp::Read);
    let mut dst_bytes = target.clone();
    for i in 0..16 {
        dst_bytes[12 * i] = 0xee;
        dst_bytes[12 * i + 11] = 0xee;
    }
    assert_eq!(bytes(&local), dst_bytes);
    assert_eq!(bytes(&remote), target);
}

#[cfg(target_os = "linux")]
fn rail_listener(
    peer: &mut crate::RemoteSegment,
    ips: [Ipv4Addr; 2],
) -> ([mpsc::Receiver<TcpStream>; 2], thread::JoinHandle<()>) {
    // Like the provider, reserve the port once across all local addresses.
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
    peer.metadata.hp = Some(Arc::new(
        Endpoint::new(listener.local_addr().unwrap().port(), 1 << 28, &ips).unwrap(),
    ));
    let (at, ar) = mpsc::sync_channel(1);
    let (bt, br) = mpsc::sync_channel(1);
    let join = thread::spawn(move || {
        let tx = [at, bt];
        for _ in 0..2 {
            let (s, from) = accept(&listener);
            let local = s.local_addr().unwrap().ip();
            let rail = ips.iter().position(|ip| local == *ip).unwrap();
            assert_eq!(from.ip(), ips[rail]);
            tx[rail].send(s).unwrap();
        }
    });
    ([ar, br], join)
}

#[cfg(target_os = "linux")]
#[test]
fn static_ipv4_pairing_balances_uneven_reads_rotates_small_requests_and_never_stripes_writes() {
    let ips = [Ipv4Addr::LOCALHOST, Ipv4Addr::new(127, 0, 0, 2)];
    let mut c = cfg();
    c.hp_tcp.rail_addresses = ips.to_vec();
    c.hp_tcp.connections_per_peer = 2;
    let n = 2 * task::STEP + 3;
    let (_src, dst, remote, local, mut peer) = setup(c, n + 4);
    let (lanes, listener) = rail_listener(&mut peer, ips);
    let base = remote.base_address() + 1;
    let data: Vec<u8> = (0..n).map(|i| (i % 239) as u8).collect();
    let handlers = lanes
        .into_iter()
        .enumerate()
        .map(|(rail, l)| {
            let data = data.clone();
            thread::spawn(move || {
                let mut s = l.recv_timeout(WAIT).unwrap();
                s.set_read_timeout(Some(WAIT)).unwrap();
                let mut frames = Vec::new();
                for _ in 0..(if rail == 0 { 3 } else { 2 }) {
                    let req = request(&mut s);
                    let pos = (req.address - base) as usize;
                    let len = req.length as usize;
                    frames.push((req.op, pos, len));
                    if req.op == TransferOp::Write {
                        let mut v = vec![0; len];
                        s.read_exact(&mut v).unwrap();
                        assert_eq!(v, &data[pos..pos + len]);
                    }
                    s.write_all(
                        &(Response {
                            status: Status::Ok,
                            id: req.id,
                            committed: req.length,
                        })
                        .encode(),
                    )
                    .unwrap();
                    if req.op == TransferOp::Read {
                        s.write_all(&data[pos..pos + len]).unwrap();
                    }
                }
                frames
            })
        })
        .collect::<Vec<_>>();
    let run = |op, off, len| {
        let req = TransferRequest::new(
            Transport::HpTcp,
            local.base_address() + 1 + off,
            base + off,
            len,
        );
        let report = dst.submit_and_wait(op, &peer, &[req], WAIT).unwrap();
        assert!(report.is_complete(), "{report:?}");
    };
    run(TransferOp::Read, 0, n);
    run(TransferOp::Write, 0, n);
    run(TransferOp::Read, 0, 5);
    run(TransferOp::Read, 7, 5);
    let got = bytes(&local);
    assert_eq!(&got[1..n + 1], &data);
    assert_eq!(got[0], 0xee);
    assert_eq!(&got[n + 1..], &[0xee; 3]);
    assert_eq!(bytes(&remote), vec![0xa5; n + 4]);
    listener.join().unwrap();
    let mut logs = handlers.into_iter().map(|h| h.join().unwrap());
    let a = logs.next().unwrap();
    let b = logs.next().unwrap();
    assert_eq!(
        a,
        [
            (TransferOp::Read, 0, n.div_ceil(2)),
            (TransferOp::Write, 0, n),
            (TransferOp::Read, 7, 5)
        ]
    );
    assert_eq!(
        b,
        [
            (TransferOp::Read, n.div_ceil(2), n / 2),
            (TransferOp::Read, 0, 5)
        ]
    );
}

#[test]
fn periodic_fanout_rotates_lanes_independently_per_peer() {
    let mut conf = cfg();
    conf.hp_tcp.connections_per_peer = 2;
    let (_a, dst, ra, local, mut pa) = setup(conf.clone(), 8);
    let (_b, _dummy, rb, _unused, mut pb) = setup(conf, 8);
    let mut handlers = Vec::new();
    for p in [&mut pa, &mut pb] {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        sockets::redirect(p, &[&listener]);
        handlers.push(thread::spawn(move || {
            thread::scope(|scope| {
                for _ in 0..2 {
                    let (mut s, _) = accept(&listener);
                    s.set_read_timeout(Some(WAIT)).unwrap();
                    scope.spawn(move || {
                        for _ in 0..2 {
                            let req = request(&mut s);
                            s.write_all(
                                &(Response {
                                    id: req.id,
                                    status: Status::Ok,
                                    committed: 2,
                                })
                                .encode(),
                            )
                            .unwrap();
                            s.write_all(&[0x81, 0x82]).unwrap();
                        }
                    });
                }
            })
        }));
    }
    for _ in 0..4 {
        for (p, r) in [(&pa, &ra), (&pb, &rb)] {
            let req = TransferRequest::new(
                Transport::HpTcp,
                local.base_address() + 2,
                r.base_address() + 2,
                2,
            );
            let report = dst
                .submit_and_wait(TransferOp::Read, p, &[req], WAIT)
                .unwrap();
            assert!(report.is_complete(), "{report:?}");
        }
    }
    for h in handlers {
        h.join().unwrap();
    }
    assert_eq!(
        bytes(&local),
        [0xee, 0xee, 0x81, 0x82, 0xee, 0xee, 0xee, 0xee]
    );
    assert_eq!(bytes(&ra), [0xa5; 8]);
    assert_eq!(bytes(&rb), [0xa5; 8]);
}

#[cfg(target_os = "linux")]
#[test]
fn stale_sliced_read_closes_the_live_sibling_before_returning() {
    let ips = [Ipv4Addr::LOCALHOST, Ipv4Addr::new(127, 0, 0, 2)];
    let mut c = cfg();
    c.hp_tcp.rail_addresses = ips.to_vec();
    c.hp_tcp.connections_per_peer = 2;
    let n = 2 * task::STEP + 1;
    let (_src, dst, remote, local, mut peer) = setup(c, n + 2);
    let ([a, b], listener) = rail_listener(&mut peer, ips);
    let (on_sibling, sibling) = mpsc::channel();
    let h = thread::spawn(move || {
        let mut s = b.recv_timeout(WAIT).unwrap();
        s.set_read_timeout(Some(WAIT)).unwrap();
        let req = request(&mut s);
        s.write_all(
            &(Response {
                id: req.id,
                status: Status::Ok,
                committed: req.length,
            })
            .encode(),
        )
        .unwrap();
        on_sibling.send(()).unwrap();
        assert!(s.read_exact(&mut [0; 1]).is_err());
    });
    let stale = thread::spawn(move || {
        let mut s = a.recv_timeout(WAIT).unwrap();
        let req = request(&mut s);
        sibling.recv_timeout(WAIT).unwrap();
        s.write_all(
            &(Response {
                id: req.id,
                status: Status::Stale,
                committed: 0,
            })
            .encode(),
        )
        .unwrap();
    });
    let req = TransferRequest::new(
        Transport::HpTcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        n,
    );
    let rep = dst
        .submit_and_wait(TransferOp::Read, &peer, &[req], WAIT)
        .unwrap();
    assert!(rep.locally_drained);
    assert!(rep.is_complete(), "{rep:?}");
    h.join().unwrap();
    stale.join().unwrap();
    listener.join().unwrap();
    let mut expected = vec![0xa5; n + 2];
    expected[0] = 0xee;
    expected[n + 1] = 0xee;
    assert_eq!(bytes(&local), expected);
    assert_eq!(bytes(&remote), vec![0xa5; n + 2]);
}

#[test]
fn least_loaded_lane_bypasses_a_delayed_ack_and_releases_after_completion() {
    let mut c = cfg();
    c.hp_tcp.connections_per_peer = 2;
    let (_src, dst, remote, local, mut peer) = setup(c, 16);
    let l = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    sockets::redirect(&mut peer, &[&l]);
    let req = |i| {
        TransferRequest::new(
            Transport::HpTcp,
            local.base_address() + i,
            remote.base_address() + i,
            1,
        )
    };
    let ack = |socket: &mut TcpStream, req: Request| {
        socket
            .write_all(
                &Response {
                    id: req.id,
                    status: Status::Ok,
                    committed: 1,
                }
                .encode(),
            )
            .unwrap();
    };
    let held = dst
        .submit_async(TransferOp::Write, &peer, &[req(1)], None, WAIT)
        .unwrap();
    let (mut slow, _) = accept(&l);
    let first = request(&mut slow);
    let mut byte = [0];
    slow.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [0xee]);
    let second = dst
        .submit_async(TransferOp::Read, &peer, &[req(3)], None, WAIT)
        .unwrap();
    let (mut fast, _) = accept(&l);
    let read = request(&mut fast);
    ack(&mut fast, read);
    fast.write_all(&[3]).unwrap();
    assert_eq!(second.wait(WAIT).unwrap().transferred_bytes, 1);
    // Rotation now points back at the held WRITE. The next WRITE must use the idle lane.
    let third = dst
        .submit_async(TransferOp::Write, &peer, &[req(5)], None, WAIT)
        .unwrap();
    let write = request(&mut fast);
    assert_eq!(write.address, remote.base_address() + 5);
    fast.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [0xee]);
    ack(&mut fast, write);
    assert_eq!(third.wait(WAIT).unwrap().transferred_bytes, 1);
    ack(&mut slow, first);
    assert_eq!(held.wait(WAIT).unwrap().transferred_bytes, 1);
    let mut expected = [0xee; 16];
    expected[3] = 3;
    assert_eq!(bytes(&local), expected);
}
