use super::*;

#[test]
fn peer_retirement_hp_tcp_waits_for_response_then_drops_only_its_lane() {
    let (_src, dst, remote, local, mut peer) = setup(cfg(), 18);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    sockets::redirect(&mut peer, &[&listener]);
    let q = TransferRequest::new(
        Transport::HpTcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        16,
    );
    let active = dst
        .submit_async(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&q),
            None,
            WAIT,
        )
        .unwrap();
    let (mut socket, _) = accept(&listener);
    let req = request(&mut socket);
    assert!(matches!(
        dst.close_segment(peer.endpoint, peer.epoch, Duration::ZERO),
        Err(Error::Unquiesced { .. })
    ));
    assert_eq!(dst.stats().unwrap().inflight_bytes, 16);
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Read, &peer, &[q], WAIT),
        Err(Error::PeerClosed)
    ));
    let (other, _other_dst, rr, _, _) = setup(cfg(), 18);
    let p = dst
        .open_segment(other.endpoint(), other.source_epoch(), WAIT)
        .unwrap();
    let ll = dst
        .register_memory(vec![0xdb; 18].into(), Permission::LocalOnly)
        .unwrap();
    let q = TransferRequest::new(
        Transport::HpTcp,
        ll.base_address() + 1,
        rr.base_address() + 1,
        16,
    );
    assert!(
        dst.submit_and_wait(TransferOp::Read, &p, std::slice::from_ref(&q), WAIT)
            .unwrap()
            .is_complete()
    );
    socket
        .write_all(
            &Response {
                status: Status::Ok,
                id: req.id,
                committed: 16,
            }
            .encode(),
        )
        .unwrap();
    socket.write_all(&[0x73; 16]).unwrap();
    assert_eq!(active.wait(WAIT).unwrap().completed_tasks, 1);
    dst.close_segment(peer.endpoint, peer.epoch, WAIT).unwrap();
    let mut b = [0; 1];
    assert_eq!(socket.read(&mut b).unwrap(), 0);
    assert!(
        dst.submit_and_wait(TransferOp::Read, &p, &[q], WAIT)
            .unwrap()
            .is_complete()
    );
    let mut want = vec![0xee; 18];
    want[1..17].fill(0x73);
    assert_eq!(bytes(&local), want);
    assert_eq!(bytes(&remote), vec![0xa5; 18]);
    let mut want = vec![0xdb; 18];
    want[1..17].fill(0xa5);
    assert_eq!(bytes(&ll), want);
}
