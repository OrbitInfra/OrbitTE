use super::*;
use crate::test_support::EVENTUAL_PROGRESS as WAIT;

fn bytes(m: &RegisteredMemory) -> Vec<u8> {
    let mut b = vec![0; m.len()];
    m.read(0, &mut b).unwrap();
    b
}

#[test]
fn peer_retirement_fences_old_snapshots_and_preserves_other_endpoints_and_epochs() -> Result<()> {
    let source = TransferEngine::new(EngineConfig::default())?;
    let other = TransferEngine::new(EngineConfig::default())?;
    let dst = TransferEngine::new(EngineConfig::default())?;
    let r = source.register_memory(vec![0x43; 18].into(), Permission::RemoteReadWrite)?;
    let other_r = other.register_memory(vec![0x97; 18].into(), Permission::RemoteReadWrite)?;
    let l = dst.register_memory(vec![0xcd; 18].into(), Permission::LocalOnly)?;
    let p = dst.open_segment(source.endpoint(), source.source_epoch(), WAIT)?;
    let o = dst.open_segment(other.endpoint(), other.source_epoch(), WAIT)?;
    let q = TransferRequest::new(
        Transport::Tcp,
        l.base_address() + 1,
        r.base_address() + 1,
        16,
    );
    assert!(
        dst.submit_and_wait(TransferOp::Read, &p, std::slice::from_ref(&q), WAIT)?
            .is_complete()
    );
    dst.close_segment(p.endpoint, p.epoch, WAIT)?;
    for result in [
        dst.open_segment(p.endpoint, p.epoch, WAIT).map(|_| ()),
        dst.submit_and_wait(TransferOp::Read, &p, &[q], WAIT)
            .map(|_| ()),
    ] {
        assert!(matches!(result, Err(Error::PeerClosed)));
    }
    let q = TransferRequest::new(
        Transport::Tcp,
        l.base_address() + 1,
        other_r.base_address() + 1,
        16,
    );
    assert!(
        dst.submit_and_wait(TransferOp::Read, &o, &[q], WAIT)?
            .is_complete()
    );
    let mut wanted = vec![0xcd; 18];
    wanted[1..17].fill(0x97);
    assert_eq!(bytes(&l), wanted);
    assert_eq!(bytes(&r), vec![0x43; 18]);
    assert_eq!(bytes(&other_r), vec![0x97; 18]);
    // Same socket, fresh authoritative epoch, without a racy port release/rebind.
    let fresh = SourceEpoch::generate()?;
    let fake_old = SourceEpoch::generate()?;
    dst.close_segment(other.endpoint(), fake_old, WAIT)?;
    let p2 = dst.open_segment(other.endpoint(), other.source_epoch(), WAIT)?;
    assert_eq!(p2.source_epoch(), o.source_epoch());
    assert!(matches!(
        dst.open_segment(other.endpoint(), fresh, WAIT),
        Err(Error::EpochMismatch)
    ));
    assert!(matches!(
        dst.open_segment(other.endpoint(), fake_old, WAIT),
        Err(Error::PeerClosed)
    ));
    Ok(())
}

#[test]
fn peer_retirement_submit_race_has_one_admission_boundary() -> Result<()> {
    let src = TransferEngine::new(EngineConfig::default())?;
    let r = src.register_memory(vec![0x5b; 18].into(), Permission::RemoteReadWrite)?;
    for op in [TransferOp::Read, TransferOp::Write] {
        let dst = TransferEngine::new(EngineConfig::default())?;
        let local = dst.register_memory(vec![0xed; 18].into(), Permission::LocalOnly)?;
        r.write(0, &[0x5b; 18])?;
        let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
        let q = TransferRequest::new(
            Transport::Tcp,
            local.base_address() + 1,
            r.base_address() + 1,
            16,
        );
        let ready = std::sync::Barrier::new(2);
        std::thread::scope(|s| {
            let send = s.spawn(|| {
                ready.wait();
                dst.submit_and_wait(op, &peer, std::slice::from_ref(&q), WAIT)
            });
            ready.wait();
            dst.close_segment(peer.endpoint, peer.epoch, WAIT).unwrap();
            let copied = match send.join().unwrap() {
                Ok(report) => {
                    assert!(report.is_complete());
                    true
                }
                Err(Error::PeerClosed) => false,
                other => panic!("{other:?}"),
            };
            let mut want_local = vec![0xed; 18];
            let mut want_remote = vec![0x5b; 18];
            if copied {
                match op {
                    TransferOp::Read => want_local[1..17].fill(0x5b),
                    TransferOp::Write => want_remote[1..17].fill(0xed),
                }
            }
            assert_eq!(bytes(&local), want_local);
            assert_eq!(bytes(&r), want_remote);
        });
        assert!(matches!(
            dst.submit_and_wait(op, &peer, &[q], WAIT),
            Err(Error::PeerClosed)
        ));
        assert_eq!(dst.stats()?.active_peer_operations, 0);
    }
    Ok(())
}

#[test]
fn peer_retirement_reopens_a_new_authoritative_epoch_at_the_same_endpoint() -> Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    let old = TransferEngine::new(EngineConfig::default())?;
    let new = TransferEngine::new(EngineConfig::default())?;
    let dst = TransferEngine::new(EngineConfig::default())?;
    let old_r = old.register_memory(vec![0x45; 18].into(), Permission::RemoteRead)?;
    let new_r = new.register_memory(vec![0x98; 18].into(), Permission::RemoteRead)?;
    let l = dst.register_memory(vec![0xda; 18].into(), Permission::LocalOnly)?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    // Keep the listener owned across generations. No released-port reuse race.
    let old_epoch = old.source_epoch();
    let new_epoch = new.source_epoch();
    let forward = std::thread::spawn(move || {
        for target in [
            old.endpoint(),
            old.endpoint(),
            new.endpoint(),
            new.endpoint(),
        ] {
            let (mut client, _) = listener.accept().unwrap();
            client.set_read_timeout(Some(WAIT)).unwrap();
            client.set_write_timeout(Some(WAIT)).unwrap();
            let mut header = [0; control_plane::REQUEST_SIZE];
            client.read_exact(&mut header).unwrap();
            let mut upstream =
                control_plane::ControlChannel::connect(target, Instant::now() + WAIT).unwrap();
            upstream.write_all(&header).unwrap();
            let epoch = SourceEpoch(header[12..28].try_into().unwrap());
            let reply = control_plane::Reply::read(&mut upstream, epoch).unwrap();
            client.write_all(&reply.encode()).unwrap();
            let mut body = vec![0; reply.length as usize];
            upstream.read_exact(&mut body).unwrap();
            client.write_all(&body).unwrap();
        }
        (old, new)
    });
    // Epochs belong to the source registrations, not to the relay address.
    let p = dst.open_segment(address, old_epoch, WAIT)?;
    let request = |r: &RegisteredMemory| {
        TransferRequest::new(
            Transport::Tcp,
            l.base_address() + 1,
            r.base_address() + 1,
            16,
        )
    };
    assert!(
        dst.submit_and_wait(TransferOp::Read, &p, &[request(&old_r)], WAIT)?
            .is_complete()
    );
    dst.close_segment(address, old_epoch, WAIT)?;
    let fresh = dst.open_segment(address, new_epoch, WAIT)?;
    assert!(
        dst.submit_and_wait(TransferOp::Read, &fresh, &[request(&new_r)], WAIT)?
            .is_complete()
    );
    assert!(matches!(
        dst.open_segment(address, old_epoch, WAIT),
        Err(Error::PeerClosed)
    ));
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Read, &p, &[request(&old_r)], WAIT),
        Err(Error::PeerClosed)
    ));
    let mut want = vec![0xda; 18];
    want[1..17].fill(0x98);
    assert_eq!(bytes(&l), want);
    forward.join().unwrap();
    Ok(())
}

#[test]
fn peer_retirement_includes_admitted_notification_retries() -> Result<()> {
    let src = TransferEngine::new(EngineConfig::default())?;
    let dst = TransferEngine::new(EngineConfig::default())?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let note = Notification {
        name: "done".into(),
        message: "payload".into(),
    };
    let mut hook =
        dst.notification_routes
            .hook(dst.local_target(), std::slice::from_ref(&peer), 11)?;
    hook.admit(&dst)?;
    assert!(matches!(
        dst.close_segment(peer.endpoint, peer.epoch, Duration::ZERO),
        Err(Error::Unquiesced { .. })
    ));
    let blocked = dst.send_notification(&peer, &note, WAIT).unwrap_err();
    assert!(matches!(blocked.error, Error::PeerClosed));
    assert!(matches!(
        *hook.attempt(&dst, &note, Instant::now() + WAIT),
        NotificationStatus::Delivered
    ));
    drop(hook);
    dst.close_segment(peer.endpoint, peer.epoch, WAIT)?;
    let got = src.receive_notifications()?;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message, "payload");
    Ok(())
}

#[test]
fn peer_retirement_returns_an_automatic_window_only_with_its_receipt() -> Result<()> {
    let source = TransferEngine::new(EngineConfig {
        receiver_credit: Some(Default::default()),
        ..Default::default()
    })?;
    let dst = TransferEngine::new(EngineConfig {
        max_receiver_credit_sessions: 2,
        ..Default::default()
    })?;
    let peer = dst.open_segment(source.endpoint(), source.source_epoch(), WAIT)?;
    dst.connect_receiver_credit(&peer, 7, WAIT)?;
    assert_eq!(
        source
            .receiver_credit_issuer_stats()
            .unwrap()
            .active_senders,
        1
    );
    dst.close_segment(peer.endpoint, peer.epoch, WAIT)?;
    let released = source.receiver_credit_issuer_stats().unwrap();
    assert_eq!((released.active_senders, released.reserved_bytes), (0, 0));
    assert!(matches!(
        dst.connect_receiver_credit(&peer, 7, WAIT),
        Err(Error::PeerClosed)
    ));
    assert!(matches!(
        dst.activate_receiver_credit(&peer, 1, 7),
        Err(Error::PeerClosed)
    ));
    Ok(())
}
