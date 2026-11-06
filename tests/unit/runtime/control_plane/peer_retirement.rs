use super::*;
use std::sync::{atomic::Ordering, mpsc};

#[test]
fn peer_retirement_tcp_reply_drain_keeps_backing_and_allows_another_peer() {
    let wait = common::EVENTUAL_PROGRESS;
    let e = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());
    let (listener, mut peer) = fault_peer();
    e.bind_test_peer(&mut peer).unwrap();

    let fixture = common::external_memory(&[0xcd; 10]);
    let memory = fixture.memory.clone();
    let local = e
        .register_host_memory(fixture.memory, 0, 10, Permission::LocalOnly)
        .unwrap();
    let q = TransferRequest::new(
        crate::Transport::Tcp,
        local.base_address() + 1,
        peer.regions[0].base_address(),
        8,
    );
    let batch = e
        .submit_async(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&q),
            None,
            wait,
        )
        .unwrap();
    let (mut stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(wait)).unwrap();
    stream.set_write_timeout(Some(wait)).unwrap();
    stream.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
    e.unregister_memory(&local).unwrap();
    drop(local);
    assert!(matches!(
        e.close_segment(peer.endpoint, peer.epoch, Duration::ZERO),
        Err(Error::Unquiesced { .. })
    ));
    assert!(matches!(
        e.submit_and_wait(TransferOp::Read, &peer, &[q], wait),
        Err(Error::PeerClosed)
    ));
    assert_eq!(e.stats().unwrap().inflight_bytes, 8);
    assert_eq!(e.stats().unwrap().active_peer_operations, 1);
    assert!(fixture.owner.upgrade().is_some());
    let other = TransferEngine::new(EngineConfig::default()).unwrap();
    let r = other
        .register_memory(vec![0x83; 10].into(), Permission::RemoteRead)
        .unwrap();
    let p = e
        .open_segment(other.endpoint(), other.source_epoch(), wait)
        .unwrap();
    let l = e
        .register_memory(vec![0xab; 10].into(), Permission::LocalOnly)
        .unwrap();
    let q = TransferRequest::new(
        crate::Transport::Tcp,
        l.base_address() + 1,
        r.base_address() + 1,
        8,
    );
    assert!(
        e.submit_and_wait(TransferOp::Read, &p, &[q], wait)
            .unwrap()
            .is_complete()
    );
    let mut got = [0; 10];
    l.read(0, &mut got).unwrap();
    assert_eq!(
        got,
        [0xab, 0x83, 0x83, 0x83, 0x83, 0x83, 0x83, 0x83, 0x83, 0xab]
    );
    let (done, finished) = mpsc::channel();
    let engine = e.clone();
    let endpoint = peer.endpoint;
    let epoch = peer.epoch;
    let close = thread::spawn(move || {
        let r = engine.close_segment(endpoint, epoch, wait);
        done.send(()).unwrap();
        r
    });
    assert!(finished.try_recv().is_err());
    stream
        .write_all(
            &Reply {
                status: 0,
                epoch,
                length: 8,
            }
            .encode(),
        )
        .unwrap();
    stream.write_all(&[0x52; 8]).unwrap();
    assert_eq!(batch.wait(wait).unwrap().completed_tasks, 1);
    close.join().unwrap().unwrap();
    memory.read(0, &mut got).unwrap();
    assert_eq!(
        got,
        [0xcd, 0x52, 0x52, 0x52, 0x52, 0x52, 0x52, 0x52, 0x52, 0xcd]
    );
    drop((memory, batch));
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(e.stats().unwrap().inflight_bytes, 0);
    assert_eq!(e.stats().unwrap().active_peer_operations, 0);
}
