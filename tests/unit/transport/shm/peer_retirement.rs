use super::*;

#[test]
fn peer_retirement_shm_preserves_prepared_mapping_and_other_peer_then_unmaps() -> Result<()> {
    static UNMAPS: AtomicUsize = AtomicUsize::new(0);
    fn unmap(p: std::ptr::NonNull<u8>, n: usize) -> io::Result<()> {
        UNMAPS.fetch_add(1, Ordering::SeqCst);
        (NATIVE.unmap)(p, n)
    }
    static TABLE: Sys = Sys { unmap, ..NATIVE };
    let dir = Dir::new();
    let cfg = config(&dir.0);
    let src = engine(cfg.clone(), &NATIVE)?;
    let other = engine(cfg.clone(), &NATIVE)?;
    let dst = engine(cfg, &TABLE)?;
    let a = src.allocate_shm_memory(18)?;
    a.write(0, &[0x61; 18])?;
    let r = grant(&src, &a, 0, 18, Permission::RemoteReadWrite);
    let b = other.allocate_shm_memory(18)?;
    b.write(0, &[0x73; 18])?;
    let rr = grant(&other, &b, 0, 18, Permission::RemoteReadWrite);
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let pp = dst.open_segment(other.endpoint(), other.source_epoch(), WAIT)?;
    let l = dst.register_memory(vec![0xcd; 18].into(), Permission::LocalOnly)?;
    let ll = dst.register_memory(vec![0xab; 18].into(), Permission::LocalOnly)?;
    let q = req(Transport::Shm, &l, &r, 1, 16);
    let qq = req(Transport::Shm, &ll, &rr, 1, 16);
    // Prepare owns the actual peer mapping and its process-liveness range lock.
    let admitted = dst.prepare(
        TransferOp::Read,
        &p,
        std::slice::from_ref(&q),
        None,
        WAIT,
        None,
    )?;
    assert!(matches!(
        dst.close_segment(p.endpoint, p.epoch, Duration::ZERO),
        Err(Error::Unquiesced { .. })
    ));
    assert_eq!(UNMAPS.load(Ordering::SeqCst), 0);
    assert_eq!(dst.stats()?.inflight_bytes, 16);
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Read, &p, &[q], WAIT),
        Err(Error::PeerClosed)
    ));
    assert!(
        dst.submit_and_wait(TransferOp::Write, &pp, std::slice::from_ref(&qq), WAIT)?
            .is_complete()
    );
    let result = dst.execute(&admitted, None);
    assert!(result.is_complete(), "{result:?}");
    drop(admitted);
    dst.close_segment(p.endpoint, p.epoch, WAIT)?;
    assert_eq!(UNMAPS.load(Ordering::SeqCst), 1);
    assert!(
        dst.submit_and_wait(TransferOp::Write, &pp, &[qq], WAIT)?
            .is_complete()
    );
    assert_eq!(UNMAPS.load(Ordering::SeqCst), 1);
    let mut want = vec![0xcd; 18];
    want[1..17].fill(0x61);
    assert_eq!(bytes(&l), want);
    let mut want = vec![0x73; 18];
    want[1..17].fill(0xab);
    assert_eq!(bytes(&rr), want);
    assert_eq!(bytes(&r), vec![0x61; 18]);
    Ok(())
}

#[test]
fn peer_retirement_failed_shm_unmap_is_sticky_and_does_not_fence_other_peers() -> Result<()> {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    fn fail_once(p: std::ptr::NonNull<u8>, n: usize) -> io::Result<()> {
        if CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(io::Error::from_raw_os_error(libc::EIO))
        } else {
            (NATIVE.unmap)(p, n)
        }
    }
    static TABLE: Sys = Sys {
        unmap: fail_once,
        ..NATIVE
    };
    let dir = Dir::new();
    let c = config(&dir.0);
    let src = engine(c.clone(), &NATIVE)?;
    let other = engine(c.clone(), &NATIVE)?;
    let dst = engine(c, &TABLE)?;
    let a = src.allocate_shm_memory(18)?;
    a.write(0, &[0x81; 18])?;
    let r = grant(&src, &a, 0, 18, Permission::RemoteRead);
    let b = other.allocate_shm_memory(18)?;
    b.write(0, &[0x28; 18])?;
    let rr = grant(&other, &b, 0, 18, Permission::RemoteRead);
    let l = dst.register_memory(vec![0xec; 18].into(), Permission::LocalOnly)?;
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let pp = dst.open_segment(other.endpoint(), other.source_epoch(), WAIT)?;
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &p,
            &[req(Transport::Shm, &l, &r, 1, 16)],
            WAIT
        )?
        .is_complete()
    );
    let name = a.shared().unwrap().mapping.name.basename();
    for _ in 0..2 {
        assert!(matches!(
            dst.close_segment(p.endpoint, p.epoch, WAIT),
            Err(Error::Unquiesced {
                operation: "SHM munmap",
                ..
            })
        ));
    }
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    assert!(
        fs::read_to_string("/proc/self/maps")?
            .lines()
            .any(|line| line.ends_with(&name))
    );
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &pp,
            &[req(Transport::Shm, &l, &rr, 1, 16)],
            WAIT
        )?
        .is_complete()
    );
    let mut want = vec![0xec; 18];
    want[1..17].fill(0x28);
    assert_eq!(bytes(&l), want);
    assert!(matches!(
        dst.close_requester(WAIT),
        Err(Error::Unquiesced { .. })
    ));
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    Ok(())
}
