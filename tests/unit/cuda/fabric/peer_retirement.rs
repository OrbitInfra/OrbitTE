use super::*;

#[test]
fn peer_retirement_mnnvl_event_drain_and_failed_native_cleanup_are_per_peer() -> Result<()> {
    for failure in [None, Some("unmap"), Some("release")] {
        let sr = Rig::new();
        let or = Rig::new();
        let dr = Rig::new().single();
        let mut c = config();
        c.nvlink_cache_entries = 4;
        let src = sr.engine(c.clone())?;
        let other = or.engine(c.clone())?;
        let dst = Arc::new(dr.engine(c)?);
        let (_, r) = gpu(&src, 32, Permission::RemoteReadWrite)?;
        r.write(0, &[0x52; 32])?;
        let (_, rr) = gpu(&other, 32, Permission::RemoteReadWrite)?;
        rr.write(0, &[0x67; 32])?;
        let (_, l) = gpu(&dst, 32, Permission::LocalOnly)?;
        l.write(0, &[0xcd; 32])?;
        let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
        let pp = dst.open_segment(other.endpoint(), other.source_epoch(), WAIT)?;
        let request = q(Transport::Mnnvl, &l, &r, 2, 3, 11);
        dr.gpus[0]
            .query_hold_address
            .store((l.base_address() + 2) as usize, Ordering::Release);
        let active = dst.submit_async(
            TransferOp::Read,
            &p,
            std::slice::from_ref(&request),
            None,
            WAIT,
        )?;
        let end = std::time::Instant::now() + WAIT;
        while dr.gpus[0].event_not_ready.load(Ordering::Acquire) == 0 {
            assert!(std::time::Instant::now() < end);
            std::thread::yield_now();
        }
        assert!(matches!(
            dst.close_segment(p.endpoint, p.epoch, Duration::ZERO),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(dst.stats()?.inflight_bytes, 11);
        assert_eq!(dr.calls("unmap"), 0);
        assert!(matches!(
            dst.submit_and_wait(TransferOp::Read, &p, &[request], WAIT),
            Err(Error::PeerClosed)
        ));
        dr.gpus[0].query_hold_address.store(0, Ordering::Release);
        assert_eq!(active.wait(WAIT)?.completed_tasks, 1);
        let mut want = [0xcd; 32];
        want[2..13].fill(0x52);
        assert_eq!(read(&l), want);
        assert!(
            dst.submit_and_wait(
                TransferOp::Read,
                &pp,
                &[q(Transport::Mnnvl, &l, &rr, 2, 3, 11)],
                WAIT
            )?
            .is_complete()
        );
        if let Some(f) = failure {
            dr.fail(f);
        }
        let before = dr.calls("unmap");
        for _ in 0..2 {
            let result = dst.close_segment(p.endpoint, p.epoch, WAIT);
            if failure.is_some() {
                assert!(matches!(result, Err(Error::Unquiesced { .. })));
            } else {
                result?;
            }
        }
        assert_eq!(dr.calls("unmap"), before + 1);
        assert_eq!(
            dst.stats()?.nvlink.unwrap().retained_imports,
            usize::from(failure.is_some())
        );
        assert!(
            dst.submit_and_wait(
                TransferOp::Read,
                &pp,
                &[q(Transport::Mnnvl, &l, &rr, 2, 3, 11)],
                WAIT
            )?
            .is_complete()
        );
        assert_eq!(dr.calls("import"), 2);
        want[2..13].fill(0x67);
        assert_eq!(read(&l), want);
        if failure.is_some() {
            assert!(matches!(
                r.read(0, &mut [0; 32]),
                Err(Error::Unavailable(_))
            ));
        } else {
            assert_eq!(read(&r), [0x52; 32]);
        }
        dst.close_segment(pp.endpoint, pp.epoch, WAIT)?;
        if failure.is_some() {
            assert!(matches!(
                dst.close_requester(WAIT),
                Err(Error::Unquiesced { .. })
            ));
        }
    }
    Ok(())
}
