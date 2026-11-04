use super::*;

#[test]
fn peer_grant_checks_domain_handle_kind_permissions_and_epoch_before_exposure() -> Result<()> {
    let sr = Rig::new();
    let src = sr.engine(config())?;
    let (_, r) = gpu(&src, 32, Permission::RemoteRead)?;
    r.write(0, &[0x73; 32])?;
    for wrong in ["domain", "kind", "permission"] {
        let rig = Rig::new();
        let mut c = config();
        c.max_failover_attempts = 0;
        if wrong == "domain" {
            c.mnnvl.as_mut().unwrap().fabric_domain = Some(SourceEpoch([0x45; 16]));
        }
        if wrong == "kind" {
            c.mnnvl.as_mut().unwrap().handle_type = MnnvlHandleType::PosixFd
        }
        let dst = rig.engine(c)?;
        let l = dst.register_memory(vec![0x9a; 16].into(), Permission::LocalOnly)?;
        let mut p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
        let own = dst.open_segment(dst.endpoint(), dst.source_epoch(), WAIT)?;
        p.metadata.mnnvl = own.metadata.mnnvl;
        for reg in &mut p.regions {
            reg.permission = Permission::RemoteReadWrite;
        }
        let op = if wrong == "permission" {
            TransferOp::Write
        } else {
            TransferOp::Read
        };
        let req = q(Transport::Mnnvl, &l, &r, 2, 3, 8);
        let report = dst.submit_and_wait(op, &p, &[req], WAIT)?;
        assert_eq!(report.completed_requests, 0);
        assert_eq!(report.failure.unwrap().remote_effect, RemoteEffect::None);
        assert_eq!(rig.calls("import"), 0);
        assert_eq!(src.stats()?.nvlink.unwrap().exports, 0);
        assert_eq!(read(&l), [0x9a; 16]);
        assert_eq!(read(&r), [0x73; 32]);
        assert!(matches!(
            dst.open_segment(src.endpoint(), SourceEpoch([7; 16]), WAIT),
            Err(Error::EpochMismatch)
        ));
    }
    Ok(())
}

#[test]
fn posix_grant_drives_checked_descriptor_relocation_for_both_directions() -> Result<()> {
    let mut c = config();
    c.mnnvl.as_mut().unwrap().handle_type = MnnvlHandleType::PosixFd;
    c.nvlink_cache_entries = 1;
    let sr = Rig::new();
    let dr = Rig::new();
    let src = sr.engine(c.clone())?;
    let dst = dr.engine(c)?;
    let (_, r) = gpu(&src, 32, Permission::RemoteReadWrite)?;
    r.write(0, &[0x13; 32])?;
    let l = dst.register_memory(vec![0xee; 24].into(), Permission::LocalOnly)?;
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let req = q(Transport::Mnnvl, &l, &r, 2, 5, 11);
    assert!(
        dst.submit_and_wait(TransferOp::Read, &p, std::slice::from_ref(&req), WAIT)?
            .is_complete()
    );
    let mut want = [0xee; 24];
    want[2..13].fill(0x13);
    assert_eq!(read(&l), want);
    l.write(2, &[0xaf; 11])?;
    assert!(
        dst.submit_and_wait(TransferOp::Write, &p, &[req], WAIT)?
            .is_complete()
    );
    dst.flush_nvlink_cache(WAIT)?;
    assert_eq!(dr.calls("import"), 1);
    let mut want = [0x13; 32];
    want[5..16].fill(0xaf);
    assert_eq!(read(&r), want);
    Ok(())
}

#[test]
fn a_failed_host_unregister_retains_the_host_registration_and_its_charge() -> Result<()> {
    let rig = Rig::new();
    let mut config = config();
    config.max_registered_regions = 1;
    let e = rig.engine(config)?;
    let host = Arc::new(crate::HostMemory::new(vec![0xa5; 32].into())?);
    let weak = Arc::downgrade(&host);
    let r = e.register_host_memory(host.clone(), 0, 32, Permission::LocalOnly)?;
    rig.fail("host-unregister");
    assert!(matches!(
        e.unregister_memory(&r),
        Err(Error::Unquiesced { .. })
    ));
    assert!(matches!(
        host.read(0, &mut [0; 1]),
        Err(Error::Unquiesced { .. })
    ));
    drop((host, r));
    assert!(weak.upgrade().is_some());
    assert_eq!(rig.calls("host-unregister"), 1);
    assert_eq!(e.stats()?.retained_bytes, 32);
    assert!(matches!(
        e.register_memory(vec![0; 1].into(), Permission::LocalOnly),
        Err(Error::ResourceExhausted)
    ));
    Ok(())
}
