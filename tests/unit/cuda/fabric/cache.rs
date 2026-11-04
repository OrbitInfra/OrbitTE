use super::*;

#[test]
fn concurrent_ranges_reuse_one_bounded_import_on_the_execution_gpu() -> Result<()> {
    let mut c = config();
    c.nvlink_cache_entries = 1;
    c.max_failover_attempts = 0;
    let sr = Rig::new();
    let dr = Rig::new().single();
    let src = sr.engine(c.clone())?;
    let dst = dr.engine(c)?;
    let owner = src.allocate_mnnvl(1, 64)?;
    owner.write(0, &[0x6a; 64])?;
    let r = src
        .register_cuda_memory_batch(&[(owner, 2, 58)], Permission::RemoteRead)?
        .remove(0);
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let barrier = std::sync::Barrier::new(6);
    std::thread::scope(|s| {
        let mut threads = Vec::new();
        for i in 0..6 {
            let r = &r;
            let p = &p;
            let e = &dst;
            let start = &barrier;
            threads.push(s.spawn(move || -> Result<()> {
                let l = e.register_memory(vec![0xff; 16].into(), Permission::LocalOnly)?;
                start.wait();
                let rep = e.submit_and_wait(
                    TransferOp::Read,
                    p,
                    &[q(Transport::Mnnvl, &l, r, 2, 4 * i, 9)],
                    WAIT,
                )?;
                assert!(rep.is_complete(), "{rep:?}");
                let mut want = [0xff; 16];
                want[2..11].fill(0x6a);
                assert_eq!(read(&l), want);
                Ok(())
            }));
        }
        for t in threads {
            t.join().unwrap()?
        }
        Ok::<(), Error>(())
    })?;
    assert_eq!(dr.calls("import"), 1);
    dst.flush_nvlink_cache(WAIT)?;
    assert_eq!(dr.calls("unmap"), 1);
    assert_eq!(read(&r), [0x6a; 58]);
    Ok(())
}

#[test]
fn clean_export_falls_through_and_shared_host_copies_hold_inode_loans() -> Result<()> {
    let sr = Rig::new();
    let dr = Rig::new();
    let src = sr.engine(config())?;
    let dst = dr.engine(config())?;
    let m = src.allocate_mnnvl(0, 32)?;
    m.write(0, &[0x86; 32])?;
    sr.fail("export");
    let r = src
        .register_cuda_memory_batch(&[(m, 2, 24)], Permission::RemoteRead)?
        .remove(0);
    let l = dst.register_memory(vec![0xee; 16].into(), Permission::LocalOnly)?;
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &p,
            &[q(Transport::Mnnvl, &l, &r, 2, 3, 8)],
            WAIT
        )
        .is_err()
    );
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &p,
            &[q(Transport::Unspec, &l, &r, 2, 3, 8)],
            WAIT
        )?
        .is_complete()
    );
    assert_eq!(dr.calls("import"), 0);
    let mut want = [0xee; 16];
    want[2..10].fill(0x86);
    assert_eq!(read(&l), want);
    let mut c = config();
    c.shm.enable = true;
    let mixed = Arc::new(dr.engine(c)?);
    let shm = mixed.allocate_shm_memory(16)?;
    let l = mixed.register_host_memory(shm.clone(), 0, 16, Permission::LocalOnly)?;
    let (_, r) = gpu(&src, 32, Permission::RemoteRead)?;
    r.write(0, &[0x6b; 32])?;
    let p = mixed.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    dr.gpus[0]
        .query_hold_address
        .store((l.base_address() + 2) as usize, Ordering::Release);
    let b = mixed.submit_async(
        TransferOp::Read,
        &p,
        &[q(Transport::Mnnvl, &l, &r, 2, 3, 8)],
        None,
        WAIT,
    )?;
    let end = std::time::Instant::now() + WAIT;
    while dr.gpus[0].event_not_ready.load(Ordering::Acquire) == 0 {
        assert!(std::time::Instant::now() < end);
        std::thread::yield_now()
    }
    assert!(matches!(
        shm.shared().unwrap().access(&(1..6), true),
        Err(Error::Unavailable(_))
    ));
    dr.gpus[0].query_hold_address.store(0, Ordering::Release);
    assert_eq!(b.wait(WAIT)?.completed_tasks, 1);
    let mut want = [0; 16];
    want[2..10].fill(0x6b);
    assert_eq!(read(&l), want);
    assert_eq!(dr.calls("import"), 1);
    Ok(())
}
#[cfg(feature = "metrics")]
#[test]
fn real_submission_labels_mnnvl_bytes_attempts_and_registered_capacity() -> Result<()> {
    let sr = Rig::new();
    let dr = Rig::new();
    let src = sr.engine(config())?;
    let mut c = config();
    c.metrics.enabled = true;
    c.metrics.report_interval_seconds = 0;
    let dst = dr.engine(c)?;
    let (_, r) = gpu(&src, 12, Permission::RemoteReadWrite)?;
    let (_, l) = gpu(&dst, 12, Permission::LocalOnly)?;
    l.write(0, &[0x62; 12])?;
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    assert!(
        dst.submit_and_wait(
            TransferOp::Write,
            &p,
            &[q(Transport::Mnnvl, &l, &r, 2, 3, 6)],
            WAIT
        )?
        .is_complete()
    );
    let text = dst.metrics().unwrap().prometheus();
    for line in [
        r#"tent_write_bytes_total{transport="mnnvl"} 6"#,
        r#"tent_transport_attempts_total{transport="mnnvl",operation="write"} 1"#,
        r#"tent_registered_buffer_bytes{transport="mnnvl"} 12"#,
    ] {
        assert!(text.contains(line), "{line}");
    }
    let mut want = [0; 12];
    want[3..9].fill(0x62);
    assert_eq!(read(&r), want);
    Ok(())
}
