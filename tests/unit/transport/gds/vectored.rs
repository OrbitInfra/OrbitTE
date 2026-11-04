use super::*;

#[test]
fn vectored_gds_depth_and_short_receipts_preserve_every_later_destination() -> Result<()> {
    const PAGE: usize = 4096;
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(
        &mock,
        EngineConfig {
            gds_batch_depth: 2,
            ..Default::default()
        },
        PAGE * 7,
    )?;
    let host = f.host(PAGE * 8)?;
    let aligned = host.base_address().div_ceil(PAGE as u64) * PAGE as u64;
    let offsets = [4, 0, 3, 1, 2].map(|i| (aligned - host.base_address()) as usize + i * PAGE);
    let spans = offsets.map(|i| (host.base_address() + i as u64, PAGE));
    let req = TransferRequest::file_vectored(Transport::Gds, PAGE as u64, &spans)?;
    let mut image = vec![0xc1; host.len()];
    for (i, &start) in offsets.iter().enumerate() {
        image[start..start + PAGE].fill(i as u8 + 17);
    }
    host.write(0, &image)?;
    let r = f.engine.submit_and_wait(
        TransferOp::Write,
        &f.segment,
        std::slice::from_ref(&req),
        WAIT,
    )?;
    assert!(r.is_complete(), "{r:?}");
    assert_eq!(mock.submits.load(Ordering::Acquire), 3);
    let mut file = vec![0; PAGE * 7];
    for (n, &off) in offsets.iter().enumerate() {
        file[(n + 1) * PAGE..(n + 2) * PAGE].copy_from_slice(&image[off..off + PAGE]);
    }
    assert_eq!(fs::read(&f.path)?, file);
    for size in [PAGE * 5, PAGE + 73] {
        host.write(0, &vec![0xa7; host.len()])?;
        if size % PAGE != 0 {
            *mock.script.lock().unwrap() = vec![(COMPLETE, PAGE), (COMPLETE, 73)];
        }
        let before = mock.submits.load(Ordering::Acquire);
        let b = f.engine.submit_async(
            TransferOp::Read,
            &f.segment,
            std::slice::from_ref(&req),
            None,
            WAIT,
        )?;
        assert!(b.wait(EVENTUAL_PROGRESS)?.locally_drained);
        assert_eq!(
            mock.submits.load(Ordering::Acquire) - before,
            size.div_ceil(PAGE)
        );
        let status = b.task_status(0)?;
        assert_eq!(status.transferred_bytes, size);
        assert_eq!(status.failure.is_some(), size != PAGE * 5);
        let mut expected = vec![0xa7; host.len()];
        for (n, &off) in offsets.iter().enumerate() {
            let count = size.saturating_sub(n * PAGE).min(PAGE);
            expected[off..off + count].copy_from_slice(&image[off..off + count]);
        }
        let mut got = vec![0; host.len()];
        host.read(0, &mut got)?;
        assert_eq!(got, expected);
    }
    Ok(())
}

#[cfg(feature = "io-uring")]
#[test]
fn vectored_unaligned_unspec_stages_and_an_aligned_failed_write_never_replays() -> Result<()> {
    if !crate::transport::io_uring::tests::kernel() {
        return Ok(());
    }
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(&mock, EngineConfig::default(), 8192)?;
    let host = f.host(16384)?;
    let base = host.base_address().div_ceil(4096) * 4096;
    let spans = [(base + 1, 2), (base + 3, 2)];
    let strict = TransferRequest::file_vectored(Transport::Gds, 2, &spans)?;
    assert!(matches!(
        f.engine
            .submit_and_wait(TransferOp::Write, &f.segment, &[strict], WAIT),
        Err(Error::Unavailable(_))
    ));
    let req = TransferRequest::file_vectored(Transport::Unspec, 2, &spans)?;
    assert!(
        f.engine
            .submit_and_wait(TransferOp::Write, &f.segment, &[req], WAIT)?
            .is_complete()
    );
    assert_eq!(mock.opens.load(Ordering::Acquire), 0);
    let mut expected = vec![0; 8192];
    expected[2..6].fill(0x6b);
    assert_eq!(fs::read(&f.path)?, expected);
    // Even a clean setup error cannot replay an accepted vectored WRITE on io_uring.
    mock.setup_error.store(true, Ordering::Release);
    let req =
        TransferRequest::file_vectored(Transport::Unspec, 0, &[(base, 4096), (base + 4096, 4096)])?;
    let r = f
        .engine
        .submit_and_wait(TransferOp::Write, &f.segment, &[req], WAIT)?;
    assert!(r.failure.is_some() && r.locally_drained);
    assert_eq!(mock.submits.load(Ordering::Acquire), 0);
    assert_eq!(fs::read(&f.path)?, expected);
    Ok(())
}

#[test]
fn vectored_unconfirmed_destroy_keeps_unissued_ranges_and_file_admission() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    mock.destroy_error.store(true, Ordering::Release);
    let f = Fixture::new(&mock, EngineConfig::default(), 8192)?;
    let host = f.host(16384)?;
    let base = host.base_address().div_ceil(4096) * 4096;
    let start = (base - host.base_address()) as usize;
    let req =
        TransferRequest::file_vectored(Transport::Gds, 0, &[(base, 4096), (base + 4096, 4096)])?;
    let b = f
        .engine
        .submit_async(TransferOp::Read, &f.segment, &[req], None, WAIT)?;
    assert!(!b.wait(EVENTUAL_PROGRESS)?.locally_drained);
    let t = b.task_status(0)?;
    assert!(matches!(t.failure.unwrap().error, Error::Unquiesced { .. }));
    assert_eq!(t.transferred_bytes, 4096);
    assert_eq!(mock.submits.load(Ordering::Acquire), 1);
    assert_eq!(f.engine.stats()?.inflight_bytes, 8192);
    assert!(host.write(start, &[1]).is_err());
    assert!(host.write(start + 4096, &[1]).is_err());
    assert!(matches!(
        f.engine.close_file_segment(&f.segment, Duration::ZERO),
        Err(Error::Timeout)
    ));
    Ok(())
}

#[cfg(feature = "io-uring")]
#[test]
fn vectored_read_with_a_visible_prefix_cannot_restart_at_zero_after_a_failed_cookie() -> Result<()>
{
    if !crate::transport::io_uring::tests::kernel() {
        return Ok(());
    }
    const PAGE: usize = 4096;
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(&mock, EngineConfig::default(), PAGE * 2)?;
    let file: Vec<_> = (0..PAGE * 2).map(|i| (i % 227) as u8).collect();
    fs::write(&f.path, &file)?;
    let host = f.host(PAGE * 4)?;
    let base = host.base_address().div_ceil(PAGE as u64) * PAGE as u64;
    *mock.script.lock().unwrap() = vec![(COMPLETE, PAGE), (FAILED, 0)];
    let req = TransferRequest::file_vectored(
        Transport::Unspec,
        0,
        &[(base + PAGE as u64, PAGE), (base, PAGE)],
    )?;
    let b = f
        .engine
        .submit_async(TransferOp::Read, &f.segment, &[req], None, WAIT)?;
    assert!(b.wait(EVENTUAL_PROGRESS)?.locally_drained);
    let receipt = b.task_status(0)?;
    assert!(matches!(
        receipt.failure.unwrap().error,
        Error::Backend {
            operation: "cuFile IO",
            ..
        }
    ));
    assert_eq!(receipt.transferred_bytes, PAGE);
    let mut expected = vec![0x6b; host.len()];
    let off = (base - host.base_address()) as usize + PAGE;
    expected[off..off + PAGE].copy_from_slice(&file[..PAGE]);
    let mut got = vec![0; host.len()];
    host.read(0, &mut got)?;
    assert_eq!(got, expected);
    Ok(())
}
