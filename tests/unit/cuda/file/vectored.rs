use super::*;
use crate::FileIoElement::Padding;

#[test]
fn vectored_mixed_gpu_layers_host_ranges_and_padding_share_chunks_and_one_file_receipt()
-> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let mut f = fixture(&[0xaf; 256], 32, 2, false)?;
    let truncate = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let armed = truncate.clone();
    let path = f.path.clone();
    let trace = f.observe_ring(false, move |_| {
        let limit = armed.swap(0, Ordering::AcqRel);
        if limit != 0 {
            fs::OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(3 + limit as u64)?;
        }
        Ok(())
    });
    let device = allocation(&[0x31; 256]);
    let (_, gpu) = register(&f, &device)?;
    let second = allocation(&[0x32; 256]);
    let (_, gpu2) = register(&f, &second)?;
    let host = f.memory.clone();
    let views = [&gpu, &gpu2, &host];
    let mut images = [[0x7e; 256]; 3];
    let mut ranges = vec![Padding(6)];
    let mut layout = Vec::new();
    let mut packed = vec![0; 6];
    for i in 0..29 {
        if i == 8 {
            // This gap crosses the first 32-byte staging chunk.
            ranges.push(Padding(20));
            packed.resize(packed.len() + 20, 0);
        }
        let owner = i % 3;
        let start = 200 - i * 5;
        let len = 1 + i % 4;
        images[owner][start..start + len].fill(i as u8 + 7);
        layout.push((owner, start, len, packed.len()));
        packed.extend_from_slice(&images[owner][start..start + len]);
        ranges.push((views[owner].base_address() + start as u64, len).into());
    }
    ranges.push(Padding(7));
    packed.resize(packed.len() + 7, 0);
    let req = TransferRequest::file_vectored(Transport::Unspec, 3, &ranges)?;
    // Dirty both reusable chunks before proving that each WRITE gap is zero.
    assert!(
        f.run(TransferOp::Read, std::slice::from_ref(&req))?
            .is_complete()
    );
    for (view, image) in views.iter().zip(&images) {
        view.write(0, image)?;
    }
    let report = f.run(TransferOp::Write, std::slice::from_ref(&req))?;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (1, packed.len())
    );
    let mut file = [0xaf; 256];
    file[3..3 + packed.len()].copy_from_slice(&packed);
    assert_eq!(fs::read(&f.path)?, file);
    assert!(!trace.lock().unwrap().round_widths.is_empty());
    for limit in [packed.len(), 60, 34] {
        for view in &views {
            view.write(0, &[0xee; 256])?;
        }
        // Truncate only after the valid, full extent has been admitted.
        if limit < packed.len() {
            fs::write(&f.path, file)?;
            truncate.store(limit, Ordering::Release);
        }
        let b = f.engine.submit_async(
            TransferOp::Read,
            &f.segment,
            std::slice::from_ref(&req),
            None,
            WAIT,
        )?;
        assert!(
            b.wait(crate::test_support::EVENTUAL_PROGRESS)?
                .locally_drained
        );
        assert_eq!(b.task_statuses()?.len(), 1);
        let status = b.task_status(0)?;
        assert_eq!(status.transferred_bytes, limit);
        assert_eq!(status.state == TaskState::Failed, limit != packed.len());
        let report = b.submissions()?.remove(0);
        assert_eq!(
            report.transferred_bytes,
            if limit == packed.len() { limit } else { 0 }
        );
        let mut expected = [[0xee; 256]; 3];
        for &(owner, start, len, cursor) in &layout {
            let n = limit.saturating_sub(cursor).min(len);
            expected[owner][start..start + n].copy_from_slice(&packed[cursor..cursor + n]);
        }
        for (view, want) in views.iter().zip(&expected) {
            let mut got = [0; 256];
            view.read(0, &mut got)?;
            assert_eq!(&got, want);
        }
    }
    Ok(())
}

#[test]
fn vectored_cancel_and_close_keep_every_range_until_the_copy_event_drains() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = fixture(b"................", 8, 1, true)?;
    let device = allocation(b"xABxCDxEFxGH");
    let (_, v) = register(&f, &device)?;
    let vector = |off, i, j| {
        TransferRequest::file_vectored(
            Transport::IoUring,
            off,
            &[
                (v.base_address() + i, 2).into(),
                Padding(2),
                (v.base_address() + j, 2).into(),
            ],
        )
    };
    let (entered, wait) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    *device.event_gate.lock().unwrap() = Some((entered, gate));
    let running = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[vector(1, 1, 4)?],
        None,
        WAIT,
    )?;
    wait.recv_timeout(crate::test_support::EVENTUAL_PROGRESS)
        .unwrap();
    let queued = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[vector(7, 7, 10)?],
        None,
        WAIT,
    )?;
    assert!(queued.cancel(0)?);
    assert!(queued.wait(WAIT)?.locally_drained);
    assert!(matches!(
        queued.task_status(0)?.failure.unwrap().error,
        Error::Cancelled
    ));
    assert!(running.cancel(0)?);
    assert_eq!(f.engine.stats()?.inflight_bytes, 6);
    assert!(v.write(1, &[0]).is_err() && v.write(4, &[0]).is_err());
    assert!(matches!(
        f.engine.close_file_segment(&f.segment, Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(&fs::read(&f.path)?, b"................");
    resume.send(()).unwrap();
    assert_eq!(running.wait(WAIT)?.completed_tasks, 1);
    f.engine.close_file_segment(&f.segment, WAIT)?;
    assert_eq!(&fs::read(&f.path)?, b".AB\0\0CD.........");
    v.write(1, &[0])?;
    v.write(4, &[0])?;
    Ok(())
}

#[test]
fn vectored_uncertain_copy_keeps_sibling_loans_chunk_and_admission() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = fixture(b"guarded", 8, 1, false)?;
    let device = allocation(b"abcd");
    let (_, gpu) = register(&f, &device)?;
    let req = TransferRequest::file_vectored(
        Transport::IoUring,
        1,
        &[
            (gpu.base_address() + 1, 2).into(),
            Padding(1),
            (f.memory.base_address() + 3, 2).into(),
        ],
    )?;
    device.event_wait_fail_at.store(1, Ordering::Release);
    let report = f.run(TransferOp::Write, &[req])?;
    assert!(
        !report.locally_drained
            && matches!(report.failure.unwrap().error, Error::Unquiesced { .. })
    );
    let stats = f.engine.stats()?;
    assert_eq!(
        (stats.inflight_bytes, stats.staging.retained_chunks),
        (5, 1)
    );
    assert!(gpu.read(1, &mut [0]).is_err() && f.memory.write(3, &[0]).is_err());
    assert!(matches!(
        f.engine.close_file_segment(&f.segment, Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(&fs::read(&f.path)?, b"guarded");
    Ok(())
}
