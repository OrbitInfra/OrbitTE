use super::*;

#[test]
fn vectored_host_iovecs_gather_scatter_and_stop_in_the_middle_of_a_range() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let ext4 = std::env::var_os("ORBITE_EXT4_DIR").map_or_else(std::env::temp_dir, Into::into);
    let tmpfs =
        std::env::var_os("ORBITE_TMPFS_DIR").map_or_else(|| PathBuf::from("/dev/shm"), Into::into);
    for dir in [ext4, tmpfs] {
        host_vector(&dir)?;
    }
    Ok(())
}

fn host_vector(dir: &Path) -> Result<()> {
    let mut f = Fixture::new(
        dir,
        &[0xab; 256],
        TransferEngine::new(EngineConfig::default())?,
    )?;
    let truncate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let armed = truncate.clone();
    let path = f.path.clone();
    Arc::get_mut(&mut f.engine)
        .unwrap()
        .io_uring
        .as_mut()
        .unwrap()
        .create = Arc::new(move |n| {
        if armed.swap(false, std::sync::atomic::Ordering::AcqRel) {
            fs::OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(7 + 63)?;
        }
        ring::create(n)
    });
    let image: Vec<u8> = (0..256).map(|i| (i * 31) as u8).collect();
    f.memory.write(0, &image)?;
    let offsets: Vec<_> = (0..24).rev().map(|i| i * 9 + 2).collect();
    let ranges: Vec<_> = offsets
        .iter()
        .map(|&i| (f.memory.base_address() + i as u64, 5))
        .collect();
    let req = TransferRequest::file_vectored(Transport::IoUring, 7, &ranges)?;
    let packed: Vec<_> = offsets
        .iter()
        .flat_map(|&i| image[i..i + 5].iter().copied())
        .collect();
    assert!(
        f.run(TransferOp::Write, std::slice::from_ref(&req))?
            .is_complete()
    );
    let mut file = [0xab; 256];
    file[7..7 + packed.len()].copy_from_slice(&packed);
    assert_eq!(fs::read(&f.path)?, file);
    for limit in [packed.len(), 63] {
        f.memory.write(0, &[0xcc; 256])?;
        truncate.store(limit < packed.len(), std::sync::atomic::Ordering::Release);
        let b = f.engine.submit_async(
            TransferOp::Read,
            &f.segment,
            std::slice::from_ref(&req),
            None,
            TIMEOUT,
        )?;
        assert!(b.wait(EVENTUAL_PROGRESS)?.locally_drained);
        let status = b.task_status(0)?;
        assert_eq!(status.transferred_bytes, limit);
        assert_eq!(status.state == TaskState::Failed, limit != packed.len());
        let mut want = [0xcc; 256];
        for (n, &offset) in offsets.iter().enumerate() {
            let count = limit.saturating_sub(5 * n).min(5);
            want[offset..offset + count].copy_from_slice(&packed[n * 5..n * 5 + count]);
        }
        let mut got = [0; 256];
        f.memory.read(0, &mut got)?;
        assert_eq!(got, want);
    }
    Ok(())
}

#[test]
fn vectored_preflight_checks_bounds_target_and_last_fragment_before_payload() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = Fixture::new(
        &std::env::temp_dir(),
        b"protected",
        TransferEngine::new(EngineConfig {
            max_batch_requests: 2,
            max_transfer_bytes: 4,
            ..Default::default()
        })?,
    )?;
    let addr = f.memory.base_address();
    let vector =
        |parts: &[(u64, usize)]| TransferRequest::file_vectored(Transport::IoUring, 0, parts);
    for parts in [&[][..], &[(addr, 0)], &[(u64::MAX, 2)]] {
        assert!(vector(parts).is_err());
    }
    assert!(vector(&vec![(addr, 1); 1025]).is_err());
    assert!(TransferRequest::file_vectored(Transport::IoUring, u64::MAX, &[(addr, 1)]).is_err());
    for r in [
        vector(&[(addr, 1), (addr + 1, 1), (addr + 2, 1)])?,
        vector(&[(addr, 3), (addr + 3, 2)])?,
        vector(&[(addr, 1), (1, 1)])?,
    ] {
        assert!(f.run(TransferOp::Write, &[r]).is_err());
    }
    let peer = f
        .engine
        .open_segment(f.engine.endpoint(), f.engine.source_epoch(), TIMEOUT)?;
    let wrong_target = vector(&[(addr, 1)])?;
    assert!(matches!(
        f.engine
            .submit_and_wait(TransferOp::Write, &peer, &[wrong_target], TIMEOUT),
        Err(Error::InvalidConfig(
            "vectored request requires a file target"
        ))
    ));
    let mut mutated = vector(&[(addr, 2), (addr + 2, 2)])?;
    mutated.length = 1;
    assert!(matches!(
        f.run(TransferOp::Write, &[mutated]),
        Err(Error::InvalidRange)
    ));
    assert_eq!(fs::read(&f.path)?, b"protected");
    f.memory.write(0, b"abc")?;
    let req = vector(&[(addr, 2), (addr + 1, 2)])?;
    let report = f.run(TransferOp::Write, &[req])?;
    assert!(report.is_complete() && report.locally_drained);
    assert_eq!(fs::read(&f.path)?, b"abbcected");
    Ok(())
}

#[test]
fn vectored_padding_uses_private_scratch_and_counts_eof_inside_a_gap() -> Result<()> {
    use crate::FileIoElement::Padding;
    if !kernel() {
        return Ok(());
    }
    let ext4 = std::env::var_os("ORBITE_EXT4_DIR").map_or_else(std::env::temp_dir, Into::into);
    let tmpfs =
        std::env::var_os("ORBITE_TMPFS_DIR").map_or_else(|| PathBuf::from("/dev/shm"), Into::into);
    for dir in [ext4, tmpfs] {
        let image: Vec<u8> = (0..4110).map(|i| (i % 251 + 1) as u8).collect();
        let mut f = Fixture::new(
            &dir,
            &image,
            TransferEngine::new(EngineConfig {
                io_uring: crate::IoUringConfig {
                    pinned_staging: true,
                    staging_slot_kb: 4,
                    staging_slots: 1,
                },
                ..Default::default()
            })?,
        )?;
        let truncate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let armed = Arc::clone(&truncate);
        let path = f.path.clone();
        Arc::get_mut(&mut f.engine)
            .unwrap()
            .io_uring
            .as_mut()
            .unwrap()
            .create = Arc::new(move |n| {
            if armed.swap(false, std::sync::atomic::Ordering::AcqRel) {
                fs::OpenOptions::new()
                    .write(true)
                    .open(&path)?
                    .set_len(5 + 7)?;
            }
            ring::create(n)
        });
        let a = f.memory.base_address();
        let req = TransferRequest::file_vectored(
            Transport::IoUring,
            5,
            &[
                Padding(3),
                (a + 2, 2).into(),
                Padding(4),
                Padding(1),
                (a + 10, 3).into(),
                Padding(4083),
            ],
        )?;
        let request = std::slice::from_ref(&req);
        let mut expected = vec![0xcc; f.memory.len()];
        f.memory.write(0, &expected)?;
        let read = f.run(TransferOp::Read, request)?;
        assert!(read.is_complete() && read.locally_drained);
        assert_eq!(read.transferred_bytes, 4096);
        expected[2..4].copy_from_slice(&image[8..10]);
        expected[10..13].copy_from_slice(&image[15..18]);
        let mut got = vec![0; f.memory.len()];
        f.memory.read(0, &mut got)?;
        assert_eq!(got, expected);
        // The discarded nonzero file bytes dirtied the only slot. A WRITE must zero it again.
        f.memory.write(2, b"AB")?;
        f.memory.write(10, b"XYZ")?;
        let write = f.run(TransferOp::Write, request)?;
        assert!(write.is_complete() && write.locally_drained);
        assert_eq!(write.transferred_bytes, 4096);
        let mut file = image;
        file[5..4101].fill(0);
        file[8..10].copy_from_slice(b"AB");
        file[15..18].copy_from_slice(b"XYZ");
        assert_eq!(fs::read(&f.path)?, file);
        let stats = f.engine.stats()?.io_uring.unwrap();
        assert_eq!(
            (
                stats.slot_acquisitions,
                stats.active_slots,
                stats.pageable_bytes
            ),
            (2, 0, 0)
        );

        expected.fill(0xbd);
        f.memory.write(0, &expected)?;
        truncate.store(true, std::sync::atomic::Ordering::Release);
        let batch = f
            .engine
            .submit_async(TransferOp::Read, &f.segment, request, None, TIMEOUT)?;
        assert!(batch.wait(EVENTUAL_PROGRESS)?.locally_drained);
        let status = batch.task_status(0)?;
        assert_eq!(
            (status.state, status.transferred_bytes),
            (TaskState::Failed, 7)
        );
        let report = batch.submissions()?.remove(0);
        assert_eq!(
            (report.completed_requests, report.transferred_bytes),
            (0, 0)
        );
        expected[2..4].copy_from_slice(b"AB");
        f.memory.read(0, &mut got)?;
        assert_eq!(got, expected);
    }
    Ok(())
}

#[test]
fn vectored_padding_counts_toward_byte_and_descriptor_admission() -> Result<()> {
    use crate::FileIoElement::{self, Padding};
    if !kernel() {
        return Ok(());
    }
    let f = Fixture::new(
        &std::env::temp_dir(),
        &[0xef; 32],
        TransferEngine::new(EngineConfig {
            max_batch_requests: 3,
            max_transfer_bytes: 16,
            max_inflight_bytes: 8,
            ..Default::default()
        })?,
    )?;
    let req =
        |parts: &[FileIoElement]| TransferRequest::file_vectored(Transport::IoUring, 2, parts);
    let range = (f.memory.base_address(), 1).into();
    for invalid in [&[Padding(0)][..], &[Padding(usize::MAX), range]] {
        assert!(matches!(req(invalid), Err(Error::InvalidRange)));
    }
    assert!(matches!(
        req(&[Padding(1); 1025]),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        TransferRequest::file_vectored(Transport::IoUring, u64::MAX, &[Padding(1)]),
        Err(Error::InvalidRange)
    ));
    for requests in [
        vec![req(&[range, Padding(16)])?],
        vec![req(&[range, Padding(4)])?, req(&[Padding(4)])?],
        vec![req(&[Padding(1); 4])?],
    ] {
        assert!(matches!(
            f.run(TransferOp::Write, &requests),
            Err(Error::ResourceExhausted)
        ));
    }
    assert_eq!(fs::read(&f.path)?, [0xef; 32]);
    let only = req(&[Padding(3), Padding(5)])?;
    let pool = &f.engine.io_uring.as_ref().unwrap().pool;
    let held = pool.charge(1)?;
    let denied = f.run(TransferOp::Write, std::slice::from_ref(&only))?;
    assert!(matches!(
        denied.failure.unwrap().error,
        Error::ResourceExhausted
    ));
    assert_eq!(pool.stats().pageable_bytes, 1);
    assert_eq!(fs::read(&f.path)?, [0xef; 32]);
    drop(held);
    for op in [TransferOp::Write, TransferOp::Read] {
        let report = f.run(op, std::slice::from_ref(&only))?;
        assert!(report.is_complete() && report.locally_drained);
        assert_eq!(report.transferred_bytes, 8);
    }
    let mut want = [0xef; 32];
    want[2..10].fill(0);
    assert_eq!(fs::read(&f.path)?, want);
    let mut untouched = [0; 32];
    f.memory.read(0, &mut untouched)?;
    assert_eq!(untouched, [0x9a; 32]);
    f.engine.unregister_memory(&f.memory)?;
    assert!(
        f.run(TransferOp::Read, std::slice::from_ref(&only))?
            .is_complete()
    );
    Ok(())
}

#[test]
fn vectored_unconfirmed_read_keeps_padding_scratch_loans_and_the_full_charge() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = fault_fixture(Fault::OversizedCqe)?;
    let req = TransferRequest::file_vectored(
        Transport::IoUring,
        0,
        &[
            (f.memory.base_address() + 1, 2).into(),
            crate::FileIoElement::Padding(4),
            (f.memory.base_address() + 9, 2).into(),
        ],
    )?;
    let report = f.run(TransferOp::Read, &[req])?;
    assert!(!report.locally_drained);
    assert!(matches!(
        report.failure.unwrap().error,
        Error::Unquiesced { .. }
    ));
    let stats = f.engine.stats()?;
    assert_eq!(
        (stats.inflight_bytes, stats.io_uring.unwrap().pageable_bytes),
        (8, 4)
    );
    assert!(f.memory.read(1, &mut [0; 2]).is_err() && f.memory.read(9, &mut [0; 2]).is_err());
    assert!(matches!(
        f.engine.close_file_segment(&f.segment, Duration::ZERO),
        Err(Error::Timeout)
    ));
    Ok(())
}
