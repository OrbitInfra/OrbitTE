use super::*;

#[test]
fn vectored_host_iovecs_gather_scatter_and_stop_in_the_middle_of_a_range() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let ext4 = std::env::var_os("ORBITTE_EXT4_DIR").map_or_else(std::env::temp_dir, Into::into);
    let tmpfs =
        std::env::var_os("ORBITTE_TMPFS_DIR").map_or_else(|| PathBuf::from("/dev/shm"), Into::into);
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
