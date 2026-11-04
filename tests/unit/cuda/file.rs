use std::fs;
use std::sync::mpsc;

use super::*;
use crate::transport::io_uring::tests::{Fixture, kernel};
use crate::{RegisteredMemory, RemoteEffect, RuntimeQueueConfig, TaskState};

const WAIT: Duration = Duration::from_secs(5);

fn fixture(contents: &[u8], chunk: usize, slots: usize, queued: bool) -> Result<Fixture> {
    fixture_with_config(
        contents,
        EngineConfig {
            staging_chunk_size: chunk,
            staging_chunk_count: slots,
            runtime_queue: queued.then(|| RuntimeQueueConfig {
                max_dispatch_owners: 1,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
}

fn fixture_with_config(contents: &[u8], config: EngineConfig) -> Result<Fixture> {
    let dir = std::env::var_os("ORBITTE_EXT4_DIR").map_or_else(std::env::temp_dir, Into::into);
    Fixture::new(&dir, contents, TransferEngine::new(config)?)
}

fn register(f: &Fixture, device: &Arc<Device>) -> Result<(Arc<CudaMemory>, RegisteredMemory)> {
    let gpu = memory(device);
    let view = f
        .engine
        .register_cuda_memory_batch(&[(gpu.clone(), 0, gpu.len())], Permission::LocalOnly)?
        .remove(0);
    Ok((gpu, view))
}

fn request(view: &RegisteredMemory, local: usize, offset: u64, length: usize) -> TransferRequest {
    TransferRequest::new(
        Transport::Unspec,
        view.base_address() + local as u64,
        offset,
        length,
    )
}

#[test]
fn single_device_read_and_write_fill_each_round_with_available_pieces() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    const CHUNK: usize = 4096;
    const LENGTH: usize = 7 * CHUNK + 37;
    let source: Vec<_> = (0..LENGTH + 6).map(|i| ((37 * i) % 251) as u8).collect();
    let start = vec![0xa3; CHUNK + LENGTH + 5];
    let mut expected_file = start.clone();
    expected_file[CHUNK..CHUNK + LENGTH].copy_from_slice(&source[2..2 + LENGTH]);

    // First the pool limits the rounds. Then the configured batch limit wins,
    // even though Linux rounds a five-entry SQ up to eight slots.
    for (slots, limit, ring_entries, widths) in [(3, 16, 8, &[3, 3, 2][..]), (7, 5, 5, &[5, 3][..])]
    {
        let mut f = fixture_with_config(
            &start,
            EngineConfig {
                staging_chunk_size: CHUNK,
                staging_chunk_count: slots,
                max_batch_requests: limit,
                ..Default::default()
            },
        )?;
        let trace = f.observe_ring(false, move |size| {
            assert_eq!(size, ring_entries);
            Ok(())
        });
        let device = allocation(&source);
        let (_, view) = register(&f, &device)?;
        let requests = [request(&view, 2, CHUNK as u64, LENGTH)];
        let mut expected_gpu = vec![0x56; source.len()];
        for op in [TransferOp::Write, TransferOp::Read] {
            if op == TransferOp::Read {
                view.write(0, &expected_gpu)?;
            }
            let report = f.run(op, &requests)?;
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (1, LENGTH)
            );
            assert_eq!(
                std::mem::take(&mut trace.lock().unwrap().round_widths),
                widths,
                "{op:?}"
            );
            assert_eq!(fs::read(&f.path)?, expected_file);
        }
        expected_gpu[2..2 + LENGTH].copy_from_slice(&source[2..2 + LENGTH]);
        let mut actual = vec![0; source.len()];
        view.read(0, &mut actual)?;
        assert_eq!(actual, expected_gpu);
    }
    Ok(())
}

#[test]
fn staged_ring_caps_piece_count_at_the_linux_entry_limit() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let contents = vec![0x71; 32_769];
    let mut f = fixture_with_config(
        &contents,
        EngineConfig {
            staging_chunk_size: 1,
            staging_chunk_count: 1,
            max_batch_requests: contents.len(),
            ..Default::default()
        },
    )?;
    f.observe_ring(false, |size| {
        assert_eq!(size, 32_768);
        // Observe setup without allocating a max-size kernel ring in CI.
        Err(std::io::Error::from_raw_os_error(libc::ENOMEM))
    });
    let device = allocation(&contents);
    let (_, view) = register(&f, &device)?;
    let report = f.run(TransferOp::Read, &[request(&view, 0, 0, contents.len())])?;
    assert!(
        matches!(report.failure.unwrap().error, Error::Io(e) if e.raw_os_error() == Some(libc::ENOMEM))
    );
    Ok(())
}

#[test]
fn device_chunks_and_tail_use_one_ring_and_verify_both_files_and_guards() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    const CHUNK: usize = 4096;
    let start = vec![0xa3; CHUNK * 4];
    let mut f = fixture(&start, CHUNK, 2, false)?;
    let rings = Arc::new(AtomicUsize::new(0));
    let calls = rings.clone();
    f.observe_ring(false, move |n| {
        assert_eq!(n, 4);
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });
    let original: Vec<_> = (0..start.len()).map(|i| (i * 43 + 15) as u8).collect();
    let device = allocation(&original);
    let (_, view) = register(&f, &device)?;
    let n = CHUNK * 2 + 37;
    // The first two chunks can use direct I/O. The tail and the second, odd
    // file offset must use the buffered fd on the same ext4 inode.
    let requests = [
        request(&view, 3, 0, n),
        request(&view, n + 8, (n + 3) as u64, 19),
    ];
    let stats = f.engine.stats()?.staging;
    let report = f.run(TransferOp::Write, &requests)?;
    assert!(report.is_complete() && report.locally_drained, "{report:?}");
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (2, n + 19)
    );
    let mut expected = start;
    expected[..n].copy_from_slice(&original[3..3 + n]);
    expected[n + 3..n + 22].copy_from_slice(&original[n + 8..n + 27]);
    assert_eq!(fs::read(&f.path)?, expected);
    if std::env::var_os("ORBITTE_EXT4_DIR").is_some() {
        assert_eq!(
            f.engine.file_segment_info(&f.segment)?.direct_io,
            Some(true)
        );
    }
    view.write(0, &vec![0x56; view.len()])?;
    let batch = f
        .engine
        .submit_async(TransferOp::Read, &f.segment, &requests, None, WAIT)?;
    assert_eq!(batch.wait(WAIT)?.completed_tasks, 2);
    let mut expected_gpu = vec![0x56; view.len()];
    expected_gpu[3..3 + n].copy_from_slice(&expected[..n]);
    expected_gpu[n + 8..n + 27].copy_from_slice(&expected[n + 3..n + 22]);
    let mut got = vec![0; view.len()];
    view.read(0, &mut got)?;
    assert_eq!(got, expected_gpu);
    let after = f.engine.stats()?.staging;
    assert_eq!(
        (after.allocations - stats.allocations, after.idle_chunks),
        (2, 2)
    );
    assert_eq!(after.completed_chunks - stats.completed_chunks, 8);
    assert_eq!(
        after.copied_bytes - stats.copied_bytes,
        (2 * (n + 19)) as u64
    );
    assert_eq!(rings.load(Ordering::Acquire), 2);
    Ok(())
}

#[test]
fn a_short_middle_chunk_copies_only_the_prefix_and_preserves_other_tasks() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    for real_eof in [true, false] {
        let mut f = fixture(b"abcdefghijklmnop", 4, 3, false)?;
        let path = f.path.clone();
        f.observe_ring(!real_eof, move |n| {
            assert_eq!(n, 5);
            if real_eof {
                fs::OpenOptions::new().write(true).open(&path)?.set_len(5)?;
            }
            Ok(())
        });
        let device = allocation(&[0x73; 24]);
        let (_, v) = register(&f, &device)?;
        // Later SQEs of the first request and independent host/device tasks
        // must never extend its short prefix.
        let requests = [
            request(&v, 1, 0, 12),
            f.request(1, 0, 2),
            request(&v, 18, 2, 2),
        ];
        let b = f
            .engine
            .submit_async(TransferOp::Read, &f.segment, &requests, None, WAIT)?;
        assert_eq!(b.wait(WAIT)?.completed_tasks, 2);
        let statuses = b.task_statuses()?;
        assert_eq!(
            (statuses[0].state, statuses[0].transferred_bytes),
            (TaskState::Failed, 5)
        );
        assert_eq!(statuses[1].transferred_bytes, 2);
        assert_eq!(statuses[2].transferred_bytes, 2);
        let reports = b.submissions()?;
        let report = &reports[0];
        assert_eq!(
            (report.completed_requests, report.transferred_bytes),
            (0, 0)
        );
        let failure = report.failure.as_ref().unwrap();
        assert_eq!(
            (
                failure.request_index,
                failure.affected_requests,
                failure.remote_effect
            ),
            (0, 3, RemoteEffect::None)
        );
        let mut got = [0; 24];
        v.read(0, &mut got)?;
        let mut expected = [0x73; 24];
        expected[1..6].copy_from_slice(b"abcde");
        expected[18..20].copy_from_slice(b"cd");
        assert_eq!(got, expected);
        let mut host = [0; 2];
        f.memory.read(1, &mut host)?;
        assert_eq!(&host, b"ab");
    }
    Ok(())
}

#[test]
fn write_with_a_short_middle_receipt_never_replays_any_chunk() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let mut f = fixture(b"................", 4, 3, false)?;
    f.observe_ring(true, |_| Ok(()));
    let device = allocation(b"abcdefghijklmno!");
    let (_, v) = register(&f, &device)?;
    let requests = [
        request(&v, 0, 0, 12),
        f.request(0, 12, 2),
        request(&v, 12, 14, 2),
    ];
    let b = f
        .engine
        .submit_async(TransferOp::Write, &f.segment, &requests, None, WAIT)?;
    assert_eq!(b.wait(WAIT)?.completed_tasks, 2);
    assert_eq!(b.task_statuses()?[0].transferred_bytes, 5);
    let reports = b.submissions()?;
    let failure = reports[0].failure.as_ref().unwrap();
    assert_eq!(
        (reports[0].completed_requests, reports[0].transferred_bytes),
        (0, 0)
    );
    assert_eq!(
        (failure.affected_requests, failure.remote_effect),
        (3, RemoteEffect::Unknown)
    );
    assert_eq!(&fs::read(&f.path)?, b"abcdefghijkl\x9a\x9amn");
    assert_eq!(device.submitted_copies.lock().unwrap().len(), 4);
    Ok(())
}

#[test]
fn staged_copy_failure_and_uncertain_event_or_restore_preserve_the_right_owners() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    for (mode, op) in [
        ("copy", TransferOp::Write),
        ("event", TransferOp::Read),
        ("restore", TransferOp::Write),
    ] {
        let mut f = fixture(b"ABCDEFGH", 4, 1, false)?;
        let ring = f.observe_ring(false, |_| Ok(()));
        let device = allocation(b"12345678");
        let (_, v) = register(&f, &device)?;
        match mode {
            "copy" => device.copy_error.store(1, Ordering::Release),
            "event" => device.event_wait_fail_at.store(2, Ordering::Release),
            _ => device.pop_after_completion.store(true, Ordering::Release),
        }
        let b = f
            .engine
            .submit_async(op, &f.segment, &[request(&v, 0, 0, 8)], None, WAIT)?;
        let status = b.wait(WAIT)?;
        let t = b.task_statuses()?.remove(0);
        assert_eq!(t.transferred_bytes, if mode == "event" { 4 } else { 0 });
        assert_eq!(&fs::read(&f.path)?, b"ABCDEFGH");
        let error = &t.failure.as_ref().unwrap().error;
        if mode == "copy" {
            assert!(matches!(error, Error::Backend { .. }));
            assert!(status.locally_drained && t.locally_drained);
            assert!(ring.lock().unwrap().lifetime.upgrade().is_none());
            assert_eq!(f.engine.stats()?.staging.idle_chunks, 1);
            f.engine.close_file_segment(&f.segment, WAIT)?;
        } else {
            assert!(matches!(error, Error::Unquiesced { .. }));
            assert!(!status.locally_drained && !t.locally_drained);
            assert!(ring.lock().unwrap().lifetime.upgrade().is_some());
            assert_eq!(f.engine.stats()?.staging.retained_chunks, 1);
            assert_eq!(f.engine.stats()?.inflight_bytes, 8);
            f.engine.unregister_memory(&v)?;
            assert!(v.read(7, &mut [0]).is_err());
            assert!(matches!(
                f.engine.close_file_segment(&f.segment, Duration::ZERO),
                Err(Error::Timeout)
            ));
        }
        assert_eq!(
            t.failure.as_ref().unwrap().remote_effect,
            RemoteEffect::None
        );
    }
    Ok(())
}

#[test]
fn delayed_event_keeps_the_whole_range_and_chunk_and_cancel_only_stops_queued_io() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    for queued in [false, true] {
        let f = fixture(b"................", 4, 1, queued)?;
        let device = allocation(b"abcdefghijklmnop");
        let (_, v) = register(&f, &device)?;
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, release) = mpsc::sync_channel(1);
        *device.event_gate.lock().unwrap() = Some((entered, release));
        let first = f.engine.submit_async(
            TransferOp::Write,
            &f.segment,
            &[request(&v, 0, 0, 8)],
            None,
            WAIT,
        )?;
        waiting
            .recv_timeout(crate::test_support::EVENTUAL_PROGRESS)
            .unwrap();
        assert!(first.cancel(0)?);
        assert!(v.write(7, &[0x11]).is_err());
        assert_eq!(f.engine.stats()?.staging.active_chunks, 1);
        assert_eq!(&fs::read(&f.path)?, b"................");
        let later = f.engine.submit_async(
            TransferOp::Write,
            &f.segment,
            &[request(&v, 8, 8, 4)],
            None,
            WAIT,
        )?;
        if queued {
            assert!(later.cancel(0)?);
        }
        let status = later.wait(WAIT)?;
        let failure = later.task_statuses()?.remove(0).failure.unwrap();
        assert!(status.locally_drained);
        if queued {
            assert!(matches!(failure.error, Error::Cancelled));
        } else {
            assert!(matches!(failure.error, Error::ResourceExhausted));
        }
        assert_eq!(failure.remote_effect, RemoteEffect::None);
        resume.send(()).unwrap();
        assert_eq!(first.wait(WAIT)?.completed_tasks, 1);
        assert_eq!(&fs::read(&f.path)?, b"abcdefgh........");
        assert_eq!(f.engine.stats()?.staging.allocations, 1);
    }
    Ok(())
}

#[test]
fn device_file_write_consumes_the_callers_producer_generation() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = fixture(b"................", 4, 1, false)?;
    let dev = allocation(&[0xab; 16]);
    let (gpu, view) = register(&f, &dev)?;
    let producer = super::readiness::Producer::new(&gpu, 0x36, true);
    let weak = Arc::downgrade(&producer);
    producer.enqueue(gpu.base_address() + 3, 7);
    producer.record(&gpu)?;
    drop(producer);
    assert!(weak.upgrade().is_some());
    let b = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[request(&view, 3, 1, 7)],
        None,
        WAIT,
    )?;
    assert_eq!(b.wait(WAIT)?.completed_tasks, 1);
    assert!(weak.upgrade().is_none());
    assert_eq!(&fs::read(&f.path)?, b".6666666........");
    assert_eq!(dev.event_waits.load(Ordering::Acquire), 3);
    Ok(())
}

#[test]
fn pinned_host_read_and_write_consume_readiness_without_a_staging_chunk() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    for op in [TransferOp::Read, TransferOp::Write] {
        let mut f = fixture(b"abcdefghijklmnop", 4, 1, false)?;
        f.deny_bounce();
        let dev = allocation(&[0x57; 64]);
        let producer = super::pinned_readiness::Producer::new(&dev, true);
        let host = producer.pinned();
        let weak = Arc::downgrade(&producer);
        let v = f
            .engine
            .register_host_memory_batch(&[(host.clone(), 1, 32)], Permission::LocalOnly)?
            .remove(0);
        producer.enqueue(&host, 2, 8);
        producer.record(&host)?;
        drop(producer);
        let report = f.run(op, &[request(&v, 1, 1, 5)])?;
        assert!(report.is_complete() && report.locally_drained, "{report:?}");
        assert!(weak.upgrade().is_none());
        let mut expected = [0xa5; 64];
        expected[2..10].fill(0x57);
        let mut file = *b"abcdefghijklmnop";
        if op == TransferOp::Read {
            expected[2..7].copy_from_slice(b"bcdef");
        } else {
            file[1..6].fill(0x57);
        }
        let mut actual = [0; 64];
        host.read(0, &mut actual)?;
        assert_eq!(actual, expected);
        assert_eq!(fs::read(&f.path)?, file);
        assert_eq!(dev.event_waits.load(Ordering::Acquire), 1);
        assert_eq!(f.engine.stats()?.staging.reserved_chunks, 0);
    }
    Ok(())
}

#[test]
fn one_available_chunk_runs_more_requests_and_an_overlapping_range_fails_before_io() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let mut f = fixture(b"................", 4, 1, false)?;
    let rings = Arc::new(AtomicUsize::new(0));
    let called = rings.clone();
    f.observe_ring(false, move |_| {
        called.fetch_add(1, Ordering::Release);
        Ok(())
    });
    let dev = allocation(b"abcdefghijklmnop");
    let (_, v) = register(&f, &dev)?;
    let failure = f.run(
        TransferOp::Write,
        &[request(&v, 0, 0, 8), request(&v, 7, 13, 2)],
    )?;
    assert!(
        failure.locally_drained && matches!(failure.failure.unwrap().error, Error::Unavailable(_))
    );
    assert_eq!(rings.load(Ordering::Acquire), 0);
    assert_eq!(&fs::read(&f.path)?, b"................");
    let requests = [
        request(&v, 0, 0, 7),
        f.request(1, 7, 1),
        request(&v, 8, 8, 7),
    ];
    let report = f.run(TransferOp::Write, &requests)?;
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (3, 15)
    );
    assert_eq!(&fs::read(&f.path)?, b"abcdefg\x9aijklmno.");
    let stats = f.engine.stats()?.staging;
    assert_eq!(
        (stats.allocations, stats.idle_chunks, stats.worker_shards),
        (1, 1, 0)
    );
    assert_eq!(rings.load(Ordering::Acquire), 1);
    Ok(())
}

#[test]
fn r1c_portable_file_slot_and_charged_proxy_fallback_share_a_real_ring() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let image: Vec<_> = (0..16384).map(|i| (i % 251) as u8).collect();
    let f = fixture_with_config(
        &image,
        EngineConfig {
            io_uring: crate::IoUringConfig {
                pinned_staging: true,
                staging_slot_kb: 4,
                staging_slots: 1,
            },
            staging_chunk_size: 4096,
            staging_chunk_count: 3,
            ..Default::default()
        },
    )?;
    let device = allocation(&[0xa3; 16400]);
    let (_, view) = register(&f, &device)?;
    let requests = [
        request(&view, 1, 0, 4096),
        request(&view, 4100, 4096, 4096),
        request(&view, 8201, 8192, 8192),
    ];
    let mut expected = vec![0xa3; 16400];
    for (at, src, n) in [(1, 0, 4096), (4100, 4096, 4096), (8201, 8192, 8192)] {
        expected[at..at + n].copy_from_slice(&image[src..src + n]);
    }
    for op in [TransferOp::Read, TransferOp::Write] {
        if op == TransferOp::Write {
            fs::write(&f.path, vec![0x54; 16384])?;
        }
        let report = f.run(op, &requests)?;
        assert!(report.is_complete() && report.locally_drained, "{report:?}");
        let mut bytes = vec![0; 16400];
        view.read(0, &mut bytes)?;
        assert_eq!(bytes, expected);
        assert_eq!(fs::read(&f.path)?, image);
    }
    let s = f.engine.stats()?;
    let file = s.io_uring.unwrap();
    assert!(file.pinned);
    assert_eq!(
        (
            file.slots,
            file.active_slots,
            file.slot_acquisitions,
            file.slot_misses,
            file.pageable_bytes
        ),
        (1, 0, 2, 4, 0)
    );
    assert_eq!(
        (s.staging.active_chunks, s.staging.copied_bytes),
        (0, 2 * 12288)
    );
    f.engine.close(WAIT)?;
    assert!(device.pins.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn r1c_unconfirmed_gpu_copy_retains_the_file_slot_loan_and_admission() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = fixture_with_config(
        b"before",
        EngineConfig {
            io_uring: crate::IoUringConfig {
                pinned_staging: true,
                staging_slot_kb: 4,
                staging_slots: 1,
            },
            ..Default::default()
        },
    )?;
    let device = allocation(&[0x47; 16]);
    let (_, view) = register(&f, &device)?;
    device.event_wait_fail_at.store(1, Ordering::Release);
    let report = f.run(TransferOp::Write, &[request(&view, 2, 1, 3)])?;
    assert!(
        !report.locally_drained
            && matches!(report.failure.unwrap().error, Error::Unquiesced { .. })
    );
    let s = f.engine.stats()?;
    assert_eq!((s.io_uring.unwrap().active_slots, s.inflight_bytes), (1, 3));
    assert!(view.read(2, &mut [0; 3]).is_err());
    assert_eq!(fs::read(&f.path)?, b"before");
    assert_eq!(device.pins.lock().unwrap().len(), 1);
    Ok(())
}
