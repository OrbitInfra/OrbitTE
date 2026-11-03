use super::*;
use crate::test_support::EVENTUAL_PROGRESS;
use crate::{
    EngineConfig, FileSegment, Permission, RegisteredMemory, RemoteEffect, TaskState,
    TransferEngine, TransferOp, TransferRequest, Transport,
};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use sys::*;

pub(crate) struct Mock {
    pub(crate) opens: AtomicUsize,
    pub(crate) open_error: AtomicBool,
    mode: GdsMode,
    native_slice_limit: AtomicUsize,
    pub(crate) register_error: AtomicBool,
    pub(crate) register_uncertain: AtomicBool,
    pub(crate) deregister_error: AtomicBool,
    pub(crate) buffer_registers: AtomicUsize,
    pub(crate) buffer_deregisters: AtomicUsize,
    file_error: AtomicBool,
    file_close_error: AtomicBool,
    pub(crate) file_registers: AtomicUsize,
    file_closes: AtomicUsize,
    setup_error: AtomicBool,
    destroy_error: AtomicBool,
    pub(crate) held: AtomicBool,
    held_cookie: AtomicUsize,
    until_cancel: AtomicBool,
    partial: AtomicBool,
    pub(super) poll_error_once: AtomicBool,
    pub(crate) submits: AtomicUsize,
    cancels: AtomicUsize,
    pub(super) polls: AtomicUsize,
    pub(crate) destroys: AtomicUsize,
    files: Mutex<HashMap<usize, File>>,
    batches: Mutex<HashMap<usize, Vec<Event>>>,
    next: AtomicUsize,
    depths: Mutex<Vec<u32>>,
    params: Mutex<Vec<Param>>,
    script: Mutex<Vec<(u32, usize)>>,
}
impl Mock {
    pub(crate) fn new(mode: GdsMode) -> Arc<Self> {
        Arc::new(Self {
            mode,
            native_slice_limit: (16usize << 20).into(),
            opens: 0.into(),
            open_error: false.into(),
            register_error: false.into(),
            register_uncertain: false.into(),
            deregister_error: false.into(),
            buffer_registers: 0.into(),
            buffer_deregisters: 0.into(),
            file_error: false.into(),
            file_close_error: false.into(),
            file_registers: 0.into(),
            file_closes: 0.into(),
            setup_error: false.into(),
            destroy_error: false.into(),
            held: false.into(),
            held_cookie: 0.into(),
            until_cancel: false.into(),
            partial: false.into(),
            poll_error_once: false.into(),
            submits: 0.into(),
            cancels: 0.into(),
            polls: 0.into(),
            destroys: 0.into(),
            files: Mutex::default(),
            batches: Mutex::default(),
            next: 1.into(),
            depths: Mutex::default(),
            params: Mutex::default(),
            script: Mutex::default(),
        })
    }
    pub(crate) fn install(self: &Arc<Self>, engine: &mut TransferEngine, config: &EngineConfig) {
        let driver = Driver::new(self.clone());
        engine.gds = driver
            .mode()
            .ok()
            .map(|_| Arc::new(GdsTransport::new(driver, config)));
    }
}
fn failed(op: &'static str) -> Error {
    Error::Backend {
        operation: op,
        code: 5035,
    }
}

impl Api for Mock {
    fn open(&self) -> Result<()> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        if self.open_error.load(Ordering::Acquire) {
            Err(failed("open"))
        } else {
            Ok(())
        }
    }
    fn profile(&self) -> Result<Profile> {
        Ok(Profile {
            mode: self.mode,
            max_native_slice: self.native_slice_limit.load(Ordering::Acquire),
        })
    }
    fn register_file(&self, fd: i32) -> Result<usize> {
        self.file_registers.fetch_add(1, Ordering::Relaxed);
        if self.file_error.load(Ordering::Acquire) {
            return Err(failed("handle register"));
        }
        // SAFETY: register_file receives the Entry-owned live descriptor.
        #[allow(unsafe_code)]
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        assert_ne!(flags & libc::O_DIRECT, 0);
        let file = OpenOptions::new()
            .read(flags & libc::O_ACCMODE != libc::O_WRONLY)
            .write(flags & libc::O_ACCMODE != libc::O_RDONLY)
            .open(format!("/proc/self/fd/{fd}"))?;
        let handle = file.as_raw_fd() as usize;
        self.files.lock().unwrap().insert(handle, file);
        Ok(handle)
    }
    fn deregister_file(&self, h: usize) -> Result<()> {
        self.file_closes.fetch_add(1, Ordering::Relaxed);
        if self.file_close_error.load(Ordering::Acquire) {
            return Err(failed("handle deregister"));
        }
        self.files.lock().unwrap().remove(&h).unwrap();
        Ok(())
    }
    fn register_buffer(&self, _: usize, _: usize) -> Result<()> {
        self.buffer_registers.fetch_add(1, Ordering::Relaxed);
        if self.register_uncertain.load(Ordering::Acquire) {
            Err(Error::Unquiesced {
                operation: "cuFileBufRegister",
                code: -1,
            })
        } else if self.register_error.load(Ordering::Acquire) {
            Err(failed("buf register"))
        } else {
            Ok(())
        }
    }
    fn deregister_buffer(&self, _: usize) -> Result<()> {
        self.buffer_deregisters.fetch_add(1, Ordering::Relaxed);
        if self.deregister_error.load(Ordering::Acquire) {
            Err(failed("buf deregister"))
        } else {
            Ok(())
        }
    }
    fn setup(&self, depth: u32) -> Result<usize> {
        self.depths.lock().unwrap().push(depth);
        if self.setup_error.load(Ordering::Acquire) {
            return Err(failed("setup"));
        }
        let h = self.next.fetch_add(1, Ordering::Relaxed);
        self.batches.lock().unwrap().insert(h, Vec::new());
        Ok(h)
    }
    #[allow(unsafe_code)]
    fn submit(&self, h: usize, params: &mut [Param]) -> Result<()> {
        let script = self.script.lock().unwrap();
        let mut batches = self.batches.lock().unwrap();
        let events = batches.get_mut(&h).unwrap();
        self.params.lock().unwrap().extend_from_slice(params);
        for (i, p) in params.iter().enumerate() {
            assert_eq!(p.mode, 1);
            assert_eq!(p.cookie, i + 1);
            let (status, length) = script.get(i).copied().unwrap_or((COMPLETE, p.size));
            let mut bytes = length;
            if status == COMPLETE && length <= p.size {
                let files = self.files.lock().unwrap();
                let file = &files[&p.handle];
                let addr = (p.base + p.buffer_offset as usize) as *mut u8;
                // SAFETY: the production range loan covers these CPU/shim-device bytes
                // through the terminal receipt; the script only moves a bounded prefix.
                bytes = unsafe {
                    if p.opcode == 0 {
                        file.read_at(
                            std::slice::from_raw_parts_mut(addr, length),
                            p.file_offset as u64,
                        )?
                    } else {
                        file.write_at(
                            std::slice::from_raw_parts(addr, length),
                            p.file_offset as u64,
                        )?
                    }
                };
            }
            events.push(Event {
                cookie: p.cookie,
                status,
                bytes,
            });
        }
        events.reverse();
        self.submits.fetch_add(1, Ordering::Release);
        if self.partial.load(Ordering::Acquire) {
            Err(failed("partial submit"))
        } else {
            Ok(())
        }
    }
    fn poll(&self, h: usize, events: &mut [Event]) -> Result<usize> {
        self.polls.fetch_add(1, Ordering::Release);
        if self.poll_error_once.swap(false, Ordering::AcqRel) {
            return Err(failed("poll"));
        }
        if self.held.load(Ordering::Acquire)
            || self.until_cancel.load(Ordering::Acquire)
                && self.cancels.load(Ordering::Acquire) == 0
        {
            return Ok(0);
        }
        let mut batches = self.batches.lock().unwrap();
        let list = batches.get_mut(&h).unwrap();
        let held = self.held_cookie.load(Ordering::Acquire);
        let mut n = 0;
        list.retain(|event| {
            if event.cookie == held {
                return true;
            }
            events[n] = *event;
            n += 1;
            false
        });
        Ok(n)
    }
    fn cancel(&self, _: usize) -> Result<()> {
        self.cancels.fetch_add(1, Ordering::Release);
        Ok(())
    }
    fn destroy(&self, h: usize) -> Result<()> {
        self.destroys.fetch_add(1, Ordering::Relaxed);
        if self.destroy_error.load(Ordering::Acquire) {
            return Err(failed("destroy"));
        }
        assert!(self.batches.lock().unwrap().remove(&h).unwrap().is_empty());
        Ok(())
    }
}

pub(crate) struct Fixture {
    pub(crate) engine: Arc<TransferEngine>,
    pub(crate) segment: FileSegment,
    pub(crate) path: PathBuf,
}
impl Fixture {
    pub(crate) fn new(mock: &Arc<Mock>, config: EngineConfig, length: usize) -> Result<Self> {
        let mut engine = TransferEngine::new(config.clone())?;
        mock.install(&mut engine, &config);
        Self::with_engine(engine, length)
    }
    pub(crate) fn with_engine(engine: TransferEngine, length: usize) -> Result<Self> {
        let dir =
            std::env::var_os("ORBITTE_EXT4_DIR").map_or_else(std::env::temp_dir, PathBuf::from);
        let path = dir.join(format!("orbitte-gds-{}", crate::SourceEpoch::generate()?));
        File::create(&path)?.set_len(length as u64)?;
        let segment = engine.open_file_segment(
            &format!("file://{}", path.display()),
            Permission::RemoteReadWrite,
        )?;
        Ok(Self {
            engine: Arc::new(engine),
            segment,
            path,
        })
    }
    pub(crate) fn host(&self, size: usize) -> Result<RegisteredMemory> {
        self.engine
            .register_memory(vec![0x6b; size].into(), Permission::LocalOnly)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
pub(crate) const WAIT: Duration = Duration::from_secs(5);
pub(crate) fn request(
    mem: &RegisteredMemory,
    local: usize,
    offset: u64,
    n: usize,
) -> TransferRequest {
    TransferRequest::new(Transport::Gds, mem.base_address() + local as u64, offset, n)
}
pub(crate) fn wait_enter(mock: &Mock) {
    let end = Instant::now() + EVENTUAL_PROGRESS;
    while mock.submits.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
}

#[test]
fn open_mode_selection_and_synchronous_file_bytes() -> Result<()> {
    for mode in [
        GdsMode::NativeOnly,
        GdsMode::CompatAllowed,
        GdsMode::CompatForced,
    ] {
        let mock = Mock::new(mode);
        let f = Fixture::new(&mock, EngineConfig::default(), 32)?;
        let m = f.host(32)?;
        m.write(3, b"payload")?;
        let r = [TransferRequest::new(
            Transport::Unspec,
            m.base_address() + 3,
            5,
            7,
        )];
        assert!(
            f.engine
                .submit_and_wait(TransferOp::Write, &f.segment, &r, WAIT)?
                .is_complete()
        );
        let mut expected = [0; 32];
        expected[5..12].copy_from_slice(b"payload");
        assert_eq!(fs::read(&f.path)?, expected);
        m.write(0, &[0xa2; 32])?;
        assert!(
            f.engine
                .submit_and_wait(TransferOp::Read, &f.segment, &r, WAIT)?
                .is_complete()
        );
        let mut actual = [0; 32];
        m.read(0, &mut actual)?;
        let mut expected = [0xa2; 32];
        expected[3..10].copy_from_slice(b"payload");
        assert_eq!(actual, expected);
        #[cfg(feature = "metrics")]
        {
            let p = f.engine.metrics().unwrap().prometheus();
            assert!(p.contains("tent_read_bytes_total{transport=\"gds\"} 7"));
            assert!(p.contains("tent_write_bytes_total{transport=\"gds\"} 7"));
        }
        assert_eq!(f.engine.file_segment_info(&f.segment)?.gds_mode, Some(mode));
        assert_eq!(f.engine.stats()?.gds.mode, Some(mode));
        assert_eq!(mock.buffer_registers.load(Ordering::Acquire), 0);
        assert_eq!(mock.file_registers.load(Ordering::Acquire), 1);
        assert_eq!(&*mock.depths.lock().unwrap(), &[32, 32]);
        f.engine.close_file_segment(&f.segment, WAIT)?;
        assert_eq!(mock.file_closes.load(Ordering::Acquire), 1);
    }
    for disabled in [false, true] {
        let mock = Mock::new(GdsMode::CompatAllowed);
        mock.open_error.store(disabled, Ordering::Release);
        let f = Fixture::new(
            &mock,
            EngineConfig {
                gds_require_native: true,
                ..Default::default()
            },
            16,
        )?;
        let m = f.host(16)?;
        assert!(!f.engine.stats()?.gds.available);
        assert!(matches!(
            f.engine
                .submit_and_wait(TransferOp::Write, &f.segment, &[request(&m, 0, 0, 8)], WAIT),
            Err(Error::Unavailable(_))
        ));
        assert_eq!(mock.file_registers.load(Ordering::Acquire), 0);
    }
    Ok(())
}

#[test]
fn native_handle_setup_and_cleanup_failures_never_replay_or_free_uncertain_owners() -> Result<()> {
    for kind in ["file", "setup", "destroy", "file-close"] {
        let mock = Mock::new(GdsMode::NativeOnly);
        mock.file_error.store(kind == "file", Ordering::Release);
        mock.setup_error.store(kind == "setup", Ordering::Release);
        mock.destroy_error
            .store(kind == "destroy", Ordering::Release);
        let f = Fixture::new(
            &mock,
            EngineConfig {
                max_failover_attempts: 0,
                ..Default::default()
            },
            16,
        )?;
        let m = f.host(16)?;
        let r = [TransferRequest::new(
            Transport::Unspec,
            m.base_address(),
            0,
            8,
        )];
        let report = f
            .engine
            .submit_and_wait(TransferOp::Write, &f.segment, &r, WAIT)?;
        match kind {
            "file" | "setup" => {
                assert!(report.locally_drained && report.failure.is_some());
                assert_eq!(fs::read(&f.path)?, [0; 16]);
                assert_eq!(mock.submits.load(Ordering::Acquire), 0);
            }
            "destroy" => {
                assert!(!report.locally_drained);
                assert!(matches!(
                    report.failure.unwrap().error,
                    Error::Unquiesced { .. }
                ));
                assert_eq!(f.engine.stats()?.inflight_bytes, 8);
                assert!(m.write(0, &[1]).is_err());
                assert!(
                    f.engine
                        .close_file_segment(&f.segment, Duration::ZERO)
                        .is_err()
                );
                assert_eq!(mock.destroys.load(Ordering::Acquire), 1);
            }
            _ => {
                assert!(report.is_complete());
                mock.file_close_error.store(true, Ordering::Release);
                for _ in 0..2 {
                    assert!(matches!(
                        f.engine.close_file_segment(&f.segment, WAIT),
                        Err(Error::Unquiesced { .. })
                    ));
                }
                assert_eq!(mock.file_closes.load(Ordering::Acquire), 1);
            }
        }
    }
    Ok(())
}

#[test]
fn slices_and_partial_submit_keep_task_prefixes_and_concurrent_write_effects() -> Result<()> {
    const SLICE: usize = 16 << 20;
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(
        &mock,
        EngineConfig {
            gds_batch_depth: 4,
            max_transfer_bytes: 3 * SLICE,
            ..Default::default()
        },
        2 * SLICE + 32,
    )?;
    let m = f.host(2 * SLICE + 32)?;
    // The provider accepted some slices before reporting submit failure. The
    // invalid final cookie and the short middle IO both have real terminal receipts.
    *mock.script.lock().unwrap() = vec![
        (COMPLETE, SLICE),
        (COMPLETE, 3),
        (INVALID, 0),
        (COMPLETE, 2),
    ];
    mock.partial.store(true, Ordering::Release);
    let r = [
        request(&m, 0, 0, 2 * SLICE + 1),
        request(&m, 2 * SLICE + 5, (2 * SLICE + 5) as u64, 2),
    ];
    let b = f
        .engine
        .submit_async(TransferOp::Write, &f.segment, &r, None, WAIT)?;
    let status = b.wait(EVENTUAL_PROGRESS)?;
    assert!(status.locally_drained);
    assert_eq!(status.completed_tasks, 1);
    let tasks = b.task_statuses()?;
    assert_eq!(
        (tasks[0].state, tasks[0].transferred_bytes),
        (TaskState::Failed, SLICE + 3)
    );
    assert_eq!(tasks[1].transferred_bytes, 2);
    let groups = b.submissions()?;
    let report = &groups[0];
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (0, 0)
    );
    let fail = report.failure.as_ref().unwrap();
    assert_eq!(
        (
            fail.request_index,
            fail.affected_requests,
            fail.remote_effect
        ),
        (0, 2, RemoteEffect::Unknown)
    );
    assert_eq!(mock.submits.load(Ordering::Acquire), 1);
    assert_eq!(mock.cancels.load(Ordering::Acquire), 1);
    let params = mock.params.lock().unwrap();
    assert_eq!(
        params.iter().map(|p| p.size).collect::<Vec<_>>(),
        [SLICE, SLICE, 1, 2]
    );
    assert_eq!(
        (params[2].file_offset, params[2].buffer_offset),
        ((2 * SLICE) as i64, (2 * SLICE) as i64)
    );
    Ok(())
}

#[test]
fn a_running_task_publishes_its_contiguous_prefix_before_the_last_receipt() -> Result<()> {
    const SLICE: usize = 16 << 20;
    let mock = Mock::new(GdsMode::NativeOnly);
    mock.held_cookie.store(2, Ordering::Release);
    let f = Fixture::new(
        &mock,
        EngineConfig {
            max_transfer_bytes: 3 * SLICE,
            ..Default::default()
        },
        2 * SLICE + 1,
    )?;
    let m = f.host(2 * SLICE + 1)?;
    let file = OpenOptions::new().write(true).open(&f.path)?;
    file.write_all_at(b"G", (2 * SLICE) as u64)?;
    let b = f.engine.submit_async(
        TransferOp::Read,
        &f.segment,
        &[request(&m, 0, 0, m.len())],
        None,
        WAIT,
    )?;
    let end = Instant::now() + EVENTUAL_PROGRESS;
    loop {
        let t = b.task_status(0)?;
        if t.transferred_bytes > 0 {
            // The provider returned cookies 3 and 1. Cookie 2 still owns the gap.
            assert_eq!((t.state, t.transferred_bytes), (TaskState::Running, SLICE));
            assert!(!t.locally_drained);
            break;
        }
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    assert!(m.write(0, &[0]).is_err());
    mock.held_cookie.store(0, Ordering::Release);
    assert_eq!(b.wait(EVENTUAL_PROGRESS)?.transferred_bytes, m.len());
    let mut actual = vec![0x6b; m.len()];
    m.read(0, &mut actual)?;
    assert!(actual[..2 * SLICE].iter().all(|byte| *byte == 0));
    assert_eq!(actual[2 * SLICE], b'G');
    Ok(())
}

#[test]
fn running_cancel_and_failed_io_are_terminal_only_after_receipts() -> Result<()> {
    for receipt in [FAILED, TIMEOUT, CANCELED] {
        let canceled = receipt == CANCELED;
        let mock = Mock::new(GdsMode::NativeOnly);
        *mock.script.lock().unwrap() = vec![(COMPLETE, 2), (receipt, usize::MAX)];
        mock.until_cancel.store(canceled, Ordering::Release);
        mock.poll_error_once.store(!canceled, Ordering::Release);
        let f = Fixture::new(
            &mock,
            EngineConfig {
                max_failover_attempts: 0,
                ..Default::default()
            },
            16,
        )?;
        let m = f.host(16)?;
        let req = [request(&m, 0, 0, 2), request(&m, 4, 4, 2)];
        let b = f
            .engine
            .submit_async(TransferOp::Read, &f.segment, &req, None, WAIT)?;
        if canceled {
            wait_enter(&mock);
            assert!(b.cancel(1)?);
        }
        assert_eq!(b.wait(EVENTUAL_PROGRESS)?.completed_tasks, 1);
        let gs = b.submissions()?;
        assert_eq!((gs[0].completed_requests, gs[0].transferred_bytes), (1, 2));
        let tasks = b.task_statuses()?;
        let failure = tasks[1].failure.as_ref().unwrap();
        assert_eq!(failure.request_index, 1);
        assert_eq!(mock.cancels.load(Ordering::Acquire), 1);
        match receipt {
            CANCELED => {
                assert!(matches!(failure.error, Error::Cancelled));
                assert_eq!(tasks[1].state, TaskState::Cancelled);
            }
            TIMEOUT => assert!(matches!(failure.error, Error::Timeout)),
            _ => assert!(
                matches!(failure.error, Error::Backend { code, .. } if code == FAILED as i32)
            ),
        }
    }
    Ok(())
}

#[test]
fn pending_cufile_io_past_the_destruction_window_keeps_all_owners_until_late_reap() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    mock.held.store(true, Ordering::Release);
    let f = Fixture::new(&mock, EngineConfig::default(), 16)?;
    let m = f.host(16)?;
    let timeout = Duration::from_millis(500);
    let start = Instant::now();
    let mut b = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[request(&m, 0, 0, 8)],
        None,
        timeout,
    )?;
    let status = b.wait(timeout + Duration::from_secs(35))?;
    assert!(start.elapsed() >= timeout + crate::transport::DESTRUCTION_TIMEOUT);
    assert!(!status.locally_drained);
    assert!(matches!(
        b.submissions()?[0].failure.as_ref().unwrap().error,
        Error::Unquiesced { .. }
    ));
    assert_eq!(mock.destroys.load(Ordering::Acquire), 0);
    assert_eq!(mock.cancels.load(Ordering::Acquire), 1);
    assert_eq!(f.engine.stats()?.inflight_bytes, 8);
    assert_eq!(f.engine.stats()?.gds.quarantined_batches, 1);
    f.engine.unregister_memory(&m)?;
    assert!(m.read(0, &mut [0]).is_err());
    assert!(matches!(b.free(), Err(Error::BatchBusy)));
    assert!(matches!(
        f.engine.close_file_segment(&f.segment, Duration::ZERO),
        Err(Error::Timeout)
    ));
    let prior = mock.polls.load(Ordering::Acquire);
    std::thread::scope(|scope| -> Result<()> {
        let closing = scope.spawn(|| f.engine.close_file_segment(&f.segment, WAIT));
        let end = Instant::now() + EVENTUAL_PROGRESS;
        while mock.polls.load(Ordering::Acquire) == prior {
            assert!(Instant::now() < end);
            std::thread::yield_now();
        }
        mock.held.store(false, Ordering::Release);
        closing.join().unwrap()
    })?;
    assert_eq!(mock.destroys.load(Ordering::Acquire), 1);
    assert_eq!(f.engine.stats()?.inflight_bytes, 0);
    assert!(b.status()?.locally_drained);
    b.free()?;
    m.write(0, &[0])?;
    Ok(())
}

#[test]
#[allow(unsafe_code)]
fn a_forked_child_reopens_the_process_driver_before_use() {
    let mock = Mock::new(GdsMode::NativeOnly);
    let driver = Driver::new(mock.clone());
    driver.mode().unwrap();
    driver.mode().unwrap();
    assert_eq!(mock.opens.load(Ordering::Acquire), 1);
    // SAFETY: the child only exercises lock-free PID/open/mode atomics in the shim
    // and immediately _exits. It runs no CUDA, test runtime or inherited destructors.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        let ok = driver.mode().is_ok()
            && driver.mode().is_ok()
            && mock.opens.load(Ordering::Acquire) == 2;
        // SAFETY: terminate only this fork child without running inherited destructors.
        unsafe { libc::_exit(i32::from(!ok)) };
    }
    let mut status = 0;
    // SAFETY: this waits for the exact child and provides writable status storage.
    assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
    assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
    assert_eq!(mock.opens.load(Ordering::Acquire), 1);
}

#[test]
fn runtime_soname_symbols_properties_and_batch_abi() -> Result<()> {
    if let Ok(mode) = std::env::var("ORBITTE_CUFILE_SHIM") {
        let engine = TransferEngine::new(EngineConfig::default())?;
        let stats = engine.stats()?.gds;
        match mode.as_str() {
            "open-error" => assert!(!stats.available),
            "control" | "allow" => assert_eq!(stats.mode, Some(GdsMode::CompatAllowed)),
            "force" => assert_eq!(stats.mode, Some(GdsMode::CompatForced)),
            _ => {
                assert_eq!(stats.mode, Some(GdsMode::NativeOnly));
                let dir = std::env::var_os("ORBITTE_EXT4_DIR")
                    .map_or_else(std::env::temp_dir, PathBuf::from);
                let path = dir.join(format!(
                    "orbitte-cuFile-{}",
                    crate::SourceEpoch::generate()?
                ));
                fs::write(&path, b"................")?;
                let file = engine.open_file_segment(
                    &format!("file://{}", path.display()),
                    Permission::RemoteReadWrite,
                )?;
                let memory = engine.register_memory(
                    b"abcdefgh".to_vec().into_boxed_slice(),
                    Permission::LocalOnly,
                )?;
                let req = [request(&memory, 2, 3, 4)];
                assert!(
                    engine
                        .submit_and_wait(TransferOp::Write, &file, &req, WAIT)?
                        .is_complete()
                );
                assert_eq!(&fs::read(&path)?, b"...cdef.........");
                memory.write(0, b"00000000")?;
                assert!(
                    engine
                        .submit_and_wait(TransferOp::Read, &file, &req, WAIT)?
                        .is_complete()
                );
                let mut bytes = [0; 8];
                memory.read(0, &mut bytes)?;
                assert_eq!(&bytes, b"00cdef00");
                engine.close_file_segment(&file, WAIT)?;
                fs::remove_file(path)?;
            }
        }
        return Ok(());
    }
    let out = std::env::temp_dir().join(format!(
        "orbitte-cufile-loader-{}",
        crate::SourceEpoch::generate()?
    ));
    fs::create_dir(&out)?;
    let c =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gds/cufile_shim.c");
    assert!(
        std::process::Command::new("cc")
            .args(["-std=c11", "-shared", "-fPIC", "-Wall", "-Werror"])
            .arg(c)
            .arg("-o")
            .arg(out.join("libcufile.so.0"))
            .status()?
            .success()
    );
    for mode in ["native", "control", "allow", "force", "open-error"] {
        let run = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "transport::gds::tests::runtime_soname_symbols_properties_and_batch_abi",
            ])
            .env("ORBITTE_CUFILE_SHIM", mode)
            .env("LD_LIBRARY_PATH", &out)
            .output()?;
        assert!(
            run.status.success() && String::from_utf8_lossy(&run.stdout).contains("1 passed"),
            "{} {}",
            String::from_utf8_lossy(&run.stdout),
            String::from_utf8_lossy(&run.stderr)
        );
    }
    fs::remove_dir_all(out)?;
    Ok(())
}

#[test]
fn native_only_excludes_compat_and_unsafe_extents_before_submit() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(
        &mock,
        EngineConfig {
            max_failover_attempts: 0,
            gds_require_native: true,
            ..Default::default()
        },
        8192,
    )?;
    let m = f.host(8192)?;
    let r = [request(&m, 1, 1, 4096)];
    let report = f
        .engine
        .submit_and_wait(TransferOp::Write, &f.segment, &r, WAIT)?;
    assert!(matches!(
        report.failure.unwrap().error,
        Error::Unavailable(_)
    ));
    assert_eq!(mock.submits.load(Ordering::Acquire), 0);
    let limited = Mock::new(GdsMode::NativeOnly);
    limited.native_slice_limit.store(4096, Ordering::Release);
    let f = Fixture::new(
        &limited,
        EngineConfig {
            max_failover_attempts: 0,
            gds_require_native: true,
            ..Default::default()
        },
        8192,
    )?;
    let memory = f.host(8192 + 4096)?;
    let aligned = (memory.base_address().next_multiple_of(4096) - memory.base_address()) as usize;
    let report = f.engine.submit_and_wait(
        TransferOp::Read,
        &f.segment,
        &[request(&memory, aligned, 0, 8192)],
        WAIT,
    )?;
    assert!(matches!(
        report.failure.unwrap().error,
        Error::Unavailable(_)
    ));
    assert_eq!(limited.submits.load(Ordering::Acquire), 0);
    assert!(
        f.engine
            .submit_and_wait(
                TransferOp::Read,
                &f.segment,
                &[request(&memory, aligned, 0, 4096)],
                WAIT
            )?
            .is_complete()
    );
    Ok(())
}

#[cfg(feature = "io-uring")]
#[test]
fn file_selection_follows_named_order_and_only_selection_can_choose_io_uring() -> Result<()> {
    for (mode, policy, expected_gds) in [
        (
            GdsMode::NativeOnly,
            Some(
                r#"{"transports":{"force_tcp":true},"policy":[{"name":"staged","segment_type":"file","transports":["io_uring"]},{"name":"native","segment_type":"file","intent_type":"checkpoint","transports":["gds"]}]}"#,
            ),
            true,
        ),
        (GdsMode::CompatForced, None, false),
        (
            GdsMode::NativeOnly,
            Some(r#"{"policy":[{"segment_type":"file","transports":["io_uring","gds"]}]}"#),
            false,
        ),
    ] {
        let mock = Mock::new(mode);
        let f = Fixture::new(
            &mock,
            EngineConfig {
                gds_require_native: !expected_gds,
                selector: policy
                    .map(crate::TransportSelectorConfig::from_json)
                    .transpose()?,
                ..Default::default()
            },
            16,
        )?;
        if f.engine.io_uring.is_none() {
            assert_ne!(
                std::env::var("ORBITTE_REQUIRE_IO_URING").as_deref(),
                Ok("1")
            );
            eprintln!("skipping optional io_uring path: kernel unavailable");
            return Ok(());
        }
        let m = f.host(16)?;
        let request = TransferRequest::new(Transport::Unspec, m.base_address(), 0, 4);
        let req = [if expected_gds {
            request.with_policy_name("native")?
        } else {
            request
        }];
        assert!(
            f.engine
                .submit_and_wait(TransferOp::Write, &f.segment, &req, WAIT)?
                .is_complete()
        );
        assert_eq!(mock.submits.load(Ordering::Acquire) > 0, expected_gds);
        assert_eq!(&fs::read(&f.path)?[..4], &[0x6b; 4]);
    }
    Ok(())
}

#[test]
fn slice_capacity_rejection_has_no_provider_side_effects() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    let config = EngineConfig {
        max_failover_attempts: 0,
        gds_batch_depth: 1,
        max_transfer_bytes: 32 << 20,
        ..Default::default()
    };
    let f = Fixture::new(&mock, config, (16 << 20) + 1)?;
    let m = f.host((16 << 20) + 1)?;
    let report = f.engine.submit_and_wait(
        TransferOp::Write,
        &f.segment,
        &[request(&m, 0, 0, m.len())],
        WAIT,
    )?;
    assert!(matches!(
        report.failure.unwrap().error,
        Error::ResourceExhausted
    ));
    assert_eq!(mock.file_registers.load(Ordering::Acquire), 0);
    assert!(mock.depths.lock().unwrap().is_empty());
    assert_eq!(mock.submits.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn deferred_sweep_skips_permanent_destroy_and_reclaims_independent_work() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    let config = EngineConfig {
        enable_progress_worker: true,
        max_allocated_batches: 2,
        ..Default::default()
    };
    let f = Fixture::new(&mock, config, 32)?;
    let stuck = f.host(8)?;
    mock.destroy_error.store(true, Ordering::Release);
    let mut b = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[request(&stuck, 0, 0, 8)],
        None,
        WAIT,
    )?;
    assert!(!b.wait(EVENTUAL_PROGRESS)?.locally_drained);
    b.free()?;
    let end = Instant::now() + EVENTUAL_PROGRESS;
    while f.engine.stats()?.quarantined_batches != 1 {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    #[cfg(feature = "metrics")]
    assert_eq!(
        f.engine.metrics().unwrap().json()["tent_quarantined_batches_total"],
        1
    );
    let healthy = f.host(8)?;
    mock.destroy_error.store(false, Ordering::Release);
    mock.held.store(true, Ordering::Release);
    let mut b = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &[request(&healthy, 0, 8, 8)],
        None,
        WAIT,
    )?;
    while mock.submits.load(Ordering::Acquire) != 2 {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    b.free()?;
    let held = f.engine.stats()?;
    assert_eq!(
        (
            held.quarantined_batches,
            held.deferred_batches,
            held.inflight_bytes
        ),
        (1, 1, 16)
    );
    assert!(matches!(
        f.engine.allocate_batch(1),
        Err(Error::ResourceExhausted)
    ));
    assert!(healthy.read(0, &mut [0]).is_err());
    mock.held.store(false, Ordering::Release);
    while f.engine.stats()?.allocated_batches != 1 {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    for _ in 0..8 {
        f.engine.allocate_batch(1)?.free()?;
    }
    let done = f.engine.stats()?;
    assert_eq!(
        (
            done.quarantined_batches,
            done.deferred_batches,
            done.inflight_bytes
        ),
        (1, 0, 8)
    );
    assert_eq!(mock.destroys.load(Ordering::Acquire), 2);
    assert!(stuck.read(0, &mut [0]).is_err());
    healthy.read(0, &mut [0])?;
    Ok(())
}

#[cfg(feature = "io-uring")]
#[test]
fn clean_file_submit_rejection_retries_each_owner_without_reissuing_its_neighbours() -> Result<()> {
    let mock = Mock::new(GdsMode::CompatAllowed);
    mock.setup_error.store(true, Ordering::Release);
    let f = Fixture::new(&mock, Default::default(), 16)?;
    let local = f.host(16)?;
    let pattern: [u8; 16] = std::array::from_fn(|i| i as u8);
    local.write(0, &pattern)?;
    let requests = [request(&local, 2, 1, 3), request(&local, 8, 10, 4)];
    let b = f
        .engine
        .submit_async(TransferOp::Write, &f.segment, &requests, None, WAIT)?;
    assert_eq!(b.wait(WAIT)?.completed_tasks, 2);
    for (i, t) in b.task_statuses()?.iter().enumerate() {
        assert_eq!(
            (t.physical_owner, t.transport, t.failover_count),
            (i, Transport::IoUring, 1)
        );
    }
    let mut expected = [0; 16];
    expected[1..4].copy_from_slice(&pattern[2..5]);
    expected[10..14].copy_from_slice(&pattern[8..12]);
    assert_eq!(fs::read(&f.path)?, expected);
    assert_eq!(mock.submits.load(Ordering::Acquire), 0);
    Ok(())
}

#[cfg(feature = "io-uring")]
#[test]
fn a_drained_file_read_can_switch_but_a_posted_write_and_a_short_read_cannot() -> Result<()> {
    for (op, first) in [
        (TransferOp::Read, (FAILED, 0)),
        (TransferOp::Write, (FAILED, 0)),
        (TransferOp::Read, (COMPLETE, 1)),
    ] {
        let mock = Mock::new(GdsMode::CompatAllowed);
        mock.script.lock().unwrap().push(first);
        let f = Fixture::new(&mock, Default::default(), 16)?;
        let pattern: [u8; 16] = std::array::from_fn(|i| i as u8);
        fs::write(&f.path, pattern)?;
        let local = f.host(16)?;
        let requests = [request(&local, 2, 1, 3), request(&local, 8, 10, 4)];
        let b = f
            .engine
            .submit_async(op, &f.segment, &requests, None, WAIT)?;
        b.wait(WAIT)?;
        let tasks = b.task_statuses()?;
        let recovered = op == TransferOp::Read && first.0 == FAILED;
        assert_eq!(
            tasks[0].state,
            if recovered {
                TaskState::Completed
            } else {
                TaskState::Failed
            }
        );
        assert_eq!(tasks[0].failover_count, usize::from(recovered));
        assert_eq!(tasks[1].failover_count, 0);
        assert_eq!(tasks[1].state, TaskState::Completed);
        assert_eq!(mock.submits.load(Ordering::Acquire), 1);
        let mut bytes = [0; 16];
        local.read(0, &mut bytes)?;
        let mut expected = [0x6b; 16];
        let mut file = pattern;
        if op == TransferOp::Read {
            let n = if recovered { 3 } else { 1 };
            expected[2..2 + n].copy_from_slice(&pattern[1..1 + n]);
            expected[8..12].copy_from_slice(&pattern[10..14]);
        } else {
            file[10..14].fill(0x6b);
            assert_eq!(
                tasks[0].failure.as_ref().unwrap().remote_effect,
                RemoteEffect::Unknown
            );
        }
        assert_eq!(expected, bytes);
        assert_eq!(fs::read(&f.path)?, file);
    }
    Ok(())
}
