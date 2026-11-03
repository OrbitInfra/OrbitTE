use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use super::*;
use crate::{
    EngineConfig, FileSegment, Notification, Permission, QosConfig, QosPolicyFields,
    RegisteredMemory, RuntimeQueueConfig, SourceEpoch, TaskState, TransferEngine, TransferRequest,
    Transport, TransportSelectorConfig,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(feature = "cuda")]
#[derive(Default)]
pub(crate) struct RingTrace {
    pub(crate) lifetime: std::sync::Weak<()>,
    pub(crate) round_widths: Vec<usize>,
}

fn required() -> bool {
    std::env::var("ORBITTE_REQUIRE_IO_URING").as_deref() == Ok("1")
}

pub(crate) fn kernel() -> bool {
    match ring::create(2) {
        Ok(_) => true,
        Err(e) if !required() => {
            eprintln!("skipping real io_uring: ring probe failed: {e}");
            false
        }
        Err(e) => panic!("ORBITTE_REQUIRE_IO_URING=1, but ring setup failed: {e}"),
    }
}

pub(crate) struct Fixture {
    pub(crate) engine: Arc<TransferEngine>,
    pub(crate) segment: FileSegment,
    pub(crate) memory: RegisteredMemory,
    pub(crate) path: PathBuf,
}

impl Fixture {
    pub(crate) fn new(dir: &Path, bytes: &[u8], engine: TransferEngine) -> Result<Self> {
        let path = dir.join(format!("orbitte-uring-{}", SourceEpoch::generate()?));
        fs::write(&path, bytes)?;
        let segment = engine.open_file_segment(
            &format!("file://{}", path.display()),
            Permission::RemoteReadWrite,
        )?;
        let memory =
            engine.register_memory(vec![0x9a; 32768].into_boxed_slice(), Permission::LocalOnly)?;
        Ok(Self {
            engine: Arc::new(engine),
            segment,
            memory,
            path,
        })
    }
    #[cfg(feature = "cuda")]
    pub(crate) fn deny_bounce(&mut self) {
        Arc::get_mut(&mut self.engine)
            .unwrap()
            .io_uring
            .as_mut()
            .unwrap()
            .allocate = |_, _| Err(Error::ResourceExhausted);
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn observe_ring(
        &mut self,
        short_middle: bool,
        before: impl Fn(u32) -> io::Result<()> + Send + Sync + 'static,
    ) -> Arc<Mutex<RingTrace>> {
        let trace = Arc::new(Mutex::new(RingTrace::default()));
        let observed = Arc::clone(&trace);
        Arc::get_mut(&mut self.engine).unwrap().io_uring = Some(IoUringTransport {
            allocate: task::Bounce::new,
            create: Arc::new(move |size| {
                before(size)?;
                let alive = Arc::new(());
                observed.lock().unwrap().lifetime = Arc::downgrade(&alive);
                Ok(Box::new(FaultRing {
                    inner: ring::create(size)?,
                    calls: 0,
                    fault: if short_middle {
                        Fault::MiddleShort
                    } else {
                        Fault::Pass
                    },
                    observer: Some((alive, Arc::clone(&observed))),
                }))
            }),
        });
        trace
    }

    pub(crate) fn request(&self, local: usize, remote: u64, length: usize) -> TransferRequest {
        TransferRequest::new(
            Transport::Unspec,
            self.memory.base_address() + local as u64,
            remote,
            length,
        )
    }
    pub(crate) fn run(&self, op: TransferOp, requests: &[TransferRequest]) -> Result<BatchReport> {
        self.engine
            .submit_and_wait(op, &self.segment, requests, TIMEOUT)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[test]
fn real_files_move_full_bytes_on_ext4_and_tmpfs() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    for (key, direct) in [("ORBITTE_EXT4_DIR", true), ("ORBITTE_TMPFS_DIR", false)] {
        let dir = match std::env::var_os(key) {
            Some(path) => PathBuf::from(path),
            None if required() => panic!("required filesystem fixture {key} missing"),
            None if !direct => PathBuf::from("/dev/shm"),
            None => {
                eprintln!("set {key} to check the ext4 profile");
                continue;
            }
        };
        let start = vec![0xc3; 16384];
        let f = Fixture::new(&dir, &start, TransferEngine::new(EngineConfig::default())?)?;
        let base = f.memory.base_address();
        let aligned = (base.next_multiple_of(4096) - base) as usize;
        // First vector is naturally aligned. Second needs a bounce. Third also
        // requires buffered I/O on a direct-capable file.
        let spans = [
            (aligned, 0, 4096),
            (aligned + 4097, 4096, 4096),
            (aligned + 9001, 9003, 31),
        ];
        let requests: Vec<_> = spans
            .iter()
            .map(|&(l, off, n)| f.request(l, off, n))
            .collect();
        let (lease, _) = f
            .engine
            .files
            .admit(&f.segment, TransferOp::Write, 0, 4096)?;
        let (aligned_fd, _) = lease.io_file(0, 4096)?;
        let (offset_fd, _) = lease.io_file(1, 4096)?;
        let (length_fd, _) = lease.io_file(0, 3)?;
        use std::os::fd::AsRawFd;
        assert_eq!(aligned_fd.as_raw_fd() != lease.file().as_raw_fd(), direct);
        assert_eq!(offset_fd.as_raw_fd(), lease.file().as_raw_fd());
        assert_eq!(length_fd.as_raw_fd(), lease.file().as_raw_fd());
        drop(lease);
        let mut expected_file = start;
        let mut expected_memory = vec![0x9a; f.memory.len()];
        for (i, &(local, remote, n)) in spans.iter().enumerate() {
            let bytes: Vec<_> = (0..n).map(|j| (j * 43 + i + 1) as u8).collect();
            f.memory.write(local, &bytes)?;
            expected_memory[local..local + n].copy_from_slice(&bytes);
            expected_file[remote as usize..remote as usize + n].copy_from_slice(&bytes);
        }
        let report = f.run(TransferOp::Write, &requests)?;
        assert!(report.is_complete() && report.locally_drained, "{report:?}");
        assert_eq!(
            (report.completed_requests, report.transferred_bytes),
            (3, 8223)
        );
        assert_eq!(fs::read(&f.path)?, expected_file);
        f.memory.write(0, &vec![0x9a; f.memory.len()])?;
        let mut reads = requests;
        reads[1].transport = Transport::IoUring;
        let batch = f
            .engine
            .submit_async(TransferOp::Read, &f.segment, &reads, None, TIMEOUT)?;
        assert_eq!(batch.wait(TIMEOUT)?.completed_tasks, 3);
        let mut got = vec![0; f.memory.len()];
        f.memory.read(0, &mut got)?;
        assert_eq!(got, expected_memory);
        assert_eq!(
            f.engine.file_segment_info(&f.segment)?.direct_io,
            Some(direct)
        );
    }
    Ok(())
}

fn truncate_on_ring_creation(depth: u32) -> Result<Fixture> {
    let dir = std::env::temp_dir();
    let mut f = Fixture::new(
        &dir,
        b"abcdefghijklmnop",
        TransferEngine::new(EngineConfig::default())?,
    )?;
    let path = f.path.clone();
    Arc::get_mut(&mut f.engine).unwrap().io_uring = Some(IoUringTransport {
        allocate: task::Bounce::new,
        create: Arc::new(move |n| {
            assert_eq!(n, depth);
            fs::OpenOptions::new().write(true).open(&path)?.set_len(3)?;
            ring::create(n)
        }),
    });
    Ok(f)
}

#[test]
fn truncation_reports_short_and_eof_per_cqe_with_exact_copyback() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = truncate_on_ring_creation(3)?;
    let r = [f.request(1, 0, 8), f.request(21, 8, 4), f.request(41, 0, 2)];
    let batch = f
        .engine
        .submit_async(TransferOp::Read, &f.segment, &r, None, TIMEOUT)?;
    let status = batch.wait(TIMEOUT)?;
    assert!(status.locally_drained);
    assert_eq!((status.completed_tasks, status.transferred_bytes), (1, 5));
    let tasks = batch.task_statuses()?;
    assert_eq!(
        tasks
            .iter()
            .map(|t| (t.state, t.transferred_bytes))
            .collect::<Vec<_>>(),
        [
            (TaskState::Failed, 3),
            (TaskState::Failed, 0),
            (TaskState::Completed, 2)
        ]
    );
    for (i, task) in tasks[..2].iter().enumerate() {
        let failure = task.failure.as_ref().unwrap();
        assert_eq!(failure.request_index, i);
        assert!(matches!(&failure.error, Error::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof));
    }
    let mut got = [0; 48];
    f.memory.read(0, &mut got)?;
    let mut expected = [0x9a; 48];
    expected[1..4].copy_from_slice(b"abc");
    expected[41..43].copy_from_slice(b"ab");
    assert_eq!(got, expected);
    Ok(())
}

#[test]
fn group_report_counts_only_the_successful_prefix_for_concurrent_file_requests() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = truncate_on_ring_creation(2)?;
    let short = f.request(1, 0, 8);
    let full = f.request(21, 1, 2);
    for (requests, prefix, bytes, failed) in [
        ([short.clone(), full.clone()], 0, 0, Some((0, 2))),
        ([full.clone(), short.clone()], 1, 2, Some((1, 1))),
        ([f.request(1, 0, 3), full.clone()], 2, 5, None),
    ] {
        fs::write(&f.path, b"abcdefghijklmnop")?;
        let report = f.run(TransferOp::Read, &requests)?;
        assert!(report.locally_drained);
        assert_eq!(
            (report.completed_requests, report.transferred_bytes),
            (prefix, bytes)
        );
        assert_eq!(
            report
                .failure
                .as_ref()
                .map(|f| (f.request_index, f.affected_requests)),
            failed
        );
        if let Some(failure) = report.failure {
            assert!(
                matches!(failure.error, Error::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof)
            );
            assert_eq!(failure.remote_effect, RemoteEffect::None);
        }
    }

    fs::write(&f.path, b"abcdefghijklmnop")?;
    let batch =
        f.engine
            .submit_async(TransferOp::Read, &f.segment, &[short, full], None, TIMEOUT)?;
    let status = batch.wait(TIMEOUT)?;
    assert_eq!((status.completed_tasks, status.transferred_bytes), (1, 5));
    let tasks = batch.task_statuses()?;
    assert_eq!(
        (tasks[0].state, tasks[0].transferred_bytes),
        (TaskState::Failed, 3)
    );
    assert_eq!(
        (tasks[1].state, tasks[1].transferred_bytes),
        (TaskState::Completed, 2)
    );
    let submissions = batch.submissions()?;
    assert_eq!(
        (
            submissions[0].completed_requests,
            submissions[0].transferred_bytes
        ),
        (0, 0)
    );
    let failure = submissions[0].failure.as_ref().unwrap();
    assert_eq!((failure.request_index, failure.affected_requests), (0, 2));
    Ok(())
}

#[test]
#[allow(unsafe_code)]
fn short_write_reports_only_the_prefix_written_by_the_kernel() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    // RLIMIT_FSIZE is process-wide. Set it only in a child running this one test.
    const CHILD: &str = "ORBITTE_SHORT_WRITE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let result = std::process::Command::new(std::env::current_exe()?)
            .env(CHILD, "1").args(["--exact", "transport::io_uring::tests::short_write_reports_only_the_prefix_written_by_the_kernel"])
            .output()?;
        assert!(
            result.status.success() && String::from_utf8_lossy(&result.stdout).contains("1 passed"),
            "child: {} {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return Ok(());
    }
    let mut expected = vec![0x66; 8192];
    let f = Fixture::new(
        &std::env::temp_dir(),
        &expected,
        TransferEngine::new(EngineConfig::default())?,
    )?;
    let limit = libc::rlimit {
        rlim_cur: 4096,
        rlim_max: 4096,
    };
    // SAFETY: limit is valid for the duration of setrlimit, in this isolated child.
    if unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &limit) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    f.memory.write(1, b"ABCD")?;
    f.memory.write(9, b"XY")?;
    let report = f.run(
        TransferOp::Write,
        &[f.request(1, 4094, 4), f.request(9, 16, 2)],
    )?;
    assert!(report.locally_drained);
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (0, 0)
    );
    let failure = report.failure.unwrap();
    assert!(matches!(failure.error, Error::Io(e) if e.kind() == io::ErrorKind::WriteZero));
    assert_eq!(
        (
            failure.request_index,
            failure.affected_requests,
            failure.remote_effect
        ),
        (0, 2, RemoteEffect::Unknown)
    );
    expected[4094..4096].copy_from_slice(b"AB");
    expected[16..18].copy_from_slice(b"XY");
    assert_eq!(fs::read(&f.path)?, expected);
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    #[cfg(feature = "cuda")]
    MiddleShort,
    #[cfg(feature = "cuda")]
    Pass,
    Short,
    ShortThenError,
    NoSpace,
    ReapError,
    OversizedCqe,
    CqeError,
}

struct FaultRing {
    inner: Box<dyn Ring>,
    fault: Fault,
    calls: usize,
    #[cfg(feature = "cuda")]
    observer: Option<(Arc<()>, Arc<Mutex<RingTrace>>)>,
}

impl Ring for FaultRing {
    fn space(&mut self) -> usize {
        if self.fault == Fault::NoSpace {
            0
        } else {
            self.inner.space()
        }
    }
    fn queued(&mut self) -> usize {
        self.inner.queued()
    }
    fn push(&mut self, entries: &[squeue::Entry]) -> io::Result<()> {
        self.inner.push(entries)?;
        #[cfg(feature = "cuda")]
        if let Some((_, trace)) = &self.observer {
            trace.lock().unwrap().round_widths.push(entries.len());
        }
        Ok(())
    }
    fn submit(&mut self, n: u32) -> io::Result<usize> {
        self.calls += 1;
        match (self.fault, self.calls) {
            (Fault::Short, 2) => Ok(0),
            (Fault::Short | Fault::ShortThenError, 1) => self.inner.submit(1),
            (Fault::ShortThenError, _) => Err(io::Error::from_raw_os_error(libc::EIO)),
            _ => self.inner.submit(n),
        }
    }
    fn reap(&mut self, until: Instant) -> io::Result<(u64, i32)> {
        if self.fault == Fault::ReapError {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        let (id, n) = self.inner.reap(until)?;
        Ok((
            id,
            match self.fault {
                #[cfg(feature = "cuda")]
                Fault::MiddleShort if self.calls == 1 && id == 1 && n > 1 => 1,
                Fault::CqeError => -libc::EIO,
                Fault::OversizedCqe => i32::MAX,
                _ => n,
            },
        ))
    }
}

fn fault_fixture(fault: Fault) -> Result<Fixture> {
    let mut engine = TransferEngine::new(EngineConfig::default())?;
    engine.io_uring = Some(IoUringTransport {
        allocate: task::Bounce::new,
        create: Arc::new(move |n| {
            Ok(Box::new(FaultRing {
                inner: ring::create(n)?,
                fault,
                calls: 0,
                #[cfg(feature = "cuda")]
                observer: None,
            }))
        }),
    });
    Fixture::new(&std::env::temp_dir(), b"abcdefgh", engine)
}

#[test]
fn short_enter_flushes_only_the_tail_or_drains_the_accepted_prefix() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    for fault in [Fault::Short, Fault::ShortThenError, Fault::NoSpace] {
        let f = fault_fixture(fault)?;
        f.memory.write(1, b"12345")?;
        let r = [f.request(1, 2, 2), f.request(3, 5, 2), f.request(5, 7, 1)];
        let report = f.run(TransferOp::Write, &r)?;
        assert!(report.locally_drained);
        let (done, bytes, expected) = match fault {
            Fault::Short => (3, 5, b"ab12e345"),
            Fault::ShortThenError => (1, 2, b"ab12efgh"),
            _ => (0, 0, b"abcdefgh"),
        };
        assert_eq!(
            (report.completed_requests, report.transferred_bytes),
            (done, bytes)
        );
        assert_eq!(fs::read(&f.path)?, expected);
        assert_eq!(report.is_complete(), done == 3);
        if let Some(failure) = report.failure {
            assert_eq!(
                (
                    failure.request_index,
                    failure.affected_requests,
                    failure.remote_effect
                ),
                (done, 1, RemoteEffect::None)
            );
        }
        f.engine.close_file_segment(&f.segment, TIMEOUT)?;
    }
    let f = fault_fixture(Fault::CqeError)?;
    let report = f.run(TransferOp::Read, &[f.request(1, 0, 2)])?;
    assert_eq!(report.transferred_bytes, 0);
    assert!(
        matches!(report.failure.unwrap().error, Error::Backend { code, .. } if code == -libc::EIO)
    );
    let mut bytes = [0; 2];
    f.memory.read(1, &mut bytes)?;
    assert_eq!(bytes, [0x9a; 2]);
    Ok(())
}

#[test]
fn missing_drain_receipt_quarantines_ring_file_memory_and_admission() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    for fault in [Fault::ReapError, Fault::OversizedCqe] {
        let f = fault_fixture(fault)?;
        let report = f.run(TransferOp::Read, &[f.request(1, 0, 4)])?;
        assert!(!report.locally_drained);
        assert!(matches!(
            report.failure.unwrap().error,
            Error::Unquiesced { .. }
        ));
        assert!(f.memory.read(1, &mut [0; 4]).is_err());
        assert_eq!(f.engine.stats()?.inflight_bytes, 4);
        assert!(matches!(
            f.engine.close_file_segment(&f.segment, Duration::ZERO),
            Err(Error::Timeout)
        ));
        assert!(matches!(
            f.engine.close_requester(Duration::ZERO),
            Err(Error::Timeout)
        ));
    }
    Ok(())
}

struct HoldReap {
    inner: Box<dyn Ring>,
    started: mpsc::SyncSender<ReapWait>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    lifetime: Arc<()>,
}

struct ReapWait {
    until: Instant,
    ring: std::sync::Weak<()>,
}

impl Ring for HoldReap {
    fn space(&mut self) -> usize {
        self.inner.space()
    }
    fn queued(&mut self) -> usize {
        self.inner.queued()
    }
    fn push(&mut self, entries: &[squeue::Entry]) -> io::Result<()> {
        self.inner.push(entries)
    }
    fn submit(&mut self, n: u32) -> io::Result<usize> {
        self.inner.submit(n)
    }
    fn reap(&mut self, until: Instant) -> io::Result<(u64, i32)> {
        self.started
            .send(ReapWait {
                until,
                ring: Arc::downgrade(&self.lifetime),
            })
            .unwrap();
        match self
            .release
            .lock()
            .unwrap()
            .recv_timeout(until.saturating_duration_since(Instant::now()))
        {
            Ok(()) => self.inner.reap(until),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err(io::Error::from_raw_os_error(libc::ETIMEDOUT))
            }
            // Withhold the data ring's CQE. Exercise the actual timed syscall on
            // an empty ring, where no completion can wake the waiter early.
            Err(mpsc::RecvTimeoutError::Disconnected) => ring::create(1)?.reap(until),
        }
    }
}

fn held_file(
    config: EngineConfig,
) -> Result<(Fixture, mpsc::Receiver<ReapWait>, mpsc::SyncSender<()>)> {
    let mut engine = TransferEngine::new(config)?;
    let (started, starting) = mpsc::sync_channel(1);
    let (release, releasing) = mpsc::sync_channel(1);
    let releasing = Arc::new(Mutex::new(releasing));
    engine.io_uring = Some(IoUringTransport {
        allocate: task::Bounce::new,
        create: Arc::new(move |n| {
            Ok(Box::new(HoldReap {
                inner: ring::create(n)?,
                started: started.clone(),
                release: Arc::clone(&releasing),
                lifetime: Arc::new(()),
            }))
        }),
    });
    let f = Fixture::new(&std::env::temp_dir(), b"12345678", engine)?;
    Ok((f, starting, release))
}

#[test]
fn a_late_cqe_times_out_only_its_own_task_and_keeps_its_bytes() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let (f, starting, release) = held_file(EngineConfig::default())?;
    let timeout = Duration::from_secs(2);
    let r = [f.request(1, 0, 2), f.request(9, 2, 2)];
    let batch = f
        .engine
        .submit_async(TransferOp::Read, &f.segment, &r, None, timeout)?;
    starting.recv_timeout(TIMEOUT).unwrap();
    release.send(()).unwrap();
    starting.recv_timeout(TIMEOUT).unwrap();
    let interim = batch.status()?;
    assert_eq!((interim.completed_tasks, interim.transferred_bytes), (1, 2));
    assert!(!interim.locally_drained);
    let first = batch
        .task_statuses()?
        .iter()
        .position(|task| task.state == TaskState::Completed)
        .unwrap();
    let mut ready = [0; 2];
    f.memory.read([1, 9][first], &mut ready)?;
    assert_eq!(&ready, [b"12", b"34"][first]);
    assert!(f.memory.read([9, 1][first], &mut ready).is_err());
    std::thread::sleep(timeout);
    release.send(()).unwrap();
    let status = batch.wait(TIMEOUT)?;
    assert!(status.locally_drained);
    assert_eq!((status.completed_tasks, status.transferred_bytes), (1, 4));
    let tasks = batch.task_statuses()?;
    let late = tasks
        .iter()
        .find(|task| task.state == TaskState::Failed)
        .unwrap();
    assert!(matches!(
        late.failure.as_ref().unwrap().error,
        Error::Timeout
    ));
    assert_eq!(late.transferred_bytes, 2);
    let mut actual = [0; 12];
    f.memory.read(0, &mut actual)?;
    let mut expected = [0x9a; 12];
    expected[1..3].copy_from_slice(b"12");
    expected[9..11].copy_from_slice(b"34");
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn a_missing_cqe_at_the_drain_deadline_quarantines_the_submitted_span() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let (f, starting, release) = held_file(EngineConfig::default())?;
    let requests = [f.request(1, 0, 2), f.request(9, 4, 2)];
    let timeout = Duration::from_secs(2);
    let window = Duration::from_secs(30);
    let before = Instant::now();
    let mut batch =
        f.engine
            .submit_async(TransferOp::Write, &f.segment, &requests, None, timeout)?;
    let after = Instant::now();
    let first = starting.recv_timeout(TIMEOUT).unwrap();
    assert!((before + timeout + window..=after + timeout + window).contains(&first.until));
    // One CQE arrives late but within the destruction window. Hide the other.
    std::thread::sleep((first.until - window).saturating_duration_since(Instant::now()));
    release.send(()).unwrap();
    let second = starting.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(second.until, first.until);
    drop(release);

    let status = batch.wait(second.until.saturating_duration_since(Instant::now()) + TIMEOUT)?;
    assert!(!status.locally_drained);
    assert!(Instant::now() >= second.until);
    let submissions = batch.submissions()?;
    let group = &submissions[0];
    assert_eq!((group.completed_requests, group.transferred_bytes), (0, 0));
    let failure = group.failure.as_ref().unwrap();
    assert!(matches!(failure.error, Error::Unquiesced { code, .. } if code == -libc::ETIMEDOUT));
    assert_eq!(
        (
            failure.request_index,
            failure.affected_requests,
            failure.remote_effect
        ),
        (0, 2, RemoteEffect::Unknown)
    );

    let tasks = batch.task_statuses()?;
    let observed = tasks.iter().position(|task| task.locally_drained).unwrap();
    assert!(matches!(
        tasks[observed].failure.as_ref().unwrap().error,
        Error::Timeout
    ));
    assert_eq!(tasks[observed].transferred_bytes, 2);
    assert!(matches!(
        tasks[1 - observed].failure.as_ref().unwrap().error,
        Error::Unquiesced { .. }
    ));
    assert_eq!(tasks[1 - observed].transferred_bytes, 0);

    assert!(second.ring.upgrade().is_some());
    f.engine.unregister_memory(&f.memory)?;
    assert!(f.memory.read([9, 1][observed], &mut [0; 2]).is_err());
    assert_eq!(f.engine.stats()?.inflight_bytes, 4);
    assert!(matches!(
        f.engine.close_file_segment(&f.segment, Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        f.engine.close_requester(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    Ok(())
}

#[test]
fn poll_fallback_times_out_and_wakes_for_a_real_completion() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let mut raw = io_uring::IoUring::new(1)?;
    let wait = Duration::from_millis(20);
    let start = Instant::now();
    ring::wait_readable(&raw, wait)?;
    assert!(start.elapsed() >= wait);
    assert!(raw.completion().next().is_none());
    raw.push(&[io_uring::opcode::Nop::new().build().user_data(7)])?;
    assert_eq!(Ring::submit(&mut raw, 1)?, 1);
    ring::wait_readable(&raw, TIMEOUT)?;
    let cqe = raw.completion().next().unwrap();
    assert_eq!((cqe.user_data(), cqe.result()), (7, 0));
    Ok(())
}

#[test]
fn file_io_shares_qos_queue_deadline_and_close_but_needs_no_receiver_credit() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let config = EngineConfig {
        runtime_queue: Some(RuntimeQueueConfig {
            max_dispatch_owners: 1,
            ..Default::default()
        }),
        qos: Some(QosConfig {
            defaults: QosPolicyFields {
                max_inflight_requests: Some(2),
                ..Default::default()
            },
            ..Default::default()
        }),
        max_receiver_credit_sessions: 1,
        ..Default::default()
    };
    let (f, starting, release) = held_file(config)?;
    let r = [f.request(1, 0, 4)];
    let running = f
        .engine
        .submit_async(TransferOp::Read, &f.segment, &r, None, TIMEOUT)?;
    starting.recv_timeout(TIMEOUT).unwrap();
    let queued = f
        .engine
        .submit_async(TransferOp::Write, &f.segment, &r, None, TIMEOUT)?;
    let stats = f.engine.stats()?.runtime_queue.unwrap();
    assert_eq!((stats.dispatching_owners, stats.queued_owners), (1, 1));
    assert!(matches!(
        f.run(TransferOp::Write, &r),
        Err(Error::ResourceExhausted)
    ));
    assert!(queued.cancel(0)?);
    assert!(queued.wait(TIMEOUT)?.locally_drained);
    assert_eq!(queued.task_status(0)?.state, TaskState::Cancelled);
    let late = f.engine.submit_async(
        TransferOp::Write,
        &f.segment,
        &r,
        None,
        Duration::from_millis(50),
    )?;
    assert!(late.wait(TIMEOUT)?.locally_drained);
    assert!(matches!(
        late.task_status(0)?.failure.unwrap().error,
        Error::Timeout
    ));
    assert!(running.cancel(0)?);
    assert!(matches!(
        f.engine.close_file_segment(&f.segment, Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        f.engine
            .resolve_file_offset(&f.segment, TransferOp::Read, 0, 1),
        Err(Error::FileSegmentClosed)
    ));
    f.engine.unregister_memory(&f.memory)?;
    assert!(f.memory.read(1, &mut [0; 4]).is_err());
    release.send(()).unwrap();
    assert_eq!(running.wait(TIMEOUT)?.completed_tasks, 1);
    let mut got = [0; 4];
    f.memory.read(1, &mut got)?;
    assert_eq!(got, *b"1234");
    assert_eq!(fs::read(&f.path)?, b"12345678");
    f.engine.close_file_segment(&f.segment, TIMEOUT)?;
    assert_eq!(f.engine.stats()?.inflight_bytes, 0);
    Ok(())
}

#[test]
fn file_selection_and_preflight_have_no_effects_on_rejection() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let selector = TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"disk","segment_type":"file","local_memory":"cpu","remote_memory":"cpu","priority":"low","transports":["io_uring"]}]}"#,
    )?;
    let f = Fixture::new(
        &std::env::temp_dir(),
        b"abcd",
        TransferEngine::new(EngineConfig {
            selector: Some(selector),
            ..Default::default()
        })?,
    )?;
    let high = f.request(1, 0, 2);
    assert!(matches!(
        f.run(TransferOp::Read, std::slice::from_ref(&high)),
        Err(Error::Unavailable(_))
    ));
    let low = high.with_priority(crate::Priority::Low);
    assert!(
        f.run(TransferOp::Read, std::slice::from_ref(&low))?
            .is_complete()
    );
    let mut invalid = low.clone();
    invalid.transport = Transport::Tcp;
    assert!(matches!(
        f.run(TransferOp::Write, &[low.clone(), invalid]),
        Err(Error::Unavailable(_))
    ));
    let mut invalid = low.clone();
    invalid.remote_address = u64::MAX;
    assert!(matches!(
        f.run(TransferOp::Write, &[low.clone(), invalid]),
        Err(Error::InvalidRange)
    ));
    let n = Notification {
        name: "x".into(),
        message: "x".into(),
    };
    assert!(matches!(
        f.engine.submit_and_notify(
            TransferOp::Write,
            &f.segment,
            std::slice::from_ref(&low),
            &n,
            TIMEOUT
        ),
        Err(Error::Unavailable(_))
    ));
    // Exclusive memory loans reject aliases before any SQE is published.
    f.memory.write(1, b"XY")?;
    let overlap = f.run(TransferOp::Write, &[low.clone(), low.clone()])?;
    assert!(!overlap.is_complete() && overlap.locally_drained);
    assert_eq!(fs::read(&f.path)?, b"abcd");
    let peer = f
        .engine
        .open_segment(f.engine.endpoint(), f.engine.source_epoch(), TIMEOUT)?;
    let mut wrong = low;
    wrong.transport = Transport::IoUring;
    assert!(matches!(
        f.engine
            .submit_and_wait(TransferOp::Read, &peer, &[wrong], TIMEOUT),
        Err(Error::Unavailable(_))
    ));
    Ok(())
}

#[test]
fn failed_second_bounce_releases_first_loan_before_any_ring_is_created() -> Result<()> {
    let mut engine = TransferEngine::new(EngineConfig::default())?;
    engine.io_uring = Some(IoUringTransport {
        create: Arc::new(|_| panic!("incomplete preflight touched the ring")),
        allocate: |length, alignment| {
            if length == 3 {
                Err(Error::ResourceExhausted)
            } else {
                task::Bounce::new(length, alignment)
            }
        },
    });
    let f = Fixture::new(&std::env::temp_dir(), b"abcdefgh", engine)?;
    let r = [f.request(1, 0, 2), f.request(17, 4, 3)];
    let report = f.run(TransferOp::Write, &r)?;
    assert!(matches!(
        report.failure.unwrap().error,
        Error::ResourceExhausted
    ));
    assert!(report.locally_drained);
    assert_eq!(report.transferred_bytes, 0);
    assert_eq!(fs::read(&f.path)?, b"abcdefgh");
    f.memory.write(1, b"AB")?;
    f.engine.close_file_segment(&f.segment, TIMEOUT)
}

#[test]
fn direct_reopen_uses_the_admitted_inode_and_honors_read_only_access() -> Result<()> {
    if !kernel() {
        return Ok(());
    }
    let f = Fixture::new(
        &std::env::temp_dir(),
        &vec![0x6d; 4096],
        TransferEngine::new(EngineConfig::default())?,
    )?;
    f.engine.close_file_segment(&f.segment, TIMEOUT)?;
    let name = format!("file://{}", f.path.display());
    let segment = f.engine.open_file_segment(&name, Permission::RemoteRead)?;
    f.engine.file_segment_info(&segment)?;
    fs::remove_file(&f.path)?;
    fs::write(&f.path, vec![0xcc; 4096])?;
    let r = [f.request(1, 0, 4096)];
    assert!(matches!(
        f.engine
            .submit_and_wait(TransferOp::Write, &segment, &r, TIMEOUT),
        Err(Error::PermissionDenied)
    ));
    assert!(
        f.engine
            .submit_and_wait(TransferOp::Read, &segment, &r, TIMEOUT)?
            .is_complete()
    );
    let mut bytes = vec![0; 4096];
    f.memory.read(1, &mut bytes)?;
    assert_eq!(bytes, vec![0x6d; 4096]);
    assert_eq!(fs::read(&f.path)?, vec![0xcc; 4096]);
    f.engine.close_file_segment(&segment, TIMEOUT)
}
