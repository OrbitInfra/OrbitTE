use super::*;
use crate::test_support::EVENTUAL_PROGRESS;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::atomic::AtomicU64;

static NEXT: AtomicU64 = AtomicU64::new(0);

pub(in crate::transport::rdma) struct Object(CString);
impl Object {
    pub(in crate::transport::rdma) fn new() -> Self {
        Self(
            CString::new(format!(
                "/orbite-slots-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
            .unwrap(),
        )
    }
    pub(in crate::transport::rdma) fn name(&self) -> &str {
        self.0.to_str().unwrap()
    }
}
impl Drop for Object {
    fn drop(&mut self) {
        // SAFETY: the fixture owns this unique NUL-terminated name until unlink.
        unsafe {
            libc::shm_unlink(self.0.as_ptr());
        }
    }
}
impl SharedSlotManager {
    pub(in crate::transport::rdma) fn attach(name: &str, interval: Duration) -> Result<Self> {
        Self::install(name, interval, &mut None)
    }
}
const INTERVAL: Duration = Duration::from_millis(2);

fn lock_until(f: &File) {
    let until = Instant::now() + EVENTUAL_PROGRESS;
    while !(sys::OS.lock)(f).unwrap() {
        assert!(Instant::now() < until, "live lock never released");
        thread::yield_now();
    }
}

#[test]
fn shared_slot_publication_is_visible_and_never_extends_a_stale_tier() {
    static NOW: AtomicU64 = AtomicU64::new(0);
    static CALLS: sys::Calls = sys::Calls {
        now: || Ok(NOW.load(Ordering::Acquire)),
        ..sys::OS
    };
    let object = Object::new();
    let first = SharedSlotManager::attach_with(object.name(), INTERVAL, &CALLS, &mut None).unwrap();
    let second =
        SharedSlotManager::attach_with(object.name(), INTERVAL, &CALLS, &mut None).unwrap();
    for now in [
        0,
        1_999_999,
        2_000_000,
        3_999_999,
        4_000_000,
        5_999_999,
        6_000_000,
        u64::MAX,
    ] {
        assert_eq!(
            slot_at(now, 2_000_000),
            (
                [Priority::High, Priority::Medium, Priority::Low][((now / 2_000_000) % 3) as usize],
                Duration::from_nanos(2_000_000 - now % 2_000_000)
            )
        );
    }
    let (held, _) = (sys::OS.open)(&object.0).unwrap();
    lock_until(&held);
    let slot = first.publisher.mapping.slot();
    NOW.store(4_000_000, Ordering::Release);
    slot.store(0, Ordering::Release);
    assert_eq!(
        second.current_slot().unwrap().0,
        Priority::High,
        "live writer's slot gates both processes"
    );
    slot.store(2, Ordering::Release);
    NOW.store(0, Ordering::Release);
    assert_eq!(
        second.current_slot().unwrap().0,
        Priority::High,
        "a stalled permissive slot cannot outlive its clock boundary"
    );
    NOW.store(4_000_000, Ordering::Release);
    assert_eq!(second.current_slot().unwrap().0, Priority::Low);
    slot.store(9, Ordering::Release);
    assert!(matches!(
        second.current_slot(),
        Err(Error::InvalidConfig(_))
    ));
    slot.store(0, Ordering::Release);
    (sys::OS.unlock)(&held).unwrap();
}

#[test]
fn foreign_partial_and_corrupt_objects_fail_without_reinitialization() {
    for name in ["", "/", "relative", "/a/b", "/..", "/nul\0"] {
        assert!(SharedSlotManager::validate(name, INTERVAL).is_err());
    }
    let object = Object::new();
    assert!(SharedSlotManager::validate(object.name(), Duration::ZERO).is_err());
    drop(SharedSlotManager::attach(object.name(), INTERVAL).unwrap());
    assert!(matches!(
        SharedSlotManager::attach(object.name(), Duration::from_millis(3)),
        Err(Error::InvalidConfig(_))
    ));
    let (file, _) = (sys::OS.open)(&object.0).unwrap();
    let mut good = [0; HEADER_SIZE];
    file.read_exact_at(&mut good, 0).unwrap();
    for i in [0, 8, 12, 16, 24, 32] {
        let mut invalid = good;
        invalid[i] ^= 0xff;
        file.write_all_at(&invalid, 0).unwrap();
        assert!(matches!(
            SharedSlotManager::attach(object.name(), INTERVAL),
            Err(Error::InvalidConfig(_))
        ));
        let mut found = [0; HEADER_SIZE];
        file.read_exact_at(&mut found, 0).unwrap();
        assert_eq!(found, invalid);
    }
    file.set_len(7).unwrap();
    assert!(matches!(
        SharedSlotManager::attach(object.name(), INTERVAL),
        Err(Error::InvalidConfig(_))
    ));
    assert_eq!(file.metadata().unwrap().len(), 7);
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o666))
        .unwrap();
    assert!(matches!(
        SharedSlotManager::attach(object.name(), INTERVAL),
        Err(Error::PermissionDenied)
    ));
}

#[test]
fn slot_attachment_propagates_native_failures_and_releases_initialization_lock() {
    static LOCK: sys::Calls = sys::Calls {
        lock: |_| Err(io::Error::from_raw_os_error(libc::EIO)),
        ..sys::OS
    };
    static MAP: sys::Calls = sys::Calls {
        map: |_| Err(io::Error::from_raw_os_error(libc::ENOMEM)),
        ..sys::OS
    };
    static CLOCK: sys::Calls = sys::Calls {
        now: || Err(Error::Unavailable("shim clock")),
        ..sys::OS
    };
    static UNLOCK: sys::Calls = sys::Calls {
        unlock: |_| Err(io::Error::from_raw_os_error(libc::EIO)),
        ..sys::OS
    };
    let object = Object::new();
    drop(SharedSlotManager::attach(object.name(), INTERVAL).unwrap());
    for calls in [&LOCK, &MAP, &CLOCK, &UNLOCK] {
        assert!(SharedSlotManager::attach_with(object.name(), INTERVAL, calls, &mut None).is_err());
        drop(SharedSlotManager::attach(object.name(), INTERVAL).unwrap());
    }
}

// A private control socket orders both child handoffs and bounds every wait.
// Never take a file-description lock in a thread that shares the publisher's fd.
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(object: &Object, mode: &str) -> (Process, BufReader<TcpStream>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut child = Process(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "transport::rdma::shared_quota::tests::shared_slot_subprocess",
                "--ignored",
                "--nocapture",
            ])
            .env("ORBITE_TEST_SLOT_NAME", object.name())
            .env(
                "ORBITE_TEST_SLOT_PEER",
                listener.local_addr().unwrap().to_string(),
            )
            .env("ORBITE_TEST_SLOT_MODE", mode)
            .spawn()
            .unwrap(),
    );
    let until = Instant::now() + EVENTUAL_PROGRESS;
    loop {
        match listener.accept() {
            Ok((s, _)) => {
                s.set_read_timeout(Some(EVENTUAL_PROGRESS)).unwrap();
                s.set_write_timeout(Some(EVENTUAL_PROGRESS)).unwrap();
                return (child, BufReader::new(s));
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                assert!(
                    child.0.try_wait().unwrap().is_none() && Instant::now() < until,
                    "child did not connect"
                );
                thread::sleep(TICK);
            }
            Err(e) => panic!("{e}"),
        }
    }
}
fn expect(ch: &mut BufReader<TcpStream>, expected: &str) {
    let mut line = String::new();
    ch.read_line(&mut line).unwrap();
    assert_eq!(line.trim_end(), expected);
}

#[test]
fn killed_publisher_releases_the_shared_lock_without_stealing_a_live_nic_charge() {
    let object = Object::new();
    let (mut victim, mut v) = start(&object, "victim");
    expect(&mut v, "charged 101");
    let (mut survivor, mut s) = start(&object, "survivor");
    expect(&mut s, "charged 203");
    writeln!(v.get_mut(), "lock").unwrap();
    expect(&mut v, "locked");
    let (observer, _) = (sys::OS.open)(&object.0).unwrap();
    assert!(
        !(sys::OS.lock)(&observer).unwrap(),
        "a live process owns the publish lock"
    );
    victim.0.kill().unwrap();
    assert!(!victim.0.wait().unwrap().success());
    writeln!(s.get_mut(), "recover").unwrap();
    expect(&mut s, "released 203");
    assert!(survivor.0.wait().unwrap().success());
    SharedSlotManager::attach(object.name(), INTERVAL)
        .unwrap()
        .current_slot()
        .unwrap();
}

#[test]
fn independent_process_attach_and_killed_initializer_recovery() {
    let object = Object::new();
    let (mut init, mut control) = start(&object, "initialize");
    expect(&mut control, "header initialized and locked");
    init.0.kill().unwrap();
    assert!(!init.0.wait().unwrap().success());
    SharedSlotManager::attach(object.name(), INTERVAL)
        .unwrap()
        .current_slot()
        .unwrap();
}

#[test]
#[ignore = "fixture launched by the real subprocess tests"]
fn shared_slot_subprocess() {
    use crate::MemoryLocation;
    use crate::transport::rdma::{
        RdmaConfig,
        quota::{DeviceSelector, tests::topology},
    };
    let name = std::env::var("ORBITE_TEST_SLOT_NAME").unwrap();
    let mut control = BufReader::new(
        TcpStream::connect(std::env::var("ORBITE_TEST_SLOT_PEER").unwrap()).unwrap(),
    );
    control
        .get_ref()
        .set_read_timeout(Some(EVENTUAL_PROGRESS))
        .unwrap();
    control
        .get_ref()
        .set_write_timeout(Some(EVENTUAL_PROGRESS))
        .unwrap();
    let mode = std::env::var("ORBITE_TEST_SLOT_MODE").unwrap();
    if mode == "initialize" {
        let (f, created) = (sys::OS.open)(&CString::new(name).unwrap()).unwrap();
        assert!(created);
        lock_until(&f);
        let domain = File::open("/proc/self/ns/time").unwrap();
        f.write_all_at(&header(INTERVAL.as_nanos() as u64, &domain).unwrap(), 0)
            .unwrap();
        writeln!(control.get_mut(), "header initialized and locked").unwrap();
        expect(&mut control, "parent should kill us");
        return;
    }
    let owner = SharedSlotManager::attach(&name, INTERVAL).unwrap();
    let (lock, _) = (sys::OS.open)(&CString::new(name).unwrap()).unwrap();
    let selector = DeviceSelector::new(&topology(), 5, &RdmaConfig::default()).unwrap();
    let bytes = if mode == "victim" { 101 } else { 203 };
    let lease = selector
        .allocate(bytes, bytes, 1, MemoryLocation::Cpu(None), 4)
        .unwrap()
        .next()
        .unwrap();
    let inflight = || {
        selector
            .stats()
            .iter()
            .find(|s| s.device_id == 2)
            .unwrap()
            .inflight_bytes
    };
    assert_eq!(inflight(), bytes);
    writeln!(control.get_mut(), "charged {bytes}").unwrap();
    if mode == "victim" {
        expect(&mut control, "lock");
        lock_until(&lock);
        assert_eq!(inflight(), 101);
        writeln!(control.get_mut(), "locked").unwrap();
        expect(&mut control, "parent should kill us");
    } else {
        expect(&mut control, "recover");
        lock_until(&lock);
        assert_eq!(inflight(), 203);
        (sys::OS.unlock)(&lock).unwrap();
        let mut seen = 0;
        let until = Instant::now() + EVENTUAL_PROGRESS;
        while seen != 7 {
            seen |= 1 << (owner.current_slot().unwrap().0 as u32);
            assert!(
                Instant::now() < until,
                "survivor failed to publish every tier"
            );
            thread::sleep(Duration::from_micros(250));
        }
        assert_eq!(inflight(), 203);
        drop(lease);
        assert_eq!(inflight(), 0);
        writeln!(control.get_mut(), "released {bytes}").unwrap();
    }
}

#[test]
fn an_uncertain_unmap_is_attempted_once_and_retains_the_descriptor() {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    static UNMAPS: AtomicU64 = AtomicU64::new(0);
    unsafe fn fail(_: NonNull<u8>) -> io::Result<()> {
        UNMAPS.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::from_raw_os_error(libc::EIO))
    }
    static CALLS: sys::Calls = sys::Calls {
        unmap: fail,
        ..sys::OS
    };
    let object = Object::new();
    let mut receipt = None;
    let owner =
        SharedSlotManager::attach_with(object.name(), INTERVAL, &CALLS, &mut receipt).unwrap();
    let release = receipt.unwrap();
    assert!(matches!(
        release.check(),
        Err(Error::Unquiesced {
            code: libc::EBUSY,
            ..
        })
    ));
    let map = &owner.publisher.mapping;
    let fd = map.object.as_raw_fd();
    let base = map.base;
    let ino = map.object.metadata().unwrap().ino();
    drop(owner);
    for _ in 0..2 {
        assert!(matches!(
            release.check(),
            Err(Error::Unquiesced {
                operation: "RDMA shared slot munmap",
                code: libc::EIO
            })
        ));
    }
    assert_eq!(UNMAPS.load(Ordering::Relaxed), 1);
    assert_eq!(
        std::fs::metadata(format!("/proc/self/fd/{fd}"))
            .unwrap()
            .ino(),
        ino
    );
    // SAFETY: the injected call above performed no OS action. With the publisher
    // joined, the fixture exclusively owns the intentionally retained map and fd.
    unsafe {
        (sys::OS.unmap)(base).unwrap();
        assert_eq!(libc::close(fd), 0);
    }
}

#[test]
fn failed_slot_start_retains_the_original_cleanup_receipt() {
    use std::os::fd::AsRawFd;
    use std::sync::atomic::AtomicI32;
    static FD: AtomicI32 = AtomicI32::new(-1);
    static UNMAPS: AtomicU64 = AtomicU64::new(0);
    unsafe fn uncertain(base: NonNull<u8>) -> io::Result<()> {
        UNMAPS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: this is the mapping owner's one destruction call.
        unsafe { (sys::OS.unmap)(base) }?;
        Err(io::Error::from_raw_os_error(libc::EIO))
    }
    static FAIL: sys::Calls = sys::Calls {
        unlock: |file| {
            FD.store(file.as_raw_fd(), Ordering::Relaxed);
            (sys::OS.unlock)(file)?;
            Err(io::Error::from_raw_os_error(libc::EACCES))
        },
        unmap: uncertain,
        ..sys::OS
    };
    let object = Object::new();
    let mut receipt = None;
    let error = SharedSlotManager::attach_with(object.name(), INTERVAL, &FAIL, &mut receipt)
        .err()
        .unwrap();
    assert!(matches!(
        error,
        Error::Unquiesced {
            operation: "RDMA shared slot munmap",
            code: libc::EIO
        }
    ));
    let receipt = receipt.unwrap();
    for _ in 0..2 {
        assert!(matches!(
            receipt.check(),
            Err(Error::Unquiesced {
                code: libc::EIO,
                ..
            })
        ));
    }
    assert_eq!(UNMAPS.load(Ordering::Relaxed), 1);
    // SAFETY: our shim actually unmapped the object. The manager retained this
    // exact descriptor, and startup never handed it to a worker.
    assert_eq!(unsafe { libc::close(FD.load(Ordering::Relaxed)) }, 0);
}
