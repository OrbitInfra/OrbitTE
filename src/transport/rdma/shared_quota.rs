//! Cross-process priority arbitration. The mapped slot controls dispatch;
//! byte charges and native release receipts belong to the local DeviceSelector.
mod sys;

use crate::{Error, Priority, Result};
use std::{
    ffi::CString,
    fs::File,
    io,
    mem::ManuallyDrop,
    os::unix::fs::{FileExt, MetadataExt},
    ptr::NonNull,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const MAGIC: [u8; 8] = *b"ORTESLOT";
const FORMAT: u32 = 1;
const SLOT_OFFSET: usize = 12;
const HEADER_SIZE: usize = 40;
const TICK: Duration = Duration::from_millis(1);
const ATTACH_TIMEOUT: Duration = Duration::from_secs(1);

fn header(interval: u64, clock: &File) -> Result<[u8; HEADER_SIZE]> {
    let domain = clock.metadata()?;
    let mut out = [0; HEADER_SIZE];
    out[..8].copy_from_slice(&MAGIC);
    out[8..12].copy_from_slice(&FORMAT.to_ne_bytes());
    out[16..24].copy_from_slice(&interval.to_ne_bytes());
    out[24..32].copy_from_slice(&domain.dev().to_ne_bytes());
    out[32..].copy_from_slice(&domain.ino().to_ne_bytes());
    Ok(out)
}
fn priority(slot: u32) -> Result<Priority> {
    match slot {
        0 => Ok(Priority::High),
        1 => Ok(Priority::Medium),
        2 => Ok(Priority::Low),
        _ => Err(Error::InvalidConfig("invalid RDMA shared quota slot")),
    }
}
fn slot_at(now: u64, interval: u64) -> (Priority, Duration) {
    let p = match (now / interval) % 3 {
        0 => Priority::High,
        1 => Priority::Medium,
        _ => Priority::Low,
    };
    (p, Duration::from_nanos(interval - now % interval))
}
// The pool can report a cleanup failure after the last worker releases its map.
#[derive(Clone, Default)]
pub(super) struct SlotRelease(Arc<OnceLock<i32>>);

impl SlotRelease {
    pub(super) fn check(&self) -> Result<()> {
        match self.0.get() {
            Some(0) => Ok(()),
            Some(&code) => Err(Error::Unquiesced {
                operation: "RDMA shared slot munmap",
                code,
            }),
            None => Err(Error::Unquiesced {
                operation: "RDMA shared slot still owned",
                code: libc::EBUSY,
            }),
        }
    }
}

struct Mapping {
    base: NonNull<u8>,
    object: ManuallyDrop<File>,
    // Pin the clock namespace used to compute slot boundaries.
    _clock: File,
    calls: &'static sys::Calls,
    interval: u64,
    release: SlotRelease,
}
// SAFETY: the owned fixed-size shared mapping outlives every Arc. The slot is
// the only mutable field. Every reader and writer uses an aligned atomic.
unsafe impl Send for Mapping {}
// SAFETY: immutable metadata was verified under an exclusive creation lock;
// concurrent publishers serialize with a kernel lock and publish atomically.
unsafe impl Sync for Mapping {}
impl Mapping {
    fn slot(&self) -> &AtomicU32 {
        // SAFETY: mmap returns a page-aligned base and SLOT_OFFSET is aligned for
        // AtomicU32. Attachment initialized and pins all four bytes until Drop.
        unsafe { &*self.base.as_ptr().add(SLOT_OFFSET).cast::<AtomicU32>() }
    }
    fn refresh(&self) -> Result<()> {
        if !(self.calls.lock)(&self.object)? {
            return Ok(());
        }
        let update = (|| {
            priority(self.slot().load(Ordering::Acquire))?;
            let now = (self.calls.now)()?;
            self.slot()
                .store(slot_at(now, self.interval).0 as u32, Ordering::Release);
            Ok(())
        })();
        (self.calls.unlock)(&self.object)?;
        update
    }
}
impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the last Arc owns the mapping and there are no remaining readers.
        if let Err(e) = unsafe { (self.calls.unmap)(self.base) } {
            let _ = self.release.0.set(e.raw_os_error().unwrap_or(libc::EIO));
            log::error!("RDMA slot munmap failed; retaining its descriptor: {e}");
            return;
        }
        // SAFETY: unmap succeeded and this is the sole destruction of the fd.
        unsafe { ManuallyDrop::drop(&mut self.object) };
        let _ = self.release.0.set(0);
    }
}

struct Publisher {
    mapping: Arc<Mapping>,
    stop: AtomicBool,
    failed: AtomicBool,
}
pub(super) struct SharedSlotManager {
    publisher: Arc<Publisher>,
    worker: Option<thread::JoinHandle<()>>,
}
impl SharedSlotManager {
    pub(super) fn validate(name: &str, interval: Duration) -> Result<()> {
        if !name.starts_with('/')
            || name.len() < 2
            || name.len() > 255
            || name[1..].contains(['/', '\0'])
            || matches!(&name[1..], "." | "..")
            || interval.is_zero()
            || interval.as_nanos() > u64::MAX as u128
        {
            return Err(Error::InvalidConfig(
                "invalid RDMA shared quota name or slot interval",
            ));
        }
        Ok(())
    }
    pub(super) fn attach(name: &str, interval: Duration) -> Result<Self> {
        Self::attach_with(name, interval, &sys::OS)
    }
    fn attach_with(name: &str, interval: Duration, calls: &'static sys::Calls) -> Result<Self> {
        Self::validate(name, interval)?;
        let name = CString::new(name)
            .map_err(|_| Error::InvalidConfig("invalid RDMA shared quota name"))?;
        let clock = File::open("/proc/self/ns/time")?;
        let expected = header(interval.as_nanos() as u64, &clock)?;
        let (object, created) = (calls.open)(&name)?;
        let meta = object.metadata()?;
        // SAFETY: geteuid reads the effective UID and takes no arguments.
        if !meta.is_file() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o022 != 0 {
            return Err(Error::PermissionDenied);
        }
        let deadline = Instant::now() + ATTACH_TIMEOUT;
        loop {
            if (calls.lock)(&object)? {
                let size = object.metadata()?.len();
                if created {
                    if size != 0 {
                        return Err(Error::InvalidConfig(
                            "unexpected data in new RDMA shared quota object",
                        ));
                    }
                    object.write_all_at(&expected, 0)?;
                } else if size == 0 && Instant::now() < deadline {
                    // A joiner can open the name before its creator takes the lock.
                    (calls.unlock)(&object)?;
                    thread::sleep(TICK);
                    continue;
                } else {
                    let mut found = [0; HEADER_SIZE];
                    if size != HEADER_SIZE as u64 {
                        return Err(Error::InvalidConfig(
                            "incompatible RDMA shared quota object size",
                        ));
                    }
                    object.read_exact_at(&mut found, 0)?;
                    priority(u32::from_ne_bytes([
                        found[12], found[13], found[14], found[15],
                    ]))?;
                    if found[..12] != expected[..12] || found[16..] != expected[16..] {
                        return Err(Error::InvalidConfig(
                            "RDMA shared quota identity, interval or clock mismatch",
                        ));
                    }
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            thread::sleep(TICK);
        }
        let base = (calls.map)(&object)?;
        let release = SlotRelease::default();
        let mapping = Arc::new(Mapping {
            base,
            object: ManuallyDrop::new(object),
            _clock: clock,
            calls,
            interval: interval.as_nanos() as u64,
            release: release.clone(),
        });
        Self::start(mapping).map_err(|error| release.check().err().unwrap_or(error))
    }
    pub(super) fn release_receipt(&self) -> SlotRelease {
        self.publisher.mapping.release.clone()
    }
    fn start(mapping: Arc<Mapping>) -> Result<Self> {
        (mapping.calls.unlock)(&mapping.object)?;
        mapping.refresh()?;
        let publisher = Arc::new(Publisher {
            mapping,
            stop: AtomicBool::default(),
            failed: AtomicBool::default(),
        });
        let owner = publisher.clone();
        let worker = thread::Builder::new()
            .name("orbitte-rdma-slot".into())
            .spawn(move || {
                while !owner.stop.load(Ordering::Acquire) {
                    if let Err(e) = owner.mapping.refresh() {
                        owner.failed.store(true, Ordering::Release);
                        log::error!("RDMA shared slot publisher stopped: {e}");
                        break;
                    }
                    thread::park_timeout(TICK);
                }
            })?;
        Ok(Self {
            publisher,
            worker: Some(worker),
        })
    }
    pub(super) fn current_slot(&self) -> Result<(Priority, Duration)> {
        if self.publisher.failed.load(Ordering::Acquire) {
            return Err(Error::Unavailable("RDMA shared slot publisher failed"));
        }
        let m = &self.publisher.mapping;
        let published = priority(m.slot().load(Ordering::Acquire))?;
        let (current, delay) = slot_at((m.calls.now)()?, m.interval);
        // A delayed or killed writer cannot extend a permissive slot past the
        // clock boundary. The mapped slot can only narrow this worker's tier.
        Ok((published.min(current), delay))
    }
}
impl Drop for SharedSlotManager {
    fn drop(&mut self) {
        self.publisher.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            if worker.join().is_err() {
                log::error!("RDMA shared slot publisher panicked");
            }
        }
    }
}
#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/shared_quota.rs"]
pub(super) mod tests;
