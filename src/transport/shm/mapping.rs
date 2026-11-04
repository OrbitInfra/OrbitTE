use super::{
    sys::{Namespace, Stat, Sys, unlink_same},
    wire::{Descriptor, GATES, Name},
};
use crate::{Error, Result, SourceEpoch};
use std::{
    fs::File,
    io,
    mem::ManuallyDrop,
    ops::Range,
    os::unix::fs::MetadataExt,
    ptr::NonNull,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

pub(super) struct Budget {
    max_count: usize,
    max_bytes: usize,
    usage: Mutex<(usize, usize)>,
}
impl Budget {
    pub fn new(max_count: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            max_count,
            max_bytes,
            usage: Mutex::default(),
        })
    }
    pub fn charge(self: &Arc<Self>, size: usize) -> Result<Charge> {
        if size == 0 || size as u64 >= GATES as u64 || size > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let mut used = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("SHM mapping budget lock"))?;
        if used.0 >= self.max_count || size > self.max_bytes - used.1 {
            return Err(Error::ResourceExhausted);
        }
        used.0 += 1;
        used.1 += size;
        Ok(Charge {
            budget: self.clone(),
            size,
        })
    }
}
pub(super) struct Charge {
    budget: Arc<Budget>,
    size: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        let mut u = self.budget.usage.lock().unwrap_or_else(|e| e.into_inner());
        u.0 -= 1;
        u.1 -= self.size;
    }
}

pub(super) struct Parts {
    pub address: NonNull<u8>,
    pub size: usize,
    pub id: Stat,
    pub name: Name,
    fd: File,
    ns: Arc<Namespace>,
    _charge: Charge,
}
pub(super) struct Mapping {
    pub sys: &'static Sys,
    source: bool,
    parts: ManuallyDrop<Box<Parts>>,
    closed: Option<Result<()>>,
}
impl std::ops::Deref for Mapping {
    type Target = Parts;
    fn deref(&self) -> &Parts {
        &self.parts
    }
}
// SAFETY: mapping lifetime is reference counted. Dereferences use checked ranges protected
// by distinct kernel lock owners, including in other processes. Retirement needs exclusive access.
unsafe impl Send for Mapping {}
// SAFETY: the mapping address is immutable. All payload access uses the inode's range locks.
unsafe impl Sync for Mapping {}
impl Mapping {
    pub fn fd(&self) -> &File {
        &self.parts.fd
    }
    fn close(&mut self) -> Result<()> {
        if let Some(previous) = &self.closed {
            return previous.as_ref().map(|_| ()).map_err(Error::for_report);
        }
        let extent = (self.sys.unmap)(self.address, self.size).map_err(|e| Error::Unquiesced {
            operation: "SHM munmap",
            code: e.raw_os_error().unwrap_or(-1),
        });
        let result = extent.and_then(|()| {
            let orphan = !self.source
                && !(self.sys.conflicts)(self.fd(), GATES, 1, false).map_err(|e| {
                    Error::Unquiesced {
                        operation: "SHM name retirement",
                        code: e.raw_os_error().unwrap_or(-1),
                    }
                })?;
            if self.source || orphan {
                unlink_same(self.sys, &self.parts.ns, self.name, self.id).map_err(|e| {
                    Error::Unquiesced {
                        operation: "SHM unlink",
                        code: e.raw_os_error().unwrap_or(-1),
                    }
                })?;
            }
            // SAFETY: munmap succeeded and the exclusive owner releases these parts exactly once.
            unsafe {
                ManuallyDrop::drop(&mut self.parts);
            }
            Ok(())
        });
        self.closed = Some(result.as_ref().map(|_| ()).map_err(Error::for_report));
        result
    }
    pub fn retire(mut self) -> Result<()> {
        self.close()
    }
    pub fn range_lock(&self, offset: usize, len: usize, write: bool) -> Result<File> {
        let fd = (self.sys.reopen)(self.fd(), write)?;
        (self.sys.lock)(&fd, offset as i64, len as i64, write).map_err(lock_error)?;
        Ok(fd)
    }
    pub fn import(
        sys: &'static Sys,
        ns: &Arc<Namespace>,
        d: Descriptor,
        writable: bool,
        charge: Charge,
    ) -> Result<Self> {
        let fd = (sys.open)(ns, d.name, false, writable).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                Error::RegionNotFound
            } else {
                e.into()
            }
        })?;
        let stat = (sys.stat)(&fd)?;
        if (stat.dev, stat.ino) != (d.dev, d.ino) || stat.size < d.size {
            return Err(Error::RegionNotFound);
        }
        if !(sys.conflicts)(&fd, d.live(writable), 1, false)? {
            return Err(if (sys.conflicts)(&fd, d.live(!writable), 1, false)? {
                Error::PermissionDenied
            } else {
                Error::RegionNotFound
            });
        }
        let size = usize::try_from(d.size).map_err(|_| Error::InvalidRange)?;
        let address = (sys.map)(&fd, size, writable)?;
        Ok(Self {
            sys,
            source: false,
            parts: ManuallyDrop::new(Box::new(Parts {
                address,
                size,
                id: stat,
                name: d.name,
                fd,
                ns: ns.clone(),
                _charge: charge,
            })),
            closed: None,
        })
    }
}
impl Drop for Mapping {
    fn drop(&mut self) {
        if self.closed.is_none() {
            if let Err(e) = self.close() {
                log::error!("retaining shared mapping: {e}");
            }
        }
    }
}
fn lock_error(e: io::Error) -> Error {
    if matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EACCES)) {
        Error::Unavailable("SHM byte range is in use")
    } else {
        e.into()
    }
}

pub(crate) struct Local {
    pub(super) mapping: Mapping,
    next_gate: AtomicU64,
    pub(super) engine: SourceEpoch,
}
impl Local {
    pub(super) fn create(
        sys: &'static Sys,
        ns: &Arc<Namespace>,
        budget: &Arc<Budget>,
        engine: SourceEpoch,
        size: usize,
    ) -> Result<Self> {
        let charge = budget.charge(size)?;
        let pid = std::process::id();
        let pidns = std::fs::metadata("/proc/self/ns/pid")?.ino();
        for _ in 0..8 {
            let name = Name {
                ns: pidns,
                pid,
                nonce: SourceEpoch::generate()?,
            };
            let fd = match (sys.open)(ns, name, true, true) {
                Ok(fd) => fd,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            };
            let created = (sys.stat)(&fd)?;
            let initialized = (|| {
                (sys.lock)(&fd, GATES, 1, true)?;
                (sys.resize)(&fd, size)?;
                let stat = (sys.stat)(&fd)?;
                if stat.size != size as u64 {
                    return Err(io::Error::other("wrong SHM allocation size"));
                }
                Ok((stat, (sys.map)(&fd, size, true)?))
            })();
            let (id, address) = match initialized {
                Ok(v) => v,
                Err(e) => {
                    if let Err(cleanup) = unlink_same(sys, ns, name, created) {
                        log::error!("SHM allocation rollback: {cleanup}");
                    }
                    return Err(e.into());
                }
            };
            let mapping = Mapping {
                sys,
                source: true,
                parts: ManuallyDrop::new(Box::new(Parts {
                    fd,
                    ns: ns.clone(),
                    _charge: charge,
                    address,
                    size,
                    id,
                    name,
                })),
                closed: None,
            };
            return Ok(Self {
                mapping,
                next_gate: AtomicU64::new(1),
                engine,
            });
        }
        Err(Error::ResourceExhausted)
    }
    pub(crate) fn address(&self) -> NonNull<u8> {
        self.mapping.address
    }
    pub(crate) fn object(&self) -> (u64, u64) {
        (self.mapping.id.dev, self.mapping.id.ino)
    }
    pub(crate) fn size(&self) -> usize {
        self.mapping.size
    }
    pub(crate) fn access(&self, range: &Range<usize>, write: bool) -> Result<File> {
        self.mapping.range_lock(range.start, range.len(), write)
    }
    pub(crate) fn retire(self) -> Result<()> {
        self.mapping.retire()
    }
    pub(super) fn grant(
        &self,
        id: u64,
        offset: usize,
        permission: crate::Permission,
    ) -> Result<(Descriptor, File)> {
        let gate = self
            .next_gate
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                (v < (i64::MAX as u64 - GATES as u64 - 3) / 3).then_some(v + 1)
            })
            .map_err(|_| Error::ResourceExhausted)?;
        let m = &self.mapping;
        let d = Descriptor {
            id,
            offset: offset as u64,
            size: m.size as u64,
            gate,
            dev: m.id.dev,
            ino: m.id.ino,
            name: m.name,
        };
        let owner = (m.sys.reopen)(m.fd(), true)?;
        for w in [false, true] {
            if permission.allows(w) {
                (m.sys.lock)(&owner, d.live(w), 1, true)?;
            }
        }
        Ok((d, owner))
    }
    pub(super) fn in_namespace(&self, ns: &Namespace) -> bool {
        self.mapping.parts.ns.cxl() == ns.cxl() && self.mapping.parts.ns.path == ns.path
    }
}
impl Descriptor {
    pub(super) fn live(&self, write: bool) -> i64 {
        GATES + 3 * self.gate as i64 + i64::from(write)
    }
    pub(super) fn activity(&self) -> i64 {
        self.live(false) + 2
    }
}

/// An enabling process can collect abandoned names from the same PID namespace.
/// A live or inaccessible PID, or a surviving creator FD (including after fork),
/// always leaves the name alone. In-flight mappings survive unlink by inode.
pub(super) fn reap_orphans(sys: &Sys, ns: &Namespace, bound: usize) -> Result<()> {
    let pidns = std::fs::metadata("/proc/self/ns/pid")?.ino();
    for entry in std::fs::read_dir(&ns.path)?.take(bound.max(64)) {
        let entry = entry?;
        let Some(n) = entry.file_name().to_str().and_then(Name::parse) else {
            continue;
        };
        if n.ns != pidns
            || !matches!(std::fs::metadata(format!("/proc/{}",n.pid)),Err(e) if e.kind()==io::ErrorKind::NotFound)
        {
            continue;
        }
        let fd = match (sys.open)(ns, n, false, false) {
            Ok(fd) => fd,
            Err(_) => continue,
        };
        if !(sys.conflicts)(&fd, GATES, 1, false)? {
            unlink_same(sys, ns, n, (sys.stat)(&fd)?)?;
        }
    }
    Ok(())
}
