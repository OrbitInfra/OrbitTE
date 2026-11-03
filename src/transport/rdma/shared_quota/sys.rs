//! Fallible Linux calls for the shared priority slot. The file-description
//! lock is released by the kernel even if a publisher is killed.
use super::*;
use std::os::fd::{AsRawFd, FromRawFd};
use std::ptr::NonNull;

pub(super) struct Calls {
    pub open: fn(&CString) -> io::Result<(File, bool)>,
    pub lock: fn(&File) -> io::Result<bool>,
    pub unlock: fn(&File) -> io::Result<()>,
    pub map: fn(&File) -> io::Result<NonNull<u8>>,
    // Only the Mapping that received this address can release it.
    pub unmap: unsafe fn(NonNull<u8>) -> io::Result<()>,
    pub now: fn() -> Result<u64>,
}
pub(super) static OS: Calls = Calls {
    open,
    lock,
    unlock,
    map,
    unmap,
    now,
};
fn open(name: &CString) -> io::Result<(File, bool)> {
    let flags = libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    // SAFETY: the name is NUL-terminated. Exclusive creation distinguishes an
    // owned new object from an existing empty or foreign file.
    let mut fd =
        unsafe { libc::shm_open(name.as_ptr(), flags | libc::O_CREAT | libc::O_EXCL, 0o600) };
    let created = fd >= 0;
    if !created {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error);
        }
        // SAFETY: the same name remains valid; an existing object is never truncated.
        fd = unsafe { libc::shm_open(name.as_ptr(), flags, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // SAFETY: a successful shm_open transferred one new owned descriptor.
    Ok((unsafe { File::from_raw_fd(fd) }, created))
}
fn lock(f: &File) -> io::Result<bool> {
    // SAFETY: f keeps the descriptor open throughout this nonblocking call.
    if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(true);
    }
    let e = io::Error::last_os_error();
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => Ok(false),
        _ => Err(e),
    }
}
fn unlock(f: &File) -> io::Result<()> {
    // SAFETY: the caller holds the lock on this live open file description.
    if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
fn map(f: &File) -> io::Result<NonNull<u8>> {
    // SAFETY: attachment validated the object's exact extent while locked. All
    // participating processes preserve that extent for the life of the object.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            HEADER_SIZE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            f.as_raw_fd(),
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    if let Some(p) = NonNull::new(p.cast()) {
        return Ok(p);
    }
    // SAFETY: a successful, null-address mapping is unsuitable for a Rust
    // reference, but still belongs to this call and has the specified extent.
    if unsafe { libc::munmap(p, HEADER_SIZE) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Err(io::Error::other("null RDMA slot mapping"))
}
unsafe fn unmap(base: NonNull<u8>) -> io::Result<()> {
    // SAFETY: the Mapping owner passes its original base and exact fixed extent
    // once, after all readers and its publisher have relinquished it.
    if unsafe { libc::munmap(base.as_ptr().cast(), HEADER_SIZE) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
fn now() -> Result<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is writable output storage for the monotonic clock.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    u64::try_from(ts.tv_sec)
        .ok()
        .and_then(|s| s.checked_mul(1_000_000_000))
        .zip(
            u64::try_from(ts.tv_nsec)
                .ok()
                .filter(|n| *n < 1_000_000_000),
        )
        .and_then(|(s, n)| s.checked_add(n))
        .ok_or(Error::Unavailable("invalid RDMA shared slot clock"))
}
