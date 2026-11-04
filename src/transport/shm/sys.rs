//! Checked Linux boundary. A consumer opens existing objects without O_CREAT.
use super::wire::Name;
use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    io,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::Arc,
};

pub(super) struct Namespace {
    pub path: PathBuf,
    dir: Option<File>,
}
impl Namespace {
    pub fn new(path: &Path) -> io::Result<Arc<Self>> {
        let dir = if path.as_os_str().is_empty() {
            None
        } else {
            if !path.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "SHM mount must be absolute",
                ));
            }
            Some(
                OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(path)?,
            )
        };
        Ok(Arc::new(Self {
            path: if dir.is_some() {
                path.to_owned()
            } else {
                "/dev/shm".into()
            },
            dir,
        }))
    }
    pub fn cxl(&self) -> bool {
        self.dir.is_some()
    }
    fn leaf(&self, name: Name) -> io::Result<CString> {
        let leaf = name.basename();
        CString::new(if self.cxl() { leaf } else { format!("/{leaf}") }).map_err(io::Error::other)
    }
}

#[derive(Clone, Copy)]
pub(super) struct Stat {
    pub dev: u64,
    pub ino: u64,
    pub size: u64,
}
#[derive(Clone, Copy)]
pub(super) struct Sys {
    pub open: fn(&Namespace, Name, bool, bool) -> io::Result<File>,
    pub resize: fn(&File, usize) -> io::Result<()>,
    pub stat: fn(&File) -> io::Result<Stat>,
    pub map: fn(&File, usize, bool) -> io::Result<NonNull<u8>>,
    pub unmap: fn(NonNull<u8>, usize) -> io::Result<()>,
    pub unlink: fn(&Namespace, Name) -> io::Result<()>,
    pub reopen: fn(&File, bool) -> io::Result<File>,
    pub lock: fn(&File, i64, i64, bool) -> io::Result<()>,
    pub unlock: fn(&File, i64, i64) -> io::Result<()>,
    pub conflicts: fn(&File, i64, i64, bool) -> io::Result<bool>,
}
pub(super) const NATIVE: Sys = Sys {
    open,
    resize,
    stat,
    map,
    unmap,
    unlink,
    reopen,
    lock,
    unlock,
    conflicts,
};
fn open(ns: &Namespace, name: Name, create: bool, write: bool) -> io::Result<File> {
    let leaf = ns.leaf(name)?;
    let flags = libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | libc::O_NONBLOCK
        | if write { libc::O_RDWR } else { libc::O_RDONLY }
        | if create {
            libc::O_CREAT | libc::O_EXCL
        } else {
            0
        };
    // SAFETY: the namespace FD and NUL-terminated leaf outlive the call, and mode is supplied for creation.
    let fd = unsafe {
        match &ns.dir {
            Some(dir) => libc::openat(dir.as_raw_fd(), leaf.as_ptr(), flags, 0o600 as libc::mode_t),
            None => libc::shm_open(leaf.as_ptr(), flags, 0o600),
        }
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: open returned a new descriptor and this File assumes its sole ownership.
    Ok(unsafe { File::from_raw_fd(fd) })
}
fn resize(fd: &File, size: usize) -> io::Result<()> {
    let len = libc::off_t::try_from(size).map_err(io::Error::other)?;
    // SAFETY: fd owns the exclusively created object and len is representable in off_t.
    if unsafe { libc::ftruncate(fd.as_raw_fd(), len) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: this reserves storage within the freshly sized object before mappings become visible.
    let rc = unsafe { libc::posix_fallocate(fd.as_raw_fd(), 0, len) };
    if rc != 0 {
        return Err(io::Error::from_raw_os_error(rc));
    }
    Ok(())
}
fn stat(fd: &File) -> io::Result<Stat> {
    let m = fd.metadata()?;
    if !m.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SHM backing is not a regular object",
        ));
    }
    Ok(Stat {
        dev: m.dev(),
        ino: m.ino(),
        size: m.len(),
    })
}
fn map(fd: &File, size: usize, write: bool) -> io::Result<NonNull<u8>> {
    let flags = libc::PROT_READ | if write { libc::PROT_WRITE } else { 0 };
    // SAFETY: the owner checked the finite object size; MAP_SHARED keeps the opened inode alive through unlink.
    let raw = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            flags,
            libc::MAP_SHARED,
            fd.as_raw_fd(),
            0,
        )
    };
    if raw == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    NonNull::new(raw.cast()).ok_or_else(|| io::Error::other("SHM mapping at address zero"))
}
fn unmap(address: NonNull<u8>, size: usize) -> io::Result<()> {
    // SAFETY: the sole retirement owner supplies the original mapping and length after its last borrow.
    if unsafe { libc::munmap(address.as_ptr().cast(), size) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
fn unlink(ns: &Namespace, name: Name) -> io::Result<()> {
    let leaf = ns.leaf(name)?;
    // SAFETY: the bounded leaf is a CString and the optional directory remains open.
    let rc = unsafe {
        match &ns.dir {
            Some(dir) => libc::unlinkat(dir.as_raw_fd(), leaf.as_ptr(), 0),
            None => libc::shm_unlink(leaf.as_ptr()),
        }
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
fn reopen(fd: &File, write: bool) -> io::Result<File> {
    // A dup shares its open-file-description locks. This open creates a separate lock owner,
    // including when the name has been unlinked or a new inode has replaced it.
    OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_CLOEXEC)
        .open(format!("/proc/self/fd/{}", fd.as_raw_fd()))
}
fn record(fd: &File, start: i64, len: i64, kind: libc::c_int, query: bool) -> io::Result<bool> {
    fn offset(n: i64) -> io::Result<libc::off_t> {
        let v = n as libc::off_t;
        if i128::from(v) == i128::from(n) {
            Ok(v)
        } else {
            Err(io::Error::from_raw_os_error(libc::EOVERFLOW))
        }
    }
    let mut range = libc::flock {
        l_type: kind as _,
        l_whence: libc::SEEK_SET as _,
        l_start: offset(start)?,
        l_len: offset(len)?,
        l_pid: 0,
    };
    let cmd = if query {
        libc::F_OFD_GETLK
    } else {
        libc::F_OFD_SETLK
    };
    loop {
        // SAFETY: fd remains live and the initialized flock is writable for the kernel's query result.
        if unsafe { libc::fcntl(fd.as_raw_fd(), cmd, &mut range) } >= 0 {
            return Ok(query && i32::from(range.l_type) != libc::F_UNLCK);
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}
fn lock(fd: &File, start: i64, len: i64, write: bool) -> io::Result<()> {
    record(
        fd,
        start,
        len,
        if write { libc::F_WRLCK } else { libc::F_RDLCK },
        false,
    )
    .map(|_| ())
}
fn unlock(fd: &File, start: i64, len: i64) -> io::Result<()> {
    record(fd, start, len, libc::F_UNLCK, false).map(|_| ())
}
fn conflicts(fd: &File, start: i64, len: i64, write: bool) -> io::Result<bool> {
    record(
        fd,
        start,
        len,
        if write { libc::F_WRLCK } else { libc::F_RDLCK },
        true,
    )
}

pub(super) fn unlink_same(sys: &Sys, ns: &Namespace, name: Name, id: Stat) -> io::Result<()> {
    match (sys.open)(ns, name, false, false) {
        Ok(fd) => {
            let now = (sys.stat)(&fd)?;
            if (now.dev, now.ino) == (id.dev, id.ino) {
                match (sys.unlink)(ns, name) {
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    other => other?,
                }
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
