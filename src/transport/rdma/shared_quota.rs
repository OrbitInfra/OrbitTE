//! TENT's process-wide priority slots, with no rotating thread or hot-path lock.

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::thread;
use std::time::{Duration, Instant};

use crate::{Error, Priority, Result};

const MAGIC: [u8; 8] = *b"ORTEQ001";
const HEADER_SIZE: u64 = 32;

pub(super) struct SharedSlotManager {
    // Pin the coordination object and clock namespace until all workers exit.
    _object: File,
    _clock_domain: File,
    interval_ns: u64,
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
        Self::validate(name, interval)?;
        let name =
            CString::new(name).map_err(|_| Error::InvalidConfig("invalid shared quota name"))?;
        let clock = File::open("/proc/self/ns/time")?;
        let domain = clock.metadata()?;
        // shm_open sets CLOEXEC. Never truncate an existing coordination object.
        let fd = unsafe {
            libc::shm_open(
                name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let object = unsafe { File::from_raw_fd(fd) };
        let metadata = object.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o022 != 0
        {
            return Err(Error::PermissionDenied);
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if !matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) {
                return Err(error.into());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            thread::sleep(Duration::from_millis(1));
        }
        let interval_ns = interval.as_nanos() as u64;
        let mut expected = [0u8; HEADER_SIZE as usize];
        expected[..8].copy_from_slice(&MAGIC);
        expected[8..16].copy_from_slice(&interval_ns.to_le_bytes());
        expected[16..24].copy_from_slice(&domain.dev().to_le_bytes());
        expected[24..32].copy_from_slice(&domain.ino().to_le_bytes());
        let length = object.metadata()?.len();
        let mut header = [0u8; HEADER_SIZE as usize];
        if length == HEADER_SIZE {
            object.read_exact_at(&mut header, 0)?;
        } else if length != 0 {
            return Err(Error::InvalidConfig(
                "incompatible RDMA shared quota object size",
            ));
        }
        if header[..8] == [0; 8] {
            // A creator may die before publishing magic. The kernel releases
            // its file lock; the next opener can finish initialization safely.
            object.set_len(HEADER_SIZE)?;
            object.write_all_at(&expected[8..], 8)?;
            object.write_all_at(&MAGIC, 0)?;
        } else if header != expected {
            return Err(Error::InvalidConfig(
                "RDMA shared quota version, interval or clock namespace mismatch",
            ));
        }
        if unsafe { libc::flock(object.as_raw_fd(), libc::LOCK_UN) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let manager = Self {
            _object: object,
            _clock_domain: clock,
            interval_ns,
        };
        manager.current_slot()?;
        Ok(manager)
    }

    pub(super) fn current_slot(&self) -> Result<(Priority, Duration)> {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        let now = u64::try_from(time.tv_sec)
            .ok()
            .and_then(|seconds| seconds.checked_mul(1_000_000_000))
            .zip(
                u64::try_from(time.tv_nsec)
                    .ok()
                    .filter(|&nanos| nanos < 1_000_000_000),
            )
            .and_then(|(seconds, nanos)| seconds.checked_add(nanos))
            .ok_or(Error::Unavailable("invalid RDMA shared slot clock"))?;
        Ok(self.slot_at(now))
    }

    fn slot_at(&self, now: u64) -> (Priority, Duration) {
        let priority = match (now / self.interval_ns) % 3 {
            0 => Priority::High,
            1 => Priority::Medium,
            _ => Priority::Low,
        };
        (
            priority,
            Duration::from_nanos(self.interval_ns - now % self.interval_ns),
        )
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/shared_quota.rs"]
mod tests;
