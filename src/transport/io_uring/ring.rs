use std::io;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use io_uring::{EnterFlags, IoUring, squeue, types};

// One submitting/reaping thread and a plain ring (no SQPOLL). Only an explicit
// submit consumes SQEs. Waiting for a CQE must never submit a failed tail.
pub(super) trait Ring {
    fn space(&mut self) -> usize;
    fn queued(&mut self) -> usize;
    fn push(&mut self, entries: &[squeue::Entry]) -> io::Result<()>;
    fn submit(&mut self, count: u32) -> io::Result<usize>;
    fn peek(&mut self) -> io::Result<Option<(u64, i32)>>;
    fn reap(&mut self, until: Instant) -> io::Result<(u64, i32)>;
}

pub(super) fn create(count: u32) -> io::Result<Box<dyn Ring>> {
    #[cfg(test)]
    super::tests::RING_SETUPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(Box::new(IoUring::new(count)?))
}

#[allow(unsafe_code)]
impl Ring for IoUring {
    fn space(&mut self) -> usize {
        let sq = self.submission();
        sq.capacity() - sq.len()
    }

    fn queued(&mut self) -> usize {
        self.submission().len()
    }

    fn push(&mut self, entries: &[squeue::Entry]) -> io::Result<()> {
        // SAFETY: each SQE refers to its live submission's fd, iovec and exclusive
        // memory loan or bounce buffer. The owner will drain or quarantine all of them.
        unsafe { self.submission().push_multiple(entries) }
            .map_err(|_| io::Error::from_raw_os_error(libc::ENOSPC))
    }

    fn submit(&mut self, count: u32) -> io::Result<usize> {
        // SAFETY: count covers only published SQEs in this single-issuer ring.
        unsafe { self.submitter().enter::<libc::sigset_t>(count, 0, 0, None) }
    }

    fn peek(&mut self) -> io::Result<Option<(u64, i32)>> {
        Ok(self
            .completion()
            .next()
            .map(|cqe| (cqe.user_data(), cqe.result())))
    }

    fn reap(&mut self, until: Instant) -> io::Result<(u64, i32)> {
        loop {
            let wait = until.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
            }
            if let Some(cqe) = self.completion().next() {
                return Ok((cqe.user_data(), cqe.result()));
            }
            if self.params().is_feature_ext_arg() {
                let ts = types::Timespec::from(wait);
                let args = types::SubmitArgs::new().timespec(&ts);
                // SAFETY: EXT_ARG selects SubmitArgs' kernel layout, and ts lives
                // through enter. Zero submissions keeps any unconsumed tail untouched.
                unsafe {
                    self.submitter().enter(
                        0,
                        1,
                        (EnterFlags::GETEVENTS | EnterFlags::EXT_ARG).bits(),
                        Some(&args),
                    )
                }?;
            } else {
                wait_readable(self, wait)?;
            }
        }
    }
}

// Kernels before EXT_ARG expose CQ readiness on the plain ring fd.
#[allow(unsafe_code)]
pub(super) fn wait_readable(ring: &IoUring, wait: Duration) -> io::Result<()> {
    let mut fd = libc::pollfd {
        fd: ring.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout = libc::timespec {
        tv_sec: wait.as_secs().try_into().unwrap_or(libc::time_t::MAX),
        tv_nsec: wait.subsec_nanos() as _,
    };
    // SAFETY: fd and timeout remain valid for this one-fd wait; no signal mask is supplied.
    let n = unsafe { libc::ppoll(&mut fd, 1, &timeout, std::ptr::null()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if n > 0 && fd.revents & libc::POLLIN == 0 {
        return Err(io::Error::from_raw_os_error(libc::EIO));
    }
    Ok(())
}
