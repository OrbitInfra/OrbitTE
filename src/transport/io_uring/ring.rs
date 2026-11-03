use std::io;

use io_uring::{EnterFlags, IoUring, squeue};

// One submitting/reaping thread and a plain ring (no SQPOLL). Only an explicit
// submit consumes SQEs. Waiting for a CQE must never submit a failed tail.
pub(super) trait Ring {
    fn space(&mut self) -> usize;
    fn queued(&mut self) -> usize;
    fn push(&mut self, entries: &[squeue::Entry]) -> io::Result<()>;
    fn submit(&mut self, count: u32) -> io::Result<usize>;
    fn reap(&mut self) -> io::Result<(u64, i32)>;
}

pub(super) fn create(count: u32) -> io::Result<Box<dyn Ring>> {
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

    fn reap(&mut self) -> io::Result<(u64, i32)> {
        loop {
            if let Some(cqe) = self.completion().next() {
                return Ok((cqe.user_data(), cqe.result()));
            }
            // SAFETY: wait for one outstanding CQE without submitting any SQEs.
            unsafe {
                self.submitter()
                    .enter::<libc::sigset_t>(0, 1, EnterFlags::GETEVENTS.bits(), None)
            }?;
        }
    }
}
