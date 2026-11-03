//! TENT's single-issuer file sub-batch. The caller or existing Batch worker owns the ring.
mod ring;
mod task;

#[cfg(test)]
#[path = "../../../tests/unit/transport/io_uring.rs"]
mod tests;

use std::io;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::time::Instant;

use io_uring::squeue;

use self::ring::Ring;
use self::task::Task;
use crate::batch::BatchProgress;
use crate::engine::remaining;
use crate::segment::file::FileLease;
use crate::{
    BatchReport, Error, RegisteredMemory, RemoteEffect, Result, TransferFailure, TransferOp,
};

pub(crate) struct FileRequest {
    pub(crate) local: RegisteredMemory,
    pub(crate) local_offset: usize,
    pub(crate) lease: FileLease,
    pub(crate) offset: u64,
    pub(crate) length: usize,
}

#[derive(Clone)]
pub(crate) struct IoUringTransport {
    allocate: fn(usize, usize) -> Result<task::Bounce>,
    create: Arc<dyn Fn(u32) -> io::Result<Box<dyn Ring>> + Send + Sync>,
}

impl IoUringTransport {
    pub(crate) fn probe() -> Option<Self> {
        drop(ring::create(2).ok()?);
        Some(Self {
            create: Arc::new(ring::create),
            allocate: task::Bounce::new,
        })
    }

    pub(crate) fn execute(
        &self,
        operation: TransferOp,
        requests: &[FileRequest],
        deadline: Instant,
        progress: Option<(&BatchProgress, usize)>,
        report: &mut BatchReport,
    ) {
        if requests.is_empty() {
            return;
        }
        let count = progress.map_or(requests.len(), |(p, first)| {
            p.begin_file_group(first, requests.len())
        });
        let mut run = || -> Result<()> {
            remaining(deadline)?;
            if count != 0 {
                let mut tasks = Vec::new();
                tasks
                    .try_reserve_exact(count)
                    .map_err(|_| Error::ResourceExhausted)?;
                let mut iovecs = Vec::new();
                iovecs
                    .try_reserve_exact(count)
                    .map_err(|_| Error::ResourceExhausted)?;
                for request in &requests[..count] {
                    let (task, iov) = Task::new(operation, request, self.allocate)?;
                    tasks.push(task);
                    iovecs.push(iov);
                }
                let size = u32::try_from(requests.len()).map_err(|_| Error::ResourceExhausted)?;
                let mut ring = (self.create)(size)?;
                if ring.space() < count {
                    return Err(Error::ResourceExhausted);
                }
                // The iovec array is immutable after this point, separate from
                // task state the worker mutates as receipts arrive.
                let entries: Vec<_> = tasks
                    .iter()
                    .zip(&iovecs)
                    .enumerate()
                    .map(|(index, (task, iov))| task.entry(operation, iov, index))
                    .collect();
                remaining(deadline)?;
                let batch = LiveBatch {
                    ring,
                    tasks,
                    _iovecs: iovecs,
                };
                batch.run(operation, &entries, deadline, progress, report)?;
            }
            if report.failure.is_none() && count != requests.len() {
                report.failure = Some(failure(count, Error::Cancelled, RemoteEffect::None));
            }
            Ok(())
        };
        if let Err(error) = run() {
            report.failure = Some(failure(0, error, RemoteEffect::None));
        }
    }
}

fn failure(index: usize, error: Error, effect: RemoteEffect) -> TransferFailure {
    TransferFailure {
        request_index: index,
        affected_requests: 1,
        error,
        remote_effect: effect,
    }
}

struct LiveBatch {
    ring: Box<dyn Ring>,
    tasks: Vec<Task>,
    _iovecs: Vec<libc::iovec>,
}

impl LiveBatch {
    fn run(
        self,
        operation: TransferOp,
        entries: &[squeue::Entry],
        deadline: Instant,
        progress: Option<(&BatchProgress, usize)>,
        report: &mut BatchReport,
    ) -> Result<()> {
        let mut live = ManuallyDrop::new(self);
        if let Err(error) = live.ring.push(entries) {
            // push_multiple is all-or-nothing and no enter has taken place.
            drop(ManuallyDrop::into_inner(live));
            return Err(error.into());
        }
        let (submit_error, drain) = live.submit_and_drain(operation, deadline, progress);
        report.locally_drained = drain.is_ok();
        for (index, task) in live.tasks.iter().enumerate() {
            let bytes = task.bytes();
            report.transferred_bytes += bytes;
            let error = || match task.result {
                None => match drain {
                    Err(code) => Some(Error::Unquiesced {
                        operation: "io_uring drain",
                        code,
                    }),
                    Ok(()) => Some(Error::Backend {
                        operation: "io_uring submit",
                        code: submit_error,
                    }),
                },
                Some(_) => task.observed_error(operation),
            };
            if let Some(reason) = error() {
                let uncertain = matches!(reason, Error::Unquiesced { .. });
                let effect =
                    if operation == TransferOp::Write && (task.result.is_some() || uncertain) {
                        RemoteEffect::Unknown
                    } else {
                        RemoteEffect::None
                    };
                if let Some((p, first)) = progress.filter(|_| task.result.is_none()) {
                    if let Some(reason) = error() {
                        p.fail_file_task(first + index, bytes, reason, effect, !uncertain);
                    }
                }
                if report.failure.is_none()
                    || uncertain
                        && report
                            .failure
                            .as_ref()
                            .is_some_and(|f| !matches!(f.error, Error::Unquiesced { .. }))
                {
                    report.failure = Some(failure(index, reason, effect));
                }
            } else {
                report.completed_requests += 1;
            }
        }
        if report.locally_drained {
            drop(ManuallyDrop::into_inner(live));
        }
        // Otherwise the ring, pointed-to iovecs, exclusive loans, fds and bounce buffers
        // remain live. The shared submission owner also retains admission and file leases.
        Ok(())
    }

    fn submit_and_drain(
        &mut self,
        operation: TransferOp,
        deadline: Instant,
        progress: Option<(&BatchProgress, usize)>,
    ) -> (i32, std::result::Result<(), i32>) {
        let count = self.tasks.len();
        let mut submitted = 0;
        let mut retries = 0;
        let mut code = -libc::EIO;
        while submitted < count {
            match self.ring.submit((count - submitted) as u32) {
                Ok(n) if n > 0 && n <= count - submitted => {
                    submitted += n;
                    retries = 0;
                }
                Ok(0) if retries < 3 => retries += 1,
                Ok(0) => break,
                Ok(_) => return (code, Err(-libc::EPROTO)),
                Err(e)
                    if matches!(e.raw_os_error(), Some(libc::EINTR | libc::EAGAIN))
                        && retries < 3 =>
                {
                    retries += 1;
                }
                Err(e) => {
                    code = -e.raw_os_error().unwrap_or(libc::EIO);
                    break;
                }
            }
            if submitted < count && Instant::now() >= deadline {
                code = -libc::ETIMEDOUT;
                break;
            }
        }
        // Even a failed enter must not erase a prefix already consumed from the SQ.
        let Some(consumed) = count.checked_sub(self.ring.queued()) else {
            return (code, Err(-libc::EPROTO));
        };
        if consumed < submitted {
            return (code, Err(-libc::EPROTO));
        }
        for _ in 0..consumed {
            let (id, result) = loop {
                match self.ring.reap() {
                    Ok(cqe) => break cqe,
                    Err(e)
                        if e.kind() == io::ErrorKind::Interrupted && Instant::now() < deadline =>
                    {
                        continue;
                    }
                    Err(e) => return (code, Err(-e.raw_os_error().unwrap_or(libc::EIO))),
                }
            };
            let Ok(index) = usize::try_from(id) else {
                return (code, Err(-libc::EPROTO));
            };
            if index >= consumed
                || !self.tasks[index].complete(operation, result, Instant::now() >= deadline)
            {
                return (code, Err(-libc::EPROTO));
            }
            if let Some((p, first)) = progress {
                let task = &self.tasks[index];
                if let Some(error) = task.observed_error(operation) {
                    let effect = if operation == TransferOp::Write {
                        RemoteEffect::Unknown
                    } else {
                        RemoteEffect::None
                    };
                    p.fail_file_task(first + index, task.bytes(), error, effect, true);
                } else {
                    p.complete_task(first + index, task.bytes());
                }
            }
        }
        (code, Ok(()))
    }
}
