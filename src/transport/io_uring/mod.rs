//! TENT's single-issuer file sub-batch. The caller or existing Batch worker owns the ring.
mod ring;
#[cfg(feature = "cuda")]
mod staging;
mod task;

#[cfg(test)]
#[path = "../../../tests/unit/transport/io_uring.rs"]
pub(crate) mod tests;

use std::io;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::time::Instant;

use io_uring::squeue;

use self::ring::Ring;
use self::task::Task;
use crate::batch::BatchProgress;
use crate::engine::remaining;
use crate::transport::DESTRUCTION_TIMEOUT;
use crate::{BatchReport, Error, RemoteEffect, Result, TransferFailure, TransferOp};

use crate::segment::file::FileRequest;

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
        #[cfg(feature = "cuda")] (proxy, max_batch_requests): (
            &Arc<crate::runtime::proxy_manager::ProxyManager>,
            usize,
        ),
    ) {
        if requests.is_empty() {
            return;
        }
        let count = progress.map_or(requests.len(), |(p, first)| {
            p.begin_file_group(first, requests.len())
        });
        let mut run = || -> Result<()> {
            remaining(deadline)?;
            #[cfg(feature = "cuda")]
            let staged = requests[..count]
                .iter()
                .any(|r| r.local.memory_type() == crate::MemoryType::Cuda);
            #[cfg(not(feature = "cuda"))]
            let staged = false;
            #[cfg(feature = "cuda")]
            if staged {
                staging::Submission {
                    requests: &requests[..count],
                    proxy,
                    operation,
                    deadline,
                    progress,
                }
                .execute(self, max_batch_requests, report)?;
            }
            if count != 0 && !staged {
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
            report.locally_drained = !matches!(error, Error::Unquiesced { .. });
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

struct Drain {
    // Only this prefix was consumed. If SQ accounting itself is invalid,
    // every published SQE must be treated as potentially in flight.
    consumed: usize,
    submit_error: i32,
    status: std::result::Result<(), i32>,
}

impl Drain {
    fn invalid_sq(count: usize) -> Self {
        Self {
            consumed: count,
            submit_error: -libc::EPROTO,
            status: Err(-libc::EPROTO),
        }
    }

    fn missing_error(&self, index: usize) -> Error {
        if index < self.consumed {
            if let Err(code) = self.status {
                return unquiesced(code);
            }
        }
        Error::Backend {
            operation: "io_uring submit",
            code: self.submit_error,
        }
    }

    fn effect(&self, op: TransferOp, index: usize) -> RemoteEffect {
        if op == TransferOp::Write && index < self.consumed {
            RemoteEffect::Unknown
        } else {
            RemoteEffect::None
        }
    }
}

fn unquiesced(code: i32) -> Error {
    Error::Unquiesced {
        operation: "io_uring drain",
        code,
    }
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
        let drain_deadline =
            deadline
                .checked_add(DESTRUCTION_TIMEOUT)
                .ok_or(Error::InvalidConfig(
                    "file drain deadline overflows monotonic clock",
                ))?;
        let mut live = ManuallyDrop::new(self);
        if let Err(error) = live.ring.push(entries) {
            // push_multiple is all-or-nothing and no enter has taken place.
            drop(ManuallyDrop::into_inner(live));
            return Err(error.into());
        }
        let live_batch = &mut *live;
        let tasks = &mut live_batch.tasks;
        let drain = submit_and_drain(
            &mut *live_batch.ring,
            tasks.len(),
            deadline,
            drain_deadline,
            |index, result| {
                let task = &mut tasks[index];
                if !task.complete(operation, result, Instant::now() >= deadline) {
                    return false;
                }
                if let Some((p, first)) = progress {
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
                true
            },
        );
        report.locally_drained = drain.status.is_ok();
        // CQEs already published independent task outcomes. Finish any tasks
        // without a receipt, including the tail a failed enter never consumed.
        if let Some((p, first)) = progress {
            for (index, _) in live
                .tasks
                .iter()
                .enumerate()
                .filter(|(_, t)| t.result.is_none())
            {
                p.fail_file_task(
                    first + index,
                    0,
                    drain.missing_error(index),
                    drain.effect(operation, index),
                    report.locally_drained || index >= drain.consumed,
                );
            }
        }
        // The group report, like a network report, counts only the successful prefix.
        for (index, task) in live.tasks.iter().enumerate() {
            let error = match task.result {
                None => Some(drain.missing_error(index)),
                Some(_) => task.observed_error(operation),
            };
            if let Some(reason) = error {
                report.failure = Some(TransferFailure {
                    request_index: index,
                    affected_requests: drain.consumed.saturating_sub(index).max(1),
                    error: drain.status.err().map_or(reason, unquiesced),
                    remote_effect: drain.effect(operation, index),
                });
                break;
            }
            report.completed_requests += 1;
            report.transferred_bytes += task.bytes();
        }
        if report.locally_drained {
            drop(ManuallyDrop::into_inner(live));
        }
        // Otherwise the ring, pointed-to iovecs, exclusive loans, fds and bounce buffers
        // remain live. The shared submission owner also retains admission and file leases.
        Ok(())
    }
}

fn submit_and_drain(
    ring: &mut dyn Ring,
    count: usize,
    deadline: Instant,
    drain_deadline: Instant,
    observe: impl FnMut(usize, i32) -> bool,
) -> Drain {
    let mut submitted = 0;
    let mut retries = 0;
    let mut code = -libc::EIO;
    while submitted < count {
        match ring.submit((count - submitted) as u32) {
            Ok(n) if n > 0 && n <= count - submitted => {
                submitted += n;
                retries = 0;
            }
            Ok(0) if retries < 3 => retries += 1,
            Ok(0) => break,
            Ok(_) => return Drain::invalid_sq(count),
            Err(e)
                if matches!(e.raw_os_error(), Some(libc::EINTR | libc::EAGAIN)) && retries < 3 =>
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
    let Some(consumed) = count.checked_sub(ring.queued()).filter(|n| *n >= submitted) else {
        return Drain::invalid_sq(count);
    };
    let status = drain(ring, consumed, drain_deadline, observe);
    Drain {
        consumed,
        submit_error: code,
        status,
    }
}

fn drain(
    ring: &mut dyn Ring,
    consumed: usize,
    drain_deadline: Instant,
    mut observe: impl FnMut(usize, i32) -> bool,
) -> std::result::Result<(), i32> {
    for _ in 0..consumed {
        let (id, result) = loop {
            if Instant::now() >= drain_deadline {
                return Err(-libc::ETIMEDOUT);
            }
            match ring.reap(drain_deadline) {
                Ok(cqe) => break cqe,
                // A wakeup or interrupted wait never starts a new window.
                Err(e)
                    if e.kind() == io::ErrorKind::Interrupted
                        || e.kind() == io::ErrorKind::TimedOut
                        || e.raw_os_error() == Some(libc::ETIME) =>
                {
                    continue;
                }
                Err(e) => return Err(-e.raw_os_error().unwrap_or(libc::EIO)),
            }
        };
        let Ok(index) = usize::try_from(id) else {
            return Err(-libc::EPROTO);
        };
        if index >= consumed || !observe(index, result) {
            return Err(-libc::EPROTO);
        }
    }
    Ok(())
}
