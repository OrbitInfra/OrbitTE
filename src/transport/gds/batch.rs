use std::ops::Range;
use std::sync::atomic::Ordering;

use super::sys::*;
use super::*;
use crate::engine::file_attempt::FileAttempts;
use crate::segment::file::FileRequest;
use crate::{BatchReport, MemoryType, RemoteEffect, TransferFailure, TransferOp};

const SLICE: usize = 16 << 20;
const POLL_INTERVAL: Duration = Duration::from_millis(1);

enum Loan {
    Host { _lease: crate::memory::DmaLease },
    Device { _lease: crate::cuda::GdsLoan },
}
struct Task {
    range: Range<usize>,
    _loans: Vec<Loan>,
}
#[derive(Clone, Copy)]
struct Receipt {
    length: usize,
    status: u32,
    bytes: usize,
    late: bool,
}
impl Receipt {
    fn terminal(self) -> bool {
        matches!(
            self.status,
            INVALID | CANCELED | COMPLETE | TIMEOUT | FAILED
        )
    }
}

pub(crate) struct LiveBatch {
    driver: Arc<Driver>,
    pid: u32,
    context: Option<Arc<crate::cuda::CudaMemory>>,
    handle: Option<usize>,
    tasks: Vec<Task>,
    params: Vec<Param>,
    events: Vec<Event>,
    receipts: Vec<Receipt>,
    submitted: bool,
    active: Range<usize>,
    wave_size: usize,
    // (first entry, current context) for each vector context transition.
    contexts: Vec<(usize, usize, Arc<crate::cuda::CudaMemory>)>,
    canceled: bool,
    pub(super) permanent: bool,
    reclaim_failures: u32,
}

#[allow(unsafe_code)]
// SAFETY: transferring a quarantined batch moves no pointed-to storage. Its
// exclusively loaned host pointers and cuFile arrays outlive every poll.
// A single owner polls it under the quarantine lock in the retained context.
unsafe impl Send for LiveBatch {}

impl LiveBatch {
    fn prepare(gds: &GdsTransport, op: TransferOp, requests: &[FileRequest]) -> Result<Self> {
        let mode = gds.mode()?;
        let count = requests
            .iter()
            .flat_map(|r| r.parts.iter().map(|p| p.length.div_ceil(SLICE)))
            .try_fold(0usize, |n, k| n.checked_add(k))
            .ok_or(Error::ResourceExhausted)?;
        let vector = requests.len() == 1 && requests[0].parts.vectored();
        if !vector && count > gds.depth as usize {
            return Err(Error::ResourceExhausted);
        }
        let mut batch = Self {
            driver: gds.driver.clone(),
            pid: gds.driver.api.pid(),
            context: None,
            handle: None,
            tasks: Vec::with_capacity(requests.len()),
            params: Vec::with_capacity(count),
            events: vec![Event::default(); count.min(gds.depth as usize)],
            receipts: Vec::with_capacity(count),
            submitted: false,
            active: 0..0,
            contexts: Vec::new(),
            // A later direct READ must never write a destination past a short prefix.
            wave_size: if vector && op == TransferOp::Read {
                1
            } else {
                gds.depth as usize
            },
            canceled: false,
            permanent: false,
            reclaim_failures: 0,
        };
        for r in requests {
            let first = batch.params.len();
            let mut file_start = r.offset;
            let (loans, spans) = r.parts.borrow_ranges(|memory, offset, length| {
                if memory.memory_type() == MemoryType::Cuda {
                    let (gpu, offset) = memory.cuda_range(offset, length)?;
                    Ok(Loan::Device {
                        _lease: gpu.gds_loan(offset, length)?,
                    })
                } else {
                    Ok(Loan::Host {
                        _lease: memory.dma_lease(offset, length)?,
                    })
                }
            })?;
            let mut handle = None;
            for (part, span) in r.parts.iter().zip(spans) {
                let local = part.registered()?;
                let span = span.ok_or(Error::Internal("GDS range without a loan"))?;
                // cuFile documents a compatibility route for oversized batch slices
                // and for certain unaligned writes, even on a direct-capable mount.
                if gds.require_native
                    && (part.length.min(SLICE)
                        > gds.driver.max_native_slice.load(Ordering::Acquire)
                        || (local.base_address() + part.local_offset as u64) % 4096 != 0
                        || file_start % 4096 != 0
                        || part.length % 4096 != 0)
                {
                    return Err(Error::Unavailable(
                        "GDS native batch requires aligned IO within the cuFile per-buffer cache limit",
                    ));
                }
                let file = match handle {
                    Some(file) => file,
                    None => *handle.insert(r.lease.gds_handle(&gds.driver, mode)?),
                };
                let (base, buffer_offset) = if local.memory_type() == MemoryType::Cuda {
                    let (gpu, _) = local.cuda_range(part.local_offset, part.length)?;
                    if vector {
                        let crate::MemoryLocation::Cuda { context, .. } =
                            local.location()?.location
                        else {
                            return Err(Error::Internal("GDS device context missing"));
                        };
                        if batch
                            .contexts
                            .last()
                            .is_none_or(|(_, id, _)| *id != context)
                        {
                            let start = if batch.contexts.is_empty() {
                                0
                            } else {
                                batch.params.len()
                            };
                            batch.contexts.push((start, context, gpu.clone()));
                        }
                    }
                    batch.context.get_or_insert(gpu);
                    (local.base_address() as usize, part.local_offset)
                } else if let Loan::Host { _lease: loan } = &loans[span.loan] {
                    (
                        loan.address().as_ptr().wrapping_add(span.offset) as usize,
                        0,
                    )
                } else {
                    return Err(Error::Internal("cuFile host range lost its loan"));
                };
                for offset in (0..part.length).step_by(SLICE) {
                    let file_offset = libc::off_t::try_from(file_start + offset as u64)
                        .map_err(|_| Error::InvalidRange)?;
                    let buffer_offset = libc::off_t::try_from(buffer_offset + offset)
                        .map_err(|_| Error::InvalidRange)?;
                    let size = SLICE.min(part.length - offset);
                    batch.receipts.push(Receipt {
                        length: size,
                        status: WAITING,
                        bytes: 0,
                        late: false,
                    });
                    batch.params.push(Param {
                        mode: 1,
                        base,
                        file_offset,
                        buffer_offset,
                        size,
                        handle: file,
                        opcode: u32::from(op == TransferOp::Write),
                        cookie: batch.params.len() + 1,
                    });
                }
                file_start += part.length as u64;
            }
            batch.tasks.push(Task {
                range: first..batch.params.len(),
                _loans: loans,
            });
        }
        Ok(batch)
    }

    fn current<T>(
        context: &Option<Arc<crate::cuda::CudaMemory>>,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        match context {
            Some(gpu) => gpu.gds_current(f),
            None => f(),
        }
    }

    fn poll(&mut self, deadline: Option<Instant>) -> Result<bool> {
        let handle = self.handle.ok_or(Error::Internal("missing cuFile batch"))?;
        let count = Self::current(&self.context, || {
            self.driver.api.poll(handle, &mut self.events)
        })?;
        let events = self
            .events
            .get(..count)
            .ok_or(Error::Protocol("cuFile event count"))?;
        let late = deadline.is_some_and(|d| Instant::now() >= d);
        let mut advanced = false;
        for e in events {
            let i = e
                .cookie
                .checked_sub(1)
                .filter(|i| self.active.contains(i))
                .ok_or(Error::Protocol("cuFile event cookie"))?;
            if !matches!(
                e.status,
                WAITING | PENDING | INVALID | CANCELED | COMPLETE | TIMEOUT | FAILED
            ) || e.status == COMPLETE
                && e.bytes as isize >= 0
                && e.bytes > self.receipts[i].length
            {
                return Err(Error::Protocol("invalid cuFile completion"));
            }
            let prior = self.receipts[i];
            // cuFile defines the return count only for a completed IO.
            let bytes = if e.status == COMPLETE { e.bytes } else { 0 };
            if prior.terminal() {
                if matches!(e.status, WAITING | PENDING) {
                    continue;
                }
                if prior.status != e.status || prior.bytes != bytes {
                    return Err(Error::Protocol("conflicting cuFile completion"));
                }
                continue;
            }
            self.receipts[i] = Receipt {
                length: prior.length,
                status: e.status,
                bytes,
                late,
            };
            advanced |= self.receipts[i].terminal();
        }
        Ok(advanced)
    }

    fn cancel(&mut self) {
        if self.canceled {
            return;
        }
        self.canceled = true;
        if let Some(handle) = self.handle {
            // A cancel acknowledgement never marks a slice terminal.
            let result = Self::current(&self.context, || self.driver.api.cancel(handle));
            if matches!(result, Err(Error::Unquiesced { .. })) {
                self.permanent = true;
            }
        }
    }
    fn terminal(&self) -> bool {
        self.receipts[self.active.clone()]
            .iter()
            .all(|r| r.terminal())
    }

    fn destroy(&mut self) -> Result<()> {
        if self.permanent || !self.terminal() {
            return Err(Error::Unquiesced {
                operation: "cuFile batch still live",
                code: -1,
            });
        }
        if let Some(handle) = self.handle.take() {
            Self::current(&self.context, || self.driver.api.destroy(handle)).map_err(|e| {
                self.permanent = true;
                uncertain("cuFileBatchIODestroy", &e)
            })?;
        }
        Ok(())
    }
    // Called only while the quarantine lock serializes the original batch owner.
    pub(super) fn collect(&mut self) -> bool {
        self.permanent |= self.pid != self.driver.api.pid();
        if self.permanent {
            return false;
        }
        if !self.terminal() {
            if let Err(e) = self.poll(None) {
                self.reclaim_failures += 1;
                self.permanent |= self.reclaim_failures >= 4096
                    || matches!(e, Error::Protocol(_) | Error::Unquiesced { .. });
                return false;
            }
            self.reclaim_failures = 0;
        }
        self.terminal() && self.destroy().is_ok()
    }

    fn run(
        &mut self,
        gds: &GdsTransport,
        deadline: Instant,
        progress: Option<(&FileAttempts<'_>, usize)>,
    ) -> Result<()> {
        let until = deadline
            .checked_add(crate::transport::DESTRUCTION_TIMEOUT)
            .ok_or(Error::InvalidConfig("GDS destruction deadline overflow"))?;
        let mut start = 0;
        while start < self.params.len() {
            let mut end = (start + self.wave_size).min(self.params.len());
            let next = self
                .contexts
                .partition_point(|(first, _, _)| *first <= start);
            if next != 0 {
                self.context = Some(self.contexts[next - 1].2.clone());
                if let Some((boundary, _, _)) = self.contexts.get(next) {
                    end = end.min(*boundary);
                }
            }
            self.wave(gds, start..end, deadline, until, progress)?;
            if self.receipts[start..end]
                .iter()
                .any(|r| r.status != COMPLETE || r.bytes != r.length || r.late)
            {
                break;
            }
            start = end;
            if start < self.params.len()
                && (self.canceled || progress.is_some_and(|(p, base)| p.cancel_requested(base)))
            {
                return Err(Error::Cancelled);
            }
        }
        Ok(())
    }

    fn wave(
        &mut self,
        gds: &GdsTransport,
        range: Range<usize>,
        deadline: Instant,
        until: Instant,
        progress: Option<(&FileAttempts<'_>, usize)>,
    ) -> Result<()> {
        crate::engine::remaining(deadline)?;
        Self::current(&self.context, || {
            self.handle = Some(self.driver.api.setup(gds.depth)?);
            Ok(())
        })?;
        if let Err(error) = crate::engine::remaining(deadline) {
            self.destroy()?;
            return Err(error);
        }
        let handle = self.handle.ok_or(Error::Internal("missing cuFile batch"))?;
        // cuFile returns one status, not an accepted count. Include every cookie
        // until a terminal receipt proves what happened, even on submit error.
        let submit = Self::current(&self.context, || {
            self.active = range.clone();
            self.submitted = true;
            gds.submitted.fetch_add(1, Ordering::Relaxed);
            self.driver
                .api
                .submit(handle, &mut self.params[range.clone()])
        });
        if let Err(e) = submit {
            if matches!(e, Error::Unquiesced { .. }) {
                self.permanent = true;
                return Err(e);
            }
            if self.active != range {
                self.destroy()?;
                return Err(e);
            }
            self.cancel();
        }
        loop {
            if Instant::now() >= until {
                return Err(Error::Unquiesced {
                    operation: "cuFile terminal receipts",
                    code: -libc::ETIMEDOUT,
                });
            }
            let poll = self.poll(Some(deadline));
            if let Err(error @ (Error::Protocol(_) | Error::Unquiesced { .. })) = &poll {
                self.permanent = true;
                return Err(uncertain("cuFile completion accounting", error));
            }
            if self.terminal() {
                return self.destroy();
            }
            if let (Ok(true), Some((p, first))) = (&poll, progress) {
                for (i, task) in self.tasks.iter().enumerate() {
                    p.file_progress(first + i, self.prefix(task));
                }
            }
            if poll.is_err()
                || Instant::now() >= deadline
                || progress.is_some_and(|(p, first)| {
                    (0..self.tasks.len()).any(|i| p.cancel_requested(first + i))
                })
                || self
                    .receipts
                    .iter()
                    .any(|r| r.terminal() && (r.status != COMPLETE || r.bytes != r.length))
            {
                self.cancel();
            }
            if self.permanent {
                return Err(Error::Unquiesced {
                    operation: "cuFile cancel context",
                    code: -1,
                });
            }
            std::thread::sleep(POLL_INTERVAL.min(until.saturating_duration_since(Instant::now())));
        }
    }

    fn prefix(&self, task: &Task) -> usize {
        let mut bytes = 0;
        for r in &self.receipts[task.range.clone()] {
            if r.status != COMPLETE || (r.bytes as isize) < 0 {
                break;
            }
            bytes += r.bytes;
            if r.bytes < r.length || r.late {
                break;
            }
        }
        bytes
    }

    fn result(&self, task: &Task, op: TransferOp) -> (Option<Error>, bool) {
        let mut all_terminal = true;
        let mut failure = None;
        let mut rank = 0;
        for r in &self.receipts[task.range.clone()] {
            all_terminal &= r.terminal();
            let (priority, error) = match r.status {
                COMPLETE if r.bytes as isize >= 0 && r.bytes != r.length => (
                    4,
                    Error::Io(std::io::Error::new(
                        if op == TransferOp::Read {
                            std::io::ErrorKind::UnexpectedEof
                        } else {
                            std::io::ErrorKind::WriteZero
                        },
                        "short cuFile completion",
                    )),
                ),
                COMPLETE if (r.bytes as isize) < 0 => (
                    4,
                    Error::Backend {
                        operation: "cuFile IO",
                        code: r.bytes as i32,
                    },
                ),
                FAILED => (
                    4,
                    Error::Backend {
                        operation: "cuFile IO",
                        code: FAILED as i32,
                    },
                ),
                TIMEOUT => (3, Error::Timeout),
                CANCELED => (2, Error::Cancelled),
                INVALID => (
                    1,
                    Error::Backend {
                        operation: "invalid cuFile IO",
                        code: INVALID as i32,
                    },
                ),
                COMPLETE if r.late => (3, Error::Timeout),
                _ => continue,
            };
            if priority > rank {
                rank = priority;
                failure = Some(error);
            }
        }
        (failure, all_terminal)
    }

    fn report(
        &self,
        op: TransferOp,
        stop: Option<&Error>,
        deregister_failure: Option<usize>,
        progress: Option<(&FileAttempts<'_>, usize)>,
        report: &mut BatchReport,
    ) {
        if !self.submitted {
            if let Some(stop) = stop {
                report.failure = Some(TransferFailure {
                    request_index: 0,
                    affected_requests: 1,
                    error: stop.for_report(),
                    remote_effect: RemoteEffect::None,
                });
            }
            return;
        }
        let effect = if self.submitted && op == TransferOp::Write {
            RemoteEffect::Unknown
        } else {
            RemoteEffect::None
        };
        let first_bad = self.tasks.iter().enumerate().position(|(i, t)| {
            let (e, terminal) = self.result(t, op);
            e.is_some() || !terminal || Some(i) == deregister_failure
        });
        // A failed physical teardown is a group failure even if all bytes arrived.
        let first_bad = first_bad.or(stop.map(|_| 0));
        for (i, t) in self.tasks.iter().enumerate() {
            let bytes = self.prefix(t);
            let (mut error, terminal) = self.result(t, op);
            if !terminal
                || Some(i) == deregister_failure
                || (error.is_none() && Some(i) == first_bad)
            {
                error = stop.map(Error::for_report).or(error);
            }
            if Some(i) == first_bad {
                let reason = if !report.locally_drained {
                    stop
                } else {
                    error.as_ref()
                };
                report.failure = Some(TransferFailure {
                    request_index: i,
                    affected_requests: if self.submitted {
                        self.tasks.len() - i
                    } else {
                        1
                    },
                    error: reason
                        .map_or(Error::Internal("missing cuFile status"), Error::for_report),
                    remote_effect: effect,
                });
            }
            if first_bad.is_none_or(|first| i < first) {
                report.completed_requests += 1;
                report.transferred_bytes += bytes;
            }
            if let Some((p, first)) = progress {
                p.posted(first + i);
                if let Some(e) = error {
                    p.fail_file_task(first + i, bytes, e, effect, report.locally_drained);
                } else if terminal {
                    p.complete_file_task(first + i, bytes, report.locally_drained);
                }
            }
        }
    }
}

impl GdsTransport {
    pub(crate) fn execute(
        &self,
        op: TransferOp,
        requests: &[FileRequest],
        deadline: Instant,
        progress: Option<(&FileAttempts<'_>, usize)>,
        report: &mut BatchReport,
        pending: &Mutex<Option<Quarantine>>,
    ) {
        let count = progress.map_or(requests.len(), |(p, first)| {
            p.begin_file_group(first, requests.len())
        });
        let mut run = || -> Result<()> {
            if count != 0 {
                crate::engine::remaining(deadline)?;
                // Install the physical owner before any cuFile batch call. Even
                // an interrupted worker cannot drop its arrays, loans or permits.
                let live = LiveBatch::prepare(self, op, &requests[..count])?;
                let mut owner = pending.lock().unwrap_or_else(|p| p.into_inner());
                *owner = Some(Quarantine::Batch(Box::new(live)));
                let Some(Quarantine::Batch(batch)) = owner.as_mut() else {
                    return Err(Error::Internal("missing GDS owner"));
                };
                let mut stop = batch.run(self, deadline, progress).err();
                let mut deregister_failure = None;
                if !matches!(stop, Some(Error::Unquiesced { .. })) {
                    for (i, r) in requests[..count].iter().enumerate() {
                        if let Err(e) = r.parts.release_buffers() {
                            batch.permanent = true;
                            stop = Some(e);
                            deregister_failure = Some(i);
                            break;
                        }
                    }
                }
                report.locally_drained = !matches!(stop, Some(Error::Unquiesced { .. }));
                batch.report(op, stop.as_ref(), deregister_failure, progress, report);
                if report.locally_drained {
                    owner.take();
                } else if !batch.submitted {
                    batch.permanent = true;
                }
            }
            if count < requests.len() && report.failure.is_none() {
                report.failure = Some(TransferFailure {
                    request_index: count,
                    affected_requests: 1,
                    error: Error::Cancelled,
                    remote_effect: RemoteEffect::None,
                });
            }
            Ok(())
        };
        if let Err(e) = run() {
            report.locally_drained = !matches!(e, Error::Unquiesced { .. });
            report.failure = Some(TransferFailure {
                request_index: 0,
                affected_requests: 1,
                error: e,
                remote_effect: RemoteEffect::None,
            });
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/gds/reclaim.rs"]
mod reclaim_tests;
