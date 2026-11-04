//! One caller-owned ring. A round moves data in only one phase at a time:
//! WRITE copies all its chunks before enter; READ waits for CQEs before HtoD.

use std::fs::File;

mod ranges;
use super::*;
use crate::cuda::StagingCopy;
use crate::runtime::proxy_manager::{ProxyManager, StageBufferLease};
use ranges::StagedRanges;

// Linux io_uring_setup caps the submission queue at IORING_MAX_ENTRIES.
const MAX_SQ_ENTRIES: usize = 32_768;

pub(super) struct Submission<'a> {
    pub requests: &'a [FileRequest],
    pub pool: &'a Arc<pool::Pool>,
    pub proxy: &'a Arc<ProxyManager>,
    pub operation: TransferOp,
    pub deadline: Instant,
    pub progress: Option<(&'a FileAttempts<'a>, usize)>,
}

enum Backing {
    Host(Task, libc::iovec),
    Device(StagedRanges),
}

struct Request {
    backing: Option<Backing>,
    bytes: usize,
    submitted: bool,
    uncertain: bool,
    error: Option<Error>,
}

enum DeviceStage {
    Slot(pool::Slot),
    Proxy(StageBufferLease),
}
impl DeviceStage {
    fn address(&self) -> Result<(std::ptr::NonNull<u8>, usize)> {
        match self {
            Self::Slot(s) => Ok((s.ptr, s.length)),
            Self::Proxy(s) => {
                let b = s.buffer()?;
                Ok((b.address, b.length))
            }
        }
    }
    fn copy(
        &mut self,
        owner: &mut StagingCopy,
        slot_offset: usize,
        offset: usize,
        length: usize,
        to_device: bool,
    ) -> Result<()> {
        match self {
            Self::Slot(s) => owner.copy_file_slot(s, slot_offset, offset, length, to_device),
            Self::Proxy(s) => {
                owner.begin_at(s, slot_offset, offset, length, to_device)?;
                owner.complete(s, length)
            }
        }
    }
}

struct Piece {
    request: usize,
    offset: usize,
    file_offset: u64,
    length: usize,
    // Host pieces borrow their fd and original buffer from the logical Task.
    stage: Option<(DeviceStage, Arc<File>)>,
    receipt: Option<(i32, bool)>,
}

struct Live {
    ring: Box<dyn Ring>,
    tasks: Vec<Request>,
    pieces: Vec<Piece>,
    // Addresses given to the kernel never alias the mutable receipt array.
    iovecs: Vec<libc::iovec>,
}

impl Submission<'_> {
    pub(super) fn execute(
        &self,
        transport: &IoUringTransport,
        max_batch_requests: usize,
        report: &mut BatchReport,
    ) -> Result<()> {
        let until = self
            .deadline
            .checked_add(DESTRUCTION_TIMEOUT)
            .ok_or(Error::InvalidConfig(
                "file drain deadline overflows monotonic clock",
            ))?;
        let mut tasks = Vec::with_capacity(self.requests.len());
        let mut piece_count = 0usize;
        for request in self.requests {
            let backing = if request
                .parts
                .iter()
                .any(|p| p.local.memory_type() == crate::MemoryType::Cuda)
            {
                let ranges = StagedRanges::new(request, self.proxy)?;
                piece_count = piece_count.saturating_add(ranges.pieces(self.proxy.chunk_size()));
                request.lease.io_file(request.offset, request.length)?;
                Backing::Device(ranges)
            } else {
                piece_count = piece_count.saturating_add(1);
                let (task, iov) = Task::new(self.operation, request, transport)?;
                Backing::Host(task, iov)
            };
            tasks.push(Request {
                backing: Some(backing),
                bytes: 0,
                submitted: false,
                uncertain: false,
                error: None,
            });
        }
        let width = piece_count.min(max_batch_requests).min(MAX_SQ_ENTRIES);
        let mut ring = (transport.create)(width as u32)?;
        if ring.space() < width {
            return Err(Error::ResourceExhausted);
        }
        let mut live = ManuallyDrop::new(Live {
            ring,
            tasks,
            pieces: Vec::with_capacity(width),
            iovecs: Vec::with_capacity(width),
        });
        let result = self.rounds(&mut live, width, until);
        report.locally_drained = !matches!(result, Err(Error::Unquiesced { .. }));
        self.finish(&mut live.tasks, result.err(), report);
        if report.locally_drained {
            drop(ManuallyDrop::into_inner(live));
        } else {
            // No destructor, pool return or retry can race outstanding native work.
            for piece in &mut live.pieces {
                if let Some((DeviceStage::Proxy(stage), _)) = &mut piece.stage {
                    stage.quarantine();
                }
            }
            for task in &live.tasks {
                if let Some(Backing::Device(copy)) = &task.backing {
                    copy.publish_failure();
                }
            }
        }
        Ok(())
    }

    fn rounds(&self, live: &mut Live, width: usize, until: Instant) -> Result<()> {
        while live.tasks.iter().any(|t| t.backing.is_some()) {
            remaining(self.deadline)?;
            self.plan(live, width)?;
            let entries = self.prepare(live)?;
            remaining(self.deadline)?;
            if live.ring.space() < entries.len() {
                return Err(Error::ResourceExhausted);
            }
            live.ring.push(&entries)?;
            for piece in &mut live.pieces {
                if let Some((DeviceStage::Proxy(stage), _)) = &mut piece.stage {
                    stage.submitting();
                }
            }
            let pieces = &mut live.pieces;
            let drain = submit_and_drain(
                &mut *live.ring,
                pieces.len(),
                self.deadline,
                until,
                |i, result| {
                    let piece = &mut pieces[i];
                    if piece.receipt.is_some() || result > 0 && result as usize > piece.length {
                        return false;
                    }
                    piece.receipt = Some((result, Instant::now() >= self.deadline));
                    true
                },
            );
            let mut fatal = drain.status.err().map(unquiesced);
            for piece in &pieces[..drain.consumed] {
                let task = &mut live.tasks[piece.request];
                task.submitted = true;
                task.uncertain |= piece.receipt.is_none();
            }
            // Receipts can arrive out of order. Count and copy only consecutive
            // chunks. A later WRITE receipt still contributes to the effect span.
            for (i, piece) in pieces.iter_mut().enumerate() {
                let task = &mut live.tasks[piece.request];
                if task.error.is_some() {
                    continue;
                }
                let result = match piece.receipt {
                    Some((n, late)) => self.apply(task, piece, n, late, &drain),
                    None => Err(drain.missing_error(i)),
                };
                if let Err(error) = result {
                    if matches!(error, Error::Unquiesced { .. }) {
                        task.uncertain = true;
                        fatal.get_or_insert_with(|| error.for_report());
                    }
                    task.error = Some(error);
                }
            }
            if let Some(error) = fatal {
                self.publish_finished(&mut live.tasks);
                return Err(error);
            }
            for piece in &mut live.pieces {
                if let Some((DeviceStage::Proxy(mut stage), _)) = piece.stage.take() {
                    stage.drained();
                    stage.release()?;
                }
            }
            live.pieces.clear();
            live.iovecs.clear();
            self.publish_finished(&mut live.tasks);
            if drain.consumed != entries.len() {
                // The unsent tail remains published in this ring. Never re-enter it.
                return Err(drain.missing_error(drain.consumed));
            }
        }
        Ok(())
    }

    fn plan(&self, live: &mut Live, width: usize) -> Result<()> {
        let mut leased = false;
        for (index, task) in live.tasks.iter().enumerate() {
            let Some(backing) = &task.backing else {
                continue;
            };
            let request = &self.requests[index];
            let mut offset = task.bytes;
            while offset < request.length && live.pieces.len() < width {
                let file_offset = request.offset + offset as u64;
                let (stage, iov) = match backing {
                    Backing::Host(_, iov) => (None, *iov),
                    Backing::Device(ranges) => {
                        let (copy, run_length) = ranges.lender(offset)?;
                        let slot = if offset == 0 {
                            self.pool.acquire(
                                request.length,
                                4096,
                                pool::Access::Device(copy.file_node()),
                                |size| copy.file_slab(size, self.proxy).map(pool::Slab::Pinned),
                            )?
                        } else {
                            None
                        };
                        let stage = if let Some(slot) = slot {
                            DeviceStage::Slot(slot)
                        } else {
                            match copy.pin(self.proxy, None) {
                                Ok(s) => DeviceStage::Proxy(s),
                                Err(Error::ResourceExhausted) if leased => return Ok(()),
                                Err(e) => return Err(e),
                            }
                        };
                        leased = true;
                        let (address, capacity) = stage.address()?;
                        let address = address.as_ptr();
                        if address as usize % 4096 != 0 {
                            return Err(Error::Unavailable(
                                "CUDA file staging requires a page-aligned chunk",
                            ));
                        }
                        let length = capacity.min(run_length);
                        let file =
                            request
                                .lease
                                .file_for_buffer(file_offset, length, address as usize)?;
                        (
                            Some((stage, file)),
                            libc::iovec {
                                iov_base: address.cast(),
                                iov_len: length,
                            },
                        )
                    }
                };
                live.pieces.push(Piece {
                    request: index,
                    offset,
                    file_offset,
                    length: iov.iov_len,
                    stage,
                    receipt: None,
                });
                live.iovecs.push(iov);
                offset += iov.iov_len;
            }
            if live.pieces.len() == width {
                break;
            }
        }
        Ok(())
    }

    fn prepare(&self, live: &mut Live) -> Result<Vec<squeue::Entry>> {
        let mut entries = Vec::with_capacity(live.pieces.len());
        for (i, (piece, iov)) in live.pieces.iter_mut().zip(&live.iovecs).enumerate() {
            let task = &mut live.tasks[piece.request];
            let entry = match (&mut task.backing, &mut piece.stage) {
                (Some(Backing::Host(host, _)), None) => host.entry(self.operation, iov, i),
                (Some(Backing::Device(copy)), Some((stage, file))) => {
                    if self.operation == TransferOp::Write {
                        remaining(self.deadline)?;
                        if let Err(e) =
                            copy.transfer(stage, piece.offset, piece.length, false, &mut task.bytes)
                        {
                            task.error = Some(e.for_report());
                            return Err(e);
                        }
                    }
                    task::entry(
                        file,
                        piece.file_offset,
                        self.operation,
                        std::slice::from_ref(iov),
                        i,
                    )
                }
                _ => return Err(Error::Internal("file chunk without its backing")),
            };
            entries.push(entry);
        }
        Ok(entries)
    }

    fn apply(
        &self,
        task: &mut Request,
        piece: &mut Piece,
        n: i32,
        late: bool,
        drain: &Drain,
    ) -> Result<()> {
        let bytes = n.max(0) as usize;
        match (&mut task.backing, &mut piece.stage) {
            (Some(Backing::Host(host, _)), None) => {
                if !host.complete(self.operation, n, late) {
                    return Err(unquiesced(-libc::EPROTO));
                }
            }
            (Some(Backing::Device(copy)), Some((stage, _))) => {
                if self.operation == TransferOp::Read && bytes != 0 {
                    if let Err(code) = drain.status {
                        // A missing SQE prevents starting the post-I/O CUDA phase.
                        return Err(unquiesced(code));
                    }
                    copy.transfer(stage, piece.offset, bytes, true, &mut task.bytes)?;
                }
            }
            _ => return Err(Error::Internal("file receipt without its backing")),
        }
        if self.operation == TransferOp::Write || piece.stage.is_none() {
            task.bytes += bytes;
        }
        match task::cqe_error(
            self.operation,
            n,
            piece.length,
            late || Instant::now() >= self.deadline,
        ) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn publish_finished(&self, tasks: &mut [Request]) {
        for (i, task) in tasks.iter_mut().enumerate() {
            if task.backing.is_some()
                && !task.uncertain
                && (task.error.is_some() || task.bytes == self.requests[i].length)
            {
                task.backing.take();
                self.publish(i, task, true);
            }
        }
    }

    fn publish(&self, index: usize, task: &Request, drained: bool) {
        let Some((p, first)) = self.progress else {
            return;
        };
        if task.submitted {
            p.posted(first + index);
        }
        if let Some(error) = &task.error {
            p.fail_file_task(
                first + index,
                task.bytes,
                error.for_report(),
                effect(self.operation, task.submitted),
                drained,
            );
        } else {
            p.complete_task(first + index, task.bytes);
        }
    }

    fn finish(&self, tasks: &mut [Request], stop: Option<Error>, report: &mut BatchReport) {
        let first = tasks
            .iter()
            .position(|t| t.backing.is_some() || t.error.is_some())
            .unwrap_or(tasks.len());
        report.completed_requests = first;
        report.transferred_bytes = tasks[..first].iter().map(|t| t.bytes).sum();
        if first == tasks.len() {
            return;
        }
        let span = &tasks[first..];
        let submitted = span.iter().rposition(|t| t.submitted);
        let error = if !report.locally_drained {
            stop.as_ref()
        } else {
            tasks[first].error.as_ref().or(stop.as_ref())
        };
        let error = error.map_or(Error::Internal("unfinished file task"), Error::for_report);
        report.failure = Some(TransferFailure {
            request_index: first,
            affected_requests: submitted.map_or(1, |i| i + 1),
            error,
            remote_effect: effect(self.operation, submitted.is_some()),
        });
        for (i, task) in tasks.iter_mut().enumerate() {
            if task.backing.is_none() {
                continue;
            }
            if task.error.is_none() && !task.submitted && i != first {
                continue;
            }
            if task.error.is_none() {
                task.error = stop.as_ref().map(Error::for_report);
            }
            if report.locally_drained {
                task.backing.take();
            }
            self.publish(i, task, report.locally_drained);
        }
    }
}

fn effect(operation: TransferOp, submitted: bool) -> RemoteEffect {
    if operation == TransferOp::Write && submitted {
        RemoteEffect::Unknown
    } else {
        RemoteEffect::None
    }
}
