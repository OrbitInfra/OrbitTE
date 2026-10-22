use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::{Buffer, Descriptor, Device, Phase, RdmaConfig, RdmaStats, Resources, progress, sys};
use crate::engine::remaining;
use crate::{Error, Result, TransferOp};

#[derive(Clone, Copy)]
enum Fault {
    Backend(&'static str, i32),
    Unquiesced(&'static str, i32),
    Protocol(&'static str),
    Internal(&'static str),
    Unavailable(&'static str),
    Timeout,
    Exhausted,
}

impl Fault {
    fn from_error(error: Error) -> Self {
        match error {
            Error::Backend { operation, code } => Self::Backend(operation, code),
            Error::Unquiesced { operation, code } => Self::Unquiesced(operation, code),
            Error::Protocol(message) => Self::Protocol(message),
            Error::Internal(message) => Self::Internal(message),
            Error::Unavailable(message) => Self::Unavailable(message),
            Error::Timeout => Self::Timeout,
            Error::ResourceExhausted => Self::Exhausted,
            _ => Self::Internal("unexpected RDMA progress failure"),
        }
    }
    fn error(self) -> Error {
        match self {
            Self::Backend(operation, code) => Error::Backend { operation, code },
            Self::Unquiesced(operation, code) => Error::Unquiesced { operation, code },
            Self::Protocol(message) => Error::Protocol(message),
            Self::Internal(message) => Error::Internal(message),
            Self::Unavailable(message) => Error::Unavailable(message),
            Self::Timeout => Error::Timeout,
            Self::Exhausted => Error::ResourceExhausted,
        }
    }
}

const WORK: u8 = 1;
const CLOSE: u8 = 2;

pub(super) struct Record {
    actions: AtomicU8,
    tag: u64,
    state: Mutex<Request>,
    changed: Condvar,
}

pub(super) struct Request {
    pub(super) resources: Option<Box<Resources>>,
    pub(super) phase: Phase,
    pub(super) qpn: u32,
    work: Option<Work>,
    close_requested: bool,
    failure: Option<Fault>,
}

struct Work {
    queue: progress::SendQueue,
    operation: progress::Operation,
    remote: Descriptor,
    deadline: Instant,
}

impl Request {
    pub(super) fn check(&self) -> Result<()> {
        match self.failure {
            Some(fault) => Err(fault.error()),
            None => Ok(()),
        }
    }
}

impl Record {
    pub(super) fn lock(&self) -> Result<MutexGuard<'_, Request>> {
        self.state.lock().map_err(|_| Error::Unquiesced {
            operation: "RDMA request lock poisoned",
            code: libc::EIO,
        })
    }
}

pub(super) struct Progress {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    control: Mutex<Control>,
    wake: Condvar,
    usage: Arc<Mutex<RdmaStats>>,
    verbs: &'static sys::Verbs,
    config: RdmaConfig,
}

struct Control {
    cq: Option<Box<CompletionQueue>>,
    records: Vec<Arc<Record>>,
    next_tag: u64,
    dirty: bool,
    stopping: bool,
    finished: bool,
    fault: Option<Fault>,
    retained: Option<(&'static str, i32)>,
}

struct CompletionQueue {
    handles: sys::Handles,
    _device: Arc<Device>,
    usage: Arc<Mutex<RdmaStats>>,
}

// The single progress thread polls/closes this CQ. Construction precedes spawn.
unsafe impl Send for CompletionQueue {}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active_cqs -= 1;
    }
}

struct WorkerExit(Arc<Shared>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        let shared = &self.0;
        if !shared
            .control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .finished
        {
            shared.retain("RDMA progress worker stopped", libc::EIO);
        }
        shared
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .progress_workers -= 1;
    }
}

impl Progress {
    pub(super) fn new(
        config: &RdmaConfig,
        device: Arc<Device>,
        usage: Arc<Mutex<RdmaStats>>,
        verbs: &'static sys::Verbs,
    ) -> Result<Arc<Self>> {
        let mut records = Vec::new();
        records
            .try_reserve_exact(config.max_endpoints)
            .map_err(|_| Error::ResourceExhausted)?;
        usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .active_cqs += 1;
        let mut cq = Box::new(CompletionQueue {
            handles: sys::Handles::default(),
            _device: Arc::clone(&device),
            usage: Arc::clone(&usage),
        });
        if let Err(error) = device.create_cq(&mut cq.handles, config.max_cqes as u32) {
            let code = unsafe { (verbs.destroy_cq)(&mut cq.handles) };
            if code != 0 {
                usage.lock().unwrap_or_else(|p| p.into_inner()).retained_cqs += 1;
                std::mem::forget(cq);
                return Err(Error::Unquiesced {
                    operation: "ibv_destroy_cq",
                    code,
                });
            }
            return Err(error);
        }
        usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .opened_cqs += 1;
        let shared = Arc::new(Shared {
            control: Mutex::new(Control {
                cq: Some(cq),
                records,
                next_tag: 0,
                dirty: false,
                stopping: false,
                finished: false,
                fault: None,
                retained: None,
            }),
            wake: Condvar::new(),
            usage,
            verbs,
            config: config.clone(),
        });
        let owner = Arc::new(Self {
            shared: Arc::clone(&shared),
            worker: Mutex::default(),
        });
        shared
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .progress_workers += 1;
        let worker = thread::Builder::new()
            .name("orbitte-rdma".into())
            .spawn(move || {
                let _exit = WorkerExit(Arc::clone(&shared));
                shared.run();
            });
        match worker {
            Ok(worker) => {
                *owner
                    .worker
                    .lock()
                    .map_err(|_| Error::Internal("RDMA worker lock poisoned"))? = Some(worker)
            }
            Err(error) => {
                owner
                    .shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .progress_workers -= 1;
                if let Err((operation, code)) = owner.shared.close_cq() {
                    owner.shared.retain(operation, code);
                    return Err(Error::Unquiesced { operation, code });
                }
                owner
                    .shared
                    .control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .finished = true;
                return Err(Error::Io(error));
            }
        }
        Ok(owner)
    }

    pub(super) fn handle(&self) -> Result<*mut std::ffi::c_void> {
        let control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        if let Some(fault) = control.fault {
            return Err(fault.error());
        }
        control
            .cq
            .as_ref()
            .map(|cq| cq.handles.cq)
            .ok_or(Error::Unavailable("RDMA CQ is closed"))
    }

    pub(super) fn register(&self, resources: Box<Resources>) -> Result<Arc<Record>> {
        let mut control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        if let Some(fault) = control.fault {
            return Err(fault.error());
        }
        if control.stopping || control.finished {
            return Err(Error::Unavailable("RDMA progress is closed"));
        }
        if control.records.len() >= self.shared.config.max_endpoints {
            return Err(Error::ResourceExhausted);
        }
        let tag = control.next_tag;
        let next = tag
            .checked_add(1)
            .filter(|next| next.checked_mul(super::WR_ID_STRIDE).is_some())
            .ok_or(Error::ResourceExhausted)?;
        control.next_tag = next;
        let record = Arc::new(Record {
            actions: AtomicU8::new(0),
            tag,
            state: Mutex::new(Request {
                resources: Some(resources),
                phase: Phase::Init,
                qpn: 0,
                work: None,
                close_requested: false,
                failure: None,
            }),
            changed: Condvar::new(),
        });
        control.records.push(Arc::clone(&record));
        control.dirty = true;
        self.shared.wake.notify_one();
        Ok(record)
    }

    pub(super) fn transfer(
        &self,
        record: &Arc<Record>,
        remote: Descriptor,
        operation: TransferOp,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        {
            let mut request = record.lock()?;
            request.check()?;
            if request.phase != Phase::Connected {
                return Err(Error::Internal("RDMA QP is not ready"));
            }
            let resources = request
                .resources
                .as_ref()
                .ok_or(Error::Internal("missing RDMA endpoint"))?;
            if remote
                .address
                .checked_add(resources.buffer.len() as u64)
                .is_none()
            {
                return Err(Error::InvalidRange);
            }
            let cap = if operation == TransferOp::Write {
                32
            } else {
                resources.max_read_slices
            };
            let queue = progress::SendQueue::new(
                resources.buffer.len(),
                self.shared.config.block_size,
                cap,
                Arc::clone(&self.shared.usage),
                record.tag * super::WR_ID_STRIDE + 1,
            )?;
            request.work = Some(Work {
                queue,
                operation: operation.into(),
                remote,
                deadline,
            });
            request.phase = Phase::Queued;
        }
        self.wait(record, deadline)
    }

    pub(super) fn window(
        &self,
        record: &Arc<Record>,
        operation: progress::Operation,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        {
            let mut request = record.lock()?;
            request.check()?;
            if request.phase != Phase::Connected {
                return Err(Error::Internal("MW QP is not ready"));
            }
            let resources = request
                .resources
                .as_mut()
                .ok_or(Error::Internal("missing MW owner"))?;
            if resources.handles.mw.is_null() {
                return Err(Error::Unavailable("source MW is unavailable"));
            }
            let slot = match operation {
                progress::Operation::Bind if !resources.window_used => {
                    resources.window_used = true;
                    65
                }
                progress::Operation::Invalidate if resources.window_bound => 66,
                _ => return Err(Error::Internal("MW authorization state")),
            };
            let remote = Descriptor {
                address: resources.buffer.address().as_ptr() as u64,
                rkey: resources.handles.window_key,
                ..Default::default()
            };
            request.work = Some(Work {
                queue: progress::SendQueue::control(
                    Arc::clone(&self.shared.usage),
                    record.tag * super::WR_ID_STRIDE + slot,
                )?,
                operation,
                remote,
                deadline,
            });
            request.phase = Phase::Queued;
        }
        self.wait(record, deadline)
    }

    fn wait(&self, record: &Arc<Record>, deadline: Instant) -> Result<()> {
        record.actions.fetch_or(WORK, Ordering::Release);
        self.shared.signal();
        let mut request = record.lock()?;
        loop {
            request.check()?;
            if request
                .work
                .as_ref()
                .is_some_and(|work| work.queue.finished())
            {
                return Ok(());
            }
            let duration = match remaining(deadline) {
                Ok(duration) => duration,
                Err(error) => {
                    let pending = request
                        .work
                        .as_ref()
                        .is_some_and(|work| work.queue.pending());
                    request.failure = Some(Fault::Timeout);
                    drop(request);
                    if pending {
                        self.shared.fail(Fault::Timeout);
                    } else {
                        self.shared.signal();
                    }
                    return Err(error);
                }
            };
            request = record
                .changed
                .wait_timeout(request, duration)
                .map_err(|_| Error::Unquiesced {
                    operation: "RDMA progress wait poisoned",
                    code: libc::EIO,
                })?
                .0;
        }
    }

    pub(super) fn close_record(&self, record: &Arc<Record>, deadline: Instant) -> Result<()> {
        let mut request = record.lock()?;
        if !matches!(request.phase, Phase::Closed | Phase::Retained) {
            request.close_requested = true;
            drop(request);
            record.actions.fetch_or(CLOSE, Ordering::Release);
            self.shared.signal();
            request = record.lock()?;
            while !matches!(request.phase, Phase::Closed | Phase::Retained) {
                let duration = remaining(deadline).map_err(|_| Error::Unquiesced {
                    operation: "RDMA close deadline before quiescence",
                    code: libc::ETIMEDOUT,
                })?;
                request = record
                    .changed
                    .wait_timeout(request, duration)
                    .map_err(|_| Error::Unquiesced {
                        operation: "RDMA close wait poisoned",
                        code: libc::EIO,
                    })?
                    .0;
            }
        }
        if request.phase == Phase::Retained {
            drop(request);
            return match self.close(deadline) {
                Err(error @ Error::Unquiesced { .. }) => Err(error),
                Err(Error::Timeout) => Err(Error::Unquiesced {
                    operation: "RDMA retention receipt deadline",
                    code: libc::ETIMEDOUT,
                }),
                _ => Err(Error::Unquiesced {
                    operation: "RDMA retained progress lane",
                    code: libc::EIO,
                }),
            };
        }
        request.check()
    }

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        let mut control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        control.stopping = true;
        control.dirty = true;
        self.shared.wake.notify_one();
        while !control.finished {
            control = self
                .shared
                .wake
                .wait_timeout(control, remaining(deadline)?)
                .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?
                .0;
        }
        let retained = control.retained;
        drop(control);
        if let Some(worker) = self
            .worker
            .lock()
            .map_err(|_| Error::Internal("RDMA worker lock poisoned"))?
            .take()
        {
            worker.join().map_err(|_| Error::Unquiesced {
                operation: "RDMA progress worker",
                code: libc::EIO,
            })?;
        }
        match retained {
            Some((operation, code)) => Err(Error::Unquiesced { operation, code }),
            None => Ok(()),
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        // Last public/native request owner is gone. Native calls remain non-preemptible.
        let deadline = Instant::now() + Duration::from_secs(300);
        let _ = self.close(deadline);
    }
}

impl Shared {
    fn signal(&self) {
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        control.dirty = true;
        self.wake.notify_one();
    }

    fn fail(&self, fault: Fault) {
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        if control.fault.is_none() {
            control.fault = Some(fault);
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .progress_failures += 1;
        }
        control.dirty = true;
        self.wake.notify_all();
    }

    fn close_cq(&self) -> std::result::Result<(), (&'static str, i32)> {
        let mut cq = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            control.cq.take()
        };
        if let Some(owner) = &mut cq {
            let code = unsafe { (self.verbs.destroy_cq)(&mut owner.handles) };
            if code != 0 {
                self.control.lock().unwrap_or_else(|p| p.into_inner()).cq = cq.take();
                return Err(("ibv_destroy_cq", code));
            }
        }
        Ok(())
    }

    fn retain(&self, operation: &'static str, code: i32) {
        let records = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            if control.finished {
                return;
            }
            if control.fault.is_none() {
                self.usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .progress_failures += 1;
            }
            control.fault = Some(Fault::Unquiesced(operation, code));
            std::mem::take(&mut control.records)
        };
        let mut count = 0;
        let mut bytes = 0;
        let mut debt = 0;
        let mut windows = 0;
        for record in &records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if request.phase != Phase::Retained {
                count += 1;
                bytes += request.resources.as_ref().map_or(0, |r| r.buffer.len());
                debt += request.work.as_ref().map_or(0, |w| w.queue.outstanding());
                windows += usize::from(
                    request
                        .resources
                        .as_ref()
                        .is_some_and(|r| !r.handles.mw.is_null()),
                );
                request.phase = Phase::Retained;
                request.failure = Some(Fault::Unquiesced(operation, code));
            }
            record.changed.notify_all();
        }
        let cq = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            control.retained = Some((operation, code));
            let cq = control.cq.take();
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.retained_endpoints += count;
            usage.retained_bytes += bytes;
            usage.retained_work_requests += debt;
            usage.retained_cqs += usize::from(cq.is_some());
            usage.retained_windows += windows;
            control.finished = true;
            cq
        };
        // Fence every constructor before publishing completion or losing owners.
        // Actual CQ/PD, QPs/MRs, backing, DMA loans and unresolved debt survive.
        std::mem::forget((cq, records));
        self.wake.notify_all();
    }

    fn close_mr(&self, resources: &mut Resources) -> std::result::Result<(), (&'static str, i32)> {
        if !resources.handles.mw.is_null() {
            let code = unsafe { (self.verbs.dealloc_window)(&mut resources.handles) };
            if code != 0 {
                return Err(("ibv_dealloc_mw", code));
            }
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .active_windows -= 1;
        }
        if let Some(cached) = &mut resources.cached_mr {
            cached.release().map_err(|error| match error {
                Error::Unquiesced { operation, code } => (operation, code),
                _ => ("RDMA MR borrow release", libc::EIO),
            })?;
            resources.handles.mr = std::ptr::null_mut();
        } else {
            let code = unsafe { (self.verbs.deregister)(&mut resources.handles) };
            if code != 0 {
                return Err(("ibv_dereg_mr", code));
            }
        }
        resources.handles.cq = std::ptr::null_mut();
        Ok(())
    }

    fn unlock(request: &mut Request) {
        if let Some(resources) = &mut request.resources {
            if let Buffer::Direct { loan, .. } = &mut resources.buffer {
                drop(loan.take());
            }
        }
        request.phase = Phase::Closed;
    }

    fn close_request(
        &self,
        record: &Arc<Record>,
    ) -> std::result::Result<bool, (&'static str, i32)> {
        if record.actions.load(Ordering::Acquire) & CLOSE == 0 {
            return Ok(false);
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if !request.close_requested || matches!(request.phase, Phase::Closed | Phase::Retained) {
            return Ok(false);
        }
        if request
            .work
            .as_ref()
            .is_some_and(|work| work.queue.pending())
        {
            drop(request);
            self.fail(Fault::Internal("RDMA close with unresolved work"));
            return Ok(false);
        }
        let resources = request
            .resources
            .as_mut()
            .ok_or(("RDMA resources missing", libc::EIO))?;
        let code = unsafe { (self.verbs.destroy_qp)(&mut resources.handles) };
        if code != 0 {
            return Err(("ibv_destroy_qp", code));
        }
        self.close_mr(resources)?;
        self.control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .records
            .retain(|entry| !Arc::ptr_eq(entry, record));
        Self::unlock(&mut request);
        drop(request);
        record.changed.notify_all();
        Ok(true)
    }

    fn abort(&self, records: &[Arc<Record>], fault: Fault) {
        // Fence accepted constructors under the same lock as native QP creation.
        // No other thread polls or submits native WRs.
        for record in records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            request.failure.get_or_insert(fault);
            let Some(resources) = &mut request.resources else {
                continue;
            };
            let code = unsafe { (self.verbs.destroy_qp)(&mut resources.handles) };
            if code != 0 {
                drop(request);
                self.retain("ibv_destroy_qp", code);
                return;
            }
        }
        if let Err((operation, code)) = self.close_cq() {
            self.retain(operation, code);
            return;
        }
        // Shared CQ destruction, after all QPs, is the cancellation receipt.
        for record in records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(work) = &mut request.work {
                work.queue.quiesce();
            }
            if let Some(resources) = &mut request.resources {
                if let Err((operation, code)) = self.close_mr(resources) {
                    drop(request);
                    self.retain(operation, code);
                    return;
                }
            }
        }
        for record in records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            request.failure.get_or_insert(fault);
            Self::unlock(&mut request);
            record.changed.notify_all();
        }
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        control.records.clear();
        control.finished = true;
        self.wake.notify_all();
    }

    fn post(&self, record: &Arc<Record>) -> bool {
        if record.actions.load(Ordering::Acquire) & WORK == 0 {
            return false;
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if request.failure.is_some() || request.close_requested {
            return false;
        }
        let Request {
            resources,
            work,
            phase,
            ..
        } = &mut *request;
        let Some(work) = work else {
            return false;
        };
        if Instant::now() >= work.deadline {
            if work.queue.pending() {
                drop(request);
                self.fail(Fault::Timeout);
            } else {
                request.failure = Some(Fault::Timeout);
                record.changed.notify_all();
            }
            return false;
        }
        if work.queue.pending() || work.queue.finished() {
            return false;
        }
        let Some(resources) = resources else {
            return false;
        };
        let requests = match work
            .queue
            .reserve(self.config.max_send_wr, self.config.max_cqes)
        {
            Ok(requests) => requests,
            Err(Error::ResourceExhausted) => return false,
            Err(error) => {
                request.failure = Some(Fault::from_error(error));
                record.changed.notify_all();
                return false;
            }
        };
        let mut posted = sys::PostResult::default();
        *phase = Phase::Posted;
        let code = unsafe {
            match work.operation {
                progress::Operation::Read | progress::Operation::Write => (self.verbs.post)(
                    &mut resources.handles,
                    requests.as_ptr(),
                    requests.len(),
                    resources.buffer.address().as_ptr() as u64,
                    resources.buffer.len(),
                    work.remote.address,
                    work.remote.rkey,
                    i32::from(work.operation == progress::Operation::Write),
                    &mut posted,
                ),
                progress::Operation::Bind | progress::Operation::Invalidate => (self.verbs.window)(
                    &mut resources.handles,
                    requests[0].id,
                    resources.buffer.address().as_ptr() as u64,
                    resources.buffer.len(),
                    resources.permission,
                    work.operation == progress::Operation::Bind,
                    &mut posted,
                ),
            }
        };
        if let Err(error) = work.queue.posted(code, posted) {
            let fault = Fault::from_error(error);
            let pending = work.queue.pending();
            request.failure = Some(fault);
            drop(request);
            if pending {
                self.fail(fault);
            } else {
                record.actions.fetch_and(!WORK, Ordering::Release);
            }
            record.changed.notify_all();
        }
        true
    }

    fn complete(&self, completion: sys::Completion) {
        let Some(tag) = completion
            .id
            .checked_sub(1)
            .map(|id| id / super::WR_ID_STRIDE)
        else {
            self.fail(Fault::Protocol("RDMA completion identity"));
            return;
        };
        let record = self
            .control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .records
            .iter()
            .find(|entry| entry.tag == tag)
            .cloned();
        let Some(record) = record else {
            self.fail(Fault::Protocol("RDMA unknown completion owner"));
            return;
        };
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        let result = if completion.qpn != request.qpn {
            Err(Error::Protocol("RDMA completion QP identity"))
        } else if let Some(work) = &mut request.work {
            work.queue.complete(completion, work.operation)
        } else {
            Err(Error::Protocol("RDMA completion before submission"))
        };
        if result.is_ok() {
            let operation = request.work.as_ref().map(|w| w.operation);
            if matches!(
                operation,
                Some(progress::Operation::Bind | progress::Operation::Invalidate)
            ) {
                if let Some(resources) = &mut request.resources {
                    resources.window_bound = operation == Some(progress::Operation::Bind);
                }
                request.phase = Phase::Connected;
            }
        }
        if let Err(error) = result {
            let fault = Fault::from_error(error);
            request.failure = Some(fault);
            drop(request);
            self.fail(fault);
        } else {
            if request
                .work
                .as_ref()
                .is_some_and(|work| work.queue.finished())
            {
                record.actions.fetch_and(!WORK, Ordering::Release);
            }
            drop(request);
        }
        // The handler owns a strong record until all of its accesses have ended.
        record.changed.notify_all();
    }

    fn run(&self) {
        let mut records = Vec::with_capacity(self.config.max_endpoints);
        let mut cursor = 0usize;
        loop {
            let (fault, stopping, cq) = {
                let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
                records.clear();
                records.extend(control.records.iter().cloned());
                control.dirty = false;
                (
                    control.fault,
                    control.stopping,
                    control.cq.as_ref().map(|cq| cq.handles.cq),
                )
            };
            if let Some(fault) = fault {
                self.abort(&records, fault);
                return;
            }
            if stopping {
                if !records.is_empty() {
                    self.fail(Fault::Unavailable("RDMA progress stopped"));
                    continue;
                }
                if let Err((operation, code)) = self.close_cq() {
                    self.retain(operation, code);
                    return;
                }
                self.control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .finished = true;
                self.wake.notify_all();
                return;
            }
            let mut index = 0;
            while index < records.len() {
                match self.close_request(&records[index]) {
                    Ok(true) => {
                        records.swap_remove(index);
                    }
                    Ok(false) => index += 1,
                    Err((operation, code)) => {
                        self.retain(operation, code);
                        return;
                    }
                }
            }
            for index in 0..records.len() {
                if self
                    .control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .fault
                    .is_some()
                {
                    break;
                }
                self.post(&records[(index + cursor) % records.len()]);
            }
            cursor = cursor.wrapping_add(1);
            let pending_qps = records
                .iter()
                .filter(|record| {
                    record.actions.load(Ordering::Acquire) & WORK != 0
                        && record
                            .state
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .work
                            .as_ref()
                            .is_some_and(|work| work.queue.pending())
                })
                .count();
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.peak_pending_qps = usage.peak_pending_qps.max(pending_qps);
            let pending = usage.work_requests != 0;
            drop(usage);
            if pending
                && self
                    .control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .fault
                    .is_none()
            {
                let mut handles = sys::Handles {
                    cq: cq.unwrap_or(std::ptr::null_mut()),
                    ..Default::default()
                };
                for _ in 0..64 {
                    let mut completion = sys::Completion::default();
                    let count = unsafe { (self.verbs.poll)(&mut handles, &mut completion) };
                    match count {
                        0 => break,
                        1 => self.complete(completion),
                        code if code < 0 => {
                            self.fail(Fault::Backend("ibv_poll_cq", code));
                            break;
                        }
                        _ => {
                            self.fail(Fault::Protocol("RDMA poll returned more than one CQE"));
                            break;
                        }
                    }
                    if self
                        .control
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .fault
                        .is_some()
                    {
                        break;
                    }
                }
            }
            records.clear();
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            if control.dirty || control.fault.is_some() || control.stopping {
                continue;
            }
            if pending {
                drop(
                    self.wake
                        .wait_timeout(control, Duration::from_micros(50))
                        .unwrap_or_else(|p| p.into_inner()),
                );
            } else {
                while !control.dirty && control.fault.is_none() && !control.stopping {
                    control = self.wake.wait(control).unwrap_or_else(|p| p.into_inner());
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/rdma/cq.rs"]
mod tests;
