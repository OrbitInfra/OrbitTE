use crate::notification::NotificationHook;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::engine::{PreparedSubmission, deadline, remaining};
use crate::{
    BatchReport, Error, Notification, NotificationStatus, RemoteEffect, Result, TransferEngine,
    TransferFailure, TransferOp, TransferRequest, TransferTarget,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BatchId(u64, crate::SourceEpoch);
impl BatchId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Waiting,
    Running,
    Completed,
    Failed,
    Cancelled,
    Skipped,
}
impl TaskState {
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Waiting | Self::Running)
    }
}

#[derive(Clone, Debug)]
pub struct TaskStatus {
    pub state: TaskState,
    pub operation: TransferOp,
    pub transferred_bytes: usize,
    pub cancel_requested: bool,
    pub locally_drained: bool,
    /// The failure index is the public task index in this batch.
    pub failure: Option<Arc<TransferFailure>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchState {
    /// Additional submissions are allowed; completed tasks never regress.
    Open,
    Pending,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchStatus {
    pub state: BatchState,
    pub submitted_tasks: usize,
    pub completed_tasks: usize,
    pub transferred_bytes: usize,
    pub locally_drained: bool,
}
impl BatchStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self.state,
            BatchState::Completed | BatchState::Failed | BatchState::Cancelled
        )
    }
}

#[derive(Clone, Debug)]
pub struct SubmissionStatus {
    pub tasks: Range<usize>,
    pub completed_requests: usize,
    pub transferred_bytes: usize,
    pub failure: Option<Arc<TransferFailure>>,
    pub notification: Arc<NotificationStatus>,
    pub locally_drained: bool,
}

/// A capacity reservation, public task mapping and one bounded FIFO worker.
/// Multiple submit groups append atomically. Seal freezes the batch's terminal
/// status. Dropping this handle detaches accepted work while retaining its
/// engine, buffers, registration and budget until the worker exits.
pub struct Batch {
    engine: Option<Arc<TransferEngine>>,
    progress: Option<Arc<BatchProgress>>,
    worker: Option<JoinHandle<()>>,
}

impl Batch {
    pub(crate) fn allocate(
        engine: Arc<TransferEngine>,
        budget: Arc<AllocationBudget>,
        capacity: usize,
        max_requests: usize,
        max_batches: usize,
        max_capacity: usize,
    ) -> Result<Self> {
        if capacity == 0 || capacity > max_requests {
            return Err(Error::ResourceExhausted);
        }
        budget.sweep_deferred();
        let reservation =
            budget.reserve(engine.source_epoch(), capacity, max_batches, max_capacity)?;
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut submissions = Vec::new();
        submissions
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let progress = Arc::new(BatchProgress {
            defer_free: engine.defers_busy_free(),
            wakes: engine.progress_worker.as_ref().map(|w| w.wakes.clone()),
            #[cfg(feature = "gds")]
            gds: engine.gds.as_ref().map(Arc::downgrade),
            reservation,
            state: Mutex::new(ProgressState {
                tasks,
                submissions,
                queue,
                ready: None,
                receipt: None,
                hooks: VecDeque::new(),
                retry: None,
                active: false,
                abandoned: false,
                sealed: false,
                pending: 0,
                worker_exited: false,
            }),
            changed: Condvar::new(),
        });
        budget.register(&progress)?;
        Ok(Self {
            engine: Some(engine),
            progress: Some(progress),
            worker: None,
        })
    }

    fn progress(&self) -> Result<&Arc<BatchProgress>> {
        self.progress.as_ref().ok_or(Error::BatchClosed)
    }
    pub fn id(&self) -> Result<BatchId> {
        Ok(self.progress()?.reservation.id)
    }
    pub fn capacity(&self) -> Result<usize> {
        Ok(self.progress()?.reservation.capacity)
    }

    /// Validates every range and reserves all physical owners before enqueue.
    /// A rejected group adds no task indices and does not replay earlier groups.
    /// The deadline begins here and includes queue time.
    pub fn submit<'a>(
        &mut self,
        operation: TransferOp,
        peer: impl Into<TransferTarget<'a>>,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
        timeout: Duration,
    ) -> Result<Range<usize>> {
        let peer = peer.into();
        let targets = match peer {
            TransferTarget::Memory(peer) => Some(std::slice::from_ref(peer)),
            TransferTarget::File(_) => None,
        };
        self.submit_group(operation, peer, requests, notification, targets, timeout)
    }

    /// Attach one logical notification for each distinct destination after
    /// this memory submission succeeds. Progress can retry a definitely
    /// unposted target within the submission deadline.
    pub fn submit_with_notification_targets(
        &mut self,
        operation: TransferOp,
        peer: &crate::RemoteSegment,
        requests: &[TransferRequest],
        notification: &Notification,
        targets: &[crate::RemoteSegment],
        timeout: Duration,
    ) -> Result<Range<usize>> {
        self.submit_group(
            operation,
            peer.into(),
            requests,
            Some(notification),
            Some(targets),
            timeout,
        )
    }

    fn submit_group(
        &mut self,
        operation: TransferOp,
        peer: TransferTarget<'_>,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
        targets: Option<&[crate::RemoteSegment]>,
        timeout: Duration,
    ) -> Result<Range<usize>> {
        let progress = Arc::clone(self.progress()?);
        {
            let state = progress.lock();
            if state.sealed || state.worker_exited {
                return Err(Error::BatchClosed);
            }
            if requests.len() > progress.reservation.capacity - state.tasks.len()
                || state.submissions.len() >= progress.reservation.capacity
            {
                return Err(Error::ResourceExhausted);
            }
        }
        let engine = Arc::clone(self.engine.as_ref().ok_or(Error::BatchClosed)?);
        let hook = match (notification, targets) {
            (Some(message), Some(peers)) if !requests.is_empty() => {
                Some(engine.notification_routes.hook(
                    engine.local_target(),
                    peers,
                    message.byte_len()?,
                )?)
            }
            _ => None,
        };
        let mut prepared = engine.prepare(
            operation,
            peer,
            requests,
            notification,
            timeout,
            Some(self.id()?),
        )?;
        if self.worker.is_none() {
            let engine = Arc::clone(&engine);
            let state = Arc::clone(&progress);
            let token = progress
                .reservation
                .budget
                .worker()
                .map_err(|e| prepared.abort_error(e))?;
            self.worker = Some(
                thread::Builder::new()
                    .name("orbitte-batch".into())
                    .spawn(move || {
                        let _exit = WorkerExit {
                            progress: Arc::clone(&state),
                            _token: token,
                        };
                        state.run(&engine);
                    })
                    .map_err(|e| prepared.abort_error(e.into()))?,
            );
        }
        let range = {
            let mut state = progress.lock();
            if state.worker_exited {
                return Err(prepared.abort_error(Error::BatchClosed));
            }
            let first = state.tasks.len();
            let range = first..first + requests.len();
            for _ in requests {
                state.tasks.push(TaskStatus {
                    state: TaskState::Waiting,
                    operation,
                    transferred_bytes: 0,
                    cancel_requested: false,
                    locally_drained: false,
                    failure: None,
                });
            }
            let submission = state.submissions.len();
            state.submissions.push(SubmissionStatus {
                tasks: range.clone(),
                completed_requests: 0,
                transferred_bytes: 0,
                failure: None,
                notification: Arc::new(if notification.is_some() {
                    NotificationStatus::Skipped
                } else {
                    NotificationStatus::NotRequested
                }),
                locally_drained: false,
            });
            state.pending += 1;
            let hook = hook.and_then(|targets| {
                prepared.notification.take().map(|message| Retry {
                    admission: None,
                    submission,
                    message,
                    targets,
                    deadline: prepared.deadline,
                })
            });
            state.queue.push_back(QueuedSubmission {
                hook,
                first_task: range.start,
                submission,
                prepared,
            });
            range
        };
        progress.kick();
        Ok(range)
    }

    /// Stops appending. A terminal failure can retain unquiesced native work;
    /// inspect locally_drained before free. An empty sealed batch completes.
    pub fn seal(&mut self) -> Result<()> {
        let progress = self.progress()?;
        progress.lock().sealed = true;
        progress.changed.notify_all();
        Ok(())
    }

    pub fn status(&self) -> Result<BatchStatus> {
        Ok(self.progress()?.lock().status())
    }
    pub fn task_status(&self, task: usize) -> Result<TaskStatus> {
        self.progress()?
            .lock()
            .tasks
            .get(task)
            .cloned()
            .ok_or(Error::InvalidTask)
    }
    pub fn task_statuses(&self) -> Result<Vec<TaskStatus>> {
        Ok(self.progress()?.lock().tasks.clone())
    }
    pub fn submissions(&self) -> Result<Vec<SubmissionStatus>> {
        Ok(self.progress()?.lock().submissions.clone())
    }

    /// Requests cancellation. Queued work is skipped; a running CPU operation
    /// finishes or reaches its deadline before exposing its actual outcome.
    /// Accepted cancellation is not physical quiescence.
    pub fn cancel(&self, task: usize) -> Result<bool> {
        let mut state = self.progress()?.lock();
        let task = state.tasks.get_mut(task).ok_or(Error::InvalidTask)?;
        if task.state.is_terminal() {
            return Ok(false);
        }
        task.cancel_requested = true;
        drop(state);
        if let Some(engine) = &self.engine {
            engine.wake_runtime_queue();
        }
        Ok(true)
    }

    pub fn cancel_all(&self) -> Result<()> {
        let mut state = self.progress()?.lock();
        for task in &mut state.tasks {
            if !task.state.is_terminal() {
                task.cancel_requested = true;
            }
        }
        drop(state);
        if let Some(engine) = &self.engine {
            engine.wake_runtime_queue();
        }
        Ok(())
    }

    /// Observational wait: timeout neither cancels nor frees work. Seal before
    /// waiting so a successful result is stable against later submissions.
    pub fn wait(&self, timeout: Duration) -> Result<BatchStatus> {
        let progress = self.progress()?;
        let mut state = progress.lock();
        if !state.sealed {
            return Err(Error::BatchOpen);
        }
        if state.status().is_terminal() {
            return Ok(state.status());
        }
        let deadline = deadline(timeout)?;
        loop {
            state = progress
                .changed
                .wait_timeout(state, remaining(deadline)?)
                .unwrap_or_else(|poison| poison.into_inner())
                .0;
            let status = state.status();
            if status.is_terminal() {
                return Ok(status);
            }
        }
    }

    /// In event-driven mode a pending free hands ownership to deferred retirement.
    /// With the worker disabled a busy handle stays valid for retry.
    pub fn free(&mut self) -> Result<()> {
        let progress = self.progress()?;
        if !progress.lock().can_free() {
            if !progress.defer_free {
                return Err(Error::BatchBusy);
            }
            self.detach();
            return Ok(());
        }
        self.seal()?;
        self.detach();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        Ok(())
    }

    fn detach(&mut self) {
        if let Some(progress) = self.progress.take() {
            progress.abandon();
        }
        self.engine.take();
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        self.detach();
    }
}

struct Retry {
    // Close waits for a pending admitted hook, even after payload loans drop.
    admission: Option<crate::engine::BatchPermit>,
    submission: usize,
    message: Notification,
    targets: NotificationHook,
    deadline: Instant,
}
struct Receipt {
    notification: Option<Arc<NotificationStatus>>,
    submission: usize,
    first_task: usize,
    report: BatchReport,
}

struct QueuedSubmission {
    hook: Option<Retry>,
    first_task: usize,
    submission: usize,
    prepared: PreparedSubmission,
}

pub(crate) struct BatchProgress {
    defer_free: bool,
    wakes: Option<Arc<crate::runtime::progress_worker::Wakes>>,
    #[cfg(feature = "gds")]
    gds: Option<Weak<crate::transport::gds::GdsTransport>>,
    reservation: Reservation,
    state: Mutex<ProgressState>,
    changed: Condvar,
}
struct ProgressState {
    queue: VecDeque<QueuedSubmission>,
    ready: Option<QueuedSubmission>,
    hooks: VecDeque<Retry>,
    retry: Option<Retry>,
    receipt: Option<Receipt>,
    active: bool,
    abandoned: bool,
    tasks: Vec<TaskStatus>,
    submissions: Vec<SubmissionStatus>,
    sealed: bool,
    pending: usize,
    worker_exited: bool,
}
impl ProgressState {
    fn can_free(&self) -> bool {
        !self.active && self.status().locally_drained
    }
    fn status(&self) -> BatchStatus {
        let state = if !self.sealed {
            BatchState::Open
        } else if self.pending != 0 {
            BatchState::Pending
        } else if self
            .tasks
            .iter()
            .any(|task| task.state == TaskState::Failed)
        {
            BatchState::Failed
        } else if self
            .tasks
            .iter()
            .any(|task| task.state == TaskState::Cancelled)
        {
            BatchState::Cancelled
        } else {
            BatchState::Completed
        };
        BatchStatus {
            state,
            submitted_tasks: self.tasks.len(),
            completed_tasks: self
                .tasks
                .iter()
                .filter(|task| task.state == TaskState::Completed)
                .count(),
            transferred_bytes: self.tasks.iter().map(|task| task.transferred_bytes).sum(),
            locally_drained: self.pending == 0
                && self
                    .submissions
                    .iter()
                    .all(|submission| submission.locally_drained),
        }
    }
}
impl BatchProgress {
    #[cfg(feature = "gds")]
    pub(crate) fn id(&self) -> BatchId {
        self.reservation.id
    }

    fn lock(&self) -> MutexGuard<'_, ProgressState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
    // Exactly one receipt or one dispatch. The batch mutex covers transitions
    // only; the blocking payload call never holds it or the registry lock.
    pub(crate) fn step(&self) -> bool {
        self.advance(true)
    }

    fn advance(&self, allow_hook: bool) -> bool {
        let mut state = self.lock();
        if let Some(receipt) = state.receipt.take() {
            self.finish_submission(
                &mut state,
                receipt.submission,
                receipt.first_task,
                receipt.report,
            );
            if let Some(notification) = receipt.notification {
                state.submissions[receipt.submission].notification = notification;
            }
            let more = !state.queue.is_empty();
            drop(state);
            if let Some(wakes) = &self.wakes {
                if more {
                    wakes.batch(self.reservation.id);
                }
            }
            return true;
        } else if !state.active {
            if let Some(next) = state.queue.pop_front() {
                state.active = true;
                state.ready = Some(next);
                self.changed.notify_all();
                return true;
            } else if allow_hook && !state.abandoned {
                if let Some(hook) = state.hooks.pop_front() {
                    state.active = true;
                    state.retry = Some(hook);
                    self.changed.notify_all();
                    return true;
                }
            }
        }
        false
    }

    fn kick(&self) {
        if let Some(wakes) = &self.wakes {
            wakes.batch(self.reservation.id);
        } else {
            self.advance(false);
            if !self.lock().queue.is_empty() {
                self.advance(false);
            }
        }
    }

    fn run(self: &Arc<Self>, engine: &TransferEngine) {
        let mut state = self.lock();
        loop {
            if let Some(job) = state.ready.take() {
                drop(state);
                self.execute_group(engine, job);
                self.kick();
            } else if let Some(mut retry) = state.retry.take() {
                drop(state);
                let status = retry
                    .targets
                    .attempt(engine, &retry.message, retry.deadline);
                let mut locked = self.lock();
                locked.submissions[retry.submission].notification = status;
                if !locked.abandoned && retry.targets.pending() {
                    locked.hooks.push_back(retry);
                }
                locked.active = false;
                let more = !locked.queue.is_empty();
                self.changed.notify_all();
                drop(locked);
                if more {
                    self.kick();
                }
            } else {
                let now = Instant::now();
                state.hooks.retain(|h| h.deadline > now);
                if let Some(at) = state.hooks.iter().map(|h| h.deadline).min() {
                    state = self
                        .changed
                        .wait_timeout(state, at.saturating_duration_since(now))
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                } else if state.sealed && state.pending == 0 {
                    return;
                } else {
                    state = self.changed.wait(state).unwrap_or_else(|e| e.into_inner());
                }
                continue;
            }
            state = self.lock();
        }
    }

    fn execute_group(self: &Arc<Self>, engine: &TransferEngine, job: QueuedSubmission) {
        let QueuedSubmission {
            first_task,
            submission,
            mut prepared,
            hook,
        } = job;
        let mut report = engine.execute(&prepared, Some((self, first_task)));
        let mut retry = None;
        let mut notification = None;
        if let Some(mut hook) = hook {
            report.notification = NotificationStatus::Skipped;
            if report.is_complete() {
                notification = Some(hook.targets.attempt(engine, &hook.message, hook.deadline));
                if hook.targets.pending() {
                    hook.admission = prepared.admission.take();
                    retry = Some(hook);
                }
            }
        }
        if report.locally_drained {
            drop(prepared);
            let mut state = self.lock();
            if let Some(hook) = retry {
                if !state.abandoned {
                    state.hooks.push_back(hook);
                }
            }
            state.receipt = Some(Receipt {
                submission,
                first_task,
                notification,
                report,
            });
        } else {
            engine.retain_undrained(prepared, Some((self.clone(), submission)), || {
                // Publish under the native quarantine lock before a late receipt.
                self.finish_submission(&mut self.lock(), submission, first_task, report);
            });
        }
    }

    pub(crate) fn progress_public(&self) -> Result<BatchStatus> {
        if self.lock().abandoned {
            return Err(Error::BatchClosed);
        }
        if !self.step() {
            self.probe_retirement();
        }
        let state = self.lock();
        if state.abandoned {
            Err(Error::BatchClosed)
        } else {
            Ok(state.status())
        }
    }

    fn abandon(&self) {
        let mut state = self.lock();
        state.abandoned = true;
        state.hooks.clear();
        if state.retry.take().is_some() {
            state.active = false;
        }
        state.sealed = true;
        if !state.can_free() {
            self.reservation
                .budget
                .lifecycle(self.reservation.id, Life::Deferred);
        }
        self.changed.notify_all();
        drop(state);
        self.probe_retirement();
        self.kick();
    }

    fn probe_retirement(&self) {
        let Ok(state) = self.state.try_lock() else {
            return;
        };
        if state.pending != 0 || state.submissions.iter().all(|s| s.locally_drained) {
            return;
        }
        drop(state);
        #[cfg(feature = "gds")]
        if let Some(gds) = self.gds.as_ref().and_then(Weak::upgrade) {
            if gds.try_collect_for(self.reservation.id) {
                return;
            }
        }
        let state = self.lock();
        if state.abandoned && !state.status().locally_drained {
            self.reservation
                .budget
                .lifecycle(self.reservation.id, Life::Quarantined);
        }
    }

    pub(crate) fn cancel_requested(&self, index: usize) -> bool {
        self.lock().tasks[index].cancel_requested
    }
    pub(crate) fn begin_task(&self, index: usize) -> bool {
        let mut state = self.lock();
        let task = &mut state.tasks[index];
        if task.cancel_requested {
            return false;
        }
        task.state = TaskState::Running;
        true
    }
    /// Commit a file group on the batch worker. A queued cancellation cuts off
    /// that request and its suffix before any SQE is published.
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    pub(crate) fn begin_file_group(&self, first: usize, count: usize) -> usize {
        let mut state = self.lock();
        let tasks = &mut state.tasks[first..first + count];
        let count = tasks
            .iter()
            .position(|task| task.cancel_requested)
            .unwrap_or(count);
        for task in &mut tasks[..count] {
            task.state = TaskState::Running;
        }
        count
    }

    #[cfg(any(feature = "io-uring", feature = "gds"))]
    pub(crate) fn fail_file_task(
        &self,
        index: usize,
        bytes: usize,
        error: Error,
        effect: RemoteEffect,
        drained: bool,
    ) {
        let mut state = self.lock();
        let task = &mut state.tasks[index];
        task.state = if matches!(error, Error::Cancelled) {
            TaskState::Cancelled
        } else {
            TaskState::Failed
        };
        task.transferred_bytes = bytes;
        task.locally_drained = drained;
        task.failure = Some(Arc::new(TransferFailure {
            request_index: index,
            affected_requests: 1,
            error,
            remote_effect: effect,
        }));
        self.changed.notify_all();
    }
    pub(crate) fn complete_task(&self, index: usize, length: usize) {
        let mut state = self.lock();
        state.tasks[index].state = TaskState::Completed;
        state.tasks[index].transferred_bytes = length;
        state.tasks[index].locally_drained = true;
        self.changed.notify_all();
    }
    #[cfg(feature = "gds")]
    pub(crate) fn file_progress(&self, index: usize, bytes: usize) {
        self.lock().tasks[index].transferred_bytes = bytes;
    }
    #[cfg(feature = "gds")]
    pub(crate) fn complete_file_task(&self, index: usize, bytes: usize, drained: bool) {
        let mut state = self.lock();
        let task = &mut state.tasks[index];
        task.state = TaskState::Completed;
        task.transferred_bytes = bytes;
        task.locally_drained = drained;
        self.changed.notify_all();
    }
    #[cfg(feature = "gds")]
    pub(crate) fn gds_drained(&self, index: usize) {
        let mut state = self.lock();
        state.submissions[index].locally_drained = true;
        let range = state.submissions[index].tasks.clone();
        for task in &mut state.tasks[range] {
            task.locally_drained = true;
        }
        self.changed.notify_all();
    }
    fn finish_submission(
        &self,
        state: &mut ProgressState,
        index: usize,
        first: usize,
        report: BatchReport,
    ) {
        state.active = false;
        let failure = report.failure.map(|mut failure| {
            failure.request_index += first;
            Arc::new(failure)
        });
        let range = state.submissions[index].tasks.clone();
        if let Some(failure) = &failure {
            for task_index in failure.request_index..range.end {
                let task = &mut state.tasks[task_index];
                // File CQEs are independent. A later request may have its own
                // successful or short completion after the first failed request.
                if task.state.is_terminal() {
                    continue;
                }
                task.state = if task_index < failure.request_index + failure.affected_requests {
                    if matches!(failure.error, Error::Cancelled) {
                        TaskState::Cancelled
                    } else {
                        TaskState::Failed
                    }
                } else if task.cancel_requested {
                    TaskState::Cancelled
                } else {
                    TaskState::Skipped
                };
                task.locally_drained = task_index
                    >= failure.request_index + failure.affected_requests
                    || report.locally_drained;
                if task_index < failure.request_index + failure.affected_requests {
                    task.failure = Some(Arc::clone(failure));
                }
            }
        }
        state.submissions[index] = SubmissionStatus {
            tasks: range,
            completed_requests: report.completed_requests,
            transferred_bytes: report.transferred_bytes,
            failure,
            notification: Arc::new(report.notification),
            locally_drained: report.locally_drained,
        };
        state.pending -= 1;
        self.changed.notify_all();
    }
}

impl Drop for BatchProgress {
    fn drop(&mut self) {
        if let Some(wakes) = &self.wakes {
            wakes.discard(self.reservation.id);
        }
    }
}

struct WorkerExit {
    progress: Arc<BatchProgress>,
    _token: WorkerToken,
}
impl Drop for WorkerExit {
    fn drop(&mut self) {
        // Drop unstarted physical owners outside the progress lock on unwind.
        let (queue, ready) = {
            let mut state = self.progress.lock();
            state.worker_exited = true;
            (std::mem::take(&mut state.queue), state.ready.take())
        };
        drop((queue, ready));
        let mut state = self.progress.lock();
        if state.pending != 0 {
            for index in 0..state.submissions.len() {
                if state.submissions[index].locally_drained
                    || state.submissions[index].failure.is_some()
                {
                    continue;
                }
                let range = state.submissions[index].tasks.clone();
                let mut completed = 0;
                let mut bytes = 0;
                for task_index in range {
                    let task = &mut state.tasks[task_index];
                    if task.state == TaskState::Completed {
                        completed += 1;
                        bytes += task.transferred_bytes;
                        continue;
                    }
                    let failure = Arc::new(TransferFailure {
                        request_index: task_index,
                        affected_requests: 1,
                        error: Error::Internal("batch worker stopped before normal completion"),
                        remote_effect: if task.operation == TransferOp::Write
                            && task.state == TaskState::Running
                        {
                            RemoteEffect::Unknown
                        } else {
                            RemoteEffect::None
                        },
                    });
                    task.state = TaskState::Failed;
                    task.locally_drained = true;
                    task.failure = Some(Arc::clone(&failure));
                    if state.submissions[index].failure.is_none() {
                        state.submissions[index].failure = Some(failure);
                    }
                }
                state.submissions[index].completed_requests = completed;
                state.submissions[index].transferred_bytes = bytes;
                state.submissions[index].locally_drained = true;
            }
            state.pending = 0;
        }
        self.progress.changed.notify_all();
        drop(state);
        self.progress.probe_retirement();
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Life {
    Live,
    Deferred,
    Quarantined,
}

pub(crate) struct AllocationBudget {
    state: Mutex<Allocated>,
}
struct Allocated {
    registry: HashMap<BatchId, (Weak<BatchProgress>, Life)>,
    next_id: u64,
    count: usize,
    capacity: usize,
    workers: usize,
}
impl AllocationBudget {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(Allocated {
                registry: HashMap::new(),
                next_id: 1,
                count: 0,
                capacity: 0,
                workers: 0,
            }),
        }
    }
    pub(crate) fn lookup(&self, id: BatchId) -> Option<Arc<BatchProgress>> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .registry
            .get(&id)?
            .0
            .upgrade()
    }
    fn register(&self, progress: &Arc<BatchProgress>) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .registry
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        state.registry.insert(
            progress.reservation.id,
            (Arc::downgrade(progress), Life::Live),
        );
        Ok(())
    }
    fn lifecycle(&self, id: BatchId, life: Life) {
        if let Some((_, current)) = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .registry
            .get_mut(&id)
        {
            *current = life;
        }
    }
    pub(crate) fn sweep_deferred(&self) {
        let parked: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .registry
            .values()
            .filter(|(_, life)| *life == Life::Deferred)
            .filter_map(|(weak, _)| weak.upgrade())
            .collect();
        for batch in parked {
            batch.probe_retirement();
        }
    }

    pub(crate) fn usage(&self) -> Result<(usize, usize, usize, usize, usize)> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("batch allocation lock poisoned"))?;
        Ok((
            state.count,
            state.capacity,
            state.workers,
            state
                .registry
                .values()
                .filter(|(_, l)| *l == Life::Deferred)
                .count(),
            state
                .registry
                .values()
                .filter(|(_, l)| *l == Life::Quarantined)
                .count(),
        ))
    }
    fn reserve(
        self: &Arc<Self>,
        epoch: crate::SourceEpoch,
        capacity: usize,
        max_batches: usize,
        max_capacity: usize,
    ) -> Result<Reservation> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("batch allocation lock poisoned"))?;
        let total = state
            .capacity
            .checked_add(capacity)
            .ok_or(Error::ResourceExhausted)?;
        let next = state
            .next_id
            .checked_add(1)
            .ok_or(Error::ResourceExhausted)?;
        if state.count >= max_batches || total > max_capacity {
            return Err(Error::ResourceExhausted);
        }
        let id = BatchId(state.next_id, epoch);
        state.next_id = next;
        state.count += 1;
        state.capacity = total;
        Ok(Reservation {
            budget: Arc::clone(self),
            id,
            capacity,
        })
    }
    fn worker(self: &Arc<Self>) -> Result<WorkerToken> {
        self.state
            .lock()
            .map_err(|_| Error::Internal("batch allocation lock poisoned"))?
            .workers += 1;
        Ok(WorkerToken {
            budget: Arc::clone(self),
        })
    }
}
struct Reservation {
    budget: Arc<AllocationBudget>,
    id: BatchId,
    capacity: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.registry.remove(&self.id);
        state.count -= 1;
        state.capacity -= self.capacity;
    }
}
struct WorkerToken {
    budget: Arc<AllocationBudget>,
}
impl Drop for WorkerToken {
    fn drop(&mut self) {
        self.budget
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .workers -= 1;
    }
}

#[cfg(test)]
#[path = "../tests/unit/batch.rs"]
mod tests;
