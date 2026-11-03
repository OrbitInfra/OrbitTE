use crate::notification::NotificationHook;
use crate::runtime::admission_queue::QueueTicket;
use crate::runtime::request_merge::{Alias, TaskMap};
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
    /// Absolute public task ID of this transfer’s physical owner.
    pub physical_owner: usize,
    /// Current route and additional attempts of the shared physical owner.
    pub transport: crate::Transport,
    /// Additional attempts, excluding the first dispatch.
    pub failover_count: usize,
    pub state: TaskState,
    pub operation: TransferOp,
    pub transferred_bytes: usize,
    pub cancel_requested: bool,
    pub locally_drained: bool,
    /// The failure identifies the affected public interval in this batch.
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

pub(crate) enum RunningCancel {
    HpTcp(tokio::sync::watch::Sender<bool>),
    Unsupported,
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    File,
    #[cfg(feature = "rdma")]
    Rdma(Arc<std::sync::atomic::AtomicBool>),
}

struct TaskControl {
    ticket: Option<QueueTicket>,
    running: RunningCancel,
}
struct Group {
    first_physical: usize,
    mapping: Arc<TaskMap>,
    status: SubmissionStatus,
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
        let mut public = Vec::new();
        public
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut controls = Vec::new();
        controls
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let progress = Arc::new(BatchProgress {
            defer_free: engine.defers_busy_free(),
            auto_failover: engine.auto_failover_on_poll(),
            wakes: engine.progress_worker.as_ref().map(|w| w.wakes.clone()),
            #[cfg(feature = "gds")]
            gds: engine.gds.as_ref().map(Arc::downgrade),
            reservation,
            state: Mutex::new(ProgressState {
                tasks,
                public,
                controls,
                submissions,
                queue,
                ready: None,
                receipt: None,
                retry_gate: None,
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
            if requests.len() > progress.reservation.capacity - state.public.len()
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
            let first = state.public.len();
            let first_physical = state.tasks.len();
            let range = first..first + requests.len();
            for a in &prepared.mapping.public {
                state.public.push(Alias {
                    owner: first_physical + a.owner,
                    ..*a
                });
            }
            for (i, owner) in prepared.mapping.owners.iter().enumerate() {
                state.controls.push(TaskControl {
                    ticket: prepared.ticket(i),
                    running: prepared.running_cancel(i),
                });
                state.tasks.push(TaskStatus {
                    physical_owner: first + owner.representative,
                    transport: prepared.route(i).transport,
                    failover_count: prepared.route(i).initial_attempts,
                    state: TaskState::Waiting,
                    operation,
                    transferred_bytes: 0,
                    cancel_requested: false,
                    locally_drained: false,
                    failure: None,
                });
            }
            let submission = state.submissions.len();
            state.submissions.push(Group {
                first_physical,
                mapping: prepared.mapping.clone(),
                status: SubmissionStatus {
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
                },
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
                first_task: first_physical,
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
        let p = self.progress()?;
        let mut state = p.lock();
        p.observe_retry(&mut state, None);
        Ok(state.status())
    }
    pub fn task_status(&self, task: usize) -> Result<TaskStatus> {
        let p = self.progress()?;
        let mut state = p.lock();
        let owner = state.public.get(task).ok_or(Error::InvalidTask)?.owner;
        p.observe_retry(&mut state, Some(owner));
        state.public_status(task)
    }
    pub fn task_statuses(&self) -> Result<Vec<TaskStatus>> {
        let p = self.progress()?;
        let mut s = p.lock();
        p.observe_retry(&mut s, None);
        (0..s.public.len()).map(|id| s.public_status(id)).collect()
    }
    pub fn submissions(&self) -> Result<Vec<SubmissionStatus>> {
        Ok(self
            .progress()?
            .lock()
            .submissions
            .iter()
            .map(|g| g.status.clone())
            .collect())
    }

    /// Cancel a queued physical owner through any public alias. Running RDMA
    /// drops only unposted slices. A running transport without cancellation
    /// support returns Unavailable. A cancel never proves physical quiescence.
    pub fn cancel(&self, task: usize) -> Result<bool> {
        self.cancel_owners(Some(task))
    }

    /// Attempt every physical owner, including queued successors of a running
    /// transport that cannot cancel. Return the first unsupported operation.
    pub fn cancel_all(&self) -> Result<()> {
        self.cancel_owners(None).map(|_| ())
    }

    fn cancel_owners(&self, public_task: Option<usize>) -> Result<bool> {
        let mut state = self.progress()?.lock();
        let owners = match public_task {
            Some(id) => {
                let i = state.public.get(id).ok_or(Error::InvalidTask)?.owner;
                i..i + 1
            }
            None => 0..state.tasks.len(),
        };
        let engine = self.engine.as_ref().ok_or(Error::BatchClosed)?;
        let mut tickets = Vec::new();
        let mut accepted = false;
        let mut unsupported = false;
        #[cfg(feature = "rdma")]
        let mut native = false;
        // Publish every bulk cancellation before a freed slot can admit a
        // successor. The queue is woken only after releasing the batch lock.
        for owner in owners {
            let control = &state.controls[owner];
            let task = &state.tasks[owner];
            if task.state.is_terminal()
                || control
                    .ticket
                    .as_ref()
                    .is_some_and(|t| t.terminal().is_err())
            {
                continue;
            }
            if task.cancel_requested {
                accepted = true;
                continue;
            }
            let between_attempts = state.retry_gate.as_ref().is_some_and(|g| g.owner == owner);
            if task.state == TaskState::Running && !between_attempts {
                match &control.running {
                    RunningCancel::HpTcp(signal) => {
                        signal.send_replace(true);
                    }
                    RunningCancel::Unsupported => {
                        unsupported = true;
                        continue;
                    }
                    #[cfg(any(feature = "io-uring", feature = "gds"))]
                    RunningCancel::File => {}
                    #[cfg(feature = "rdma")]
                    RunningCancel::Rdma(flag) => {
                        flag.store(true, std::sync::atomic::Ordering::Release);
                        native = true;
                    }
                }
            }
            if let Some(ticket) = &control.ticket {
                tickets.push(ticket.clone());
            }
            state.tasks[owner].cancel_requested = true;
            accepted = true;
        }
        self.progress()?.changed.notify_all();
        drop(state);
        for ticket in tickets {
            engine.cancel_queued(&ticket);
        }
        #[cfg(feature = "rdma")]
        if native {
            engine.wake_rdma_cancel();
        }
        if accepted {
            engine.wake_runtime_queue();
        }
        if unsupported {
            return Err(Error::Unavailable(
                "running transport does not support cancellation",
            ));
        }
        Ok(accepted)
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
            progress.decide_retry(&mut state, true);
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
        let drained = progress.retire(progress.defer_free)?;
        self.progress.take();
        self.engine.take();
        if drained {
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
        Ok(())
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        if let Some(progress) = self.progress.take() {
            let _ = progress.retire(true);
        }
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
struct RetryGate {
    owner: usize,
    failure: Arc<TransferFailure>,
    decision: Option<bool>,
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
    auto_failover: bool,
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
    // At most one unresolved attempt on the single executing batch worker.
    retry_gate: Option<RetryGate>,
    active: bool,
    abandoned: bool,
    tasks: Vec<TaskStatus>,
    public: Vec<Alias>,
    controls: Vec<TaskControl>,
    submissions: Vec<Group>,
    sealed: bool,
    pending: usize,
    worker_exited: bool,
}
impl ProgressState {
    fn public_status(&self, id: usize) -> Result<TaskStatus> {
        let a = *self.public.get(id).ok_or(Error::InvalidTask)?;
        let mut task = self.tasks[a.owner].clone();
        task.transferred_bytes = task
            .transferred_bytes
            .saturating_sub(a.offset)
            .min(a.length);
        if task.state == TaskState::Waiting {
            if let Some(failure) = self.ticket_failure(a.owner) {
                task.state = TaskState::Cancelled;
                task.failure = Some(failure);
            }
        }
        Ok(task)
    }
    fn ticket_failure(&self, slot: usize) -> Option<Arc<TransferFailure>> {
        let error = self.controls[slot].ticket.as_ref()?.terminal().err()?;
        let span = self.public_span(slot..slot + 1);
        Some(Arc::new(TransferFailure {
            request_index: span.start,
            affected_requests: span.len(),
            error,
            remote_effect: RemoteEffect::None,
        }))
    }
    fn public_span(&self, physical: Range<usize>) -> Range<usize> {
        let start = self.tasks[physical.start].physical_owner;
        let end = self
            .public
            .iter()
            .rposition(|a| physical.contains(&a.owner))
            .map_or(start, |i| i + 1);
        start..end
    }
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
            submitted_tasks: self.public.len(),
            completed_tasks: self
                .public
                .iter()
                .filter(|a| self.tasks[a.owner].state == TaskState::Completed)
                .count(),
            transferred_bytes: self.tasks.iter().map(|task| task.transferred_bytes).sum(),
            locally_drained: self.pending == 0
                && self
                    .submissions
                    .iter()
                    .all(|group| group.status.locally_drained),
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
        if self.decide_retry(&mut self.lock(), true) {
            return true;
        }
        self.advance(true)
    }

    fn decide_retry(&self, state: &mut ProgressState, allow: bool) -> bool {
        let Some(gate) = &mut state.retry_gate else {
            return false;
        };
        if gate.decision.is_some() {
            return false;
        }
        gate.decision = Some(allow);
        if !allow {
            let task = &mut state.tasks[gate.owner];
            task.state = TaskState::Failed;
            task.failure = Some(gate.failure.clone());
            task.locally_drained = true;
        }
        self.changed.notify_all();
        true
    }

    fn observe_retry(&self, state: &mut ProgressState, owner: Option<usize>) {
        if state
            .retry_gate
            .as_ref()
            .is_some_and(|g| owner.is_none_or(|i| i == g.owner))
        {
            self.decide_retry(state, self.auto_failover);
        }
    }

    /// Publish the drained backend failure privately until one poll, progress
    /// turn or wait decides it. Logical failure, once observed, is final.
    pub(crate) fn retry_on_progress(&self, owner: usize, error: Error, deadline: Instant) -> bool {
        let mut s = self.lock();
        if s.tasks[owner].cancel_requested || s.abandoned {
            return false;
        }
        let span = s.public_span(owner..owner + 1);
        s.retry_gate = Some(RetryGate {
            owner,
            decision: None,
            failure: Arc::new(TransferFailure {
                request_index: span.start,
                affected_requests: span.len(),
                error,
                remote_effect: RemoteEffect::None,
            }),
        });
        self.changed.notify_all();
        if let Some(wakes) = &self.wakes {
            wakes.batch(self.reservation.id);
        }
        let allow = loop {
            if s.abandoned || s.tasks[owner].cancel_requested || Instant::now() >= deadline {
                break false;
            }
            if let Some(d) = s.retry_gate.as_ref().and_then(|g| g.decision) {
                break d;
            }
            s = self
                .changed
                .wait_timeout(s, deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        };
        s.retry_gate = None;
        allow
    }

    pub(crate) fn start_retry(
        &self,
        owner: usize,
        transport: crate::Transport,
        count: usize,
        running: RunningCancel,
    ) -> bool {
        let mut s = self.lock();
        if s.abandoned || s.tasks[owner].cancel_requested || s.tasks[owner].state.is_terminal() {
            return false;
        }
        s.tasks[owner].transport = transport;
        s.tasks[owner].failover_count = count;
        s.controls[owner].running = running;
        self.changed.notify_all();
        true
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
                state.submissions[receipt.submission].status.notification = notification;
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
                locked.submissions[retry.submission].status.notification = status;
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
        let mut report = engine.execute_physical(&prepared, Some((self, first_task)));
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

    fn retire(&self, allow_pending: bool) -> Result<bool> {
        let mut state = self.lock();
        // Fence new retries in the same transition that decides whether free
        // can join. A concurrent progress turn may already own a socket.
        if !allow_pending && !state.can_free() {
            return Err(Error::BatchBusy);
        }
        state.abandoned = true;
        state.hooks.clear();
        if state.retry.take().is_some() {
            state.active = false;
        }
        state.sealed = true;
        let drained = state.can_free();
        if !drained {
            self.reservation
                .budget
                .lifecycle(self.reservation.id, Life::Deferred);
        }
        self.changed.notify_all();
        drop(state);
        self.probe_retirement();
        self.kick();
        Ok(drained)
    }

    fn probe_retirement(&self) {
        let Ok(state) = self.state.try_lock() else {
            return;
        };
        if state.pending != 0 || state.submissions.iter().all(|s| s.status.locally_drained) {
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
    pub(crate) fn fail_owners(&self, base: usize, mut failure: TransferFailure, drained: bool) {
        let mut s = self.lock();
        let first = base + failure.request_index;
        let range = first..first + failure.affected_requests;
        let public = s.public_span(range.clone());
        failure.request_index = public.start;
        failure.affected_requests = public.len();
        let state = if matches!(failure.error, Error::Cancelled | Error::DeadlineInfeasible) {
            TaskState::Cancelled
        } else {
            TaskState::Failed
        };
        let shared = Arc::new(failure);
        for t in &mut s.tasks[range] {
            if !t.state.is_terminal() {
                t.state = state;
                t.locally_drained = drained;
                t.failure = Some(shared.clone());
            }
        }
        self.changed.notify_all();
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
            request_index: task.physical_owner,
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
    #[cfg(any(feature = "io-uring", feature = "gds"))]
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
        state.submissions[index].status.locally_drained = true;
        let group = &state.submissions[index];
        let range = group.first_physical..group.first_physical + group.mapping.owners.len();
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
        // A queued cancel/drop can become visible before this worker reaches
        // its group. Preserve that terminal outcome if a predecessor fails.
        let count = state.submissions[index].mapping.owners.len();
        for slot in first..first + count {
            if state.tasks[slot].state == TaskState::Waiting {
                if let Some(failure) = state.ticket_failure(slot) {
                    let task = &mut state.tasks[slot];
                    task.state = TaskState::Cancelled;
                    task.failure = Some(failure);
                    task.locally_drained = true;
                }
            }
        }
        let group = &state.submissions[index];
        let physical = report
            .failure
            .as_ref()
            .map(|f| f.request_index..f.request_index + f.affected_requests);
        let operation = state
            .tasks
            .get(first)
            .map_or(TransferOp::Read, |t| t.operation);
        let report = group.mapping.public_report(report, operation);
        let failure = report.failure.map(|mut f| {
            f.request_index += group.status.tasks.start;
            Arc::new(f)
        });
        if let (Some(failed), Some(failure)) = (physical, &failure) {
            for i in failed.start..group.mapping.owners.len() {
                let task = &mut state.tasks[first + i];
                if task.state.is_terminal() {
                    continue;
                }
                task.state = if failed.contains(&i) {
                    if matches!(failure.error, Error::Cancelled | Error::DeadlineInfeasible) {
                        TaskState::Cancelled
                    } else {
                        TaskState::Failed
                    }
                } else if task.cancel_requested {
                    TaskState::Cancelled
                } else {
                    TaskState::Skipped
                };
                task.locally_drained = !failed.contains(&i) || report.locally_drained;
                if failed.contains(&i) {
                    task.failure = Some(failure.clone());
                }
            }
        }
        let group = &mut state.submissions[index].status;
        group.completed_requests = report.completed_requests;
        group.transferred_bytes = report.transferred_bytes;
        group.failure = failure;
        group.notification = Arc::new(report.notification);
        group.locally_drained = report.locally_drained;
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
                if state.submissions[index].status.locally_drained
                    || state.submissions[index].status.failure.is_some()
                {
                    continue;
                }
                let group = &state.submissions[index];
                let first = group.first_physical;
                let mapping = group.mapping.clone();
                let range = first..first + mapping.owners.len();
                for task_index in range {
                    let span = state.public_span(task_index..task_index + 1);
                    let task = &mut state.tasks[task_index];
                    if task.state.is_terminal() {
                        continue;
                    }
                    let failure = Arc::new(TransferFailure {
                        request_index: span.start,
                        affected_requests: span.len(),
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
                }
                let prefix = mapping
                    .public
                    .iter()
                    .take_while(|a| state.tasks[first + a.owner].state == TaskState::Completed)
                    .count();
                let failure = mapping
                    .public
                    .get(prefix)
                    .and_then(|a| state.tasks[first + a.owner].failure.clone());
                let group = &mut state.submissions[index].status;
                group.failure = failure;
                group.completed_requests = prefix;
                group.transferred_bytes = mapping.public[..prefix].iter().map(|a| a.length).sum();
                state.submissions[index].status.locally_drained = true;
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
    #[cfg(feature = "metrics")]
    pub(crate) metrics: std::sync::OnceLock<std::sync::Weak<crate::metrics::Recorder>>,

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
            #[cfg(feature = "metrics")]
            metrics: Default::default(),
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
            #[cfg(feature = "metrics")]
            if *current != Life::Quarantined && life == Life::Quarantined {
                if let Some(recorder) = self.metrics.get().and_then(std::sync::Weak::upgrade) {
                    recorder.quarantine();
                }
            }
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
            #[cfg(test)]
            release_receipt: Default::default(),
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
    #[cfg(test)]
    release_receipt: std::sync::OnceLock<std::sync::mpsc::SyncSender<()>>,
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
        #[cfg(test)]
        if let Some(receipt) = self.release_receipt.get() {
            let _ = receipt.try_send(());
        }
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
