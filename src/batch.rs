use std::ops::Range;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::engine::{PreparedSubmission, deadline, remaining};
use crate::{
    BatchReport, Error, Notification, NotificationStatus, RemoteEffect, RemoteSegment, Result,
    TransferEngine, TransferFailure, TransferOp, TransferRequest,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BatchId(u64);
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
    sender: Option<mpsc::SyncSender<QueuedSubmission>>,
    receiver: Option<mpsc::Receiver<QueuedSubmission>>,
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
        let reservation = budget.reserve(capacity, max_batches, max_capacity)?;
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut submissions = Vec::new();
        submissions
            .try_reserve_exact(capacity)
            .map_err(|_| Error::ResourceExhausted)?;
        let (sender, receiver) = mpsc::sync_channel(capacity);
        Ok(Self {
            engine: Some(engine),
            progress: Some(Arc::new(BatchProgress {
                reservation,
                state: Mutex::new(ProgressState {
                    tasks,
                    submissions,
                    sealed: false,
                    pending: 0,
                    worker_exited: false,
                }),
                changed: Condvar::new(),
            })),
            sender: Some(sender),
            receiver: Some(receiver),
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
    pub fn submit(
        &mut self,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
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
        let prepared = engine.prepare(operation, peer, requests, notification, timeout)?;
        if self.worker.is_none() {
            let receiver = self.receiver.take().ok_or(Error::BatchClosed)?;
            let engine = Arc::clone(&engine);
            let state = Arc::clone(&progress);
            let token = progress.reservation.budget.worker()?;
            self.worker = Some(thread::Builder::new().name("orbitte-batch".into()).spawn(
                move || {
                    let _exit = WorkerExit {
                        progress: Arc::clone(&state),
                        _token: token,
                    };
                    // Drop queued owners before WorkerExit publishes quiescence,
                    // including during unwinding from an unexpected worker panic.
                    let receiver = receiver;
                    for queued in receiver {
                        let QueuedSubmission {
                            first_task,
                            submission,
                            prepared,
                        } = queued;
                        let report = engine.execute(&prepared, Some((&state, first_task)));
                        if report.locally_drained {
                            drop(prepared);
                        } else {
                            // Quarantine real owners and capacity on unproven native
                            // termination. Logical failure does not authorize free.
                            std::mem::forget((prepared, Arc::clone(&engine), Arc::clone(&state)));
                        }
                        state.finish_submission(submission, first_task, report);
                    }
                },
            )?);
        }
        let (range, submission) = {
            let mut state = progress.lock();
            if state.worker_exited {
                return Err(Error::BatchClosed);
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
            (range, submission)
        };
        let queued = QueuedSubmission {
            first_task: range.start,
            submission,
            prepared,
        };
        match self
            .sender
            .as_ref()
            .ok_or(Error::BatchClosed)?
            .try_send(queued)
        {
            Ok(()) => Ok(range),
            Err(error) => {
                // This group never became visible to the worker. Drop its real
                // owners before restoring its public task/capacity reservation.
                let failed = match error {
                    mpsc::TrySendError::Full(queued) => (queued, Error::ResourceExhausted),
                    mpsc::TrySendError::Disconnected(queued) => (queued, Error::BatchClosed),
                };
                drop(failed.0);
                let mut state = progress.lock();
                state.tasks.truncate(range.start);
                state.submissions.truncate(submission);
                // WorkerExit may already have accounted for a disconnected job.
                if !state.worker_exited {
                    state.pending -= 1;
                }
                progress.changed.notify_all();
                Err(failed.1)
            }
        }
    }

    /// Stops appending. A terminal failure can retain unquiesced native work;
    /// inspect locally_drained before free. An empty sealed batch completes.
    pub fn seal(&mut self) -> Result<()> {
        let progress = self.progress()?;
        progress.lock().sealed = true;
        progress.changed.notify_all();
        self.sender.take();
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
        Ok(true)
    }

    pub fn cancel_all(&self) -> Result<()> {
        let mut state = self.progress()?.lock();
        for task in &mut state.tasks {
            if !task.state.is_terminal() {
                task.cancel_requested = true;
            }
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

    /// Explicit free rejects pending work and preserves the handle for retry.
    /// Dropping a pending handle instead leaves its worker/reservations alive.
    pub fn free(&mut self) -> Result<()> {
        if !self.status()?.locally_drained {
            return Err(Error::BatchBusy);
        }
        self.seal()?;
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        self.progress.take();
        self.engine.take();
        Ok(())
    }
}

struct QueuedSubmission {
    first_task: usize,
    submission: usize,
    prepared: PreparedSubmission,
}

pub(crate) struct BatchProgress {
    reservation: Reservation,
    state: Mutex<ProgressState>,
    changed: Condvar,
}
struct ProgressState {
    tasks: Vec<TaskStatus>,
    submissions: Vec<SubmissionStatus>,
    sealed: bool,
    pending: usize,
    worker_exited: bool,
}
impl ProgressState {
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
    fn lock(&self) -> MutexGuard<'_, ProgressState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
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
    pub(crate) fn complete_task(&self, index: usize, length: usize) {
        let mut state = self.lock();
        state.tasks[index].state = TaskState::Completed;
        state.tasks[index].transferred_bytes = length;
        state.tasks[index].locally_drained = true;
        self.changed.notify_all();
    }
    fn finish_submission(&self, index: usize, first: usize, report: BatchReport) {
        let mut state = self.lock();
        let failure = report.failure.map(|mut failure| {
            failure.request_index += first;
            Arc::new(failure)
        });
        let range = state.submissions[index].tasks.clone();
        if let Some(failure) = &failure {
            for task_index in failure.request_index..range.end {
                let task = &mut state.tasks[task_index];
                task.state = if task_index == failure.request_index {
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
                task.locally_drained =
                    task_index != failure.request_index || report.locally_drained;
                if task_index == failure.request_index {
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

struct WorkerExit {
    progress: Arc<BatchProgress>,
    _token: WorkerToken,
}
impl Drop for WorkerExit {
    fn drop(&mut self) {
        let mut state = self.progress.lock();
        state.worker_exited = true;
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
    }
}

pub(crate) struct AllocationBudget {
    state: Mutex<Allocated>,
}
struct Allocated {
    next_id: u64,
    count: usize,
    capacity: usize,
    workers: usize,
}
impl AllocationBudget {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(Allocated {
                next_id: 1,
                count: 0,
                capacity: 0,
                workers: 0,
            }),
        }
    }
    pub(crate) fn usage(&self) -> Result<(usize, usize, usize)> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("batch allocation lock poisoned"))?;
        Ok((state.count, state.capacity, state.workers))
    }
    fn reserve(
        self: &Arc<Self>,
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
        let id = BatchId(state.next_id);
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
