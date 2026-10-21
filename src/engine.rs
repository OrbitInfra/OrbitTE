use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::batch::{AllocationBudget, Batch, BatchProgress};
use crate::memory::MemoryRegistry;
use crate::notification::NotificationInbox;
use crate::tcp::{self, TcpTransport};
use crate::{
    EngineConfig, Error, HostMemory, Notification, NotificationFailure, NotificationStatus,
    Permission, RegisteredMemory, RemoteRegion, RemoteSegment, Result, SourceEpoch,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferOp {
    Read,
    Write,
}

/// Absolute host addresses within active local and epoch-fenced peer ranges.
/// Submission resolves and retains the local backing before any network I/O.
#[derive(Clone, Debug)]
pub struct TransferRequest {
    pub local_address: u64,
    pub remote_address: u64,
    pub length: usize,
}

pub(crate) struct PreparedTransfer {
    pub(crate) local: RegisteredMemory,
    pub(crate) local_offset: usize,
    pub(crate) remote: RemoteRegion,
    pub(crate) remote_offset: usize,
    pub(crate) length: usize,
}

/// Whether the failed request may have modified remote memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteEffect {
    None,
    /// A WRITE body was attempted but its final acknowledgement was not received.
    /// Do not automatically replay it or treat a timeout as remote quiescence.
    Unknown,
}

#[derive(Debug)]
pub struct TransferFailure {
    pub request_index: usize,
    pub error: Error,
    pub remote_effect: RemoteEffect,
}

/// Requests run in order and stop on the first failure. Earlier completed
/// requests remain committed; later requests were never attempted.
#[derive(Debug)]
pub struct BatchReport {
    pub completed_requests: usize,
    pub transferred_bytes: usize,
    pub failure: Option<TransferFailure>,
    pub notification: NotificationStatus,
}

impl BatchReport {
    /// Whether all data transfers completed; inspect `notification` separately.
    pub fn is_complete(&self) -> bool {
        self.failure.is_none()
    }
}

/// Independently sampled resource counters, including unregistered allocations
/// still retained by local handles or admitted handlers. Not a global snapshot.
#[derive(Clone, Copy, Debug)]
pub struct EngineStats {
    pub allocated_batches: usize,
    pub allocated_requests: usize,
    pub requester_workers: usize,
    pub retained_regions: usize,
    pub retained_bytes: usize,
    pub active_source_operations: usize,
    pub inflight_batches: usize,
    pub inflight_bytes: usize,
    pub queued_notifications: usize,
    /// Queued messages plus incoming bodies that reserved inbox capacity.
    pub retained_notifications: usize,
    pub retained_notification_bytes: usize,
}

/// Engine, source registry and bounded TCP handler ownership.
pub struct TransferEngine {
    config: EngineConfig,
    source: Arc<Source>,
    transport: TcpTransport,
    batches: Arc<RequesterAdmission>,
    allocations: Arc<AllocationBudget>,
}

impl TransferEngine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        config.validate()?;
        let epoch = SourceEpoch::generate()?;
        let source = Arc::new(Source {
            epoch,
            registry: MemoryRegistry::new(
                epoch,
                config.max_registered_regions,
                config.max_registered_bytes,
            ),
            notifications: NotificationInbox::new(
                config.max_pending_notifications,
                config.max_pending_notification_bytes,
                config.max_notification_bytes,
            ),
            admission: Mutex::new(SourceAdmission {
                open: true,
                active: 0,
            }),
            drained: Condvar::new(),
        });
        let transport = TcpTransport::start(Arc::clone(&source), &config)?;
        Ok(Self {
            config,
            source,
            transport,
            batches: Arc::default(),
            allocations: Arc::new(AllocationBudget::new()),
        })
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.transport.endpoint()
    }
    pub fn source_epoch(&self) -> SourceEpoch {
        self.source.epoch
    }

    pub fn stats(&self) -> Result<EngineStats> {
        let (retained_regions, retained_bytes) = self.source.registry.usage()?;
        let (queued_notifications, retained_notifications, retained_notification_bytes) =
            self.source.notifications.usage()?;
        let active_source_operations = self
            .source
            .admission
            .lock()
            .map_err(|_| Error::Internal("source lock poisoned"))?
            .active;
        let allocated = self.allocations.usage()?;
        let batches = self
            .batches
            .usage
            .lock()
            .map_err(|_| Error::Internal("batch lock poisoned"))?;
        Ok(EngineStats {
            allocated_batches: allocated.0,
            allocated_requests: allocated.1,
            requester_workers: allocated.2,
            retained_regions,
            retained_bytes,
            active_source_operations,
            inflight_batches: batches.count,
            inflight_bytes: batches.bytes,
            queued_notifications,
            retained_notifications,
            retained_notification_bytes,
        })
    }

    pub fn register_memory(
        &self,
        bytes: Box<[u8]>,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        let _admission = self.source.admit()?;
        self.source.registry.register(bytes, permission)
    }

    /// Registers an existing host allocation range. Exact duplicate ranges share
    /// a registration reference and retain the first permission, as in TENT.
    /// Overlapping ranges must share the same HostMemory access lock.
    pub fn register_host_memory(
        &self,
        allocation: Arc<HostMemory>,
        offset: usize,
        length: usize,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        let _admission = self.source.admit()?;
        self.source
            .registry
            .register_host(allocation, offset, length, permission)
    }

    /// Registers `(allocation, offset, length)` ranges in input order. Admission
    /// is atomic: failure publishes no ranges or duplicate references. Each
    /// element adds one explicit reference; exact duplicates keep the first
    /// permission and share one retained-range charge.
    pub fn register_host_memory_batch(
        &self,
        ranges: &[(Arc<HostMemory>, usize, usize)],
        permission: Permission,
    ) -> Result<Vec<RegisteredMemory>> {
        if ranges.len() > self.config.max_batch_requests {
            return Err(Error::ResourceExhausted);
        }
        let _admission = self.source.admit()?;
        self.source.registry.register_host_batch(ranges, permission)
    }

    /// Resolves an absolute local address to an owned registered range and offset.
    /// Matches TENT's address order, preferring the longer range at equal bases.
    pub fn resolve_local_address(
        &self,
        address: u64,
        length: usize,
    ) -> Result<(RegisteredMemory, usize)> {
        self.source.registry.resolve_address(address, length)
    }

    /// Removes one explicit registration reference; the final reference removes
    /// visibility for new remote requests. Already admitted requests and
    /// local handles keep the allocation and its budget charge until last use.
    /// Call `close_source` to establish source-handler quiescence first.
    pub fn unregister_memory(&self, memory: &RegisteredMemory) -> Result<()> {
        self.source.registry.unregister(memory)
    }

    /// Open/refresh using an epoch supplied by the authoritative source owner.
    /// Discovery and a newly responding process cannot silently replace it.
    pub fn open_segment(
        &self,
        endpoint: SocketAddr,
        expected_epoch: SourceEpoch,
        timeout: Duration,
    ) -> Result<RemoteSegment> {
        let deadline = deadline(timeout)?;
        let _admission = self.admit_batch(0)?;
        tcp::open_segment(endpoint, expected_epoch, deadline)
    }

    /// Validates the entire batch before sending any request. Admission and
    /// validation failures return `Err`; execution failures are in the report.
    /// READ commits each local slice only after its full response arrives.
    /// WRITE uses a final source acknowledgement and never retries automatically.
    pub fn submit_and_wait(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[TransferRequest],
        timeout: Duration,
    ) -> Result<BatchReport> {
        self.submit(operation, peer, requests, None, timeout)
    }

    /// Sends one notification to this peer after every request completes.
    /// Empty or failed batches do not notify. Data and notification completion
    /// share one deadline, but their results are reported separately.
    pub fn submit_and_notify(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[TransferRequest],
        notification: &Notification,
        timeout: Duration,
    ) -> Result<BatchReport> {
        self.submit(operation, peer, requests, Some(notification), timeout)
    }

    pub(crate) fn prepare(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
        timeout: Duration,
    ) -> Result<PreparedSubmission> {
        if requests.len() > self.config.max_batch_requests {
            return Err(Error::ResourceExhausted);
        }
        let deadline = deadline(timeout)?;
        let mut total_bytes = match notification {
            Some(notification) => self.validate_notification(notification)?,
            None => 0,
        };
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(requests.len())
            .map_err(|_| Error::ResourceExhausted)?;
        for request in requests {
            if request.length > self.config.max_transfer_bytes {
                return Err(Error::ResourceExhausted);
            }
            let (local, local_offset) = self
                .source
                .registry
                .resolve_address(request.local_address, request.length)?;
            let (remote, remote_offset) =
                peer.resolve_address(request.remote_address, request.length)?;
            if !remote.permission.allows(operation == TransferOp::Write) {
                return Err(Error::PermissionDenied);
            }
            total_bytes = total_bytes
                .checked_add(request.length)
                .ok_or(Error::ResourceExhausted)?;
            prepared.push(PreparedTransfer {
                local,
                local_offset,
                remote,
                remote_offset,
                length: request.length,
            });
        }
        let admission = self.admit_batch(total_bytes)?;
        Ok(PreparedSubmission {
            operation,
            peer: peer.clone(),
            requests: prepared,
            notification: notification.cloned(),
            deadline,
            _admission: admission,
        })
    }

    fn submit(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
        timeout: Duration,
    ) -> Result<BatchReport> {
        let prepared = self.prepare(operation, peer, requests, notification, timeout)?;
        Ok(self.execute(&prepared, None))
    }

    pub(crate) fn execute(
        &self,
        submission: &PreparedSubmission,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let PreparedSubmission {
            operation,
            peer,
            requests,
            notification,
            deadline,
            ..
        } = submission;

        let mut report = BatchReport {
            completed_requests: 0,
            transferred_bytes: 0,
            failure: None,
            notification: if notification.is_some() {
                NotificationStatus::Skipped
            } else {
                NotificationStatus::NotRequested
            },
        };
        for (index, request) in requests.iter().enumerate() {
            if let Some((progress, first_task)) = progress {
                if !progress.begin_task(first_task + index) {
                    report.failure = Some(TransferFailure {
                        request_index: index,
                        error: Error::Cancelled,
                        remote_effect: RemoteEffect::None,
                    });
                    break;
                }
            }
            if let Err((error, remote_effect)) = tcp::transfer(*operation, peer, request, *deadline)
            {
                report.failure = Some(TransferFailure {
                    request_index: index,
                    error,
                    remote_effect,
                });
                break;
            }
            report.completed_requests += 1;
            report.transferred_bytes += request.length;
            if let Some((progress, first_task)) = progress {
                progress.complete_task(first_task + index, request.length);
            }
        }
        if report.is_complete() && !requests.is_empty() {
            if let Some(notification) = notification {
                report.notification = match self.notify(peer, notification, *deadline) {
                    Ok(()) => NotificationStatus::Delivered,
                    Err(failure) => NotificationStatus::Failed(failure),
                };
            }
        }
        report
    }

    /// Allocates a bounded batch and retains this engine until explicit free or
    /// the final detached worker exits. Submission reserves actual backing.
    pub fn allocate_batch(self: &Arc<Self>, capacity: usize) -> Result<Batch> {
        let _admission = self.admit_batch(0)?;
        Batch::allocate(
            Arc::clone(self),
            Arc::clone(&self.allocations),
            capacity,
            self.config.max_batch_requests,
            self.config.max_allocated_batches,
            self.config.max_allocated_requests,
        )
    }

    /// Creates and seals a single asynchronous submission. Polling and waiting
    /// observe worker progress; a dropped handle cannot release active owners.
    pub fn submit_async(
        self: &Arc<Self>,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
        timeout: Duration,
    ) -> Result<Batch> {
        let mut batch = self.allocate_batch(requests.len().max(1))?;
        batch.submit(operation, peer, requests, notification, timeout)?;
        batch.seal()?;
        Ok(batch)
    }

    /// Delivers to this engine's inbox without requiring a transport connection.
    pub fn send_local_notification(&self, notification: &Notification) -> Result<()> {
        let _admission = self.source.admit()?;
        self.source.notifications.push(notification)
    }

    /// A successful receipt means enqueued, not application-consumed. An unknown
    /// outcome must not be automatically retried; deliberate repeated messages
    /// are distinct deliveries, as in TENT.
    pub fn send_notification(
        &self,
        peer: &RemoteSegment,
        notification: &Notification,
        timeout: Duration,
    ) -> std::result::Result<(), NotificationFailure> {
        let deadline = deadline(timeout)?;
        let bytes = self.validate_notification(notification)?;
        let _admission = self.admit_batch(bytes)?;
        self.notify(peer, notification, deadline)
    }

    /// Atomically drains this engine's shared inbox in enqueue order. An empty
    /// poll returns an empty vector; concurrent receivers never share a message.
    /// Already queued messages remain drainable after source admission closes.
    pub fn receive_notifications(&self) -> Result<Vec<Notification>> {
        self.source.notifications.receive()
    }

    fn validate_notification(&self, notification: &Notification) -> Result<usize> {
        let bytes = notification.byte_len()?;
        if bytes > self.config.max_notification_bytes {
            return Err(Error::ResourceExhausted);
        }
        Ok(bytes)
    }

    fn notify(
        &self,
        peer: &RemoteSegment,
        notification: &Notification,
        deadline: Instant,
    ) -> std::result::Result<(), NotificationFailure> {
        remaining(deadline)?;
        if peer.source_epoch() == self.source_epoch() && peer.endpoint() == self.endpoint() {
            self.send_local_notification(notification)
                .map_err(Into::into)
        } else {
            tcp::notify(peer, notification, deadline)
        }
    }

    /// Closes source admission permanently, then waits for handlers already
    /// admitted to release backing references. Timeout leaves admission closed;
    /// a later call may wait again. It does not claim that a remote requester
    /// received a WRITE acknowledgement. Requester operations remain available.
    pub fn close_source(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))?;
        let mut admission = self
            .source
            .admission
            .lock()
            .map_err(|_| Error::Internal("source lock poisoned"))?;
        admission.open = false;
        while admission.active != 0 {
            let remaining = remaining(deadline)?;
            let (guard, _) = self
                .source
                .drained
                .wait_timeout(admission, remaining)
                .map_err(|_| Error::Internal("source lock poisoned"))?;
            admission = guard;
        }
        Ok(())
    }

    /// Permanently closes outgoing admission and waits for accepted calls,
    /// including queued async work, to release physical owners. Timeout leaves
    /// admission closed and permits another wait. Source admission is separate.
    pub fn close_requester(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))?;
        let mut usage = self
            .batches
            .usage
            .lock()
            .map_err(|_| Error::Internal("batch lock poisoned"))?;
        usage.open = false;
        while usage.count != 0 {
            let duration = remaining(deadline)?;
            usage = self
                .batches
                .drained
                .wait_timeout(usage, duration)
                .map_err(|_| Error::Internal("batch lock poisoned"))?
                .0;
        }
        Ok(())
    }

    fn admit_batch(&self, bytes: usize) -> Result<BatchPermit> {
        let mut usage = self
            .batches
            .usage
            .lock()
            .map_err(|_| Error::Internal("batch lock poisoned"))?;
        if !usage.open {
            return Err(Error::RequesterClosed);
        }
        let new_bytes = usage
            .bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceExhausted)?;
        if usage.count >= self.config.max_inflight_batches
            || new_bytes > self.config.max_inflight_bytes
        {
            return Err(Error::ResourceExhausted);
        }
        usage.count += 1;
        usage.bytes = new_bytes;
        Ok(BatchPermit {
            batches: Arc::clone(&self.batches),
            bytes,
        })
    }
}

pub(crate) fn deadline(timeout: Duration) -> Result<Instant> {
    if timeout.is_zero() {
        return Err(Error::Timeout);
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))
}

pub(crate) fn remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::Timeout);
    }
    Ok(remaining)
}

pub(crate) struct Source {
    pub(crate) epoch: SourceEpoch,
    pub(crate) registry: MemoryRegistry,
    pub(crate) notifications: NotificationInbox,
    admission: Mutex<SourceAdmission>,
    drained: Condvar,
}

struct SourceAdmission {
    open: bool,
    active: usize,
}

impl Source {
    pub(crate) fn admit(&self) -> Result<SourcePermit<'_>> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| Error::Internal("source lock poisoned"))?;
        if !admission.open {
            return Err(Error::SourceClosed);
        }
        admission.active += 1;
        Ok(SourcePermit(self))
    }
}

pub(crate) struct SourcePermit<'a>(&'a Source);

impl Drop for SourcePermit<'_> {
    fn drop(&mut self) {
        let mut admission = self
            .0
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        admission.active -= 1;
        if admission.active == 0 {
            self.0.drained.notify_all();
        }
    }
}

pub(crate) struct PreparedSubmission {
    pub(crate) operation: TransferOp,
    pub(crate) peer: RemoteSegment,
    pub(crate) requests: Vec<PreparedTransfer>,
    pub(crate) notification: Option<Notification>,
    deadline: Instant,
    _admission: BatchPermit,
}

struct BatchUsage {
    open: bool,
    count: usize,
    bytes: usize,
}
impl Default for BatchUsage {
    fn default() -> Self {
        Self {
            open: true,
            count: 0,
            bytes: 0,
        }
    }
}
#[derive(Default)]
struct RequesterAdmission {
    usage: Mutex<BatchUsage>,
    drained: Condvar,
}
struct BatchPermit {
    batches: Arc<RequesterAdmission>,
    bytes: usize,
}
impl Drop for BatchPermit {
    fn drop(&mut self) {
        let mut usage = self
            .batches
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.count -= 1;
        usage.bytes -= self.bytes;
        if usage.count == 0 {
            self.batches.drained.notify_all();
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/engine.rs"]
mod tests;
