use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::batch::{AllocationBudget, Batch, BatchProgress};
use crate::memory::MemoryRegistry;
use crate::notification::NotificationInbox;
use crate::runtime::control_plane::{self, ControlService};
use crate::transport::tcp::TcpTransport;
use crate::{
    EngineConfig, Error, HostMemory, Notification, NotificationFailure, NotificationStatus,
    Permission, RegisteredMemory, RemoteRegion, RemoteSegment, Result, SourceEpoch,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferOp {
    Read,
    Write,
}

/// Explicit byte transport; unsupported hints fail before batch I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transport {
    Tcp,
    Rdma,
    Nvlink,
}

/// TENT request priority; smaller discriminants are served first by RDMA workers.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum Priority {
    #[default]
    High = 0,
    Medium = 1,
    Low = 2,
}

/// Absolute host/device addresses within active local and epoch-fenced peer ranges.
/// Submission resolves and retains the local backing before any network I/O.
#[derive(Clone, Debug)]
pub struct TransferRequest {
    /// Local RDMA payload dispatch priority. TCP/NVLink require High.
    /// This does not reorder requests within a batch or preempt posted DMA.
    pub priority: Priority,
    /// Use Pin/Delegate/Unpin for the remote CUDA range over TCP or RDMA.
    /// The source must enable EngineConfig::remote_staging. False uses the
    /// selected transport's ordinary memory policy, including local staging.
    pub remote_staging: bool,
    pub transport: Transport,
    pub local_address: u64,
    pub remote_address: u64,
    pub length: usize,
}

#[derive(Clone)]
pub(crate) struct PreparedTransfer {
    #[cfg(feature = "rdma")]
    pub(crate) priority: Priority,
    #[cfg(feature = "cuda")]
    pub(crate) remote_staging: bool,
    pub(crate) transport: Transport,
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
    /// Consecutive requests sharing this failure, beginning at request_index.
    /// These may have executed; only requests after this span were skipped.
    pub affected_requests: usize,
    pub error: Error,
    pub remote_effect: RemoteEffect,
}

/// Requests run in order and stop on the first failure. Earlier completed
/// requests remain committed. A shared transport failure covers its explicit
/// affected span; only later requests were never attempted.
#[derive(Debug)]
pub struct BatchReport {
    pub completed_requests: usize,
    pub transferred_bytes: usize,
    pub failure: Option<TransferFailure>,
    pub notification: NotificationStatus,
    /// False means native work/resources are retained and free/drain must wait.
    pub locally_drained: bool,
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
    #[cfg(feature = "cuda")]
    pub nvlink: Option<crate::NVLinkStats>,
    #[cfg(feature = "cuda")]
    pub staging: crate::StagingStats,
    pub allocated_batches: usize,
    pub allocated_requests: usize,
    pub requester_workers: usize,
    #[cfg(feature = "rdma")]
    pub rdma: Option<crate::transport::rdma::RdmaStats>,
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

/// Engine, source registry and bounded control-service handler ownership.
pub struct TransferEngine {
    config: EngineConfig,
    source: Arc<Source>,
    control: ControlService,
    batches: Arc<RequesterAdmission>,
    allocations: Arc<AllocationBudget>,
}

impl TransferEngine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        config.validate()?;
        let epoch = SourceEpoch::generate()?;
        #[cfg(feature = "cuda")]
        let proxy = crate::runtime::proxy_manager::ProxyManager::new(
            config.staging_chunk_size,
            config.staging_chunk_count,
            config.staging_location,
        )?;
        #[cfg(feature = "rdma")]
        let rdma = config
            .rdma
            .clone()
            .map(|config| {
                crate::transport::rdma::RdmaTransport::new(
                    config,
                    #[cfg(feature = "cuda")]
                    Arc::clone(&proxy),
                )
            })
            .transpose()?;
        #[cfg(feature = "cuda")]
        let nvlink = config
            .nvlink
            .then(|| {
                crate::transport::nvlink::NVLinkTransport::new(
                    config.max_inflight_bytes,
                    config.nvlink_cache_entries,
                    config.nvlink_stream_count,
                )
            })
            .transpose()?;
        let source = Arc::new(Source {
            #[cfg(feature = "cuda")]
            remote_staging: config.remote_staging,
            #[cfg(feature = "cuda")]
            staging_pipeline_depth: config.staging_pipeline_depth,
            epoch,
            #[cfg(feature = "cuda")]
            proxy,
            #[cfg(feature = "cuda")]
            nvlink,
            #[cfg(feature = "rdma")]
            rdma,
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
        let control = ControlService::start(Arc::clone(&source), &config)?;
        Ok(Self {
            config,
            source,
            control,
            batches: Arc::default(),
            allocations: Arc::new(AllocationBudget::new()),
        })
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.control.endpoint()
    }
    pub fn source_epoch(&self) -> SourceEpoch {
        self.source.epoch
    }

    /// Snapshot of the selected RDMA topology; None before lazy RDMA initialization.
    /// This never probes devices or refreshes an existing context's route.
    #[cfg(feature = "rdma")]
    pub fn topology(&self) -> Result<Option<crate::Topology>> {
        self.source
            .rdma
            .as_ref()
            .map(|runtime| runtime.topology())
            .transpose()
            .map(Option::flatten)
    }

    /// Local payload bandwidth estimates for available NICs, without discovery.
    #[cfg(feature = "rdma")]
    pub fn nic_load_stats(&self) -> Result<Vec<crate::transport::rdma::NicLoadStats>> {
        self.source
            .rdma
            .as_ref()
            .map_or_else(|| Ok(Vec::new()), |runtime| runtime.nic_load_stats())
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
            #[cfg(feature = "cuda")]
            staging: self.source.proxy.stats()?,
            #[cfg(feature = "cuda")]
            nvlink: self
                .source
                .nvlink
                .as_ref()
                .map(|transport| transport.stats())
                .transpose()?,
            allocated_batches: allocated.0,
            allocated_requests: allocated.1,
            requester_workers: allocated.2,
            #[cfg(feature = "rdma")]
            rdma: self
                .source
                .rdma
                .as_ref()
                .map(|runtime| runtime.stats())
                .transpose()?,
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

    /// Registers owned host bytes, preparing configured host Direct RDMA MRs
    /// in every usable context, including paused ports, before exposing the range.
    pub fn register_memory(
        &self,
        bytes: Box<[u8]>,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        let _admission = self.source.admit()?;
        let length = bytes.len();
        self.source
            .registry
            .register_host_batch(
                &[(Arc::new(HostMemory::new(bytes)?), 0, length)],
                permission,
                |memory| self.register_native(memory),
            )?
            .pop()
            .ok_or(Error::Internal("single registration returned no handle"))
    }

    /// Registers an existing host allocation range. Exact duplicate ranges share
    /// a registration reference and retain the first permission, as in TENT.
    /// Overlapping ranges must share the same HostMemory access lock. Configured
    /// host Direct RDMA prepares usable contexts, including paused ports, before publication.
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
            .register_host_batch(&[(allocation, offset, length)], permission, |memory| {
                self.register_native(memory)
            })?
            .pop()
            .ok_or(Error::Internal("single registration returned no handle"))
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
        self.source
            .registry
            .register_host_batch(ranges, permission, |memory| self.register_native(memory))
    }

    fn register_native(&self, memory: &[RegisteredMemory]) -> Result<()> {
        #[cfg(feature = "rdma")]
        if let Some(runtime) = &self.source.rdma {
            return runtime.register_memory(memory, deadline(self.config.handler_timeout)?);
        }
        let _ = memory;
        Ok(())
    }

    /// Registers retained CUDA device ranges, using the same transactional
    /// registry and address lookup as host memory. Producers must hand off ready
    /// bytes; this profile synchronizes copies and restores the calling context.
    #[cfg(feature = "cuda")]
    pub fn register_cuda_memory_batch(
        &self,
        ranges: &[(Arc<crate::cuda::CudaMemory>, usize, usize)],
        permission: Permission,
    ) -> Result<Vec<RegisteredMemory>> {
        if ranges.len() > self.config.max_batch_requests {
            return Err(Error::ResourceExhausted);
        }
        let _admission = self.source.admit()?;
        self.source
            .registry
            .register_cuda_batch(ranges, permission, |memory| self.register_native(memory))
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
    /// Final unregister also retires a cached local RDMA MR. An idle MR is
    /// destroyed here; failure returns `Unquiesced` after lookup visibility has
    /// already been removed. Admitted borrowers finish retiring it on release.
    /// Call `close_source` to establish source-handler quiescence first.
    pub fn unregister_memory(&self, memory: &RegisteredMemory) -> Result<()> {
        self.source.registry.unregister(memory)?;
        #[cfg(feature = "rdma")]
        if let Some(runtime) = &self.source.rdma {
            runtime.registrations.retire(memory)?;
        }
        Ok(())
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
        control_plane::open_segment(endpoint, expected_epoch, deadline)
    }

    /// Validates the entire batch before sending any request. Admission and
    /// validation failures return `Err`; execution failures are in the report.
    /// Remote staging commits READ destinations one completed chunk at a time.
    /// Remote staging, Direct RDMA and CUDA IPC can change local READ or remote
    /// WRITE bytes before a final receipt, including on failure. Inspect locally_drained
    /// and remote_effect before releasing owners or considering a retry.
    /// WRITE success requires source acknowledgement. No automatic retries.
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
        #[cfg(feature = "rdma")]
        let mut rdma_ready = false;
        for request in requests {
            if request.priority != Priority::High && request.transport != Transport::Rdma {
                return Err(Error::Unavailable("request priority requires RDMA"));
            }
            if request.remote_staging && request.transport == Transport::Nvlink {
                return Err(Error::Unavailable("remote staging requires TCP or RDMA"));
            }
            #[cfg(not(feature = "cuda"))]
            if request.remote_staging {
                return Err(Error::Unavailable(
                    "remote staging requires --features cuda",
                ));
            }
            if request.transport == Transport::Rdma {
                #[cfg(feature = "rdma")]
                {
                    if !rdma_ready {
                        self.source
                            .rdma
                            .as_ref()
                            .ok_or(Error::Unavailable("RDMA is not configured"))?
                            .check_ready()?;
                        rdma_ready = true;
                    }
                }
                #[cfg(not(feature = "rdma"))]
                return Err(Error::Unavailable("build with --features rdma"));
            }
            if request.length > self.config.max_transfer_bytes {
                return Err(Error::ResourceExhausted);
            }
            let (local, local_offset) = self
                .source
                .registry
                .resolve_address(request.local_address, request.length)?;
            if request.transport == Transport::Nvlink {
                #[cfg(feature = "cuda")]
                {
                    self.source
                        .nvlink
                        .as_ref()
                        .ok_or(Error::Unavailable("NVLink is not configured"))?;
                    local
                        .allocation_range(local_offset, request.length)?
                        .0
                        .ipc_context(deadline)?;
                }
                #[cfg(not(feature = "cuda"))]
                return Err(Error::Unavailable("build with --features cuda"));
            }
            let (remote, remote_offset) =
                peer.resolve_address(request.remote_address, request.length)?;
            if !remote.permission.allows(operation == TransferOp::Write) {
                return Err(Error::PermissionDenied);
            }
            total_bytes = total_bytes
                .checked_add(request.length)
                .ok_or(Error::ResourceExhausted)?;
            prepared.push(PreparedTransfer {
                #[cfg(feature = "rdma")]
                priority: request.priority,
                #[cfg(feature = "cuda")]
                remote_staging: request.remote_staging,
                transport: request.transport,
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
        let report = self.execute(&prepared, None);
        if !report.locally_drained {
            // Preserve actual registrations and admission, not a pointer surrogate.
            std::mem::forget((prepared, Arc::clone(&self.source)));
        }
        Ok(report)
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
            locally_drained: true,
            notification: if notification.is_some() {
                NotificationStatus::Skipped
            } else {
                NotificationStatus::NotRequested
            },
        };
        let mut index = 0;
        while index < requests.len() {
            let request = &requests[index];
            if let Some((progress, first_task)) = progress {
                if !progress.begin_task(first_task + index) {
                    report.failure = Some(TransferFailure {
                        request_index: index,
                        affected_requests: 1,
                        error: Error::Cancelled,
                        remote_effect: RemoteEffect::None,
                    });
                    break;
                }
            }
            let single = |result: std::result::Result<(), (Error, RemoteEffect)>| {
                result
                    .map(|()| 1)
                    .map_err(|(error, remote_effect)| TransferFailure {
                        request_index: 0,
                        affected_requests: 1,
                        error,
                        remote_effect,
                    })
            };
            let result = match request.transport {
                #[cfg(feature = "cuda")]
                Transport::Tcp | Transport::Rdma if request.remote_staging => {
                    single(self.source.proxy.transfer_remote(
                        *operation,
                        peer,
                        request,
                        &self.source,
                        *deadline,
                    ))
                }
                Transport::Tcp => single(TcpTransport::transfer(
                    *operation,
                    peer,
                    request,
                    *deadline,
                    #[cfg(feature = "cuda")]
                    &self.source.proxy,
                )),
                Transport::Nvlink => {
                    #[cfg(feature = "cuda")]
                    {
                        match &self.source.nvlink {
                            Some(transport) => transport.transfer(
                                submission,
                                index,
                                self.source.epoch,
                                self.config.max_transfer_bytes,
                                |next| {
                                    progress.is_none_or(|(progress, first_task)| {
                                        progress.begin_task(first_task + index + next)
                                    })
                                },
                            ),
                            None => single(Err((
                                Error::Unavailable("NVLink is not configured"),
                                RemoteEffect::None,
                            ))),
                        }
                    }
                    #[cfg(not(feature = "cuda"))]
                    {
                        single(Err((
                            Error::Unavailable("build with --features cuda"),
                            RemoteEffect::None,
                        )))
                    }
                }
                Transport::Rdma => {
                    #[cfg(feature = "rdma")]
                    {
                        single(match &self.source.rdma {
                            Some(transport) => transport.transfer(
                                *operation,
                                peer,
                                request,
                                self.source.epoch,
                                *deadline,
                            ),
                            None => Err((
                                Error::Unavailable("RDMA is not configured"),
                                RemoteEffect::None,
                            )),
                        })
                    }
                    #[cfg(not(feature = "rdma"))]
                    {
                        single(Err((
                            Error::Unavailable("build with --features rdma"),
                            RemoteEffect::None,
                        )))
                    }
                }
            };
            let completed = match &result {
                Ok(count) => *count,
                Err(failure) => failure.request_index,
            };
            for (next, request) in requests[index..index + completed].iter().enumerate() {
                report.completed_requests += 1;
                report.transferred_bytes += request.length;
                if let Some((progress, first_task)) = progress {
                    progress.complete_task(first_task + index + next, request.length);
                }
            }
            if let Err(mut failure) = result {
                report.locally_drained = !matches!(failure.error, Error::Unquiesced { .. });
                failure.request_index += index;
                report.failure = Some(failure);
                break;
            }
            index += completed;
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
            control_plane::notify(peer, notification, deadline)
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
        self.close_source_until(deadline)
    }

    fn close_source_until(&self, deadline: Instant) -> Result<()> {
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
        drop(admission);
        #[cfg(feature = "cuda")]
        if let Some(transport) = &self.source.nvlink {
            transport.exports.wait_empty(deadline)?;
        }
        Ok(())
    }

    /// Closes cached CUDA IPC imports captured at this call without fencing new
    /// submissions. Each captured key waits for its current borrower, bounded
    /// by the deadline; concurrent submissions can refill the cache. Stop
    /// submitting first when a completely empty cache is required. Failures
    /// retain close obligations, and a later flush retries only safe receipts.
    /// Native close cannot be preempted. Disabled caching is a no-op.
    #[cfg(feature = "cuda")]
    pub fn flush_nvlink_cache(&self, timeout: Duration) -> Result<()> {
        if let Some(transport) = &self.source.nvlink {
            transport.close_imports(self.source.epoch, deadline(timeout)?)?;
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
        self.close_requester_until(deadline)
    }

    fn close_requester_until(&self, deadline: Instant) -> Result<()> {
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
        drop(usage);
        #[cfg(feature = "cuda")]
        if let Some(transport) = &self.source.nvlink {
            transport.close_requester(self.source.epoch, deadline)?;
        }
        Ok(())
    }

    /// Fences both directions before waiting for accepted source and requester
    /// operations, then explicitly closes the shared RDMA context/PD if enabled.
    /// Timeout leaves both admissions closed and permits another drain wait.
    /// Native teardown failure retains its owner and is never retried, including
    /// by Drop or another close caller. The deadline bounds drain waits, not a
    /// native provider call that cannot be preempted.
    pub fn close(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))?;
        {
            // Every path taking both locks uses source before requester.
            let mut source = self
                .source
                .admission
                .lock()
                .map_err(|_| Error::Internal("source lock poisoned"))?;
            let mut requester = self
                .batches
                .usage
                .lock()
                .map_err(|_| Error::Internal("batch lock poisoned"))?;
            source.open = false;
            requester.open = false;
        }
        // Peers may export to each other. Release outgoing imports before
        // waiting for incoming exports, keeping close receipts serviceable.
        self.close_requester_until(deadline)?;
        self.close_source_until(deadline)?;
        #[cfg(feature = "cuda")]
        self.source.proxy.close(deadline)?;
        #[cfg(feature = "rdma")]
        if let Some(runtime) = &self.source.rdma {
            runtime.close(deadline)?;
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
    #[cfg(feature = "cuda")]
    pub(crate) remote_staging: bool,
    #[cfg(feature = "cuda")]
    pub(crate) staging_pipeline_depth: usize,
    #[cfg(feature = "cuda")]
    pub(crate) nvlink: Option<crate::transport::nvlink::NVLinkTransport>,
    #[cfg(feature = "cuda")]
    pub(crate) proxy: Arc<crate::runtime::proxy_manager::ProxyManager>,
    pub(crate) epoch: SourceEpoch,
    pub(crate) registry: MemoryRegistry,
    pub(crate) notifications: NotificationInbox,
    #[cfg(feature = "rdma")]
    pub(crate) rdma: Option<Arc<crate::transport::rdma::RdmaTransport>>,
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
    pub(crate) deadline: Instant,
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
pub(crate) mod tests;
