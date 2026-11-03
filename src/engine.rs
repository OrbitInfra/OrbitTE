use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::batch::{AllocationBudget, Batch, BatchId, BatchProgress};
use crate::memory::MemoryRegistry;
use crate::notification::NotificationInbox;
use crate::runtime::admission_queue::{
    LocalTransferAdmissionQueue, QueuePermit, RuntimeQueueStats,
};
use crate::runtime::control_plane::{self, ControlService};
use crate::runtime::qos_contract::{QosCharge, QosContractResolver, QosPermit};
use crate::runtime::receiver_credit::{CreditPermit, ReceiverCreditIssuer, SenderCreditLedger};
#[cfg(any(feature = "io-uring", feature = "gds"))]
use crate::runtime::transport_selector::Filters;
use crate::runtime::transport_selector::{
    SelectionContext, SelectionResult, TransportSelector, qp_pools::QpLayout,
};
use crate::segment::{FileSegments, SegmentMetadata};
use crate::transport::tcp::TcpTransport;
use crate::{
    CreditUpdateDisposition, EngineConfig, Error, FileSegment, FileSegmentInfo, HostMemory,
    Notification, NotificationFailure, NotificationStatus, Permission, ReceiverCreditStats,
    ReceiverCreditUpdateV1, RegisteredMemory, RemoteRegion, RemoteSegment, Result, SourceEpoch,
    TransferTarget,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferOp {
    Read,
    Write,
}

/// Byte transport or policy selection. Explicit hints never silently downgrade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Transport {
    Tcp,
    Rdma,
    Nvlink,
    /// Linux io_uring for local regular files and registered memory.
    IoUring,
    /// Local cuFile batch I/O.
    Gds,
    /// Choose an eligible transport before any payload I/O. No failure replay.
    Unspec,
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

/// A registered local address and a peer address or zero-based file byte offset.
/// Submission resolves and retains the local backing before any network I/O.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TransferRequest {
    /// Bind to exactly this policy and segment type. Resolved during preparation.
    pub policy_name: Option<crate::PolicyName>,
    /// Business intent for automatic policy selection, independent of QoS.
    pub intent: crate::IntentType,
    /// Pre-resolved engine-bound contract. None uses the engine's default/unspec
    /// contract, with `priority` as its fallback. Some captures its own requested
    /// priority at resolution and replaces this request's legacy priority.
    pub qos: Option<crate::EffectiveQosPolicy>,
    /// Priority for transport policy matching and RDMA payload dispatch.
    /// TCP and NVLink accept all priorities and treat them as advisory.
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

impl TransferRequest {
    /// Request an ordinary transfer at the default priority without a QoS override.
    pub fn new(
        transport: Transport,
        local_address: u64,
        remote_address: u64,
        length: usize,
    ) -> Self {
        Self {
            transport,
            local_address,
            remote_address,
            length,
            policy_name: None,
            intent: crate::IntentType::Unspec,
            qos: None,
            priority: Priority::default(),
            remote_staging: false,
        }
    }

    /// Bind to a named selector rule. Allocate the bounded name at request construction.
    pub fn with_policy_name(mut self, name: impl AsRef<str>) -> Result<Self> {
        self.policy_name = Some(crate::PolicyName::new(name.as_ref())?);
        Ok(self)
    }

    #[must_use]
    pub fn with_intent(mut self, intent: crate::IntentType) -> Self {
        self.intent = intent;
        self
    }

    /// Use an engine-resolved policy, or clear it to use the default contract.
    #[must_use]
    pub fn with_qos(mut self, policy: impl Into<Option<crate::EffectiveQosPolicy>>) -> Self {
        self.qos = policy.into();
        self
    }

    /// Set the requested priority used in the absence of a QoS override.
    #[must_use]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Select whether to stage the remote CUDA range over TCP or RDMA.
    #[must_use]
    pub fn with_remote_staging(mut self, enabled: bool) -> Self {
        self.remote_staging = enabled;
        self
    }
}

#[derive(Clone)]
pub(crate) struct PreparedTransfer {
    #[cfg(feature = "rdma")]
    pub(crate) priority: Priority,
    #[cfg(feature = "cuda")]
    pub(crate) remote_staging: bool,
    pub(crate) selection: SelectionResult,
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
    /// Consecutive requests affected by the failure, beginning at request_index.
    /// For concurrent file I/O the span includes later requests submitted to the kernel.
    pub affected_requests: usize,
    pub error: Error,
    pub remote_effect: RemoteEffect,
}

/// Counts and bytes cover the consecutive fully successful requests before the
/// first failure. A later file SQE may have completed inside the affected span.
/// Async task status reports its individual result and exact transferred bytes.
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
    /// Device name entries missing or outside the policy mask at construction.
    pub unresolved_policy_devices: usize,
    #[cfg(feature = "gds")]
    pub gds: crate::GdsStats,
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
    /// Whole-submission dispatch reservations; None when the queue is disabled.
    pub runtime_queue: Option<RuntimeQueueStats>,
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
    runtime_queue: Option<Arc<LocalTransferAdmissionQueue>>,
    receiver_credits: Option<Arc<SenderCreditLedger>>,
    qos: Option<QosContractResolver>,
    selector: Option<TransportSelector>,
    pub(crate) files: FileSegments,
    #[cfg(feature = "io-uring")]
    pub(crate) io_uring: Option<crate::transport::io_uring::IoUringTransport>,
    #[cfg(feature = "gds")]
    pub(crate) gds: Option<crate::transport::gds::GdsTransport>,
}

impl TransferEngine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        config.validate()?;

        let runtime_queue = config.runtime_queue.map(LocalTransferAdmissionQueue::new);
        let qos = config
            .qos
            .as_ref()
            .map(|qos| QosContractResolver::new(qos, runtime_queue.as_ref()))
            .transpose()?;
        let epoch = SourceEpoch::generate()?;
        let layout = std::sync::Arc::new(Self::qp_layout(&config)?);
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
            .map(|rdma_config| {
                let config_layout = config
                    .selector
                    .as_ref()
                    .filter(|s| !s.qp_pools.is_empty() || s.qp_mul_factor.is_some())
                    .map(|_| Arc::clone(&layout));
                crate::transport::rdma::RdmaTransport::new(
                    rdma_config,
                    config_layout,
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
        #[allow(unused_mut)]
        let mut metadata = SegmentMetadata::default();
        #[cfg(feature = "rdma")]
        {
            metadata.rdma = rdma.is_some();
        }
        #[cfg(feature = "cuda")]
        {
            metadata.nvlink = nvlink.as_ref().map(|transport| transport.identity);
            metadata.remote_staging = config.remote_staging;
        }
        metadata.boot = metadata
            .nvlink
            .map(|id| id.machine)
            .or_else(|| crate::segment::ProcessIdentity::boot_id().ok());
        let selector = Self::compile_selector(
            &config,
            &layout,
            #[cfg(feature = "rdma")]
            rdma.as_deref(),
        )?;
        let source = Arc::new(Source {
            metadata,
            credit_issuer: config.receiver_credit.map(ReceiverCreditIssuer::new),
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
        let receiver_credits = (config.max_receiver_credit_sessions != 0).then(|| {
            SenderCreditLedger::new(config.max_receiver_credit_sessions, runtime_queue.as_ref())
        });
        Ok(Self {
            runtime_queue,
            files: FileSegments::default(),
            #[cfg(feature = "io-uring")]
            io_uring: crate::transport::io_uring::IoUringTransport::probe(),
            #[cfg(feature = "gds")]
            gds: crate::transport::gds::GdsTransport::probe(&config),
            receiver_credits,
            qos,
            selector,
            config,
            source,
            control,
            batches: Arc::default(),
            allocations: Arc::new(AllocationBudget::new()),
        })
    }

    fn qp_layout(config: &EngineConfig) -> Result<QpLayout> {
        #[cfg(feature = "rdma")]
        let flat = config.rdma.as_ref().map_or(1, |rdma| rdma.num_lanes);
        #[cfg(not(feature = "rdma"))]
        let flat = 1;
        let selector = config.selector.as_ref();
        QpLayout::new(
            selector.map_or(&[][..], |s| s.qp_pools.as_slice()),
            selector.and_then(|s| s.qp_mul_factor).unwrap_or(flat),
        )
    }

    fn compile_selector(
        config: &EngineConfig,
        layout: &QpLayout,
        #[cfg(feature = "rdma")] rdma: Option<&crate::transport::rdma::RdmaTransport>,
    ) -> Result<Option<TransportSelector>> {
        let Some(config) = &config.selector else {
            return Ok(None);
        };
        #[cfg(feature = "rdma")]
        let devices = if config.policy.iter().any(|p| !p.devices.is_empty()) {
            rdma.map(|r| r.policy_devices()).transpose()?
        } else {
            None
        };
        #[cfg(not(feature = "rdma"))]
        let devices: Option<Vec<String>> = None;
        TransportSelector::compile(config, devices.as_deref(), layout).map(Some)
    }

    /// Resolves names once and returns an immutable policy handle for reuse in
    /// requests. Repeated resolutions share the same configured budget slot.
    pub fn resolve_qos(
        &self,
        context: &crate::QosRequestContext,
    ) -> Result<crate::EffectiveQosPolicy> {
        QosContractResolver::resolve(self.qos.as_ref(), self.source.epoch, context)
    }

    /// Activates one cooperative credit class for this peer/source epoch. Same
    /// epoch replay does not reset totals; a newer epoch requires drained owners.
    /// A recreated binding after deactivation requires a fresh administrative
    /// epoch and fenced old updates. This API does not authenticate a receiver.
    pub fn activate_receiver_credit(
        &self,
        peer: &RemoteSegment,
        epoch: u64,
        qos_class: u32,
    ) -> Result<()> {
        self.receiver_credits
            .as_ref()
            .ok_or(Error::Unavailable("receiver credits are disabled"))?
            .activate(peer, epoch, qos_class)
    }

    /// Applies an application-delivered cumulative update and wakes eligible
    /// data owners. Remains usable during requester close to drain accepted work.
    pub fn apply_receiver_credit(
        &self,
        peer: &RemoteSegment,
        update: &ReceiverCreditUpdateV1,
    ) -> Result<CreditUpdateDisposition> {
        self.receiver_credits
            .as_ref()
            .ok_or(Error::Unavailable("receiver credits are disabled"))?
            .apply_update(peer, update)
    }

    /// Fences unstarted owners. Busy preserves the binding until all actual
    /// owners drain; repeat with the same epoch to retire it. Started groups may
    /// finish, and Unquiesced owners keep the binding busy.
    pub fn deactivate_receiver_credit(&self, peer: &RemoteSegment, epoch: u64) -> Result<()> {
        self.receiver_credits
            .as_ref()
            .ok_or(Error::Unavailable("receiver credits are disabled"))?
            .deactivate(peer, epoch)
    }

    pub fn receiver_credit_stats(&self, peer: &RemoteSegment) -> Option<ReceiverCreditStats> {
        self.receiver_credits
            .as_ref()
            .and_then(|ledger| ledger.stats(peer))
    }

    /// Negotiates a receiver-owned fixed window for this sender instance.
    /// Subsequent credit shortages synchronize completed ownership automatically.
    /// A failed initial exchange remains pending; retry this same peer and class.
    pub fn connect_receiver_credit(
        &self,
        peer: &RemoteSegment,
        qos_class: u32,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = deadline(timeout)?;
        let ledger = self
            .receiver_credits
            .as_ref()
            .ok_or(Error::Unavailable("receiver credits are disabled"))?;
        let _admission = if ledger.stats(peer).is_some_and(|entry| entry.automatic) {
            None
        } else {
            Some(self.admit_batch(0)?)
        };
        ledger.connect_automatic(peer, self.source.epoch, qos_class, deadline)
    }

    /// Permanently fences this binding and returns its window only after all
    /// data owners completed successfully and dropped. Busy/uncertain bindings
    /// stay fenced; retry after clean drain. Closed identities cannot reopen.
    pub fn disconnect_receiver_credit(
        &self,
        peer: &RemoteSegment,
        timeout: Duration,
    ) -> Result<()> {
        self.receiver_credits
            .as_ref()
            .ok_or(Error::Unavailable("receiver credits are disabled"))?
            .disconnect_automatic(peer, deadline(timeout)?)
    }

    pub fn receiver_credit_issuer_stats(&self) -> Option<crate::ReceiverCreditIssuerStats> {
        self.source
            .credit_issuer
            .as_ref()
            .map(|issuer| issuer.stats())
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
        // Quarantine cleanup drops admission permits. Sample it before locking usage.
        #[cfg(feature = "gds")]
        let gds = self
            .gds
            .as_ref()
            .map_or_else(Default::default, |g| g.stats());
        let batches = self
            .batches
            .usage
            .lock()
            .map_err(|_| Error::Internal("batch lock poisoned"))?;
        Ok(EngineStats {
            unresolved_policy_devices: self.selector.as_ref().map_or(0, |s| s.unresolved_devices),
            #[cfg(feature = "gds")]
            gds,
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
            runtime_queue: self.runtime_queue.as_ref().map(|queue| queue.stats()),
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
        #[cfg(feature = "gds")]
        let result = self.gds.as_ref().map_or(Ok(()), |gds| {
            memory.iter().try_for_each(|region| gds.register(region))
        });
        #[cfg(not(feature = "gds"))]
        let result = Ok(());
        #[cfg(feature = "rdma")]
        let result = result.and_then(|()| {
            self.source.rdma.as_ref().map_or(Ok(()), |runtime| {
                runtime.register_memory(memory, deadline(self.config.handler_timeout)?)
            })
        });
        #[cfg(not(any(feature = "rdma", feature = "gds")))]
        let _ = memory;
        #[cfg(feature = "gds")]
        if result.is_err() {
            let mut cleanup = Ok(());
            for region in memory {
                cleanup = cleanup.and(crate::transport::gds::BufferLease::retire(region));
            }
            cleanup?;
        }
        result
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
        let _last = self.source.registry.unregister(memory)?;
        #[cfg(feature = "gds")]
        let gds_result = if _last {
            crate::transport::gds::BufferLease::retire(memory)
        } else {
            Ok(())
        };
        #[cfg(feature = "rdma")]
        if let Some(runtime) = &self.source.rdma {
            runtime.registrations.retire(memory)?;
        }
        #[cfg(feature = "gds")]
        gds_result?;
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

    /// Opens a local `file://` name with fixed access. Descriptor lookup is lazy.
    /// Reopening the same name and permission shares the handle until close.
    pub fn open_file_segment(&self, name: &str, permission: Permission) -> Result<FileSegment> {
        let _admission = self.admit_batch(0)?;
        self.files
            .open(name, permission, self.config.max_file_segments)
    }

    /// Returns the cached descriptor of the regular file, opening it on first use.
    pub fn file_segment_info(&self, segment: &FileSegment) -> Result<FileSegmentInfo> {
        let _admission = self.admit_batch(0)?;
        self.files.info(segment, false)
    }

    /// Refreshes the descriptor from the open file, including after a resize or unlink.
    pub fn refresh_file_segment(&self, segment: &FileSegment) -> Result<FileSegmentInfo> {
        let _admission = self.admit_batch(0)?;
        self.files.info(segment, true)
    }

    /// Checks access and bounds and returns the starting file byte offset. No payload I/O occurs.
    pub fn resolve_file_offset(
        &self,
        segment: &FileSegment,
        operation: TransferOp,
        offset: u64,
        length: usize,
    ) -> Result<u64> {
        let _admission = self.admit_batch(0)?;
        self.files
            .admit(segment, operation, offset, length)
            .map(|(_, offset)| offset)
    }

    /// Closes admission through every alias, then waits for admitted file users to drain.
    /// After a timeout, retry close on the same handle.
    pub fn close_file_segment(&self, segment: &FileSegment, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))?;
        self.files.close(
            segment,
            deadline,
            #[cfg(feature = "gds")]
            self.gds.as_ref(),
        )
    }

    /// Validates the entire batch before sending any request. Admission and
    /// validation failures return `Err`; execution failures are in the report.
    /// Remote staging commits READ destinations one completed chunk at a time.
    /// Remote staging, Direct RDMA and CUDA IPC can change local READ or remote
    /// WRITE bytes before a final receipt, including on failure. Inspect locally_drained
    /// and remote_effect before releasing owners or considering a retry.
    /// WRITE success requires source acknowledgement. No automatic retries.
    pub fn submit_and_wait<'a>(
        &self,
        operation: TransferOp,
        peer: impl Into<TransferTarget<'a>>,
        requests: &[TransferRequest],
        timeout: Duration,
    ) -> Result<BatchReport> {
        self.submit(operation, peer, requests, None, timeout)
    }

    /// Sends one notification to this peer after every request completes.
    /// Empty or failed batches do not notify. Data and notification completion
    /// share one deadline, but their results are reported separately.
    pub fn submit_and_notify<'a>(
        &self,
        operation: TransferOp,
        peer: impl Into<TransferTarget<'a>>,
        requests: &[TransferRequest],
        notification: &Notification,
        timeout: Duration,
    ) -> Result<BatchReport> {
        self.submit(operation, peer, requests, Some(notification), timeout)
    }

    #[cfg(any(feature = "io-uring", feature = "gds"))]
    fn file_available(&self, transport: Transport, _local: Option<&RegisteredMemory>) -> bool {
        match transport {
            #[cfg(feature = "io-uring")]
            Transport::IoUring => self.io_uring.is_some(),
            #[cfg(feature = "gds")]
            Transport::Gds => {
                _local.is_none_or(crate::transport::gds::BufferSlot::supports)
                    && self.gds.as_ref().is_some_and(|g| g.mode().is_ok())
            }
            _ => false,
        }
    }

    pub(crate) fn prepare<'a>(
        &self,
        operation: TransferOp,
        peer: impl Into<TransferTarget<'a>>,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
        timeout: Duration,
        batch: Option<BatchId>,
    ) -> Result<PreparedSubmission> {
        if requests.len() > self.config.max_batch_requests {
            return Err(Error::ResourceExhausted);
        }
        let deadline = deadline(timeout)?;
        let target = peer.into();
        let kind = match target {
            TransferTarget::Memory(_) => crate::SegmentType::Memory,
            TransferTarget::File(_) => crate::SegmentType::File,
        };
        let mut bindings = TransportSelector::bindings(self.selector.as_ref(), kind, requests)?
            .into_iter()
            .peekable();
        let mut payload = match target {
            TransferTarget::Memory(peer) => PreparedTarget::Memory {
                peer: peer.clone(),
                requests: Vec::new(),
            },
            TransferTarget::File(_) if notification.is_some() => {
                return Err(Error::Unavailable(
                    "file transfers do not support notifications",
                ));
            }
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            TransferTarget::File(segment) => {
                if requests.is_empty()
                    && !self.file_available(Transport::Gds, None)
                    && !self.file_available(Transport::IoUring, None)
                {
                    return Err(Error::Unavailable(
                        "file_storage: no local file transport is available",
                    ));
                }
                PreparedTarget::File {
                    segment: segment.clone(),
                    requests: Vec::new(),
                }
            }
            #[cfg(not(any(feature = "io-uring", feature = "gds")))]
            TransferTarget::File(_) => {
                return Err(Error::Unavailable(
                    "file_storage: no local file transport is available",
                ));
            }
        };
        let notification_bytes = match notification {
            Some(notification) => self.validate_notification(notification)?,
            None => 0,
        };
        let mut total_bytes = notification_bytes;
        let mut qos_charges = Vec::new();
        match &mut payload {
            PreparedTarget::Memory {
                requests: prepared, ..
            } => prepared.try_reserve_exact(requests.len()),
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            PreparedTarget::File {
                requests: prepared, ..
            } => prepared.try_reserve_exact(requests.len()),
        }
        .map_err(|_| Error::ResourceExhausted)?;
        #[cfg(feature = "rdma")]
        let mut rdma_ready = false;
        for (index, request) in requests.iter().enumerate() {
            let bound = bindings.next_if(|(i, _)| *i == index).map(|(_, rule)| rule);
            let (priority, contract) = match &request.qos {
                Some(policy) => (
                    policy.effective_priority(),
                    policy.contract(self.source.epoch)?,
                ),
                None => {
                    let contract = self.qos.as_ref().map(QosContractResolver::default_contract);
                    (
                        contract
                            .and_then(|contract| contract.fields.priority)
                            .unwrap_or(request.priority),
                        contract,
                    )
                }
            };
            if request.length > self.config.max_transfer_bytes {
                return Err(Error::ResourceExhausted);
            }
            let (local, local_offset) = self
                .source
                .registry
                .resolve_address(request.local_address, request.length)?;
            total_bytes = total_bytes
                .checked_add(request.length)
                .ok_or(Error::ResourceExhausted)?;
            if let Some(contract) = contract {
                QosCharge::add(&mut qos_charges, contract, request.length)?;
            }
            let (peer, prepared) = match &mut payload {
                PreparedTarget::Memory { peer, requests } => (peer, requests),
                #[cfg(any(feature = "io-uring", feature = "gds"))]
                PreparedTarget::File {
                    segment,
                    requests: files,
                    ..
                } => {
                    if request.remote_staging {
                        return Err(Error::Unavailable(
                            "file I/O does not support remote staging",
                        ));
                    }
                    let selection = TransportSelector::select(
                        self.selector.as_ref(),
                        Filters {
                            segment: crate::SegmentType::File,
                            local: local.memory_type(),
                            remote: crate::MemoryType::Cpu,
                            size: request.length,
                            priority,
                            same_machine: true,
                        },
                        request,
                        bound,
                        |t| self.file_available(t, Some(&local)),
                    )?;
                    let (lease, offset) = self.files.admit(
                        segment,
                        operation,
                        request.remote_address,
                        request.length,
                    )?;
                    files.push(crate::segment::file::FileRequest {
                        selection,
                        #[cfg(feature = "gds")]
                        buffer: None,
                        local,
                        local_offset,
                        lease,
                        offset,
                        length: request.length,
                    });
                    continue;
                }
            };
            if matches!(request.transport, Transport::Gds | Transport::IoUring) {
                let reason = "memory_default: file transport requires a file target";
                return Err(match &request.policy_name {
                    Some(name) => Error::TransportPolicy {
                        policy: name.clone(),
                        reason: "file transport requires a file target",
                    },
                    None => Error::Unavailable(reason),
                });
            }
            let (remote, remote_offset) =
                peer.resolve_address(request.remote_address, request.length)?;
            if !remote.permission.allows(operation == TransferOp::Write) {
                return Err(Error::PermissionDenied);
            }
            let context = SelectionContext {
                local_memory: local.memory_type(),
                #[cfg(feature = "cuda")]
                local_ipc: local.ipc_requester(),
                #[cfg(not(feature = "cuda"))]
                local_ipc: false,
                remote: &remote,
                local_segment: self.source.metadata,
                remote_segment: peer.metadata,
                transfer_size: request.length,
                priority,
                remote_staging: request.remote_staging,
            };
            let selection = context.select_bound(self.selector.as_ref(), request, bound)?;
            let transport = selection.transport;
            if request.remote_staging && transport == Transport::Nvlink {
                return Err(Error::Unavailable("remote staging requires TCP or RDMA"));
            }
            #[cfg(not(feature = "cuda"))]
            if request.remote_staging {
                return Err(Error::Unavailable(
                    "remote staging requires --features cuda",
                ));
            }
            if transport == Transport::Rdma {
                #[cfg(feature = "rdma")]
                {
                    if selection.device_mask != u64::MAX {
                        self.source
                            .rdma
                            .as_ref()
                            .ok_or_else(|| selection.rejected("RDMA is unavailable"))?
                            .check_policy_paths(
                                (!request.remote_staging)
                                    .then(|| local.location())
                                    .transpose()?
                                    .map(|r| r.location),
                                &selection,
                            )?;
                    }
                    if !rdma_ready {
                        self.source
                            .rdma
                            .as_ref()
                            .ok_or(Error::Unavailable("RDMA is not configured"))?
                            .check_ready()
                            .map_err(|error| match error {
                                Error::Unavailable(reason) => selection.rejected(reason),
                                error => error,
                            })?;
                        rdma_ready = true;
                    }
                }
                #[cfg(not(feature = "rdma"))]
                return Err(Error::Unavailable("build with --features rdma"));
            }
            if transport == Transport::Nvlink {
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
            prepared.push(PreparedTransfer {
                #[cfg(feature = "rdma")]
                priority,
                #[cfg(feature = "cuda")]
                remote_staging: request.remote_staging,
                selection,
                local,
                local_offset,
                remote,
                remote_offset,
                length: request.length,
            });
        }
        #[cfg(feature = "gds")]
        if let (TransferTarget::File(_), Some(gds)) = (target, &self.gds) {
            gds.cleanup();
        }
        let admission = self.admit_batch(total_bytes)?;
        let qos = match &self.qos {
            Some(resolver) => resolver.admit(qos_charges)?,
            None => None,
        };
        let credit = match (target, &self.receiver_credits) {
            (TransferTarget::Memory(peer), Some(ledger)) if !requests.is_empty() => {
                Some(ledger.prepare(peer, total_bytes - notification_bytes, requests.len())?)
            }
            _ => None,
        };
        let queue = self
            .runtime_queue
            .as_ref()
            .filter(|_| !requests.is_empty())
            .map(|queue| {
                queue.try_admit(
                    total_bytes,
                    deadline,
                    batch,
                    credit
                        .as_ref()
                        .map(|credit| Arc::clone(&credit.reservation)),
                    qos.as_ref().and_then(QosPermit::reservation),
                )
            })
            .transpose()?;
        let prepared = PreparedSubmission {
            #[cfg(feature = "gds")]
            gds_pending: Mutex::default(),
            operation,
            payload,
            notification: notification.cloned(),
            deadline,
            queue,
            credit,
            _qos: qos,
            _admission: Some(admission),
        };
        #[cfg(feature = "gds")]
        let prepared = prepared.pin_gds_buffers()?;
        Ok(prepared)
    }

    fn submit<'a>(
        &self,
        operation: TransferOp,
        peer: impl Into<TransferTarget<'a>>,
        requests: &[TransferRequest],
        notification: Option<&Notification>,
        timeout: Duration,
    ) -> Result<BatchReport> {
        let prepared = self.prepare(operation, peer, requests, notification, timeout, None)?;
        let report = self.execute(&prepared, None);
        if !report.locally_drained {
            // Preserve actual registrations and admission, not a pointer surrogate.
            self.retain_undrained(prepared, None, None, || {});
        }
        Ok(report)
    }

    pub(crate) fn retain_undrained(
        &self,
        prepared: PreparedSubmission,
        progress: Option<(Arc<BatchProgress>, usize)>,
        engine: Option<Arc<TransferEngine>>,
        publish: impl FnOnce(),
    ) {
        #[cfg(feature = "gds")]
        let pending = prepared
            .gds_pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        #[cfg(feature = "gds")]
        if let Some(batch) = pending {
            if let Some(gds) = &self.gds {
                gds.retain(batch, prepared, self.source.clone(), progress, publish);
                return;
            }
            std::mem::forget(batch);
        }
        std::mem::forget((prepared, self.source.clone(), progress, engine));
        publish();
    }

    pub(crate) fn execute(
        &self,
        submission: &PreparedSubmission,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let report = self.execute_payload(submission, progress);
        #[cfg(feature = "gds")]
        let report = submission.finish_gds_report(report, progress);
        report
    }

    fn execute_payload(
        &self,
        submission: &PreparedSubmission,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let PreparedSubmission {
            operation,
            payload,
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
        let cancelled =
            || progress.is_some_and(|(progress, first)| progress.cancel_requested(first));
        let admission = match (&submission.queue, &submission.credit) {
            (Some(queue), _) => queue.wait(cancelled),
            (None, Some(credit)) => credit.reservation.wait(*deadline, cancelled),
            (None, None) => Ok(()),
        };
        if let Err(error) = admission {
            report.failure = Some(TransferFailure {
                request_index: 0,
                affected_requests: 1,
                error,
                remote_effect: RemoteEffect::None,
            });
            return report;
        }
        let (peer, requests) = match payload {
            PreparedTarget::Memory { peer, requests } => (peer, requests),
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            PreparedTarget::File { requests, .. } => {
                let mut first = 0;
                while first < requests.len() {
                    let transport = requests[first].selection.transport;
                    let count = requests[first..]
                        .iter()
                        .take_while(|r| r.selection == requests[first].selection)
                        .count();
                    let group = &requests[first..first + count];
                    let progress = progress.map(|(p, base)| (p, base + first));
                    let mut result = BatchReport {
                        completed_requests: 0,
                        transferred_bytes: 0,
                        failure: None,
                        locally_drained: true,
                        notification: NotificationStatus::NotRequested,
                    };
                    let executed = match transport {
                        #[cfg(feature = "io-uring")]
                        Transport::IoUring => self.io_uring.as_ref().map(|io| {
                            io.execute(
                                *operation,
                                group,
                                *deadline,
                                progress,
                                &mut result,
                                #[cfg(feature = "cuda")]
                                (&self.source.proxy, self.config.max_batch_requests),
                            )
                        }),
                        #[cfg(feature = "gds")]
                        Transport::Gds => self.gds.as_ref().map(|gds| {
                            gds.execute(
                                *operation,
                                group,
                                *deadline,
                                progress,
                                &mut result,
                                &submission.gds_pending,
                            )
                        }),
                        _ => None,
                    };
                    if executed.is_none() {
                        result.failure = Some(TransferFailure {
                            request_index: 0,
                            affected_requests: 1,
                            error: Error::Unavailable("selected file transport is unavailable"),
                            remote_effect: RemoteEffect::None,
                        });
                    }
                    report.completed_requests += result.completed_requests;
                    report.transferred_bytes += result.transferred_bytes;
                    report.locally_drained = result.locally_drained;
                    if let Some(mut failure) = result.failure {
                        failure.request_index += first;
                        report.failure = Some(failure);
                        break;
                    }
                    first += count;
                }
                return report;
            }
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
            if index == 0 {
                if let Some(credit) = &submission.credit {
                    if let Err(error) = credit.reservation.commit() {
                        report.failure = Some(TransferFailure {
                            request_index: 0,
                            affected_requests: 1,
                            error,
                            remote_effect: RemoteEffect::None,
                        });
                        break;
                    }
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
            let result = match request.selection.transport {
                Transport::IoUring | Transport::Gds | Transport::Unspec => single(Err((
                    Error::Internal("unresolved transport"),
                    RemoteEffect::None,
                ))),
                #[cfg(feature = "cuda")]
                Transport::Tcp | Transport::Rdma if request.remote_staging => single(
                    self.source
                        .proxy
                        .transfer_remote(submission, peer, request, &self.source),
                ),
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
                                (peer, &requests[index..]),
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
            if let Some(credit) = &submission.credit {
                credit.reservation.complete();
            }
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
    pub fn submit_async<'a>(
        self: &Arc<Self>,
        operation: TransferOp,
        peer: impl Into<TransferTarget<'a>>,
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
            #[cfg(feature = "gds")]
            if let Some(gds) = &self.gds {
                drop(usage);
                gds.cleanup();
                usage = self
                    .batches
                    .usage
                    .lock()
                    .map_err(|_| Error::Internal("batch lock poisoned"))?;
                if usage.count == 0 {
                    break;
                }
            }
            let duration = remaining(deadline)?;
            #[cfg(feature = "gds")]
            let duration = if self.gds.is_some() {
                duration.min(Duration::from_millis(10))
            } else {
                duration
            };
            usage = self
                .batches
                .drained
                .wait_timeout(usage, duration)
                .map_err(|_| Error::Internal("batch lock poisoned"))?
                .0;
        }
        drop(usage);
        self.files.close_all(deadline)?;
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
        #[cfg(feature = "gds")]
        self.source.registry.close_gds_buffers()?;
        #[cfg(feature = "cuda")]
        self.source.proxy.close(deadline)?;
        #[cfg(feature = "rdma")]
        if let Some(runtime) = &self.source.rdma {
            runtime.close(deadline)?;
        }
        Ok(())
    }

    pub(crate) fn wake_runtime_queue(&self) {
        if let Some(queue) = &self.runtime_queue {
            queue.wake();
        }
        if let Some(ledger) = &self.receiver_credits {
            ledger.wake();
        }
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
    pub(crate) metadata: SegmentMetadata,
    pub(crate) credit_issuer: Option<Arc<ReceiverCreditIssuer>>,
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

enum PreparedTarget {
    Memory {
        peer: RemoteSegment,
        requests: Vec<PreparedTransfer>,
    },
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    File {
        segment: FileSegment,
        requests: Vec<crate::segment::file::FileRequest>,
    },
}

pub(crate) struct PreparedSubmission {
    #[cfg(feature = "gds")]
    gds_pending: Mutex<Option<crate::transport::gds::Quarantine>>,
    pub(crate) operation: TransferOp,
    payload: PreparedTarget,
    pub(crate) notification: Option<Notification>,
    pub(crate) deadline: Instant,
    // Backing drops first. Return QoS before advancing the Batch head so a
    // reject-only successor does not see its predecessor's retiring charge.
    _qos: Option<QosPermit>,
    queue: Option<QueuePermit>,
    // Remove queue metadata before credit rollback can wake/refill the queue.
    pub(crate) credit: Option<CreditPermit>,
    _admission: Option<BatchPermit>,
}

#[cfg(feature = "gds")]
impl PreparedSubmission {
    fn pin_gds_buffers(mut self) -> Result<Self> {
        if let PreparedTarget::File { requests, .. } = &mut self.payload {
            for r in requests {
                if r.selection.transport == Transport::Gds {
                    match crate::transport::gds::BufferLease::acquire(&r.local) {
                        Ok(lease) => r.buffer = lease,
                        Err(e) => return Err(self.abort_error(e)),
                    }
                }
            }
        }
        Ok(self)
    }

    fn finish_gds_report(
        &self,
        mut report: BatchReport,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let PreparedTarget::File { requests, .. } = &self.payload else {
            return report;
        };
        if report.locally_drained {
            if let Err((index, error)) = self.finish_gds_buffers() {
                // A cancelled/queued group can be the last registration borrower.
                // Its admission stays charged even if it never obtained a native batch.
                *self.gds_pending.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(crate::transport::gds::Quarantine::BufferRelease);
                if let Some((p, base)) = progress {
                    p.fail_file_task(
                        base + index,
                        0,
                        error.for_file_report(),
                        RemoteEffect::None,
                        false,
                    );
                }
                report.locally_drained = false;
                let first = report.completed_requests.min(index);
                let end = report
                    .failure
                    .as_ref()
                    .map_or(report.completed_requests, |f| {
                        f.request_index + f.affected_requests
                    });
                let effect = if self.operation == TransferOp::Write
                    && (first < report.completed_requests
                        || report
                            .failure
                            .as_ref()
                            .is_some_and(|f| f.remote_effect == RemoteEffect::Unknown))
                {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                };
                report.failure = Some(TransferFailure {
                    request_index: first,
                    affected_requests: end.saturating_sub(first).max(1),
                    error,
                    remote_effect: effect,
                });
                report.completed_requests = first;
                report.transferred_bytes = requests[..first].iter().map(|r| r.length).sum();
            }
        }
        report
    }

    fn finish_gds_buffers(&self) -> std::result::Result<(), (usize, Error)> {
        if let PreparedTarget::File { requests, .. } = &self.payload {
            for (index, request) in requests.iter().enumerate() {
                if let Some(buffer) = &request.buffer {
                    buffer.release().map_err(|e| (index, e))?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn release_gds_buffers(&self) -> bool {
        self.finish_gds_buffers().is_ok()
    }
}

impl PreparedSubmission {
    pub(crate) fn abort_error(&self, error: Error) -> Error {
        #[cfg(feature = "gds")]
        if let Err((_, cleanup)) = self.finish_gds_buffers() {
            return cleanup;
        }
        error
    }
}

#[cfg(feature = "gds")]
impl Drop for PreparedSubmission {
    fn drop(&mut self) {
        let pending = self
            .gds_pending
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if pending.is_some() || self.finish_gds_buffers().is_err() {
            // Rollback before enqueue (including worker creation failure) cannot
            // return a file borrower or its charges without a release receipt.
            std::mem::forget(pending);
            if let PreparedTarget::File { requests, .. } = &mut self.payload {
                std::mem::forget(std::mem::take(requests));
            }
            std::mem::forget((
                self._admission.take(),
                self._qos.take(),
                self.queue.take(),
                self.credit.take(),
            ));
        }
    }
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
