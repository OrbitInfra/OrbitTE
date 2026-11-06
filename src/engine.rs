mod admission;
mod failover;
#[cfg(any(feature = "io-uring", feature = "gds"))]
pub(crate) mod file_attempt;
#[cfg(any(feature = "io-uring", feature = "gds"))]
mod file_prepare;
mod hp_tcp;
#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "cuda")]
mod mnnvl;
mod segment;
#[cfg(target_os = "linux")]
mod shm;
use admission::OwnerAdmission;
use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::batch::{AllocationBudget, Batch, BatchId, BatchProgress};
use crate::memory::MemoryRegistry;
use crate::notification::{NotificationInbox, NotificationRoutes, Target};
use crate::runtime::admission_queue::{
    LocalTransferAdmissionQueue, QueueTicket, RuntimeQueueStats,
};
use crate::runtime::control_plane::{self, ControlService};
use crate::runtime::qos_contract::QosContractResolver;
use crate::runtime::receiver_credit::{ReceiverCreditIssuer, SenderCreditLedger};
use crate::runtime::request_merge::TaskMap;
use crate::runtime::segment_manager::{PeerKey, PeerPermit, SegmentManager};
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

/// Byte transport or policy selection. An explicit hint must be eligible for the first attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Transport {
    Tcp,
    /// Persistent CPU socket lanes using TENT HP-TCP framing.
    HpTcp,
    /// Same-host CPU copies through source-granted shared mappings.
    Shm,
    Rdma,
    Nvlink,
    Mnnvl,
    /// Linux io_uring for local regular files and registered memory.
    IoUring,
    /// Local cuFile batch I/O.
    Gds,
    /// Choose the first eligible transport in the matched policy.
    Unspec,
}

impl Transport {
    /// Frozen TENT transport discriminant, independent of the Rust enum layout.
    pub fn tent_id(self) -> u8 {
        match self {
            Self::Unspec => 0,
            Self::Rdma => 1,
            Self::Mnnvl => 2,
            Self::Shm => 3,
            Self::Nvlink => 4,
            Self::Gds => 5,
            Self::IoUring => 6,
            Self::Tcp => 7,
            Self::HpTcp => 13,
        }
    }
    /// Return a transport represented by a Rust owner. Installation is checked separately.
    pub fn from_tent_id(id: u8) -> Option<Self> {
        Some(match id {
            0 => Self::Unspec,
            1 => Self::Rdma,
            2 => Self::Mnnvl,
            3 => Self::Shm,
            4 => Self::Nvlink,
            5 => Self::Gds,
            6 => Self::IoUring,
            7 => Self::Tcp,
            13 => Self::HpTcp,
            _ => return None,
        })
    }
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

/// One file-ordered element of a vectored READ or WRITE.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileIoElement {
    /// A nonempty range in one local host or CUDA registration.
    Range { address: u64, length: usize },
    /// Write zeros or read and discard this many bytes. Needs no registration.
    Padding(usize),
}

impl From<(u64, usize)> for FileIoElement {
    fn from((address, length): (u64, usize)) -> Self {
        Self::Range { address, length }
    }
}

impl FileIoElement {
    fn address(self) -> Option<u64> {
        match self {
            Self::Range { address, .. } => Some(address),
            Self::Padding(_) => None,
        }
    }

    fn length(self) -> usize {
        match self {
            Self::Range { length, .. } | Self::Padding(length) => length,
        }
    }
}

/// A registered local address and a peer address or zero-based file byte offset.
/// Submission resolves and retains the local backing before any network I/O.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TransferRequest {
    /// Optional absolute owner deadline, capped by the submission timeout.
    pub deadline: Option<Instant>,
    /// Bind to exactly this policy and segment type. Resolved during preparation.
    pub policy_name: Option<crate::PolicyName>,
    /// Business intent for automatic policy selection, independent of QoS.
    pub intent: crate::IntentType,
    /// Pre-resolved engine-bound contract. None uses the engine's default/unspec
    /// contract, with `priority` as its fallback. Some captures its own requested
    /// priority at resolution and replaces this request's legacy priority.
    pub qos: Option<crate::EffectiveQosPolicy>,
    /// Priority for transport policy matching and RDMA payload dispatch.
    /// TCP, HP-TCP and NVLink accept all priorities and treat them as advisory.
    /// This does not reorder requests within a batch or preempt posted DMA.
    pub priority: Priority,
    /// Use Pin/Delegate/Unpin for the remote CUDA range over TCP or RDMA.
    /// The source must enable EngineConfig::remote_staging. False uses the
    /// selected transport's ordinary memory policy, including local staging.
    pub remote_staging: bool,
    pub transport: Transport,
    pub local_address: u64,
    pub(crate) file_ranges: Option<Arc<[FileIoElement]>>,
    pub(crate) require_cancellation: bool,
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
            file_ranges: None,
            require_cancellation: false,
            remote_address,
            length,
            deadline: None,
            policy_name: None,
            intent: crate::IntentType::Unspec,
            qos: None,
            priority: Priority::default(),
            remote_staging: false,
        }
    }

    /// Require a direct route that can cancel unposted native work after submission.
    /// A classic scatter interval owns its dispatch and disables transport retries.
    pub fn requiring_post_submit_cancellation(mut self) -> Self {
        self.require_cancellation = true;
        self
    }

    /// Gather on WRITE or scatter on READ, in list order, starting at a file byte offset.
    /// Accepts registered `(address, length)` pairs or [`FileIoElement`]s with explicit
    /// padding. Every element must be nonempty. All elements, including padding, count
    /// toward the 1024-element limit, the engine's descriptor budget and the file extent.
    pub fn file_vectored(
        transport: Transport,
        file_offset: u64,
        elements: &[impl Copy + Into<FileIoElement>],
    ) -> Result<Self> {
        if elements.is_empty() {
            return Err(Error::InvalidRange);
        }
        if elements.len() > 1024 {
            return Err(Error::ResourceExhausted);
        }
        let elements: Arc<[FileIoElement]> = elements.iter().copied().map(Into::into).collect();
        let length = elements.iter().try_fold(0usize, |sum, element| {
            let len = element.length();
            if len == 0
                || element
                    .address()
                    .is_some_and(|a| a.checked_add(len as u64).is_none())
            {
                return Err(Error::InvalidRange);
            }
            sum.checked_add(len).ok_or(Error::InvalidRange)
        })?;
        file_offset
            .checked_add(length as u64)
            .ok_or(Error::InvalidRange)?;
        let local = elements.iter().find_map(|e| e.address()).unwrap_or(0);
        let mut request = Self::new(transport, local, file_offset, length);
        request.file_ranges = Some(elements);
        Ok(request)
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
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
    #[cfg(target_os = "linux")]
    pub(crate) shm: Option<Arc<crate::transport::shm::Lease>>,
    pub(crate) hp_cancel: Option<tokio::sync::watch::Sender<bool>>,
    #[cfg(feature = "rdma")]
    pub(crate) priority: Priority,
    #[cfg(feature = "rdma")]
    pub(crate) rdma_cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
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
    /// Freed batches awaiting physical completion, and batches with no safe release path.
    pub deferred_batches: usize,
    pub quarantined_batches: usize,
    /// Device name entries missing or outside the policy mask at construction.
    pub unresolved_policy_devices: usize,
    #[cfg(feature = "gds")]
    pub gds: crate::GdsStats,
    #[cfg(feature = "cuda")]
    pub nvlink: Option<crate::NVLinkStats>,
    #[cfg(feature = "io-uring")]
    pub io_uring: Option<crate::IoUringStats>,
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
    /// Charged endpoint/epoch identities (live plus retired).
    pub peer_segments: usize,
    /// Identities currently open for admission.
    pub live_peer_segments: usize,
    /// Fenced identities still referenced or awaiting physical cleanup.
    pub retired_peer_segments: usize,
    /// Retired identities with a timed-out drain or sticky cleanup failure.
    pub unquiesced_peer_segments: usize,
    /// Identities reclaimed after their last reference and any required cleanup.
    pub reclaimed_peer_segments: u64,
    /// Submission, Describe and notification owners that have not drained.
    pub active_peer_operations: usize,
    pub inflight_batches: usize,
    pub inflight_bytes: usize,
    pub queued_notifications: usize,
    /// Queued messages plus incoming bodies that reserved inbox capacity.
    pub retained_notifications: usize,
    pub retained_notification_bytes: usize,
}

/// Engine, source registry and bounded control-service handler ownership.
pub struct TransferEngine {
    hp_tcp: Option<crate::transport::hp_tcp::HpTcpTransport>,
    #[cfg(feature = "metrics")]
    metrics: Option<crate::metrics::Metrics>,
    pub(crate) progress_worker: Option<crate::runtime::progress_worker::ProgressWorker>,
    config: EngineConfig,
    source: Arc<Source>,
    control: ControlService,
    pub(crate) notification_routes: NotificationRoutes,
    batches: Arc<RequesterAdmission>,
    peers: SegmentManager,
    allocations: Arc<AllocationBudget>,
    runtime_queue: Option<Arc<LocalTransferAdmissionQueue>>,
    receiver_credits: Option<Arc<SenderCreditLedger>>,
    qos: Option<QosContractResolver>,
    selector: Option<TransportSelector>,
    pub(crate) files: FileSegments,
    #[cfg(feature = "io-uring")]
    pub(crate) io_uring: Option<crate::transport::io_uring::IoUringTransport>,
    #[cfg(feature = "gds")]
    pub(crate) gds: Option<Arc<crate::transport::gds::Provider>>,
}

impl TransferEngine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        #[cfg(any(feature = "rdma", feature = "io-uring", feature = "metrics"))]
        let config = config.resolve_subsystems()?;
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
        // The TENT loader installs RDMA only for an eligible discovered NIC.
        #[cfg(feature = "rdma")]
        let rdma = match rdma {
            Some(owner) if config.tent.is_some() && owner.policy_devices()?.is_empty() => None,
            owner => owner,
        };
        #[cfg(feature = "cuda")]
        let mut nvlink = config
            .nvlink
            .then(|| {
                crate::transport::nvlink::NVLinkTransport::new(
                    config.max_inflight_bytes,
                    config.nvlink_cache_entries,
                    config.nvlink_stream_count,
                    config.nvlink_async_memcpy_threshold_kb,
                )
            })
            .transpose()?;
        #[cfg(feature = "cuda")]
        if let Some(c) = &config.mnnvl {
            match crate::transport::nvlink::NVLinkTransport::install_mnnvl(c, &config) {
                Ok(owner) => nvlink = Some(owner),
                Err(e @ (Error::Unavailable(_) | Error::Backend { .. })) => {
                    log::warn!("MNNVL unavailable: {e}")
                }
                Err(e) => return Err(e),
            }
        }
        #[allow(unused_mut)]
        let hp_listener = config
            .hp_tcp
            .enable
            .then(|| config.hp_tcp.bind(config.bind_addr))
            .transpose()?;
        let mut metadata = SegmentMetadata {
            tcp_disabled: !config.tcp,
            hp: hp_listener.as_ref().map(|(_, end)| Arc::new(*end)),
            shm: config.shm.enable.then(|| {
                Arc::new(crate::transport::shm::wire::Manifest {
                    cxl: !config.shm.cxl_mount_path.as_os_str().is_empty(),
                    entries: Vec::new(),
                })
            }),
            max_transfer_bytes: config.max_transfer_bytes as u64,
            ..Default::default()
        };
        #[cfg(feature = "rdma")]
        {
            metadata.rdma = rdma.is_some();
            metadata.rdma_direct_gpu = rdma.as_ref().is_some_and(|r| r.direct_gpu_ranges());
        }
        #[cfg(feature = "cuda")]
        {
            metadata.nvlink = nvlink
                .as_ref()
                .filter(|t| t.fabric.is_none())
                .map(|transport| transport.identity);
            metadata.mnnvl = nvlink
                .as_ref()
                .and_then(|t| t.fabric.as_ref())
                .map(|f| f.caps);
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
        #[cfg(target_os = "linux")]
        let shm = config
            .shm
            .enable
            .then(|| {
                crate::transport::shm::ShmTransport::new(
                    &config.shm,
                    epoch,
                    config.max_registered_regions,
                    config.max_registered_bytes,
                )
            })
            .transpose()?;
        let source = Arc::new(Source {
            #[cfg(target_os = "linux")]
            shm,
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
            tcp_notifications: config.tcp_notifications,
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
        let allocations = Arc::new(AllocationBudget::new());
        let progress_worker = Self::start_progress(&config, &allocations, &runtime_queue, &source)?;
        let hp_tcp = hp_listener
            .map(|(listener, _)| {
                crate::transport::hp_tcp::HpTcpTransport::start(
                    &config.hp_tcp,
                    listener,
                    source.clone(),
                )
            })
            .transpose()?;
        let control = ControlService::start(Arc::clone(&source), &config)?;
        let receiver_credits = (config.max_receiver_credit_sessions != 0).then(|| {
            SenderCreditLedger::new(config.max_receiver_credit_sessions, runtime_queue.as_ref())
        });
        let engine = Self {
            hp_tcp,
            #[cfg(feature = "metrics")]
            metrics: None,
            runtime_queue,
            files: FileSegments::default(),
            #[cfg(feature = "io-uring")]
            io_uring: if config.io_uring_enabled {
                crate::transport::io_uring::IoUringTransport::probe(&config)?
            } else {
                None
            },
            #[cfg(feature = "gds")]
            gds: config.gds_enabled.then(|| {
                crate::transport::gds::Provider::new(crate::transport::gds::GdsTransport::probe)
            }),
            receiver_credits,
            qos,
            selector,
            notification_routes: NotificationRoutes::new(&config),
            peers: SegmentManager::new(config.max_peer_segments),
            config,
            source,
            control,
            batches: Arc::default(),
            allocations,
            progress_worker,
        };
        #[cfg(feature = "metrics")]
        let engine = {
            let metrics = engine.config.metrics.clone();
            engine.install_metrics(&metrics)?
        };
        if let Some(settings) = &engine.config.tent {
            settings.log_startup(&engine.config);
        }
        Ok(engine)
    }

    /// Effective configuration captured by this engine at construction.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    fn start_progress(
        config: &EngineConfig,
        allocations: &Arc<AllocationBudget>,
        queue: &Option<Arc<LocalTransferAdmissionQueue>>,
        _source: &Source,
    ) -> Result<Option<crate::runtime::progress_worker::ProgressWorker>> {
        #[cfg(feature = "rdma")]
        if let (Some(queue), Some(rdma)) = (queue, &_source.rdma) {
            let rdma = Arc::downgrade(rdma);
            queue.set_bandwidth(Box::new(move |transport, mask| {
                if transport != Transport::Rdma {
                    return 0.0;
                }
                rdma.upgrade().map_or(0.0, |r| r.admission_bandwidth(mask))
            }))?;
        }
        (config.enable_progress_worker || queue.is_some())
            .then(|| {
                crate::runtime::progress_worker::ProgressWorker::start(
                    allocations.clone(),
                    queue.clone(),
                    config.max_allocated_batches,
                    config.progress_fallback_interval,
                )
            })
            .transpose()
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn wake_rdma_cancel(&self) {
        if let Some(rdma) = &self.source.rdma {
            rdma.wake_cancel();
        }
    }
    pub(crate) fn cancel_queued(&self, ticket: &QueueTicket) -> bool {
        self.runtime_queue
            .as_ref()
            .is_some_and(|q| q.cancel(ticket))
    }
    pub(crate) fn auto_failover_on_poll(&self) -> bool {
        self.config.enable_auto_failover_on_poll
    }
    pub(crate) fn defers_busy_free(&self) -> bool {
        self.config.enable_progress_worker
    }

    /// Advance one dispatch or completion for this batch, then observe its status.
    /// Native I/O stays on the worker that owns the admitted submission.
    pub fn progress_batch(&self, id: BatchId) -> Result<crate::BatchStatus> {
        let batch = self.allocations.lookup(id).ok_or(Error::BatchClosed)?;
        batch.progress_public()
    }

    pub(crate) fn qp_layout(config: &EngineConfig) -> Result<QpLayout> {
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
    /// epoch replay does not reset totals. A newer epoch requires drained owners
    /// and confirmation of every committed charge.
    /// A recreated binding after deactivation requires a fresh administrative
    /// epoch and fenced old updates. This API does not authenticate a receiver.
    pub fn activate_receiver_credit(
        &self,
        peer: &RemoteSegment,
        epoch: u64,
        qos_class: u32,
    ) -> Result<()> {
        let _peer = self.admit_peer(peer)?;
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
    /// owners drain and every committed charge has a completion receipt. Repeat
    /// with the same epoch to retire it. Started groups may
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
        let _peer = self.admit_peer(peer)?;
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

    /// Snapshot for native consumers without starting a verbs worker or creating
    /// a CUDA context. Prefer the ranked live provider topology when installed.
    #[cfg(target_os = "linux")]
    pub fn local_topology_snapshot(&self) -> Result<crate::Topology> {
        #[cfg(feature = "rdma")]
        if let Some(topology) = self.topology()? {
            return Ok(topology);
        }
        let nodes = (crate::platform::numa::NATIVE.nodes)().unwrap_or_default();
        let topology = crate::Topology {
            nic_list: Vec::new(),
            mem_list: nodes
                .into_iter()
                .map(|node| crate::MemEntry {
                    name: format!("cpu:{node}"),
                    pci_bus_id: None,
                    kind: crate::MemType::Host,
                    numa_node: Some(node),
                    device_list: Default::default(),
                })
                .collect(),
        };
        #[cfg(feature = "cuda")]
        let topology = {
            let mut visible = topology;
            crate::cuda::discover_topology(&mut visible, std::path::Path::new("/sys"))?;
            visible
        };
        Ok(topology)
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
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.sweep()?;
        }
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
        let peers = self.peers.stats();
        // Quarantine cleanup drops admission permits. Sample it before locking usage.
        #[cfg(feature = "gds")]
        let gds = self
            .loaded_gds()
            .map_or_else(Default::default, |g| g.stats());
        let batches = self
            .batches
            .usage
            .lock()
            .map_err(|_| Error::Internal("batch lock poisoned"))?;
        Ok(EngineStats {
            #[cfg(feature = "io-uring")]
            io_uring: self.io_uring.as_ref().map(|io| io.stats()),
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
            deferred_batches: allocated.3,
            quarantined_batches: allocated.4,
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
            peer_segments: peers.identities,
            live_peer_segments: peers.live,
            retired_peer_segments: peers.retired,
            unquiesced_peer_segments: peers.unquiesced,
            reclaimed_peer_segments: peers.reclaimed,
            active_peer_operations: peers.active,
            inflight_batches: batches.count,
            inflight_bytes: batches.bytes,
            queued_notifications,
            retained_notifications,
            retained_notification_bytes,
        })
    }

    /// Allocate named shared host storage. Register one or more ranges with register_host_memory.
    pub fn allocate_shm_memory(&self, length: usize) -> Result<Arc<HostMemory>> {
        let _admission = self.source.admit()?;
        #[cfg(target_os = "linux")]
        {
            self.source
                .shm
                .as_ref()
                .ok_or(Error::Unavailable("SHM is disabled"))?
                .allocate(length)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = length;
            Err(Error::Unavailable("SHM requires Linux"))
        }
    }
    /// Release the last allocation handle and report native retirement. Registrations and
    /// admitted peers keep their owners; dropping the last handle also performs cleanup.
    pub fn free_shm_memory(&self, memory: Arc<HostMemory>) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.source
                .shm
                .as_ref()
                .ok_or(Error::Unavailable("SHM is disabled"))?
                .free(memory)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = memory;
            Err(Error::Unavailable("SHM requires Linux"))
        }
    }
    /// Registers owned host bytes, preparing configured host Direct RDMA MRs
    /// in every usable context, including paused ports, before exposing the range.
    pub fn register_memory(
        &self,
        bytes: Box<[u8]>,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        let _admission = self.source.admit()?;
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.sweep()?;
        }
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
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.sweep()?;
        }
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
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.sweep()?;
        }
        self.source
            .registry
            .register_host_batch(ranges, permission, |memory| self.register_native(memory))
    }

    fn register_native(&self, memory: &[RegisteredMemory]) -> Result<()> {
        #[cfg(feature = "cuda")]
        if let Some(f) = self.source.nvlink.as_ref().and_then(|t| t.fabric.as_ref()) {
            for m in memory
                .iter()
                .filter(|m| m.0.descriptor.profile.permits(Transport::Mnnvl))
            {
                match f.register(m) {
                    Err(e @ Error::Unquiesced { .. }) => {
                        std::mem::forget(m.clone());
                        return Err(e);
                    }
                    Err(e) if m.0.descriptor.profile.route == Transport::Mnnvl => return Err(e),
                    Err(e) => log::warn!("registration unavailable to MNNVL: {e}"),
                    Ok(()) => (),
                }
            }
        }
        #[cfg(target_os = "linux")]
        let pending = self
            .source
            .shm
            .as_ref()
            .filter(|_| {
                memory
                    .first()
                    .is_some_and(|m| m.0.descriptor.profile.permits(Transport::Shm))
            })
            .map(|s| s.prepare_exports(memory))
            .transpose()?;
        #[cfg(feature = "rdma")]
        let result = self
            .source
            .rdma
            .as_ref()
            .filter(|_| {
                memory
                    .first()
                    .is_some_and(|m| m.0.descriptor.profile.permits(Transport::Rdma))
            })
            .map_or(Ok(()), |runtime| {
                runtime.register_memory(memory, deadline(self.config.handler_timeout)?)
            });
        #[cfg(not(feature = "rdma"))]
        let result = Ok(());
        #[cfg(not(feature = "rdma"))]
        let _ = memory;
        #[cfg(target_os = "linux")]
        if result.is_ok() {
            if let Some((shm, pending)) = self.source.shm.as_ref().zip(pending) {
                shm.publish(pending);
            }
        }
        #[cfg(feature = "gds")]
        if result.is_err() {
            let mut cleanup = Ok(());
            for region in memory {
                cleanup = cleanup.and(crate::transport::gds::BufferLease::retire(region));
            }
            cleanup?;
        }
        #[cfg(feature = "metrics")]
        if result.is_ok() && self.metrics.is_some() {
            self.record_registration_routes(memory);
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
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.sweep()?;
        }
        self.source
            .registry
            .register_cuda_batch(ranges, permission, |memory| self.register_native(memory))
    }

    /// Generated SHM basename of this engine's retained mapping. The caller
    /// can verify a C-supplied path without granting authority over an arbitrary file.
    pub fn shm_backing_name(&self, backing: &crate::MemoryBacking) -> Option<String> {
        match backing {
            #[cfg(target_os = "linux")]
            crate::MemoryBacking::Host(h) => {
                h.shared().and_then(|m| m.basename_for(self.source_epoch()))
            }
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Borrow the engine's compiled policy identity for a dynamically supplied request.
    pub fn resolve_policy_name(&self, name: &str) -> Result<crate::PolicyName> {
        self.selector
            .as_ref()
            .and_then(|s| s.interned_name(name))
            .ok_or(Error::InvalidConfig("unknown request policy name"))
    }

    /// Whether this engine installed the named transport. File library and
    /// ring probes remain lazy until the first eligible file request.
    pub fn transport_installed(&self, route: Transport) -> bool {
        match route {
            Transport::Unspec => true,
            Transport::Tcp => self.config.tcp,
            Transport::HpTcp => self.hp_tcp.is_some(),
            #[cfg(target_os = "linux")]
            Transport::Shm => self.source.shm.is_some(),
            #[cfg(feature = "rdma")]
            Transport::Rdma => self.source.rdma.is_some(),
            #[cfg(feature = "cuda")]
            Transport::Nvlink => self.source.metadata.nvlink.is_some(),
            #[cfg(feature = "cuda")]
            Transport::Mnnvl => self.source.metadata.mnnvl.is_some(),
            #[cfg(feature = "gds")]
            Transport::Gds => self.gds.is_some(),
            #[cfg(feature = "io-uring")]
            Transport::IoUring => self.io_uring.is_some(),
            #[allow(unreachable_patterns)]
            _ => false,
        }
    }

    /// Atomically publish host and device ranges in one registry transaction.
    pub fn register_memory_ranges(
        &self,
        ranges: &[(crate::MemoryBacking, usize, usize)],
        permission: Permission,
        route: Transport,
        internal: bool,
    ) -> Result<Vec<RegisteredMemory>> {
        if !self.transport_installed(route) {
            return Err(Error::Unavailable("requested registration transport"));
        }
        if ranges.len() > self.config.max_batch_requests {
            return Err(Error::ResourceExhausted);
        }
        let _admission = self.source.admit()?;
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.sweep()?;
        }
        self.source
            .registry
            .register_batch(ranges, permission, route, internal, |memory| {
                self.register_native(memory)
            })
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
        #[cfg(target_os = "linux")]
        let shm_result = if _last {
            self.source
                .shm
                .as_ref()
                .map_or(Ok(()), |s| s.revoke(memory.id()))
        } else {
            Ok(())
        };
        #[cfg(feature = "gds")]
        let gds_result = if _last {
            crate::transport::gds::BufferLease::retire(memory)
        } else {
            Ok(())
        };
        #[cfg(feature = "cuda")]
        let fabric_result = if _last && Arc::strong_count(&memory.0) == 1 {
            memory.0.fabric.get().map_or(Ok(()), |slot| slot.retire())
        } else {
            Ok(())
        };
        #[cfg(feature = "rdma")]
        if let Some(runtime) = &self.source.rdma {
            runtime.registrations.retire(memory)?;
        }
        #[cfg(feature = "gds")]
        gds_result?;
        #[cfg(target_os = "linux")]
        shm_result?;
        #[cfg(feature = "cuda")]
        fabric_result?;
        Ok(())
    }

    /// Establish an initial P2P identity for an address-only C segment name.
    /// The caller must trust the endpoint at first lookup and pin the result
    /// for the lifetime of the handle. Never call this to refresh a stale handle.
    pub fn discover_peer_epoch(
        &self,
        endpoint: SocketAddr,
        timeout: Duration,
    ) -> Result<SourceEpoch> {
        if endpoint == self.endpoint() {
            return Ok(self.source_epoch());
        }
        let _permit = self.admit_batch(0)?;
        let epoch = control_plane::discover_epoch(endpoint, deadline(timeout)?)?;
        let (_handle, _peer) = self.peers.open(PeerKey { endpoint, epoch })?;
        Ok(epoch)
    }

    /// Open/refresh using an epoch supplied by the authoritative source owner.
    /// Discovery and a newly responding process cannot silently replace it.
    /// The returned descriptor and its clones belong to this requester. A retired
    /// identity can reopen only after all old references are gone and cleanup has
    /// succeeded; this explicit open still validates the supplied epoch remotely.
    pub fn open_segment(
        &self,
        endpoint: SocketAddr,
        expected_epoch: SourceEpoch,
        timeout: Duration,
    ) -> Result<RemoteSegment> {
        let deadline = deadline(timeout)?;
        let _admission = self.admit_batch(0)?;
        let (handle, _peer) = self.peers.open(PeerKey {
            endpoint,
            epoch: expected_epoch,
        })?;
        if endpoint == self.endpoint() && expected_epoch == self.source_epoch() {
            let _permit = self.source.admit()?;
            let (metadata, regions) = self.source.describe()?;
            return Ok(RemoteSegment {
                identity: Some(handle),
                endpoint,
                epoch: expected_epoch,
                regions,
                metadata,
            });
        }
        let mut peer = control_plane::open_segment(endpoint, expected_epoch, deadline)?;
        peer.identity = Some(handle);
        Ok(peer)
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
            self.loaded_gds(),
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

    #[cfg(feature = "gds")]
    fn loaded_gds(&self) -> Option<&crate::transport::gds::GdsTransport> {
        self.gds.as_ref()?.get()
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
        #[cfg(feature = "metrics")]
        let submitted = self
            .metrics
            .as_ref()
            .and_then(crate::metrics::Metrics::timestamp);
        let target = peer.into();
        let peer_permit = match target {
            TransferTarget::Memory(peer) => Some(self.admit_peer(peer)?),
            TransferTarget::File(_) => None,
        };
        let scatter = requests.first().is_some_and(|r| r.require_cancellation);
        if requests.iter().any(|r| r.require_cancellation != scatter) {
            return Err(Error::InvalidConfig("mixed scatter admission modes"));
        }
        if scatter && matches!(target, TransferTarget::File(_)) {
            return Err(Error::Unavailable(
                "scatter requires cancellable direct memory",
            ));
        }
        let mut descriptors = 0usize;
        for r in requests {
            let count = if let Some(parts) = &r.file_ranges {
                if !matches!(target, TransferTarget::File(_)) {
                    return Err(Error::InvalidConfig(
                        "vectored request requires a file target",
                    ));
                }
                if r.local_address != parts.iter().find_map(|p| p.address()).unwrap_or(0)
                    || r.length != parts.iter().map(|p| p.length()).sum::<usize>()
                {
                    return Err(Error::InvalidRange);
                }
                parts.len()
            } else {
                1
            };
            descriptors = descriptors
                .checked_add(count)
                .ok_or(Error::ResourceExhausted)?;
        }
        if descriptors > self.config.max_batch_requests {
            return Err(Error::ResourceExhausted);
        }
        let kind = match target {
            TransferTarget::Memory(_) => crate::SegmentType::Memory,
            TransferTarget::File(_) => crate::SegmentType::File,
        };
        let mut bindings = TransportSelector::bindings(self.selector.as_ref(), kind, requests)?
            .into_iter()
            .peekable();
        let mut payload = match target {
            TransferTarget::Memory(peer) => {
                let missing = requests.iter().any(|r| {
                    matches!(
                        peer.resolve_address(r.remote_address, r.length),
                        Err(Error::RegionNotFound)
                    )
                });
                let peer = if missing {
                    let mut refreshed =
                        control_plane::open_segment(peer.endpoint, peer.epoch, deadline)?;
                    refreshed.identity = peer.identity.clone();
                    refreshed
                } else {
                    peer.clone()
                };
                PreparedTarget::Memory {
                    peer,
                    requests: Vec::new(),
                }
            }
            TransferTarget::File(_) if notification.is_some() => {
                return Err(Error::Unavailable(
                    "file transfers do not support notifications",
                ));
            }
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            TransferTarget::File(segment) => {
                if requests.is_empty()
                    && !self.file_available(Transport::Gds, None)?
                    && !self.file_available(Transport::IoUring, None)?
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
        let mut contracts = Vec::with_capacity(requests.len());
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
            total_bytes = total_bytes
                .checked_add(request.length)
                .ok_or(Error::ResourceExhausted)?;
            contracts.push(contract);
            let (peer, prepared) = match &mut payload {
                PreparedTarget::Memory { peer, requests } => (peer, requests),
                #[cfg(any(feature = "io-uring", feature = "gds"))]
                PreparedTarget::File {
                    segment,
                    requests: files,
                    ..
                } => {
                    files.push(self.prepare_file(segment, operation, request, bound, priority)?);
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
            let (local, local_offset) = self
                .source
                .registry
                .resolve_address(request.local_address, request.length)?;
            let (remote, remote_offset) =
                peer.resolve_address(request.remote_address, request.length)?;
            if !remote.permission.allows(operation == TransferOp::Write) {
                return Err(Error::PermissionDenied);
            }
            let context = SelectionContext {
                local_memory: local.memory_type(),
                local_route: local.0.descriptor.profile.route,
                #[cfg(feature = "cuda")]
                local_ipc: if self.source.metadata.mnnvl.is_some() {
                    local.0.fabric.get().is_some_and(|r| r.local)
                } else {
                    local.ipc_requester()
                },
                #[cfg(not(feature = "cuda"))]
                local_ipc: false,
                remote: &remote,
                local_segment: self.source.metadata.clone(),
                remote_segment: peer.metadata.clone(),
                transfer_size: request.length,
                priority,
                remote_staging: request.remote_staging,
            };
            let selection = context.select_bound(self.selector.as_ref(), request, bound)?;
            let mut selected = selection;
            if request.require_cancellation {
                if !matches!(selected.transport, Transport::Rdma | Transport::HpTcp)
                    || request.remote_staging
                {
                    return Err(Error::Unavailable(
                        "scatter requires a direct cancellable route",
                    ));
                }
                if selected.transport == Transport::Rdma
                    && remote.memory_type() == crate::MemoryType::Cuda
                    && !peer.metadata.rdma_direct_gpu
                {
                    return Err(Error::Unavailable("scatter peer requires CUDA staging"));
                }
                #[cfg(feature = "rdma")]
                if selected.transport == Transport::Rdma
                    && local.memory_type() == crate::MemoryType::Cuda
                    && !self.source.rdma.as_ref().is_some_and(|r| r.direct(&local))
                {
                    return Err(Error::Unavailable(
                        "scatter requester requires CUDA staging",
                    ));
                }
                selected.pin_cancellable();
            }
            let mut used = 0;
            loop {
                match self.validate_memory_route(
                    &selected,
                    &local,
                    local_offset,
                    request.length,
                    request.remote_staging,
                    deadline,
                    &mut rdma_ready,
                ) {
                    Ok(()) => break,
                    Err(error) => {
                        let Some(next) = selected.candidate(used + 1).filter(|_| {
                            used < self.config.max_failover_attempts && failover::retryable(&error)
                        }) else {
                            return Err(error);
                        };
                        used += 1;
                        selected = next;
                    }
                }
            }
            let mut selection = selected;
            selection.initial_attempts = used;
            prepared.push(PreparedTransfer {
                #[cfg(target_os = "linux")]
                shm: None,
                hp_cancel: None,
                #[cfg(feature = "rdma")]
                priority,
                #[cfg(feature = "rdma")]
                rdma_cancel: None,
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
        if let (TransferTarget::File(_), Some(gds)) = (target, self.loaded_gds()) {
            gds.cleanup();
        }
        let mut admission = self.admit_batch(total_bytes)?;
        admission.peer = peer_permit;
        let merge = self.config.merge_requests;
        #[cfg(target_os = "linux")]
        let merge = merge
            && !matches!(&payload,PreparedTarget::Memory{peer,requests} if self.shm_aliases(peer,requests));
        let mapping = Arc::new(match &mut payload {
            PreparedTarget::Memory {
                peer,
                requests: transfers,
            } => TaskMap::merge(
                requests,
                transfers,
                &contracts,
                self.source
                    .metadata
                    .boot
                    .zip(peer.metadata.boot)
                    .is_none_or(|(local, remote)| local == remote),
                operation,
                (
                    self.config.max_transfer_bytes.min(
                        usize::try_from(peer.metadata.max_transfer_bytes).unwrap_or(usize::MAX),
                    ),
                    self.source
                        .metadata
                        .hp
                        .as_ref()
                        .zip(peer.metadata.hp.as_ref())
                        .map_or(usize::MAX, |(a, b)| {
                            usize::try_from(a.max_bytes.min(b.max_bytes)).unwrap_or(usize::MAX)
                        }),
                ),
                merge,
            ),
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            PreparedTarget::File { .. } => TaskMap::identity(requests),
        });
        #[cfg(target_os = "linux")]
        match &mut payload {
            PreparedTarget::Memory { requests, peer } => {
                self.preflight_shm(peer, operation, requests, deadline)?
            }
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            PreparedTarget::File { .. } => {}
        }
        if batch.is_some() {
            match &mut payload {
                PreparedTarget::Memory {
                    requests: transfers,
                    ..
                } => {
                    for transfer in transfers {
                        if transfer.selection.includes(Transport::HpTcp) {
                            transfer.hp_cancel = Some(tokio::sync::watch::Sender::new(false));
                        }
                        #[cfg(feature = "rdma")]
                        if transfer.selection.includes(Transport::Rdma) {
                            transfer.rdma_cancel = Some(Arc::default());
                        }
                    }
                }
                #[cfg(any(feature = "io-uring", feature = "gds"))]
                PreparedTarget::File { .. } => {}
            }
        }
        let owners = self.admit_physical(
            &mut payload,
            &mapping,
            requests,
            &contracts,
            (batch, deadline),
            #[cfg(feature = "metrics")]
            (operation, submitted),
            notification_bytes,
        )?;
        let prepared = PreparedSubmission {
            #[cfg(feature = "gds")]
            gds_pending: Mutex::default(),
            operation,
            payload,
            notification: notification.cloned(),
            deadline,
            owners,
            uncertain: Mutex::default(),
            mapping,
            admission: Some(admission),
        };
        #[cfg(feature = "gds")]
        let prepared = prepared.pin_gds_buffers(self.config.max_failover_attempts)?;
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
        #[cfg(feature = "metrics")]
        prepared.accept_metrics();
        let report = self.execute(&prepared, None);
        if !report.locally_drained {
            // Preserve actual registrations and admission, not a pointer surrogate.
            self.retain_undrained(prepared, None, || {});
        }
        Ok(report)
    }

    pub(crate) fn retain_undrained(
        &self,
        mut prepared: PreparedSubmission,
        progress: Option<(Arc<BatchProgress>, usize)>,
        publish: impl FnOnce(),
    ) {
        prepared.retain_uncertain_owners();
        #[cfg(feature = "gds")]
        let pending = prepared
            .gds_pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        #[cfg(feature = "gds")]
        if let Some(batch) = pending {
            if let Some(gds) = self.loaded_gds() {
                gds.retain(batch, prepared, self.source.clone(), progress, publish);
                return;
            }
            std::mem::forget(batch);
        }
        std::mem::forget((prepared, self.source.clone(), progress));
        publish();
    }

    pub(crate) fn execute(
        &self,
        submission: &PreparedSubmission,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let report = self.execute_physical(submission, progress);
        submission
            .mapping
            .public_report(report, submission.operation)
    }

    pub(crate) fn execute_physical(
        &self,
        submission: &PreparedSubmission,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let report = self.execute_payload(submission, progress);
        #[cfg(feature = "gds")]
        let report = submission.finish_gds_report(report, progress);
        #[cfg(feature = "metrics")]
        for (i, o) in submission.owners.iter().enumerate() {
            if let Some(m) = &o.metrics {
                m.settle_skipped(|| progress.is_some_and(|(p, base)| p.cancel_requested(base + i)));
            }
        }
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
        let (peer, requests) = match payload {
            PreparedTarget::Memory { peer, requests } => (peer, requests),
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            PreparedTarget::File { requests, .. } => {
                let mut first = 0;
                while first < requests.len() {
                    let transport = requests[first].selection.transport;
                    let owner = &submission.owners[first];
                    let owner_deadline = owner.deadline;
                    if let Err(error) = owner.start(|| {
                        progress.is_some_and(|(p, base)| p.cancel_requested(base + first))
                    }) {
                        #[cfg(feature = "metrics")]
                        if let Some(m) = &owner.metrics {
                            m.terminal(Some(&error));
                        }
                        report.failure = Some(TransferFailure {
                            request_index: first,
                            affected_requests: 1,
                            error,
                            remote_effect: RemoteEffect::None,
                        });
                        break;
                    }
                    let count = requests[first..]
                        .iter()
                        .enumerate()
                        .take_while(|(i, r)| {
                            (transport != Transport::Gds
                                || *i == 0
                                || (!r.parts.vectored() && !requests[first].parts.vectored()))
                                && r.selection == requests[first].selection
                                && submission.owners[first + *i].deadline == owner_deadline
                        })
                        .take(if self.runtime_queue.is_some() {
                            1
                        } else {
                            usize::MAX
                        })
                        .count();
                    let group = &requests[first..first + count];
                    let progress = progress.map(|(p, base)| (p, base + first));
                    let result = self.file_group(
                        *operation,
                        transport,
                        group,
                        owner_deadline,
                        progress,
                        submission,
                        #[cfg(feature = "metrics")]
                        &submission.owners[first..first + count],
                    );
                    #[cfg(feature = "gds")]
                    let result = submission.finish_gds_range(result, group, progress);
                    #[cfg(feature = "metrics")]
                    for (i, o) in submission.owners[first..first + count].iter().enumerate() {
                        if let Some(m) = &o.metrics {
                            let failure = result.failure.as_ref().filter(|f| i == f.request_index);
                            m.file_terminal(failure.map(|f| &f.error));
                        }
                    }
                    if result.locally_drained {
                        submission.retire_owners(first, count, result.completed_requests);
                    } else {
                        submission.retain_window(first..first + count);
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
        let mut first_interruption = None;
        // A queue drop or an accepted task cancel retires just this owner.
        // Later owners on the same worker can still carry independent payload.
        let mut interrupt = |index: usize, reason: Error, effect: RemoteEffect| {
            if let Some((p, base)) = progress {
                let task_error = if matches!(reason, Error::DeadlineInfeasible) {
                    Error::DeadlineInfeasible
                } else {
                    Error::Cancelled
                };
                p.fail_owners(
                    base,
                    TransferFailure {
                        request_index: index,
                        affected_requests: 1,
                        error: task_error,
                        remote_effect: effect,
                    },
                    true,
                );
            }
            let first = first_interruption.get_or_insert(TransferFailure {
                request_index: index,
                affected_requests: 1,
                error: reason,
                remote_effect: effect,
            });
            if effect == RemoteEffect::Unknown {
                first.remote_effect = effect;
            }
            submission.retire_owners(index, 1, 0);
        };
        while index < requests.len() {
            let hp_count = self.hp_group_len(submission, &requests[index..], index);
            if hp_count > 1 {
                let result = self.hp_group(
                    submission,
                    peer,
                    &requests[index..index + hp_count],
                    index,
                    progress,
                );
                report.completed_requests += result.completed_requests;
                report.transferred_bytes += result.transferred_bytes;
                if let Some(mut f) = result.failure {
                    f.request_index += index;
                    report.failure = Some(f);
                    report.locally_drained = result.locally_drained;
                    break;
                }
                index += hp_count;
                continue;
            }
            let request = &requests[index];
            let owner = &submission.owners[index];
            let owner_deadline = owner.deadline;
            let cancelled = || progress.is_some_and(|(p, base)| p.cancel_requested(base + index));
            let start = owner.start(cancelled).and_then(|()| {
                if progress.is_some_and(|(p, base)| !p.begin_task(base + index)) {
                    Err(Error::Cancelled)
                } else {
                    Ok(())
                }
            });
            if let Err(error) = start {
                #[cfg(feature = "metrics")]
                if let Some(m) = &owner.metrics {
                    m.terminal(Some(&error));
                }
                if matches!(error, Error::DeadlineInfeasible)
                    || (matches!(error, Error::Cancelled) && cancelled())
                {
                    interrupt(index, error, RemoteEffect::None);
                    index += 1;
                    continue;
                }
                report.failure = Some(TransferFailure {
                    request_index: index,
                    affected_requests: 1,
                    error,
                    remote_effect: RemoteEffect::None,
                });
                break;
            }
            #[cfg(feature = "metrics")]
            if let Some(m) = &owner.metrics {
                m.dispatched();
            }
            let credit = owner.credit();
            if let Some(c) = &credit {
                if let Err(error) = c.commit() {
                    #[cfg(feature = "metrics")]
                    if let Some(m) = &owner.metrics {
                        m.terminal(Some(&error));
                    }
                    report.failure = Some(TransferFailure {
                        request_index: index,
                        affected_requests: 1,
                        error,
                        remote_effect: RemoteEffect::None,
                    });
                    break;
                }
            }
            #[cfg(feature = "cuda")]
            let cohort = if matches!(
                request.selection.transport,
                Transport::Nvlink | Transport::Mnnvl
            ) {
                crate::transport::nvlink::NVLinkTransport::group_len(
                    &requests[index..index + submission.native_group(index)],
                    self.config.max_transfer_bytes,
                    owner_deadline,
                )
            } else {
                1
            };
            #[cfg(not(feature = "cuda"))]
            let cohort = 1;
            let group = &requests[index..index + cohort];
            let progress_at = progress.map(|(p, base)| (p, base + index));
            let mut posted = false;
            let result = self.memory_attempt(
                *operation,
                peer,
                group,
                &submission.owners[index..index + cohort],
                owner_deadline,
                credit.as_deref(),
                progress_at,
                &mut posted,
            );
            #[cfg(feature = "metrics")]
            for (i, o) in submission.owners[index..index + cohort].iter().enumerate() {
                if let Some(m) = &o.metrics {
                    let err = result.as_ref().err().filter(|f| {
                        i >= f.request_index && i < f.request_index + f.affected_requests
                    });
                    m.finish(err.map(|f| &f.error), posted);
                }
            }
            let result = match result {
                Err(failure) => self.retry_memory_group(
                    *operation,
                    peer,
                    group,
                    &submission.owners[index..index + cohort],
                    owner_deadline,
                    credit.as_deref(),
                    progress_at,
                    posted,
                    failure,
                ),
                success => success,
            };
            let completed = match &result {
                Ok(count) => *count,
                Err(failure) => failure.request_index,
            };
            #[cfg(feature = "metrics")]
            for (i, o) in submission.owners[index..index + cohort].iter().enumerate() {
                if let Some(m) = &o.metrics {
                    if i < completed {
                        m.terminal(None);
                    } else if let Err(f) = &result {
                        if i < f.request_index + f.affected_requests {
                            m.terminal(Some(&f.error));
                        }
                    }
                }
            }
            for (next, request) in requests[index..index + completed].iter().enumerate() {
                report.completed_requests += 1;
                report.transferred_bytes += request.length;
                if let Some((progress, first_task)) = progress {
                    progress.complete_task(first_task + index + next, request.length);
                }
            }
            // Earlier completed physical owners have a transport receipt. Release
            // their windows before waiting for another owner in this submission.
            if matches!(&result,Err(f) if matches!(f.error,Error::Unquiesced{..})) {
                submission.retain_window(index..index + cohort);
            } else {
                submission.retire_owners(index, completed, completed);
            }
            if let Err(mut failure) = result {
                if request.selection.transport == Transport::Rdma
                    && matches!(failure.error, Error::Cancelled)
                    && cancelled()
                {
                    interrupt(index, failure.error, failure.remote_effect);
                    index += 1;
                    continue;
                }
                report.locally_drained = !matches!(failure.error, Error::Unquiesced { .. });
                failure.request_index += index;
                report.failure = Some(failure);
                break;
            }
            index += completed;
        }
        if let Some(mut interrupted) = first_interruption {
            // The report counts the successful public prefix. Async task
            // statuses retain independent successes after the first gap.
            let first = interrupted.request_index;
            let mut end = index;
            if *operation == TransferOp::Write && report.completed_requests > first {
                interrupted.remote_effect = RemoteEffect::Unknown;
            }
            if let Some(failure) = report.failure.take() {
                end = failure.request_index + failure.affected_requests;
                if failure.remote_effect == RemoteEffect::Unknown {
                    interrupted.remote_effect = RemoteEffect::Unknown;
                }
                if let Error::Unquiesced { operation, code } = &failure.error {
                    interrupted.error = Error::Unquiesced {
                        operation,
                        code: *code,
                    };
                }
                if let Some((p, base)) = progress {
                    p.fail_owners(base, failure, report.locally_drained);
                }
            }
            report.completed_requests = first;
            report.transferred_bytes = submission.mapping.owners[..first]
                .iter()
                .map(|o| o.length)
                .sum();
            interrupted.affected_requests = end - first;
            report.failure = Some(interrupted);
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
        let _peer = self.admit_peer(peer)?;
        self.notify(peer, notification, deadline)
    }

    /// Atomically drains this engine's shared inbox in enqueue order. An empty
    /// poll returns an empty vector; concurrent receivers never share a message.
    /// Already queued messages remain drainable after source admission closes.
    pub fn receive_notifications(&self) -> Result<Vec<Notification>> {
        self.notification_routes.receive(&self.source.notifications)
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
        self.notify_target(peer.into(), notification, deadline)
            .map_err(|failure| failure.outcome)
    }

    pub(crate) fn local_target(&self) -> Target {
        Target {
            endpoint: self.endpoint(),
            epoch: self.source_epoch(),
        }
    }
    pub(crate) fn notify_target(
        &self,
        peer: Target,
        notification: &Notification,
        deadline: Instant,
    ) -> crate::notification::Attempt {
        remaining(deadline)?;
        if peer == self.local_target() {
            self.send_local_notification(notification)
                .map_err(Into::into)
        } else {
            self.notification_routes.send(peer, notification, deadline)
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
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.fence_exports()?;
        }
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
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.drain_exports(deadline)?;
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
            if let Some(gds) = self.loaded_gds() {
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
            let duration = if self.loaded_gds().is_some() {
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
        self.peers.close_cleanup(deadline)?;
        self.files.close_all(deadline)?;
        #[cfg(feature = "io-uring")]
        if let Some(io) = &self.io_uring {
            io.close()?;
        }
        #[cfg(feature = "cuda")]
        if let Some(transport) = &self.source.nvlink {
            transport.close_requester(self.source.epoch, deadline)?;
        }
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            shm.close_imports(deadline)?;
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
            #[cfg(target_os = "linux")]
            if let Some(shm) = &self.source.shm {
                shm.fence_exports()?;
            }
        }
        // Peers may export to each other. Release outgoing imports before
        // waiting for incoming exports, keeping close receipts serviceable.
        self.close_requester_until(deadline)?;
        self.close_source_until(deadline)?;
        if let Some(hp) = &self.hp_tcp {
            hp.stop()?;
        }
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
            peer: None,
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
    #[cfg(target_os = "linux")]
    pub(crate) shm: Option<crate::transport::shm::ShmTransport>,
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
    pub(crate) tcp_notifications: bool,
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
    pub(crate) fn describe(&self) -> Result<(SegmentMetadata, Vec<RemoteRegion>)> {
        let regions = self.registry.descriptors(self.metadata.hp.is_some())?;
        #[allow(unused_mut)]
        let mut metadata = self.metadata.clone();
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.shm {
            metadata.shm = Some(Arc::new(shm.manifest(&regions)?));
        }
        Ok((metadata, regions))
    }
    pub(crate) fn admit(self: &Arc<Self>) -> Result<SourcePermit> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| Error::Internal("source lock poisoned"))?;
        if !admission.open {
            return Err(Error::SourceClosed);
        }
        admission.active += 1;
        Ok(SourcePermit(Arc::clone(self)))
    }
}

/// Admission travels with the accepted socket when a source session is dispatched.
pub(crate) struct SourcePermit(Arc<Source>);

impl Drop for SourcePermit {
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
    pub(crate) mapping: Arc<TaskMap>,
    owners: Vec<OwnerAdmission>,
    uncertain: Mutex<Option<std::ops::Range<usize>>>,
    pub(crate) admission: Option<BatchPermit>,
}

#[cfg(feature = "gds")]
impl PreparedSubmission {
    fn pin_gds_buffers(mut self, max_attempts: usize) -> Result<Self> {
        if let PreparedTarget::File { requests, .. } = &mut self.payload {
            for r in requests {
                if r.selection.transport == Transport::Gds
                    || (max_attempts != 0 && r.selection.includes(Transport::Gds))
                {
                    match r.parts.as_mut_slice().iter_mut().try_for_each(|part| {
                        part.buffer =
                            crate::transport::gds::BufferLease::acquire(part.registered()?)?;
                        Ok(())
                    }) {
                        Ok(()) => (),
                        // Unregister can retire a GDS slot after selection. It
                        // cannot invalidate the independent file lease/CPU route.
                        Err(Error::Unavailable(_)) if r.selection.omit_gds(max_attempts) => {}
                        Err(e) => return Err(self.abort_error(e)),
                    }
                }
            }
        }
        Ok(self)
    }

    fn finish_gds_report(
        &self,
        report: BatchReport,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let PreparedTarget::File { requests, .. } = &self.payload else {
            return report;
        };
        self.finish_gds_range(report, requests, progress)
    }
    fn finish_gds_range(
        &self,
        mut report: BatchReport,
        requests: &[crate::segment::file::FileRequest],
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        if report.locally_drained {
            if let Err((index, error)) = Self::release_buffers(requests) {
                // A cancelled/queued group can be the last registration borrower.
                // Its admission stays charged even if it never obtained a native batch.
                *self.gds_pending.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(crate::transport::gds::Quarantine::BufferRelease);
                if let Some((p, base)) = progress {
                    p.fail_file_task(
                        base + index,
                        0,
                        error.for_report(),
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
            Self::release_buffers(requests)?;
        }
        Ok(())
    }
    fn release_buffers(
        requests: &[crate::segment::file::FileRequest],
    ) -> std::result::Result<(), (usize, Error)> {
        for (index, request) in requests.iter().enumerate() {
            request.parts.release_buffers().map_err(|e| (index, e))?;
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
            std::mem::forget((self.admission.take(), std::mem::take(&mut self.owners)));
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
pub(crate) struct BatchPermit {
    batches: Arc<RequesterAdmission>,
    bytes: usize,
    // Dropped after the payload and owner permits, or retained with quarantine.
    peer: Option<Arc<PeerPermit>>,
}
impl BatchPermit {
    fn retain_bytes(&mut self, bytes: usize) {
        let mut usage = self.batches.usage.lock().unwrap_or_else(|p| p.into_inner());
        usage.bytes -= self.bytes - bytes;
        self.bytes = bytes;
    }
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
