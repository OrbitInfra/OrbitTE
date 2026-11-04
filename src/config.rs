use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::{Error, Result};

/// Host staging location, resolved independently by each GPU owner.
#[cfg(feature = "cuda")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StagingLocation {
    /// Preserve CUDA driver allocation; no NUMA placement claim.
    #[default]
    Any,
    /// Prefer the GPU's NUMA node, then the first eligible host node.
    Auto,
    /// Require this allowed, online host node (`cpu:N`); no fallback.
    Cpu(u32),
}

/// Opt-in runtime dispatch window across all outgoing data submissions.
/// Total retained work remains bounded by EngineConfig's inflight budgets.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeQueueConfig {
    /// Total queued and dispatching physical owners and their bytes.
    pub max_outstanding_owners: usize,
    pub max_outstanding_bytes: usize,
    /// Capacity which only internal staging sessions may consume.
    pub staging_owner_reserve: usize,
    pub staging_byte_reserve: usize,
    /// Skip a dispatch reaching this predicted deadline MLU. Zero disables it.
    pub mlu_local_threshold: f64,
    /// Promote future deadlines strictly within this slack ahead of other heads.
    pub promotion_slack_ns: u64,
    /// Maximum physical owners holding dispatch reservations simultaneously.
    pub max_dispatch_owners: usize,
    /// Payload bytes in the window, plus notification bytes on the first owner.
    /// An owner larger than this window is rejected before publishing tasks or doing I/O.
    pub max_dispatch_bytes: usize,
    /// Earliest absolute submission deadline first, with FIFO ties. False uses
    /// admission FIFO. Later groups in the same Batch wait for their predecessor.
    /// Running work is not preempted; this is independent of RDMA Priority.
    pub deadline_aware: bool,
}

impl Default for RuntimeQueueConfig {
    fn default() -> Self {
        Self {
            max_outstanding_owners: 1024,
            max_outstanding_bytes: 1 << 30,
            staging_owner_reserve: 0,
            staging_byte_reserve: 0,
            mlu_local_threshold: 0.0,
            promotion_slack_ns: 0,
            max_dispatch_owners: 64,
            max_dispatch_bytes: 64 << 20,
            deadline_aware: false,
        }
    }
}

/// Immutable per-engine budgets and optional provider configuration.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Enable ordinary TCP payloads. The control service is independent.
    pub tcp: bool,
    pub hp_tcp: crate::HpTcpConfig,
    pub shm: crate::ShmConfig,
    /// Typed metrics defaults. The metrics loader resolves overrides at engine construction.
    #[cfg(feature = "metrics")]
    pub metrics: crate::metrics::MetricsConfig,
    /// Optional TENT JSON file containing the metrics section.
    #[cfg(feature = "metrics")]
    pub metrics_config_file: Option<std::path::PathBuf>,
    /// Merge adjacent requests within the same pair of registered ranges.
    pub merge_requests: bool,
    /// Maximum additional attempts per physical owner. Zero pins the first route.
    pub max_failover_attempts: usize,
    /// Status polling may advance a recoverable failure. Explicit progress and wait always can.
    pub enable_auto_failover_on_poll: bool,
    /// Maximum number of 16 MiB slices in one cuFile batch.
    #[cfg(feature = "gds")]
    pub gds_batch_depth: u32,
    /// Exclude GDS from selection if cuFile permits or forces compatibility I/O.
    #[cfg(feature = "gds")]
    pub gds_require_native: bool,
    /// Cooperative same-host CUDA IPC. Both processes must opt in and keep the
    /// exporter alive until imports close. Handles authorize whole allocations.
    #[cfg(feature = "cuda")]
    pub nvlink: bool,
    #[cfg(feature = "cuda")]
    pub mnnvl: Option<crate::MnnvlConfig>,
    /// Maximum persistent CUDA IPC imports and exports, independently per side.
    /// At capacity, cold imports reclaim the least recently used idle mapping
    /// after native close and source acknowledgement. Active/uncertain mappings
    /// stay charged. Zero closes imports after each subbatch. Nonzero requires cooperating
    /// peers to close requesters before releasing exporters. Cached exports keep
    /// their whole allocation loan until an explicit native-close receipt.
    #[cfg(feature = "cuda")]
    pub nvlink_cache_entries: usize,
    /// Shared NVLink stream/event slots across actual CUDA contexts (1..=4096).
    /// Lazy creation, exclusive group leases; uncertain slots remain charged.
    #[cfg(feature = "cuda")]
    pub nvlink_stream_count: usize,
    /// Per-engine lazy CUDA-pinned staging. Local copies check out one chunk;
    /// remote sessions reserve independent source and requester rings. Idle chunks are reused
    /// until engine close. Total capacity is at most 1 GiB.
    #[cfg(feature = "cuda")]
    pub staging_chunk_size: usize,
    #[cfg(feature = "cuda")]
    pub staging_chunk_count: usize,
    /// Applies to engine-owned CUDA staging chunks, including both remote roles.
    #[cfg(feature = "cuda")]
    pub staging_location: StagingLocation,
    /// Maximum staging slots per session and side (1..=16), bounded by the pool.
    /// The source negotiates its width. CUDA requesters reserve up to this cap
    /// before Pin, shortened by the request's local physical-chunk count.
    /// One preserves serial copies; larger values overlap CUDA work and CROSS.
    #[cfg(feature = "cuda")]
    pub staging_pipeline_depth: usize,
    /// Allow incoming Pin/Delegate/Unpin sessions for this engine's CUDA ranges.
    /// Outgoing requests select remote staging independently. TCP/RDMA remain
    /// the payload transports; Direct CUDA registration can coexist with Pin.
    #[cfg(feature = "cuda")]
    pub remote_staging: bool,
    #[cfg(feature = "rdma")]
    pub rdma: Option<crate::transport::rdma::RdmaConfig>,
    pub bind_addr: SocketAddr,
    pub max_registered_regions: usize,
    pub max_registered_bytes: usize,
    /// Open or draining local file segments. Zero disables file segment opens.
    pub max_file_segments: usize,
    /// Maximum descriptors in one transfer or host-registration batch.
    pub max_batch_requests: usize,
    pub max_allocated_batches: usize,
    pub max_allocated_requests: usize,
    /// Enable event-driven batch progress. Configuring a runtime queue also enables it.
    pub enable_progress_worker: bool,
    /// Active runtime queue fallback tick. Zero disables timed wakes.
    pub progress_fallback_interval: Duration,
    /// Allow another notification provider or the control RPC after an unavailable channel.
    pub notification_rpc_fallback: bool,
    /// Install the TCP notification channel. The control RPC is independent.
    pub tcp_notifications: bool,
    /// Optional FIFO/EDF runtime queue. None preserves direct dispatch.
    pub runtime_queue: Option<RuntimeQueueConfig>,
    /// Immutable typed QoS contracts. None preserves caller priorities and adds
    /// no QoS ledger. Resolved budgets cover admitted payload owners until drain.
    /// Allowing QoS delay requires runtime_queue; waiting groups still consume
    /// max_inflight_batches/bytes but acquire QoS capacity at dispatch.
    pub qos: Option<crate::QosConfig>,
    /// Ordered transport policy. None preserves explicit routing; Unspec requests
    /// still use default eligible NVLink -> RDMA -> TCP selection.
    pub selector: Option<crate::TransportSelectorConfig>,
    /// Zero disables receiver credit gating. A positive limit (at most 4096)
    /// requires an activated peer session for every nonempty data submission.
    /// Bindings either accept application updates or connect to a peer issuer.
    pub max_receiver_credit_sessions: usize,
    /// Receiver-side fixed cooperative windows; None allocates no issuer.
    pub receiver_credit: Option<crate::ReceiverCreditConfig>,
    /// Accepted outgoing calls, including queued and unquiesced submissions.
    pub max_inflight_batches: usize,
    pub max_inflight_bytes: usize,
    pub max_transfer_bytes: usize,
    /// Count and payload budgets include queued messages and incoming bodies.
    pub max_pending_notifications: usize,
    pub max_pending_notification_bytes: usize,
    /// Combined UTF-8 bytes of a single notification name and message.
    pub max_notification_bytes: usize,
    pub handler_threads: usize,
    pub handler_timeout: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            tcp: true,
            hp_tcp: Default::default(),
            shm: Default::default(),
            #[cfg(feature = "metrics")]
            metrics: Default::default(),
            #[cfg(feature = "metrics")]
            metrics_config_file: None,
            #[cfg(feature = "gds")]
            gds_batch_depth: 32,
            #[cfg(feature = "gds")]
            gds_require_native: false,
            #[cfg(feature = "cuda")]
            nvlink: false,
            #[cfg(feature = "cuda")]
            mnnvl: std::env::var_os("MC_ENABLE_MNNVL").map(|_| crate::MnnvlConfig::default()),
            #[cfg(feature = "cuda")]
            nvlink_cache_entries: 0,
            #[cfg(feature = "cuda")]
            nvlink_stream_count: 8,
            #[cfg(feature = "cuda")]
            staging_chunk_size: 4 << 20,
            #[cfg(feature = "cuda")]
            staging_chunk_count: 64,
            #[cfg(feature = "cuda")]
            staging_location: StagingLocation::Any,
            #[cfg(feature = "cuda")]
            remote_staging: false,
            #[cfg(feature = "cuda")]
            staging_pipeline_depth: 1,
            #[cfg(feature = "rdma")]
            rdma: None,
            merge_requests: true,
            max_failover_attempts: 3,
            enable_auto_failover_on_poll: true,
            bind_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            max_registered_regions: 1024,
            max_registered_bytes: 1 << 30,
            max_file_segments: 1024,
            max_batch_requests: 1024,
            max_allocated_batches: 32,
            max_allocated_requests: 32 * 1024,
            enable_progress_worker: false,
            progress_fallback_interval: Duration::from_micros(50_000),
            notification_rpc_fallback: true,
            tcp_notifications: true,
            runtime_queue: None,
            qos: None,
            selector: None,
            max_receiver_credit_sessions: 0,
            receiver_credit: None,
            max_inflight_batches: 32,
            max_inflight_bytes: 64 << 20,
            max_transfer_bytes: 8 << 20,
            max_pending_notifications: 1024,
            max_pending_notification_bytes: 1 << 20,
            max_notification_bytes: 64 << 10,
            handler_threads: 4,
            handler_timeout: Duration::from_secs(5),
        }
    }
}

impl EngineConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        self.hp_tcp.validate(self.tcp)?;
        if self.shm.enable
            && !self.shm.cxl_mount_path.as_os_str().is_empty()
            && !self.shm.cxl_mount_path.is_absolute()
        {
            return Err(Error::InvalidConfig("SHM cxl_mount_path must be absolute"));
        }
        #[cfg(not(target_os = "linux"))]
        if self.shm.enable {
            return Err(Error::Unavailable(
                "SHM requires Linux same-machine identity and OFD locks",
            ));
        }
        if let Some(q) = self.runtime_queue {
            if q.staging_owner_reserve > q.max_outstanding_owners
                || q.staging_byte_reserve > q.max_outstanding_bytes
                || !q.mlu_local_threshold.is_finite()
            {
                return Err(Error::InvalidConfig(
                    "invalid runtime queue staging reserve or deadline threshold",
                ));
            }
        }
        if let Some(credit) = self.receiver_credit {
            credit.validate()?;
        }
        if self.max_receiver_credit_sessions > 4096 {
            return Err(Error::InvalidConfig(
                "receiver credit sessions must be at most 4096",
            ));
        }
        if self
            .runtime_queue
            .is_some_and(|queue| queue.max_dispatch_owners == 0 || queue.max_dispatch_bytes == 0)
        {
            return Err(Error::InvalidConfig(
                "runtime queue dispatch window must be positive",
            ));
        }
        #[cfg(feature = "cuda")]
        if !(1..=4096).contains(&self.nvlink_stream_count) {
            return Err(Error::InvalidConfig("NVLink stream count must be 1..=4096"));
        }
        #[cfg(feature = "cuda")]
        if let Some(c) = &self.mnnvl {
            if self.nvlink
                || c.fabric_domain.is_some_and(|d| d.0 == [0; 16])
                || c.async_memcpy_threshold.checked_mul(128 * 1024).is_none()
            {
                return Err(Error::InvalidConfig(
                    "MNNVL requires exclusive NVLink mode, a valid domain and bounded threshold",
                ));
            }
        }
        #[cfg(feature = "cuda")]
        if self.nvlink_cache_entries > 4096
            || (self.nvlink_cache_entries != 0 && !self.nvlink && self.mnnvl.is_none())
        {
            return Err(Error::InvalidConfig(
                "NVLink cache requires NVLink and at most 4096 entries",
            ));
        }
        #[cfg(feature = "cuda")]
        if !(1..=16).contains(&self.staging_pipeline_depth)
            || self.staging_pipeline_depth > self.staging_chunk_count
        {
            return Err(Error::InvalidConfig(
                "staging pipeline depth must be 1..=16 and fit the pool",
            ));
        }
        #[cfg(feature = "rdma")]
        if let Some(config) = &self.rdma {
            config.validate()?;
        }
        #[cfg(feature = "gds")]
        if !(1..=65536).contains(&self.gds_batch_depth) {
            return Err(Error::InvalidConfig("GDS batch depth must be in 1..=65536"));
        }
        if self.max_registered_regions == 0 || self.max_registered_regions > 4096 {
            return Err(Error::InvalidConfig(
                "registered regions must be in 1..=4096",
            ));
        }
        if self.max_file_segments > 4096 {
            return Err(Error::InvalidConfig("at most 4096 open file segments"));
        }
        if self.max_registered_bytes == 0
            || self.max_batch_requests == 0
            || self.max_allocated_batches == 0
            || self.max_allocated_requests == 0
            || self.max_inflight_batches == 0
            || self.max_inflight_bytes == 0
            || self.max_transfer_bytes == 0
            || self.max_transfer_bytes > 64 << 20
        {
            return Err(Error::InvalidConfig(
                "budgets must be positive; each transfer is at most 64 MiB",
            ));
        }
        if self.handler_threads == 0 || self.handler_threads > 128 {
            return Err(Error::InvalidConfig("handler threads must be in 1..=128"));
        }
        if self.max_pending_notifications == 0
            || self.max_pending_notification_bytes == 0
            || self.max_notification_bytes == 0
            || self.max_notification_bytes > 64 << 10
            || self.max_notification_bytes > self.max_pending_notification_bytes
        {
            return Err(Error::InvalidConfig(
                "notification budgets must be positive; each message fits the inbox and is at most 64 KiB",
            ));
        }
        if self.handler_timeout.is_zero() || self.handler_timeout > Duration::from_secs(300) {
            return Err(Error::InvalidConfig("handler timeout must be in (0, 300s]"));
        }
        Ok(())
    }
}
