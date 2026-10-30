use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::{Error, Result};

/// Host staging location, resolved independently by each GPU owner.
#[cfg(feature = "cuda")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StagingLocation {
    /// CUDA driver allocation (TENT's wildcard `*`); no NUMA placement claim.
    #[default]
    Any,
    /// Prefer the GPU's NUMA node, then the first eligible host node.
    Auto,
    /// Require this allowed, online host node (`cpu:N`); no fallback.
    Cpu(u32),
}

/// Immutable per-engine budgets. No process environment is read or modified.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Cooperative same-host CUDA IPC. Both processes must opt in and keep the
    /// exporter alive until imports close. Handles authorize whole allocations.
    #[cfg(feature = "cuda")]
    pub nvlink: bool,
    /// Maximum persistent CUDA IPC imports and exports, independently per side.
    /// Zero closes imports after each subbatch. Nonzero requires cooperating
    /// peers to close requesters before releasing exporters. Cached exports keep
    /// their whole allocation loan until an explicit native-close receipt.
    #[cfg(feature = "cuda")]
    pub nvlink_cache_entries: usize,
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
    /// Opt-in Pin/Delegate/Unpin sessions for remote CUDA ranges.
    /// Both peers must enable this. TCP/RDMA remain the payload transports;
    /// remote host memory and explicit GPU Direct RDMA are rejected.
    #[cfg(feature = "cuda")]
    pub remote_staging: bool,
    #[cfg(feature = "rdma")]
    pub rdma: Option<crate::transport::rdma::RdmaConfig>,
    pub bind_addr: SocketAddr,
    pub max_registered_regions: usize,
    pub max_registered_bytes: usize,
    /// Maximum descriptors in one transfer or host-registration batch.
    pub max_batch_requests: usize,
    pub max_allocated_batches: usize,
    pub max_allocated_requests: usize,
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
            #[cfg(feature = "cuda")]
            nvlink: false,
            #[cfg(feature = "cuda")]
            nvlink_cache_entries: 0,
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
            bind_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            max_registered_regions: 1024,
            max_registered_bytes: 1 << 30,
            max_batch_requests: 1024,
            max_allocated_batches: 32,
            max_allocated_requests: 32 * 1024,
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
        #[cfg(feature = "cuda")]
        if self.nvlink_cache_entries > 4096 || (self.nvlink_cache_entries != 0 && !self.nvlink) {
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
        #[cfg(all(feature = "cuda", feature = "rdma"))]
        if self.remote_staging
            && self.rdma.as_ref().is_some_and(|config| {
                config.cuda_memory == crate::transport::rdma::RdmaCudaMemory::Direct
            })
        {
            return Err(Error::InvalidConfig(
                "remote staging conflicts with GPU Direct RDMA",
            ));
        }
        if self.max_registered_regions == 0 || self.max_registered_regions > 4096 {
            return Err(Error::InvalidConfig(
                "registered regions must be in 1..=4096",
            ));
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
