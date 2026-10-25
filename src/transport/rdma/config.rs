use super::discovery;
use crate::{Error, Result};

/// Host-memory policy. CUDA allocations continue to use owned host staging.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RdmaHostMemory {
    /// Register the actual host range. Registration errors never select staging.
    #[default]
    Direct,
    /// Isolate application bytes in private host staging until completion.
    Staged,
}

/// Shared CQ progress policy. The frozen TENT polling path remains the default.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RdmaProgressMode {
    #[default]
    Polling,
    /// Arm both CQs before polling; native events wake the existing data worker.
    EventDriven,
}

/// Automatic or pinned NIC discovery and bounded request-level rail selection.
#[derive(Clone, Debug)]
pub struct RdmaConfig {
    /// None discovers an accessible provider device; Some is a strict pin.
    pub device: Option<String>,
    /// None discovers an active port; port zero is never valid.
    pub port: Option<u8>,
    /// None ranks valid GIDs; Some(0) explicitly selects GID zero.
    pub gid_index: Option<u8>,
    /// Applied before opening a device. A nonempty whitelist supersedes blacklist.
    pub rdma_whitelist: Vec<String>,
    pub rdma_blacklist: Vec<String>,
    /// Explicit memory-location hint; discovery never assumes allocation placement.
    pub preferred_numa_node: Option<u32>,
    /// Allowed topology NIC IDs for dispatch (up to 64); never compacts context slots.
    pub device_mask: u64,
    pub progress_mode: RdmaProgressMode,
    /// Transport-level data workers, independent of the number of NIC contexts (1..=64).
    pub num_workers: usize,
    pub traffic_class: u8,
    pub hop_limit: u8,
    pub max_endpoints: usize,
    pub max_staging_bytes: usize,
    pub host_memory: RdmaHostMemory,
    pub max_direct_bytes: usize,
    /// TENT's base slice size. Large requests adapt to a bounded slice count.
    pub block_size: usize,
    /// Maximum signaled work requests in one request QP window (1..=64).
    pub max_send_wr: usize,
    /// Each CQ capacity and aggregate SQ/RQ debt limits (1..=4096), charged separately.
    pub max_cqes: usize,
    /// Aggregate physical MR copies, including every enabled context/capability.
    /// Published host Direct registrations are pinned until unregister or close.
    pub max_cached_mrs: usize,
    /// Aggregate full-view bytes of every physical MR copy.
    pub max_cached_mr_bytes: usize,
    /// Historical MW indices retained without eviction until the PD is destroyed.
    pub max_window_key_indices: usize,
}

impl Default for RdmaConfig {
    fn default() -> Self {
        Self {
            device: None,
            port: None,
            gid_index: None,
            rdma_whitelist: Vec::new(),
            rdma_blacklist: Vec::new(),
            preferred_numa_node: None,
            device_mask: u64::MAX,
            progress_mode: RdmaProgressMode::Polling,
            num_workers: 1,
            traffic_class: 0,
            hop_limit: 1,
            max_endpoints: 64,
            max_staging_bytes: 64 << 20,
            host_memory: RdmaHostMemory::Direct,
            max_direct_bytes: 64 << 20,
            block_size: 64 << 10,
            max_send_wr: 16,
            max_cqes: 1024,
            max_cached_mrs: 64,
            max_cached_mr_bytes: 64 << 20,
            max_window_key_indices: 65_536,
        }
    }
}

impl RdmaConfig {
    /// Preserve explicit device/port 1/GID 0 selection. Use Default for discovery.
    pub fn new(device: impl Into<String>) -> Self {
        Self {
            device: Some(device.into()),
            port: Some(1),
            gid_index: Some(0),
            ..Self::default()
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let valid_name = |name: &str| {
            !name.is_empty()
                && name.len() <= 128
                && !name.contains(['\0', '/'])
                && !matches!(name, "." | "..")
        };
        if self.device.as_deref().is_some_and(|name| !valid_name(name))
            || self
                .rdma_whitelist
                .iter()
                .chain(&self.rdma_blacklist)
                .any(|name| !valid_name(name))
            || self
                .device
                .as_deref()
                .is_some_and(|name| !discovery::permitted(self, name))
            || self.device_mask == 0
            || !(1..=64).contains(&self.num_workers)
            || self.port == Some(0)
            || self.hop_limit == 0
            || self.max_cached_mrs == 0
            || self.max_cached_mr_bytes == 0
            || !(1..=1 << 24).contains(&self.max_window_key_indices)
            || !(1..=4096).contains(&self.max_cqes)
            || !(1..=4096).contains(&self.max_endpoints)
            || self.max_staging_bytes == 0
            || self.max_direct_bytes == 0
            || !(1..=64 << 20).contains(&self.block_size)
            || !(1..=64).contains(&self.max_send_wr)
        {
            return Err(Error::InvalidConfig(
                "RDMA needs valid selectors, a nonzero hop limit and positive budgets",
            ));
        }
        Ok(())
    }
}
