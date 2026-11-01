use std::path::PathBuf;
use std::time::Duration;

use super::discovery;
use crate::{Error, Result};

/// Host-memory registration policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RdmaHostMemory {
    /// Register the actual host range. Registration errors never select staging.
    #[default]
    Direct,
    /// Isolate application bytes in private host staging until completion.
    Staged,
}

/// NVIDIA device-memory policy. Direct requires native GPU registration support.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RdmaCudaMemory {
    #[default]
    Staged,
    /// Register actual device ranges. Failure never silently selects staging.
    /// Set MLX5_SCATTER_TO_CQE=0 before process startup and keep it unchanged.
    Direct,
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
    /// None prefers active ports but retains valid paused ports; zero is invalid.
    pub port: Option<u8>,
    /// None ranks valid GIDs again on recovery; Some(0) strictly pins GID zero.
    pub gid_index: Option<u8>,
    /// Applied before opening a device. A nonempty whitelist supersedes blacklist.
    pub rdma_whitelist: Vec<String>,
    pub rdma_blacklist: Vec<String>,
    /// TENT location -> [preferred NIC names, available NIC names] JSON.
    /// Takes precedence over custom_json_path. Resolved once against discovery.
    pub priority_matrix: Option<String>,
    /// TENT priority matrix or native nics/mems JSON file (at most 1 MiB).
    /// Overrides ranks, preserving discovered NIC IDs and physical metadata.
    pub custom_json_path: Option<PathBuf>,
    /// Explicit memory-location hint; discovery never assumes allocation placement.
    pub preferred_numa_node: Option<u32>,
    /// TENT strict_local_numa: exclude only known cross-NUMA NICs. Unknown
    /// locality remains eligible; device masks still apply independently.
    pub strict_local_numa: bool,
    /// Allowed topology NIC IDs for dispatch (up to 64); never compacts context slots.
    pub device_mask: u64,
    /// TENT bandwidth-aware selection and payload transmit meter.
    pub scheduling: super::SchedulingParams,
    /// TENT deadline_bw_arbitration: reorder ready payloads within a priority
    /// tier and NIC by predicted completion time / remaining deadline. Off by default.
    pub deadline_bw_arbitration: bool,
    /// TENT aging threshold, measured from the original enqueue (default 10 ms).
    pub priority_promotion_timeout: Duration,
    /// False preserves TENT's head-only tier promotion; true checks each entry.
    pub priority_promotion_per_entry: bool,
    /// Optional POSIX shared-memory name, e.g. /orbitte-rdma-slots. Disabled by default.
    pub shared_quota_shm_path: Option<String>,
    /// Shared High / High+Medium / all rotation; matching participants must agree.
    pub slot_rotation_interval: Duration,
    pub progress_mode: RdmaProgressMode,
    /// TENT lane count: transport workers and SQ/RQ CQ pairs per context (1..=64).
    pub num_lanes: usize,
    pub traffic_class: u8,
    pub hop_limit: u8,
    /// Aggregate physical QP limit, independent of grant concurrency.
    pub max_endpoints: usize,
    /// Whole-parent grant slots, retained until every child drains (1..=4096).
    pub max_grants: usize,
    pub max_staging_bytes: usize,
    pub host_memory: RdmaHostMemory,
    pub cuda_memory: RdmaCudaMemory,
    pub max_direct_bytes: usize,
    /// TENT's base slice size. Large requests adapt to a bounded slice count.
    pub block_size: usize,
    /// Maximum unresolved signaled sends per QP (1..=64). RQ depth uses max_cqes.
    pub max_send_wr: usize,
    /// Each CQ capacity and aggregate SQ/RQ debt limits (1..=4096), charged separately.
    pub max_cqes: usize,
    /// Aggregate physical MR copies, including usable paused contexts/capabilities.
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
            priority_matrix: None,
            custom_json_path: None,
            preferred_numa_node: None,
            strict_local_numa: false,
            device_mask: u64::MAX,
            scheduling: Default::default(),
            deadline_bw_arbitration: false,
            priority_promotion_timeout: Duration::from_millis(10),
            priority_promotion_per_entry: false,
            shared_quota_shm_path: None,
            slot_rotation_interval: Duration::from_millis(2),
            progress_mode: RdmaProgressMode::Polling,
            num_lanes: 1,
            traffic_class: 0,
            hop_limit: 1,
            max_endpoints: 64,
            max_grants: 64,
            max_staging_bytes: 64 << 20,
            host_memory: RdmaHostMemory::Direct,
            cuda_memory: RdmaCudaMemory::Staged,
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
        self.scheduling.validate()?;
        if self.priority_promotion_timeout.as_nanos() > u64::MAX as u128
            || self.slot_rotation_interval.is_zero()
            || self.slot_rotation_interval.as_nanos() > u64::MAX as u128
        {
            return Err(Error::InvalidConfig("invalid RDMA priority timing"));
        }
        if let Some(name) = &self.shared_quota_shm_path {
            super::shared_quota::SharedSlotManager::validate(name, self.slot_rotation_interval)?;
        }
        if self.cuda_memory == RdmaCudaMemory::Direct
            && std::env::var_os("MLX5_SCATTER_TO_CQE").as_deref() != Some(std::ffi::OsStr::new("0"))
        {
            return Err(Error::InvalidConfig(
                "CUDA direct RDMA requires MLX5_SCATTER_TO_CQE=0 at process startup",
            ));
        }

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
            || !(1..=64).contains(&self.num_lanes)
            || self.port == Some(0)
            || self.hop_limit == 0
            || self.max_cached_mrs == 0
            || self.max_cached_mr_bytes == 0
            || !(1..=1 << 24).contains(&self.max_window_key_indices)
            || !(1..=4096).contains(&self.max_cqes)
            || !(1..=4096).contains(&self.max_endpoints)
            || !(1..=4096).contains(&self.max_grants)
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
