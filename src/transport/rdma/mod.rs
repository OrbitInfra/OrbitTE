//! RC verbs with per-NIC contexts, shared progress and owned memory grants.

#![allow(unsafe_code)]

use std::sync::{Arc, Condvar, Mutex};

use crate::{Error, Result};

mod buffers;
mod context;
mod cq;
mod discovery;
mod endpoint;
mod endpoint_store;
mod events;
mod keys;
mod quota;
mod rail_monitor;
mod rdma_transport;
mod receive;
mod slice;
mod sys;
mod workers;

use context::RdmaContext;
use endpoint::{
    Buffer, ConnectionId, ConnectionResources, Descriptor, GrantResources, Phase, RdmaEndpoint,
    Resources, Staging,
};

pub(crate) const DESCRIPTOR_SIZE: usize = 40;
pub(super) const WR_ID_STRIDE: u64 = 128;
pub(super) const COMPLETION_MESSAGE_SIZE: usize = 141;

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

/// A retained endpoint is charged and blocks further RDMA admission. It can
/// still own pinned pages or RNIC resources after native teardown failed.
#[derive(Clone, Copy, Debug, Default)]
pub struct RdmaStats {
    /// Shared context/PD owners, including partially constructed retained ones.
    pub active_devices: usize,
    pub retained_devices: usize,
    /// Successful context/PD constructions during this runtime lifetime.
    pub opened_devices: u64,
    pub active_windows: usize,
    pub opened_windows: u64,
    /// Historical native MW indices charged for the lifetime of their PD.
    pub window_key_indices: usize,
    /// Full rkeys burned before an endpoint can submit BIND, including failures.
    pub burned_window_keys: u64,
    pub window_key_exhaustions: u64,
    pub retained_windows: usize,
    pub completed_window_binds: u64,
    pub completed_window_invalidations: u64,
    pub receive_work_requests: usize,
    pub retained_receive_work_requests: usize,
    pub peak_receive_work_requests: usize,
    pub receive_post_calls: u64,
    pub completed_receives: u64,
    pub completed_sends: u64,
    pub active_message_mrs: usize,
    pub retained_message_mrs: usize,
    pub message_bytes: usize,
    pub retained_message_bytes: usize,
    pub active_completion_channels: usize,
    pub opened_completion_channels: u64,
    pub retained_completion_channels: usize,
    pub completion_event_workers: usize,
    /// Successful native gets, each acknowledged before notifying progress.
    pub completion_events: u64,
    pub cq_notification_arms: u64,
    pub cq_event_waits: u64,
    pub active_cqs: usize,
    pub retained_cqs: usize,
    pub opened_cqs: u64,
    pub progress_workers: usize,
    pub async_event_workers: usize,
    pub acknowledged_async_events: u64,
    /// A fatal shared-lane fault permanently fences new RDMA admission.
    pub progress_failures: u64,
    /// Highest pending data-QP count observed by any single context worker.
    pub peak_pending_qps: usize,
    pub active_endpoints: usize,
    pub idle_endpoints: usize,
    pub opened_connections: u64,
    pub closed_connections: u64,
    pub reused_connections: u64,
    pub evicted_connections: u64,
    pub completed_reusable_grants: u64,
    /// Per-request memory owners, including outputs after physical QP close.
    pub active_grants: usize,
    pub retained_grants: usize,
    pub staging_bytes: usize,
    /// Direct host bytes charged through endpoint release, including retention.
    pub direct_bytes: usize,
    /// Successfully constructed direct host endpoints during this runtime.
    pub opened_direct_endpoints: u64,
    pub retained_endpoints: usize,
    pub retained_bytes: usize,
    /// All cached physical MRs, including creation, closing and retained failures.
    pub persistent_mrs: usize,
    pub persistent_mr_bytes: usize,
    pub retained_persistent_mrs: usize,
    pub opened_persistent_mrs: u64,
    pub reused_persistent_mrs: u64,
    pub evicted_persistent_mrs: u64,
    /// MW-bind parent subset of persistent_mrs; never remote-enabled MRs.
    pub persistent_source_mrs: usize,
    pub opened_source_mrs: u64,
    pub reused_source_mrs: u64,
    /// Source parents retained by failed deregistration. Failed MW cleanup keeps
    /// its parent borrow charged in persistent_source_mrs without a dereg attempt.
    pub retained_source_mrs: usize,
    pub completed_reads: u64,
    pub completed_writes: u64,
    pub completed_read_bytes: u64,
    pub completed_write_bytes: u64,
    /// Reserved or possibly posted WR slots pending CQ validation or teardown.
    pub work_requests: usize,
    pub retained_work_requests: usize,
    pub peak_work_requests: usize,
    pub posted_work_requests: u64,
    pub post_calls: u64,
    pub partial_posts: u64,
    pub uncertain_posts: u64,
    pub completion_errors: u64,
}

pub(crate) struct RdmaTransport {
    config: RdmaConfig,
    usage: Arc<Mutex<RdmaStats>>,
    counters: Arc<events::Counters>,
    cq_quota: Arc<quota::CqQuota>,
    verbs: &'static sys::Verbs,
    context_set: Mutex<ContextSet>,
    drained: Arc<Condvar>,
    pub(crate) registrations: Arc<buffers::LocalBufferManager>,
}

#[derive(Default)]
struct ContextSet {
    closed: bool,
    topology: Option<crate::Topology>,
    slots: Vec<ContextSlot>,
    selector: Option<Arc<quota::DeviceSelector>>,
    failure: Option<(&'static str, i32)>,
}

#[derive(Default)]
struct ContextSlot {
    owner: Option<Arc<RdmaContext>>,
    progress: Option<Arc<workers::Workers>>,
}

fn check(operation: &'static str, code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::Backend { operation, code })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/mod.rs"]
pub(crate) mod tests;
