use crate::{Error, Result};
use std::time::Duration;

/// Frozen TENT provider profiles. Auto tries EFA, never the software provider.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FabricProvider {
    #[default]
    Auto,
    Efa,
    /// `tcp;ofi_rxm`, including delivery-complete WRITE semantics.
    Tcp,
}

/// Explicit opt-in libfabric configuration. `EngineConfig::fabric` defaults to None.
#[derive(Clone, Debug)]
pub struct FabricConfig {
    pub provider: FabricProvider,
    pub fabric_name: String,
    pub devices: Vec<String>,
    pub slice_size: usize,
    pub idle_sleep_us: u64,
    pub post_timeout_ms: u64,
    pub quiesce_timeout_ms: u64,
    pub op_timeout_ms: u64,
    /// Zero uses provider TX capacity, bounded by max_pending_ops.
    pub max_posted_ops: usize,
    pub max_pte_entries: u64,
    pub max_register_threads: usize,
    /// Zero uses the provider/PTE limit.
    pub max_mr_size: usize,
    /// Additional OrbiTE bounds: pending plus posted slices per domain.
    pub max_pending_ops: usize,
    /// Bounded address-vector entries per domain, including retained failures.
    pub max_peer_addresses: usize,
    pub max_domains: usize,
}
impl Default for FabricConfig {
    fn default() -> Self {
        Self {
            provider: FabricProvider::Auto,
            fabric_name: String::new(),
            devices: Vec::new(),
            slice_size: 512 << 10,
            idle_sleep_us: 50,
            post_timeout_ms: 10_000,
            quiesce_timeout_ms: 10_000,
            op_timeout_ms: 10_000,
            max_posted_ops: 0,
            max_pte_entries: 22 * 1024 * 1024,
            max_register_threads: 8,
            max_mr_size: 0,
            max_pending_ops: 4096,
            max_peer_addresses: 4096,
            max_domains: 16,
        }
    }
}
impl FabricConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.slice_size == 0
            || self.slice_size > 1 << 30
            || !(1..=65536).contains(&self.max_pending_ops)
            || self.max_posted_ops > self.max_pending_ops
            || !(1..=65536).contains(&self.max_peer_addresses)
            || !(1..=16).contains(&self.max_domains)
            || !(1..=64).contains(&self.max_register_threads)
            || self.devices.len() > 64
            || self.fabric_name.len() > 255
            || self.fabric_name.contains('\0')
            || self
                .devices
                .iter()
                .any(|s| s.is_empty() || s.len() > 255 || s.contains('\0'))
            || self.idle_sleep_us > 1_000_000
            || [
                self.post_timeout_ms,
                self.quiesce_timeout_ms,
                self.op_timeout_ms,
            ]
            .iter()
            .any(|&n| n == 0 || n > 3_600_000)
        {
            return Err(Error::InvalidConfig(
                "invalid or unbounded libfabric configuration",
            ));
        }
        Ok(())
    }
    pub(super) fn tcp(&self) -> bool {
        self.provider == FabricProvider::Tcp
    }
    pub(super) fn quiesce(&self) -> Duration {
        Duration::from_millis(self.quiesce_timeout_ms)
    }
}
