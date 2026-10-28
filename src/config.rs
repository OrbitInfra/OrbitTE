use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::{Error, Result};

/// Immutable per-engine budgets. No process environment is read or modified.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Cooperative same-host CUDA IPC. Both processes must opt in and keep the
    /// exporter alive until imports close. Handles authorize whole allocations.
    #[cfg(feature = "cuda")]
    pub nvlink: bool,
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
        #[cfg(feature = "rdma")]
        if let Some(config) = &self.rdma {
            config.validate()?;
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
