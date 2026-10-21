use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::{Error, Result};

/// Immutable per-engine budgets. No process environment is read or modified.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub bind_addr: SocketAddr,
    pub max_registered_regions: usize,
    pub max_registered_bytes: usize,
    pub max_batch_requests: usize,
    pub max_inflight_batches: usize,
    pub max_inflight_bytes: usize,
    pub max_transfer_bytes: usize,
    pub handler_threads: usize,
    pub handler_timeout: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            bind_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            max_registered_regions: 1024,
            max_registered_bytes: 1 << 30,
            max_batch_requests: 1024,
            max_inflight_batches: 32,
            max_inflight_bytes: 64 << 20,
            max_transfer_bytes: 8 << 20,
            handler_threads: 4,
            handler_timeout: Duration::from_secs(5),
        }
    }
}

impl EngineConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_registered_regions == 0 || self.max_registered_regions > 4096 {
            return Err(Error::InvalidConfig(
                "registered regions must be in 1..=4096",
            ));
        }
        if self.max_registered_bytes == 0
            || self.max_batch_requests == 0
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
        if self.handler_timeout.is_zero() || self.handler_timeout > Duration::from_secs(300) {
            return Err(Error::InvalidConfig("handler timeout must be in (0, 300s]"));
        }
        Ok(())
    }
}
