use std::sync::{Arc, Mutex};

use super::super::RdmaStats;
use crate::{Error, Result};

/// Reserves all grants before a parent exposes any remote authority. The
/// reservation survives the parent when native cleanup retains a child.
pub(in crate::transport::rdma) struct GrantCredits {
    usage: Arc<Mutex<RdmaStats>>,
    count: usize,
    available: Mutex<usize>,
}

impl GrantCredits {
    pub(in crate::transport::rdma) fn acquire(
        usage: Arc<Mutex<RdmaStats>>,
        capacity: usize,
        count: usize,
    ) -> Result<Arc<Self>> {
        if count == 0 || count > capacity {
            return Err(Error::InvalidRange);
        }
        {
            let mut stats = usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            let next = stats
                .grant_credits
                .checked_add(count)
                .ok_or(Error::ResourceExhausted)?;
            if next > capacity {
                return Err(Error::ResourceExhausted);
            }
            stats.grant_credits = next;
        }
        Ok(Arc::new(Self {
            usage,
            count,
            available: Mutex::new(count),
        }))
    }

    pub(in crate::transport::rdma) fn take(self: &Arc<Self>) -> Result<GrantCredit> {
        let mut available = self
            .available
            .lock()
            .map_err(|_| Error::Internal("RDMA grant reservation poisoned"))?;
        *available = available.checked_sub(1).ok_or(Error::ResourceExhausted)?;
        Ok(GrantCredit(Arc::clone(self)))
    }
}

impl Drop for GrantCredits {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .grant_credits -= self.count;
    }
}

pub(in crate::transport::rdma) struct GrantCredit(Arc<GrantCredits>);

impl Drop for GrantCredit {
    fn drop(&mut self) {
        *self.0.available.lock().unwrap_or_else(|p| p.into_inner()) += 1;
    }
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/grant/admission.rs"]
mod tests;
