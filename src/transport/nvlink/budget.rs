use std::sync::{Arc, Mutex};

use crate::{Error, Result};

/// Physical whole-allocation IPC charges, sampled independently of batch bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct NVLinkStats {
    pub exports: usize,
    pub imports: usize,
    pub allocation_bytes: usize,
    pub retained_exports: usize,
    pub retained_imports: usize,
}

pub(super) struct MappingBudget {
    limit: usize,
    usage: Mutex<NVLinkStats>,
}

impl MappingBudget {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            usage: Mutex::default(),
        })
    }

    pub(super) fn stats(&self) -> Result<NVLinkStats> {
        self.usage
            .lock()
            .map(|usage| *usage)
            .map_err(|_| Error::Internal("NVLink budget lock poisoned"))
    }

    pub(super) fn reserve(self: &Arc<Self>, extent: usize, export: bool) -> Result<MappingPermit> {
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("NVLink budget lock poisoned"))?;
        let total = usage
            .allocation_bytes
            .checked_add(extent)
            .ok_or(Error::ResourceExhausted)?;
        if extent == 0 || total > self.limit {
            return Err(Error::ResourceExhausted);
        }
        usage.allocation_bytes = total;
        if export {
            usage.exports += 1;
        } else {
            usage.imports += 1;
        }
        Ok(MappingPermit {
            budget: Arc::clone(self),
            extent,
            export,
        })
    }
}

pub(super) struct MappingPermit {
    budget: Arc<MappingBudget>,
    extent: usize,
    export: bool,
}

impl MappingPermit {
    pub(super) fn retain(self) {
        {
            let mut usage = self.budget.usage.lock().unwrap_or_else(|p| p.into_inner());
            if self.export {
                usage.retained_exports += 1;
            } else {
                usage.retained_imports += 1;
            }
        }
        std::mem::forget(self);
    }
}

impl Drop for MappingPermit {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock().unwrap_or_else(|p| p.into_inner());
        usage.allocation_bytes -= self.extent;
        if self.export {
            usage.exports -= 1;
        } else {
            usage.imports -= 1;
        }
    }
}
