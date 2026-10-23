use std::sync::{Arc, Mutex};

use super::{RdmaContext, RdmaStats, sys};

pub(super) struct RdmaCq {
    pub(super) handles: sys::ConnectionHandles,
    pub(super) _device: Arc<RdmaContext>,
    pub(super) usage: Arc<Mutex<RdmaStats>>,
}

// The single progress thread polls/closes this CQ. Construction precedes spawn.
unsafe impl Send for RdmaCq {}

impl Drop for RdmaCq {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active_cqs -= 1;
    }
}
