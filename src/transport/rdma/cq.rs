use std::sync::{Arc, Mutex};

use super::{RdmaContext, RdmaStats, sys};
use crate::{Error, Result};

pub(super) struct RdmaCq {
    pub(super) handles: sys::ConnectionHandles,
    pub(super) _device: Arc<RdmaContext>,
    pub(super) usage: Arc<Mutex<RdmaStats>>,
}

impl RdmaCq {
    pub(super) fn new(
        device: Arc<RdmaContext>,
        usage: Arc<Mutex<RdmaStats>>,
        capacity: usize,
        verbs: &sys::Verbs,
    ) -> Result<Box<Self>> {
        usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .active_cqs += 1;
        let mut cq = Box::new(Self {
            handles: sys::ConnectionHandles::default(),
            _device: device,
            usage,
        });
        if let Err(error) = cq._device.create_cq(&mut cq.handles, capacity as u32) {
            if let Err((operation, code)) = cq.close(verbs) {
                cq.usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .retained_cqs += 1;
                std::mem::forget(cq);
                return Err(Error::Unquiesced { operation, code });
            }
            return Err(error);
        }
        cq.usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .opened_cqs += 1;
        Ok(cq)
    }

    pub(super) fn close(
        &mut self,
        verbs: &sys::Verbs,
    ) -> std::result::Result<(), (&'static str, i32)> {
        if let Err(Error::Unquiesced { operation, code }) = self._device.asynchronous.check() {
            return Err((operation, code));
        }
        let code = unsafe { (verbs.destroy_cq)(&mut self.handles) };
        if code != 0 {
            return Err(("ibv_destroy_cq", code));
        }
        Ok(())
    }
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
