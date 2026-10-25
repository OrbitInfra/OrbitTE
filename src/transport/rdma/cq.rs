use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::{RdmaContext, RdmaStats, check, events, sys};
use crate::{Error, Result};

pub(super) struct CompletionEvents {
    resources: Mutex<Option<Box<ChannelResources>>>,
    children: AtomicUsize,
    asynchronous: Arc<events::State>,
}

struct ChannelResources {
    handles: sys::ChannelHandles,
    _device: Arc<RdmaContext>,
    usage: Arc<Mutex<RdmaStats>>,
    verbs: sys::CqEventVerbs,
    registration: Option<super::monitor::Registration>,
    charged: bool,
    retained: bool,
    failure: Option<(&'static str, i32)>,
}

impl ChannelResources {
    fn retain(&mut self) {
        if self.charged && !self.retained {
            self.retained = true;
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .retained_completion_channels += 1;
        }
    }
}

impl Drop for ChannelResources {
    fn drop(&mut self) {
        if self.charged {
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .active_completion_channels -= 1;
        }
    }
}

impl CompletionEvents {
    pub(super) fn new(
        device: Arc<RdmaContext>,
        usage: Arc<Mutex<RdmaStats>>,
        verbs: &'static sys::Verbs,
    ) -> Result<Arc<Self>> {
        let native = verbs
            .completion
            .ok_or(Error::Unavailable("RDMA CQ events are unavailable"))?;
        let asynchronous = Arc::clone(&device.asynchronous);
        let mut resources = Box::new(ChannelResources {
            handles: sys::ChannelHandles::default(),
            _device: device,
            usage,
            verbs: native,
            registration: None,
            charged: false,
            retained: false,
            failure: None,
        });
        let created = resources
            ._device
            .create_completion_channel(&mut resources.handles, native);
        if !resources.handles.channel.is_null() {
            resources.charged = true;
            let mut usage = resources.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.active_completion_channels += 1;
            usage.opened_completion_channels += 1;
        }
        let initialized = created.and_then(|()| {
            let channel = NonNull::new(resources.handles.channel)
                .ok_or(Error::Internal("missing RDMA completion channel"))?;
            resources.registration = Some(resources._device.monitor.register(
                super::monitor::Source::Completion {
                    channel,
                    fd: resources.handles.fd,
                    verbs: native,
                },
                Arc::clone(&asynchronous),
                Arc::clone(&resources._device.counters),
            )?);
            Ok(())
        });
        let owner = Arc::new(Self {
            resources: Mutex::new(Some(resources)),
            children: AtomicUsize::new(0),
            asynchronous,
        });
        if let Err(error) = initialized {
            owner
                .close()
                .map_err(|(operation, code)| Error::Unquiesced { operation, code })?;
            return Err(error);
        }
        Ok(owner)
    }

    fn handle(&self) -> Result<*mut c_void> {
        self.asynchronous.check()?;
        self.resources
            .lock()
            .map_err(|_| Error::Internal("RDMA CQ event lock poisoned"))?
            .as_ref()
            .map(|r| r.handles.channel)
            .ok_or(Error::Unavailable("RDMA completion channel is closed"))
    }

    pub(super) fn retain(&self) {
        if let Some(resources) = self
            .resources
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_mut()
        {
            resources.retain();
        }
    }

    pub(super) fn close(&self) -> std::result::Result<(), (&'static str, i32)> {
        let mut state = self.resources.lock().unwrap_or_else(|p| p.into_inner());
        let Some(resources) = state.as_mut() else {
            return Ok(());
        };
        if let Some(error) = resources.failure {
            return Err(error);
        }
        if self.children.load(Ordering::Acquire) != 0 {
            return Err(("RDMA completion channel still owns CQs", libc::EBUSY));
        }
        let closed = (|| {
            if let Err(Error::Unquiesced { operation, code }) = self.asynchronous.check() {
                return Err((operation, code));
            }
            if let Some(registration) = &mut resources.registration {
                registration.stop().map_err(|error| match error {
                    Error::Unquiesced { operation, code } => (operation, code),
                    _ => ("RDMA completion monitor detach", libc::EIO),
                })?;
                resources.registration = None;
            }
            if let Err(Error::Unquiesced { operation, code }) = self.asynchronous.check() {
                return Err((operation, code));
            }
            if !resources.handles.channel.is_null() {
                let code = unsafe { (resources.verbs.close)(&mut resources.handles) };
                if code != 0 {
                    return Err(("ibv_destroy_comp_channel", code));
                }
            }
            Ok(())
        })();
        if let Err(error) = closed {
            resources.failure = Some(error);
            resources.retain();
            return Err(error);
        }
        drop(state.take());
        Ok(())
    }
}

impl Drop for CompletionEvents {
    fn drop(&mut self) {
        if let Some(resources) = self
            .resources
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let mut resources = resources;
            resources.retain();
            // A failed explicit close never loses the native channel or context owner.
            std::mem::forget(resources);
        }
    }
}

pub(super) struct RdmaCq {
    pub(super) handles: sys::CqHandles,
    pub(super) _device: Arc<RdmaContext>,
    pub(super) usage: Arc<Mutex<RdmaStats>>,
    events: Option<Arc<CompletionEvents>>,
}

impl RdmaCq {
    pub(super) fn new(
        device: Arc<RdmaContext>,
        usage: Arc<Mutex<RdmaStats>>,
        capacity: usize,
        events: Option<Arc<CompletionEvents>>,
        verbs: &sys::Verbs,
    ) -> Result<Box<Self>> {
        let channel = events
            .as_ref()
            .map(|e| e.handle())
            .transpose()?
            .unwrap_or(std::ptr::null_mut());
        usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .active_cqs += 1;
        if let Some(events) = &events {
            events.children.fetch_add(1, Ordering::AcqRel);
        }
        let mut cq = Box::new(Self {
            handles: sys::CqHandles::default(),
            _device: device,
            usage,
            events,
        });
        if let Err(error) = cq
            ._device
            .create_cq(&mut cq.handles, capacity as u32, channel)
        {
            if let Err((operation, code)) = cq.close(verbs) {
                cq.retain();
                return Err(Error::Unquiesced { operation, code });
            }
            return Err(error);
        }
        cq.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .opened_cqs += 1;
        Ok(cq)
    }

    pub(super) fn retain(self: Box<Self>) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retained_cqs += 1;
        if let Some(events) = &self.events {
            events.retain();
        }
        std::mem::forget(self);
    }

    pub(super) fn arm(&self, verbs: &sys::Verbs) -> Result<()> {
        if self.events.is_some() {
            self._device.asynchronous.check()?;
            let native = verbs
                .completion
                .ok_or(Error::Internal("missing RDMA CQ event verbs"))?;
            check("ibv_req_notify_cq", unsafe {
                (native.arm)(self.handles.cq)
            })?;
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .cq_notification_arms += 1;
        }
        Ok(())
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

// The sole progress thread arms, polls and closes this CQ.
unsafe impl Send for RdmaCq {}

impl Drop for RdmaCq {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active_cqs -= 1;
        if let Some(events) = &self.events {
            events.children.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/cq.rs"]
pub(super) mod tests;
