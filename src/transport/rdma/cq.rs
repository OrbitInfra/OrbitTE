use std::ffi::c_void;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

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
    stopping: Arc<AtomicBool>,
    wake: UnixStream,
    worker: Option<JoinHandle<()>>,
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

struct EventSource {
    channel: NonNull<c_void>,
    fd: i32,
    verbs: sys::CqEventVerbs,
}

// The channel owner joins this sole consumer before destroying the native handle.
unsafe impl Send for EventSource {}

struct EventExit {
    asynchronous: Arc<events::State>,
    usage: Arc<Mutex<RdmaStats>>,
    stopping: Arc<AtomicBool>,
}

impl Drop for EventExit {
    fn drop(&mut self) {
        if !self.stopping.load(Ordering::Acquire) {
            self.asynchronous.lost_completion(libc::EIO);
        }
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completion_event_workers -= 1;
    }
}

impl EventSource {
    fn run(&self, wake: UnixStream, exit: &EventExit) {
        let mut fds = [
            libc::pollfd {
                fd: self.fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            let ready = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, -1) };
            if ready < 0 {
                let code = std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO);
                if code == libc::EINTR {
                    continue;
                }
                exit.asynchronous.lost_completion(code);
                return;
            }
            if exit.stopping.load(Ordering::Acquire) {
                return;
            }
            if fds
                .iter()
                .any(|fd| fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0)
            {
                exit.asynchronous.lost_completion(libc::EIO);
                return;
            }
            if fds[0].revents & libc::POLLIN == 0 {
                continue;
            }
            loop {
                if exit.stopping.load(Ordering::Acquire) {
                    return;
                }
                let mut cq = std::ptr::null_mut();
                let code = unsafe { (self.verbs.get)(self.channel.as_ptr(), &mut cq) };
                if code == libc::EINTR {
                    continue;
                }
                if code == libc::EAGAIN {
                    break;
                }
                if code != 0 || cq.is_null() {
                    exit.asynchronous
                        .lost_completion(if code == 0 { libc::EPROTO } else { code });
                    return;
                }
                // ACK the actual returned CQ before acquiring any progress/usage lock.
                // Native destruction may be waiting for this exact event receipt.
                unsafe { (self.verbs.ack)(cq) };
                exit.usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .completion_events += 1;
                exit.asynchronous.notify();
                if exit.stopping.load(Ordering::Acquire) {
                    return;
                }
            }
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
        let (wake, receiver) = UnixStream::pair()?;
        wake.set_nonblocking(true)?;
        let asynchronous = Arc::clone(&device.asynchronous);
        let mut resources = Box::new(ChannelResources {
            handles: sys::ChannelHandles::default(),
            _device: device,
            usage,
            verbs: native,
            stopping: Arc::new(AtomicBool::new(false)),
            wake,
            worker: None,
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
            let source = EventSource {
                channel: NonNull::new(resources.handles.channel)
                    .ok_or(Error::Internal("missing RDMA completion channel"))?,
                fd: resources.handles.fd,
                verbs: native,
            };
            if source.fd < 0 {
                return Err(Error::Internal("missing RDMA completion fd"));
            }
            let state = Arc::clone(&asynchronous);
            let usage = Arc::clone(&resources.usage);
            let stopping = Arc::clone(&resources.stopping);
            usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .completion_event_workers += 1;
            match thread::Builder::new()
                .name("orbitte-rdma-cq-events".into())
                .spawn(move || {
                    let exit = EventExit {
                        asynchronous: state,
                        usage,
                        stopping,
                    };
                    source.run(receiver, &exit);
                }) {
                Ok(worker) => {
                    resources.worker = Some(worker);
                    Ok(())
                }
                Err(error) => {
                    resources
                        .usage
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .completion_event_workers -= 1;
                    Err(Error::Io(error))
                }
            }
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
            resources.stopping.store(true, Ordering::Release);
            if resources.worker.is_some() {
                loop {
                    match resources.wake.write(&[1]) {
                        Ok(_) => break,
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(error) => {
                            return Err((
                                "RDMA completion consumer wake",
                                error.raw_os_error().unwrap_or(libc::EIO),
                            ));
                        }
                    }
                }
            }
            if let Some(worker) = resources.worker.take() {
                if worker.join().is_err() {
                    return Err(("RDMA completion consumer join", libc::EIO));
                }
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
