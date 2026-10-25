//! One transport monitor; registered event sources borrow native handles until detached.

use std::ffi::c_void;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use super::{RdmaStats, events, sys};
use crate::engine::remaining;
use crate::{Error, Result};

pub(super) enum Source {
    Async {
        context: NonNull<c_void>,
        fd: i32,
        verbs: sys::AsyncVerbs,
    },
    Completion {
        channel: NonNull<c_void>,
        fd: i32,
        verbs: sys::CqEventVerbs,
    },
}

// Registration::stop serializes with get/ACK before the native owner can close.
unsafe impl Send for Source {}

impl Source {
    fn fd(&self) -> i32 {
        match self {
            Self::Async { fd, .. } | Self::Completion { fd, .. } => *fd,
        }
    }

    fn charge(&self, counters: &Counters, add: bool) {
        let count = match self {
            Self::Async { .. } => &counters.asynchronous,
            Self::Completion { .. } => &counters.completion,
        };
        if add {
            count.fetch_add(1, Ordering::AcqRel);
        } else {
            count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn lost(&self, state: &events::State, code: i32) {
        match self {
            Self::Async { .. } => state.lost(code),
            Self::Completion { .. } => state.lost_completion(code),
        }
    }

    fn consume(
        &self,
        state: &events::State,
        counters: &events::Counters,
    ) -> std::result::Result<(), i32> {
        #[cfg(test)]
        tests::observe(self);
        // A busy source cannot starve other devices, shutdown or new registrations.
        for _ in 0..64 {
            let code = match self {
                Self::Async { context, verbs, .. } => {
                    let mut event = sys::AsyncEvent::default();
                    let code = unsafe { (verbs.get)(context.as_ptr(), &mut event) };
                    if code == 0 {
                        state.event(event.kind);
                        unsafe { (verbs.ack)(context.as_ptr(), &mut event) };
                        counters
                            .acknowledged_async_events
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    code
                }
                Self::Completion { channel, verbs, .. } => {
                    let mut cq = std::ptr::null_mut();
                    let code = unsafe { (verbs.get)(channel.as_ptr(), &mut cq) };
                    if code == 0 {
                        if cq.is_null() {
                            return Err(libc::EPROTO);
                        }
                        unsafe { (verbs.ack)(cq) };
                        counters.completion_events.fetch_add(1, Ordering::Relaxed);
                    }
                    code
                }
            };
            match code {
                0 => state.notify(),
                libc::EAGAIN => return Ok(()),
                libc::EINTR => continue,
                code => return Err(code),
            }
        }
        Ok(())
    }
}

struct Watch {
    source: Mutex<Option<Source>>,
    state: Arc<events::State>,
    counters: Arc<events::Counters>,
    monitor_counters: Arc<Counters>,
}

impl Watch {
    fn remove(&self, source: &mut Option<Source>, error: Option<i32>) {
        if let Some(source) = source.take() {
            if let Some(code) = error {
                source.lost(&self.state, code);
            }
            source.charge(&self.monitor_counters, false);
        }
    }
}

#[derive(Default)]
pub(super) struct Monitor {
    running: Mutex<Option<Running>>,
    counters: Arc<Counters>,
}

#[derive(Default)]
struct Counters {
    workers: AtomicUsize,
    opened: AtomicU64,
    asynchronous: AtomicUsize,
    completion: AtomicUsize,
}

struct Running {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}

struct Shared {
    watches: Mutex<Vec<Weak<Watch>>>,
    wake: Mutex<UnixStream>,
    stopping: AtomicBool,
    exited: Mutex<bool>,
    changed: Condvar,
    monitor_counters: Arc<Counters>,
}

impl Shared {
    fn wake(&self) -> Result<()> {
        let mut writer = self.wake.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            match writer.write(&[1]) {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => {
                    return Err(Error::Unquiesced {
                        operation: "RDMA monitor wake",
                        code: error.raw_os_error().unwrap_or(libc::EIO),
                    });
                }
            }
        }
    }

    fn snapshot(&self) -> Vec<Arc<Watch>> {
        let mut watches = self.watches.lock().unwrap_or_else(|p| p.into_inner());
        let active: Vec<_> = watches.iter().filter_map(Weak::upgrade).collect();
        watches.retain(|watch| watch.strong_count() != 0);
        active
    }

    fn run(&self, mut wake: UnixStream) {
        while !self.stopping.load(Ordering::Acquire) {
            let watches = self.snapshot();
            let mut fds = Vec::with_capacity(watches.len() + 1);
            fds.push(libc::pollfd {
                fd: wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            });
            for watch in &watches {
                let source = watch.source.lock().unwrap_or_else(|p| p.into_inner());
                fds.push(libc::pollfd {
                    fd: source.as_ref().map_or(-1, Source::fd),
                    events: libc::POLLIN,
                    revents: 0,
                });
            }
            let ready = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, -1) };
            if ready < 0 {
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return;
            }
            if fds[0].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                return;
            }
            if fds[0].revents & libc::POLLIN != 0 {
                let mut bytes = [0; 256];
                loop {
                    match wake.read(&mut bytes) {
                        Ok(0) => return,
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(_) => return,
                    }
                }
            }
            for (watch, fd) in watches.iter().zip(&fds[1..]) {
                let mut source = watch.source.lock().unwrap_or_else(|p| p.into_inner());
                let Some(current) = source.as_ref() else {
                    continue;
                };
                // A retired registration is never replaced in place, even if its
                // fd number is reused while this poll snapshot is outstanding.
                let error = if fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                    Some(libc::EIO)
                } else if fd.revents & libc::POLLIN != 0 {
                    current.consume(&watch.state, &watch.counters).err()
                } else {
                    None
                };
                if let Some(code) = error {
                    watch.remove(&mut source, Some(code));
                }
            }
        }
    }
}

struct Exit(Arc<Shared>);
impl Drop for Exit {
    fn drop(&mut self) {
        self.0.stopping.store(true, Ordering::Release);
        for watch in self.0.snapshot() {
            let mut source = watch.source.lock().unwrap_or_else(|p| p.into_inner());
            // Any live source at monitor exit has lost its sole ACK consumer.
            watch.remove(&mut source, Some(libc::EIO));
        }
        self.0
            .monitor_counters
            .workers
            .fetch_sub(1, Ordering::AcqRel);
        *self.0.exited.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.0.changed.notify_all();
    }
}

impl Monitor {
    pub(super) fn stats(&self, stats: &mut RdmaStats) {
        stats.monitor_workers = self.counters.workers.load(Ordering::Acquire);
        stats.opened_monitor_workers = self.counters.opened.load(Ordering::Relaxed);
        stats.async_event_sources = self.counters.asynchronous.load(Ordering::Acquire);
        stats.completion_event_sources = self.counters.completion.load(Ordering::Acquire);
    }

    pub(super) fn register(
        self: &Arc<Self>,
        source: Source,
        state: Arc<events::State>,
        counters: Arc<events::Counters>,
    ) -> Result<Registration> {
        if source.fd() < 0 {
            return Err(Error::Internal("missing RDMA monitor fd"));
        }
        let mut running = self
            .running
            .lock()
            .map_err(|_| Error::Internal("RDMA monitor lock poisoned"))?;
        if running.is_none() {
            let (wake, receiver) = UnixStream::pair()?;
            wake.set_nonblocking(true)?;
            receiver.set_nonblocking(true)?;
            let shared = Arc::new(Shared {
                watches: Mutex::default(),
                wake: Mutex::new(wake),
                stopping: AtomicBool::new(false),
                exited: Mutex::new(false),
                changed: Condvar::new(),
                monitor_counters: Arc::clone(&self.counters),
            });
            self.counters.workers.fetch_add(1, Ordering::AcqRel);
            let exit = Exit(Arc::clone(&shared));
            let worker = thread::Builder::new()
                .name("orbitte-rdma-monitor".into())
                .spawn(move || {
                    exit.0.run(receiver);
                    drop(exit);
                })?;
            self.counters.opened.fetch_add(1, Ordering::Relaxed);
            *running = Some(Running {
                shared,
                worker: Some(worker),
            });
        }
        let live = running
            .as_ref()
            .ok_or(Error::Internal("RDMA monitor missing"))?;
        if live.shared.stopping.load(Ordering::Acquire)
            || *live.shared.exited.lock().unwrap_or_else(|p| p.into_inner())
        {
            return Err(Error::Unquiesced {
                operation: "RDMA monitor stopped",
                code: libc::EIO,
            });
        }
        source.charge(&self.counters, true);
        let watch = Arc::new(Watch {
            source: Mutex::new(Some(source)),
            state,
            counters,
            monitor_counters: Arc::clone(&self.counters),
        });
        {
            let mut watches = live
                .shared
                .watches
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if live.shared.stopping.load(Ordering::Acquire) {
                watch.remove(
                    &mut watch.source.lock().unwrap_or_else(|p| p.into_inner()),
                    Some(libc::EIO),
                );
                return Err(Error::Unquiesced {
                    operation: "RDMA monitor stopped",
                    code: libc::EIO,
                });
            }
            watches.push(Arc::downgrade(&watch));
        }
        let registration = Registration {
            monitor: Arc::clone(self),
            watch,
        };
        if let Err(error) = live.shared.wake() {
            let mut source = registration
                .watch
                .source
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            registration.watch.remove(&mut source, Some(libc::EIO));
            return Err(error);
        }
        Ok(registration)
    }

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        let mut running = self
            .running
            .lock()
            .map_err(|_| Error::Internal("RDMA monitor lock poisoned"))?;
        let Some(live) = running.as_mut() else {
            return Ok(());
        };
        for watch in live.shared.snapshot() {
            if watch
                .source
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_some()
            {
                return Err(Error::Unquiesced {
                    operation: "RDMA monitor still owns event sources",
                    code: libc::EBUSY,
                });
            }
        }
        live.shared.stopping.store(true, Ordering::Release);
        live.shared.wake()?;
        let mut exited = live.shared.exited.lock().unwrap_or_else(|p| p.into_inner());
        while !*exited {
            exited = live
                .shared
                .changed
                .wait_timeout(exited, remaining(deadline)?)
                .map_err(|_| Error::Internal("RDMA monitor exit lock poisoned"))?
                .0;
        }
        if let Some(worker) = live.worker.take() {
            worker.join().map_err(|_| Error::Unquiesced {
                operation: "RDMA monitor join",
                code: libc::EIO,
            })?;
        }
        Ok(())
    }
}

pub(super) struct Registration {
    monitor: Arc<Monitor>,
    watch: Arc<Watch>,
}

impl Registration {
    pub(super) fn stop(&mut self) -> Result<()> {
        let mut source = self.watch.source.lock().unwrap_or_else(|p| p.into_inner());
        if source.is_none() {
            return Err(Error::Unquiesced {
                operation: "RDMA monitor source lost",
                code: libc::EIO,
            });
        }
        self.watch.remove(&mut source, None);
        drop(source);
        if let Some(live) = self
            .monitor
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            live.shared.wake()?;
        }
        Ok(())
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut source = self.watch.source.lock().unwrap_or_else(|p| p.into_inner());
        self.watch.remove(&mut source, Some(libc::EIO));
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        let _ = self.close(Instant::now() + std::time::Duration::from_secs(30));
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/monitor.rs"]
mod tests;
