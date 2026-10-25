use std::ffi::c_void;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};

use ibverbs_sys::{IBV_EVENT_COMM_EST, IBV_EVENT_SQ_DRAINED};

use super::{RdmaStats, progress, sys};
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct Counters {
    pub(super) acknowledged_async_events: AtomicU64,
    pub(super) completion_events: AtomicU64,
}

#[derive(Default)]
pub(super) struct State {
    // Positive: event type + 1; negative: the consumer can no longer ACK.
    fault: AtomicI32,
    stopping: AtomicBool,
    route_fault: AtomicBool,
    completion_fault: AtomicI32,
    progress: Mutex<Weak<progress::ProgressState>>,
}

impl State {
    pub(super) fn check(&self) -> Result<()> {
        let asynchronous = self.fault.load(Ordering::Acquire);
        let completion = self.completion_fault.load(Ordering::Acquire);
        if asynchronous >= 0 && completion != 0 {
            return Err(Error::Unquiesced {
                operation: "RDMA completion event consumer",
                code: completion,
            });
        }
        match asynchronous {
            0 if self.route_fault.load(Ordering::Acquire) => Err(Error::Backend {
                operation: "RDMA route changed",
                code: libc::ESTALE,
            }),
            0 => Ok(()),
            code if code > 0 => Err(Error::Backend {
                operation: "RDMA asynchronous event",
                code: code - 1,
            }),
            code => Err(Error::Unquiesced {
                operation: "RDMA asynchronous event consumer",
                code: -code,
            }),
        }
    }

    pub(super) fn attach(&self, progress: Weak<progress::ProgressState>) {
        *self.progress.lock().unwrap_or_else(|p| p.into_inner()) = progress;
        self.notify();
    }

    pub(super) fn fence_route(&self) {
        self.route_fault.store(true, Ordering::Release);
    }

    pub(super) fn notify(&self) {
        let progress = self
            .progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .upgrade();
        if let Some(progress) = progress {
            progress.wake();
        }
    }

    pub(super) fn lost_completion(&self, code: i32) {
        let _ = self.completion_fault.compare_exchange(
            0,
            code.max(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.notify();
    }

    fn lost(&self, code: i32) {
        self.fault.store(-code.max(1), Ordering::Release);
        self.notify();
    }

    fn event(&self, kind: u32) {
        // Only transport-independent informational events keep this fixed route
        // healthy. Unknown, port, topology and device changes fence the lane.
        if !matches!(kind, IBV_EVENT_COMM_EST | IBV_EVENT_SQ_DRAINED) {
            let code = kind.min(i32::MAX as u32 - 1) as i32 + 1;
            let _ = self
                .fault
                .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire);
        }
    }
}

struct Source {
    context: NonNull<c_void>,
    fd: i32,
    verbs: sys::AsyncVerbs,
}
// RdmaContext retains context/fd until this consumer has stopped and joined.
unsafe impl Send for Source {}

impl Source {
    fn run(self, wake: UnixStream, state: &State, counters: &Counters) {
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
            // SAFETY: both descriptors remain owned throughout this loop.
            let ready = unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) };
            if ready < 0 {
                let code = std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO);
                if code == libc::EINTR {
                    continue;
                }
                state.lost(code);
                return;
            }
            if state.stopping.load(Ordering::Acquire) {
                return;
            }
            if fds
                .iter()
                .any(|fd| fd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0)
            {
                state.lost(libc::EIO);
                return;
            }
            if fds[0].revents & libc::POLLIN == 0 {
                continue;
            }
            let mut event = sys::AsyncEvent::default();
            // SAFETY: the sole reader owns a nonblocking context fd. The event
            // remains on this stack until exactly one acknowledgment.
            let code = unsafe { (self.verbs.get)(self.context.as_ptr(), &mut event) };
            if matches!(code, libc::EAGAIN | libc::EINTR) {
                continue;
            }
            if code != 0 {
                state.lost(code);
                return;
            }
            let kind = event.kind;
            // Fence without Record/RdmaContext/control locks before ACK. Native
            // destroy may be holding those locks while awaiting this receipt.
            state.event(kind);
            unsafe { (self.verbs.ack)(self.context.as_ptr(), &mut event) };
            counters
                .acknowledged_async_events
                .fetch_add(1, Ordering::Relaxed);
            state.notify();
        }
    }
}

pub(super) struct Events {
    state: Arc<State>,
    wake: UnixStream,
    worker: Option<JoinHandle<()>>,
}

struct Exit {
    state: Arc<State>,
    usage: Arc<Mutex<RdmaStats>>,
}
impl Drop for Exit {
    fn drop(&mut self) {
        if !self.state.stopping.load(Ordering::Acquire)
            && self.state.fault.load(Ordering::Acquire) >= 0
        {
            self.state.lost(libc::EIO);
        }
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .async_event_workers -= 1;
    }
}

impl Events {
    pub(super) fn start(
        handles: &sys::DeviceHandles,
        verbs: sys::AsyncVerbs,
        state: Arc<State>,
        usage: Arc<Mutex<RdmaStats>>,
        counters: Arc<Counters>,
    ) -> Result<Self> {
        let context =
            NonNull::new(handles.context).ok_or(Error::Internal("missing RDMA context"))?;
        if handles.async_fd < 0 {
            return Err(Error::Internal("missing RDMA async fd"));
        }
        let (wake, receiver) = UnixStream::pair()?;
        wake.set_nonblocking(true)?;
        let source = Source {
            context,
            fd: handles.async_fd,
            verbs,
        };
        usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .async_event_workers += 1;
        let exit = Exit {
            state: Arc::clone(&state),
            usage: Arc::clone(&usage),
        };
        let worker = thread::Builder::new()
            .name("orbitte-rdma-events".into())
            .spawn(move || {
                let _exit = exit;
                source.run(receiver, &_exit.state, &counters);
            });
        // A failed spawn drops the closure/Exit and repays the worker charge.
        Ok(Self {
            state,
            wake,
            worker: Some(worker?),
        })
    }

    // All affiliated QPs/CQs have already been destroyed. No destroy may wait
    // for a future ACK after the event consumer has joined.
    pub(super) fn stop(&mut self) -> Result<()> {
        self.state.stopping.store(true, Ordering::Release);
        if let Err(error) = self.wake.write(&[1]) {
            if error.kind() != std::io::ErrorKind::WouldBlock {
                return Err(Error::Unquiesced {
                    operation: "RDMA async consumer wake",
                    code: error.raw_os_error().unwrap_or(libc::EIO),
                });
            }
        }
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| Error::Unquiesced {
                operation: "RDMA async consumer join",
                code: libc::EIO,
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/events.rs"]
mod tests;
