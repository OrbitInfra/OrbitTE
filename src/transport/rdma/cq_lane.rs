//! CQ pair lifetime, physical-QP records and the state shared with its data worker.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use super::COMPLETION_MESSAGE_SIZE;
use super::cq::{CompletionEvents, RdmaCq};
use super::endpoint::Peer;
use super::protocol::BoundDone;
use super::qp_store::QpStore;
use super::receive::{MessageBuffer, ReceiveWork};
use super::{
    Buffer, ConnectionId, Descriptor, Phase, RdmaConfig, RdmaContext, RdmaGrant, RdmaProgressMode,
    RdmaStats, Resources, events, slice, sys,
};
use crate::engine::remaining;
use crate::{Error, Result, TransferOp};

#[derive(Clone, Copy)]
enum Fault {
    Backend(&'static str, i32),
    Unquiesced(&'static str, i32),
    Protocol(&'static str),
    Internal(&'static str),
    Unavailable(&'static str),
    Timeout,
    Exhausted,
}

impl Fault {
    fn from_error(error: Error) -> Self {
        match error {
            Error::Backend { operation, code } => Self::Backend(operation, code),
            Error::Unquiesced { operation, code } => Self::Unquiesced(operation, code),
            Error::Protocol(message) => Self::Protocol(message),
            Error::Internal(message) => Self::Internal(message),
            Error::Unavailable(message) => Self::Unavailable(message),
            Error::Timeout => Self::Timeout,
            Error::ResourceExhausted => Self::Exhausted,
            _ => Self::Internal("unexpected RDMA progress failure"),
        }
    }
    fn error(self) -> Error {
        match self {
            Self::Backend(operation, code) => Error::Backend { operation, code },
            Self::Unquiesced(operation, code) => Error::Unquiesced { operation, code },
            Self::Protocol(message) => Error::Protocol(message),
            Self::Internal(message) => Error::Internal(message),
            Self::Unavailable(message) => Error::Unavailable(message),
            Self::Timeout => Error::Timeout,
            Self::Exhausted => Error::ResourceExhausted,
        }
    }
}

const WORK: u8 = 1;
const CLOSE: u8 = 2;
const RECEIVE: u8 = 4;
const FINISH: u8 = 8;

pub(super) struct Record {
    actions: AtomicU8,
    pub(super) tag: u64,
    pub(super) terminal: AtomicBool,
    pub(super) send_namespace: AtomicU64,
    pub(super) receive_namespace: AtomicU64,
    state: Mutex<Request>,
    changed: Condvar,
}

pub(super) struct Request {
    pub(super) resources: Option<Box<Resources>>,
    pub(super) phase: Phase,
    pub(super) qpn: u32,
    pub(super) claim: u64,
    finish: Option<GrantDrain>,
    work: Option<Work>,
    receive: Option<Arc<ReceiveWork>>,
    message_sent: bool,
    payload_operation: Option<TransferOp>,
    mixed_operations: bool,
    close_requested: bool,
    failure: Option<Fault>,
}

pub(super) struct GrantDrain {
    connection: ConnectionId,
    generation: u64,
    permission: u32,
}

impl GrantDrain {
    pub(super) fn matches(
        &self,
        resources: &super::ConnectionResources,
        grant: &super::GrantResources,
    ) -> bool {
        resources.incarnation == Some(self.connection)
            && grant.connection == Some(self.connection)
            && self.generation != 0
            && grant.generation == self.generation
            && grant.permission == self.permission
            && !grant.window_bound
    }
}

struct Work {
    queue: slice::SendWork,
    operation: slice::Operation,
    remote: Descriptor,
    deadline: Instant,
}

impl Request {
    pub(super) fn check_claim(&self, claim: u64) -> Result<()> {
        if self.claim != claim {
            return Err(Error::Unavailable("stale RDMA grant handle"));
        }
        self.check()
    }
    pub(super) fn check(&self) -> Result<()> {
        match self.failure {
            Some(fault) => Err(fault.error()),
            None => self.resources.as_ref().map_or(Ok(()), |resources| {
                resources.connection.device.asynchronous.check()
            }),
        }
    }
}

impl Record {
    pub(super) fn require_live(&self) -> Result<()> {
        if self.terminal.load(Ordering::Acquire) {
            return Err(Error::Unavailable("RDMA endpoint is terminal"));
        }
        Ok(())
    }

    pub(super) fn lock(&self) -> Result<MutexGuard<'_, Request>> {
        self.state.lock().map_err(|_| Error::Unquiesced {
            operation: "RDMA request lock poisoned",
            code: libc::EIO,
        })
    }
}

pub(super) struct CqLane {
    pub(super) worker_id: OnceLock<usize>,
    pub(super) workers: Arc<super::workers::Workers>,
    pub(super) shared: Arc<ProgressState>,
}

pub(super) struct ProgressState {
    asynchronous: Arc<events::State>,
    next_work_namespace: AtomicU64,
    quota: Arc<super::quota::CqQuota>,
    quota_waiting: AtomicBool,
    completion_events: Option<Arc<CompletionEvents>>,
    control: Mutex<Control>,
    changed: Condvar,
    progress_thread: OnceLock<thread::Thread>,
    pub(super) usage: Arc<Mutex<RdmaStats>>,
    verbs: &'static sys::Verbs,
    pub(super) config: RdmaConfig,
}

struct Control {
    cq: Option<Box<RdmaCq>>,
    receive_cq: Option<Box<RdmaCq>>,
    qps: QpStore,
    next_tag: u64,
    dirty: bool,
    stopping: bool,
    accepting: bool,
    finished: bool,
    detached: bool,
    fault: Option<Fault>,
    retained: Option<(&'static str, i32)>,
}

impl CqLane {
    pub(super) fn new(
        config: &RdmaConfig,
        workers: Arc<super::workers::Workers>,
        device: Arc<RdmaContext>,
        usage: Arc<Mutex<RdmaStats>>,
        quota: Arc<super::quota::CqQuota>,
        verbs: &'static sys::Verbs,
    ) -> Result<Arc<Self>> {
        let qps = QpStore::new(config.max_endpoints)?;
        let completion_events = if config.progress_mode == RdmaProgressMode::EventDriven {
            Some(CompletionEvents::new(
                Arc::clone(&device),
                Arc::clone(&usage),
                verbs,
            )?)
        } else {
            None
        };
        let mut cq = match RdmaCq::new(
            Arc::clone(&device),
            Arc::clone(&usage),
            config.max_cqes,
            completion_events.clone(),
            verbs,
        ) {
            Ok(cq) => cq,
            Err(error) => {
                if let Some(events) = &completion_events {
                    events
                        .close()
                        .map_err(|(operation, code)| Error::Unquiesced { operation, code })?;
                }
                return Err(error);
            }
        };
        let receive_cq = match RdmaCq::new(
            Arc::clone(&device),
            Arc::clone(&usage),
            config.max_cqes,
            completion_events.clone(),
            verbs,
        ) {
            Ok(receive_cq) => receive_cq,
            Err(error) => {
                if let Err((operation, code)) = cq.close(verbs) {
                    cq.retain();
                    return Err(Error::Unquiesced { operation, code });
                }
                drop(cq);
                if let Some(events) = &completion_events {
                    events
                        .close()
                        .map_err(|(operation, code)| Error::Unquiesced { operation, code })?;
                }
                return Err(error);
            }
        };
        let shared = Arc::new(ProgressState {
            asynchronous: Arc::clone(&device.asynchronous),
            next_work_namespace: AtomicU64::new(1),
            quota,
            quota_waiting: AtomicBool::new(false),
            completion_events,
            control: Mutex::new(Control {
                cq: Some(cq),
                receive_cq: Some(receive_cq),
                qps,
                next_tag: 0,
                dirty: false,
                stopping: false,
                accepting: true,
                finished: false,
                detached: false,
                fault: None,
                retained: None,
            }),
            changed: Condvar::new(),
            progress_thread: OnceLock::new(),
            usage,
            verbs,
            config: config.clone(),
        });
        shared.quota.attach(&shared);
        device.asynchronous.attach(Arc::downgrade(&shared));
        Ok(Arc::new(Self {
            worker_id: OnceLock::new(),
            workers,
            shared,
        }))
    }

    pub(super) fn handle(&self, receive: bool) -> Result<*mut std::ffi::c_void> {
        let control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        self.shared.asynchronous.check()?;
        if let Some(fault) = control.fault {
            return Err(fault.error());
        }
        (if receive {
            &control.receive_cq
        } else {
            &control.cq
        })
        .as_ref()
        .map(|cq| cq.handles.cq)
        .ok_or(Error::Unavailable("RDMA CQ is closed"))
    }

    pub(super) fn register(&self, resources: Box<Resources>) -> Result<Arc<Record>> {
        let mut control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        self.shared.asynchronous.check()?;
        if let Some(fault) = control.fault {
            return Err(fault.error());
        }
        if control.stopping || control.finished {
            return Err(Error::Unavailable("RDMA progress is closed"));
        }
        if control.qps.len() >= self.shared.config.max_endpoints {
            return Err(Error::ResourceExhausted);
        }
        let tag = control.next_tag;
        let next = tag
            .checked_add(1)
            .filter(|next| next.checked_mul(super::WR_ID_STRIDE).is_some())
            .ok_or(Error::ResourceExhausted)?;
        control.next_tag = next;
        let record = Arc::new(Record {
            actions: AtomicU8::new(0),
            tag,
            terminal: AtomicBool::new(false),
            send_namespace: AtomicU64::new(0),
            receive_namespace: AtomicU64::new(0),
            state: Mutex::new(Request {
                resources: Some(resources),
                phase: Phase::Init,
                qpn: 0,
                claim: 1,
                finish: None,
                work: None,
                receive: None,
                message_sent: false,
                payload_operation: None,
                mixed_operations: false,
                close_requested: false,
                failure: None,
            }),
            changed: Condvar::new(),
        });
        control.qps.publish(Arc::clone(&record))?;
        control.dirty = true;
        self.shared.wake();
        Ok(record)
    }

    pub(super) fn fence(&self, deadline: Instant) -> Result<()> {
        self.shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?
            .accepting = false;
        while self.evict_idle(deadline)? {}
        Ok(())
    }

    pub(super) fn close_record(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<()> {
        let mut request = record.lock()?;
        if request.claim != claim {
            return Ok(());
        }
        if !matches!(request.phase, Phase::Closed | Phase::Retained) {
            request.close_requested = true;
            drop(request);
            {
                let mut control = self.shared.control.lock().map_err(|_| Error::Unquiesced {
                    operation: "RDMA endpoint store poisoned",
                    code: libc::EIO,
                })?;
                if control.qps.detach(record) {
                    #[cfg(test)]
                    tests::before_idle_debit(&self.shared);
                    self.shared
                        .usage
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .idle_endpoints -= 1;
                }
            }
            record.actions.fetch_or(CLOSE, Ordering::Release);
            self.shared.signal();
            request = record.lock()?;
            while !matches!(request.phase, Phase::Closed | Phase::Retained) {
                let duration = remaining(deadline).map_err(|_| Error::Unquiesced {
                    operation: "RDMA close deadline before quiescence",
                    code: libc::ETIMEDOUT,
                })?;
                request = record
                    .changed
                    .wait_timeout(request, duration)
                    .map_err(|_| Error::Unquiesced {
                        operation: "RDMA close wait poisoned",
                        code: libc::EIO,
                    })?
                    .0;
            }
        }
        if request.phase == Phase::Retained {
            drop(request);
            return match self.close(deadline) {
                Err(error @ Error::Unquiesced { .. }) => Err(error),
                Err(Error::Timeout) => Err(Error::Unquiesced {
                    operation: "RDMA retention receipt deadline",
                    code: libc::ETIMEDOUT,
                }),
                _ => Err(Error::Unquiesced {
                    operation: "RDMA retained progress lane",
                    code: libc::EIO,
                }),
            };
        }
        request.check()
    }

    pub(super) fn recoverable(&self) -> Result<bool> {
        let control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        Ok(control.fault.is_some()
            && control.finished
            && control.detached
            && control.retained.is_none()
            && control.cq.is_none()
            && control.receive_cq.is_none()
            && control.qps.len() == 0)
    }

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        let mut control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        if self.shared.progress_thread.get().is_none() {
            drop(control);
            if let Err((operation, code)) = self.shared.close_cq() {
                self.shared.retain(operation, code);
            }
            control = self
                .shared
                .control
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            control.finished = true;
            control.detached = true;
        }
        control.stopping = true;
        control.dirty = true;
        self.shared.wake();
        while !control.finished || !control.detached {
            control = self
                .shared
                .changed
                .wait_timeout(control, remaining(deadline)?)
                .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?
                .0;
        }
        let retained = control.retained;
        drop(control);
        match retained {
            Some((operation, code)) => Err(Error::Unquiesced { operation, code }),
            None => Ok(()),
        }
    }
}

impl Drop for CqLane {
    fn drop(&mut self) {
        // Last public/native request owner is gone. Native calls remain non-preemptible.
        let deadline = Instant::now() + Duration::from_secs(300);
        let _ = self.close(deadline);
    }
}

pub(super) enum Schedule {
    Again,
    Wait(Option<Instant>),
    Finished,
}

impl ProgressState {
    pub(super) fn attach(&self, worker: thread::Thread) -> Result<()> {
        self.progress_thread
            .set(worker)
            .map_err(|_| Error::Internal("RDMA progress already assigned"))?;
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .progress_lanes += 1;
        Ok(())
    }

    pub(super) fn stop(&self) {
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        control.stopping = true;
        control.dirty = true;
    }

    pub(super) fn detached(&self) {
        if !self
            .control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .finished
        {
            self.retain("RDMA progress worker stopped", libc::EIO);
        }
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .progress_lanes -= 1;
        control.detached = true;
        self.changed.notify_all();
    }
}

mod poll;
mod reuse;
mod submit;

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/cq_lane.rs"]
mod tests;
