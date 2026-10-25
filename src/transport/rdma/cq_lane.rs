use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use super::COMPLETION_MESSAGE_SIZE;
use super::cq::{CompletionEvents, RdmaCq};
use super::endpoint::Peer;
use super::qp_store::QpStore;
use super::rdma_transport::BoundDone;
use super::receive::{MessageBuffer, ReceiveQueue};
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
    receive: Option<ReceiveQueue>,
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
    queue: slice::SendQueue,
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
    endpoints: QpStore,
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
        let endpoints = QpStore::new(config.max_endpoints)?;
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
                endpoints,
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
        if control.endpoints.len() >= self.shared.config.max_endpoints {
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
        control.endpoints.publish(Arc::clone(&record))?;
        control.dirty = true;
        self.shared.wake();
        Ok(record)
    }

    pub(super) fn submit(
        &self,
        record: &Arc<Record>,
        claim: u64,
        remote: Descriptor,
        operation: TransferOp,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected || request.message_sent {
                return Err(Error::Internal("RDMA QP is not ready"));
            }
            let resources = request
                .resources
                .as_ref()
                .ok_or(Error::Internal("missing RDMA endpoint"))?;
            if remote
                .address
                .checked_add(resources.grant()?.buffer.len() as u64)
                .is_none()
            {
                return Err(Error::InvalidRange);
            }
            let cap = if operation == TransferOp::Write {
                32
            } else {
                resources.grant()?.max_read_slices
            };
            let queue = slice::SendQueue::new(
                resources.grant()?.buffer.len(),
                self.shared.config.block_size,
                cap,
                Arc::clone(&self.shared.usage),
                self.shared.next_work_namespace()? * super::WR_ID_STRIDE + 1,
            )?;
            record
                .send_namespace
                .store(queue.namespace(), Ordering::Release);
            request.mixed_operations |= request
                .payload_operation
                .is_some_and(|previous| previous != operation);
            request.payload_operation = Some(operation);
            request.work = Some(Work {
                queue,
                operation: operation.into(),
                remote,
                deadline,
            });
            request.phase = Phase::Queued;
        }
        record.actions.fetch_or(WORK, Ordering::Release);
        self.shared.signal();
        Ok(())
    }

    pub(super) fn window(
        &self,
        record: &Arc<Record>,
        claim: u64,
        operation: slice::Operation,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected {
                return Err(Error::Internal("MW QP is not ready"));
            }
            let resources = request
                .resources
                .as_mut()
                .ok_or(Error::Internal("missing MW owner"))?;
            if resources.grant_mut()?.handles.mw.is_null() {
                return Err(Error::Unavailable("source MW is unavailable"));
            }
            let slot = match operation {
                slice::Operation::Bind if !resources.grant_mut()?.window_used => {
                    resources.grant_mut()?.window_used = true;
                    65
                }
                #[cfg(test)]
                slice::Operation::Invalidate if resources.grant_mut()?.window_bound => 66,
                _ => return Err(Error::Internal("MW authorization state")),
            };
            let namespace = self.shared.next_work_namespace()?;
            record.send_namespace.store(namespace, Ordering::Release);
            let remote = Descriptor {
                address: resources.grant_mut()?.buffer.address().as_ptr() as u64,
                rkey: resources.grant_mut()?.handles.window_key,
                ..Default::default()
            };
            request.work = Some(Work {
                queue: slice::SendQueue::control(
                    Arc::clone(&self.shared.usage),
                    namespace * super::WR_ID_STRIDE + slot,
                )?,
                operation,
                remote,
                deadline,
            });
            request.phase = Phase::Queued;
        }
        self.wait(record, claim, deadline)
    }

    fn prepare_message(&self, request: &mut Request) -> Result<()> {
        let resources = request
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA grant"))?;
        let grant = resources.grant_mut()?;
        if grant.message.is_some() {
            return Err(Error::Internal("RDMA message already prepared"));
        }
        grant.message = Some(MessageBuffer::new(
            Arc::clone(&self.shared.usage),
            self.shared.config.max_endpoints,
        )?);
        let message = grant
            .message
            .as_mut()
            .ok_or(Error::Internal("missing RDMA message"))?;
        let registered = grant.device.register_message(
            &mut message.handles,
            message.memory.address,
            message.memory.length,
        );
        message.registered();
        registered
    }

    pub(super) fn post_receive(
        &self,
        record: &Arc<Record>,
        claim: u64,
        expected: [u8; COMPLETION_MESSAGE_SIZE],
        credits: super::receive::ReceiveCredits,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected
                || request.receive.is_some()
                || !request
                    .resources
                    .as_ref()
                    .is_some_and(|r| r.grant.as_ref().is_some_and(|g| g.window_bound))
            {
                return Err(Error::Internal("RDMA receive before source grant"));
            }
            let namespace = self.shared.next_work_namespace()?;
            self.prepare_message(&mut request)?;
            let rkey = request
                .resources
                .as_ref()
                .ok_or(Error::Internal("missing RDMA grant"))?
                .grant()?
                .rkey;
            request.receive = Some(ReceiveQueue::new(
                namespace * super::WR_ID_STRIDE + 1,
                expected,
                rkey,
                deadline,
                credits,
            ));
            record.receive_namespace.store(namespace, Ordering::Release);
        }
        record.actions.fetch_or(RECEIVE, Ordering::Release);
        self.shared.signal();
        self.wait_receive(record, claim, deadline, false)
    }

    pub(super) fn receive_completion(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<()> {
        self.wait_receive(record, claim, deadline, true)
    }

    fn wait_receive(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
        completed: bool,
    ) -> Result<()> {
        let mut request = record.lock()?;
        loop {
            request.check_claim(claim)?;
            let receive = request
                .receive
                .as_ref()
                .ok_or(Error::Internal("missing RDMA receive"))?;
            if if completed {
                receive.completed
            } else {
                receive.posted
            } {
                return Ok(());
            }
            let duration = match remaining(deadline) {
                Ok(duration) => duration,
                Err(error) => {
                    let pending = receive.pending();
                    request.failure = Some(Fault::Timeout);
                    drop(request);
                    if pending {
                        self.shared.fail(Fault::Timeout);
                    } else {
                        self.shared.signal();
                    }
                    return Err(error);
                }
            };
            request = record
                .changed
                .wait_timeout(request, duration)
                .map_err(|_| Error::Unquiesced {
                    operation: "RDMA receive wait poisoned",
                    code: libc::EIO,
                })?
                .0;
        }
    }

    pub(super) fn send_completion(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: [u8; COMPLETION_MESSAGE_SIZE],
        rkey: u32,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected
                || request.message_sent
                || !request.work.as_ref().is_some_and(|w| {
                    matches!(
                        w.operation,
                        slice::Operation::Read | slice::Operation::Write
                    ) && w.queue.finished()
                })
            {
                return Err(Error::Internal("RDMA completion before payload CQ drain"));
            }
            if request.work.as_ref().is_none_or(|w| w.remote.rkey != rkey) {
                return Err(Error::Protocol(
                    "completion invalidation changed its payload key",
                ));
            }
            let namespace = self.shared.next_work_namespace()?;
            self.prepare_message(&mut request)?;
            let message = request
                .resources
                .as_mut()
                .and_then(|r| r.grant.as_mut())
                .and_then(|g| g.message.as_mut())
                .ok_or(Error::Internal("missing RDMA message"))?;
            message.memory.bytes_mut().copy_from_slice(&binding);
            request.message_sent = true;
            request.work = Some(Work {
                queue: slice::SendQueue::control(
                    Arc::clone(&self.shared.usage),
                    namespace * super::WR_ID_STRIDE + 1,
                )?,
                operation: slice::Operation::Send,
                remote: Descriptor {
                    rkey,
                    ..Descriptor::default()
                },
                deadline,
            });
            record.send_namespace.store(namespace, Ordering::Release);
            request.phase = Phase::Queued;
        }
        self.wait(record, claim, deadline)
    }

    pub(super) fn wait(&self, record: &Arc<Record>, claim: u64, deadline: Instant) -> Result<()> {
        record.actions.fetch_or(WORK, Ordering::Release);
        self.shared.signal();
        let mut request = record.lock()?;
        loop {
            request.check_claim(claim)?;
            if request
                .work
                .as_ref()
                .is_some_and(|work| work.queue.finished())
            {
                return Ok(());
            }
            let duration = match remaining(deadline) {
                Ok(duration) => duration,
                Err(error) => {
                    let pending = request
                        .work
                        .as_ref()
                        .is_some_and(|work| work.queue.pending());
                    request.failure = Some(Fault::Timeout);
                    drop(request);
                    if pending {
                        self.shared.fail(Fault::Timeout);
                    } else {
                        self.shared.signal();
                    }
                    return Err(error);
                }
            };
            request = record
                .changed
                .wait_timeout(request, duration)
                .map_err(|_| Error::Unquiesced {
                    operation: "RDMA progress wait poisoned",
                    code: libc::EIO,
                })?
                .0;
        }
    }

    pub(super) fn finish_sent(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: &[u8; COMPLETION_MESSAGE_SIZE],
        deadline: Instant,
    ) -> Result<()> {
        self.finish(record, claim, binding, None, deadline)
    }

    pub(super) fn finish_received(
        &self,
        record: &Arc<Record>,
        claim: u64,
        done: &BoundDone,
        deadline: Instant,
    ) -> Result<()> {
        self.finish(record, claim, done.binding(), Some(done), deadline)
    }

    fn finish(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: &[u8; COMPLETION_MESSAGE_SIZE],
        done: Option<&BoundDone>,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        record.require_live()?;
        let mut request = record.lock()?;
        request.check_claim(claim)?;
        if request.phase != Phase::Connected || request.finish.is_some() {
            return Err(Error::Internal("RDMA grant cannot finish"));
        }
        let resources = request
            .resources
            .as_ref()
            .ok_or(Error::Internal("missing RDMA connection"))?;
        let grant = resources.grant()?;
        if !matches!(grant.buffer, Buffer::Direct { .. })
            || binding[COMPLETION_MESSAGE_SIZE - 1] != 1
        {
            return Err(Error::Protocol(
                "only negotiated host Direct grants can retain a QP",
            ));
        }
        let work = request
            .work
            .as_ref()
            .ok_or(Error::Internal("missing RDMA completion work"))?;
        if !work.queue.finished() || work.queue.pending() || grant.window_bound {
            return Err(Error::Internal("RDMA operation lacks completion drain"));
        }
        let connection = resources
            .connection
            .incarnation
            .ok_or(Error::Internal("missing connection incarnation"))?;
        let identity_offset = crate::runtime::control_plane::REQUEST_SIZE
            + if grant.permission == 0 { 40 } else { 0 };
        let native_identity_matches =
            binding[identity_offset + 16..identity_offset + 40] == connection.encode();
        let length_matches =
            crate::runtime::control_plane::u64_at(binding, 44) == grant.buffer.len() as u64;
        let generation_matches = grant.permission == 0
            || crate::runtime::control_plane::u64_at(binding, COMPLETION_MESSAGE_SIZE - 9)
                == grant.generation;
        let operation_matches = match grant.permission {
            0 => matches!(binding[10], 4 | 5),
            1 => binding[10] == 4,
            2 => binding[10] == 5,
            _ => false,
        };
        if !native_identity_matches || !length_matches || !generation_matches || !operation_matches
        {
            return Err(Error::Protocol(
                "RDMA operation receipt changed native grant identity",
            ));
        }
        let valid = if grant.permission == 0 {
            done.is_none()
                && request.message_sent
                && !request.mixed_operations
                && request
                    .payload_operation
                    .is_some_and(|op| binding[10] == if op == TransferOp::Read { 4 } else { 5 })
                && work.operation == slice::Operation::Send
                && request.receive.is_none()
                && grant
                    .message
                    .as_ref()
                    .is_some_and(|m| m.memory.bytes() == binding)
        } else {
            done.is_some()
                && !request.message_sent
                && work.operation == slice::Operation::Bind
                && request.receive.as_ref().is_some_and(|r| {
                    r.completed && !r.pending() && r.expected == *binding && r.rkey == grant.rkey
                })
        };
        if !valid {
            return Err(Error::Protocol("RDMA grant lacks sealed operation receipt"));
        }
        request.finish = Some(GrantDrain {
            connection: resources
                .connection
                .incarnation
                .ok_or(Error::Internal("missing connection incarnation"))?,
            generation: grant.generation,
            permission: grant.permission,
        });
        request.phase = Phase::Drained;
        record.actions.fetch_or(FINISH, Ordering::Release);
        drop(request);
        self.shared.signal();
        let mut request = record.lock()?;
        loop {
            request.check_claim(claim)?;
            if request.finish.is_none()
                && request
                    .resources
                    .as_ref()
                    .is_some_and(|r| r.grant.is_none())
            {
                return Ok(());
            }
            let duration = remaining(deadline).map_err(|_| Error::Unquiesced {
                operation: "RDMA grant cleanup deadline",
                code: libc::ETIMEDOUT,
            })?;
            request = record
                .changed
                .wait_timeout(request, duration)
                .map_err(|_| Error::Unquiesced {
                    operation: "RDMA grant wait poisoned",
                    code: libc::EIO,
                })?
                .0;
        }
    }

    pub(super) fn claim(
        self: &Arc<Self>,
        matches: impl Fn(&Peer) -> bool,
        deadline: Instant,
    ) -> Result<Option<RdmaGrant>> {
        remaining(deadline)?;
        let claimed = {
            let mut control = self
                .shared
                .control
                .lock()
                .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
            self.shared.asynchronous.check()?;
            if let Some(fault) = control.fault {
                return Err(fault.error());
            }
            if !control.accepting {
                return Err(Error::Unavailable("RDMA claims are fenced"));
            }
            let claimed = control.endpoints.claim(matches);
            if claimed.is_some() {
                self.shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .idle_endpoints -= 1;
            }
            claimed
        };
        let Some((record, peer)) = claimed else {
            return Ok(None);
        };
        let (local, incarnation, claim) = {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check()?;
            if request.phase != Phase::Ready || request.close_requested {
                let old = request.claim;
                drop(request);
                self.close_record(&record, old, deadline)?;
                return Err(Error::Unavailable("claimed RDMA QP is retiring"));
            }
            let claim = match request.claim.checked_add(1) {
                Some(claim) => claim,
                None => {
                    let old = request.claim;
                    drop(request);
                    self.close_record(&record, old, deadline)?;
                    return Err(Error::ResourceExhausted);
                }
            };
            let resources = request
                .resources
                .as_ref()
                .ok_or(Error::Internal("missing idle connection"))?;
            if resources.grant.is_some() {
                return Err(Error::Internal("idle connection retains a grant"));
            }
            let local = resources.connection.local;
            let incarnation = resources.connection.incarnation;
            request.claim = claim;
            request.phase = Phase::Init;
            (local, incarnation, claim)
        };
        self.shared
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reused_connections += 1;
        Ok(Some(RdmaGrant {
            record,
            progress: Arc::clone(self),
            local,
            incarnation,
            config: self.shared.config.clone(),
            deadline,
            output: None,
            claim,
            released: false,
            cached_peer: Some(peer),
            endpoint: None,
        }))
    }

    pub(super) fn cache(&self, record: &Arc<Record>, claim: u64, peer: Peer) -> Result<bool> {
        let mut request = record.lock()?;
        request.check_claim(claim)?;
        record.require_live()?;
        if request.phase != Phase::Drained
            || request.close_requested
            || request.finish.is_some()
            || request.resources.as_ref().is_none_or(|r| r.grant.is_some())
            || request.work.is_some()
            || request.receive.is_some()
            || record.actions.load(Ordering::Acquire) != 0
        {
            return Err(Error::Internal("RDMA READY retains grant state"));
        }
        let mut control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
        if let Some(fault) = control.fault {
            return Err(fault.error());
        }
        if !control.accepting {
            return Ok(false);
        }
        control.endpoints.ready(record, peer, claim)?;
        request.phase = Phase::Ready;
        self.shared
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .idle_endpoints += 1;
        Ok(true)
    }

    pub(super) fn evict_idle(&self, deadline: Instant) -> Result<bool> {
        let record = {
            let mut control = self
                .shared
                .control
                .lock()
                .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
            if control.fault.is_some() {
                return Ok(false);
            }
            let record = control.endpoints.evict();
            if record.is_some() {
                let mut usage = self.shared.usage.lock().unwrap_or_else(|p| p.into_inner());
                usage.idle_endpoints -= 1;
                usage.evicted_connections += 1;
            }
            record
        };
        let Some(record) = record else {
            return Ok(false);
        };
        let claim = record.lock()?.claim;
        match self.close_record(&record, claim, deadline) {
            Err(error @ Error::Unquiesced { .. }) => return Err(error),
            Err(error) => {
                // Eviction releases capacity after the actual barrier, even if
                // the lane records a logical fault during native retirement.
                if record.lock()?.phase != Phase::Closed {
                    return Err(error);
                }
            }
            Ok(()) => {}
        }
        Ok(true)
    }

    pub(super) fn retire_idle(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<()> {
        let retired = {
            let mut control = self
                .shared
                .control
                .lock()
                .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
            let retired = control.endpoints.retire_idle(record, claim);
            if retired {
                #[cfg(test)]
                tests::before_idle_debit(&self.shared);
                self.shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .idle_endpoints -= 1;
            }
            retired
        };
        if retired {
            self.close_record(record, claim, deadline)?;
        }
        Ok(())
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
                if control.endpoints.detach(record) {
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
            && control.endpoints.len() == 0)
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

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/cq_lane.rs"]
mod tests;
