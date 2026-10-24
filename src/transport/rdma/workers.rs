use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use super::COMPLETION_MESSAGE_SIZE;
use super::cq::{CompletionEvents, RdmaCq};
use super::endpoint_store::{EndpointStore, Peer};
use super::rdma_transport::BoundDone;
use super::receive::{MessageBuffer, ReceiveQueue};
use super::{
    Buffer, ConnectionId, Descriptor, Phase, RdmaConfig, RdmaContext, RdmaEndpoint,
    RdmaProgressMode, RdmaStats, Resources, events, slice, sys,
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

pub(super) struct Workers {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

pub(super) struct Shared {
    asynchronous: Arc<events::State>,
    next_work_namespace: AtomicU64,
    completion_events: Option<Arc<CompletionEvents>>,
    control: Mutex<Control>,
    wake: Condvar,
    usage: Arc<Mutex<RdmaStats>>,
    verbs: &'static sys::Verbs,
    config: RdmaConfig,
}

struct Control {
    cq: Option<Box<RdmaCq>>,
    receive_cq: Option<Box<RdmaCq>>,
    endpoints: EndpointStore,
    next_tag: u64,
    dirty: bool,
    stopping: bool,
    accepting: bool,
    finished: bool,
    fault: Option<Fault>,
    retained: Option<(&'static str, i32)>,
}

struct WorkerExit(Arc<Shared>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        let shared = &self.0;
        if !shared
            .control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .finished
        {
            shared.retain("RDMA progress worker stopped", libc::EIO);
        }
        shared
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .progress_workers -= 1;
    }
}

impl Workers {
    pub(super) fn new(
        config: &RdmaConfig,
        device: Arc<RdmaContext>,
        usage: Arc<Mutex<RdmaStats>>,
        verbs: &'static sys::Verbs,
    ) -> Result<Arc<Self>> {
        let endpoints = EndpointStore::new(config.max_endpoints)?;
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
        let shared = Arc::new(Shared {
            asynchronous: Arc::clone(&device.asynchronous),
            next_work_namespace: AtomicU64::new(1),
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
                fault: None,
                retained: None,
            }),
            wake: Condvar::new(),
            usage,
            verbs,
            config: config.clone(),
        });
        device.asynchronous.attach(Arc::downgrade(&shared));
        let owner = Arc::new(Self {
            shared: Arc::clone(&shared),
            worker: Mutex::default(),
        });
        shared
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
            .progress_workers += 1;
        let worker = thread::Builder::new()
            .name("orbitte-rdma".into())
            .spawn(move || {
                let _exit = WorkerExit(Arc::clone(&shared));
                shared.run();
            });
        match worker {
            Ok(worker) => {
                *owner
                    .worker
                    .lock()
                    .map_err(|_| Error::Internal("RDMA worker lock poisoned"))? = Some(worker)
            }
            Err(error) => {
                owner
                    .shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .progress_workers -= 1;
                if let Err((operation, code)) = owner.shared.close_cq() {
                    owner.shared.retain(operation, code);
                    return Err(Error::Unquiesced { operation, code });
                }
                owner
                    .shared
                    .control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .finished = true;
                return Err(Error::Io(error));
            }
        }
        Ok(owner)
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
        self.shared.wake.notify_one();
        Ok(record)
    }

    pub(super) fn transfer(
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
        self.wait(record, claim, deadline)
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
                Arc::clone(&self.shared.usage),
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

    fn wait(&self, record: &Arc<Record>, claim: u64, deadline: Instant) -> Result<()> {
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
    ) -> Result<Option<RdmaEndpoint>> {
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
        Ok(Some(RdmaEndpoint {
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

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        let mut control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?;
        control.stopping = true;
        control.dirty = true;
        self.shared.wake.notify_one();
        while !control.finished {
            control = self
                .shared
                .wake
                .wait_timeout(control, remaining(deadline)?)
                .map_err(|_| Error::Internal("RDMA progress lock poisoned"))?
                .0;
        }
        let retained = control.retained;
        drop(control);
        if let Some(worker) = self
            .worker
            .lock()
            .map_err(|_| Error::Internal("RDMA worker lock poisoned"))?
            .take()
        {
            worker.join().map_err(|_| Error::Unquiesced {
                operation: "RDMA progress worker",
                code: libc::EIO,
            })?;
        }
        match retained {
            Some((operation, code)) => Err(Error::Unquiesced { operation, code }),
            None => Ok(()),
        }
    }
}

impl Drop for Workers {
    fn drop(&mut self) {
        // Last public/native request owner is gone. Native calls remain non-preemptible.
        let deadline = Instant::now() + Duration::from_secs(300);
        let _ = self.close(deadline);
    }
}

impl Shared {
    pub(super) fn signal(&self) {
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        control.dirty = true;
        self.wake.notify_one();
    }

    fn fail(&self, fault: Fault) {
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        if control.fault.is_none() {
            control.accepting = false;
            control.endpoints.detach_all();
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .idle_endpoints = 0;
            control.fault = Some(fault);
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .progress_failures += 1;
        }
        control.dirty = true;
        self.wake.notify_all();
    }

    fn close_cq(&self) -> std::result::Result<(), (&'static str, i32)> {
        let mut queues = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            [control.cq.take(), control.receive_cq.take()]
        };
        for index in 0..queues.len() {
            if let Some(queue) = &mut queues[index] {
                if let Err(error) = queue.close(self.verbs) {
                    let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
                    control.cq = queues[0].take();
                    control.receive_cq = queues[1].take();
                    return Err(error);
                }
            }
            queues[index] = None;
        }
        if let Some(events) = &self.completion_events {
            events.close()?;
        }
        Ok(())
    }

    fn next_work_namespace(&self) -> Result<u64> {
        let mut current = self.next_work_namespace.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(1)
                .filter(|next| current != 0 && next.checked_mul(super::WR_ID_STRIDE).is_some())
                .ok_or(Error::ResourceExhausted)?;
            match self.next_work_namespace.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(current),
                Err(observed) => current = observed,
            }
        }
    }

    fn retain(&self, operation: &'static str, code: i32) {
        let records = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            if control.finished {
                return;
            }
            if control.fault.is_none() {
                self.usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .progress_failures += 1;
            }
            control.fault = Some(Fault::Unquiesced(operation, code));
            control.accepting = false;
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .idle_endpoints = 0;
            control.endpoints.detach_all();
            control.endpoints.take_all()
        };
        let mut count = 0;
        let mut grants = 0;
        let mut bytes = 0;
        let mut debt = 0;
        let mut windows = 0;
        let mut receive_debt = 0;
        let mut message_mrs = 0;
        let mut message_bytes = 0;
        for record in &records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if request.phase != Phase::Retained {
                count += usize::from(
                    request
                        .resources
                        .as_ref()
                        .is_some_and(|r| r.connection.lease.is_some()),
                );
                grants += usize::from(
                    request
                        .resources
                        .as_ref()
                        .is_some_and(|r| r.grant.is_some()),
                );
                bytes += request
                    .resources
                    .as_ref()
                    .and_then(|r| r.grant.as_ref())
                    .map_or(0, |g| g.buffer.len());
                debt += request.work.as_ref().map_or(0, |w| w.queue.outstanding());
                receive_debt += usize::from(request.receive.as_ref().is_some_and(|r| r.pending()));
                if let Some(message) = request
                    .resources
                    .as_ref()
                    .and_then(|r| r.grant.as_ref())
                    .and_then(|g| g.message.as_ref())
                {
                    message_mrs += usize::from(!message.handles.mr.is_null());
                    message_bytes += message.memory.length;
                }
                windows +=
                    usize::from(request.resources.as_ref().is_some_and(|r| {
                        r.grant.as_ref().is_some_and(|g| !g.handles.mw.is_null())
                    }));
                request.phase = Phase::Retained;
                request.failure = Some(Fault::Unquiesced(operation, code));
            }
            record.changed.notify_all();
        }
        let cq = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            control.retained = Some((operation, code));
            let cq = [control.cq.take(), control.receive_cq.take()];
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.retained_endpoints += count;
            usage.retained_grants += grants;
            usage.retained_bytes += bytes;
            usage.retained_work_requests += debt;
            usage.retained_cqs += cq.iter().filter(|q| q.is_some()).count();
            usage.retained_receive_work_requests += receive_debt;
            usage.retained_message_mrs += message_mrs;
            usage.retained_message_bytes += message_bytes;
            usage.retained_windows += windows;
            control.finished = true;
            cq
        };
        // Fence every constructor before publishing completion or losing owners.
        // Actual CQ/PD, QPs/MRs, backing, DMA loans and unresolved debt survive.
        if let Some(events) = &self.completion_events {
            events.retain();
        }
        std::mem::forget((cq, records));
        self.wake.notify_all();
    }

    fn unlock(request: &mut Request) -> std::result::Result<(), (&'static str, i32)> {
        if request.work.as_ref().is_some_and(|w| w.queue.pending())
            || request.receive.as_ref().is_some_and(|r| r.pending())
        {
            return Err(("RDMA grant has unresolved completion debt", libc::EBUSY));
        }
        if let Some(resources) = &mut request.resources {
            if !resources.connection.closed {
                return Err(("RDMA connection lacks native drain receipt", libc::EBUSY));
            }
            if let Some(grant) = &mut resources.grant {
                if !grant.handles.mw.is_null() || !grant.handles.mr.is_null() {
                    return Err(("RDMA grant lacks native drain receipt", libc::EBUSY));
                }
                if let Buffer::Direct { loan, .. } = &mut grant.buffer {
                    drop(loan.take());
                }
            }
        }
        request.phase = Phase::Closed;
        Ok(())
    }

    fn finish_request(&self, record: &Arc<Record>) -> std::result::Result<(), (&'static str, i32)> {
        if record.actions.load(Ordering::Acquire) & FINISH == 0 {
            return Ok(());
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if record.terminal.load(Ordering::Acquire) || request.close_requested {
            return Ok(());
        }
        if request.phase != Phase::Drained {
            return Err(("RDMA finish phase changed", libc::EIO));
        }
        let Request {
            resources, finish, ..
        } = &mut *request;
        let resources = resources
            .as_mut()
            .ok_or(("missing RDMA finish resources", libc::EIO))?;
        let receipt = finish
            .as_ref()
            .ok_or(("missing sealed RDMA receipt", libc::EIO))?;
        let grant = resources
            .grant
            .as_mut()
            .ok_or(("missing RDMA finish grant", libc::EIO))?;
        grant.close_drained(&resources.connection, receipt, self.verbs)?;
        if let Buffer::Direct { loan, .. } = &mut grant.buffer {
            drop(loan.take());
        }
        drop(resources.grant.take());
        request.work = None;
        request.receive = None;
        request.message_sent = false;
        request.payload_operation = None;
        request.mixed_operations = false;
        request.finish = None;
        record.actions.store(0, Ordering::Release);
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completed_reusable_grants += 1;
        record.changed.notify_all();
        Ok(())
    }

    fn close_request(
        &self,
        record: &Arc<Record>,
    ) -> std::result::Result<bool, (&'static str, i32)> {
        if record.actions.load(Ordering::Acquire) & CLOSE == 0 {
            return Ok(false);
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if !request.close_requested || matches!(request.phase, Phase::Closed | Phase::Retained) {
            return Ok(false);
        }
        if request
            .work
            .as_ref()
            .is_some_and(|work| work.queue.pending())
            || request.receive.as_ref().is_some_and(|r| r.pending())
        {
            drop(request);
            self.fail(Fault::Internal("RDMA close with unresolved work"));
            return Ok(false);
        }
        let resources = request
            .resources
            .as_mut()
            .ok_or(("RDMA resources missing", libc::EIO))?;
        if let Err(Error::Unquiesced { operation, code }) = self.asynchronous.check() {
            return Err((operation, code));
        }
        resources.connection.close(self.verbs)?;
        if let Some(grant) = &mut resources.grant {
            grant.close(&resources.connection, self.verbs)?;
        }
        Self::unlock(&mut request)?;
        self.control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .endpoints
            .remove_retired(record);
        drop(request);
        record.changed.notify_all();
        Ok(true)
    }

    fn abort(&self, records: &[Arc<Record>], fault: Fault) {
        // Fence accepted constructors under the same lock as native QP creation.
        // No other thread polls or submits native WRs.
        for record in records {
            #[cfg(test)]
            tests::before_abort_lock(self, record);
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(Error::Unquiesced { operation, code }) = self.asynchronous.check() {
                drop(request);
                self.retain(operation, code);
                return;
            }
            request.failure.get_or_insert(fault);
            let Some(resources) = &mut request.resources else {
                continue;
            };
            if let Err((operation, code)) = resources.connection.close(self.verbs) {
                drop(request);
                self.retain(operation, code);
                return;
            }
        }
        if let Err((operation, code)) = self.close_cq() {
            self.retain(operation, code);
            return;
        }
        // Shared CQ destruction, after all QPs, is the cancellation receipt.
        for record in records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(work) = &mut request.work {
                work.queue.quiesce();
            }
            if let Some(receive) = &mut request.receive {
                receive.quiesce();
            }
            if let Some(resources) = &mut request.resources {
                if let Some(grant) = &mut resources.grant {
                    if let Err((operation, code)) = grant.close(&resources.connection, self.verbs) {
                        drop(request);
                        self.retain(operation, code);
                        return;
                    }
                }
            }
        }
        for record in records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            request.failure.get_or_insert(fault);
            if let Err((operation, code)) = Self::unlock(&mut request) {
                drop(request);
                self.retain(operation, code);
                return;
            }
            record.changed.notify_all();
        }
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        control.endpoints.clear_after_drain();
        control.finished = true;
        self.wake.notify_all();
    }

    fn post(&self, record: &Arc<Record>) -> bool {
        if record.actions.load(Ordering::Acquire) & WORK == 0 {
            return false;
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if record.terminal.load(Ordering::Acquire)
            || request.failure.is_some()
            || request.close_requested
        {
            return false;
        }
        let Request {
            resources,
            work,
            phase,
            ..
        } = &mut *request;
        let Some(work) = work else {
            return false;
        };
        if Instant::now() >= work.deadline {
            if work.queue.pending() {
                drop(request);
                self.fail(Fault::Timeout);
            } else {
                request.failure = Some(Fault::Timeout);
                record.changed.notify_all();
            }
            return false;
        }
        if work.queue.pending() || work.queue.finished() {
            return false;
        }
        let Some(resources) = resources else {
            return false;
        };
        let Some(grant) = &mut resources.grant else {
            return false;
        };
        if work.operation == slice::Operation::Send && grant.message.is_none() {
            request.failure = Some(Fault::Internal("missing RDMA SEND message"));
            record.changed.notify_all();
            return false;
        }
        let requests = match work
            .queue
            .reserve(self.config.max_send_wr, self.config.max_cqes)
        {
            Ok(requests) => requests,
            Err(Error::ResourceExhausted) => return false,
            Err(error) => {
                request.failure = Some(Fault::from_error(error));
                record.changed.notify_all();
                return false;
            }
        };
        let mut posted = sys::PostResult::default();
        if let Err(error) = self.asynchronous.check() {
            drop(request);
            self.fail(Fault::from_error(error));
            return false;
        }
        *phase = Phase::Posted;
        let code = unsafe {
            match work.operation {
                slice::Operation::Read | slice::Operation::Write => (self.verbs.post)(
                    &mut resources.connection.handles,
                    &grant.handles,
                    requests.as_ptr(),
                    requests.len(),
                    grant.buffer.address().as_ptr() as u64,
                    grant.buffer.len(),
                    work.remote.address,
                    work.remote.rkey,
                    i32::from(work.operation == slice::Operation::Write),
                    &mut posted,
                ),
                slice::Operation::Send => {
                    let Some(message) = &grant.message else {
                        return false;
                    };
                    (self.verbs.send_completion)(
                        &mut resources.connection.handles,
                        &message.handles,
                        requests[0].id,
                        message.memory.address.as_ptr() as u64,
                        message.memory.length,
                        work.remote.rkey,
                        &mut posted,
                    )
                }
                _ => (self.verbs.window)(
                    &mut resources.connection.handles,
                    &mut grant.handles,
                    requests[0].id,
                    grant.buffer.address().as_ptr() as u64,
                    grant.buffer.len(),
                    grant.permission,
                    work.operation == slice::Operation::Bind,
                    &mut posted,
                ),
            }
        };
        if let Err(error) = work.queue.posted(code, posted) {
            let fault = Fault::from_error(error);
            let pending = work.queue.pending();
            request.failure = Some(fault);
            drop(request);
            if pending {
                self.fail(fault);
            } else {
                record.actions.fetch_and(!WORK, Ordering::Release);
            }
            record.changed.notify_all();
        }
        true
    }

    fn complete(&self, completion: sys::Completion, receive_cq: bool) {
        let Some(tag) = completion
            .id
            .checked_sub(1)
            .map(|id| id / super::WR_ID_STRIDE)
        else {
            self.fail(Fault::Protocol("RDMA completion identity"));
            return;
        };
        let record = self
            .control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .endpoints
            .work_owner(tag, receive_cq);
        let Some(record) = record else {
            self.fail(Fault::Protocol("RDMA unknown completion owner"));
            return;
        };
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        let result = if completion.qpn != request.qpn {
            Err(Error::Protocol("RDMA completion QP identity"))
        } else if receive_cq {
            Self::complete_receive(&mut request, completion)
        } else if let Some(work) = &mut request.work {
            work.queue.complete(completion, work.operation)
        } else {
            Err(Error::Protocol("RDMA completion before submission"))
        };
        if result.is_ok() && !receive_cq {
            let operation = request.work.as_ref().map(|w| w.operation);
            if request.work.as_ref().is_some_and(|w| w.queue.finished()) {
                request.phase = Phase::Connected;
            }
            if operation == Some(slice::Operation::Bind) {
                if let Some(grant) = request.resources.as_mut().and_then(|r| r.grant.as_mut()) {
                    grant.window_bound = true;
                }
            }
            #[cfg(test)]
            if operation == Some(slice::Operation::Invalidate) {
                if let Some(grant) = request.resources.as_mut().and_then(|r| r.grant.as_mut()) {
                    grant.window_bound = false;
                }
            }
        }
        if let Err(error) = result {
            let fault = Fault::from_error(error);
            request.failure = Some(fault);
            drop(request);
            self.fail(fault);
        } else {
            if request
                .work
                .as_ref()
                .is_some_and(|work| work.queue.finished())
            {
                record.actions.fetch_and(!WORK, Ordering::Release);
            }
            drop(request);
        }
        // The handler owns a strong record until all of its accesses have ended.
        record.changed.notify_all();
    }

    fn post_receive(&self, record: &Arc<Record>) -> bool {
        if record.actions.load(Ordering::Acquire) & RECEIVE == 0 {
            return false;
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if record.terminal.load(Ordering::Acquire)
            || request.failure.is_some()
            || request.close_requested
        {
            return false;
        }
        let Request {
            resources, receive, ..
        } = &mut *request;
        let (Some(resources), Some(receive)) = (resources, receive) else {
            return false;
        };
        if receive.completed {
            return false;
        }
        if Instant::now() >= receive.deadline {
            let pending = receive.pending();
            request.failure = Some(Fault::Timeout);
            drop(request);
            if pending {
                self.fail(Fault::Timeout);
            }
            record.changed.notify_all();
            return false;
        }
        if receive.posted {
            return false;
        }
        let Some(message) = resources.grant.as_ref().and_then(|g| g.message.as_ref()) else {
            request.failure = Some(Fault::Internal("missing RDMA receive message"));
            record.changed.notify_all();
            return false;
        };
        if let Err(error) = self.asynchronous.check() {
            drop(request);
            self.fail(Fault::from_error(error));
            return false;
        }
        match receive.reserve(self.config.max_cqes) {
            Err(Error::ResourceExhausted) => return false,
            Err(error) => {
                request.failure = Some(Fault::from_error(error));
                record.changed.notify_all();
                return false;
            }
            Ok(()) => {}
        }
        let mut result = sys::PostResult::default();
        let code = unsafe {
            (self.verbs.post_receive)(
                &mut resources.connection.handles,
                &message.handles,
                receive.id,
                message.memory.address.as_ptr() as u64,
                message.memory.length,
                &mut result,
            )
        };
        if let Err(error) = receive.posted(code, result) {
            let fault = Fault::from_error(error);
            let pending = receive.pending();
            request.failure = Some(fault);
            drop(request);
            if pending {
                self.fail(fault);
            }
        } else {
            drop(request);
        }
        record.changed.notify_all();
        true
    }

    fn complete_receive(request: &mut Request, completion: sys::Completion) -> Result<()> {
        let receive = request
            .receive
            .as_mut()
            .ok_or(Error::Protocol("RDMA unexpected receive"))?;
        receive.complete(completion)?;
        let resources = request
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA receive owner"))?;
        let grant = resources.grant_mut()?;
        let message = grant
            .message
            .as_ref()
            .ok_or(Error::Internal("missing RDMA receive bytes"))?;
        // The actual successful RQ CQE ended this receive's DMA; no new receive is posted.
        if message.memory.bytes() != receive.expected {
            return Err(Error::Protocol("RDMA received grant binding mismatch"));
        }
        if !grant.window_bound || grant.rkey != receive.rkey {
            return Err(Error::Protocol("RDMA receive changed grant authority"));
        }
        grant.window_bound = false;
        receive.completed = true;
        let mut usage = grant._lease.usage.lock().unwrap_or_else(|p| p.into_inner());
        usage.completed_receives += 1;
        usage.completed_window_invalidations += 1;
        Ok(())
    }

    fn run(&self) {
        let mut records = Vec::with_capacity(self.config.max_endpoints);
        let mut cursor = 0usize;
        loop {
            if let Err(error) = self.asynchronous.check() {
                match error {
                    Error::Unquiesced { operation, code } => {
                        self.retain(operation, code);
                        return;
                    }
                    error => self.fail(Fault::from_error(error)),
                }
            }
            let (fault, stopping, cqs) = {
                let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
                records.clear();
                control.endpoints.snapshot(&mut records);
                control.dirty = false;
                (
                    control.fault,
                    control.stopping,
                    [
                        control.cq.as_ref().map(|cq| cq.handles.cq),
                        control.receive_cq.as_ref().map(|cq| cq.handles.cq),
                    ],
                )
            };
            if let Some(fault) = fault {
                self.abort(&records, fault);
                return;
            }
            if stopping {
                if !records.is_empty() {
                    self.fail(Fault::Unavailable("RDMA progress stopped"));
                    continue;
                }
                if let Err((operation, code)) = self.close_cq() {
                    self.retain(operation, code);
                    return;
                }
                self.control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .finished = true;
                self.wake.notify_all();
                return;
            }
            if self.completion_events.is_some() {
                let control = self.control.lock().unwrap_or_else(|p| p.into_inner());
                let armed = [control.cq.as_ref(), control.receive_cq.as_ref()]
                    .into_iter()
                    .flatten()
                    .try_for_each(|cq| cq.arm(self.verbs));
                drop(control);
                if let Err(error) = armed {
                    match error {
                        Error::Unquiesced { operation, code } => {
                            self.retain(operation, code);
                            return;
                        }
                        error => self.fail(Fault::from_error(error)),
                    }
                    continue;
                }
            }
            for record in &records {
                if let Err((operation, code)) = self.finish_request(record) {
                    self.retain(operation, code);
                    return;
                }
            }
            let mut index = 0;
            while index < records.len() {
                match self.close_request(&records[index]) {
                    Ok(true) => {
                        records.swap_remove(index);
                    }
                    Ok(false) => index += 1,
                    Err((operation, code)) => {
                        self.retain(operation, code);
                        return;
                    }
                }
            }
            for index in 0..records.len() {
                if self
                    .control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .fault
                    .is_some()
                {
                    break;
                }
                let record = &records[(index + cursor) % records.len()];
                self.post(record);
                self.post_receive(record);
            }
            cursor = cursor.wrapping_add(1);
            let pending_qps = records
                .iter()
                .filter(|record| {
                    record.actions.load(Ordering::Acquire) & WORK != 0
                        && record
                            .state
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .work
                            .as_ref()
                            .is_some_and(|work| work.queue.pending())
                })
                .count();
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.peak_pending_qps = usage.peak_pending_qps.max(pending_qps);
            let pending = usage.work_requests != 0 || usage.receive_work_requests != 0;
            drop(usage);
            let mut progressed = false;
            if (pending || self.completion_events.is_some())
                && self
                    .control
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .fault
                    .is_none()
            {
                for (receive_cq, cq) in [(false, cqs[0]), (true, cqs[1])] {
                    let mut handles = sys::ConnectionHandles {
                        cq: cq.unwrap_or(std::ptr::null_mut()),
                        ..Default::default()
                    };
                    for _ in 0..64 {
                        let mut completion = sys::Completion::default();
                        let count = unsafe { (self.verbs.poll)(&mut handles, &mut completion) };
                        match count {
                            0 => break,
                            1 => {
                                progressed = true;
                                self.complete(completion, receive_cq);
                            }
                            code if code < 0 => {
                                self.fail(Fault::Backend("ibv_poll_cq", code));
                                break;
                            }
                            _ => {
                                self.fail(Fault::Protocol("RDMA poll returned more than one CQE"));
                                break;
                            }
                        }
                        if self
                            .control
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .fault
                            .is_some()
                        {
                            break;
                        }
                    }
                }
            }
            let wait = if self.completion_events.is_some() {
                if progressed {
                    continue;
                }
                let deadline = records
                    .iter()
                    .filter_map(|record| {
                        let request = record.state.lock().unwrap_or_else(|p| p.into_inner());
                        request
                            .work
                            .as_ref()
                            .filter(|work| !work.queue.finished())
                            .map(|work| work.deadline)
                            .into_iter()
                            .chain(
                                request
                                    .receive
                                    .as_ref()
                                    .filter(|receive| receive.pending())
                                    .map(|receive| receive.deadline),
                            )
                            .min()
                    })
                    .min();
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
            } else {
                Some(Duration::from_micros(50))
            };
            records.clear();
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            if control.dirty || control.fault.is_some() || control.stopping {
                continue;
            }
            if pending {
                if self.completion_events.is_some() {
                    self.usage
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .cq_event_waits += 1;
                }
                if let Some(duration) = wait {
                    drop(
                        self.wake
                            .wait_timeout(control, duration)
                            .unwrap_or_else(|p| p.into_inner()),
                    );
                } else {
                    drop(self.wake.wait(control).unwrap_or_else(|p| p.into_inner()));
                }
            } else {
                while !control.dirty && control.fault.is_none() && !control.stopping {
                    control = self.wake.wait(control).unwrap_or_else(|p| p.into_inner());
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/workers.rs"]
mod tests;
