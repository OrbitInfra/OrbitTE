//! Per-domain native resources, MR loans and a bounded CQ worker.
#![allow(unsafe_code)]
use super::{
    FabricConfig,
    fabric_attrs::Nic,
    sys::{self, Api, Handles, Info},
};
use crate::{Error, Result, TransferOp, memory::DmaLease, runtime::segment_manager::PeerKey};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    ffi::c_void,
    mem::ManuallyDrop,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Default)]
pub struct FabricStats {
    pub domains: usize,
    pub workers: usize,
    pub memory_regions: usize,
    pub registered_bytes: u64,
    pub peer_addresses: usize,
    pub retained_slices: usize,
    pub posted_slices: usize,
    pub quarantined: u64,
    pub post_calls: u64,
    pub cq_errors: u64,
    pub overdue_rejections: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
}
#[derive(Default)]
pub(super) struct Counters {
    domains: AtomicUsize,
    workers: AtomicUsize,
    mrs: AtomicUsize,
    bytes: AtomicU64,
    peers: AtomicUsize,
    slices: AtomicUsize,
    posted: AtomicUsize,
    quarantined: AtomicU64,
    posts: AtomicU64,
    errors: AtomicU64,
    overdue_rejections: AtomicU64,
    read: AtomicU64,
    write: AtomicU64,
}
impl Counters {
    pub fn retain(&self) {
        self.quarantined.fetch_add(1, Ordering::Relaxed);
    }
    pub fn stats(&self) -> FabricStats {
        let o = Ordering::Relaxed;
        FabricStats {
            domains: self.domains.load(o),
            workers: self.workers.load(o),
            memory_regions: self.mrs.load(o),
            registered_bytes: self.bytes.load(o),
            peer_addresses: self.peers.load(o),
            retained_slices: self.slices.load(o),
            posted_slices: self.posted.load(o),
            quarantined: self.quarantined.load(o),
            post_calls: self.posts.load(o),
            cq_errors: self.errors.load(o),
            overdue_rejections: self.overdue_rejections.load(o),
            read_bytes: self.read.load(o),
            written_bytes: self.write.load(o),
        }
    }
}
type Failure = (&'static str, i32);
pub(super) struct Loan(pub DmaLease);
// SAFETY: fabric accepts only host memory. The exclusive native range loan is
// retained across workers, and no Rust slice accesses the provider-owned bytes.
unsafe impl Send for Loan {}
// SAFETY: the loan is immutable; each native slice is nonoverlapping and lives
// until its CQE. Loan release happens once after every MR reference is gone.
unsafe impl Sync for Loan {}
impl Loan {
    pub fn address(&self) -> std::ptr::NonNull<u8> {
        self.0.address()
    }
}
fn unquiesced(e: Failure) -> Error {
    sys::uncertain(e.0, e.1)
}
struct Native {
    api: Arc<Api>,
    handles: Handles,
}
struct Address {
    address: u64,
    failed: Option<Failure>,
}
struct State {
    native: ManuallyDrop<Native>,
    failure: Option<Failure>,
    closed: bool,
    mrs: usize,
    slices: usize,
    posted: BTreeMap<usize, (Arc<Task>, usize)>,
    queue: VecDeque<Arc<Task>>,
    peers: HashMap<(PeerKey, Vec<u8>), Address>,
    overdue: HashMap<PeerKey, usize>,
}
// SAFETY: all native calls and provider pointers in State are protected by the
// domain mutex. The provider was checked for FI_THREAD_SAFE.
unsafe impl Send for State {}
pub(super) struct Context {
    pub info: Info,
    pub nic: Nic,
    config: FabricConfig,
    pub counters: Arc<Counters>,
    state: Mutex<State>,
    stop: AtomicBool,
    changed: Condvar,
    key: AtomicU64,
    #[cfg(test)]
    test_drain_expired: Arc<AtomicBool>,
}
impl Context {
    pub fn open(
        api: Arc<Api>,
        raw: *mut c_void,
        info: Info,
        config: &FabricConfig,
        counters: Arc<Counters>,
    ) -> Result<Arc<Self>> {
        let name = sys::string(&info.name)?;
        let mut h = Handles::default();
        let mut address = [0u8; 1024];
        let cq = info.tx_size.min(config.max_pending_ops).max(1);
        let opened = api.open(
            raw,
            cq,
            config.max_peer_addresses,
            config.tcp(),
            &mut h,
            &mut address,
        );
        let nic = Nic {
            name,
            addr: super::fabric_attrs::hex(&address[..opened.as_ref().copied().unwrap_or(0)]),
            numa: info.numa,
        };
        let context = Arc::new(Self {
            info,
            nic,
            config: config.clone(),
            counters,
            state: Mutex::new(State {
                native: ManuallyDrop::new(Native { api, handles: h }),
                failure: None,
                closed: false,
                mrs: 0,
                slices: 0,
                posted: BTreeMap::new(),
                queue: VecDeque::new(),
                peers: HashMap::new(),
                overdue: HashMap::new(),
            }),
            stop: AtomicBool::new(false),
            changed: Condvar::new(),
            key: AtomicU64::new(1),
            #[cfg(test)]
            test_drain_expired: super::tests::drain_clock(),
        });
        context.counters.domains.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = opened {
            context.close()?;
            return Err(e);
        }
        Ok(context)
    }
    pub fn worker(self: &Arc<Self>, index: usize) -> Result<JoinHandle<()>> {
        let owner = self.clone();
        let worker = thread::Builder::new()
            .name(format!("orbite-fabric-{index}"))
            .spawn(move || {
                owner.counters.workers.fetch_add(1, Ordering::Relaxed);
                let mut idle = 0u32;
                while !owner.stop.load(Ordering::Acquire) {
                    let active = owner.tick();
                    if active {
                        idle = 0;
                    } else {
                        idle = idle.saturating_add(1);
                    }
                    if idle > 1000 {
                        let state = owner.state.lock().unwrap_or_else(|p| p.into_inner());
                        let _ = owner.changed.wait_timeout(
                            state,
                            Duration::from_micros(owner.config.idle_sleep_us.max(1)),
                        );
                    } else {
                        std::hint::spin_loop();
                    }
                }
                owner.stop_tasks();
                owner.counters.workers.fetch_sub(1, Ordering::Relaxed);
            })?;
        Ok(worker)
    }
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.changed.notify_all();
    }
    pub fn close(&self) -> Result<()> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.closed {
            return Ok(());
        }
        if let Some(e) = s.failure {
            return Err(unquiesced(e));
        }
        if s.mrs != 0 || s.slices != 0 || !s.posted.is_empty() {
            return Err(sys::uncertain(
                "libfabric close before physical drain",
                libc::EBUSY,
            ));
        }
        let close = s.native.api.calls.close;
        let mut failure = None;
        let h = &mut s.native.handles;
        for slot in [
            &mut h.ep,
            &mut h.av,
            &mut h.cq,
            &mut h.domain,
            &mut h.fabric,
        ] {
            if !slot.is_null() {
                // SAFETY: children (including MRs) have drained. Each nonnull
                // resource is destroyed once, stopping immediately on failure.
                let code = unsafe { close(*slot) };
                if code != 0 {
                    failure = Some(code);
                    break;
                }
                *slot = std::ptr::null_mut();
            }
        }
        if let Some(code) = failure {
            s.failure = Some(("fi_close", code));
            self.counters.quarantined.fetch_add(1, Ordering::Relaxed);
            return Err(sys::uncertain("fi_close", code));
        }
        self.counters
            .peers
            .fetch_sub(s.peers.len(), Ordering::Relaxed);
        s.peers.clear();
        s.closed = true;
        self.counters.domains.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }
    pub fn register(
        self: &Arc<Self>,
        loan: Arc<Loan>,
        offset: usize,
        length: usize,
        remote: i32,
    ) -> Result<Arc<Mr>> {
        crate::memory::checked_range(offset, length, loan.0.len())?;
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(e) = s.failure {
            return Err(unquiesced(e));
        }
        if self.stop.load(Ordering::Acquire) || s.closed {
            return Err(Error::Unavailable("fabric domain is closed"));
        }
        if s.mrs >= self.config.max_pending_ops {
            return Err(Error::ResourceExhausted);
        }
        let key = self
            .key
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::ResourceExhausted)?;
        let address = loan.address().as_ptr().wrapping_add(offset);
        let mut mr = std::ptr::null_mut();
        let mut desc = std::ptr::null_mut();
        let mut actual = 0;
        // SAFETY: the entire range is checked by Buffer and retained in loan.
        // Partial MR construction is returned to this owner for checked close.
        let code = unsafe {
            (s.native.api.calls.register)(
                &s.native.handles,
                address.cast(),
                length,
                remote,
                key,
                self.info.endpoint_mr,
                &mut mr,
                &mut desc,
                &mut actual,
            )
        };
        if mr.is_null() {
            sys::check("fi_mr_reg", code)?;
            return Err(Error::Protocol("fi_mr_reg returned no MR"));
        }
        s.mrs += 1;
        self.counters.mrs.fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes
            .fetch_add(length as u64, Ordering::Relaxed);
        drop(s);
        let owned = Arc::new(Mr {
            context: ManuallyDrop::new(self.clone()),
            address: address as usize,
            state: Mutex::new((mr, None, Some(loan))),
            desc: desc as usize,
            key: actual,
            off: offset,
            len: length,
        });
        if code != 0 {
            owned.close()?;
            return Err(Error::Backend {
                operation: "fi_mr_reg/bind/enable",
                code,
            });
        }
        Ok(owned)
    }
    pub fn peer(&self, peer: PeerKey, address: &[u8]) -> Result<u64> {
        if address.len() * 2 != self.nic.addr.len() {
            return Err(Error::Protocol(
                "fabric peer address size differs from provider",
            ));
        }
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(e) = s.failure {
            return Err(unquiesced(e));
        }
        if s.closed || self.stop.load(Ordering::Acquire) {
            return Err(Error::Unavailable("fabric domain is closed"));
        }
        let key = (peer, address.to_vec());
        if let Some(a) = s.peers.get(&key) {
            return a.failed.map_or(Ok(a.address), |e| Err(unquiesced(e)));
        }
        if s.peers.len() == self.config.max_peer_addresses {
            return Err(Error::ResourceExhausted);
        }
        let mut out = 0;
        // SAFETY: address was decoded and bounded from the matching provider's
        // getname result; the provider consumes it during fi_av_insert.
        let code =
            unsafe { (s.native.api.calls.insert)(&s.native.handles, address.as_ptr(), &mut out) };
        sys::check("fi_av_insert", code)?;
        s.peers.insert(
            key,
            Address {
                address: out,
                failed: None,
            },
        );
        self.counters.peers.fetch_add(1, Ordering::Relaxed);
        Ok(out)
    }
    pub fn close_peer(&self, peer: PeerKey) -> Result<()> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let keys: Vec<_> = s
            .peers
            .keys()
            .filter(|(p, _)| *p == peer)
            .cloned()
            .collect();
        for key in keys {
            let a = &s.peers[&key];
            if let Some(e) = a.failed {
                return Err(unquiesced(e));
            }
            // SAFETY: SegmentManager has fenced and drained every permit for
            // this endpoint/epoch; no queued or posted operation can use it.
            let code = unsafe { (s.native.api.calls.remove)(&s.native.handles, a.address) };
            if code != 0 {
                if let Some(a) = s.peers.get_mut(&key) {
                    a.failed = Some(("fi_av_remove", code));
                }
                s.failure = Some(("fi_av_remove", code));
                self.counters.quarantined.fetch_add(1, Ordering::Relaxed);
                return Err(sys::uncertain("fi_av_remove", code));
            }
            s.peers.remove(&key);
            self.counters.peers.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }
}
impl Drop for Context {
    fn drop(&mut self) {
        if self.close().is_ok() {
            let s = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
            // SAFETY: every native resource was successfully closed. Otherwise
            // ManuallyDrop retains the provider library and all native objects.
            unsafe { ManuallyDrop::drop(&mut s.native) };
        }
    }
}

type MrState = (*mut c_void, Option<Failure>, Option<Arc<Loan>>);
pub(super) struct Mr {
    context: ManuallyDrop<Arc<Context>>,
    address: usize,
    state: Mutex<MrState>,
    pub desc: usize,
    pub key: u64,
    pub off: usize,
    pub len: usize,
}
// SAFETY: a MR retains its exclusive range loan. Pointer use and close are
// serialized by the domain; no host slice is formed over provider-owned bytes.
unsafe impl Send for Mr {}
// SAFETY: sharing an MR shares its immutable range; only physically drained
// callers may close it, and the close state is protected by a mutex.
unsafe impl Sync for Mr {}
impl Mr {
    pub fn address(&self) -> usize {
        self.address
    }
    pub fn close(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(e) = state.1 {
            return Err(unquiesced(e));
        }
        if state.0.is_null() {
            return Ok(());
        }
        let mut s = self.context.state.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: the last provider references are terminal before the buffer
        // calls close. No retry is made after an uncertain destroy.
        let code = unsafe { (s.native.api.calls.close)(state.0) };
        if code != 0 {
            state.1 = Some(("fi_close MR", code));
            self.context
                .counters
                .quarantined
                .fetch_add(1, Ordering::Relaxed);
            return Err(sys::uncertain("fi_close MR", code));
        }
        state.0 = std::ptr::null_mut();
        s.mrs -= 1;
        self.context.counters.mrs.fetch_sub(1, Ordering::Relaxed);
        self.context
            .counters
            .bytes
            .fetch_sub(self.len as u64, Ordering::Relaxed);
        drop(s);
        drop(state.2.take());
        Ok(())
    }
}
impl Drop for Mr {
    fn drop(&mut self) {
        if self.close().is_ok() {
            // SAFETY: successful MR destruction follows physical completion.
            // On failure both the backing and the entire provider stay retained.
            unsafe {
                ManuallyDrop::drop(&mut self.context);
            }
        } else if let Some(loan) = self
            .state
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .2
            .take()
        {
            std::mem::forget(loan);
        }
    }
}

pub(super) struct Op {
    pub mr: Arc<Mr>,
    pub offset: usize,
    pub length: usize,
    pub peer: u64,
    pub remote: u64,
    pub key: u64,
    cookie: Box<[usize; 8]>,
    status: u8,
    overdue: bool,
}
impl Op {
    pub fn new(
        mr: Arc<Mr>,
        offset: usize,
        length: usize,
        peer: u64,
        remote: u64,
        key: u64,
    ) -> Result<Self> {
        crate::memory::checked_range(offset, length, mr.len)?;
        Ok(Self {
            mr,
            offset,
            length,
            peer,
            remote,
            key,
            cookie: Box::new([0; 8]),
            status: 0,
            overdue: false,
        })
    }
}
struct TaskState {
    ops: Vec<Op>,
    completed: usize,
    error: Option<Error>,
    posted: bool,
}
pub(super) struct Task {
    state: Mutex<TaskState>,
    changed: Condvar,
    operation: TransferOp,
    peer: PeerKey,
    until: Instant,
    posted_at: Instant,
    context: Arc<Context>,
}
impl Task {
    pub fn wait(&self, posted: &mut bool) -> Result<()> {
        let end = self
            .until
            .checked_add(self.context.config.quiesce())
            .ok_or(Error::InvalidRange)?;
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            *posted |= s.posted;
            if s.completed == s.ops.len() {
                return s.error.as_ref().map_or(Ok(()), |e| Err(e.for_report()));
            }
            #[cfg(test)]
            let expired = self.context.test_drain_expired.load(Ordering::Acquire);
            #[cfg(not(test))]
            let expired = false;
            if expired || Instant::now() >= end {
                self.context
                    .counters
                    .quarantined
                    .fetch_add(1, Ordering::Relaxed);
                return Err(sys::uncertain(
                    "libfabric completion drain",
                    libc::ETIMEDOUT,
                ));
            }
            let wait = end
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(10));
            s = self
                .changed
                .wait_timeout(s, wait)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }
}
impl Drop for Task {
    fn drop(&mut self) {
        let n = self
            .state
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .ops
            .len();
        let mut s = self.context.state.lock().unwrap_or_else(|p| p.into_inner());
        s.slices -= n;
        self.context.counters.slices.fetch_sub(n, Ordering::Relaxed);
    }
}
impl Context {
    pub fn prepare(
        self: &Arc<Self>,
        ops: Vec<Op>,
        operation: TransferOp,
        peer: PeerKey,
        until: Instant,
    ) -> Result<Arc<Task>> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(e) = s.failure {
            return Err(unquiesced(e));
        }
        if s.closed || self.stop.load(Ordering::Acquire) {
            return Err(Error::Unavailable("fabric domain is closed"));
        }
        if s.overdue.contains_key(&peer) {
            return Err(Error::Unavailable("fabric peer has overdue operations"));
        }
        if ops.is_empty() || ops.len() > self.config.max_pending_ops - s.slices {
            return Err(Error::ResourceExhausted);
        }
        s.slices += ops.len();
        self.counters.slices.fetch_add(ops.len(), Ordering::Relaxed);
        let task = Arc::new(Task {
            state: Mutex::new(TaskState {
                ops,
                completed: 0,
                error: None,
                posted: false,
            }),
            changed: Condvar::new(),
            operation,
            peer,
            until,
            posted_at: Instant::now(),
            context: self.clone(),
        });
        Ok(task)
    }
    pub fn commit(&self, task: Arc<Task>) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .push_back(task);
        self.changed.notify_all();
    }
    fn tick(&self) -> bool {
        // Completed task references must drop outside the context mutex: their
        // final MR/charge release acquires the same owner lock.
        let mut released = Vec::new();
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.failure.is_some() {
            return false;
        }
        let mut active = false;
        for _ in 0..64 {
            let mut c = sys::Completion::default();
            // SAFETY: this is the sole CQ consumer and all cookie allocations
            // remain in posted until their matching terminal CQE is consumed.
            let code = unsafe { (s.native.api.calls.poll)(&s.native.handles, &mut c) };
            if code == 1 {
                break;
            }
            if code != 0 {
                s.failure.get_or_insert(("fi_cq_read", code));
                break;
            }
            active = true;
            let Some((task, index)) = s.posted.remove(&(c.context as usize)) else {
                s.failure
                    .get_or_insert(("fi_cq_read unknown context", libc::EPROTO));
                break;
            };
            let mut t = task.state.lock().unwrap_or_else(|p| p.into_inner());
            let op = &mut t.ops[index];
            op.status = 2;
            if op.overdue {
                if let Some(n) = s.overdue.get_mut(&task.peer) {
                    *n -= 1;
                    if *n == 0 {
                        s.overdue.remove(&task.peer);
                    }
                }
            }
            self.counters.posted.fetch_sub(1, Ordering::Relaxed);
            if c.error != 0 {
                self.counters.errors.fetch_add(1, Ordering::Relaxed);
                t.error.get_or_insert(Error::Backend {
                    operation: "fi_cq_readerr",
                    code: c.error,
                });
            } else {
                let bytes = op.length as u64;
                if task.operation == TransferOp::Read {
                    self.counters.read.fetch_add(bytes, Ordering::Relaxed);
                } else {
                    self.counters.write.fetch_add(bytes, Ordering::Relaxed);
                }
            }
            t.completed += 1;
            drop(t);
            task.changed.notify_all();
            released.push(task);
        }
        let depth = self.info.tx_size.min(self.config.max_pending_ops).min(
            if self.config.max_posted_ops == 0 {
                usize::MAX
            } else {
                self.config.max_posted_ops
            },
        );
        let mut again = VecDeque::new();
        while let Some(task) = s.queue.pop_front() {
            let mut t = task.state.lock().unwrap_or_else(|p| p.into_inner());
            let now = Instant::now();
            if s.overdue.contains_key(&task.peer) && t.error.is_none() {
                self.counters
                    .overdue_rejections
                    .fetch_add(1, Ordering::Relaxed);
            }
            let reject = s
                .failure
                .map(unquiesced)
                .or_else(|| {
                    s.overdue
                        .contains_key(&task.peer)
                        .then_some(Error::Unavailable("fabric peer has overdue operations"))
                })
                .or_else(|| {
                    (now >= task.until
                        || now.duration_since(task.posted_at)
                            >= Duration::from_millis(self.config.post_timeout_ms))
                    .then_some(Error::Timeout)
                });
            if let Some(e) = reject {
                t.error.get_or_insert(e);
            }
            let mut queued = false;
            for i in 0..t.ops.len() {
                if t.ops[i].status != 0 {
                    continue;
                }
                if t.error.is_some() {
                    t.ops[i].status = 2;
                    t.completed += 1;
                    continue;
                }
                if s.posted.len() >= depth {
                    queued = true;
                    continue;
                }
                let op = &mut t.ops[i];
                let cookie = op.cookie.as_mut_ptr().cast::<c_void>();
                self.counters.posts.fetch_add(1, Ordering::Relaxed);
                // SAFETY: the MR owns a checked loan and cookie is stable until
                // the matching CQE; the provider may access them after return.
                let code = unsafe {
                    (s.native.api.calls.post)(
                        &s.native.handles,
                        i32::from(task.operation == TransferOp::Write),
                        (op.mr.address() + op.offset) as *mut c_void,
                        op.length,
                        op.mr.desc as *mut c_void,
                        op.peer,
                        op.remote,
                        op.key,
                        cookie,
                    )
                };
                if code == -libc::EAGAIN {
                    queued = true;
                    continue;
                }
                if code > 0 {
                    // An impossible return is not proof of rejection. Retain
                    // the context and cookie as potentially posted work.
                    op.status = 1;
                    t.posted = true;
                    s.posted.insert(cookie as usize, (task.clone(), i));
                    self.counters.posted.fetch_add(1, Ordering::Relaxed);
                    s.failure = Some(("fi_read/write invalid return", code));
                    t.error
                        .get_or_insert(sys::uncertain("fi_read/write invalid return", code));
                    continue;
                }
                if code != 0 {
                    op.status = 2;
                    t.completed += 1;
                    t.error.get_or_insert(Error::Backend {
                        operation: "fi_read/write",
                        code,
                    });
                    continue;
                }
                op.status = 1;
                t.posted = true;
                s.posted.insert(cookie as usize, (task.clone(), i));
                self.counters.posted.fetch_add(1, Ordering::Relaxed);
                active = true;
            }
            drop(t);
            task.changed.notify_all();
            if queued {
                again.push_back(task);
            } else {
                released.push(task);
            }
        }
        s.queue = again;
        let now = Instant::now();
        let mut overdue = Vec::new();
        for (task, index) in s.posted.values() {
            let mut t = task.state.lock().unwrap_or_else(|p| p.into_inner());
            let op = &mut t.ops[*index];
            if !op.overdue
                && now.duration_since(task.posted_at)
                    >= Duration::from_millis(self.config.op_timeout_ms)
            {
                op.overdue = true;
                overdue.push(task.peer);
            }
        }
        for peer in overdue {
            *s.overdue.entry(peer).or_default() += 1;
        }
        drop(s);
        drop(released);
        active
    }
    fn stop_tasks(&self) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let queued: Vec<_> = s.queue.drain(..).collect();
        for task in &queued {
            let mut t = task.state.lock().unwrap_or_else(|p| p.into_inner());
            for i in 0..t.ops.len() {
                if t.ops[i].status == 0 {
                    t.ops[i].status = 2;
                    t.completed += 1;
                }
            }
            t.error.get_or_insert(Error::RequesterClosed);
            task.changed.notify_all();
        }
        // Posted tasks intentionally form a retained cycle through their MRs and
        // this context. Stopping a worker is never a completion receipt.
        drop(s);
        drop(queued);
    }
}
