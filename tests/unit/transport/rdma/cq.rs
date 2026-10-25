use super::*;
use std::collections::{HashMap, VecDeque};
use std::ffi::c_int;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::{Duration, Instant};

use crate::TransferOp;
use crate::transport::rdma::tests::{
    FakeCq, FakeDevice, VERBS as BASE, events as event_log, runtime,
};
use crate::transport::rdma::{RdmaProgressMode, RdmaTransport};

struct Channel {
    read: Mutex<UnixStream>,
    write: Mutex<UnixStream>,
    pending: Mutex<VecDeque<usize>>,
    log: Arc<Mutex<Vec<&'static str>>>,
    mode: String,
    hold: AtomicBool,
}

struct Entry {
    cq: usize,
    channel: Arc<Channel>,
    armed: AtomicBool,
    retiring: AtomicBool,
    debt: (Mutex<usize>, std::sync::Condvar),
}

fn entries() -> &'static Mutex<HashMap<usize, Arc<Entry>>> {
    static ENTRIES: OnceLock<Mutex<HashMap<usize, Arc<Entry>>>> = OnceLock::new();
    ENTRIES.get_or_init(Default::default)
}

impl Entry {
    fn emit(&self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        *self.debt.0.lock().unwrap() += 1;
        self.channel.pending.lock().unwrap().push_back(self.cq);
        self.channel.write.lock().unwrap().write_all(&[1]).unwrap();
    }
}

fn poke_all() {
    for entry in entries().lock().unwrap().values() {
        if entry.retiring.load(Ordering::Acquire) {
            continue;
        }
        let cq = unsafe { &*(entry.cq as *const FakeCq) };
        if !cq.completions.lock().unwrap().is_empty() {
            entry.emit();
        }
    }
}

unsafe extern "C" fn create_channel(
    h: *mut sys::ChannelHandles,
    d: *const sys::DeviceHandles,
) -> c_int {
    let device = unsafe { &*(*d).context.cast::<FakeDevice>() };
    let (read, write) = UnixStream::pair().unwrap();
    read.set_nonblocking(true).unwrap();
    let fd = read.as_raw_fd();
    let channel = Arc::new(Channel {
        read: Mutex::new(read),
        write: Mutex::new(write),
        pending: Mutex::default(),
        log: Arc::clone(&device.events),
        mode: device.mode.clone(),
        hold: AtomicBool::new(device.mode == "completion_wait"),
    });
    channel
        .log
        .lock()
        .unwrap()
        .push("create_completion_channel");
    unsafe {
        (*h).channel = Arc::into_raw(channel).cast_mut().cast();
        (*h).fd = fd;
    }
    if device.mode == "completion_create_fail" {
        libc::EIO
    } else {
        0
    }
}

unsafe extern "C" fn create_cq(
    h: *mut sys::CqHandles,
    d: *const sys::DeviceHandles,
    capacity: u32,
    channel: *mut c_void,
) -> c_int {
    assert!(
        !channel.is_null(),
        "event mode must pass its real channel to both CQs"
    );
    let code = unsafe { (BASE.create_cq)(h, d, capacity, std::ptr::null_mut()) };
    if !unsafe { (*h).cq }.is_null() {
        unsafe {
            Arc::increment_strong_count(channel.cast::<Channel>());
        }
        let channel = unsafe { Arc::from_raw(channel.cast::<Channel>()) };
        let cq = unsafe { (*h).cq } as usize;
        entries().lock().unwrap().insert(
            cq,
            Arc::new(Entry {
                cq,
                channel,
                armed: AtomicBool::new(false),
                retiring: AtomicBool::new(false),
                debt: (Mutex::new(0), std::sync::Condvar::new()),
            }),
        );
    }
    code
}

unsafe extern "C" fn get(channel: *mut c_void, cq: *mut *mut c_void) -> c_int {
    let channel = unsafe { &*channel.cast::<Channel>() };
    let mut byte = [0];
    if let Err(error) = channel.read.lock().unwrap().read_exact(&mut byte) {
        return error.raw_os_error().unwrap_or(libc::EIO);
    }
    channel.log.lock().unwrap().push("get_cq_event");
    if channel.mode == "completion_get_error" {
        return libc::EIO;
    }
    let next = channel.pending.lock().unwrap().pop_front().unwrap();
    unsafe {
        *cq = next as *mut c_void;
    }
    0
}

unsafe extern "C" fn ack(cq: *mut c_void) {
    let entry = entries()
        .lock()
        .unwrap()
        .get(&(cq as usize))
        .cloned()
        .unwrap();
    entry.channel.log.lock().unwrap().push("ack_cq_event");
    let mut debt = entry.debt.0.lock().unwrap();
    assert!(*debt > 0, "every ACK must consume a successful native get");
    *debt -= 1;
    entry.debt.1.notify_all();
}

unsafe extern "C" fn arm(cq: *mut c_void) -> c_int {
    let entry = entries()
        .lock()
        .unwrap()
        .get(&(cq as usize))
        .cloned()
        .unwrap();
    entry.channel.log.lock().unwrap().push("arm_cq");
    if entry.channel.mode == "completion_arm_error" {
        return libc::EIO;
    }
    entry.armed.store(true, Ordering::Release);
    0
}

unsafe extern "C" fn close_channel(h: *mut sys::ChannelHandles) -> c_int {
    let channel = unsafe { &*(*h).channel.cast::<Channel>() };
    channel.log.lock().unwrap().push("close_completion_channel");
    if channel.mode == "completion_channel_close" {
        return libc::EBUSY;
    }
    assert!(
        !entries()
            .lock()
            .unwrap()
            .values()
            .any(|e| std::ptr::eq(Arc::as_ptr(&e.channel), channel))
    );
    assert!(channel.pending.lock().unwrap().is_empty());
    unsafe {
        drop(Arc::from_raw((*h).channel.cast::<Channel>()));
        (*h).channel = std::ptr::null_mut();
        (*h).fd = -1;
    }
    0
}

unsafe extern "C" fn destroy_cq(h: *mut sys::CqHandles) -> c_int {
    if unsafe { (*h).cq }.is_null() {
        return 0;
    }
    let key = unsafe { (*h).cq } as usize;
    let entry = {
        let map = entries().lock().unwrap();
        let entry = map.get(&key).cloned().unwrap();
        entry.retiring.store(true, Ordering::Release);
        entry
    };
    let count = match entry.channel.mode.as_str() {
        "completion_destroy_event" => 1,
        "completion_destroy_burst" => 8,
        _ => 0,
    };
    for _ in 0..count {
        entry.armed.store(true, Ordering::Release);
        entry.emit();
    }
    let (debt, timeout) = entry
        .debt
        .1
        .wait_timeout_while(
            entry.debt.0.lock().unwrap(),
            Duration::from_secs(2),
            |debt| *debt != 0,
        )
        .unwrap();
    if timeout.timed_out() || *debt != 0 {
        return libc::EBUSY;
    }
    drop(debt);
    let code = unsafe { (BASE.destroy_cq)(h) };
    if code == 0 {
        entries().lock().unwrap().remove(&key);
    }
    code
}

unsafe extern "C" fn post(
    h: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    wr: *const sys::WorkRequest,
    count: usize,
    local: u64,
    length: usize,
    remote: u64,
    rkey: u32,
    write: c_int,
    result: *mut sys::PostResult,
) -> c_int {
    let code = unsafe {
        (BASE.post)(
            h, memory, wr, count, local, length, remote, rkey, write, result,
        )
    };
    poke_all();
    code
}

unsafe extern "C" fn window(
    h: *mut sys::ConnectionHandles,
    memory: *mut sys::MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    permission: u32,
    bind: bool,
    result: *mut sys::PostResult,
) -> c_int {
    let code = unsafe { (BASE.window)(h, memory, id, address, length, permission, bind, result) };
    poke_all();
    code
}

unsafe extern "C" fn send(
    h: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    rkey: u32,
    result: *mut sys::PostResult,
) -> c_int {
    let code = unsafe { (BASE.send_completion)(h, memory, id, address, length, rkey, result) };
    poke_all();
    code
}

unsafe extern "C" fn poll(
    h: *mut sys::ConnectionHandles,
    completion: *mut sys::Completion,
) -> c_int {
    let entry = entries()
        .lock()
        .unwrap()
        .get(&(unsafe { (*h).cq } as usize))
        .cloned()
        .unwrap();
    if entry.channel.hold.load(Ordering::Acquire) {
        return 0;
    }
    unsafe { (BASE.poll)(h, completion) }
}

pub(in crate::transport::rdma) static VERBS: sys::Verbs = sys::Verbs {
    completion: Some(sys::CqEventVerbs {
        create: create_channel,
        get,
        ack,
        arm,
        close: close_channel,
    }),
    create_cq,
    destroy_cq,
    post,
    window,
    send_completion: send,
    poll,
    ..BASE
};

pub(crate) fn event_runtime(mode: &str) -> RdmaTransport {
    let mut runtime = runtime(mode);
    runtime.config.progress_mode = RdmaProgressMode::EventDriven;
    runtime.config.block_size = 2;
    runtime.config.max_send_wr = 1;
    runtime.verbs = &VERBS;
    runtime.registrations = super::super::buffers::LocalBufferManager::new(
        &runtime.config,
        Arc::clone(&runtime.usage),
        &VERBS,
    );
    runtime
}

fn events(runtime: &RdmaTransport) -> Vec<&'static str> {
    event_log(runtime).lock().unwrap().clone()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let end = deadline();
    while !predicate() {
        assert!(Instant::now() < end, "completion event control timed out");
        thread::yield_now();
    }
}

#[test]
fn polling_default_does_not_require_completion_event_verbs() {
    assert_eq!(
        super::super::RdmaConfig::default().progress_mode,
        RdmaProgressMode::Polling
    );
    let mut runtime = runtime("ok");
    runtime.config.progress_mode = RdmaProgressMode::EventDriven;
    assert!(matches!(
        runtime.staging_endpoint(8, 0),
        Err(Error::Unavailable(_))
    ));
    runtime.close(deadline()).unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_cqs,
            stats.active_completion_channels,
            stats.active_devices
        ),
        (0, 0, 0)
    );
}

#[test]
fn event_wakes_the_existing_worker_and_both_cqs_close_before_the_channel() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let runtime = event_runtime("completion_wait");
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        endpoint.connect(&endpoint.descriptor()).unwrap();
        let cq = endpoint
            .record
            .lock()
            .unwrap()
            .resources
            .as_ref()
            .unwrap()
            .connection
            .lock()
            .unwrap()
            .handles
            .cq as usize;
        let entry = entries().lock().unwrap().get(&cq).cloned().unwrap();
        thread::scope(|scope| {
            let work =
                scope.spawn(|| endpoint.transfer(&endpoint.descriptor(), operation, deadline()));
            wait_until(|| {
                runtime.stats().unwrap().cq_event_waits != 0 && entry.armed.load(Ordering::Acquire)
            });
            assert_eq!(
                runtime.stats().unwrap().completed_reads
                    + runtime.stats().unwrap().completed_writes,
                0
            );
            entry.channel.hold.store(false, Ordering::Release);
            entry.emit();
            work.join().unwrap().unwrap();
        });
        let log = event_log(&runtime);
        endpoint.close().unwrap();
        drop(endpoint);
        runtime.close(deadline()).unwrap();
        let stats = runtime.stats().unwrap();
        assert_eq!((stats.opened_cqs, stats.opened_completion_channels), (2, 1));
        assert!(
            stats.completion_events > 0
                && stats.cq_notification_arms >= 2
                && stats.cq_event_waits > 0
        );
        assert_eq!(
            (
                stats.active_cqs,
                stats.active_completion_channels,
                stats.completion_event_sources,
                stats.progress_workers,
                stats.active_devices
            ),
            (0, 0, 0, 0, 0)
        );
        let log = log.lock().unwrap();
        assert_eq!(
            log.iter().filter(|e| **e == "get_cq_event").count(),
            log.iter().filter(|e| **e == "ack_cq_event").count()
        );
        assert!(
            log.iter().rposition(|e| *e == "destroy_cq").unwrap()
                < log
                    .iter()
                    .position(|e| *e == "close_completion_channel")
                    .unwrap()
        );
    }
}

#[test]
fn partial_channel_and_cq_creation_release_every_proven_owner() {
    for mode in ["completion_create_fail", "cq_create", "receive_cq_create"] {
        let runtime = event_runtime(mode);
        assert!(matches!(
            runtime.staging_endpoint(8, 0),
            Err(Error::Backend { .. })
        ));
        let log = event_log(&runtime);
        runtime.close(deadline()).unwrap();
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_cqs,
                stats.active_completion_channels,
                stats.completion_event_sources,
                stats.active_devices
            ),
            (0, 0, 0, 0),
            "{mode}"
        );
        assert_eq!(
            log.lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "close_completion_channel")
                .count(),
            1
        );
    }
}

#[test]
fn arm_failure_uses_physical_lane_retirement_without_polling_fallback() {
    let runtime = event_runtime("completion_arm_error");
    let endpoint = runtime.staging_endpoint(8, 0);
    wait_until(|| runtime.stats().unwrap().progress_failures != 0);
    if let Ok(mut endpoint) = endpoint {
        assert!(endpoint.connect(&endpoint.descriptor()).is_err());
        let _ = endpoint.close();
    }
    let log = event_log(&runtime);
    let _ = runtime.close(deadline());
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_cqs,
            stats.active_completion_channels,
            stats.completion_event_sources,
            stats.active_devices
        ),
        (0, 0, 0, 0)
    );
    assert!(log.lock().unwrap().contains(&"arm_cq"));
}

#[test]
fn lost_event_consumer_retains_the_actual_lane_and_never_fabricates_an_ack() {
    let runtime = event_runtime("completion_get_error");
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    endpoint.connect(&endpoint.descriptor()).unwrap();
    let _ = endpoint.transfer(&endpoint.descriptor(), TransferOp::Read, deadline());
    wait_until(|| runtime.stats().unwrap().completion_event_sources == 0);
    assert!(matches!(
        runtime.close(deadline()),
        Err(Error::Unquiesced { .. })
    ));
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_cqs,
            stats.retained_cqs,
            stats.active_completion_channels,
            stats.retained_completion_channels,
            stats.active_devices
        ),
        (2, 2, 1, 1, 1)
    );
    assert_eq!(stats.completion_events, 0);
    assert!(!events(&runtime).iter().any(|e| matches!(
        *e,
        "ack_cq_event" | "destroy_qp" | "destroy_cq" | "close_completion_channel"
    )));
}

#[test]
fn channel_close_failure_keeps_its_context_and_is_not_retried() {
    let runtime = event_runtime("completion_channel_close");
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    endpoint.close().unwrap();
    drop(endpoint);
    for _ in 0..2 {
        assert!(matches!(
            runtime.close(deadline()),
            Err(Error::Unquiesced {
                operation: "ibv_destroy_comp_channel",
                ..
            })
        ));
    }
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_cqs,
            stats.active_completion_channels,
            stats.retained_completion_channels,
            stats.completion_event_sources,
            stats.active_devices
        ),
        (0, 1, 1, 0, 1)
    );
    assert_eq!(
        events(&runtime)
            .iter()
            .filter(|e| **e == "close_completion_channel")
            .count(),
        1
    );
}

#[test]
fn event_wait_honors_transfer_deadline_and_physical_retirement() {
    let runtime = event_runtime("timeout");
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    endpoint.connect(&endpoint.descriptor()).unwrap();
    assert!(matches!(
        endpoint.transfer(
            &endpoint.descriptor(),
            TransferOp::Read,
            Instant::now() + Duration::from_millis(20)
        ),
        Err(Error::Timeout)
    ));
    let _ = endpoint.close();
    drop(endpoint);
    let _ = runtime.close(deadline());
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_endpoints,
            stats.active_cqs,
            stats.active_completion_channels,
            stats.completion_event_sources,
            stats.active_devices,
            stats.work_requests
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert!(stats.cq_event_waits > 0);
}

#[test]
fn failed_partial_cq_cleanup_retains_channel_children_and_blocks_reconstruction() {
    for (mode, count) in [("cq_create_close2", 1), ("receive_cq_create_close2", 2)] {
        let runtime = event_runtime(mode);
        assert!(matches!(
            runtime.staging_endpoint(8, 0),
            Err(Error::Unquiesced { .. })
        ));
        let log = event_log(&runtime);
        assert!(runtime.staging_endpoint(8, 0).is_err());
        assert!(matches!(
            runtime.close(deadline()),
            Err(Error::Unquiesced { .. })
        ));
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_cqs,
                stats.retained_cqs,
                stats.active_completion_channels,
                stats.retained_completion_channels,
                stats.active_devices
            ),
            (count, count, 1, 1, 1),
            "{mode}"
        );
        let log = log.lock().unwrap();
        assert_eq!(
            log.iter()
                .filter(|e| **e == "create_completion_channel")
                .count(),
            1
        );
        assert!(!log.contains(&"close_completion_channel"));
    }
}
