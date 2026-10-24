#![allow(unsafe_code)]

use crate::TransferOp;

use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub(crate) use super::context::tests::{events, pause};
use super::*;

pub(crate) struct FakeDevice {
    pub(super) mode: String,
    pub(super) route: Mutex<Option<discovery::RdmaAddress>>,
    asynchronous: Option<(
        Mutex<std::os::unix::net::UnixStream>,
        Mutex<std::os::unix::net::UnixStream>,
    )>,
    async_acks: (Mutex<usize>, Condvar),
    pub(crate) events: Arc<Mutex<Vec<&'static str>>>,
    endpoints: AtomicUsize,
    mrs: AtomicUsize,
    windows: AtomicUsize,
    cqs: AtomicUsize,
    next_qpn: AtomicU32,
    pub(crate) pause: Arc<(Mutex<(bool, bool)>, Condvar)>,
}

struct FakeCq {
    device: *const FakeDevice,
    completions: Mutex<std::collections::VecDeque<sys::Completion>>,
    qps: AtomicUsize,
    retiring: AtomicBool,
    ordinal: usize,
    owners: Mutex<std::collections::HashMap<u32, usize>>,
}

struct Fake {
    device: *const FakeDevice,
    cq: *const FakeCq,
    receive_cq: *const FakeCq,
    messages: Mutex<Vec<*mut FakeMr>>,
    receive: Mutex<Option<(u64, u64, usize)>>,
    length: AtomicUsize,
    bytes: AtomicU64,
    qpn: u32,
    first_id: AtomicU64,
    send_id: AtomicU64,
    remote_address: AtomicU64,
    window_key: AtomicU32,
    window: AtomicUsize,
    mr: AtomicUsize,
}

struct FakeReceive {
    qp: usize,
    cq: usize,
    address: u64,
    length: usize,
    id: u64,
    qpn: u32,
    window: usize,
}

fn receives() -> &'static Mutex<std::collections::HashMap<(u64, u32), FakeReceive>> {
    static RECEIVES: std::sync::OnceLock<
        Mutex<std::collections::HashMap<(u64, u32), FakeReceive>>,
    > = std::sync::OnceLock::new();
    RECEIVES.get_or_init(Default::default)
}

struct FakeMr {
    device: *const FakeDevice,
    address: usize,
    length: usize,
    local_only: bool,
    window_bind: bool,
    qps: AtomicUsize,
    windows: AtomicUsize,
}

struct FakeWindow {
    bound: AtomicBool,
    qp_alive: AtomicBool,
    qp: AtomicUsize,
    received: AtomicBool,
}

fn block(device: &FakeDevice) {
    let (lock, changed) = &*device.pause;
    let mut state = lock.lock().unwrap();
    state.0 = true;
    changed.notify_all();
    while !state.1 {
        state = changed.wait(state).unwrap();
    }
}

// CPU ABI controls, not a device emulator or proof that DMA completed.
pub(super) unsafe extern "C" fn open_device(
    h: *mut sys::DeviceHandles,
    name: *const c_char,
) -> c_int {
    let mode = unsafe { CStr::from_ptr(name) }.to_str().unwrap().to_owned();
    if mode == "device_missing" {
        return libc::ENODEV;
    }
    let asynchronous = mode.starts_with("async_").then(|| {
        let (read, write) = std::os::unix::net::UnixStream::pair().unwrap();
        read.set_nonblocking(true).unwrap();
        (Mutex::new(read), Mutex::new(write))
    });
    let state = Box::into_raw(Box::new(FakeDevice {
        mode,
        route: Mutex::default(),
        asynchronous,
        async_acks: (Mutex::new(0), Condvar::new()),
        events: Arc::new(Mutex::new(vec!["open_device"])),
        endpoints: AtomicUsize::new(0),
        mrs: AtomicUsize::new(0),
        windows: AtomicUsize::new(0),
        cqs: AtomicUsize::new(0),
        next_qpn: AtomicU32::new(9),
        pause: Arc::default(),
    }));
    unsafe {
        (*h).context = state.cast();
        if let Some((read, _)) = &(*state).asynchronous {
            use std::os::fd::AsRawFd;
            (*h).async_fd = read.lock().unwrap().as_raw_fd();
        }
        if (*state).mode.starts_with("device_open") {
            return libc::EIO;
        }
        (*state).events.lock().unwrap().push("alloc_pd");
        (*h).pd = std::ptr::dangling_mut::<u8>().cast();
        if (*state).mode == "device_pd_close" {
            return libc::EIO;
        }
    }
    0
}

unsafe extern "C" fn create_cq(
    h: *mut sys::ConnectionHandles,
    handles: *const sys::DeviceHandles,
    _: u32,
) -> c_int {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    device.events.lock().unwrap().push("create_cq");
    let ordinal = device.cqs.fetch_add(1, Ordering::SeqCst) + 1;
    unsafe {
        (*h).cq = Box::into_raw(Box::new(FakeCq {
            device,
            completions: Mutex::default(),
            qps: AtomicUsize::new(0),
            retiring: AtomicBool::new(false),
            ordinal,
            owners: Mutex::default(),
        }))
        .cast();
    }
    if device.mode.starts_with("cq_create")
        || (device.mode.starts_with("receive_cq_create") && ordinal == 2)
    {
        libc::EIO
    } else {
        0
    }
}

unsafe extern "C" fn register_memory(
    h: *mut sys::MemoryHandles,
    handles: *const sys::DeviceHandles,
    address: *mut c_void,
    length: usize,
    window_bind: bool,
) -> c_int {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    device.events.lock().unwrap().push(if window_bind {
        "register_parent"
    } else {
        "register_local"
    });
    device.mrs.fetch_add(1, Ordering::SeqCst);
    unsafe {
        (*h).mr = Box::into_raw(Box::new(FakeMr {
            device,
            address: address as usize,
            length,
            local_only: true,
            window_bind,
            qps: AtomicUsize::new(0),
            windows: AtomicUsize::new(0),
        }))
        .cast();
    }
    if device.mode == "register_pause" {
        block(device);
    }
    if device.mode.starts_with("register_fail")
        || (device.mode.starts_with("message_register_fail") && length == COMPLETION_MESSAGE_SIZE)
    {
        libc::EIO
    } else {
        0
    }
}

unsafe extern "C" fn prepare_grant(
    h: *mut sys::ConnectionHandles,
    memory: *mut sys::MemoryHandles,
    handles: *const sys::DeviceHandles,
    bytes: *mut c_void,
    length: usize,
    permission: u32,
) -> c_int {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    let qp = unsafe { &*(*h).qp.cast::<Fake>() };
    assert_eq!(
        qp.mr.load(Ordering::Acquire),
        0,
        "previous grant must detach all MR dependencies"
    );
    assert!(qp.messages.lock().unwrap().is_empty());
    assert_eq!(
        qp.window.load(Ordering::Acquire),
        0,
        "no old MW in reused QP"
    );
    unsafe {
        if (*memory).mr.is_null() {
            device.mrs.fetch_add(1, Ordering::SeqCst);
            (*memory).mr = Box::into_raw(Box::new(FakeMr {
                device,
                address: bytes as usize,
                length,
                local_only: false,
                window_bind: permission != 0,
                qps: AtomicUsize::new(0),
                windows: AtomicUsize::new(0),
            }))
            .cast();
        } else {
            let mr = &*(*memory).mr.cast::<FakeMr>();
            assert_eq!(mr.window_bind, permission != 0);
            assert!(
                bytes as usize >= mr.address && bytes as usize + length <= mr.address + mr.length
            );
        }
        (*(*memory).mr.cast::<FakeMr>())
            .qps
            .fetch_add(1, Ordering::SeqCst);
        qp.mr.store((*memory).mr as usize, Ordering::Release);
        qp.bytes.store(bytes as u64, Ordering::Release);
        qp.length.store(length, Ordering::Release);
        qp.first_id.store(0, Ordering::Release);
        qp.send_id.store(0, Ordering::Release);
        if permission != 0 && !device.mode.starts_with("open") {
            device.events.lock().unwrap().push("alloc_window");
            if device.mode == "window_unsupported" {
                return libc::EOPNOTSUPP;
            }
            device.windows.fetch_add(1, Ordering::SeqCst);
            (*memory).mw = Box::into_raw(Box::new(FakeWindow {
                bound: AtomicBool::new(false),
                qp_alive: AtomicBool::new(true),
                qp: AtomicUsize::new(qp as *const Fake as usize),
                received: AtomicBool::new(false),
            }))
            .cast();
            qp.window.store((*memory).mw as usize, Ordering::Release);
            (*(*memory).mr.cast::<FakeMr>())
                .windows
                .fetch_add(1, Ordering::SeqCst);
            (*memory).window_key = if device.mode == "key_indices" {
                (qp.qpn << 8) | 1
            } else {
                0x101
            };
        }
    }
    0
}

unsafe extern "C" fn open(
    h: *mut sys::ConnectionHandles,
    memory: *mut sys::MemoryHandles,
    handles: *const sys::DeviceHandles,
    _: u8,
    _: u8,
    _: *const Descriptor,
    bytes: *mut c_void,
    length: usize,
    permission: u32,
    psn: u32,
    _: u32,
    descriptor: *mut Descriptor,
) -> c_int {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    let cq = unsafe { &*(*h).cq.cast::<FakeCq>() };
    assert_eq!(cq.device, device as *const FakeDevice);
    device.events.lock().unwrap().push("open_endpoint");
    if device.mode == "route_second_query_error" {
        return libc::ESTALE;
    }
    if cq.retiring.load(Ordering::Acquire) {
        return libc::EIO;
    }
    device.endpoints.fetch_add(1, Ordering::SeqCst);
    cq.qps.fetch_add(1, Ordering::SeqCst);
    let receive_cq = unsafe { &*(*h).receive_cq.cast::<FakeCq>() };
    receive_cq.qps.fetch_add(1, Ordering::SeqCst);
    let qpn = device.next_qpn.fetch_add(1, Ordering::SeqCst);
    let qp = Box::into_raw(Box::new(Fake {
        device,
        cq,
        receive_cq,
        messages: Mutex::default(),
        receive: Mutex::default(),
        length: AtomicUsize::new(0),
        bytes: AtomicU64::new(0),
        qpn,
        first_id: AtomicU64::new(0),
        send_id: AtomicU64::new(0),
        remote_address: AtomicU64::new(0),
        window_key: AtomicU32::new(0),
        window: AtomicUsize::new(0),
        mr: AtomicUsize::new(0),
    }));
    unsafe {
        (*h).qp = qp.cast();
    }
    cq.owners.lock().unwrap().insert(qpn, qp as usize);
    receive_cq.owners.lock().unwrap().insert(qpn, qp as usize);
    let code = unsafe { prepare_grant(h, memory, handles, bytes, length, permission) };
    if code != 0 {
        return code;
    }
    unsafe {
        *descriptor = Descriptor {
            address: bytes as u64,
            rkey: if permission == 0 {
                7
            } else {
                (*memory).window_key
            },
            qpn,
            psn,
            lid: 1,
            mtu: 3,
            link_layer: 1,
            gid: [0; 16],
        };
    }
    if device.mode.starts_with("open") {
        5
    } else {
        0
    }
}

fn detach_grant(qp: &Fake) {
    let mr = qp.mr.swap(0, Ordering::AcqRel);
    if mr != 0 {
        unsafe { &*(mr as *const FakeMr) }
            .qps
            .fetch_sub(1, Ordering::SeqCst);
    }
    for mr in qp.messages.lock().unwrap().drain(..) {
        unsafe { &*mr }.qps.fetch_sub(1, Ordering::SeqCst);
    }
}

unsafe extern "C" fn window(
    handles: *mut sys::ConnectionHandles,
    memory: *mut sys::MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    permission: u32,
    bind: bool,
    result: *mut sys::PostResult,
) -> c_int {
    let h = unsafe { &mut *handles };
    let qp = unsafe { &*h.qp.cast::<Fake>() };
    let device = unsafe { &*qp.device };
    let cq = unsafe { &*qp.cq };
    let state = unsafe { &(*(*memory).mw.cast::<FakeWindow>()).bound };
    qp.window_key
        .store(unsafe { (*memory).window_key }, Ordering::Release);
    qp.window
        .store(unsafe { (*memory).mw } as usize, Ordering::Release);
    assert_eq!(address, qp.bytes.load(Ordering::Acquire));
    assert_eq!(length, qp.length.load(Ordering::Acquire));
    assert!(matches!(permission, 1 | 2));
    assert_ne!(state.load(Ordering::Acquire), bind);
    device.events.lock().unwrap().push(if bind {
        "bind_window"
    } else {
        "invalidate_window"
    });
    let fault = device
        .mode
        .strip_suffix("_close0")
        .unwrap_or(device.mode.as_str());
    let result = unsafe { &mut *result };
    *result = sys::PostResult::default();
    if (bind && fault == "window_bind_post") || (!bind && fault == "window_inv_post") {
        return libc::ENOMEM;
    }
    if fault == "window_unknown_post" || (!bind && fault == "window_inv_unknown") {
        state.store(bind, Ordering::Release);
        result.uncertain = true;
        return libc::EIO;
    }
    state.store(bind, Ordering::Release);
    result.accepted = 1;
    if (bind && fault == "window_bind_timeout") || (!bind && fault == "window_inv_timeout") {
        return 0;
    }
    if (bind && fault == "window_bind_pause") || (!bind && fault == "window_inv_pause") {
        block(device);
    }
    cq.completions.lock().unwrap().push_back(sys::Completion {
        id,
        qpn: qp.qpn,
        opcode: if bind { 5 } else { 6 },
        status: u32::from(
            (bind && fault == "window_bind_error") || (!bind && fault == "window_inv_error"),
        ),
        ..Default::default()
    });
    0
}

unsafe extern "C" fn dealloc_window(handles: *mut sys::MemoryHandles) -> c_int {
    let h = unsafe { &mut *handles };
    if h.mw.is_null() {
        return 0;
    }
    let mr = unsafe { &*h.mr.cast::<FakeMr>() };
    let window = unsafe { &*h.mw.cast::<FakeWindow>() };
    assert!(
        !window.qp_alive.load(Ordering::Acquire)
            || (!window.bound.load(Ordering::Acquire) && window.received.load(Ordering::Acquire)),
        "MW cleanup requires creating QP close or actual receive/invalidation"
    );
    let device = unsafe { &*mr.device };
    device.events.lock().unwrap().push("dealloc_window");
    if device.mode == "window_close" {
        return libc::EIO;
    }
    if window.qp_alive.load(Ordering::Acquire) {
        let qp = unsafe { &*(window.qp.load(Ordering::Acquire) as *const Fake) };
        assert_eq!(qp.mr.load(Ordering::Acquire), 0);
        assert!(
            qp.window
                .compare_exchange(h.mw as usize, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        );
    }
    unsafe {
        drop(Box::from_raw(h.mw.cast::<FakeWindow>()));
    }
    h.mw = std::ptr::null_mut();
    device.windows.fetch_sub(1, Ordering::SeqCst);
    mr.windows.fetch_sub(1, Ordering::SeqCst);
    0
}

unsafe extern "C" fn connect(
    h: *mut sys::ConnectionHandles,
    _: *const Descriptor,
    _: *const Descriptor,
    _: u8,
    _: u8,
    _: u8,
    _: u8,
) -> c_int {
    let state = unsafe { &*(*h).qp.cast::<Fake>() };
    let device = unsafe { &*state.device };
    device.events.lock().unwrap().push("connect");
    i32::from(device.mode == "connect")
}

unsafe extern "C" fn post(
    h: *mut sys::ConnectionHandles,
    _: *const sys::MemoryHandles,
    requests: *const sys::WorkRequest,
    count: usize,
    local: u64,
    local_length: usize,
    remote: u64,
    _: u32,
    write: c_int,
    result: *mut sys::PostResult,
) -> c_int {
    let state = unsafe { &*(*h).qp.cast::<Fake>() };
    let device = unsafe { &*state.device };
    let cq = unsafe { &*state.cq };
    let mode = device.mode.as_str();
    assert_eq!(local, state.bytes.load(Ordering::Acquire));
    assert_eq!(local_length, state.length.load(Ordering::Acquire));
    state.remote_address.store(remote, Ordering::Release);
    let requests = unsafe { std::slice::from_raw_parts(requests, count) };
    if state.first_id.load(Ordering::Acquire) == 0 {
        state.first_id.store(requests[0].id, Ordering::Release);
    }
    let partial = mode.starts_with("partial");
    let uncertain =
        (mode.contains("unknown") && !mode.starts_with("send_completion")) || mode == "copy_post";
    let accepted = if partial {
        count.min(1)
    } else if mode.starts_with("post") {
        0
    } else {
        count
    };
    for request in &requests[..accepted] {
        assert!(
            request.offset as usize + request.length as usize
                <= state.length.load(Ordering::Acquire)
        );
        if mode.contains("copy") {
            // CPU mutation under production leases, not a DMA receipt.
            unsafe {
                let local =
                    (state.bytes.load(Ordering::Acquire) as *mut u8).add(request.offset as usize);
                let remote = (remote as usize as *mut u8).add(request.offset as usize);
                if write != 0 {
                    std::ptr::copy(local, remote, request.length as usize);
                } else {
                    std::ptr::copy(remote, local, request.length as usize);
                }
            }
        }
        cq.completions.lock().unwrap().push_back(sys::Completion {
            id: if mode == "identity" {
                u64::MAX
            } else if mode == "duplicate" {
                state.first_id.load(Ordering::Acquire)
            } else {
                request.id
            },
            qpn: state.qpn + u32::from(mode == "wrong_qpn"),
            opcode: if write == 1 { 1 } else { 2 },
            length: request.length - u32::from(mode == "length"),
            status: u32::from(mode == "wc" || mode == "wc_close0"),
            vendor_error: 0,
            ..Default::default()
        });
    }
    if mode == "pause_copy" || mode.starts_with("pause_post") {
        block(device);
    }
    unsafe {
        *result = sys::PostResult {
            accepted: if uncertain { 0 } else { accepted },
            uncertain,
        };
    }
    if partial || uncertain || mode.starts_with("post") {
        libc::ENOMEM
    } else {
        0
    }
}

unsafe extern "C" fn post_receive(
    handles: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    result: *mut sys::PostResult,
) -> c_int {
    let qp = unsafe { &*(*handles).qp.cast::<Fake>() };
    let device = unsafe { &*qp.device };
    device.events.lock().unwrap().push("post_receive");
    assert_eq!(length, COMPLETION_MESSAGE_SIZE);
    assert_ne!(
        unsafe { (*memory).mr },
        qp.mr.load(Ordering::Acquire) as *mut c_void
    );
    unsafe {
        *result = sys::PostResult::default();
    }
    if device.mode.starts_with("receive_post_fail") {
        return libc::ENOMEM;
    }
    let mr = unsafe { (*memory).mr.cast::<FakeMr>() };
    unsafe { &*mr }.qps.fetch_add(1, Ordering::SeqCst);
    qp.messages.lock().unwrap().push(mr);
    *qp.receive.lock().unwrap() = Some((id, address, length));
    receives().lock().unwrap().insert(
        (
            qp.bytes.load(Ordering::Acquire),
            qp.window_key.load(Ordering::Acquire),
        ),
        FakeReceive {
            qp: qp as *const Fake as usize,
            cq: qp.receive_cq as usize,
            address,
            length,
            id,
            qpn: qp.qpn,
            window: qp.window.load(Ordering::Acquire),
        },
    );
    if device.mode.contains("receive_unknown") {
        unsafe {
            (*result).uncertain = true;
        }
        return libc::EIO;
    }
    unsafe {
        (*result).accepted = 1;
    }
    0
}

unsafe extern "C" fn send_completion(
    handles: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    rkey: u32,
    result: *mut sys::PostResult,
) -> c_int {
    let qp = unsafe { &*(*handles).qp.cast::<Fake>() };
    let device = unsafe { &*qp.device };
    device.events.lock().unwrap().push("send_completion");
    assert_eq!(length, COMPLETION_MESSAGE_SIZE);
    assert_ne!(
        unsafe { (*memory).mr },
        qp.mr.load(Ordering::Acquire) as *mut c_void
    );
    let mr = unsafe { (*memory).mr.cast::<FakeMr>() };
    assert_eq!(unsafe { &*mr }.address, address as usize);
    unsafe {
        *result = sys::PostResult::default();
    }
    if device.mode == "send_completion_post" {
        return libc::ENOMEM;
    }
    qp.send_id.store(id, Ordering::Release);
    unsafe { &*mr }.qps.fetch_add(1, Ordering::SeqCst);
    qp.messages.lock().unwrap().push(mr);
    if device.mode.contains("send_completion_unknown") {
        unsafe {
            (*result).uncertain = true;
        }
        return libc::EIO;
    }
    unsafe {
        (*result).accepted = 1;
    }
    if let Some(receive) = receives()
        .lock()
        .unwrap()
        .remove(&(qp.remote_address.load(Ordering::Acquire), rkey))
    {
        assert_eq!(receive.length, length);
        // Explicit CPU provider control, not evidence of RNIC DMA or ordering.
        unsafe {
            std::ptr::copy_nonoverlapping(address as *const u8, receive.address as *mut u8, length);
            (*(receive.window as *mut FakeWindow))
                .bound
                .store(false, Ordering::Release);
            let cq = &*(receive.cq as *const FakeCq);
            cq.completions.lock().unwrap().push_back(sys::Completion {
                id: receive.id,
                qpn: receive.qpn,
                opcode: 128,
                length: length as u32,
                flags: sys::WITH_INVALIDATE,
                invalidated_rkey: rkey,
                ..Default::default()
            });
        }
    }
    unsafe { &*qp.cq }
        .completions
        .lock()
        .unwrap()
        .push_back(sys::Completion {
            id,
            qpn: qp.qpn,
            opcode: 0,
            ..Default::default()
        });
    0
}

unsafe extern "C" fn poll(
    h: *mut sys::ConnectionHandles,
    completion: *mut sys::Completion,
) -> c_int {
    let state = unsafe { &*(*h).cq.cast::<FakeCq>() };
    let device = unsafe { &*state.device };
    if device.mode.starts_with("timeout") {
        return 0;
    }
    if device.mode == "poll" {
        return -1;
    }
    let mut queue = state.completions.lock().unwrap();
    let value = if device.mode.contains("reverse") {
        queue.pop_back()
    } else {
        queue.pop_front()
    };
    if let Some(value) = value {
        if value.status == 0 && matches!(value.opcode, 0 | 128) {
            if let Some(owner) = state.owners.lock().unwrap().get(&value.qpn).copied() {
                let qp = unsafe { &*(owner as *const Fake) };
                let exact = if value.opcode == 0 {
                    qp.send_id.load(Ordering::Acquire) == value.id
                } else {
                    qp.receive.lock().unwrap().is_some_and(|r| r.0 == value.id)
                };
                if exact {
                    if value.opcode == 128 {
                        let window = qp.window.load(Ordering::Acquire);
                        if window != 0 {
                            unsafe { &*(window as *const FakeWindow) }
                                .received
                                .store(true, Ordering::Release);
                        }
                    }
                    detach_grant(qp);
                }
            }
        }
        unsafe {
            *completion = value;
        }
        1
    } else {
        0
    }
}

unsafe extern "C" fn qp(h: *mut sys::ConnectionHandles) -> c_int {
    let handles = unsafe { &mut *h };
    if handles.qp.is_null() {
        return 0;
    }
    let state = unsafe { &*handles.qp.cast::<Fake>() };
    let device = unsafe { &*state.device };
    device.events.lock().unwrap().push("destroy_qp");
    if device.mode == "async_destroy" {
        device.emit_async(3);
        let (acks, changed) = &device.async_acks;
        let (acks, _) = changed
            .wait_timeout_while(acks.lock().unwrap(), Duration::from_secs(2), |n| *n == 0)
            .unwrap();
        if *acks != 1 {
            return libc::ETIMEDOUT;
        }
        device
            .events
            .lock()
            .unwrap()
            .push("destroy_after_async_ack");
    }
    if device.mode.contains("qp_pause") {
        block(device);
    }
    if device.mode.ends_with("close0") {
        return 16;
    }
    receives()
        .lock()
        .unwrap()
        .retain(|_, receive| receive.qp != state as *const Fake as usize);
    let cq = unsafe { &*state.cq };
    let window = state.window.load(Ordering::Acquire);
    if window != 0 {
        unsafe { &*(window as *const FakeWindow) }
            .qp_alive
            .store(false, Ordering::Release);
    }
    detach_grant(state);
    cq.owners.lock().unwrap().remove(&state.qpn);
    unsafe { &*state.receive_cq }
        .owners
        .lock()
        .unwrap()
        .remove(&state.qpn);
    cq.qps.fetch_sub(1, Ordering::SeqCst);
    unsafe { &*state.receive_cq }
        .qps
        .fetch_sub(1, Ordering::SeqCst);
    device.endpoints.fetch_sub(1, Ordering::SeqCst);
    unsafe {
        drop(Box::from_raw(handles.qp.cast::<Fake>()));
    }
    handles.qp = std::ptr::null_mut();
    0
}

unsafe extern "C" fn mr(h: *mut sys::MemoryHandles) -> c_int {
    let handles = unsafe { &mut *h };
    if handles.mr.is_null() {
        return 0;
    }
    let state = unsafe { &*handles.mr.cast::<FakeMr>() };
    let device = unsafe { &*state.device };
    device
        .events
        .lock()
        .unwrap()
        .push(if state.local_only && state.window_bind {
            "deregister_parent"
        } else if state.local_only {
            "deregister_local"
        } else {
            "deregister"
        });
    assert_eq!(
        state.qps.load(Ordering::SeqCst),
        0,
        "QP barrier precedes MR cleanup"
    );
    assert_eq!(
        state.windows.load(Ordering::SeqCst),
        0,
        "MWs close before parent MR"
    );
    if device.mode.ends_with("close1")
        || device.mode == "mr_close"
        || (device.mode == "message_close"
            && state.local_only
            && state.length == COMPLETION_MESSAGE_SIZE)
    {
        return 16;
    }
    if device.mode == "mr_close_pause" {
        block(device);
    }
    device.mrs.fetch_sub(1, Ordering::SeqCst);
    unsafe {
        drop(Box::from_raw(handles.mr.cast::<FakeMr>()));
    }
    handles.mr = std::ptr::null_mut();
    0
}

unsafe extern "C" fn cq(h: *mut sys::ConnectionHandles) -> c_int {
    let handles = unsafe { &mut *h };
    if handles.cq.is_null() {
        return 0;
    }
    let state = unsafe { &*handles.cq.cast::<FakeCq>() };
    let device = unsafe { &*state.device };
    assert_eq!(
        state.qps.load(Ordering::SeqCst),
        0,
        "all attached QPs close before shared CQ"
    );
    device.events.lock().unwrap().push("destroy_cq");
    state.retiring.store(true, Ordering::Release);
    if device.mode == "cq_pause" {
        block(device);
    }
    if device.mode.ends_with("close2")
        || (device.mode.contains("receive_cq_close") && state.ordinal == 2)
    {
        return 16;
    }
    device.cqs.fetch_sub(1, Ordering::SeqCst);
    unsafe {
        drop(Box::from_raw(handles.cq.cast::<FakeCq>()));
    }
    handles.cq = std::ptr::null_mut();
    0
}

unsafe extern "C" fn pd(h: *mut sys::DeviceHandles) -> c_int {
    let h = unsafe { &mut *h };
    if h.pd.is_null() {
        return 0;
    }
    let state = unsafe { &*h.context.cast::<FakeDevice>() };
    assert_eq!(state.endpoints.load(Ordering::SeqCst), 0);
    assert_eq!(state.mrs.load(Ordering::SeqCst), 0, "MRs close before PD");
    assert_eq!(
        state.windows.load(Ordering::SeqCst),
        0,
        "MWs close before PD"
    );
    assert_eq!(state.cqs.load(Ordering::SeqCst), 0, "CQs close before PD");
    state.events.lock().unwrap().push("dealloc_pd");
    if matches!(state.mode.as_str(), "close3" | "device_pd_close") {
        return 16;
    }
    h.pd = std::ptr::null_mut();
    0
}

unsafe extern "C" fn ctx(h: *mut sys::DeviceHandles) -> c_int {
    let h = unsafe { &mut *h };
    if h.context.is_null() {
        return 0;
    }
    let state = unsafe { &*h.context.cast::<FakeDevice>() };
    assert!(h.pd.is_null(), "PD closes before context");
    assert_eq!(state.endpoints.load(Ordering::SeqCst), 0);
    state.events.lock().unwrap().push("close_device");
    if matches!(state.mode.as_str(), "close4" | "device_open_close") {
        return 16;
    }
    unsafe {
        drop(Box::from_raw(h.context.cast::<FakeDevice>()));
    }
    h.context = std::ptr::null_mut();
    0
}
pub(super) fn discover(
    config: &RdmaConfig,
) -> Result<(crate::Topology, Vec<discovery::RdmaAddress>)> {
    let name = config.device.as_deref().unwrap_or("ok");
    let address = discovery::RdmaAddress {
        device: name.into(),
        port: config.port.unwrap_or(1),
        gid_index: config.gid_index.unwrap_or(0),
        gid_type: discovery::GidType::Ib,
        net_device: None,
        net_device_index: 0,
        descriptor: Descriptor {
            link_layer: 1,
            ..Descriptor::default()
        },
    };
    let topology = crate::Topology {
        nic_list: vec![crate::NicEntry {
            name: name.into(),
            pci_bus_id: None,
            kind: crate::NicType::Rdma,
            numa_node: None,
            device_attrs: Default::default(),
        }],
        mem_list: vec![],
    };
    Ok((topology, vec![address]))
}

unsafe fn query_address(
    handles: *const sys::DeviceHandles,
    address: &discovery::RdmaAddress,
) -> Result<discovery::RdmaAddress> {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    if let Some(route) = &*device.route.lock().unwrap() {
        return Ok(route.clone());
    }
    if device.mode == "route_rejected" {
        let mut observed = address.clone();
        observed.descriptor.gid[15] ^= 1;
        return Ok(observed);
    }
    Ok(discovery::RdmaAddress {
        descriptor: Descriptor {
            lid: 1,
            mtu: 3,
            link_layer: 1,
            ..address.descriptor
        },
        ..address.clone()
    })
}

pub(super) static VERBS: sys::Verbs = sys::Verbs {
    discover,
    query_address,
    asynchronous: None,
    open_device,
    create_cq,
    register_memory,
    prepare_grant,
    open,
    connect,
    post,
    window,
    post_receive,
    send_completion,
    dealloc_window,
    poll,
    destroy_qp: qp,
    deregister: mr,
    destroy_cq: cq,
    dealloc_pd: pd,
    close_device: ctx,
};
impl FakeDevice {
    pub(crate) fn emit_async(&self, kind: u8) {
        use std::io::Write;
        self.asynchronous
            .as_ref()
            .unwrap()
            .1
            .lock()
            .unwrap()
            .write_all(&[kind])
            .unwrap();
    }
}

unsafe extern "C" fn async_get(context: *mut c_void, event: *mut sys::AsyncEvent) -> c_int {
    use std::io::Read;
    let device = unsafe { &*context.cast::<FakeDevice>() };
    let mut kind = [0];
    if let Err(error) = device
        .asynchronous
        .as_ref()
        .unwrap()
        .0
        .lock()
        .unwrap()
        .read_exact(&mut kind)
    {
        return error.raw_os_error().unwrap_or(libc::EIO);
    }
    if kind[0] == 255 {
        return libc::EIO;
    }
    unsafe {
        (*event).kind = kind[0] as u32;
    }
    0
}
unsafe extern "C" fn async_ack(context: *mut c_void, _: *mut sys::AsyncEvent) {
    let device = unsafe { &*context.cast::<FakeDevice>() };
    device.events.lock().unwrap().push("async_ack");
    *device.async_acks.0.lock().unwrap() += 1;
    device.async_acks.1.notify_all();
}
static ASYNC_VERBS: sys::Verbs = sys::Verbs {
    asynchronous: Some(sys::AsyncVerbs {
        get: async_get,
        ack: async_ack,
    }),
    ..VERBS
};

pub(crate) fn runtime(mode: &str) -> RdmaTransport {
    let (host_memory, mode) = match mode.strip_prefix("direct:") {
        Some(mode) => (RdmaHostMemory::Direct, mode),
        None => (RdmaHostMemory::Staged, mode),
    };
    let config = RdmaConfig {
        max_endpoints: 1,
        max_staging_bytes: 8,
        host_memory,
        max_direct_bytes: 8,
        block_size: if mode == "pause_copy" { 2 } else { 64 << 10 },
        max_send_wr: if mode == "pause_copy" { 2 } else { 16 },
        ..RdmaConfig::new(mode)
    };
    let usage = Arc::default();
    let verbs = if mode.starts_with("async_") {
        &ASYNC_VERBS
    } else {
        &VERBS
    };
    let registrations = buffers::LocalBufferManager::new(&config, Arc::clone(&usage), verbs);
    RdmaTransport {
        config,
        usage,
        drained: Arc::default(),
        verbs,
        device: Mutex::default(),
        registrations,
    }
}

impl RdmaEndpoint {
    pub(crate) fn has_resources(&self) -> bool {
        self.record
            .lock()
            .map(|state| !matches!(state.phase, Phase::Closed | Phase::Retained))
            .unwrap_or(true)
    }
}

type CloseHook = Box<dyn FnOnce(&mut RdmaEndpoint, &Result<()>)>;
thread_local! { static BEFORE_REQUESTER_CLOSE: std::cell::RefCell<Option<CloseHook>> = const { std::cell::RefCell::new(None) }; }

pub(crate) fn on_requester_close(hook: impl FnOnce(&mut RdmaEndpoint, &Result<()>) + 'static) {
    BEFORE_REQUESTER_CLOSE
        .with(|slot| assert!(slot.borrow_mut().replace(Box::new(hook)).is_none()));
}

pub(crate) fn before_requester_close(endpoint: &mut RdmaEndpoint, result: &Result<()>) {
    let hook = BEFORE_REQUESTER_CLOSE.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook(endpoint, result);
    }
}

pub(crate) fn before_source_ack(runtime: &RdmaTransport) {
    if std::ptr::eq(runtime.verbs, &VERBS) && runtime.config.device.as_deref() == Some("ack_pause")
    {
        let pause = pause(runtime);
        let (lock, changed) = &*pause;
        let mut state = lock.lock().unwrap();
        state.0 = true;
        changed.notify_all();
        while !state.1 {
            state = changed.wait(state).unwrap();
        }
    }
}

pub(crate) fn work_usage(runtime: &RdmaTransport) -> Arc<Mutex<RdmaStats>> {
    Arc::clone(&runtime.usage)
}

impl RdmaTransport {
    pub(crate) fn staging_endpoint(&self, length: usize, permission: u32) -> Result<RdmaEndpoint> {
        let registry = crate::memory::MemoryRegistry::new(SourceEpoch([1; 16]), 1, length.max(1));
        let memory = registry.register(
            vec![0; length.max(1)].into_boxed_slice(),
            crate::Permission::LocalOnly,
        )?;
        self.endpoint(
            &memory,
            0,
            length,
            permission,
            Instant::now() + Duration::from_secs(2),
            None,
        )
    }
}

#[test]
fn descriptor_wire_layout_and_untrusted_fields_are_checked_before_native_use() {
    assert_eq!(std::mem::size_of::<Descriptor>(), 40);
    let descriptor = Descriptor {
        address: 0x1020304050607080,
        rkey: 0,
        qpn: 0xffffff,
        psn: 0xffffff,
        lid: 0x1234,
        mtu: 5,
        link_layer: 2,
        gid: [9; 16],
    };
    assert_eq!(&descriptor.encode()[..8], &descriptor.address.to_be_bytes());
    assert_eq!(
        Descriptor::decode(&descriptor.encode()).unwrap().encode(),
        descriptor.encode()
    );
    for invalid in [
        Descriptor {
            address: 0,
            ..descriptor
        },
        Descriptor {
            qpn: 0,
            ..descriptor
        },
        Descriptor {
            qpn: 1 << 24,
            ..descriptor
        },
        Descriptor {
            psn: 1 << 24,
            ..descriptor
        },
        Descriptor {
            mtu: 0,
            ..descriptor
        },
        Descriptor {
            mtu: 6,
            ..descriptor
        },
        Descriptor {
            link_layer: 3,
            ..descriptor
        },
        Descriptor {
            gid: [0; 16],
            ..descriptor
        },
    ] {
        assert!(matches!(
            Descriptor::decode(&invalid.encode()),
            Err(Error::Protocol(_))
        ));
    }
    for config in [
        RdmaConfig::new(""),
        RdmaConfig::new("x\0y"),
        RdmaConfig {
            max_window_key_indices: 0,
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            max_window_key_indices: (1 << 24) + 1,
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            max_cached_mrs: 0,
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            max_cached_mr_bytes: 0,
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            max_endpoints: 0,
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            port: Some(0),
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            max_send_wr: 0,
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            max_send_wr: 65,
            ..RdmaConfig::new("x")
        },
        RdmaConfig {
            block_size: 0,
            ..RdmaConfig::new("x")
        },
    ] {
        assert!(config.validate().is_err());
    }
}

#[test]
fn endpoint_budget_and_error_paths_close_dependencies_before_returning_capacity() {
    for mode in [
        "ok", "open", "connect", "post", "poll", "wc", "identity", "timeout",
    ] {
        let runtime = runtime(mode);
        assert!(matches!(
            runtime.staging_endpoint(0, 0),
            Err(Error::InvalidRange)
        ));
        assert!(matches!(
            runtime.staging_endpoint(9, 0),
            Err(Error::ResourceExhausted)
        ));
        let result = runtime.staging_endpoint(8, 1).map(|mut endpoint| {
            assert!(matches!(
                runtime.staging_endpoint(1, 0),
                Err(Error::ResourceExhausted)
            ));
            let output = crate::memory::MemoryRegistry::new(SourceEpoch([2; 16]), 1, 8)
                .register(
                    vec![0xcc; 8].into_boxed_slice(),
                    crate::Permission::LocalOnly,
                )
                .unwrap();
            assert!(endpoint.copy_back(&output, 0).is_err());
            let descriptor = endpoint.descriptor();
            let result = endpoint.connect(&descriptor).and_then(|()| {
                endpoint.transfer(
                    &descriptor,
                    TransferOp::Read,
                    Instant::now() + Duration::from_millis(250),
                )
            });
            assert_eq!(result.is_ok(), mode == "ok", "{mode}");
            let closed = endpoint.close();
            assert!(
                !matches!(closed, Err(Error::Unquiesced { .. })),
                "{mode}: {closed:?}"
            );
            if mode == "ok" || mode == "connect" {
                endpoint.copy_back(&output, 0).unwrap();
                let mut observed = [0; 8];
                output.read(0, &mut observed).unwrap();
                assert_eq!(observed, [0; 8]);
            } else {
                assert!(endpoint.copy_back(&output, 0).is_err());
            }
        });
        assert_eq!(result.is_ok(), mode != "open", "{mode}");
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.staging_bytes,
                stats.retained_endpoints
            ),
            (0, 0, 0),
            "{mode}"
        );
    }
}

#[test]
fn teardown_failure_retains_exact_staging_and_blocks_new_admission() {
    for index in 0..2 {
        let runtime = runtime(&format!("close{index}"));
        let mut endpoint = runtime.staging_endpoint(8, 2).unwrap();
        assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
        assert!(!endpoint.has_resources());
        drop(endpoint);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.staging_bytes,
                stats.retained_endpoints,
                stats.retained_bytes,
                stats.active_grants,
                stats.retained_grants
            ),
            (usize::from(index == 0), 8, usize::from(index == 0), 8, 1, 1)
        );
        assert!(matches!(
            runtime.staging_endpoint(1, 0),
            Err(Error::Unavailable(_))
        ));
        assert!(
            runtime
                .close(Instant::now() + Duration::from_secs(2))
                .is_err()
        );
        let events = super::context::tests::events(&runtime);
        let usage = Arc::clone(&runtime.usage);
        drop(runtime);
        assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
        assert_eq!(usage.lock().unwrap().active_devices, 1);
    }
}

#[test]
fn direct_registration_uses_exact_range_and_keeps_capacity_separate_from_staging() {
    let registry = crate::memory::MemoryRegistry::new(SourceEpoch([1; 16]), 4, 32);
    let memory = registry
        .register(
            vec![0x55; 24].into_boxed_slice(),
            crate::Permission::RemoteReadWrite,
        )
        .unwrap();
    let mut runtime = runtime("direct:ok");
    runtime.config.max_endpoints = 3;
    let mut first = runtime
        .endpoint(
            &memory,
            3,
            5,
            1,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    let mut second = runtime
        .endpoint(
            &memory,
            13,
            3,
            2,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    assert_eq!(first.descriptor().address, memory.base_address() + 3);
    assert_eq!(second.descriptor().address, memory.base_address() + 13);
    {
        let state = first.record.lock().unwrap();
        let resources = state.resources.as_ref().unwrap();
        let fake = unsafe { &*resources.connection.handles.qp.cast::<Fake>() };
        assert_eq!(
            (
                fake.bytes.load(Ordering::Acquire),
                fake.length.load(Ordering::Acquire)
            ),
            (memory.base_address() + 3, 5)
        );
    }
    assert!(matches!(
        runtime.endpoint(
            &memory,
            9,
            1,
            0,
            Instant::now() + Duration::from_secs(2),
            None
        ),
        Err(Error::ResourceExhausted)
    ));
    memory.write(9, &[7]).unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.direct_bytes,
            stats.staging_bytes,
            stats.opened_direct_endpoints
        ),
        (8, 0, 2)
    );
    assert!(matches!(
        memory.read(3, &mut [0; 1]),
        Err(Error::Unavailable(_))
    ));
    first.close().unwrap();
    memory.write(3, &[9; 5]).unwrap();
    assert_eq!(runtime.stats().unwrap().direct_bytes, 8);
    drop(first);
    assert_eq!(runtime.stats().unwrap().direct_bytes, 3);
    assert!(matches!(
        memory.write(13, &[0; 1]),
        Err(Error::Unavailable(_))
    ));
    second.close().unwrap();
    drop(second);
    assert_eq!(runtime.stats().unwrap().direct_bytes, 0);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn direct_native_failures_release_or_retain_actual_backing_registration_and_access_lease() {
    use crate::test_support;
    use std::sync::atomic::Ordering;
    for mode in [
        "ok",
        "open",
        "device_missing",
        "open_close1",
        "close0",
        "close1",
    ] {
        let fixture = test_support::external_memory(&[0x33; 16]);
        let allocation = Arc::clone(&fixture.memory);
        let registry = crate::memory::MemoryRegistry::new(SourceEpoch([1; 16]), 1, 8);
        let memory = registry
            .register_host(allocation.clone(), 4, 8, crate::Permission::RemoteReadWrite)
            .unwrap();
        let runtime = runtime(&format!("direct:{mode}"));
        if mode == "open_close1" {
            // Already-resolved work may outlive final unregister; construction must
            // not resurrect an idle parent when partial cleanup fails.
            registry.unregister(&memory).unwrap();
        }
        let result = runtime.endpoint(
            &memory,
            2,
            4,
            2,
            Instant::now() + Duration::from_secs(2),
            None,
        );
        let retained = mode.contains("close");
        if memory.is_active() {
            registry.unregister(&memory).unwrap();
        }
        let retired = runtime.registrations.retire(&memory);
        if mode == "open_close1" {
            assert!(matches!(retired, Err(Error::Unquiesced { .. })));
        } else {
            retired.unwrap();
        }
        match result {
            Ok(mut endpoint) => {
                assert_eq!(endpoint.close().is_err(), retained, "{mode}");
                drop(endpoint);
            }
            Err(error) => assert_eq!(
                matches!(error, Error::Unquiesced { .. }),
                retained,
                "{mode}: {error:?}"
            ),
        }
        drop(memory);
        drop(fixture.memory);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.direct_bytes,
                stats.staging_bytes,
                stats.retained_endpoints,
                stats.active_grants,
                stats.retained_grants
            ),
            (
                if retained { 4 } else { 0 },
                0,
                usize::from(mode.ends_with("close0")),
                usize::from(retained),
                usize::from(retained)
            ),
            "{mode}"
        );
        if retained {
            assert_eq!(registry.usage().unwrap(), (1, 8));
            assert!(matches!(
                allocation.read(6, &mut [0; 4]),
                Err(Error::Unavailable(_))
            ));
            allocation.write(0, &[1; 6]).unwrap();
            let probe = crate::memory::MemoryRegistry::new(SourceEpoch([2; 16]), 1, 8)
                .register(vec![0; 8].into_boxed_slice(), crate::Permission::LocalOnly)
                .unwrap();
            assert!(matches!(
                runtime.endpoint(
                    &probe,
                    0,
                    1,
                    0,
                    Instant::now() + Duration::from_secs(2),
                    None
                ),
                Err(Error::Unavailable(_))
            ));
            assert!(
                runtime
                    .close(Instant::now() + Duration::from_secs(2))
                    .is_err()
            );
        } else {
            assert_eq!(registry.usage().unwrap(), (0, 0));
            allocation.write(6, &[9; 4]).unwrap();
            runtime
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
        }
        drop(allocation);
        drop(runtime);
        assert_eq!(fixture.owner.upgrade().is_some(), retained, "{mode}");
        assert_eq!(
            fixture.drops.load(Ordering::SeqCst),
            usize::from(!retained),
            "{mode}"
        );
    }
}

#[test]
fn native_send_windows_keep_the_whole_direct_lease_and_clear_remaining_debt_only_after_close() {
    for (mode, owed, retained) in [
        ("reverse", 0, false),
        ("partial", 1, false),
        ("unknown", 4, false),
        ("identity", 4, false),
        ("duplicate", 3, false),
        ("length", 4, false),
        ("wc", 3, false),
        ("timeout", 4, false),
        ("partial_close0", 1, true),
        ("unknown_close1", 4, true),
        ("timeout_close2", 4, true),
    ] {
        let registry = crate::memory::MemoryRegistry::new(SourceEpoch([1; 16]), 1, 128);
        let memory = registry
            .register(
                vec![0x55; 128].into_boxed_slice(),
                crate::Permission::LocalOnly,
            )
            .unwrap();
        let mut runtime = runtime(&format!("direct:{mode}"));
        runtime.config.max_direct_bytes = 128;
        runtime.config.block_size = 16;
        runtime.config.max_send_wr = 4;
        let mut endpoint = runtime
            .endpoint(
                &memory,
                0,
                128,
                1,
                Instant::now() + Duration::from_secs(2),
                None,
            )
            .unwrap();
        let remote = endpoint.descriptor();
        endpoint.connect(&remote).unwrap();
        if mode.ends_with("close1") {
            // Retire while the borrower is held: the worker can finish failed
            // posting before transfer() returns to this test thread.
            registry.unregister(&memory).unwrap();
            runtime.registrations.retire(&memory).unwrap();
        }
        let result = endpoint.transfer(
            &remote,
            TransferOp::Read,
            Instant::now() + Duration::from_millis(250),
        );
        assert_eq!(result.is_ok(), mode == "reverse", "{mode}: {result:?}");
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.peak_work_requests, 4);
        assert_eq!(stats.post_calls, if mode == "reverse" { 2 } else { 1 });
        let closed = endpoint.close();
        assert_eq!(
            matches!(closed, Err(Error::Unquiesced { .. })),
            retained,
            "{mode}: {closed:?}"
        );
        let stats = runtime.stats().unwrap();
        let owed = if mode.ends_with("close1") { 0 } else { owed };
        assert_eq!(stats.work_requests, if retained { owed } else { 0 });
        assert_eq!(
            stats.retained_work_requests,
            if retained { owed } else { 0 }
        );
        if retained {
            assert!(matches!(
                memory.write(127, &[9]),
                Err(Error::Unavailable(_))
            ));
            assert!(
                runtime
                    .close(Instant::now() + Duration::from_secs(2))
                    .is_err()
            );
        } else {
            memory.write(127, &[9]).unwrap();
            drop(endpoint);
            runtime
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
        }
    }
}

mod windows;

impl RdmaEndpoint {
    pub(crate) fn descriptor(&self) -> Descriptor {
        let state = self.record.lock().unwrap();
        let resources = state.resources.as_ref().unwrap();
        Descriptor {
            address: resources.grant.as_ref().unwrap().buffer.address().as_ptr() as u64,
            rkey: resources.grant.as_ref().unwrap().rkey,
            ..resources.connection.local
        }
    }
}

pub(super) fn deliver_completion(
    endpoint: &RdmaEndpoint,
    bytes: &[u8; COMPLETION_MESSAGE_SIZE],
    mutate: impl FnOnce(&mut sys::Completion),
    send_cq: bool,
) {
    let request = endpoint.record.lock().unwrap();
    let resources = request.resources.as_ref().unwrap();
    let qp = unsafe { &*resources.connection.handles.qp.cast::<Fake>() };
    assert_ne!(
        qp.cq, qp.receive_cq,
        "SQ and RQ controls must use different actual CQ handles"
    );
    let (id, address, length) = qp.receive.lock().unwrap().unwrap();
    assert_eq!(length, bytes.len());
    // CPU owner/routing control only; this cannot qualify provider DMA ordering.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), address as *mut u8, length);
    }
    let mut completion = sys::Completion {
        id,
        qpn: qp.qpn,
        opcode: 128,
        length: length as u32,
        flags: sys::WITH_INVALIDATE,
        invalidated_rkey: qp.window_key.load(Ordering::Acquire),
        ..Default::default()
    };
    mutate(&mut completion);
    let cq = if send_cq { qp.cq } else { qp.receive_cq };
    unsafe { &*cq }
        .completions
        .lock()
        .unwrap()
        .push_back(completion);
}

pub(crate) fn before_reused_open(runtime: &RdmaTransport, endpoint: &RdmaEndpoint) {
    if endpoint.cached_peer.is_some()
        && runtime.config.device.as_deref() == Some("async_copy_reuse_open_pause")
    {
        let pause = pause(runtime);
        let (lock, changed) = &*pause;
        let mut state = lock.lock().unwrap();
        state.0 = true;
        changed.notify_all();
        while !state.1 {
            state = changed.wait(state).unwrap();
        }
    }
}
