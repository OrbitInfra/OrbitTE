#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

pub(crate) use super::device::tests::{events, pause};
use super::*;

pub(crate) struct FakeDevice {
    mode: String,
    pub(crate) events: Arc<Mutex<Vec<&'static str>>>,
    endpoints: AtomicUsize,
    mrs: AtomicUsize,
    cqs: AtomicUsize,
    next_qpn: AtomicU32,
    pub(crate) pause: Arc<(Mutex<(bool, bool)>, Condvar)>,
}

struct FakeCq {
    device: *const FakeDevice,
    completions: Mutex<std::collections::VecDeque<sys::Completion>>,
    qps: AtomicUsize,
}

struct Fake {
    device: *const FakeDevice,
    cq: *const FakeCq,
    length: usize,
    bytes: *mut u8,
    qpn: u32,
    first_id: u64,
}

struct FakeMr {
    device: *const FakeDevice,
    address: usize,
    length: usize,
    local_only: bool,
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
unsafe extern "C" fn open_device(h: *mut sys::DeviceHandles, name: *const c_char) -> c_int {
    let mode = unsafe { CStr::from_ptr(name) }.to_str().unwrap().to_owned();
    if mode == "device_missing" {
        return libc::ENODEV;
    }
    let state = Box::into_raw(Box::new(FakeDevice {
        mode,
        events: Arc::new(Mutex::new(vec!["open_device"])),
        endpoints: AtomicUsize::new(0),
        mrs: AtomicUsize::new(0),
        cqs: AtomicUsize::new(0),
        next_qpn: AtomicU32::new(9),
        pause: Arc::default(),
    }));
    unsafe {
        (*h).context = state.cast();
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
    h: *mut sys::Handles,
    handles: *const sys::DeviceHandles,
    _: u32,
) -> c_int {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    device.events.lock().unwrap().push("create_cq");
    device.cqs.fetch_add(1, Ordering::SeqCst);
    unsafe {
        (*h).cq = Box::into_raw(Box::new(FakeCq {
            device,
            completions: Mutex::default(),
            qps: AtomicUsize::new(0),
        }))
        .cast();
    }
    if device.mode.starts_with("cq_create") {
        libc::EIO
    } else {
        0
    }
}

unsafe extern "C" fn register_local(
    h: *mut sys::Handles,
    handles: *const sys::DeviceHandles,
    address: *mut c_void,
    length: usize,
) -> c_int {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    device.events.lock().unwrap().push("register_local");
    device.mrs.fetch_add(1, Ordering::SeqCst);
    unsafe {
        (*h).mr = Box::into_raw(Box::new(FakeMr {
            device,
            address: address as usize,
            length,
            local_only: true,
        }))
        .cast();
    }
    if device.mode == "register_pause" {
        block(device);
    }
    if device.mode.starts_with("register_fail") {
        libc::EIO
    } else {
        0
    }
}

unsafe extern "C" fn open(
    h: *mut sys::Handles,
    handles: *const sys::DeviceHandles,
    _: u8,
    _: u8,
    bytes: *mut c_void,
    length: usize,
    _: u32,
    psn: u32,
    _: u32,
    descriptor: *mut Descriptor,
) -> c_int {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    let cq = unsafe { &*(*h).cq.cast::<FakeCq>() };
    assert_eq!(cq.device, device as *const FakeDevice);
    device.events.lock().unwrap().push("open_endpoint");
    device.endpoints.fetch_add(1, Ordering::SeqCst);
    cq.qps.fetch_add(1, Ordering::SeqCst);
    let qpn = device.next_qpn.fetch_add(1, Ordering::SeqCst);
    unsafe {
        if (*h).mr.is_null() {
            device.mrs.fetch_add(1, Ordering::SeqCst);
            (*h).mr = Box::into_raw(Box::new(FakeMr {
                device,
                address: bytes as usize,
                length,
                local_only: false,
            }))
            .cast();
        } else {
            let mr = &*(*h).mr.cast::<FakeMr>();
            assert!(
                bytes as usize >= mr.address && bytes as usize + length <= mr.address + mr.length
            );
        }
        (*h).qp = Box::into_raw(Box::new(Fake {
            device,
            cq,
            length,
            bytes: bytes.cast(),
            qpn,
            first_id: 0,
        }))
        .cast();
        *descriptor = Descriptor {
            address: bytes as u64,
            rkey: 7,
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

unsafe extern "C" fn connect(
    h: *mut sys::Handles,
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
    h: *mut sys::Handles,
    requests: *const sys::WorkRequest,
    count: usize,
    local: u64,
    local_length: usize,
    remote: u64,
    _: u32,
    write: c_int,
    result: *mut sys::PostResult,
) -> c_int {
    let state = unsafe { &mut *(*h).qp.cast::<Fake>() };
    let device = unsafe { &*state.device };
    let cq = unsafe { &*state.cq };
    let mode = device.mode.as_str();
    assert_eq!(local, state.bytes as u64);
    assert_eq!(local_length, state.length);
    let requests = unsafe { std::slice::from_raw_parts(requests, count) };
    if state.first_id == 0 {
        state.first_id = requests[0].id;
    }
    let partial = mode.starts_with("partial");
    let uncertain = mode.contains("unknown") || mode == "copy_post";
    let accepted = if partial {
        count.min(1)
    } else if mode == "post" {
        0
    } else {
        count
    };
    for request in &requests[..accepted] {
        assert!(request.offset as usize + request.length as usize <= state.length);
        if mode.contains("copy") {
            // CPU mutation under production leases, not a DMA receipt.
            unsafe {
                let local = state.bytes.add(request.offset as usize);
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
                state.first_id
            } else {
                request.id
            },
            qpn: state.qpn + u32::from(mode == "wrong_qpn"),
            opcode: if write == 1 { 1 } else { 2 },
            length: request.length - u32::from(mode == "length"),
            status: u32::from(mode == "wc" || mode == "wc_close0"),
            vendor_error: 0,
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
    if partial || uncertain || mode == "post" {
        libc::ENOMEM
    } else {
        0
    }
}

unsafe extern "C" fn poll(h: *mut sys::Handles, completion: *mut sys::Completion) -> c_int {
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
        unsafe {
            *completion = value;
        }
        1
    } else {
        0
    }
}

unsafe extern "C" fn qp(h: *mut sys::Handles) -> c_int {
    let handles = unsafe { &mut *h };
    if handles.qp.is_null() {
        return 0;
    }
    let state = unsafe { &*handles.qp.cast::<Fake>() };
    let device = unsafe { &*state.device };
    device.events.lock().unwrap().push("destroy_qp");
    if device.mode.contains("qp_pause") {
        block(device);
    }
    if device.mode.ends_with("close0") {
        return 16;
    }
    let cq = unsafe { &*state.cq };
    cq.qps.fetch_sub(1, Ordering::SeqCst);
    device.endpoints.fetch_sub(1, Ordering::SeqCst);
    unsafe {
        drop(Box::from_raw(handles.qp.cast::<Fake>()));
    }
    handles.qp = std::ptr::null_mut();
    0
}

unsafe extern "C" fn mr(h: *mut sys::Handles) -> c_int {
    let handles = unsafe { &mut *h };
    if handles.mr.is_null() {
        return 0;
    }
    let state = unsafe { &*handles.mr.cast::<FakeMr>() };
    let device = unsafe { &*state.device };
    device.events.lock().unwrap().push(if state.local_only {
        "deregister_local"
    } else {
        "deregister"
    });
    if !state.local_only {
        assert!(handles.qp.is_null());
    }
    if device.mode.ends_with("close1") || device.mode == "mr_close" {
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

unsafe extern "C" fn cq(h: *mut sys::Handles) -> c_int {
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
    if device.mode.ends_with("close2") {
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
static VERBS: sys::Verbs = sys::Verbs {
    open_device,
    create_cq,
    register_local,
    open,
    connect,
    post,
    poll,
    destroy_qp: qp,
    deregister: mr,
    destroy_cq: cq,
    dealloc_pd: pd,
    close_device: ctx,
};
pub(crate) fn runtime(mode: &str) -> Runtime {
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
    let registrations = registration::HostRegistrations::new(&config, Arc::clone(&usage), &VERBS);
    Runtime {
        config,
        usage,
        drained: Arc::default(),
        verbs: &VERBS,
        device: Mutex::default(),
        registrations,
    }
}

pub(crate) fn work_usage(runtime: &Runtime) -> Arc<Mutex<RdmaStats>> {
    Arc::clone(&runtime.usage)
}

impl Runtime {
    pub(crate) fn staging_endpoint(&self, length: usize, permission: u32) -> Result<Endpoint> {
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
            port: 0,
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
                    Instant::now() + Duration::from_millis(2),
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
                stats.retained_bytes
            ),
            (1, 8, 1, 8)
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
        let events = super::device::tests::events(&runtime);
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
        .endpoint(&memory, 3, 5, 1, Instant::now() + Duration::from_secs(2))
        .unwrap();
    let mut second = runtime
        .endpoint(&memory, 13, 3, 2, Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(first.descriptor().address, memory.base_address() + 3);
    assert_eq!(second.descriptor().address, memory.base_address() + 13);
    {
        let state = first.record.lock().unwrap();
        let resources = state.resources.as_ref().unwrap();
        let fake = unsafe { &*resources.handles.qp.cast::<Fake>() };
        assert_eq!(
            (fake.bytes as u64, fake.length),
            (memory.base_address() + 3, 5)
        );
    }
    assert!(matches!(
        runtime.endpoint(&memory, 9, 1, 0, Instant::now() + Duration::from_secs(2)),
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
        let result = runtime.endpoint(&memory, 2, 4, 2, Instant::now() + Duration::from_secs(2));
        let retained = mode.contains("close");
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
        registry.unregister(&memory).unwrap();
        drop(memory);
        drop(fixture.memory);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.direct_bytes,
                stats.staging_bytes,
                stats.retained_endpoints
            ),
            (if retained { 4 } else { 0 }, 0, usize::from(retained)),
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
                runtime.endpoint(&probe, 0, 1, 0, Instant::now() + Duration::from_secs(2)),
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
            .endpoint(&memory, 0, 128, 1, Instant::now() + Duration::from_secs(2))
            .unwrap();
        let remote = endpoint.descriptor();
        endpoint.connect(&remote).unwrap();
        let result = endpoint.transfer(
            &remote,
            TransferOp::Read,
            Instant::now() + Duration::from_millis(2),
        );
        assert_eq!(result.is_ok(), mode == "reverse", "{mode}: {result:?}");
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.peak_work_requests, 4);
        assert_eq!(stats.post_calls, if mode == "reverse" { 2 } else { 1 });
        if mode.ends_with("close1") {
            registry.unregister(&memory).unwrap();
        }
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
