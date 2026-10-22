#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_int, c_void};

pub(crate) use super::device::tests::{events, pause};
use super::*;

pub(crate) struct FakeDevice {
    mode: String,
    pub(crate) events: Arc<Mutex<Vec<&'static str>>>,
    endpoints: std::sync::atomic::AtomicUsize,
    pub(crate) pause: Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>,
}

struct Fake {
    mode: String,
    device: *const FakeDevice,
    length: usize,
    bytes: *mut u8,
    operation: i32,
    closed: usize,
    completions: std::collections::VecDeque<sys::Completion>,
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
        endpoints: std::sync::atomic::AtomicUsize::new(0),
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

unsafe extern "C" fn open(
    h: *mut sys::Handles,
    device: *const sys::DeviceHandles,
    _: u8,
    _: u8,
    bytes: *mut c_void,
    length: usize,
    _: u32,
    psn: u32,
    _: u32,
    descriptor: *mut Descriptor,
) -> c_int {
    let device = unsafe { &*(*device).context.cast::<FakeDevice>() };
    device.events.lock().unwrap().push("open_endpoint");
    device
        .endpoints
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let failing = device.mode.starts_with("open");
    let state = Box::into_raw(Box::new(Fake {
        mode: device.mode.clone(),
        device,
        length,
        bytes: bytes.cast(),
        operation: 0,
        closed: 0,
        completions: std::collections::VecDeque::new(),
    }));
    unsafe {
        (*h).cq = state.cast();
        (*h).qp = std::ptr::dangling_mut::<u8>().cast();
        (*h).mr = std::ptr::dangling_mut::<u8>().cast();
        *descriptor = Descriptor {
            address: bytes as u64,
            rkey: 7,
            qpn: 9,
            psn,
            lid: 1,
            mtu: 3,
            link_layer: 1,
            gid: [0; 16],
        };
    }
    if failing { 5 } else { 0 }
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
    i32::from(unsafe { &*(*h).cq.cast::<Fake>() }.mode == "connect")
}
unsafe extern "C" fn post(
    h: *mut sys::Handles,
    requests: *const sys::WorkRequest,
    count: usize,
    remote: u64,
    _: u32,
    write: c_int,
    result: *mut sys::PostResult,
) -> c_int {
    let state = unsafe { &mut *(*h).cq.cast::<Fake>() };
    state.operation = write;
    let requests = unsafe { std::slice::from_raw_parts(requests, count) };
    let partial = state.mode.starts_with("partial");
    let uncertain = state.mode.contains("unknown") || state.mode == "copy_post";
    let accepted = if partial {
        count.min(1)
    } else if state.mode == "post" {
        0
    } else {
        count
    };
    for request in &requests[..accepted] {
        assert!(request.offset as usize + request.length as usize <= state.length);
        if state.mode.contains("copy") {
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
        state.completions.push_back(sys::Completion {
            id: if state.mode == "identity" {
                u64::MAX
            } else {
                request.id
            },
            opcode: if write == 1 { 1 } else { 2 },
            length: if state.mode == "length" {
                request.length - 1
            } else {
                request.length
            },
            status: u32::from(state.mode == "wc"),
            vendor_error: 0,
        });
    }
    if state.mode == "pause_copy" {
        let device = unsafe { &*state.device };
        let (lock, ready) = &*device.pause;
        let mut state = lock.lock().unwrap();
        state.0 = true;
        ready.notify_all();
        while !state.1 {
            state = ready.wait(state).unwrap();
        }
    }
    unsafe {
        *result = sys::PostResult {
            accepted: if uncertain { 0 } else { accepted },
            uncertain,
        };
    }
    if partial || uncertain || state.mode == "post" {
        libc::ENOMEM
    } else {
        0
    }
}
unsafe extern "C" fn poll(h: *mut sys::Handles, completion: *mut sys::Completion) -> c_int {
    let state = unsafe { &mut *(*h).cq.cast::<Fake>() };
    if state.mode.starts_with("timeout") {
        return 0;
    }
    if state.mode == "poll" {
        return -1;
    }
    let value = if state.mode == "reverse" {
        state.completions.pop_back()
    } else {
        state.completions.pop_front()
    };
    if let Some(mut value) = value {
        if state.mode == "duplicate" {
            value.id = 1;
        }
        unsafe {
            *completion = value;
        }
        1
    } else {
        0
    }
}

unsafe fn close(h: *mut sys::Handles, index: usize) -> c_int {
    let handles = unsafe { &mut *h };
    let state_pointer = handles.cq.cast::<Fake>();
    let handle = match index {
        0 => &mut handles.qp,
        1 => &mut handles.mr,
        _ => &mut handles.cq,
    };
    if handle.is_null() {
        return 0;
    }
    let state = unsafe { &mut *state_pointer };
    assert_eq!(state.closed, index, "endpoint dependencies close in order");
    let device = unsafe { &*state.device };
    device
        .events
        .lock()
        .unwrap()
        .push(["destroy_qp", "deregister", "destroy_cq"][index]);
    if state.mode.ends_with(&format!("close{index}")) {
        return 16;
    }
    *handle = std::ptr::null_mut();
    state.closed += 1;
    if index == 2 {
        device
            .endpoints
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        let state = state as *mut Fake;
        unsafe {
            drop(Box::from_raw(state));
        }
    }
    0
}
unsafe extern "C" fn qp(h: *mut sys::Handles) -> c_int {
    unsafe { close(h, 0) }
}
unsafe extern "C" fn mr(h: *mut sys::Handles) -> c_int {
    unsafe { close(h, 1) }
}
unsafe extern "C" fn cq(h: *mut sys::Handles) -> c_int {
    unsafe { close(h, 2) }
}
unsafe extern "C" fn pd(h: *mut sys::DeviceHandles) -> c_int {
    let h = unsafe { &mut *h };
    if h.pd.is_null() {
        return 0;
    }
    let state = unsafe { &*h.context.cast::<FakeDevice>() };
    assert_eq!(state.endpoints.load(std::sync::atomic::Ordering::SeqCst), 0);
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
    assert_eq!(state.endpoints.load(std::sync::atomic::Ordering::SeqCst), 0);
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
    Runtime {
        config: RdmaConfig {
            max_endpoints: 1,
            max_staging_bytes: 8,
            host_memory,
            max_direct_bytes: 8,
            block_size: if mode == "pause_copy" { 2 } else { 64 << 10 },
            max_send_wr: if mode == "pause_copy" { 2 } else { 16 },
            ..RdmaConfig::new(mode)
        },
        usage: Arc::default(),
        verbs: &VERBS,
        device: Mutex::default(),
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
        self.endpoint(&memory, 0, length, permission)
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
            endpoint.close().unwrap();
            endpoint.copy_back(&output, 0).unwrap();
            let mut observed = [0; 8];
            output.read(0, &mut observed).unwrap();
            assert_eq!(observed, [0; 8]);
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
    for index in 0..3 {
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
        assert!(runtime.close().is_err());
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
    let mut first = runtime.endpoint(&memory, 3, 5, 1).unwrap();
    let mut second = runtime.endpoint(&memory, 13, 3, 2).unwrap();
    assert_eq!(first.descriptor().address, memory.base_address() + 3);
    assert_eq!(second.descriptor().address, memory.base_address() + 13);
    let resources = first.resources.as_ref().unwrap();
    let fake = unsafe { &*resources.handles.cq.cast::<Fake>() };
    assert_eq!(
        (fake.bytes as u64, fake.length),
        (memory.base_address() + 3, 5)
    );
    assert!(matches!(
        runtime.endpoint(&memory, 9, 1, 0),
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
    runtime.close().unwrap();
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
        "close2",
    ] {
        let fixture = test_support::external_memory(&[0x33; 16]);
        let allocation = Arc::clone(&fixture.memory);
        let registry = crate::memory::MemoryRegistry::new(SourceEpoch([1; 16]), 1, 8);
        let memory = registry
            .register_host(allocation.clone(), 4, 8, crate::Permission::RemoteReadWrite)
            .unwrap();
        let runtime = runtime(&format!("direct:{mode}"));
        let result = runtime.endpoint(&memory, 2, 4, 2);
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
                runtime.endpoint(&probe, 0, 1, 0),
                Err(Error::Unavailable(_))
            ));
            assert!(runtime.close().is_err());
        } else {
            assert_eq!(registry.usage().unwrap(), (0, 0));
            allocation.write(6, &[9; 4]).unwrap();
            runtime.close().unwrap();
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
        let mut endpoint = runtime.endpoint(&memory, 0, 128, 0).unwrap();
        let remote = endpoint.descriptor();
        endpoint.connect(&remote).unwrap();
        let result = endpoint.transfer(
            &remote,
            TransferOp::Read,
            Instant::now() + Duration::from_millis(2),
        );
        assert_eq!(result.is_ok(), mode == "reverse", "{mode}: {result:?}");
        assert!(
            matches!(memory.read(0, &mut [0; 1]), Err(Error::Unavailable(_))),
            "all request bytes remain leased until native close"
        );
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.work_requests, owed, "{mode}");
        assert_eq!(stats.peak_work_requests, 4);
        assert_eq!(stats.post_calls, if mode == "reverse" { 2 } else { 1 });
        assert_eq!(endpoint.close().is_err(), retained, "{mode}");
        let stats = runtime.stats().unwrap();
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
            assert!(runtime.close().is_err());
        } else {
            memory.write(127, &[9]).unwrap();
            drop(endpoint);
            runtime.close().unwrap();
        }
    }
}
