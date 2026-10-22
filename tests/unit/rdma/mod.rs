#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_int, c_void};

use super::*;

struct Fake {
    mode: String,
    length: usize,
    operation: i32,
    closed: usize,
}

// This CPU ABI shim exercises ownership and errors; it is not a device emulator
// or evidence that a provider completed DMA.
unsafe extern "C" fn open(
    h: *mut sys::Handles,
    name: *const c_char,
    _: u8,
    _: u8,
    bytes: *mut c_void,
    length: usize,
    _: u32,
    psn: u32,
    descriptor: *mut Descriptor,
) -> c_int {
    let mode = unsafe { CStr::from_ptr(name) }.to_str().unwrap().to_owned();
    let failing = mode == "open";
    let state = Box::into_raw(Box::new(Fake {
        mode,
        length,
        operation: 0,
        closed: 0,
    }));
    unsafe {
        (*h).context = state.cast();
        (*h).pd = std::ptr::dangling_mut::<u8>().cast();
        (*h).cq = std::ptr::dangling_mut::<u8>().cast();
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
    i32::from(unsafe { &*(*h).context.cast::<Fake>() }.mode == "connect")
}
unsafe extern "C" fn post(h: *mut sys::Handles, _: u64, _: u32, _: u32, write: c_int) -> c_int {
    let state = unsafe { &mut *(*h).context.cast::<Fake>() };
    state.operation = write;
    i32::from(state.mode == "post")
}
unsafe extern "C" fn poll(h: *mut sys::Handles, completion: *mut sys::Completion) -> c_int {
    let state = unsafe { &*(*h).context.cast::<Fake>() };
    if state.mode == "timeout" {
        return 0;
    }
    if state.mode == "poll" {
        return -1;
    }
    unsafe {
        *completion = sys::Completion {
            id: if state.mode == "identity" { 2 } else { 1 },
            opcode: if state.operation == 1 { 1 } else { 2 },
            length: state.length as u32,
            status: u32::from(state.mode == "wc"),
            vendor_error: 0,
        };
    }
    1
}
unsafe fn close(h: *mut sys::Handles, index: usize) -> c_int {
    let handles = unsafe { &mut *h };
    if handles.context.is_null() {
        return 0;
    }
    let state = unsafe { &mut *handles.context.cast::<Fake>() };
    let handle = match index {
        0 => &mut handles.qp,
        1 => &mut handles.mr,
        2 => &mut handles.cq,
        3 => &mut handles.pd,
        _ => &mut handles.context,
    };
    if handle.is_null() {
        return 0;
    }
    assert_eq!(
        state.closed, index,
        "native dependencies must be closed in order"
    );
    if state.mode == format!("close{index}") {
        return 16;
    }
    *handle = std::ptr::null_mut();
    state.closed += 1;
    if index == 4 {
        // Every dependent handle has been cleared; the mock's final owner ends.
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
unsafe extern "C" fn pd(h: *mut sys::Handles) -> c_int {
    unsafe { close(h, 3) }
}
unsafe extern "C" fn ctx(h: *mut sys::Handles) -> c_int {
    unsafe { close(h, 4) }
}
static VERBS: sys::Verbs = sys::Verbs {
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
    Runtime {
        config: RdmaConfig {
            max_endpoints: 1,
            max_staging_bytes: 8,
            ..RdmaConfig::new(mode)
        },
        usage: Arc::default(),
        verbs: &VERBS,
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
        assert!(matches!(runtime.endpoint(0, 0), Err(Error::InvalidRange)));
        assert!(matches!(
            runtime.endpoint(9, 0),
            Err(Error::ResourceExhausted)
        ));
        let result = runtime.endpoint(8, 1).map(|mut endpoint| {
            assert!(matches!(
                runtime.endpoint(1, 0),
                Err(Error::ResourceExhausted)
            ));
            assert!(endpoint.bytes().is_err());
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
            assert_eq!(endpoint.bytes().unwrap(), &[0; 8]);
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
    for index in 0..5 {
        let runtime = runtime(&format!("close{index}"));
        let mut endpoint = runtime.endpoint(8, 2).unwrap();
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
        assert!(matches!(runtime.endpoint(1, 0), Err(Error::Unavailable(_))));
    }
}
