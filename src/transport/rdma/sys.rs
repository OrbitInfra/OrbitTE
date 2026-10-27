//! Fallible native verbs contract; resources stay with the calling Rust owner.

#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_int, c_void};

use ibverbs_sys as ffi;

use super::{
    Descriptor, RdmaConfig,
    discovery::{self, RdmaAddress},
};
use crate::runtime::topology::Topology;
use crate::{Error, Result};

mod cq;
mod device;
mod memory;
mod qp;

use cq::{
    ack_completion_event, arm_completion, create_completion_channel, create_cq,
    get_completion_event, poll,
};
pub(super) use device::devices;
use device::{ack_async_event, get_async_event, open_device, query_address};
use memory::{dealloc_window, prepare_grant, register_memory, window};
use qp::{connect, open, post, post_receive, send_completion};

#[repr(C)]
pub(super) struct DeviceHandles {
    pub context: *mut c_void,
    pub pd: *mut c_void,
    pub async_fd: c_int,
}

impl Default for DeviceHandles {
    fn default() -> Self {
        Self {
            context: std::ptr::null_mut(),
            pd: std::ptr::null_mut(),
            async_fd: -1,
        }
    }
}

// libibverbs contexts and PDs support calls from multiple threads. RdmaContext owns
// these handles behind a mutex and retains them through every child endpoint.
unsafe impl Send for DeviceHandles {}

#[repr(C)]
pub(super) struct ConnectionHandles {
    pub cq: *mut c_void,
    pub receive_cq: *mut c_void,
    pub qp: *mut c_void,
}

impl Default for ConnectionHandles {
    fn default() -> Self {
        Self {
            cq: std::ptr::null_mut(),
            receive_cq: std::ptr::null_mut(),
            qp: std::ptr::null_mut(),
        }
    }
}

#[repr(C)]
pub(super) struct CqHandles {
    pub cq: *mut c_void,
}

impl Default for CqHandles {
    fn default() -> Self {
        Self {
            cq: std::ptr::null_mut(),
        }
    }
}

#[repr(C)]
pub(super) struct ChannelHandles {
    pub channel: *mut c_void,
    pub fd: c_int,
}

impl Default for ChannelHandles {
    fn default() -> Self {
        Self {
            channel: std::ptr::null_mut(),
            fd: -1,
        }
    }
}

// Access is serialized by the completion-channel owner; its consumer is joined first.
unsafe impl Send for ChannelHandles {}

#[derive(Clone, Copy)]
pub(super) struct CqEventVerbs {
    pub create: unsafe extern "C" fn(*mut ChannelHandles, *const DeviceHandles) -> c_int,
    pub get: unsafe extern "C" fn(*mut c_void, *mut *mut c_void) -> c_int,
    pub ack: unsafe extern "C" fn(*mut c_void),
    pub arm: unsafe extern "C" fn(*mut c_void) -> c_int,
    pub close: unsafe extern "C" fn(*mut ChannelHandles) -> c_int,
}

#[repr(C)]
pub(super) struct MemoryHandles {
    pub mr: *mut c_void,
    pub mw: *mut c_void,
    pub window_key: u32,
}

impl Default for MemoryHandles {
    fn default() -> Self {
        Self {
            mr: std::ptr::null_mut(),
            mw: std::ptr::null_mut(),
            window_key: 0,
        }
    }
}

#[derive(Default)]
#[repr(C)]
pub(super) struct Completion {
    pub id: u64,
    pub status: u32,
    pub opcode: u32,
    pub length: u32,
    pub vendor_error: u32,
    pub qpn: u32,
    pub flags: u32,
    pub invalidated_rkey: u32,
}

pub(super) const WITH_INVALIDATE: u32 = ffi::ibv_wc_flags::IBV_WC_WITH_INV.0;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub(super) struct WorkRequest {
    pub id: u64,
    pub offset: u32,
    pub length: u32,
}

#[derive(Default)]
#[repr(C)]
pub(super) struct PostResult {
    pub accepted: usize,
    pub uncertain: bool,
}

pub(super) struct AsyncEvent {
    pub kind: u32,
    pub port: u8,
    native: std::mem::MaybeUninit<ffi::ibv_async_event>,
}

impl Default for AsyncEvent {
    fn default() -> Self {
        Self {
            kind: 0,
            port: 0,
            native: std::mem::MaybeUninit::uninit(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct AsyncVerbs {
    pub get: unsafe extern "C" fn(*mut c_void, *mut AsyncEvent) -> c_int,
    pub ack: unsafe extern "C" fn(*mut c_void, *mut AsyncEvent),
}

type Discover = fn(&RdmaConfig) -> Result<(Topology, Vec<RdmaAddress>)>;
type CloseConnection = unsafe extern "C" fn(*mut ConnectionHandles) -> c_int;
type CloseMemory = unsafe extern "C" fn(*mut MemoryHandles) -> c_int;
pub(super) struct Verbs {
    pub asynchronous: Option<AsyncVerbs>,
    pub completion: Option<CqEventVerbs>,
    pub discover: Discover,
    pub query_address:
        unsafe fn(*const DeviceHandles, &RdmaAddress, Option<u8>) -> Result<RdmaAddress>,
    pub open_device: unsafe extern "C" fn(*mut DeviceHandles, *const c_char) -> c_int,
    pub create_cq:
        unsafe extern "C" fn(*mut CqHandles, *const DeviceHandles, u32, *mut c_void) -> c_int,
    pub register_memory: unsafe extern "C" fn(
        *mut MemoryHandles,
        *const DeviceHandles,
        *mut c_void,
        usize,
        bool,
    ) -> c_int,
    pub prepare_grant: unsafe extern "C" fn(
        *mut ConnectionHandles,
        *mut MemoryHandles,
        *const DeviceHandles,
        *mut c_void,
        usize,
        u32,
    ) -> c_int,
    pub open: unsafe extern "C" fn(
        *mut ConnectionHandles,
        *mut MemoryHandles,
        *const DeviceHandles,
        u8,
        u8,
        *const Descriptor,
        *mut c_void,
        usize,
        u32,
        u32,
        u32,
        u32,
        *mut Descriptor,
    ) -> c_int,
    pub connect: unsafe extern "C" fn(
        *mut ConnectionHandles,
        *const Descriptor,
        *const Descriptor,
        u8,
        u8,
        u8,
        u8,
    ) -> c_int,
    pub post: unsafe extern "C" fn(
        *mut ConnectionHandles,
        *const MemoryHandles,
        *const WorkRequest,
        usize,
        u64,
        usize,
        u64,
        u32,
        c_int,
        *mut PostResult,
    ) -> c_int,
    pub window: unsafe extern "C" fn(
        *mut ConnectionHandles,
        *mut MemoryHandles,
        u64,
        u64,
        usize,
        u32,
        bool,
        *mut PostResult,
    ) -> c_int,
    pub post_receive: unsafe extern "C" fn(
        *mut ConnectionHandles,
        *const MemoryHandles,
        u64,
        u64,
        usize,
        *mut PostResult,
    ) -> c_int,
    pub send_completion: unsafe extern "C" fn(
        *mut ConnectionHandles,
        *const MemoryHandles,
        u64,
        u64,
        usize,
        u32,
        *mut PostResult,
    ) -> c_int,
    pub dealloc_window: CloseMemory,
    pub poll: unsafe extern "C" fn(*mut ConnectionHandles, *mut Completion) -> c_int,
    pub destroy_qp: CloseConnection,
    pub deregister: CloseMemory,
    pub destroy_cq: unsafe extern "C" fn(*mut CqHandles) -> c_int,
    pub dealloc_pd: unsafe extern "C" fn(*mut DeviceHandles) -> c_int,
    pub close_device: unsafe extern "C" fn(*mut DeviceHandles) -> c_int,
}

// The opaque handles belong to Resources, not to a wrapper's infallible Drop.
// Every successful destroy clears exactly one handle; any error leaves all
// remaining handles and the actual DMA backing owned by the caller.
fn failure() -> c_int {
    std::io::Error::last_os_error()
        .raw_os_error()
        .filter(|code| *code != 0)
        .unwrap_or(libc::EIO)
}

macro_rules! destroy {
    ($name:ident, $owner:ident, $field:ident, $verb:ident) => {
        unsafe extern "C" fn $name(h: *mut $owner) -> c_int {
            // SAFETY: Resources owns every handle; clearing follows native
            // success only, and the caller stops before destroying dependencies.
            unsafe {
                let handle = &mut (*h).$field;
                if handle.is_null() {
                    return 0;
                }
                let error = ffi::$verb((*handle).cast());
                if error == 0 {
                    *handle = std::ptr::null_mut();
                }
                error
            }
        }
    };
}
destroy!(destroy_qp, ConnectionHandles, qp, ibv_destroy_qp);
destroy!(deregister, MemoryHandles, mr, ibv_dereg_mr);
destroy!(destroy_cq, CqHandles, cq, ibv_destroy_cq);
destroy!(
    close_completion_channel,
    ChannelHandles,
    channel,
    ibv_destroy_comp_channel
);
destroy!(dealloc_pd, DeviceHandles, pd, ibv_dealloc_pd);
destroy!(close_device, DeviceHandles, context, ibv_close_device);

pub(super) static REAL: Verbs = Verbs {
    discover: discovery::discover,
    query_address,
    completion: Some(CqEventVerbs {
        create: create_completion_channel,
        get: get_completion_event,
        ack: ack_completion_event,
        arm: arm_completion,
        close: close_completion_channel,
    }),
    asynchronous: Some(AsyncVerbs {
        get: get_async_event,
        ack: ack_async_event,
    }),
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
    destroy_qp,
    deregister,
    destroy_cq,
    dealloc_pd,
    close_device,
};

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/sys.rs"]
mod tests;
