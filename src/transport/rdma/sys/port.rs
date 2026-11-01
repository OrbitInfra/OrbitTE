//! Public libibverbs port-query ABI and optional effective-rate symbol.

use std::sync::OnceLock;

use libloading::Library;

use super::{DeviceHandles, c_int, ffi};
use crate::{Error, Result};

mod shim {
    #![allow(non_camel_case_types)]
    include!(concat!(env!("OUT_DIR"), "/port.rs"));
}

type QueryPortSpeed = unsafe extern "C" fn(*mut ffi::ibv_context, u32, *mut u64) -> c_int;

pub(super) unsafe fn query_port(
    context: *mut ffi::ibv_context,
    port: u8,
) -> std::result::Result<shim::orbitte_port_attr, c_int> {
    let mut attr = shim::orbitte_port_attr::default();
    // The C compiler uses the system's verbs inline and provider layout. Only
    // our generated, fixed-width result crosses back into Rust.
    let code = unsafe { shim::orbitte_query_port(context.cast(), port, &mut attr) };
    if code == 0 { Ok(attr) } else { Err(code) }
}

pub(super) fn link_speed_mbps(attr: &shim::orbitte_port_attr) -> Option<u64> {
    let speed = attr.active_speed;
    let lane = match speed {
        1 => 2_500,
        2 => 5_000,
        4 | 8 => 10_000,
        16 => 14_000,
        32 => 25_000,
        64 => 50_000,
        128 => 100_000,
        256 => 200_000,
        _ => return None,
    };
    let lanes = match attr.active_width {
        1 => 1,
        2 => 4,
        4 => 8,
        8 => 12,
        16 => 2,
        _ => return None,
    };
    Some(lane * lanes)
}

struct PortSpeedQuery {
    query: QueryPortSpeed,
    _library: Library,
}

impl PortSpeedQuery {
    fn from_library(library: Library) -> Option<Self> {
        // Optional since rdma-core 62. Keep the library alive with its function.
        let query = *unsafe { library.get::<QueryPortSpeed>(b"ibv_query_port_speed\0") }.ok()?;
        Some(Self {
            query,
            _library: library,
        })
    }
}

pub(super) unsafe fn query_port_speed(
    device: *const DeviceHandles,
    port: u8,
) -> Result<Option<u64>> {
    static QUERY: OnceLock<Option<PortSpeedQuery>> = OnceLock::new();
    let query = QUERY.get_or_init(|| {
        let library = unsafe { Library::new("libibverbs.so.1") }.ok()?;
        PortSpeedQuery::from_library(library)
    });
    match query {
        Some(query) => unsafe {
            effective_speed(query.query, (*device).context.cast(), port).map(Some)
        },
        None => Ok(None),
    }
}

unsafe fn effective_speed(
    query: QueryPortSpeed,
    context: *mut ffi::ibv_context,
    port: u8,
) -> Result<u64> {
    let mut speed = 0;
    let code = unsafe { query(context, u32::from(port), &mut speed) };
    if code != 0 {
        return Err(Error::Backend {
            operation: "ibv_query_port_speed",
            code,
        });
    }
    // The native result is in 100 Mb/s units, not Mb/s or bytes/s.
    speed.checked_mul(100).ok_or(Error::Backend {
        operation: "ibv_query_port_speed overflow",
        code: libc::EOVERFLOW,
    })
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/sys/port.rs"]
mod tests;
