//! C records. Integer discriminants remain integers until checked.
use std::ffi::{c_char, c_int, c_void};

pub type Handle = *mut c_void;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Request {
    pub opcode: c_int,
    pub source: *mut c_void,
    pub target_id: u64,
    pub target_offset: u64,
    pub length: u64,
    pub priority: c_int,
    pub transport_hint: c_int,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Status {
    pub status: c_int,
    pub transferred_bytes: u64,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Buffer {
    pub base: u64,
    pub length: u64,
    pub location: [c_char; 64],
}
#[repr(C)]
pub struct SegmentInfo {
    pub kind: c_int,
    pub count: c_int,
    pub buffers: *mut Buffer,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NotifyRecord {
    pub handle: u64,
    pub name: [c_char; 256],
    pub msg: [c_char; 4096],
}
#[repr(C)]
pub struct NotifyInfo {
    pub count: c_int,
    pub records: *mut NotifyRecord,
}
#[repr(C)]
pub struct Options {
    pub location: [c_char; 64],
    pub permission: c_int,
    pub transport_type: c_int,
    pub shm_path: [c_char; 256],
    pub shm_offset: usize,
    pub internal: c_int,
}
#[repr(C)]
pub struct NicStat {
    pub device_name: [c_char; 64],
    pub inflight_bytes: u64,
    pub ewma_bandwidth_bps: f64,
}
