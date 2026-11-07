//! Internal Python request/status values. Discriminants remain integers until checked.
use std::ffi::{c_char, c_int, c_void};

pub type Handle = usize;

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
#[derive(Clone, Copy)]
pub struct Status {
    pub status: c_int,
    pub transferred_bytes: u64,
}
#[derive(Clone, Copy)]
pub struct Buffer {
    pub base: u64,
    pub length: u64,
    pub location: [c_char; 64],
}
