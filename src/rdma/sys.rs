#![allow(unsafe_code)]

use std::ffi::{c_char, c_int, c_void};

use super::Descriptor;

#[derive(Default)]
#[repr(C)]
pub(super) struct Handles {
    pub context: *mut c_void,
    pub pd: *mut c_void,
    pub cq: *mut c_void,
    pub qp: *mut c_void,
    pub mr: *mut c_void,
}

#[derive(Default)]
#[repr(C)]
pub(super) struct Completion {
    pub id: u64,
    pub status: u32,
    pub opcode: u32,
    pub length: u32,
    pub vendor_error: u32,
}

type Close = unsafe extern "C" fn(*mut Handles) -> c_int;
pub(super) struct Verbs {
    pub open: unsafe extern "C" fn(
        *mut Handles,
        *const c_char,
        u8,
        u8,
        *mut c_void,
        usize,
        u32,
        u32,
        *mut Descriptor,
    ) -> c_int,
    pub connect: unsafe extern "C" fn(
        *mut Handles,
        *const Descriptor,
        *const Descriptor,
        u8,
        u8,
        u8,
        u8,
    ) -> c_int,
    pub post: unsafe extern "C" fn(*mut Handles, u64, u32, u32, c_int) -> c_int,
    pub poll: unsafe extern "C" fn(*mut Handles, *mut Completion) -> c_int,
    pub destroy_qp: Close,
    pub deregister: Close,
    pub destroy_cq: Close,
    pub dealloc_pd: Close,
    pub close_device: Close,
}

unsafe extern "C" {
    fn ote_verbs_open(
        h: *mut Handles,
        name: *const c_char,
        port: u8,
        gid: u8,
        buffer: *mut c_void,
        length: usize,
        permission: u32,
        psn: u32,
        local: *mut Descriptor,
    ) -> c_int;
    fn ote_verbs_connect(
        h: *mut Handles,
        local: *const Descriptor,
        remote: *const Descriptor,
        port: u8,
        gid: u8,
        traffic_class: u8,
        hop_limit: u8,
    ) -> c_int;
    fn ote_verbs_post(h: *mut Handles, address: u64, rkey: u32, length: u32, write: c_int)
    -> c_int;
    fn ote_verbs_poll(h: *mut Handles, completion: *mut Completion) -> c_int;
    fn ote_verbs_destroy_qp(h: *mut Handles) -> c_int;
    fn ote_verbs_deregister(h: *mut Handles) -> c_int;
    fn ote_verbs_destroy_cq(h: *mut Handles) -> c_int;
    fn ote_verbs_dealloc_pd(h: *mut Handles) -> c_int;
    fn ote_verbs_close_device(h: *mut Handles) -> c_int;
}

pub(super) static REAL: Verbs = Verbs {
    open: ote_verbs_open,
    connect: ote_verbs_connect,
    post: ote_verbs_post,
    poll: ote_verbs_poll,
    destroy_qp: ote_verbs_destroy_qp,
    deregister: ote_verbs_deregister,
    destroy_cq: ote_verbs_destroy_cq,
    dealloc_pd: ote_verbs_dealloc_pd,
    close_device: ote_verbs_close_device,
};
