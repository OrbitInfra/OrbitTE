#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_int, c_void};

use rdma_mummy_sys as ffi;

use super::Descriptor;

#[repr(C)]
pub(super) struct Handles {
    pub context: *mut c_void,
    pub pd: *mut c_void,
    pub cq: *mut c_void,
    pub qp: *mut c_void,
    pub mr: *mut c_void,
}

impl Default for Handles {
    fn default() -> Self {
        Self {
            context: std::ptr::null_mut(),
            pd: std::ptr::null_mut(),
            cq: std::ptr::null_mut(),
            qp: std::ptr::null_mut(),
            mr: std::ptr::null_mut(),
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

// The opaque handles belong to Resources, not to a wrapper's infallible Drop.
// Every successful destroy clears exactly one handle; any error leaves all
// remaining handles and the actual DMA backing owned by the caller.
fn failure() -> c_int {
    std::io::Error::last_os_error()
        .raw_os_error()
        .filter(|code| *code != 0)
        .unwrap_or(libc::EIO)
}

unsafe extern "C" fn open(
    handles: *mut Handles,
    name: *const c_char,
    port: u8,
    gid_index: u8,
    buffer: *mut c_void,
    length: usize,
    permission: u32,
    psn: u32,
    local: *mut Descriptor,
) -> c_int {
    // SAFETY: Runtime supplies initialized handles/descriptor and an exclusive,
    // stable allocation. Partial handles remain in Resources for ordered close.
    unsafe {
        let h = &mut *handles;
        let mut count = 0;
        let devices = ffi::ibv_get_device_list(&mut count);
        if devices.is_null() {
            return failure();
        }
        let mut error = libc::ENODEV;
        for index in 0..count {
            let device = *devices.add(index as usize);
            let device_name = ffi::ibv_get_device_name(device);
            if !device_name.is_null() && CStr::from_ptr(name) == CStr::from_ptr(device_name) {
                h.context = ffi::ibv_open_device(device).cast();
                error = if h.context.is_null() { failure() } else { 0 };
                break;
            }
        }
        ffi::ibv_free_device_list(devices);
        if error != 0 {
            return error;
        }
        let mut attr: ffi::ibv_port_attr = std::mem::zeroed();
        let error = ffi::ibv_query_port(h.context.cast(), port, &mut attr);
        if error != 0 {
            return error;
        }
        if attr.state != ffi::ibv_port_state::IBV_PORT_ACTIVE {
            return libc::ENETDOWN;
        }
        let mut gid = ffi::ibv_gid { raw: [0; 16] };
        let error = ffi::ibv_query_gid(h.context.cast(), port, i32::from(gid_index), &mut gid);
        if error != 0 {
            return error;
        }
        if attr.link_layer == 2 && gid.raw == [0; 16] {
            return libc::EADDRNOTAVAIL;
        }
        h.pd = ffi::ibv_alloc_pd(h.context.cast()).cast();
        if h.pd.is_null() {
            return failure();
        }
        h.cq = ffi::ibv_create_cq(
            h.context.cast(),
            4,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
        .cast();
        if h.cq.is_null() {
            return failure();
        }
        let mut access = ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0;
        if permission & 1 != 0 {
            access |= ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0;
        }
        if permission & 2 != 0 {
            access |= ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
        }
        h.mr = ffi::ibv_reg_mr(h.pd.cast(), buffer, length, access as c_int).cast();
        if h.mr.is_null() {
            return failure();
        }
        let mut init: ffi::ibv_qp_init_attr = std::mem::zeroed();
        init.send_cq = h.cq.cast();
        init.recv_cq = h.cq.cast();
        init.qp_type = ffi::ibv_qp_type::IBV_QPT_RC;
        init.sq_sig_all = 1;
        init.cap.max_send_wr = 1;
        init.cap.max_recv_wr = 1;
        init.cap.max_send_sge = 1;
        init.cap.max_recv_sge = 1;
        h.qp = ffi::ibv_create_qp(h.pd.cast(), &mut init).cast();
        if h.qp.is_null() {
            return failure();
        }
        let mut qp_attr: ffi::ibv_qp_attr = std::mem::zeroed();
        qp_attr.qp_state = ffi::ibv_qp_state::IBV_QPS_INIT;
        qp_attr.port_num = port;
        qp_attr.qp_access_flags = access & !ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0;
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT
            | ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        let error = ffi::ibv_modify_qp(h.qp.cast(), &mut qp_attr, mask.0 as c_int);
        if error != 0 {
            return error;
        }
        *local = Descriptor {
            address: buffer as u64,
            rkey: (*h.mr.cast::<ffi::ibv_mr>()).rkey,
            qpn: (*h.qp.cast::<ffi::ibv_qp>()).qp_num,
            psn,
            lid: attr.lid,
            mtu: attr.active_mtu as u8,
            link_layer: attr.link_layer,
            gid: gid.raw,
        };
        0
    }
}

unsafe extern "C" fn connect(
    handles: *mut Handles,
    local: *const Descriptor,
    remote: *const Descriptor,
    port: u8,
    gid_index: u8,
    traffic_class: u8,
    hop_limit: u8,
) -> c_int {
    // SAFETY: Endpoint owns an initialized QP and validates both descriptors
    // before this call. Each mask matches the initialized fields for its state.
    unsafe {
        let h = &mut *handles;
        let local = &*local;
        let remote = &*remote;
        let mut attr: ffi::ibv_qp_attr = std::mem::zeroed();
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = u32::from(local.mtu.min(remote.mtu));
        attr.dest_qp_num = remote.qpn;
        attr.rq_psn = remote.psn;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = 12;
        attr.ah_attr.dlid = remote.lid;
        attr.ah_attr.port_num = port;
        if local.link_layer == 2 || remote.gid != [0; 16] {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid.raw = remote.gid;
            attr.ah_attr.grh.sgid_index = gid_index;
            attr.ah_attr.grh.hop_limit = hop_limit;
            attr.ah_attr.grh.traffic_class = traffic_class;
        }
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_AV
            | ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        let error = ffi::ibv_modify_qp(h.qp.cast(), &mut attr, mask.0 as c_int);
        if error != 0 {
            return error;
        }
        let mut attr: ffi::ibv_qp_attr = std::mem::zeroed();
        attr.qp_state = ffi::ibv_qp_state::IBV_QPS_RTS;
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        attr.sq_psn = local.psn;
        attr.max_rd_atomic = 1;
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE
            | ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        ffi::ibv_modify_qp(h.qp.cast(), &mut attr, mask.0 as c_int)
    }
}

unsafe extern "C" fn post(
    h: *mut Handles,
    address: u64,
    rkey: u32,
    length: u32,
    write: c_int,
) -> c_int {
    // SAFETY: Endpoint holds the MR/QP and exclusive staging until close.
    // libibverbs consumes the stack WR/SGE during post; only MR bytes outlive it.
    unsafe {
        let h = &*h;
        let mr = &*h.mr.cast::<ffi::ibv_mr>();
        let mut sge = ffi::ibv_sge {
            addr: mr.addr as u64,
            length,
            lkey: mr.lkey,
        };
        let mut wr: ffi::ibv_send_wr = std::mem::zeroed();
        wr.wr_id = 1;
        wr.sg_list = &mut sge;
        wr.num_sge = 1;
        wr.opcode = if write != 0 {
            ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE
        } else {
            ffi::ibv_wr_opcode::IBV_WR_RDMA_READ
        };
        wr.send_flags = ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        wr.wr.rdma.remote_addr = address;
        wr.wr.rdma.rkey = rkey;
        let mut bad = std::ptr::null_mut();
        ffi::ibv_post_send(h.qp.cast(), &mut wr, &mut bad)
    }
}

unsafe extern "C" fn poll(h: *mut Handles, result: *mut Completion) -> c_int {
    // SAFETY: Endpoint retains the CQ; one initialized output receives one CQE.
    unsafe {
        let mut completion: ffi::ibv_wc = std::mem::zeroed();
        let count = ffi::ibv_poll_cq((*h).cq.cast(), 1, &mut completion);
        if count == 1 {
            *result = Completion {
                id: completion.wr_id,
                status: completion.status,
                opcode: completion.opcode,
                length: completion.byte_len,
                vendor_error: completion.vendor_err,
            };
        }
        count
    }
}

macro_rules! destroy {
    ($name:ident, $field:ident, $verb:ident) => {
        unsafe extern "C" fn $name(h: *mut Handles) -> c_int {
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
destroy!(destroy_qp, qp, ibv_destroy_qp);
destroy!(deregister, mr, ibv_dereg_mr);
destroy!(destroy_cq, cq, ibv_destroy_cq);
destroy!(dealloc_pd, pd, ibv_dealloc_pd);
destroy!(close_device, context, ibv_close_device);

pub(super) static REAL: Verbs = Verbs {
    open,
    connect,
    post,
    poll,
    destroy_qp,
    deregister,
    destroy_cq,
    dealloc_pd,
    close_device,
};
