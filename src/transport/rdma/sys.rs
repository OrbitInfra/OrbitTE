#![allow(unsafe_code)]

use std::ffi::{CStr, c_char, c_int, c_void};

use ibverbs_sys as ffi;

use super::{
    Descriptor, RdmaConfig,
    discovery::{self, RdmaAddress},
};
use crate::runtime::topology::Topology;
use crate::{Error, Result};

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
    native: std::mem::MaybeUninit<ffi::ibv_async_event>,
}

impl Default for AsyncEvent {
    fn default() -> Self {
        Self {
            kind: 0,
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
    pub discover: Discover,
    pub query_address: unsafe fn(*const DeviceHandles, &RdmaAddress) -> Result<RdmaAddress>,
    pub open_device: unsafe extern "C" fn(*mut DeviceHandles, *const c_char) -> c_int,
    pub create_cq: unsafe extern "C" fn(*mut ConnectionHandles, *const DeviceHandles, u32) -> c_int,
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
    pub destroy_cq: CloseConnection,
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

pub(super) fn devices() -> Result<Vec<(String, String)>> {
    // This inventory operation never opens a provider context.
    unsafe {
        let mut count = 0;
        let list = ffi::ibv_get_device_list(&mut count);
        if list.is_null() {
            return Err(Error::Backend {
                operation: "ibv_get_device_list",
                code: failure(),
            });
        }
        let mut result = Vec::new();
        for index in 0..count {
            let device = *list.add(index as usize);
            let name = ffi::ibv_get_device_name(device);
            if name.is_null() {
                continue;
            }
            let node: Vec<_> = (*device)
                .dev_name
                .iter()
                .take_while(|ch| **ch != 0)
                .map(|ch| *ch as u8)
                .collect();
            if let (Ok(name), Ok(node)) = (CStr::from_ptr(name).to_str(), String::from_utf8(node)) {
                if !node.is_empty() && !node.contains('/') {
                    result.push((name.to_owned(), node));
                }
            }
        }
        ffi::ibv_free_device_list(list);
        Ok(result)
    }
}

unsafe fn query_descriptor(
    device: *const DeviceHandles,
    port: u8,
    gid_index: u8,
) -> std::result::Result<Descriptor, c_int> {
    unsafe {
        let mut attr: ffi::ibv_port_attr = std::mem::zeroed();
        // All consumed fields are in the published binding's legacy output prefix.
        let code = ffi::ibv_query_port(
            (*device).context.cast(),
            port,
            (&mut attr as *mut ffi::ibv_port_attr).cast(),
        );
        if code != 0 {
            return Err(code);
        }
        if attr.state != ffi::ibv_port_state::IBV_PORT_ACTIVE {
            return Err(libc::ENETDOWN);
        }
        if !matches!(attr.link_layer, 1 | 2)
            || !(1..=5).contains(&(attr.active_mtu as u8))
            || i32::from(gid_index) >= attr.gid_tbl_len
        {
            return Err(libc::EADDRNOTAVAIL);
        }
        let mut gid = ffi::ibv_gid { raw: [0; 16] };
        let code = ffi::ibv_query_gid(
            (*device).context.cast(),
            port,
            i32::from(gid_index),
            &mut gid,
        );
        if code != 0 {
            return Err(code);
        }
        if gid.raw == [0; 16] || std::net::Ipv6Addr::from(gid.raw).is_multicast() {
            return Err(libc::EADDRNOTAVAIL);
        }
        Ok(Descriptor {
            lid: attr.lid,
            mtu: attr.active_mtu as u8,
            link_layer: attr.link_layer,
            gid: gid.raw,
            ..Descriptor::default()
        })
    }
}

fn validate_gid_entry(entry: &ffi::ibv_gid_entry, address: &RdmaAddress) -> Result<()> {
    let (kind, link_layer) = match address.gid_type {
        discovery::GidType::Ib => (ffi::IBV_GID_TYPE_IB, 1),
        discovery::GidType::RoceV1 => (ffi::IBV_GID_TYPE_ROCE_V1, 2),
        discovery::GidType::RoceV2 => (ffi::IBV_GID_TYPE_ROCE_V2, 2),
    };
    if entry.gid_index != u32::from(address.gid_index)
        || entry.port_num != u32::from(address.port)
        || unsafe { entry.gid.raw } != address.descriptor.gid
        || entry.gid_type != kind
        || address.descriptor.link_layer != link_layer
        || entry.ndev_ifindex != address.net_device_index
    {
        return Err(Error::Backend {
            operation: "RDMA route query changed",
            code: libc::ESTALE,
        });
    }
    Ok(())
}

unsafe fn query_address(
    device: *const DeviceHandles,
    expected: &RdmaAddress,
) -> Result<RdmaAddress> {
    let descriptor = unsafe { query_descriptor(device, expected.port, expected.gid_index) }
        .map_err(|code| Error::Backend {
            operation: "ibverbs route query",
            code,
        })?;
    let mut entry: ffi::ibv_gid_entry = unsafe { std::mem::zeroed() };
    let code = unsafe {
        ffi::_ibv_query_gid_ex(
            (*device).context.cast(),
            u32::from(expected.port),
            u32::from(expected.gid_index),
            &mut entry,
            0,
            std::mem::size_of::<ffi::ibv_gid_entry>(),
        )
    };
    super::check("ibv_query_gid_ex", code)?;
    let (gid_type, net_device, net_device_index) = discovery::gid_metadata(
        std::path::Path::new("/sys"),
        &expected.device,
        expected.port,
        expected.gid_index,
        descriptor.link_layer,
    )?;
    let observed = RdmaAddress {
        descriptor,
        gid_type,
        net_device,
        net_device_index,
        ..expected.clone()
    };
    validate_gid_entry(&entry, &observed)?;
    if entry.ndev_ifindex != 0 {
        let mut name = [0; libc::IF_NAMESIZE];
        let value = unsafe { libc::if_indextoname(entry.ndev_ifindex, name.as_mut_ptr()) };
        if value.is_null() {
            return Err(Error::Backend {
                operation: "if_indextoname",
                code: failure(),
            });
        }
        if unsafe { CStr::from_ptr(value) }.to_str().ok() != observed.net_device.as_deref() {
            return Err(Error::Backend {
                operation: "RDMA network device changed",
                code: libc::ESTALE,
            });
        }
    }
    Ok(observed)
}

fn endpoint_address(
    observed: std::result::Result<Descriptor, c_int>,
    expected: &Descriptor,
) -> std::result::Result<Descriptor, c_int> {
    match observed {
        Ok(address) if address == *expected => Ok(address),
        _ => Err(libc::ESTALE),
    }
}

unsafe extern "C" fn open_device(handles: *mut DeviceHandles, name: *const c_char) -> c_int {
    // SAFETY: RdmaContext owns initialized handles, including partial construction.
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
        h.async_fd = (*h.context.cast::<ffi::ibv_context>()).async_fd;
        let flags = libc::fcntl(h.async_fd, libc::F_GETFL);
        if flags == -1 || libc::fcntl(h.async_fd, libc::F_SETFL, flags | libc::O_NONBLOCK) == -1 {
            return failure();
        }
        h.pd = ffi::ibv_alloc_pd(h.context.cast()).cast();
        if h.pd.is_null() {
            return failure();
        }
        0
    }
}

unsafe extern "C" fn create_cq(
    handles: *mut ConnectionHandles,
    device: *const DeviceHandles,
    capacity: u32,
) -> c_int {
    // SAFETY: the progress owner retains this CQ and its actual RdmaContext/PD.
    unsafe {
        (*handles).cq = ffi::ibv_create_cq(
            (*device).context.cast(),
            capacity as c_int,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
        .cast();
        if (*handles).cq.is_null() {
            failure()
        } else if (*(*handles).cq.cast::<ffi::ibv_cq>()).cqe < capacity as c_int {
            libc::EOPNOTSUPP
        } else {
            0
        }
    }
}

unsafe extern "C" fn register_memory(
    handles: *mut MemoryHandles,
    device: *const DeviceHandles,
    buffer: *mut c_void,
    length: usize,
    window_bind: bool,
) -> c_int {
    // SAFETY: the MR owner retains the device and entire host registration.
    // Window-bind capability never grants access through the parent MR rkey.
    unsafe {
        (*handles).mr = ffi::ibv_reg_mr(
            (*device).pd.cast(),
            buffer,
            length,
            (ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
                | if window_bind {
                    ffi::ibv_access_flags::IBV_ACCESS_MW_BIND.0
                } else {
                    0
                }) as c_int,
        )
        .cast();
        if (*handles).mr.is_null() {
            failure()
        } else {
            0
        }
    }
}

unsafe extern "C" fn prepare_grant(
    handles: *mut ConnectionHandles,
    memory: *mut MemoryHandles,
    device: *const DeviceHandles,
    buffer: *mut c_void,
    length: usize,
    permission: u32,
) -> c_int {
    // SAFETY: one exclusive grant claim owns these bytes and the actual PD/QP.
    // This never resets/reconnects an existing QP or publishes its parent key.
    unsafe {
        let memory = &mut *memory;
        let device = &*device;
        if !(*handles).qp.is_null() && (*(*handles).qp.cast::<ffi::ibv_qp>()).pd != device.pd.cast()
        {
            return libc::EINVAL;
        }
        let access = ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
            | if permission != 0 {
                ffi::ibv_access_flags::IBV_ACCESS_MW_BIND.0
            } else {
                0
            };
        if memory.mr.is_null() {
            memory.mr = ffi::ibv_reg_mr(device.pd.cast(), buffer, length, access as c_int).cast();
            if memory.mr.is_null() {
                return failure();
            }
        } else {
            let mr = &*memory.mr.cast::<ffi::ibv_mr>();
            if mr.pd != device.pd.cast()
                || (buffer as usize)
                    .checked_sub(mr.addr as usize)
                    .and_then(|offset| offset.checked_add(length))
                    .is_none_or(|end| end > mr.length)
            {
                return libc::EINVAL;
            }
        }
        if permission != 0 {
            let mut caps: ffi::ibv_device_attr = std::mem::zeroed();
            let code = ffi::ibv_query_device(device.context.cast(), &mut caps);
            if code != 0 {
                return code;
            }
            if caps.device_cap_flags & ffi::IBV_DEVICE_MEM_WINDOW_TYPE_2B == 0 {
                return libc::EOPNOTSUPP;
            }
            let context = &*device.context.cast::<ffi::ibv_context>();
            let Some(allocate) = context.ops.alloc_mw else {
                return libc::EOPNOTSUPP;
            };
            memory.mw = allocate(device.pd.cast(), ffi::IBV_MW_TYPE_2).cast();
            if memory.mw.is_null() {
                return failure();
            }
            let key = (*memory.mw.cast::<ffi::ibv_mw>()).rkey;
            // RdmaContext selects and permanently burns a fresh full key in its PD.
            memory.window_key = key;
        }
        0
    }
}

unsafe extern "C" fn open(
    handles: *mut ConnectionHandles,
    memory: *mut MemoryHandles,
    device: *const DeviceHandles,
    port: u8,
    gid_index: u8,
    expected: *const Descriptor,
    buffer: *mut c_void,
    length: usize,
    permission: u32,
    psn: u32,
    max_send_wr: u32,
    local: *mut Descriptor,
) -> c_int {
    // SAFETY: the context owns a frozen route; query before constructing children.
    unsafe {
        let h = &mut *handles;
        let memory = &mut *memory;
        let descriptor =
            match endpoint_address(query_descriptor(device, port, gid_index), &*expected) {
                Ok(address) => address,
                Err(code) => return code,
            };
        let device = &*device;
        if h.cq.is_null() || h.receive_cq.is_null() {
            return libc::EINVAL;
        }
        let mut remote_access = 0;
        if permission & 1 != 0 {
            remote_access |= ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0;
        }
        if permission & 2 != 0 {
            remote_access |= ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
        }
        let code = prepare_grant(handles, memory, device, buffer, length, permission);
        if code != 0 {
            return code;
        }
        let mut init: ffi::ibv_qp_init_attr = std::mem::zeroed();
        init.send_cq = h.cq.cast();
        init.recv_cq = h.receive_cq.cast();
        init.qp_type = ffi::ibv_qp_type::IBV_QPT_RC;
        init.sq_sig_all = 1;
        init.cap.max_send_wr = max_send_wr;
        init.cap.max_recv_wr = 1;
        init.cap.max_send_sge = 1;
        init.cap.max_recv_sge = 1;
        h.qp = ffi::ibv_create_qp(device.pd.cast(), &mut init).cast();
        if h.qp.is_null() {
            return failure();
        }
        if init.cap.max_send_wr < max_send_wr
            || init.cap.max_send_sge < 1
            || init.cap.max_recv_wr < 1
            || init.cap.max_recv_sge < 1
        {
            return libc::EOPNOTSUPP;
        }
        let mut qp_attr: ffi::ibv_qp_attr = std::mem::zeroed();
        qp_attr.qp_state = ffi::ibv_qp_state::IBV_QPS_INIT;
        qp_attr.port_num = port;
        qp_attr.qp_access_flags = remote_access;
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
            rkey: if permission != 0 {
                memory.window_key
            } else {
                (*memory.mr.cast::<ffi::ibv_mr>()).rkey
            },
            qpn: (*h.qp.cast::<ffi::ibv_qp>()).qp_num,
            psn,
            ..descriptor
        };
        0
    }
}

unsafe extern "C" fn connect(
    handles: *mut ConnectionHandles,
    local: *const Descriptor,
    remote: *const Descriptor,
    port: u8,
    gid_index: u8,
    traffic_class: u8,
    hop_limit: u8,
) -> c_int {
    // SAFETY: RdmaEndpoint owns an initialized QP and validates both descriptors
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

unsafe extern "C" fn window(
    handles: *mut ConnectionHandles,
    memory: *mut MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    permission: u32,
    bind: bool,
    result: *mut PostResult,
) -> c_int {
    // The record retains the MW, parent MR and exact DMA loan through CQ or teardown.
    unsafe {
        *result = PostResult::default();
        let h = &mut *handles;
        let memory = &mut *memory;
        if h.qp.is_null() || memory.mw.is_null() || memory.mr.is_null() || id == 0 {
            return libc::EINVAL;
        }
        let qp = h.qp.cast::<ffi::ibv_qp>();
        let Some(post) = (*(*qp).context).ops.post_send else {
            return libc::EOPNOTSUPP;
        };
        let mut request: ffi::ibv_send_wr = std::mem::zeroed();
        request.wr_id = id;
        request.send_flags = ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        if bind {
            request.opcode = ffi::ibv_wr_opcode::IBV_WR_BIND_MW;
            let mut access = 0;
            if permission & 1 != 0 {
                access |= ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0;
            }
            if permission & 2 != 0 {
                access |= ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
            }
            request.__bindgen_anon_2.bind_mw.mw = memory.mw.cast();
            request.__bindgen_anon_2.bind_mw.rkey = memory.window_key;
            request.__bindgen_anon_2.bind_mw.bind_info = ffi::ibv_mw_bind_info {
                mr: memory.mr.cast(),
                addr: address,
                length: length as u64,
                mw_access_flags: access,
            };
        } else {
            request.opcode = ffi::ibv_wr_opcode::IBV_WR_LOCAL_INV;
            request.__bindgen_anon_1.invalidate_rkey = memory.window_key;
        }
        let mut bad = std::ptr::null_mut();
        let code = post(qp, &mut request, &mut bad);
        if code == 0 {
            (*result).accepted = 1;
        } else if !std::ptr::eq(bad, &request) {
            (*result).uncertain = true;
        }
        code
    }
}

unsafe extern "C" fn dealloc_window(handles: *mut MemoryHandles) -> c_int {
    unsafe {
        let h = &mut *handles;
        if h.mw.is_null() {
            return 0;
        }
        let mw = h.mw.cast::<ffi::ibv_mw>();
        let Some(close) = (*(*mw).context).ops.dealloc_mw else {
            return libc::EOPNOTSUPP;
        };
        let code = close(mw);
        if code == 0 {
            h.mw = std::ptr::null_mut();
        }
        code
    }
}

unsafe extern "C" fn post(
    h: *mut ConnectionHandles,
    memory: *const MemoryHandles,
    requests: *const WorkRequest,
    count: usize,
    local_address: u64,
    local_length: usize,
    address: u64,
    rkey: u32,
    write: c_int,
    result: *mut PostResult,
) -> c_int {
    // SAFETY: RdmaEndpoint owns the MR/QP and bytes until successful teardown. The
    // provider consumes these WR/SGE descriptors synchronously during post.
    unsafe {
        *result = PostResult::default();
        if count == 0 || count > 64 {
            return libc::EINVAL;
        }
        let h = &*h;
        let qp = h.qp.cast::<ffi::ibv_qp>();
        let Some(post_send) = (*(*qp).context).ops.post_send else {
            return libc::EOPNOTSUPP;
        };
        let mr = &*(*memory).mr.cast::<ffi::ibv_mr>();
        let Some(local_offset) = local_address.checked_sub(mr.addr as u64) else {
            return libc::EINVAL;
        };
        if local_length == 0
            || local_address.checked_add(local_length as u64).is_none()
            || local_offset
                .checked_add(local_length as u64)
                .is_none_or(|end| end > mr.length as u64)
        {
            return libc::EINVAL;
        }
        let requests = std::slice::from_raw_parts(requests, count);
        let mut sges = Vec::new();
        let mut wrs = Vec::new();
        if sges.try_reserve_exact(count).is_err() || wrs.try_reserve_exact(count).is_err() {
            return libc::ENOMEM;
        }
        for request in requests {
            if request.length == 0
                || (request.offset as usize)
                    .checked_add(request.length as usize)
                    .is_none_or(|end| end > local_length)
            {
                return libc::EINVAL;
            }
            let Some(local) = local_address.checked_add(u64::from(request.offset)) else {
                return libc::EINVAL;
            };
            let Some(remote) = address.checked_add(u64::from(request.offset)) else {
                return libc::EINVAL;
            };
            if remote.checked_add(u64::from(request.length)).is_none() {
                return libc::EINVAL;
            }
            sges.push(ffi::ibv_sge {
                addr: local,
                length: request.length,
                lkey: mr.lkey,
            });
            let mut wr: ffi::ibv_send_wr = std::mem::zeroed();
            wr.wr_id = request.id;
            wr.num_sge = 1;
            wr.opcode = if write != 0 {
                ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE
            } else {
                ffi::ibv_wr_opcode::IBV_WR_RDMA_READ
            };
            wr.send_flags = ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
            wr.wr.rdma.remote_addr = remote;
            wr.wr.rdma.rkey = rkey;
            wrs.push(wr);
        }
        let sges_ptr = sges.as_mut_ptr();
        let wrs_ptr = wrs.as_mut_ptr();
        for index in 0..count {
            let wr = wrs_ptr.add(index);
            (*wr).sg_list = sges_ptr.add(index);
            (*wr).next = if index + 1 == count {
                std::ptr::null_mut()
            } else {
                wrs_ptr.add(index + 1)
            };
        }
        let mut bad = std::ptr::null_mut();
        let code = post_send(qp, wrs_ptr, &mut bad);
        if code == 0 {
            (*result).accepted = count;
        } else if let Some(index) = (0..count).find(|index| std::ptr::eq(wrs_ptr.add(*index), bad))
        {
            (*result).accepted = index;
        } else {
            // A missing/foreign bad_wr cannot prove an unposted suffix. Do not
            // dereference it or subtract it from an unrelated allocation.
            (*result).uncertain = true;
        }
        code
    }
}

fn message_sge(
    memory: &MemoryHandles,
    address: u64,
    length: usize,
) -> std::result::Result<ffi::ibv_sge, c_int> {
    if memory.mr.is_null() || length != super::COMPLETION_MESSAGE_SIZE {
        return Err(libc::EINVAL);
    }
    let mr = unsafe { &*memory.mr.cast::<ffi::ibv_mr>() };
    let offset = address.checked_sub(mr.addr as u64).ok_or(libc::EINVAL)?;
    if offset
        .checked_add(length as u64)
        .is_none_or(|end| end > mr.length as u64)
    {
        return Err(libc::EINVAL);
    }
    Ok(ffi::ibv_sge {
        addr: address,
        length: length as u32,
        lkey: mr.lkey,
    })
}

unsafe extern "C" fn post_receive(
    handles: *mut ConnectionHandles,
    memory: *const MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    result: *mut PostResult,
) -> c_int {
    unsafe {
        *result = PostResult::default();
        if (*handles).qp.is_null() || id == 0 {
            return libc::EINVAL;
        }
        let mut sge = match message_sge(&*memory, address, length) {
            Ok(sge) => sge,
            Err(code) => return code,
        };
        let qp = (*handles).qp.cast::<ffi::ibv_qp>();
        let Some(post) = (*(*qp).context).ops.post_recv else {
            return libc::EOPNOTSUPP;
        };
        let mut wr: ffi::ibv_recv_wr = std::mem::zeroed();
        wr.wr_id = id;
        wr.sg_list = &mut sge;
        wr.num_sge = 1;
        let mut bad = std::ptr::null_mut();
        let code = post(qp, &mut wr, &mut bad);
        if code == 0 {
            (*result).accepted = 1;
        } else if !std::ptr::eq(bad, &wr) {
            (*result).uncertain = true;
        }
        code
    }
}

unsafe extern "C" fn send_completion(
    handles: *mut ConnectionHandles,
    memory: *const MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    rkey: u32,
    result: *mut PostResult,
) -> c_int {
    unsafe {
        *result = PostResult::default();
        if (*handles).qp.is_null() || id == 0 {
            return libc::EINVAL;
        }
        let mut sge = match message_sge(&*memory, address, length) {
            Ok(sge) => sge,
            Err(code) => return code,
        };
        let qp = (*handles).qp.cast::<ffi::ibv_qp>();
        let Some(post) = (*(*qp).context).ops.post_send else {
            return libc::EOPNOTSUPP;
        };
        let mut wr: ffi::ibv_send_wr = std::mem::zeroed();
        wr.wr_id = id;
        wr.sg_list = &mut sge;
        wr.num_sge = 1;
        wr.opcode = ffi::ibv_wr_opcode::IBV_WR_SEND_WITH_INV;
        wr.send_flags =
            ffi::ibv_send_flags::IBV_SEND_FENCE.0 | ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        wr.__bindgen_anon_1.invalidate_rkey = rkey;
        let mut bad = std::ptr::null_mut();
        let code = post(qp, &mut wr, &mut bad);
        if code == 0 {
            (*result).accepted = 1;
        } else if !std::ptr::eq(bad, &wr) {
            (*result).uncertain = true;
        }
        code
    }
}

unsafe extern "C" fn poll(h: *mut ConnectionHandles, result: *mut Completion) -> c_int {
    // SAFETY: RdmaEndpoint retains the CQ; one initialized output receives one CQE.
    unsafe {
        let cq = (*h).cq.cast::<ffi::ibv_cq>();
        let Some(poll_cq) = (*(*cq).context).ops.poll_cq else {
            return -libc::EOPNOTSUPP;
        };
        let mut completion = ffi::ibv_wc::default();
        let count = poll_cq(cq, 1, &mut completion);
        if count == 1 {
            let mut output = Completion {
                id: completion.wr_id(),
                qpn: completion.qp_num,
                ..Completion::default()
            };
            if let Some((status, vendor_error)) = completion.error() {
                output.status = status;
                output.vendor_error = vendor_error;
            } else {
                output.opcode = completion.opcode();
                output.length = completion.len() as u32;
                output.flags = completion.wc_flags.0;
                if output.flags & WITH_INVALIDATE != 0 {
                    // ibverbs-sys exposes the host-order union as imm_data.
                    output.invalidated_rkey = completion.imm_data;
                }
            }
            *result = output;
        }
        count
    }
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
destroy!(destroy_cq, ConnectionHandles, cq, ibv_destroy_cq);
destroy!(dealloc_pd, DeviceHandles, pd, ibv_dealloc_pd);
destroy!(close_device, DeviceHandles, context, ibv_close_device);

unsafe extern "C" fn get_async_event(context: *mut c_void, event: *mut AsyncEvent) -> c_int {
    // SAFETY: the context owner keeps the native fd/context alive and is its
    // sole consumer. The provider initializes native only on successful get.
    unsafe {
        if ffi::ibv_get_async_event(context.cast(), (*event).native.as_mut_ptr()) != 0 {
            return failure();
        }
        (*event).kind = (*(*event).native.as_ptr()).event_type;
        0
    }
}

unsafe extern "C" fn ack_async_event(_: *mut c_void, event: *mut AsyncEvent) {
    // SAFETY: each successful get is acknowledged once before touching locks;
    // no QP/CQ pointer from this event is accessed after acknowledgment.
    unsafe { ffi::ibv_ack_async_event((*event).native.as_mut_ptr()) };
}

pub(super) static REAL: Verbs = Verbs {
    discover: discovery::discover,
    query_address,
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
