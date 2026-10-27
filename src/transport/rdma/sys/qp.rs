//! Native RC QP setup and payload or completion-message work requests.

use super::device::{endpoint_address, query_descriptor};
use super::*;

pub(super) unsafe extern "C" fn open(
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
    max_recv_wr: u32,
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
        init.cap.max_recv_wr = max_recv_wr;
        init.cap.max_send_sge = 1;
        init.cap.max_recv_sge = 1;
        let pd = if h.private_pd.is_null() {
            device.pd
        } else {
            h.private_pd
        };
        h.qp = ffi::ibv_create_qp(pd.cast(), &mut init).cast();
        if h.qp.is_null() {
            return failure();
        }
        if init.cap.max_send_wr < max_send_wr
            || init.cap.max_send_sge < 1
            || init.cap.max_recv_wr < max_recv_wr
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

pub(super) unsafe extern "C" fn connect(
    handles: *mut ConnectionHandles,
    local: *const Descriptor,
    remote: *const Descriptor,
    port: u8,
    gid_index: u8,
    traffic_class: u8,
    hop_limit: u8,
) -> c_int {
    // SAFETY: RdmaGrant owns an initialized QP and validates both descriptors
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

pub(super) unsafe extern "C" fn post(
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
    // SAFETY: RdmaGrant owns the MR/QP and bytes until successful teardown. The
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
    if memory.mr.is_null() || length != crate::transport::rdma::COMPLETION_MESSAGE_SIZE {
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

pub(super) unsafe extern "C" fn post_receive(
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

pub(super) unsafe extern "C" fn send_completion(
    handles: *mut ConnectionHandles,
    memory: *const MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    rkey: u32,
    invalidate: bool,
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
        wr.opcode = if invalidate {
            ffi::ibv_wr_opcode::IBV_WR_SEND_WITH_INV
        } else {
            ffi::ibv_wr_opcode::IBV_WR_SEND
        };
        wr.send_flags =
            ffi::ibv_send_flags::IBV_SEND_FENCE.0 | ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        wr.__bindgen_anon_1.invalidate_rkey = if invalidate { rkey } else { 0 };
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
