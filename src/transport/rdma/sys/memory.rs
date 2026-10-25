//! Native MR registration and exact-range MW commands.

use super::*;

pub(super) unsafe extern "C" fn register_memory(
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

pub(super) unsafe extern "C" fn prepare_grant(
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

pub(super) unsafe extern "C" fn window(
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

pub(super) unsafe extern "C" fn dealloc_window(handles: *mut MemoryHandles) -> c_int {
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
