//! Native completion queues, polling and completion-channel events.

use super::*;

pub(super) unsafe extern "C" fn create_cq(
    handles: *mut CqHandles,
    device: *const DeviceHandles,
    capacity: u32,
    channel: *mut c_void,
) -> c_int {
    // SAFETY: the progress owner retains this CQ and its actual RdmaContext/PD.
    unsafe {
        (*handles).cq = ffi::ibv_create_cq(
            (*device).context.cast(),
            capacity as c_int,
            std::ptr::null_mut(),
            channel.cast(),
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

pub(super) unsafe extern "C" fn poll(h: *mut ConnectionHandles, result: *mut Completion) -> c_int {
    // SAFETY: RdmaGrant retains the CQ; one initialized output receives one CQE.
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

pub(super) unsafe extern "C" fn create_completion_channel(
    handles: *mut ChannelHandles,
    device: *const DeviceHandles,
) -> c_int {
    unsafe {
        (*handles).channel = ffi::ibv_create_comp_channel((*device).context.cast()).cast();
        if (*handles).channel.is_null() {
            return failure();
        }
        let fd = (*(*handles).channel.cast::<ffi::ibv_comp_channel>()).fd;
        (*handles).fd = fd;
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags == -1 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) == -1 {
            return failure();
        }
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags == -1 || libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) == -1 {
            return failure();
        }
        0
    }
}

pub(super) unsafe extern "C" fn get_completion_event(
    channel: *mut c_void,
    cq: *mut *mut c_void,
) -> c_int {
    unsafe {
        let mut native_cq = std::ptr::null_mut();
        let mut context = std::ptr::null_mut();
        if ffi::ibv_get_cq_event(channel.cast(), &mut native_cq, &mut context) != 0 {
            return failure();
        }
        *cq = native_cq.cast();
        0
    }
}

pub(super) unsafe extern "C" fn ack_completion_event(cq: *mut c_void) {
    unsafe { ffi::ibv_ack_cq_events(cq.cast(), 1) };
}

pub(super) unsafe extern "C" fn arm_completion(cq: *mut c_void) -> c_int {
    unsafe {
        let cq = cq.cast::<ffi::ibv_cq>();
        let Some(arm) = (*(*cq).context).ops.req_notify_cq else {
            return libc::EOPNOTSUPP;
        };
        arm(cq, 0)
    }
}
