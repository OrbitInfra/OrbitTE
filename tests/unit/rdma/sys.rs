#![allow(unsafe_code)]

use super::*;

#[derive(Debug, PartialEq)]
struct SendRequest {
    id: u64,
    opcode: u32,
    flags: u32,
    address: u64,
    length: u32,
    lkey: u32,
    remote_address: u64,
    rkey: u32,
}

#[derive(Default)]
struct Posted {
    result: i32,
    request: Option<SendRequest>,
}

unsafe extern "C" fn capture_post(
    qp: *mut ffi::ibv_qp,
    wr: *mut ffi::ibv_send_wr,
    _: *mut *mut ffi::ibv_send_wr,
) -> c_int {
    // This callback consumes only stack descriptors; it never starts DMA.
    unsafe {
        let state = &mut *(*qp).qp_context.cast::<Posted>();
        let wr = &*wr;
        let sge = &*wr.sg_list;
        state.request = Some(SendRequest {
            id: wr.wr_id,
            opcode: wr.opcode,
            flags: wr.send_flags,
            address: sge.addr,
            length: sge.length,
            lkey: sge.lkey,
            remote_address: wr.wr.rdma.remote_addr,
            rkey: wr.wr.rdma.rkey,
        });
        state.result
    }
}

#[test]
fn post_dispatch_uses_generated_provider_abi_and_returns_native_errors() {
    let mut context = Box::new(ffi::ibv_context::default());
    let mut state = Posted::default();
    let mut qp = ffi::ibv_qp {
        context: &mut *context,
        qp_context: (&mut state as *mut Posted).cast(),
        ..ffi::ibv_qp::default()
    };
    let mut bytes = [0u8; 16];
    let mut mr = ffi::ibv_mr {
        addr: bytes.as_mut_ptr().cast(),
        lkey: 0x01234567,
        ..ffi::ibv_mr::default()
    };
    let mut handles = Handles {
        qp: (&mut qp as *mut ffi::ibv_qp).cast(),
        mr: (&mut mr as *mut ffi::ibv_mr).cast(),
        ..Handles::default()
    };
    assert_eq!(
        unsafe { post(&mut handles, 0x123456789, 0xabcdef, 16, 0) },
        libc::EOPNOTSUPP
    );
    assert!(state.request.is_none());
    context.ops.post_send = Some(capture_post);
    for (write, error, opcode) in [
        (0, 0, ffi::ibv_wr_opcode::IBV_WR_RDMA_READ),
        (1, libc::ENOMEM, ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE),
    ] {
        state.result = error;
        assert_eq!(
            unsafe { post(&mut handles, 0x123456789, 0xabcdef, 16, write) },
            error
        );
        assert_eq!(
            state.request,
            Some(SendRequest {
                id: 1,
                opcode,
                flags: ffi::ibv_send_flags::IBV_SEND_SIGNALED.0,
                address: bytes.as_ptr() as u64,
                length: 16,
                lkey: mr.lkey,
                remote_address: 0x123456789,
                rkey: 0xabcdef,
            })
        );
    }
}

struct NativeCq {
    count: i32,
    bytes: [u8; 48],
}

unsafe extern "C" fn capture_poll(
    cq: *mut ffi::ibv_cq,
    _: c_int,
    completion: *mut ffi::ibv_wc,
) -> c_int {
    // A native-layout fixture writes the published binding's private CQ fields.
    // This is a CPU ABI control, not an emulated completion or device receipt.
    unsafe {
        let state = &*(*cq).cq_context.cast::<NativeCq>();
        if state.count == 1 {
            std::ptr::copy_nonoverlapping(state.bytes.as_ptr(), completion.cast(), 48);
        }
        state.count
    }
}

#[test]
fn poll_dispatch_decodes_native_cqes_and_only_reads_error_valid_fields() {
    assert_eq!(std::mem::size_of::<ffi::ibv_wc>(), 48);
    let mut context = Box::new(ffi::ibv_context::default());
    let mut state = NativeCq {
        count: 0,
        bytes: [0; 48],
    };
    let mut cq = ffi::ibv_cq {
        context: &mut *context,
        cq_context: (&mut state as *mut NativeCq).cast(),
        ..ffi::ibv_cq::default()
    };
    let mut handles = Handles {
        cq: (&mut cq as *mut ffi::ibv_cq).cast(),
        ..Handles::default()
    };
    let mut output = Completion::default();
    assert_eq!(
        unsafe { poll(&mut handles, &mut output) },
        -libc::EOPNOTSUPP
    );
    context.ops.poll_cq = Some(capture_poll);
    for count in [0, -libc::EIO] {
        state.count = count;
        assert_eq!(unsafe { poll(&mut handles, &mut output) }, count);
        assert_eq!(output.id, 0);
    }
    state.count = 1;
    for (status, opcode, length, vendor) in [
        (
            ffi::ibv_wc_status::IBV_WC_SUCCESS,
            ffi::ibv_wc_opcode::IBV_WC_RDMA_READ,
            16,
            0,
        ),
        (
            ffi::ibv_wc_status::IBV_WC_SUCCESS,
            ffi::ibv_wc_opcode::IBV_WC_RDMA_WRITE,
            0,
            0,
        ),
        (
            ffi::ibv_wc_status::IBV_WC_REM_ACCESS_ERR,
            u32::MAX,
            u32::MAX,
            0x83,
        ),
    ] {
        // libibverbs' native ibv_wc prefix: wr_id, status, opcode, vendor_err,
        // byte_len. Its remaining fields are zero in this deterministic fixture.
        state.bytes[..8].copy_from_slice(&7u64.to_ne_bytes());
        for (index, field) in [status, opcode, vendor, length].into_iter().enumerate() {
            state.bytes[8 + index * 4..12 + index * 4].copy_from_slice(&field.to_ne_bytes());
        }
        assert_eq!(unsafe { poll(&mut handles, &mut output) }, 1);
        assert_eq!(
            (output.id, output.status, output.vendor_error),
            (7, status, vendor)
        );
        let expected = if status == 0 {
            (opcode, length)
        } else {
            (0, 0)
        };
        assert_eq!((output.opcode, output.length), expected);
    }
}
