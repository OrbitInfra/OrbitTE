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
    requests: Vec<SendRequest>,
    bad_index: Option<usize>,
    foreign_bad: bool,
    bad_byte_offset: Option<usize>,
}

unsafe extern "C" fn capture_post(
    qp: *mut ffi::ibv_qp,
    wr: *mut ffi::ibv_send_wr,
    bad: *mut *mut ffi::ibv_send_wr,
) -> c_int {
    // CPU ABI control: consume only stable linked descriptors, never DMA.
    unsafe {
        let state = &mut *(*qp).qp_context.cast::<Posted>();
        state.requests.clear();
        let mut pointer = wr;
        while !pointer.is_null() {
            let request = &*pointer;
            let sge = &*request.sg_list;
            if state.bad_index == Some(state.requests.len()) {
                *bad = pointer;
            }
            state.requests.push(SendRequest {
                id: request.wr_id,
                opcode: request.opcode,
                flags: request.send_flags,
                address: sge.addr,
                length: sge.length,
                lkey: sge.lkey,
                remote_address: request.wr.rdma.remote_addr,
                rkey: request.wr.rdma.rkey,
            });
            pointer = request.next;
        }
        if state.foreign_bad {
            *bad = std::ptr::dangling_mut();
        }
        if let Some(offset) = state.bad_byte_offset {
            *bad = wr.cast::<u8>().add(offset).cast();
        }
        state.result
    }
}

#[test]
fn chained_posts_preserve_generated_abi_offsets_ids_and_accepted_prefix_without_dereferencing_bad_wr()
 {
    let mut context = Box::new(ffi::ibv_context::default());
    let mut state = Posted::default();
    let mut qp = ffi::ibv_qp {
        context: &mut *context,
        qp_context: (&mut state as *mut Posted).cast(),
        ..ffi::ibv_qp::default()
    };
    let mut bytes = [0u8; 64];
    let mut mr = ffi::ibv_mr {
        addr: bytes.as_mut_ptr().cast(),
        length: bytes.len(),
        lkey: 0x01234567,
        ..ffi::ibv_mr::default()
    };
    let mut handles = Handles {
        qp: (&mut qp as *mut ffi::ibv_qp).cast(),
        mr: (&mut mr as *mut ffi::ibv_mr).cast(),
        ..Handles::default()
    };
    let requests = [
        WorkRequest {
            id: 4,
            offset: 0,
            length: 7,
        },
        WorkRequest {
            id: 5,
            offset: 7,
            length: 9,
        },
        WorkRequest {
            id: 6,
            offset: 16,
            length: 16,
        },
    ];
    let mut result = PostResult::default();
    assert_eq!(
        unsafe {
            post(
                &mut handles,
                requests.as_ptr(),
                requests.len(),
                bytes.as_ptr() as u64 + 16,
                32,
                0x123456789,
                0xabcdef,
                0,
                &mut result,
            )
        },
        libc::EOPNOTSUPP
    );
    assert_eq!((result.accepted, result.uncertain), (0, false));
    assert!(state.requests.is_empty());
    context.ops.post_send = Some(capture_post);
    for write in [0, 1] {
        for (code, bad_index, foreign_bad, bad_byte_offset, expected, uncertain) in [
            (0, None, false, None, 3, false),
            (libc::ENOMEM, Some(0), false, None, 0, false),
            (libc::ENOMEM, Some(1), false, None, 1, false),
            (libc::ENOMEM, Some(2), false, None, 2, false),
            (libc::EIO, None, false, None, 0, true),
            (libc::EIO, None, true, None, 0, true),
            (
                libc::EIO,
                None,
                false,
                Some(std::mem::size_of::<ffi::ibv_send_wr>() * requests.len()),
                0,
                true,
            ),
            (libc::EIO, None, false, Some(1), 0, true),
        ] {
            state.result = code;
            state.bad_index = bad_index;
            state.foreign_bad = foreign_bad;
            state.bad_byte_offset = bad_byte_offset;
            assert_eq!(
                unsafe {
                    post(
                        &mut handles,
                        requests.as_ptr(),
                        requests.len(),
                        bytes.as_ptr() as u64 + 16,
                        32,
                        0x123456789,
                        0xabcdef,
                        write,
                        &mut result,
                    )
                },
                code
            );
            assert_eq!((result.accepted, result.uncertain), (expected, uncertain));
            assert_eq!(state.requests.len(), 3);
            for (request, actual) in requests.iter().zip(&state.requests) {
                assert_eq!(
                    actual,
                    &SendRequest {
                        id: request.id,
                        opcode: if write == 0 {
                            ffi::ibv_wr_opcode::IBV_WR_RDMA_READ
                        } else {
                            ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE
                        },
                        flags: ffi::ibv_send_flags::IBV_SEND_SIGNALED.0,
                        address: bytes.as_ptr() as u64 + 16 + u64::from(request.offset),
                        length: request.length,
                        lkey: mr.lkey,
                        remote_address: 0x123456789 + u64::from(request.offset),
                        rkey: 0xabcdef
                    }
                );
            }
        }
    }
    state.requests.clear();
    for invalid in [
        WorkRequest {
            id: 1,
            offset: 31,
            length: 2,
        },
        WorkRequest {
            id: 1,
            offset: 0,
            length: 0,
        },
    ] {
        assert_eq!(
            unsafe {
                post(
                    &mut handles,
                    &invalid,
                    1,
                    bytes.as_ptr() as u64 + 16,
                    32,
                    0x100,
                    7,
                    0,
                    &mut result,
                )
            },
            libc::EINVAL
        );
        assert_eq!((result.accepted, result.uncertain), (0, false));
        assert!(state.requests.is_empty());
    }
    for (base, length) in [
        (bytes.as_ptr() as u64 - 1, 32),
        (bytes.as_ptr() as u64 + 48, 32),
        (bytes.as_ptr() as u64 + 16, 0),
    ] {
        state.requests.clear();
        assert_eq!(
            unsafe {
                post(
                    &mut handles,
                    requests.as_ptr(),
                    requests.len(),
                    base,
                    length,
                    0x100,
                    7,
                    0,
                    &mut result,
                )
            },
            libc::EINVAL
        );
        assert_eq!((result.accepted, result.uncertain), (0, false));
        assert!(state.requests.is_empty());
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
        // byte_len, plus the error-valid QP number used by shared CQ routing.
        state.bytes[..8].copy_from_slice(&7u64.to_ne_bytes());
        for (index, field) in [status, opcode, vendor, length].into_iter().enumerate() {
            state.bytes[8 + index * 4..12 + index * 4].copy_from_slice(&field.to_ne_bytes());
        }
        let qpn = std::mem::offset_of!(ffi::ibv_wc, qp_num);
        state.bytes[qpn..qpn + 4].copy_from_slice(&0x12345u32.to_ne_bytes());
        assert_eq!(unsafe { poll(&mut handles, &mut output) }, 1);
        assert_eq!(
            (output.id, output.status, output.vendor_error, output.qpn),
            (7, status, vendor, 0x12345)
        );
        let expected = if status == 0 {
            (opcode, length)
        } else {
            (0, 0)
        };
        assert_eq!((output.opcode, output.length), expected);
    }
}

struct WindowPost {
    bind: bool,
    code: i32,
    foreign: bool,
    address: u64,
    length: u64,
    mr: *mut ffi::ibv_mr,
    mw: *mut ffi::ibv_mw,
}
unsafe extern "C" fn capture_window(
    qp: *mut ffi::ibv_qp,
    wr: *mut ffi::ibv_send_wr,
    bad: *mut *mut ffi::ibv_send_wr,
) -> c_int {
    unsafe {
        let s = &*(*qp).qp_context.cast::<WindowPost>();
        let w = &*wr;
        assert_eq!(w.wr_id, 65);
        assert_eq!(w.num_sge, 0);
        assert!(w.sg_list.is_null() && w.next.is_null());
        assert_eq!(w.send_flags, ffi::ibv_send_flags::IBV_SEND_SIGNALED.0);
        if s.bind {
            assert_eq!(w.opcode, ffi::ibv_wr_opcode::IBV_WR_BIND_MW);
            let b = w.__bindgen_anon_2.bind_mw;
            assert_eq!((b.mw, b.rkey, b.bind_info.mr), (s.mw, 0x123456ff, s.mr));
            assert_eq!(
                (b.bind_info.addr, b.bind_info.length),
                (s.address, s.length)
            );
            assert_eq!(
                b.bind_info.mw_access_flags,
                ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
            );
        } else {
            assert_eq!(w.opcode, ffi::ibv_wr_opcode::IBV_WR_LOCAL_INV);
            assert_eq!(w.__bindgen_anon_1.invalidate_rkey, 0x123456ff);
        }
        if s.code != 0 {
            *bad = if s.foreign {
                wr.cast::<u8>().add(1).cast()
            } else {
                wr
            };
        }
        s.code
    }
}
#[test]
fn type2_window_posts_preserve_native_union_fields_and_unknown_post_debt() {
    for bind in [true, false] {
        for (code, foreign) in [(0, false), (libc::EIO, false), (libc::EIO, true)] {
            let mut context = Box::new(ffi::ibv_context::default());
            context.ops.post_send = Some(capture_window);
            let mut mr = ffi::ibv_mr::default();
            let mut mw = ffi::ibv_mw::default();
            let mut state = WindowPost {
                bind,
                code,
                foreign,
                address: 0x123456781000,
                length: 4097,
                mr: &mut mr,
                mw: &mut mw,
            };
            let mut qp = ffi::ibv_qp {
                context: &mut *context,
                qp_context: (&mut state as *mut WindowPost).cast(),
                ..Default::default()
            };
            let mut handles = Handles {
                qp: (&mut qp as *mut ffi::ibv_qp).cast(),
                mr: (&mut mr as *mut ffi::ibv_mr).cast(),
                mw: (&mut mw as *mut ffi::ibv_mw).cast(),
                window_key: 0x123456ff,
                ..Default::default()
            };
            let mut result = PostResult::default();
            assert_eq!(
                unsafe {
                    window(
                        &mut handles,
                        65,
                        state.address,
                        state.length as usize,
                        2,
                        bind,
                        &mut result,
                    )
                },
                code
            );
            assert_eq!(
                (result.accepted, result.uncertain),
                (usize::from(code == 0), foreign)
            );
        }
    }
}
