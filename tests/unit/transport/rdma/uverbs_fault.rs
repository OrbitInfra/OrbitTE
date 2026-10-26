//! Thread-local syscall faults on one test-owned uverbs command descriptor.

pub(super) fn deny_commands(fd: libc::c_int) {
    // Linux seccomp_data: nr at 0, args[0] at 16. This filter affects only
    // this calling thread and only write/ioctl on the selected uverbs FD.
    let mut instructions = [
        libc::sock_filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 1,
            jf: 0,
            k: libc::SYS_write as u32,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 3,
            k: libc::SYS_ioctl as u32,
        },
        libc::sock_filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 16,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 1,
            k: fd.try_into().unwrap(),
        },
        libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: 0x0005_0000 | libc::EIO as u32,
        },
        libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: 0x7fff_0000,
        },
    ];
    let program = libc::sock_fprog {
        len: instructions.len().try_into().unwrap(),
        filter: instructions.as_mut_ptr(),
    };
    assert_eq!(
        unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
        0
    );
    assert_eq!(unsafe { libc::prctl(libc::PR_SET_SECCOMP, 2, &program) }, 0);
}
