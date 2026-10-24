//! Reject the second native WR through the actual mlx5 provider's SGE limit.
use std::ffi::{c_char, c_int};

use ibverbs_sys as ffi;

use super::*;

type PostSend = unsafe extern "C" fn(
    *mut ffi::ibv_qp,
    *mut ffi::ibv_send_wr,
    *mut *mut ffi::ibv_send_wr,
) -> c_int;

struct Provider {
    context: usize,
    post: PostSend,
    calls: usize,
    restored: bool,
    closed: bool,
}

static PROVIDER: OnceLock<Mutex<Option<Provider>>> = OnceLock::new();

unsafe extern "C" fn open_device(h: *mut sys::DeviceHandles, name: *const c_char) -> c_int {
    let code = unsafe { (sys::REAL.open_device)(h, name) };
    if code == 0 {
        let context = unsafe { (*h).context.cast::<ffi::ibv_context>() };
        let mut state = PROVIDER.get_or_init(Default::default).lock().unwrap();
        assert!(state.is_none());
        *state = Some(Provider {
            context: context as usize,
            post: unsafe { (*context).ops.post_send.unwrap() },
            calls: 0,
            restored: false,
            closed: false,
        });
        // This context is private to the exact native test. Install before any
        // event/progress worker starts; all other provider operations stay native.
        unsafe { (*context).ops.post_send = Some(post_send) };
    }
    code
}

unsafe extern "C" fn post_send(
    qp: *mut ffi::ibv_qp,
    first: *mut ffi::ibv_send_wr,
    bad: *mut *mut ffi::ibv_send_wr,
) -> c_int {
    let post = {
        let mut state = PROVIDER.get().unwrap().lock().unwrap();
        let provider = state.as_mut().unwrap();
        assert_eq!(unsafe { (*qp).context } as usize, provider.context);
        assert_eq!(provider.calls, 0, "a failed WRITE must never be replayed");
        provider.calls += 1;
        provider.post
    };
    unsafe {
        let second = (*first).next;
        assert!(!second.is_null() && !(*second).next.is_null());
        let third = (*second).next;
        assert!((*third).next.is_null());
        assert!(matches!(
            (*first).opcode,
            ffi::ibv_wr_opcode::IBV_WR_RDMA_READ | ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE
        ));
        assert_eq!((*second).num_sge, 1);
        // mlx5 checks max SGEs before reading the SGE list. Its real post_send
        // accepts WR 0, returns ENOMEM and bad_wr == WR 1, and never submits WR 2.
        (*second).num_sge = c_int::MAX;
        let code = post(qp, first, bad);
        (*second).num_sge = 1;
        assert_eq!(code, libc::ENOMEM, "expected a native provider rejection");
        assert_eq!(*bad, second, "the actual provider must identify WR 1");
        code
    }
}

unsafe extern "C" fn close_device(h: *mut sys::DeviceHandles) -> c_int {
    {
        let mut state = PROVIDER.get().unwrap().lock().unwrap();
        let provider = state.as_mut().unwrap();
        let context = unsafe { (*h).context.cast::<ffi::ibv_context>() };
        assert_eq!(context as usize, provider.context);
        // RdmaContext joins its children before closing the native context.
        unsafe { (*context).ops.post_send = Some(provider.post) };
        provider.restored = true;
    }
    let code = unsafe { (sys::REAL.close_device)(h) };
    if code == 0 {
        PROVIDER
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .closed = true;
    }
    code
}

pub(super) static VERBS: sys::Verbs = sys::Verbs {
    open_device,
    close_device,
    ..fragmented::VERBS
};

pub(super) fn finish() {
    let provider = PROVIDER.get().unwrap().lock().unwrap().take().unwrap();
    assert_eq!(provider.calls, 1);
    assert!(provider.restored && provider.closed);
    println!(
        "ORBITTE_PARTIAL_POST {{\"attempted_wr\":3,\"native_accepted_wr\":1,\"native_bad_wr_index\":1,\"native_errno\":12,\"provider_calls\":1,\"provider_restored\":true,\"native_context_closed\":true}}"
    );
}

pub(super) fn verify_prefix(memory: &crate::RegisteredMemory, original: &[u8], incoming: u8) {
    let mut actual = vec![0; original.len()];
    memory.read(0, &mut actual).unwrap();
    let mut changed = 0;
    for (index, (&byte, &before)) in actual.iter().zip(original).enumerate() {
        if (GUARD..GUARD + 4096).contains(&index) {
            assert!(
                byte == before || byte == incoming,
                "invalid prefix byte at {index}"
            );
            changed += usize::from(byte != before);
        } else {
            assert_eq!(byte, before, "unposted suffix/guard changed at {index}");
        }
    }
    println!(
        "ORBITTE_PARTIAL_BYTES {{\"changed_prefix_bytes\":{changed},\"possible_prefix_bytes\":4096,\"unposted_suffix_and_guards_unchanged\":true,\"confirmed_completion_bytes\":0}}"
    );
}
