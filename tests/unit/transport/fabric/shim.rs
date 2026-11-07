//! Interpose on real tcp-provider calls: every unmodified call reaches libfabric.
#![allow(unsafe_code)]
use super::super::sys::{Calls, Completion, Handles, REAL, Symbols};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    ffi::c_void,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering},
    },
};

#[derive(Default)]
pub(super) struct Faults {
    pub hold: AtomicBool,
    pub drain_expired: Arc<AtomicBool>,
    pub reject: AtomicBool,
    pub cq_error: AtomicBool,
    pub partial_post: AtomicI32,
    pub close_kind: AtomicI32,
    pub register_error: AtomicBool,
    pub remove_error: AtomicBool,
    pub poll_error: AtomicBool,
    pub posts: AtomicUsize,
    pub closes: AtomicUsize,
    pub intercepted: AtomicUsize,
    held: Mutex<VecDeque<(usize, usize, i32, i32)>>,
}
thread_local! {static CURRENT:RefCell<Option<Arc<Faults>>>=const {RefCell::new(None)};}
type Objects = HashMap<usize, (Arc<Faults>, i32)>;
fn objects() -> &'static Mutex<Objects> {
    static O: OnceLock<Mutex<Objects>> = OnceLock::new();
    O.get_or_init(Mutex::default)
}
pub(super) fn configured<T>(faults: Arc<Faults>, f: impl FnOnce() -> T) -> T {
    struct Reset(Option<Arc<Faults>>);
    impl Drop for Reset {
        fn drop(&mut self) {
            CURRENT.with(|s| *s.borrow_mut() = self.0.take());
        }
    }
    let _reset = Reset(CURRENT.with(|s| s.replace(Some(faults))));
    f()
}
pub(super) fn calls(real: Calls) -> Calls {
    if CURRENT.with(|s| s.borrow().is_some()) {
        Calls {
            open,
            close,
            register,
            post,
            poll,
            remove,
            ..real
        }
    } else {
        real
    }
}
unsafe extern "C" fn open(
    a: *const Symbols,
    i: *mut c_void,
    cq: usize,
    peers: usize,
    tcp: i32,
    h: *mut Handles,
    address: *mut u8,
    len: *mut usize,
) -> i32 {
    // SAFETY: forward the production ABI arguments unchanged.
    let code = unsafe { (REAL.open)(a, i, cq, peers, tcp, h, address, len) };
    CURRENT.with(|current| {
        if let Some(f) = current.borrow().as_ref() {
            // SAFETY: h is the live constructor output, including partial handles.
            let h = unsafe { &*h };
            let mut map = objects().lock().unwrap();
            for (p, kind) in [
                (h.fabric, 1),
                (h.domain, 2),
                (h.av, 3),
                (h.cq, 4),
                (h.ep, 5),
            ] {
                if !p.is_null() {
                    map.insert(p as usize, (f.clone(), kind));
                }
            }
        }
    });
    code
}
unsafe extern "C" fn close(p: *mut c_void) -> i32 {
    let state = objects().lock().unwrap().get(&(p as usize)).cloned();
    if let Some((f, kind)) = &state {
        if f.close_kind.load(Ordering::SeqCst) == *kind {
            f.closes.fetch_add(1, Ordering::SeqCst);
            return -libc::EIO;
        }
    }
    // SAFETY: forward the live native object once; a failed destroy is never retried.
    let r = unsafe { (REAL.close)(p) };
    if r == 0 {
        objects().lock().unwrap().remove(&(p as usize));
    }
    r
}
unsafe extern "C" fn register(
    h: *const Handles,
    p: *mut c_void,
    len: usize,
    remote: i32,
    key: u64,
    ep: i32,
    mr: *mut *mut c_void,
    desc: *mut *mut c_void,
    actual: *mut u64,
) -> i32 {
    // SAFETY: h is a live domain handle supplied by the registration owner.
    let domain = unsafe { (*h).domain as usize };
    let f = objects()
        .lock()
        .unwrap()
        .get(&domain)
        .map(|(f, _)| f.clone());
    if f.as_ref()
        .is_some_and(|f| f.register_error.load(Ordering::SeqCst))
    {
        return -libc::ENOMEM;
    }
    // SAFETY: forward the checked range and every output unchanged.
    let r = unsafe { (REAL.register)(h, p, len, remote, key, ep, mr, desc, actual) };
    // SAFETY: mr is initialized by the native call, also on partial construction.
    let handle = unsafe { *mr };
    if !handle.is_null() {
        if let Some(f) = f {
            objects().lock().unwrap().insert(handle as usize, (f, 6));
        }
    }
    r
}
unsafe extern "C" fn post(
    h: *const Handles,
    write: i32,
    p: *mut c_void,
    len: usize,
    desc: *mut c_void,
    peer: u64,
    remote: u64,
    key: u64,
    cookie: *mut c_void,
) -> i32 {
    // SAFETY: h is the retained endpoint of this operation.
    let ep = unsafe { (*h).ep as usize };
    let f = objects().lock().unwrap().get(&ep).map(|(f, _)| f.clone());
    if let Some(f) = &f {
        if f.reject.load(Ordering::SeqCst) {
            return -libc::EAGAIN;
        }
        let before = f.posts.load(Ordering::SeqCst);
        let cap = f.partial_post.load(Ordering::SeqCst);
        if cap > 0 && before >= cap as usize {
            return -libc::EIO;
        }
    }
    // SAFETY: forward the native range, address and retained context unchanged.
    let result = unsafe { (REAL.post)(h, write, p, len, desc, peer, remote, key, cookie) };
    if result == 0 {
        if let Some(f) = f {
            f.posts.fetch_add(1, Ordering::SeqCst);
        }
    }
    result
}
unsafe extern "C" fn poll(h: *const Handles, c: *mut Completion) -> i32 {
    // SAFETY: h names the CQ owned by the polling worker.
    let ep = unsafe { (*h).ep as usize };
    let f = objects().lock().unwrap().get(&ep).map(|(f, _)| f.clone());
    if let Some(f) = &f {
        if f.poll_error.swap(false, Ordering::SeqCst) {
            return -libc::EIO;
        }
        if !f.hold.load(Ordering::SeqCst) {
            if let Some((cookie, len, error, provider_error)) = f.held.lock().unwrap().pop_front() {
                // SAFETY: c is the worker's exclusive output; the cookie stays
                // retained until the withheld native completion is delivered.
                unsafe {
                    *c = Completion {
                        context: cookie as *mut c_void,
                        length: len,
                        error,
                        provider_error,
                    };
                }
                return 0;
            }
        }
    }
    // SAFETY: this call still drives native target progress while withholding CQEs.
    let r = unsafe { (REAL.poll)(h, c) };
    if r == 0 {
        if let Some(f) = f {
            f.intercepted.fetch_add(1, Ordering::SeqCst);
            // SAFETY: real poll initialized c for one terminal CQE.
            let c = unsafe { &mut *c };
            if f.cq_error.swap(false, Ordering::SeqCst) {
                c.error = libc::EIO;
                c.length = 3;
            }
            if f.hold.load(Ordering::SeqCst) {
                f.held.lock().unwrap().push_back((
                    c.context as usize,
                    c.length,
                    c.error,
                    c.provider_error,
                ));
                return 1;
            }
        }
    }
    r
}
unsafe extern "C" fn remove(h: *const Handles, address: u64) -> i32 {
    // SAFETY: h is retained by the peer-retirement owner.
    let ep = unsafe { (*h).ep as usize };
    let f = objects().lock().unwrap().get(&ep).map(|(f, _)| f.clone());
    if let Some(f) = f {
        if f.remove_error.load(Ordering::SeqCst) {
            f.closes.fetch_add(1, Ordering::SeqCst);
            return -libc::EIO;
        }
    }
    // SAFETY: the peer's operations have physically drained before removal.
    unsafe { (REAL.remove)(h, address) }
}

pub(super) fn drain_clock() -> Arc<AtomicBool> {
    CURRENT.with(|s| {
        s.borrow()
            .as_ref()
            .map(|f| f.drain_expired.clone())
            .unwrap_or_default()
    })
}
