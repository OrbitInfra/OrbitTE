use super::*;

pub(super) const VERBS: sys::GpuKeyVerbs = sys::GpuKeyVerbs { create, destroy };

unsafe extern "C" fn create(
    handles: *const sys::DeviceHandles,
    memory: *mut sys::MemoryHandles,
    qpn: u32,
    address: u64,
    length: u32,
    permission: u32,
    key: u32,
    probe: bool,
) -> c_int {
    // SAFETY: the registration/grant owns the fixture handles and parent for this callback.
    let (device, mr, native) = unsafe {
        (
            &*(*handles).context.cast::<FakeDevice>(),
            &*(*memory).mr.cast::<FakeMr>(),
            &mut *memory,
        )
    };
    assert!(mr.local_only && !mr.window_bind && mr.private_pd.is_null());
    assert!(
        address >= mr.address as u64
            && address + u64::from(length) <= (mr.address + mr.length) as u64
    );
    assert_eq!(qpn == sys::GPU_PROBE_QPN, probe);
    assert!(native.gpu_key.is_null());
    if probe && device.mode == "gpu_key_probe_unknown" {
        return libc::ETIMEDOUT;
    }
    if device.mode == "gpu_key_busy" {
        return libc::EBUSY;
    }
    if probe && device.mode == "gpu_key_unsupported" {
        return libc::EOPNOTSUPP;
    }
    let window = Box::into_raw(Box::new(FakeWindow {
        device,
        index: key >> 8,
        mr: AtomicUsize::new(native.mr as usize),
        bound: AtomicBool::new(!probe),
        qp_alive: AtomicBool::new(!probe),
        qp: AtomicUsize::new(0),
        received: AtomicBool::new(false),
        key: AtomicU32::new(key),
    }));
    native.gpu_key = window.cast();
    mr.windows.fetch_add(1, Ordering::SeqCst);
    if !probe {
        let routes = native_qps().lock().unwrap();
        // SAFETY: the route table pins this specific connected QP while locked.
        let qp = unsafe { &*(routes[&qpn] as *const Fake) };
        assert_eq!(qp.device, device as *const _);
        let mut grants = qp.grants.lock().unwrap();
        let grant = grants.get_mut(&(memory as usize)).unwrap();
        assert_eq!(
            (grant.mr, grant.address, grant.length, grant.permission),
            (native.mr as usize, address, length as usize, permission)
        );
        grant.window = window as usize;
        // SAFETY: this uniquely owned window is still pinned by the grant.
        unsafe {
            (*window)
                .qp
                .store(qp as *const _ as usize, Ordering::Release)
        };
        let mut history = device.gpu_key_history.lock().unwrap();
        assert!(!history.contains(&key), "stale key rebound on the same PD");
        history.push(key);
    }
    device.events.lock().unwrap().push(if probe {
        "gpu_key_probe"
    } else {
        "gpu_key_bind"
    });
    if (probe && device.mode == "gpu_key_probe_query") || (!probe && device.mode == "gpu_key_query")
    {
        return libc::EPROTO;
    }
    0
}

unsafe extern "C" fn destroy(handle: *mut c_void) -> c_int {
    // SAFETY: fixture create installed this object and its retained parent and device.
    let (window, device) = unsafe {
        let w = &*handle.cast::<FakeWindow>();
        (w, &*w.device)
    };
    let probe = window.qp.load(Ordering::Acquire) == 0;
    device.events.lock().unwrap().push(if probe {
        "gpu_key_probe_destroy"
    } else {
        "gpu_key_destroy"
    });
    if (probe && device.mode == "gpu_key_probe_destroy")
        || (!probe && device.mode == "gpu_key_destroy")
    {
        return libc::EIO;
    }
    if window.qp_alive.load(Ordering::Acquire) {
        assert!(!window.bound.load(Ordering::Acquire) && window.received.load(Ordering::Acquire));
        // SAFETY: the grant retains this QP while the successful receive is being retired.
        let qp = unsafe { &*(window.qp.load(Ordering::Acquire) as *const Fake) };
        let mut grants = qp.grants.lock().unwrap();
        let grant = *grants
            .iter()
            .find(|(_, g)| g.window == handle as usize)
            .unwrap()
            .0;
        assert_eq!(grants.remove(&grant).unwrap().mr, 0);
    }
    // SAFETY: the key owns this dependency on the parent until successful destruction.
    unsafe {
        (*(window.mr.load(Ordering::Acquire) as *const FakeMr))
            .windows
            .fetch_sub(1, Ordering::SeqCst);
        drop(Box::from_raw(handle.cast::<FakeWindow>()));
    }
    0
}

pub(super) fn check_post(qpn: u32, rkey: u32, address: u64, length: u32, write: bool) {
    let routes = native_qps().lock().unwrap();
    // SAFETY: the route table pins the source QP while checking the key, before touching bytes.
    let source = unsafe { &*(routes[&qpn] as *const Fake) };
    let grants = source.grants.lock().unwrap();
    let authorized = |qpn, key, addr: u64, op| {
        grants.values().any(|grant| {
            if grant.window == 0 {
                return false;
            }
            // SAFETY: the source grant holds the fake key under this lock.
            let window = unsafe { &*(grant.window as *const FakeWindow) };
            qpn == source.qpn
                && window.bound.load(Ordering::Acquire)
                && window.key.load(Ordering::Acquire) == key
                && grant.permission == op
                && addr >= grant.address
                && addr
                    .checked_add(u64::from(length))
                    .is_some_and(|end| end <= grant.address + grant.length as u64)
        })
    };
    let op = if write { 2 } else { 1 };
    assert!(authorized(qpn, rkey, address, op));
    assert!(!authorized(qpn ^ 1, rkey, address, op));
    assert!(!authorized(qpn, rkey, address, 3 - op));
    let grant = grants
        .values()
        .find(|g| g.window != 0 && g.address <= address && address < g.address + g.length as u64)
        .unwrap();
    assert!(!authorized(qpn, rkey, grant.address - 1, op));
    assert!(!authorized(
        qpn,
        rkey,
        grant.address + grant.length as u64,
        op
    ));
    // SAFETY: the device outlives its QP and every grant.
    let device = unsafe { &*source.device };
    for &retired in device
        .gpu_key_history
        .lock()
        .unwrap()
        .iter()
        .filter(|&&k| k != rkey)
    {
        assert!(!authorized(qpn, retired, address, op));
    }
}
