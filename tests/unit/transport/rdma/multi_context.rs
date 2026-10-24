//! Actual same-RNIC/different-PD registration, followed by the shared-CQ gate.
use std::collections::BTreeSet;

use ibverbs_sys as ffi;

use super::*;

#[derive(Default)]
struct Native {
    domains: BTreeSet<usize>,
    active: BTreeSet<usize>,
    registrations: usize,
    deregistrations: usize,
}

static NATIVE: OnceLock<Mutex<Native>> = OnceLock::new();

unsafe extern "C" fn register(
    memory: *mut sys::MemoryHandles,
    device: *const sys::DeviceHandles,
    address: *mut std::ffi::c_void,
    length: usize,
    bind: bool,
) -> i32 {
    let code = unsafe { (sys::REAL.register_memory)(memory, device, address, length, bind) };
    if code == 0 {
        let mr = unsafe { &*(*memory).mr.cast::<ffi::ibv_mr>() };
        assert_eq!(mr.pd.cast::<std::ffi::c_void>(), unsafe { (*device).pd });
        assert_eq!(mr.context.cast::<std::ffi::c_void>(), unsafe {
            (*device).context
        });
        let mut native = NATIVE.get_or_init(Default::default).lock().unwrap();
        native.domains.insert(mr.pd as usize);
        assert!(native.active.insert(mr as *const _ as usize));
        native.registrations += 1;
    }
    code
}

unsafe extern "C" fn deregister(memory: *mut sys::MemoryHandles) -> i32 {
    let handle = unsafe { (*memory).mr } as usize;
    let code = unsafe { (sys::REAL.deregister)(memory) };
    if code == 0 && handle != 0 {
        let mut native = NATIVE.get().unwrap().lock().unwrap();
        assert!(native.active.remove(&handle));
        native.deregistrations += 1;
    }
    code
}

static VERBS: sys::Verbs = sys::Verbs {
    register_memory: register,
    deregister,
    ..sys::REAL
};

pub(super) fn registration() {
    println!();
    let mut config = rdma_config();
    let length = 4096 + 2 * GUARD;
    config.max_cached_mrs = 4;
    config.max_cached_mr_bytes = 4 * length;
    let usage = Arc::default();
    let counters = Arc::default();
    let cache = buffers::LocalBufferManager::new(&config, Arc::clone(&usage), &VERBS);
    let owners = [
        RdmaContext::open(&config, &VERBS, Arc::clone(&usage), Arc::clone(&counters)).unwrap(),
        RdmaContext::open(&config, &VERBS, Arc::clone(&usage), counters).unwrap(),
    ];
    assert_ne!(owners[0].domain().unwrap(), owners[1].domain().unwrap());
    let expected = vec![0x3a; length];
    let backing = Arc::new(HostMemory::new(expected.clone().into_boxed_slice()).unwrap());
    let weak = Arc::downgrade(&backing);
    let registry = MemoryRegistry::new(SourceEpoch::generate().unwrap(), 2, length);
    let memory = registry
        .register_host(Arc::clone(&backing), 0, length, Permission::RemoteReadWrite)
        .unwrap();
    let duplicate = registry
        .register_host(Arc::clone(&backing), 0, length, Permission::LocalOnly)
        .unwrap();
    let mut handles = Vec::new();
    let mut leases = Vec::new();
    for context in &owners {
        for bind in [false, true] {
            let lease = cache
                .acquire(Arc::clone(context), &memory, bind, Instant::now() + TIMEOUT)
                .unwrap();
            let handle = lease.handle().unwrap();
            assert!(
                !handles.contains(&handle),
                "one registration must not reuse an MR across protection domains or capabilities"
            );
            handles.push(handle);
            leases.push(lease);
        }
    }
    for (index, context) in owners.iter().enumerate() {
        for (capability, bind) in [false, true].into_iter().enumerate() {
            let same = cache
                .acquire(Arc::clone(context), &memory, bind, Instant::now() + TIMEOUT)
                .unwrap();
            assert_eq!(same.handle().unwrap(), handles[2 * index + capability]);
        }
    }
    let stats = *usage.lock().unwrap();
    assert_eq!((stats.active_devices, stats.opened_devices), (2, 2));
    assert_eq!(
        (
            stats.persistent_mrs,
            stats.persistent_source_mrs,
            stats.persistent_mr_bytes
        ),
        (4, 2, 4 * length)
    );
    assert_eq!(
        (stats.opened_persistent_mrs, stats.reused_persistent_mrs),
        (4, 4)
    );
    for lease in &mut leases[..2] {
        lease.release().unwrap();
    }
    registry.unregister(&memory).unwrap();
    cache.retire(&memory).unwrap();
    assert_eq!(usage.lock().unwrap().persistent_mrs, 4);
    registry.unregister(&duplicate).unwrap();
    cache.retire(&memory).unwrap();
    assert_eq!(usage.lock().unwrap().persistent_mrs, 2);
    for lease in &mut leases[2..] {
        lease.release().unwrap();
    }
    drop(leases);
    cache.close(Instant::now() + TIMEOUT).unwrap();
    for context in &owners {
        context.close().unwrap();
    }
    let stats = *usage.lock().unwrap();
    assert_eq!(
        (
            stats.active_devices,
            stats.persistent_mrs,
            stats.persistent_mr_bytes,
            stats.persistent_source_mrs,
            stats.async_event_workers
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(
        (
            stats.retained_devices,
            stats.retained_persistent_mrs,
            stats.retained_source_mrs
        ),
        (0, 0, 0)
    );
    verify(&memory, &expected);
    drop(memory);
    drop(duplicate);
    drop(backing);
    assert!(weak.upgrade().is_none());
    let native = NATIVE.get().unwrap().lock().unwrap();
    assert_eq!(
        (
            native.domains.len(),
            native.registrations,
            native.deregistrations
        ),
        (2, 4, 4)
    );
    assert!(native.active.is_empty());
    println!(
        "ORBITTE_MULTI_CONTEXT_MR {{\"contexts\":2,\"protection_domains\":2,\"registered_mrs\":4,\"reused_mrs\":4,\"peak_mr_bytes\":{},\"retired_idle_mrs\":2,\"retired_borrowed_mrs\":2,\"native_deregistration_receipts\":4,\"closed_contexts\":2,\"retained_mrs\":0,\"allocation_released\":true,\"same_rnic\":true}}",
        4 * length
    );
}
