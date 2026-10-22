use std::time::{Duration, Instant};

use super::*;
use crate::memory::MemoryRegistry;
use crate::rdma::tests::{events, runtime};
use crate::{Permission, SourceEpoch};

#[test]
fn whole_registration_mr_is_reused_while_request_dma_lease_stays_exact() {
    let fixture = crate::test_support::external_memory(&[0x55; 24]);
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 2, 32);
    let memory = registry
        .register_host(Arc::clone(&fixture.memory), 0, 24, Permission::LocalOnly)
        .unwrap();
    let runtime = runtime("direct:ok");
    for offset in [3, 13, 5] {
        let mut endpoint = runtime
            .endpoint(
                &memory,
                offset,
                4,
                0,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
        assert_eq!(
            endpoint.descriptor().address,
            memory.base_address() + offset as u64
        );
        assert!(memory.read(offset, &mut [0; 4]).is_err());
        memory.write(0, &[9]).unwrap();
        endpoint.close().unwrap();
        memory.write(offset, &[7; 4]).unwrap();
    }
    let usage = runtime.stats().unwrap();
    assert_eq!(
        (
            usage.opened_persistent_mrs,
            usage.reused_persistent_mrs,
            usage.persistent_mrs,
            usage.persistent_mr_bytes
        ),
        (1, 2, 1, 24)
    );
    let events = events(&runtime);
    assert!(!events.lock().unwrap().contains(&"deregister_local"));
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mr_bytes, 0);
    drop(memory);
    drop(fixture.memory);
    assert!(fixture.owner.upgrade().is_none());
    assert_eq!(fixture.drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(registry.usage().unwrap().0, 0);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    let events = events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == "register_local")
            .count(),
        1
    );
    let mr = events
        .iter()
        .position(|event| *event == "deregister_local")
        .unwrap();
    let pd = events
        .iter()
        .position(|event| *event == "dealloc_pd")
        .unwrap();
    assert!(mr < pd);
}

fn limited_runtime(mode: &str, regions: usize, bytes: usize) -> crate::rdma::Runtime {
    let mut runtime = runtime(mode);
    runtime.config.max_endpoints = 3;
    runtime.config.max_cached_mrs = regions;
    runtime.config.max_cached_mr_bytes = bytes;
    runtime.registrations =
        HostRegistrations::new(&runtime.config, Arc::clone(&runtime.usage), runtime.verbs);
    runtime
}

#[test]
fn idle_lru_eviction_obeys_physical_limits_and_never_evicts_a_live_mr_borrow() {
    let registry = MemoryRegistry::new(SourceEpoch([2; 16]), 3, 48);
    let a = registry
        .register(vec![1; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let b = registry
        .register(vec![2; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let runtime = limited_runtime("direct:ok", 1, 16);
    let mut first = runtime
        .endpoint(&a, 4, 4, 0, Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert!(matches!(
        runtime.endpoint(&b, 4, 4, 0, Instant::now() + Duration::from_secs(2)),
        Err(Error::ResourceExhausted)
    ));
    first.close().unwrap();
    // A closed endpoint can still own its output while its MR borrow is idle.
    let mut next = runtime
        .endpoint(&b, 4, 4, 0, Instant::now() + Duration::from_secs(2))
        .unwrap();
    next.close().unwrap();
    drop(next);
    drop(first);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.opened_persistent_mrs,
            stats.evicted_persistent_mrs,
            stats.persistent_mrs,
            stats.persistent_mr_bytes
        ),
        (2, 1, 1, 16)
    );
    a.write(4, &[3; 4]).unwrap();
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
    let too_small = limited_runtime("direct:ok", 1, 15);
    assert!(matches!(
        too_small.endpoint(&a, 4, 1, 0, Instant::now() + Duration::from_secs(2)),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(too_small.stats().unwrap().persistent_mrs, 0);
    too_small
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn final_unregister_retirement_respects_duplicate_references_and_already_resolved_work() {
    let allocation = Arc::new(crate::HostMemory::new(vec![1; 24].into_boxed_slice()).unwrap());
    let registry = MemoryRegistry::new(SourceEpoch([3; 16]), 2, 48);
    let memory = registry
        .register_host(Arc::clone(&allocation), 0, 16, Permission::LocalOnly)
        .unwrap();
    let duplicate = registry
        .register_host(Arc::clone(&allocation), 0, 16, Permission::RemoteReadWrite)
        .unwrap();
    let runtime = limited_runtime("direct:ok", 2, 48);
    runtime
        .endpoint(&memory, 4, 4, 0, Instant::now() + Duration::from_secs(2))
        .unwrap()
        .close()
        .unwrap();
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    assert!(memory.is_active());
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 1);
    registry.unregister(&duplicate).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    assert!(!memory.is_active());
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
    // Resolution before final unregister is an admitted owner, not a new lookup.
    let mut admitted = runtime
        .endpoint(&memory, 8, 4, 0, Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 1);
    admitted.close().unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
    drop(admitted);
    let new_view = registry
        .register_host(allocation, 0, 16, Permission::LocalOnly)
        .unwrap();
    assert_ne!(new_view.id(), memory.id());
    runtime
        .endpoint(&new_view, 4, 4, 0, Instant::now() + Duration::from_secs(2))
        .unwrap()
        .close()
        .unwrap();
    assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, 3);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn creating_mr_can_be_retired_without_blocking_unregister_or_resurrecting_an_idle_entry() {
    use std::thread;
    let registry = MemoryRegistry::new(SourceEpoch([4; 16]), 1, 16);
    let memory = registry
        .register(vec![4; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let runtime = Arc::new(limited_runtime("direct:register_pause", 1, 16));
    let pause = crate::rdma::tests::pause(&runtime);
    let events = events(&runtime);
    let task_runtime = Arc::clone(&runtime);
    let task_memory = memory.clone();
    let worker = thread::spawn(move || {
        let mut endpoint = task_runtime
            .endpoint(
                &task_memory,
                4,
                4,
                0,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
        endpoint.close().unwrap();
    });
    let (lock, changed) = &*pause;
    let guard = lock.lock().unwrap();
    let (guard, timed) = changed
        .wait_timeout_while(guard, std::time::Duration::from_secs(2), |state| !state.0)
        .unwrap();
    assert!(!timed.timed_out());
    drop(guard);
    assert!(matches!(
        runtime.endpoint(&memory, 8, 4, 0, Instant::now() + Duration::from_millis(10)),
        Err(Error::Timeout)
    ));
    memory.write(8, &[9; 4]).unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!((stats.active_endpoints, stats.direct_bytes), (1, 4));
    assert_eq!((stats.persistent_mrs, stats.persistent_mr_bytes), (1, 16));
    assert_eq!(stats.opened_persistent_mrs, 0);
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    assert!(matches!(runtime.close(Instant::now()), Err(Error::Timeout)));
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    memory.write(0, &[7]).unwrap();
    assert!(memory.read(4, &mut [0; 4]).is_err());
    lock.lock().unwrap().1 = true;
    changed.notify_all();
    worker.join().unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
    memory.write(4, &[9; 4]).unwrap();
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    let events = events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == "register_local")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == "deregister_local")
            .count(),
        1
    );
}

#[test]
fn concurrent_request_ranges_share_one_creating_mr_and_keep_independent_dma_leases() {
    use std::sync::Barrier;
    use std::thread;
    let registry = MemoryRegistry::new(SourceEpoch([5; 16]), 1, 16);
    let memory = registry
        .register(vec![5; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let runtime = Arc::new(limited_runtime("direct:register_pause", 1, 16));
    let pause = crate::rdma::tests::pause(&runtime);
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for offset in [0, 8] {
        let runtime = Arc::clone(&runtime);
        let memory = memory.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let mut endpoint = runtime
                .endpoint(
                    &memory,
                    offset,
                    4,
                    0,
                    Instant::now() + Duration::from_secs(2),
                )
                .unwrap();
            barrier.wait();
            barrier.wait();
            endpoint.close().unwrap();
        }));
    }
    let (lock, changed) = &*pause;
    let guard = lock.lock().unwrap();
    let (mut guard, timed) = changed
        .wait_timeout_while(guard, std::time::Duration::from_secs(2), |state| !state.0)
        .unwrap();
    assert!(!timed.timed_out());
    guard.1 = true;
    changed.notify_all();
    drop(guard);
    barrier.wait();
    assert_eq!(
        (
            runtime.stats().unwrap().opened_persistent_mrs,
            runtime.stats().unwrap().reused_persistent_mrs
        ),
        (1, 1)
    );
    assert!(memory.read(0, &mut [0; 4]).is_err());
    assert!(memory.read(8, &mut [0; 4]).is_err());
    memory.write(4, &[3; 4]).unwrap();
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    memory.write(0, &[3; 12]).unwrap();
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn deregistration_failure_keeps_actual_owner_device_and_budget_and_never_retries() {
    for boundary in [
        "idle_unregister",
        "active_unregister",
        "eviction",
        "whole_close",
        "drop",
        "partial_open",
    ] {
        let fixture = crate::test_support::external_memory(&[0x55; 16]);
        let registry = MemoryRegistry::new(SourceEpoch([6; 16]), 2, 32);
        let memory = registry
            .register_host(Arc::clone(&fixture.memory), 0, 16, Permission::LocalOnly)
            .unwrap();
        let runtime = limited_runtime(
            if boundary == "partial_open" {
                "direct:register_fail_close1"
            } else {
                "direct:mr_close"
            },
            1,
            16,
        );
        let usage = Arc::clone(&runtime.usage);
        let endpoint = runtime.endpoint(&memory, 4, 4, 0, Instant::now() + Duration::from_secs(2));
        let events = events(&runtime);
        if boundary == "partial_open" {
            assert!(matches!(endpoint, Err(Error::Unquiesced { .. })));
        } else {
            let mut endpoint = endpoint.unwrap();
            if boundary == "active_unregister" {
                registry.unregister(&memory).unwrap();
                runtime.registrations.retire(&memory).unwrap();
                assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
                assert!(fixture.memory.read(4, &mut [0; 4]).is_err());
            } else {
                endpoint.close().unwrap();
                fixture.memory.write(4, &[3; 4]).unwrap();
            }
            drop(endpoint);
            match boundary {
                "idle_unregister" => {
                    registry.unregister(&memory).unwrap();
                    assert!(matches!(
                        runtime.registrations.retire(&memory),
                        Err(Error::Unquiesced { .. })
                    ));
                }
                "eviction" => {
                    let next = registry
                        .register(vec![0; 16].into_boxed_slice(), Permission::LocalOnly)
                        .unwrap();
                    assert!(matches!(
                        runtime.endpoint(&next, 4, 4, 0, Instant::now() + Duration::from_secs(2)),
                        Err(Error::Unquiesced { .. })
                    ));
                    next.write(4, &[7; 4]).unwrap();
                    registry.unregister(&next).unwrap();
                }
                "whole_close" => assert!(matches!(
                    runtime.close(Instant::now() + Duration::from_secs(2)),
                    Err(Error::Unquiesced { .. })
                )),
                _ => {}
            }
        }
        if boundary != "drop" {
            assert!(
                runtime
                    .close(Instant::now() + Duration::from_secs(2))
                    .is_err()
            );
            assert!(
                runtime
                    .close(Instant::now() + Duration::from_secs(2))
                    .is_err()
            );
        }
        if memory.is_active() {
            registry.unregister(&memory).unwrap();
        }
        drop(memory);
        drop(fixture.memory);
        drop(runtime);
        let stats = *usage.lock().unwrap();
        assert_eq!((stats.persistent_mrs, stats.persistent_mr_bytes), (1, 16));
        assert_eq!(usage.lock().unwrap().retained_persistent_mrs, 1);
        assert!(fixture.owner.upgrade().is_some());
        assert_eq!(fixture.drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| **event == "deregister_local")
                .count(),
            1
        );
        assert_eq!(registry.usage().unwrap().0, 1);
    }
}

#[test]
fn engine_close_waits_for_native_close_already_started_by_unregister() {
    use std::thread;
    let registry = MemoryRegistry::new(SourceEpoch([7; 16]), 2, 32);
    let memory = registry
        .register(vec![7; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let runtime = Arc::new(limited_runtime("direct:mr_close_pause", 1, 16));
    runtime
        .endpoint(&memory, 4, 4, 0, Instant::now() + Duration::from_secs(2))
        .unwrap()
        .close()
        .unwrap();
    let pause = crate::rdma::tests::pause(&runtime);
    let events = events(&runtime);
    registry.unregister(&memory).unwrap();
    let retiring = Arc::clone(&runtime);
    let retired_memory = memory.clone();
    let cleanup = thread::spawn(move || retiring.registrations.retire(&retired_memory));
    let (lock, changed) = &*pause;
    let guard = lock.lock().unwrap();
    let (guard, timed) = changed
        .wait_timeout_while(guard, std::time::Duration::from_secs(2), |state| !state.0)
        .unwrap();
    assert!(!timed.timed_out());
    drop(guard);
    assert_eq!(runtime.stats().unwrap().persistent_mr_bytes, 16);
    let next = registry
        .register(vec![0; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    assert!(matches!(
        runtime.endpoint(&next, 4, 4, 0, Instant::now() + Duration::from_secs(2)),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(runtime.close(Instant::now()), Err(Error::Timeout)));
    assert_eq!(runtime.stats().unwrap().persistent_mr_bytes, 16);
    let start = Arc::new(std::sync::Barrier::new(3));
    let mut waiters = Vec::new();
    for _ in 0..2 {
        let closing = Arc::clone(&runtime);
        let start = Arc::clone(&start);
        waiters.push(thread::spawn(move || {
            start.wait();
            closing.close(Instant::now() + Duration::from_secs(2))
        }));
    }
    start.wait();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !runtime.registrations.state.lock().unwrap().closed {
        assert!(std::time::Instant::now() < deadline);
        thread::yield_now();
    }
    assert!(waiters.iter().all(|waiter| !waiter.is_finished()));
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    lock.lock().unwrap().1 = true;
    changed.notify_all();
    cleanup.join().unwrap().unwrap();
    for waiter in waiters {
        waiter.join().unwrap().unwrap();
    }
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "dealloc_pd")
            .count(),
        1
    );
}
