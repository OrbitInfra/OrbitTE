#[path = "buffers/publication.rs"]
pub(crate) mod publication;

use std::time::{Duration, Instant};

use super::*;
use crate::memory::MemoryRegistry;
use crate::transport::rdma::tests::{events, runtime};
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
                None,
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

fn limited_runtime(
    mode: &str,
    regions: usize,
    bytes: usize,
) -> crate::transport::rdma::RdmaTransport {
    let mut runtime = runtime(mode);
    runtime.config.max_endpoints = 3;
    runtime.config.max_cached_mrs = regions;
    runtime.config.max_cached_mr_bytes = bytes;
    runtime.registrations =
        LocalBufferManager::new(&runtime.config, Arc::clone(&runtime.usage), runtime.verbs);
    runtime
}

#[test]
fn idle_lru_eviction_obeys_physical_limits_and_never_evicts_a_live_mr_borrow() {
    for permission in [0, 2] {
        let registry = MemoryRegistry::new(SourceEpoch([2; 16]), 3, 48);
        let a = registry
            .register(vec![1; 16].into_boxed_slice(), Permission::RemoteReadWrite)
            .unwrap();
        let b = registry
            .register(vec![2; 16].into_boxed_slice(), Permission::RemoteReadWrite)
            .unwrap();
        let runtime = limited_runtime("direct:ok", 1, 16);
        let mut first = runtime
            .endpoint(
                &a,
                4,
                4,
                permission,
                Instant::now() + Duration::from_secs(2),
                None,
            )
            .unwrap();
        assert!(matches!(
            runtime.endpoint(
                &b,
                4,
                4,
                permission,
                Instant::now() + Duration::from_secs(2),
                None
            ),
            Err(Error::ResourceExhausted)
        ));
        first.close().unwrap();
        // A closed endpoint can still own its output while its MR borrow is idle.
        let mut next = runtime
            .endpoint(
                &b,
                4,
                4,
                permission,
                Instant::now() + Duration::from_secs(2),
                None,
            )
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
            too_small.endpoint(
                &a,
                4,
                1,
                permission,
                Instant::now() + Duration::from_secs(2),
                None
            ),
            Err(Error::ResourceExhausted)
        ));
        assert_eq!(too_small.stats().unwrap().persistent_mrs, 0);
        too_small
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
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
        .endpoint(
            &memory,
            4,
            4,
            0,
            Instant::now() + Duration::from_secs(2),
            None,
        )
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
        .endpoint(
            &memory,
            8,
            4,
            0,
            Instant::now() + Duration::from_secs(2),
            None,
        )
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
        .endpoint(
            &new_view,
            4,
            4,
            0,
            Instant::now() + Duration::from_secs(2),
            None,
        )
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
    for permission in [0, 2] {
        use std::thread;
        let registry = MemoryRegistry::new(SourceEpoch([4; 16]), 1, 16);
        let memory = registry
            .register(vec![4; 16].into_boxed_slice(), Permission::RemoteReadWrite)
            .unwrap();
        let runtime = Arc::new(limited_runtime("direct:register_pause", 1, 16));
        let pause = crate::transport::rdma::tests::pause(&runtime);
        let events = events(&runtime);
        let task_runtime = Arc::clone(&runtime);
        let task_memory = memory.clone();
        let worker = thread::spawn(move || {
            let mut endpoint = task_runtime
                .endpoint(
                    &task_memory,
                    4,
                    4,
                    permission,
                    Instant::now() + Duration::from_secs(2),
                    None,
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
            runtime.endpoint(
                &memory,
                8,
                4,
                permission,
                Instant::now() + Duration::from_millis(10),
                None
            ),
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
                .filter(|event| **event
                    == if permission == 0 {
                        "register_local"
                    } else {
                        "register_parent"
                    })
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| **event
                    == if permission == 0 {
                        "deregister_local"
                    } else {
                        "deregister_parent"
                    })
                .count(),
            1
        );
    }
}

#[test]
fn concurrent_request_ranges_share_one_creating_mr_and_keep_independent_dma_leases() {
    for permission in [0, 2] {
        use std::sync::Barrier;
        use std::thread;
        let registry = MemoryRegistry::new(SourceEpoch([5; 16]), 1, 16);
        let memory = registry
            .register(vec![5; 16].into_boxed_slice(), Permission::RemoteReadWrite)
            .unwrap();
        let runtime = Arc::new(limited_runtime("direct:register_pause", 1, 16));
        let pause = crate::transport::rdma::tests::pause(&runtime);
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
                        permission,
                        Instant::now() + Duration::from_secs(2),
                        None,
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
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.persistent_source_mrs,
                stats.opened_source_mrs,
                stats.reused_source_mrs
            ),
            (
                usize::from(permission != 0),
                u64::from(permission != 0),
                u64::from(permission != 0)
            )
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
}

#[test]
fn deregistration_failure_keeps_actual_owner_device_and_budget_and_never_retries() {
    for permission in [0, 2] {
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
                .register_host(
                    Arc::clone(&fixture.memory),
                    0,
                    16,
                    Permission::RemoteReadWrite,
                )
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
            let endpoint = runtime.endpoint(
                &memory,
                4,
                4,
                permission,
                Instant::now() + Duration::from_secs(2),
                None,
            );
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
                            .register(vec![0; 16].into_boxed_slice(), Permission::RemoteReadWrite)
                            .unwrap();
                        assert!(matches!(
                            runtime.endpoint(
                                &next,
                                4,
                                4,
                                permission,
                                Instant::now() + Duration::from_secs(2),
                                None
                            ),
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
            assert_eq!(
                (stats.persistent_source_mrs, stats.retained_source_mrs),
                (usize::from(permission != 0), usize::from(permission != 0))
            );
            assert!(fixture.owner.upgrade().is_some());
            assert_eq!(fixture.drops.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
            assert_eq!(
                events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|event| **event
                        == if permission == 0 {
                            "deregister_local"
                        } else {
                            "deregister_parent"
                        })
                    .count(),
                1
            );
            assert_eq!(registry.usage().unwrap().0, 1);
        }
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
        .endpoint(
            &memory,
            4,
            4,
            0,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap()
        .close()
        .unwrap();
    let pause = crate::transport::rdma::tests::pause(&runtime);
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
        runtime.endpoint(
            &next,
            4,
            4,
            0,
            Instant::now() + Duration::from_secs(2),
            None
        ),
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

#[test]
fn local_and_source_capabilities_charge_separate_parents_and_sources_share_exact_mw_grants() {
    let registry = MemoryRegistry::new(SourceEpoch([8; 16]), 1, 24);
    let memory = registry
        .register(
            vec![0x55; 24].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let runtime = limited_runtime("direct:ok", 2, 48);
    let end = Instant::now() + Duration::from_secs(2);
    let mut local = runtime.endpoint(&memory, 0, 4, 0, end, None).unwrap();
    let local_parent = local
        .record
        .lock()
        .unwrap()
        .resources
        .as_ref()
        .unwrap()
        .grant
        .as_ref()
        .unwrap()
        .handles
        .mr;
    let mut parent: *mut std::ffi::c_void = std::ptr::null_mut();
    let mut keys = std::collections::HashSet::new();
    let mut connections = std::collections::HashSet::new();
    let mut grants = std::collections::HashSet::new();
    for (permission, offset) in [(1, 8), (2, 16), (1, 12)] {
        let mut source = runtime
            .endpoint(&memory, offset, 4, permission, end, None)
            .unwrap();
        let actual_parent = source
            .record
            .lock()
            .unwrap()
            .resources
            .as_ref()
            .unwrap()
            .grant
            .as_ref()
            .unwrap()
            .handles
            .mr;
        assert_ne!(
            actual_parent, local_parent,
            "LOCAL_WRITE must not be upgraded into MW_BIND"
        );
        if parent.is_null() {
            parent = actual_parent;
        } else {
            assert_eq!(parent, actual_parent);
        }
        assert!(connections.insert(source.incarnation().unwrap().encode()));
        assert!(grants.insert(source.grant_generation().unwrap()));
        source.connect(&source.descriptor()).unwrap();
        source.bind_window().unwrap();
        let grant = source.grant().unwrap();
        assert_eq!(grant.address, memory.base_address() + offset as u64);
        assert!(
            keys.insert(grant.rkey),
            "a cached parent cannot resurrect an old MW key"
        );
        assert!(memory.read(offset, &mut [0; 4]).is_err());
        memory.write(4, &[7; 4]).unwrap();
        source.invalidate_window().unwrap();
        assert!(memory.read(offset, &mut [0; 4]).is_err());
        source.close().unwrap();
        memory.write(offset, &[3; 4]).unwrap();
    }
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.persistent_mrs,
            stats.persistent_mr_bytes,
            stats.opened_persistent_mrs,
            stats.reused_persistent_mrs
        ),
        (2, 48, 2, 2)
    );
    assert_eq!(
        (
            stats.persistent_source_mrs,
            stats.opened_source_mrs,
            stats.reused_source_mrs
        ),
        (1, 1, 2)
    );
    assert_eq!((stats.active_windows, stats.burned_window_keys), (0, 3));
    // Retire must reach the idle source parent even when the first local parent
    // is borrowed. The last local borrower then retires independently.
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.persistent_mrs,
            stats.persistent_source_mrs,
            stats.persistent_mr_bytes
        ),
        (1, 0, 24)
    );
    local.close().unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
    drop(local);
    runtime.close(end).unwrap();
}

#[test]
fn source_parent_cannot_bypass_shared_limits_or_release_a_failed_mw_borrow() {
    for (mode, regions, bytes) in [("ok", 1, 48), ("ok", 2, 47), ("window_close", 2, 48)] {
        let registry = MemoryRegistry::new(SourceEpoch([9; 16]), 1, 24);
        let memory = registry
            .register(vec![9; 24].into_boxed_slice(), Permission::RemoteReadWrite)
            .unwrap();
        let runtime = limited_runtime(&format!("direct:{mode}"), regions, bytes);
        let end = Instant::now() + Duration::from_secs(2);
        let mut local = runtime.endpoint(&memory, 0, 4, 0, end, None).unwrap();
        let source = runtime.endpoint(&memory, 8, 4, 2, end, None);
        if mode == "ok" {
            assert!(matches!(source, Err(Error::ResourceExhausted)));
            assert_eq!(
                (
                    runtime.stats().unwrap().persistent_mrs,
                    runtime.stats().unwrap().persistent_source_mrs
                ),
                (1, 0)
            );
            memory.write(8, &[1; 4]).unwrap();
            local.close().unwrap();
            drop(local);
            registry.unregister(&memory).unwrap();
            runtime.registrations.retire(&memory).unwrap();
            runtime.close(end).unwrap();
        } else {
            let mut source = source.unwrap();
            source.connect(&source.descriptor()).unwrap();
            source.bind_window().unwrap();
            source.invalidate_window().unwrap();
            local.close().unwrap();
            drop(local);
            registry.unregister(&memory).unwrap();
            runtime.registrations.retire(&memory).unwrap();
            assert!(matches!(
                source.close(),
                Err(Error::Unquiesced {
                    operation: "ibv_dealloc_mw",
                    ..
                })
            ));
            let stats = runtime.stats().unwrap();
            assert_eq!(
                (
                    stats.persistent_mrs,
                    stats.persistent_source_mrs,
                    stats.persistent_mr_bytes
                ),
                (1, 1, 24)
            );
            // Parent deregistration has not even been attempted: its actual MW
            // still owns the borrower, independently of the completed INV.
            assert_eq!(stats.retained_source_mrs, 0);
            assert_eq!(
                (
                    stats.active_windows,
                    stats.retained_grants,
                    stats.direct_bytes
                ),
                (1, 1, 4)
            );
            assert!(memory.write(8, &[1; 4]).is_err());
            memory.write(4, &[1; 4]).unwrap();
            let cache = runtime.registrations.state.lock().unwrap();
            assert_eq!(cache.entries.len(), 1);
            assert_eq!(cache.entries[0].borrowers, 1);
            assert!(cache.entries[0].retiring);
            drop(cache);
            let log = events(&runtime);
            assert!(!log.lock().unwrap().contains(&"deregister_parent"));
            let before = log.lock().unwrap().clone();
            assert!(source.close().is_err());
            drop(source);
            drop(runtime);
            assert_eq!(
                *log.lock().unwrap(),
                before,
                "failed MW owners never retry during Drop"
            );
        }
    }
}

#[test]
fn retirement_marks_every_capability_before_native_failure_can_interrupt_cleanup() {
    let registry = MemoryRegistry::new(SourceEpoch([10; 16]), 1, 24);
    let memory = registry
        .register(vec![10; 24].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let runtime = limited_runtime("direct:mr_close", 2, 48);
    let end = Instant::now() + Duration::from_secs(2);
    let mut local = runtime.endpoint(&memory, 0, 4, 0, end, None).unwrap();
    local.close().unwrap();
    drop(local);
    let mut source = runtime.endpoint(&memory, 8, 4, 2, end, None).unwrap();
    registry.unregister(&memory).unwrap();
    let log = events(&runtime);
    assert!(matches!(
        runtime.registrations.retire(&memory),
        Err(Error::Unquiesced {
            operation: "ibv_dereg_mr",
            ..
        })
    ));
    {
        let cache = runtime.registrations.state.lock().unwrap();
        assert_eq!(cache.entries.len(), 1);
        let entry = &cache.entries[0];
        assert!(entry.owner.window_bind);
        assert_eq!(entry.borrowers, 1);
        assert!(
            entry.retiring,
            "first native failure must not leave another capability active"
        );
    }
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.persistent_mrs,
            stats.persistent_source_mrs,
            stats.persistent_mr_bytes
        ),
        (2, 1, 48)
    );
    assert_eq!(
        (stats.retained_persistent_mrs, stats.retained_source_mrs),
        (1, 0)
    );
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "deregister_local")
            .count(),
        1
    );
    assert!(!log.lock().unwrap().contains(&"deregister_parent"));
    memory.write(0, &[1; 4]).unwrap();
    assert!(memory.write(8, &[1; 4]).is_err());
    assert!(matches!(source.close(), Err(Error::Unquiesced { .. })));
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_windows,
            stats.retained_persistent_mrs,
            stats.retained_source_mrs
        ),
        (0, 2, 1)
    );
    assert!(memory.write(8, &[1; 4]).is_err());
    let before = log.lock().unwrap().clone();
    drop((source, runtime));
    assert_eq!(
        *log.lock().unwrap(),
        before,
        "retained parents never retry native cleanup"
    );
}

#[test]
fn context_and_capability_select_distinct_physical_mrs_and_retire_every_copy() {
    let registry = MemoryRegistry::new(SourceEpoch([21; 16]), 2, 16);
    let backing = Arc::new(crate::HostMemory::new(vec![0x35; 16].into_boxed_slice()).unwrap());
    let memory = registry
        .register_host(Arc::clone(&backing), 0, 16, Permission::RemoteReadWrite)
        .unwrap();
    let duplicate = registry
        .register_host(backing, 0, 16, Permission::LocalOnly)
        .unwrap();
    let first = limited_runtime("direct:ok", 4, 64);
    let second = runtime("direct:ok");
    let contexts = [first.device().unwrap(), second.device().unwrap()];
    let cache = &first.registrations;
    let mut leases = Vec::new();
    let mut handles = Vec::new();
    for context in &contexts {
        for bind in [false, true] {
            let lease = cache
                .acquire(
                    Arc::clone(context),
                    &memory,
                    bind,
                    Instant::now() + Duration::from_secs(2),
                )
                .unwrap();
            let handle = lease.handle().unwrap();
            assert!(
                !handles.contains(&handle),
                "a physical MR belongs to one context and capability"
            );
            handles.push(handle);
            leases.push(lease);
        }
    }
    for (index, context) in contexts.iter().enumerate() {
        for (capability, bind) in [false, true].into_iter().enumerate() {
            let same = cache
                .acquire(
                    Arc::clone(context),
                    &memory,
                    bind,
                    Instant::now() + Duration::from_secs(2),
                )
                .unwrap();
            assert_eq!(same.handle().unwrap(), handles[2 * index + capability]);
        }
    }
    let stats = first.stats().unwrap();
    assert_eq!(
        (
            stats.persistent_mrs,
            stats.persistent_source_mrs,
            stats.persistent_mr_bytes
        ),
        (4, 2, 64)
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
    assert_eq!(first.stats().unwrap().persistent_mrs, 4);
    registry.unregister(&duplicate).unwrap();
    cache.retire(&memory).unwrap();
    assert_eq!(first.stats().unwrap().persistent_mrs, 2);
    assert!(
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .iter()
            .all(|entry| entry.retiring)
    );
    for lease in &mut leases[2..] {
        lease.release().unwrap();
    }
    assert_eq!(first.stats().unwrap().persistent_mr_bytes, 0);
    drop(leases);
    first
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    second
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn shared_mr_limits_charge_busy_copies_in_every_context() {
    for (regions, bytes) in [(1, 32), (2, 31)] {
        for bind in [false, true] {
            let registry = MemoryRegistry::new(SourceEpoch([22; 16]), 1, 16);
            let memory = registry
                .register(vec![0; 16].into_boxed_slice(), Permission::RemoteReadWrite)
                .unwrap();
            let first = limited_runtime("direct:ok", regions, bytes);
            let second = runtime("direct:ok");
            let a = first.device().unwrap();
            let b = second.device().unwrap();
            let mut lease = first
                .registrations
                .acquire(a, &memory, bind, Instant::now() + Duration::from_secs(2))
                .unwrap();
            assert!(
                matches!(
                    first.registrations.acquire(
                        b,
                        &memory,
                        bind,
                        Instant::now() + Duration::from_secs(2)
                    ),
                    Err(Error::ResourceExhausted)
                ),
                "another PD cannot bypass the aggregate MR budget"
            );
            assert_eq!(first.stats().unwrap().persistent_mrs, 1);
            lease.release().unwrap();
            first
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
            second
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
        }
    }
}

#[test]
fn a_failed_context_registration_preserves_previous_context_owners() {
    for bind in [false, true] {
        for retain in [false, true] {
            let fixture = crate::test_support::external_memory(&[0x26; 16]);
            let registry = MemoryRegistry::new(SourceEpoch([23; 16]), 1, 16);
            let memory = registry
                .register_host(
                    Arc::clone(&fixture.memory),
                    0,
                    16,
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let first = limited_runtime("direct:ok", 2, 32);
            let second = runtime(if retain {
                "direct:register_fail_close1"
            } else {
                "direct:register_fail"
            });
            let a = first.device().unwrap();
            let b = second.device().unwrap();
            let weak_b = Arc::downgrade(&b);
            let cache = &first.registrations;
            let mut healthy = cache
                .acquire(
                    Arc::clone(&a),
                    &memory,
                    bind,
                    Instant::now() + Duration::from_secs(2),
                )
                .unwrap();
            let handle = healthy.handle().unwrap();
            let failure = cache.acquire(
                Arc::clone(&b),
                &memory,
                bind,
                Instant::now() + Duration::from_secs(2),
            );
            if retain {
                assert!(matches!(failure, Err(Error::Unquiesced { .. })));
                assert!(matches!(
                    cache.acquire(a, &memory, bind, Instant::now() + Duration::from_secs(2)),
                    Err(Error::Unquiesced { .. })
                ));
            } else {
                assert!(matches!(
                    failure,
                    Err(Error::Backend {
                        operation: "ibv_reg_mr",
                        ..
                    })
                ));
                let previous = cache
                    .acquire(a, &memory, bind, Instant::now() + Duration::from_secs(2))
                    .unwrap();
                assert_eq!(previous.handle().unwrap(), handle);
            }
            assert_eq!(
                healthy.handle().unwrap(),
                handle,
                "a different context's failure cannot revoke this borrow"
            );
            registry.unregister(&memory).unwrap();
            assert_eq!(cache.retire(&memory).is_err(), retain);
            healthy.release().unwrap();
            assert_eq!(first.stats().unwrap().persistent_mrs, usize::from(retain));
            assert_eq!(
                first.stats().unwrap().retained_persistent_mrs,
                usize::from(retain)
            );
            let events = events(&second);
            let attempts = events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| {
                    **event
                        == if bind {
                            "deregister_parent"
                        } else {
                            "deregister_local"
                        }
                })
                .count();
            assert_eq!(attempts, 1);
            assert_eq!(
                cache
                    .close(Instant::now() + Duration::from_secs(2))
                    .is_err(),
                retain
            );
            drop(b);
            drop(second);
            drop(healthy);
            drop(memory);
            drop(fixture.memory);
            assert_eq!(weak_b.upgrade().is_some(), retain);
            assert_eq!(fixture.owner.upgrade().is_some(), retain);
            if retain {
                assert_eq!(
                    events
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|event| **event
                            == if bind {
                                "deregister_parent"
                            } else {
                                "deregister_local"
                            })
                        .count(),
                    1,
                    "retained native cleanup is never retried"
                );
            } else {
                first
                    .close(Instant::now() + Duration::from_secs(2))
                    .unwrap();
            }
        }
    }
}

#[test]
fn creating_mrs_in_different_contexts_do_not_share_a_registration_wait() {
    let registry = MemoryRegistry::new(SourceEpoch([24; 16]), 1, 16);
    let memory = registry
        .register(vec![0; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let first = limited_runtime("direct:register_pause", 2, 32);
    let second = runtime("direct:ok");
    let a = first.device().unwrap();
    let b = second.device().unwrap();
    let pause = crate::transport::rdma::tests::pause(&first);
    let cache = Arc::clone(&first.registrations);
    let task_memory = memory.clone();
    let task = std::thread::spawn(move || {
        cache.acquire(
            a,
            &task_memory,
            false,
            Instant::now() + Duration::from_secs(5),
        )
    });
    let (lock, changed) = &*pause;
    let (state, timed) = changed
        .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(2), |state| {
            !state.0
        })
        .unwrap();
    assert!(!timed.timed_out() && state.0);
    drop(state);
    let other =
        first
            .registrations
            .acquire(b, &memory, false, Instant::now() + Duration::from_secs(2));
    lock.lock().unwrap().1 = true;
    changed.notify_all();
    let mut original = task.join().unwrap().unwrap();
    let mut other = other.expect("a different PD must not wait for this registration");
    assert_ne!(original.handle().unwrap(), other.handle().unwrap());
    registry.unregister(&memory).unwrap();
    first.registrations.retire(&memory).unwrap();
    original.release().unwrap();
    other.release().unwrap();
    first
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    second
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}
