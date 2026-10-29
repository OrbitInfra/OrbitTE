use std::time::{Duration, Instant};

use super::*;
use crate::transport::rdma::{RdmaTransport, tests::FakeDevice, tests::runtime};
use std::sync::Barrier;
use std::thread;

pub(crate) fn events(runtime: &RdmaTransport) -> Arc<Mutex<Vec<&'static str>>> {
    let cache = runtime.context_set.lock().unwrap();
    let owner = cache
        .slots
        .iter()
        .find_map(|slot| slot.owner.as_ref())
        .unwrap();
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("device is not open");
    };
    Arc::clone(&unsafe { &*resources.handles.context.cast::<FakeDevice>() }.events)
}

#[cfg(feature = "cuda")]
pub(crate) fn after_open(runtime: &RdmaTransport, hook: impl FnOnce() + Send + 'static) {
    let owner = runtime.device().unwrap();
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("device is not open");
    };
    let device = unsafe { &*resources.handles.context.cast::<FakeDevice>() };
    assert!(
        device
            .after_open
            .lock()
            .unwrap()
            .replace(Box::new(hook))
            .is_none()
    );
}

pub(crate) fn pause(runtime: &RdmaTransport) -> Arc<(Mutex<(bool, bool)>, std::sync::Condvar)> {
    pause_device(&runtime.device().unwrap())
}

pub(in crate::transport::rdma) fn pause_device(
    owner: &RdmaContext,
) -> Arc<(Mutex<(bool, bool)>, std::sync::Condvar)> {
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("device is not open");
    };
    Arc::clone(&unsafe { &*resources.handles.context.cast::<FakeDevice>() }.pause)
}

#[test]
fn slices_reuse_one_device_and_explicit_close_is_permanent() {
    let runtime = runtime("ok");
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    for _ in 0..16 {
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        endpoint.close().unwrap();
    }
    let events = events(&runtime);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.opened_devices,
            stats.active_devices,
            stats.active_endpoints
        ),
        (1, 1, 0)
    );
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "open_device")
            .count(),
        1
    );
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    let closed = events.lock().unwrap().clone();
    assert_eq!(&closed[closed.len() - 2..], &["dealloc_pd", "close_device"]);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert!(matches!(
        runtime.staging_endpoint(8, 0),
        Err(Error::Unavailable(_))
    ));
    drop(runtime);
    assert_eq!(*events.lock().unwrap(), closed);
}

#[test]
fn endpoint_owners_keep_device_live_after_runtime_drop() {
    let mut runtime = runtime("ok");
    runtime.config.max_endpoints = 2;
    runtime.config.max_staging_bytes = 16;
    let mut first = runtime.staging_endpoint(8, 0).unwrap();
    let mut second = runtime.staging_endpoint(8, 0).unwrap();
    let events = events(&runtime);
    let usage = Arc::clone(&runtime.usage);
    drop(runtime);
    first.close().unwrap();
    drop(first);
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    assert_eq!(usage.lock().unwrap().active_devices, 1);
    second.close().unwrap();
    drop(second);
    assert_eq!(usage.lock().unwrap().active_devices, 0);
    assert_eq!(usage.lock().unwrap().active_endpoints, 0);
    let events = events.lock().unwrap();
    assert_eq!(&events[events.len() - 2..], &["dealloc_pd", "close_device"]);
}

#[test]
fn concurrent_slices_and_closers_construct_and_destroy_device_once() {
    let mut runtime = runtime("ok");
    runtime.config.max_endpoints = 8;
    runtime.config.max_staging_bytes = 64;
    let runtime = Arc::new(runtime);
    let barrier = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let runtime = Arc::clone(&runtime);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
            barrier.wait();
            barrier.wait();
            endpoint.close().unwrap();
        }));
    }
    barrier.wait();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.opened_devices,
            stats.active_devices,
            stats.active_endpoints
        ),
        (1, 1, 8)
    );
    let events = events(&runtime);
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let mut closers = Vec::new();
    for _ in 0..8 {
        let runtime = Arc::clone(&runtime);
        closers.push(thread::spawn(move || {
            runtime.close(Instant::now() + Duration::from_secs(2))
        }));
    }
    for closer in closers {
        closer.join().unwrap().unwrap();
    }
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    let events = events.lock().unwrap();
    assert_eq!(events.iter().filter(|e| **e == "open_device").count(), 1);
    assert_eq!(events.iter().filter(|e| **e == "dealloc_pd").count(), 1);
    assert_eq!(events.iter().filter(|e| **e == "close_device").count(), 1);
}

#[test]
fn device_teardown_failures_are_retained_without_retry_from_close_or_drop() {
    for (mode, operation) in [("close3", "ibv_dealloc_pd"), ("close4", "ibv_close_device")] {
        let runtime = Arc::new(runtime(mode));
        runtime.staging_endpoint(8, 0).unwrap().close().unwrap();
        let events = events(&runtime);
        let usage = Arc::clone(&runtime.usage);
        let mut closers = Vec::new();
        for _ in 0..4 {
            let runtime = Arc::clone(&runtime);
            closers.push(thread::spawn(move || {
                runtime.close(Instant::now() + Duration::from_secs(2))
            }));
        }
        for closer in closers {
            assert!(
                matches!(closer.join().unwrap(), Err(Error::Unquiesced { operation: op, code: 16 }) if op == operation)
            );
        }
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_devices,
                stats.retained_devices,
                stats.active_endpoints
            ),
            (1, 1, 0)
        );
        let retained = events.lock().unwrap().clone();
        assert_eq!(
            retained
                .iter()
                .filter(|e| **e
                    == if mode == "close3" {
                        "dealloc_pd"
                    } else {
                        "close_device"
                    })
                .count(),
            1
        );
        assert!(matches!(
            runtime.staging_endpoint(8, 0),
            Err(Error::Unavailable(_))
        ));
        drop(runtime);
        assert_eq!(*events.lock().unwrap(), retained);
        assert_eq!(usage.lock().unwrap().active_devices, 1);
    }
}

#[test]
fn partial_device_construction_cleans_up_or_poison_retains_its_charge() {
    for mode in [
        "device_missing",
        "device_open",
        "device_open_close",
        "device_pd_close",
    ] {
        let runtime = runtime(mode);
        let retained = mode.ends_with("close");
        let error = runtime.staging_endpoint(8, 0).err().unwrap();
        assert_eq!(
            matches!(error, Error::Unquiesced { .. }),
            retained,
            "{mode}: {error:?}"
        );
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.staging_bytes,
                stats.opened_devices
            ),
            (0, 0, 0)
        );
        assert_eq!(
            (stats.active_devices, stats.retained_devices),
            (usize::from(retained), usize::from(retained))
        );
        if retained {
            assert!(matches!(
                runtime.staging_endpoint(8, 0),
                Err(Error::Unquiesced { .. })
            ));
            assert!(matches!(
                runtime.close(Instant::now() + Duration::from_secs(2)),
                Err(Error::Unquiesced { .. })
            ));
        } else {
            runtime
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
        }
    }
}

#[test]
fn close_cannot_destroy_a_device_while_any_endpoint_still_owns_it() {
    let runtime = runtime("ok");
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    let events = events(&runtime);
    assert!(matches!(runtime.close(Instant::now()), Err(Error::Timeout)));
    assert!(matches!(
        runtime.staging_endpoint(1, 0),
        Err(Error::Unavailable(_)) | Err(Error::ResourceExhausted)
    ));
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    endpoint.close().unwrap();
    // Closed endpoints still retain their stable output and the device until drop.
    assert!(matches!(runtime.close(Instant::now()), Err(Error::Timeout)));
    drop(endpoint);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
}

pub(crate) fn emit_async(runtime: &RdmaTransport, kind: u8) {
    let owner = runtime.device().unwrap();
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("device is not open");
    };
    unsafe { &*resources.handles.context.cast::<FakeDevice>() }.emit_async(kind);
}

#[test]
fn native_index_reallocation_cannot_repeat_any_burned_key_or_return_burns_on_close() {
    let runtime = runtime("ok");
    let mut published = std::collections::HashSet::new();
    for index in 0..255 {
        let mut endpoint = runtime.staging_endpoint(8, 1).unwrap();
        assert_eq!(endpoint.incarnation().unwrap().generation, index + 1);
        assert_eq!(endpoint.grant_generation().unwrap(), index + 1);
        endpoint.connect(&endpoint.descriptor()).unwrap();
        endpoint.bind_window().unwrap();
        assert!(published.insert(endpoint.grant().unwrap().rkey));
        endpoint.invalidate_window().unwrap();
        endpoint.close().unwrap();
        drop(endpoint);
        assert_eq!(runtime.stats().unwrap().window_key_indices, 1);
    }
    assert!(matches!(
        runtime.staging_endpoint(8, 1),
        Err(Error::ResourceExhausted)
    ));
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (stats.burned_window_keys, stats.window_key_exhaustions),
        (255, 1)
    );
    assert_eq!(
        (
            stats.completed_window_binds,
            stats.completed_window_invalidations
        ),
        (255, 255)
    );
    assert_eq!(
        (
            stats.active_windows,
            stats.active_endpoints,
            stats.staging_bytes,
            stats.work_requests
        ),
        (1, 0, 0, 0)
    );
    assert_eq!(stats.exhausted_windows, 1);
    // Exhausted remote authority must not silently turn into a local MR or
    // erase PD history. Local-only requests need no remote key.
    runtime.staging_endpoint(8, 0).unwrap().close().unwrap();
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().window_key_indices, 0);
    assert_eq!(runtime.stats().unwrap().burned_window_keys, 255);
}

#[test]
fn key_index_limit_and_unused_grants_keep_real_owner_cleanup_and_history() {
    let mut runtime = runtime("key_indices");
    runtime.config.max_window_key_indices = 1;
    let mut first = runtime.staging_endpoint(8, 1).unwrap();
    let first_key = first.descriptor().rkey;
    first.close().unwrap();
    drop(first);
    assert!(matches!(
        runtime.staging_endpoint(8, 1),
        Err(Error::ResourceExhausted)
    ));
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.window_key_indices,
            stats.burned_window_keys,
            stats.window_key_exhaustions
        ),
        (1, 1, 1)
    );
    assert_eq!(
        (
            stats.opened_windows,
            stats.active_windows,
            stats.active_endpoints,
            stats.staging_bytes
        ),
        (2, 0, 0, 0)
    );
    assert_eq!(stats.completed_window_binds, 0);
    assert_ne!(first_key & !255, 0);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().window_key_indices, 0);

    let runtime = super::super::tests::runtime("ok");
    let first = runtime.staging_endpoint(8, 1).unwrap();
    let key = first.descriptor().rkey;
    let first_id = first.incarnation().unwrap();
    drop(first); // No BIND: the reserved key still cannot be returned.
    let local = runtime.staging_endpoint(8, 0).unwrap();
    let local_id = local.incarnation().unwrap();
    assert!(local.grant_generation().is_err());
    drop(local);
    let second = runtime.staging_endpoint(8, 1).unwrap();
    assert_eq!(second.incarnation().unwrap().domain, first_id.domain);
    assert_eq!(
        (
            first_id.generation,
            local_id.generation,
            second.incarnation().unwrap().generation
        ),
        (1, 2, 3)
    );
    assert_eq!(second.grant_generation().unwrap(), 3);
    assert_ne!(second.descriptor().rkey, key);
    drop(second);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    let restarted = super::super::tests::runtime("ok");
    let endpoint = restarted.staging_endpoint(8, 1).unwrap();
    assert_ne!(endpoint.incarnation().unwrap().domain, first_id.domain);
    assert_eq!(endpoint.incarnation().unwrap().generation, 1);
    drop(endpoint);
    restarted
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn connection_and_grant_counters_refuse_overflow_before_native_construction() {
    for source_grant in [false, true] {
        let runtime = runtime("ok");
        let owner = runtime.device().unwrap();
        {
            let mut state = owner.state.lock().unwrap();
            let State::Open(resources) = &mut *state else {
                panic!("closed")
            };
            if source_grant {
                resources.next_grant = u64::MAX;
            } else {
                resources.next_connection = u64::MAX;
            }
        }
        for _ in 0..2 {
            assert!(matches!(
                runtime.staging_endpoint(8, 1),
                Err(Error::ResourceExhausted)
            ));
        }
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.opened_windows,
                stats.burned_window_keys
            ),
            (0, 0, 0)
        );
        assert!(!events(&runtime).lock().unwrap().contains(&"open_endpoint"));
        if source_grant {
            assert!(matches!(
                runtime.staging_endpoint(8, 0),
                Err(Error::ResourceExhausted)
            ));
        }
        runtime
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
}

#[test]
fn concurrent_key_reservations_and_retained_cleanup_preserve_the_pd_domain() {
    let mut runtime = runtime("ok");
    runtime.config.max_endpoints = 16;
    runtime.config.max_staging_bytes = 128;
    let runtime = Arc::new(runtime);
    let barrier = Arc::new(Barrier::new(17));
    let mut workers = Vec::new();
    for _ in 0..16 {
        let runtime = Arc::clone(&runtime);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let endpoint = runtime.staging_endpoint(8, 1).unwrap();
            let key = endpoint.descriptor().rkey;
            let identity = endpoint.incarnation().unwrap();
            barrier.wait();
            drop(endpoint);
            (key, identity)
        }));
    }
    barrier.wait();
    let mut keys = std::collections::HashSet::new();
    let mut identities = std::collections::HashSet::new();
    for worker in workers {
        let (key, id) = worker.join().unwrap();
        assert!(keys.insert(key));
        assert!(identities.insert(id.encode()));
    }
    assert_eq!(
        (
            runtime.stats().unwrap().window_key_indices,
            runtime.stats().unwrap().burned_window_keys
        ),
        (1, 16)
    );
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();

    let retained = super::super::tests::runtime("window_close");
    let mut endpoint = retained.staging_endpoint(8, 1).unwrap();
    assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
    drop(endpoint);
    assert!(
        retained
            .close(Instant::now() + Duration::from_secs(2))
            .is_err()
    );
    let stats = retained.stats().unwrap();
    assert_eq!(
        (
            stats.window_key_indices,
            stats.burned_window_keys,
            stats.active_windows,
            stats.retained_windows,
            stats.active_devices
        ),
        (1, 1, 1, 1, 1)
    );
}

#[test]
fn reused_grant_exhaustion_and_pre_open_async_fault_preserve_its_creating_identity() {
    use crate::engine::tests::{rdma_fixture, source_runtime};
    use crate::{Permission, TransferOp, TransferRequest, Transport};
    for asynchronous in [false, true] {
        let source = rdma_fixture("direct:copy");
        let requester = rdma_fixture(if asynchronous {
            "direct:async_copy_reuse_open_pause"
        } else {
            "direct:copy"
        });
        let local = requester
            .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(vec![0x29; 8].into_boxed_slice(), Permission::RemoteRead)
            .unwrap();
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        assert!(
            requester
                .submit_and_wait(
                    TransferOp::Read,
                    &peer,
                    &[TransferRequest {
                        transport: Transport::Rdma,
                        local_address: local.base_address(),
                        remote_address: remote.base_address(),
                        length: 8,
                    }],
                    Duration::from_secs(3)
                )
                .unwrap()
                .is_complete()
        );
        let runtime = source_runtime(&requester);
        let owner = runtime.device().unwrap();
        let progress = runtime.progress(Arc::clone(&owner)).unwrap();
        let claimed = progress
            .claim(|_| true, Instant::now() + Duration::from_secs(3))
            .unwrap()
            .unwrap();
        let old_identity = claimed.incarnation().unwrap();
        if asynchronous {
            let pause = pause(runtime);
            thread::scope(|scope| {
                let open = scope.spawn(|| {
                    runtime.endpoint(
                        &local,
                        0,
                        8,
                        0,
                        Instant::now() + Duration::from_secs(3),
                        Some(claimed),
                    )
                });
                let (lock, changed) = &*pause;
                let (state, timed) = changed
                    .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(2), |s| !s.0)
                    .unwrap();
                assert!(!timed.timed_out());
                drop(state);
                emit_async(runtime, 3);
                let end = Instant::now() + Duration::from_secs(2);
                while runtime.stats().unwrap().active_endpoints != 0 && Instant::now() < end {
                    thread::yield_now();
                }
                let drained = runtime.stats().unwrap().active_endpoints == 0;
                lock.lock().unwrap().1 = true;
                changed.notify_all();
                let result = open.join().unwrap();
                assert!(
                    drained,
                    "pre-open fault did not physically drain accepted connection"
                );
                assert!(matches!(
                    result,
                    Err(Error::Backend { .. } | Error::Unavailable(_))
                ));
            });
        } else {
            let mut state = owner.state.lock().unwrap();
            let State::Open(resources) = &mut *state else {
                panic!("closed")
            };
            resources.next_grant = u64::MAX;
            drop(state);
            assert!(matches!(
                runtime.endpoint(
                    &local,
                    0,
                    8,
                    0,
                    Instant::now() + Duration::from_secs(3),
                    Some(claimed)
                ),
                Err(Error::ResourceExhausted)
            ));
            let state = owner.state.lock().unwrap();
            let State::Open(resources) = &*state else {
                panic!("closed")
            };
            assert_eq!(resources.next_grant, u64::MAX);
            assert_eq!(resources.domain, old_identity.domain);
        }
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.idle_endpoints,
                stats.active_grants,
                stats.retained_endpoints,
                stats.retained_grants,
                stats.direct_bytes
            ),
            (0, 0, 0, 0, 0, 0)
        );
        assert_eq!((stats.opened_connections, stats.closed_connections), (1, 1));
        local.write(0, &[0x55; 8]).unwrap();
        requester.close(Duration::from_secs(3)).unwrap();
        source.close(Duration::from_secs(3)).unwrap();
    }
}

pub(in crate::transport::rdma) fn change_route(
    runtime: &RdmaTransport,
    change: impl FnOnce(&mut RdmaAddress),
) {
    let cache = runtime.context_set.lock().unwrap();
    let state = cache
        .slots
        .iter()
        .find_map(|slot| slot.owner.as_ref())
        .unwrap()
        .state
        .lock()
        .unwrap();
    let State::Open(resources) = &*state else {
        panic!("not open");
    };
    let mut route = resources.address.clone();
    change(&mut route);
    *unsafe { &*resources.handles.context.cast::<FakeDevice>() }
        .route
        .lock()
        .unwrap() = Some(route);
}

#[test]
fn every_frozen_route_field_fences_the_same_pd_permanently() {
    for field in 0..9 {
        let runtime = runtime("direct:ok");
        assert_eq!(runtime.topology().unwrap(), None);
        let owner = runtime.device().unwrap();
        let domain = owner.domain().unwrap();
        let initial = runtime.topology().unwrap().unwrap();
        assert_eq!(initial.nic_list[0].device_attrs["rdma.selected"], "true");
        change_route(&runtime, |route| match field {
            0 => route.descriptor.gid[15] ^= 1,
            1 => route.descriptor.lid += 1,
            2 => route.descriptor.mtu += 1,
            3 => route.descriptor.link_layer = 2,
            4 => route.gid_type = super::super::discovery::GidType::RoceV2,
            5 => route.net_device = Some("new-netdev".into()),
            6 => route.net_device_index = 7,
            7 => route.gid_index += 1,
            _ => route.port += 1,
        });
        assert!(
            matches!(
                owner.check_address(),
                Err(Error::Backend {
                    operation: "RDMA route changed",
                    code: libc::ESTALE
                })
            ),
            "field {field}"
        );
        let state = owner.state.lock().unwrap();
        let State::Open(resources) = &*state else {
            panic!("old owner lost");
        };
        assert_eq!(resources.domain, domain);
        assert_eq!(runtime.stats().unwrap().opened_devices, 1);
        *unsafe { &*resources.handles.context.cast::<FakeDevice>() }
            .route
            .lock()
            .unwrap() = None;
        drop(state);
        assert!(
            owner.check_address().is_err(),
            "restoring a route cannot heal the old PD"
        );
        assert_eq!(runtime.topology().unwrap().unwrap(), initial);
        runtime
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        assert_eq!(runtime.stats().unwrap().active_devices, 0);
        assert_eq!(runtime.topology().unwrap(), None);
    }
}

#[test]
fn endpoint_construction_and_connect_cannot_bypass_frozen_route_checks() {
    for stage in [0, 1] {
        let runtime = runtime("direct:ok");
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        let native_events = events(&runtime);
        change_route(&runtime, |route| route.descriptor.mtu += 1);
        let result = if stage == 0 {
            endpoint.open(0, 3)
        } else {
            {
                let local = endpoint.local;
                endpoint.connect(&local)
            }
        };
        assert!(result.is_err());
        assert!(!native_events.lock().unwrap().contains(&"connect"));
        let _ = endpoint.close();
        drop(endpoint);
        runtime
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.opened_devices,
                stats.opened_connections,
                stats.active_endpoints,
                stats.active_devices
            ),
            (1, 1, 0, 0)
        );
    }
}

type Probe = (String, Arc<Mutex<Vec<&'static str>>>);
thread_local! {
    static PROBES: std::cell::RefCell<Vec<Probe>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn probe_candidates(config: &RdmaConfig) -> Result<(crate::Topology, Vec<RdmaAddress>)> {
    let (_, mut candidates) = super::super::tests::discover(&RdmaConfig::new("ok"))?;
    let mut first = candidates[0].clone();
    first.device = config.device.clone().unwrap();
    candidates.insert(0, first);
    let topology = crate::Topology {
        nic_list: candidates
            .iter()
            .map(|route| crate::NicEntry {
                name: route.device.clone(),
                pci_bus_id: None,
                kind: crate::NicType::Rdma,
                numa_node: None,
                device_attrs: Default::default(),
            })
            .collect(),
        mem_list: vec![],
    };
    Ok((topology, candidates))
}

unsafe extern "C" fn probe_open(
    handles: *mut sys::DeviceHandles,
    name: *const std::ffi::c_char,
) -> i32 {
    let code = unsafe { super::super::tests::open_device(handles, name) };
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    PROBES.with(|probes| {
        probes
            .borrow_mut()
            .push((device.mode.clone(), Arc::clone(&device.events)))
    });
    code
}

#[test]
fn candidate_fallback_requires_actual_cleanup_and_retained_failure_stops_discovery() {
    static VERBS: sys::Verbs = sys::Verbs {
        discover: probe_candidates,
        open_device: probe_open,
        ..super::super::tests::VERBS
    };
    for (mode, retained) in [
        ("device_open", false),
        ("route_rejected", false),
        ("device_open_close", true),
        ("device_pd_close", true),
    ] {
        PROBES.with(|probes| probes.borrow_mut().clear());
        let mut runtime = runtime(mode);
        runtime.verbs = &VERBS;
        let result = runtime.device();
        assert_eq!(matches!(result, Err(Error::Unquiesced { .. })), retained);
        let probes = PROBES.with(|probes| probes.borrow().clone());
        assert_eq!(probes.len(), if retained { 1 } else { 2 });
        let first = probes[0].1.lock().unwrap().clone();
        if !retained {
            assert_eq!(first.last(), Some(&"close_device"));
            assert_eq!(
                runtime.topology().unwrap().unwrap().nic_list[1].device_attrs["rdma.selected"],
                "true"
            );
            runtime
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
            assert_eq!(runtime.stats().unwrap().active_devices, 0);
        } else {
            assert!(matches!(runtime.device(), Err(Error::Unquiesced { .. })));
            assert!(matches!(
                runtime.close(Instant::now() + Duration::from_secs(2)),
                Err(Error::Unquiesced { .. })
            ));
            assert_eq!(runtime.stats().unwrap().retained_devices, 1);
            drop(runtime);
            assert_eq!(*probes[0].1.lock().unwrap(), first, "no hidden close retry");
        }
    }
}

#[test]
fn second_native_query_failure_cannot_reopen_a_restored_tuple_or_replace_the_pd() {
    let runtime = runtime("direct:route_second_query_error");
    assert!(runtime.staging_endpoint(8, 0).is_err());
    let owner = runtime.device().unwrap();
    let native_events = events(&runtime);
    assert!(owner.check_address().is_err());
    assert!(runtime.staging_endpoint(8, 0).is_err());
    assert_eq!(
        native_events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "open_endpoint")
            .count(),
        1
    );
    assert_eq!(runtime.stats().unwrap().opened_devices, 1);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
}

impl RdmaContext {
    pub(in crate::transport::rdma) fn open(
        config: &RdmaConfig,
        verbs: &'static sys::Verbs,
        usage: Arc<Mutex<RdmaStats>>,
        counters: Arc<events::Counters>,
    ) -> Result<Arc<Self>> {
        let (_, candidates) = (verbs.discover)(config)?;
        let mut last_error = Error::Unavailable("no RDMA discovery candidate");
        for address in candidates {
            match Self::open_selected(
                config,
                verbs,
                Arc::clone(&usage),
                Arc::clone(&counters),
                address,
                Arc::default(),
            ) {
                Ok(owner) => return Ok(owner),
                Err(error @ Error::Unquiesced { .. }) => return Err(error),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }
}

#[path = "recovery.rs"]
mod recovery;

#[path = "native_context_recovery.rs"]
mod native_context_recovery;

#[path = "context/key_domain.rs"]
mod key_domain;
