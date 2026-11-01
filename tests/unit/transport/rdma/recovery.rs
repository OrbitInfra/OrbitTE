use super::*;
use crate::transport::rdma::RdmaStats;
use std::sync::atomic::Ordering;

fn wait(runtime: &RdmaTransport, predicate: impl Fn(RdmaStats) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let stats = runtime.stats().unwrap();
        if predicate(stats) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "periodic progress missing: {stats:?}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn native(owner: &RdmaContext, apply: impl FnOnce(&FakeDevice)) {
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("closed context");
    };
    apply(unsafe { &*resources.handles.context.cast::<FakeDevice>() });
}

pub(super) fn identity(owner: &RdmaContext) -> (usize, usize, [u8; 16]) {
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("closed context");
    };
    (
        resources.handles.context as usize,
        resources.handles.pd as usize,
        resources.domain,
    )
}

struct Release(Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.0.lock().unwrap_or_else(|p| p.into_inner()).1 = true;
        self.0.1.notify_all();
    }
}

#[test]
fn monitor_keeps_acking_during_native_drain_and_recovers_without_new_requests() {
    let mut runtime = runtime("async_qp_pause");
    runtime.config.num_lanes = 2;
    let mut grant = runtime.staging_endpoint(8, 0).unwrap();
    let identity = grant.incarnation().unwrap();
    let owner = runtime.device().unwrap();
    let gate = pause_device(&owner);
    let release = Release(Arc::clone(&gate));
    emit_async(&runtime, ibverbs_sys::IBV_EVENT_PORT_ERR as u8);
    let end = Instant::now() + Duration::from_secs(2);
    while !gate.0.lock().unwrap().0 {
        assert!(Instant::now() < end);
        thread::yield_now();
    }
    let ticks = runtime.stats().unwrap().monitor_ticks;
    wait(&runtime, |s| s.monitor_ticks > ticks);
    assert!(owner.asynchronous.paused());
    assert_eq!(runtime.stats().unwrap().route_recoveries, 0);
    assert!(
        runtime
            .allocate(8, crate::MemoryLocation::Cpu(None), u64::MAX)
            .is_err()
    );
    emit_async(&runtime, ibverbs_sys::IBV_EVENT_COMM_EST as u8);
    wait(&runtime, |s| s.acknowledged_async_events == 2);
    drop(release);
    wait(&runtime, |s| {
        s.route_recoveries == 1 && s.progress_lanes == 0
    });
    assert_eq!(runtime.stats().unwrap().active_cqs, 0);
    assert_eq!(owner.domain().unwrap(), identity.domain);
    assert!(grant.progress.handle(false).is_err());
    let _ = grant.close();
    drop(grant);
    let mut next = runtime.staging_endpoint(8, 0).unwrap();
    assert_eq!(next.incarnation().unwrap().domain, identity.domain);
    assert_ne!(next.incarnation().unwrap(), identity);
    next.close().unwrap();
    drop(next);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn periodic_queries_detect_down_ports_and_publish_changed_addresses_without_events() {
    let runtime = runtime("ok");
    let mut grant = runtime.staging_endpoint(8, 0).unwrap();
    let owner = runtime.device().unwrap();
    let identity = grant.incarnation().unwrap();
    let old = owner.address().unwrap();
    native(&owner, |device| {
        device.route_error.store(libc::ENETDOWN, Ordering::Release)
    });
    wait(&runtime, |s| {
        s.progress_recoveries == 1 && s.active_cqs == 0
    });
    assert!(owner.asynchronous.paused());
    assert_eq!(owner.address().unwrap(), old);
    assert!(
        runtime
            .allocate(8, crate::MemoryLocation::Cpu(None), u64::MAX)
            .is_err()
    );
    let mut new = old.clone();
    new.descriptor.lid += 1;
    new.descriptor.gid[15] ^= 1;
    new.descriptor.mtu = 4;
    native(&owner, |device| {
        *device.route.lock().unwrap() = Some(new.clone());
        device.route_error.store(0, Ordering::Release);
    });
    wait(&runtime, |s| s.route_recoveries == 1);
    assert_eq!(owner.address().unwrap(), new);
    assert_eq!(owner.domain().unwrap(), identity.domain);
    let topology = runtime.topology().unwrap().unwrap();
    assert_eq!(
        topology.nic_list[0].device_attrs["rdma.lid"],
        new.descriptor.lid.to_string()
    );
    assert_eq!(topology.nic_list[0].device_attrs["rdma.mtu_bytes"], "2048");
    assert!(
        runtime
            .allocate(8, crate::MemoryLocation::Cpu(None), 1)
            .is_ok()
    );
    let _ = grant.close();
    drop(grant);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn address_changes_while_active_fence_old_qps_before_publication() {
    let runtime = runtime("qp_pause");
    let mut grant = runtime.staging_endpoint(8, 0).unwrap();
    let owner = runtime.device().unwrap();
    let old = owner.address().unwrap();
    let mut new = old.clone();
    new.descriptor.mtu = 4;
    let gate = pause_device(&owner);
    let release = Release(gate);
    native(&owner, |device| {
        *device.route.lock().unwrap() = Some(new.clone())
    });
    wait(&runtime, |s| s.progress_failures == 1);
    let ticks = runtime.stats().unwrap().monitor_ticks;
    wait(&runtime, |s| s.monitor_ticks > ticks);
    assert!(owner.asynchronous.paused());
    assert_eq!(owner.address().unwrap(), old);
    assert_eq!(runtime.stats().unwrap().route_recoveries, 0);
    drop(release);
    wait(&runtime, |s| s.route_recoveries == 1);
    assert_eq!(owner.address().unwrap(), new);
    let _ = grant.close();
    drop(grant);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn failed_native_cleanup_never_resumes_or_retries_on_monitor_ticks() {
    for mode in ["close0", "close1", "close2"] {
        let runtime = runtime(mode);
        let mut grant = runtime.staging_endpoint(8, 0).unwrap();
        let owner = runtime.device().unwrap();
        let log = events(&runtime);
        owner.asynchronous.fence_route();
        wait(&runtime, |s| s.retained_grants > 0);
        let before = log.lock().unwrap().clone();
        let ticks = runtime.stats().unwrap().monitor_ticks;
        wait(&runtime, |s| s.monitor_ticks > ticks);
        assert_eq!(runtime.stats().unwrap().route_recoveries, 0, "{mode}");
        assert!(owner.asynchronous.paused());
        assert_eq!(
            *log.lock().unwrap(),
            before,
            "monitor retried retained native cleanup"
        );
        assert!(grant.close().is_err());
        drop(grant);
        assert!(
            runtime
                .close(Instant::now() + Duration::from_secs(2))
                .is_err()
        );
    }
}

#[test]
fn initially_down_context_registers_mrs_and_recovers_without_reopening_its_pd() {
    use crate::engine::tests::{rdma_fixture_with_runtime, source_runtime};
    let engine = rdma_fixture_with_runtime(runtime("direct:async_initial_down"));
    let memory = engine
        .register_memory(
            vec![7; 8].into_boxed_slice(),
            crate::Permission::RemoteReadWrite,
        )
        .unwrap();
    let runtime = source_runtime(&engine);
    let owner = runtime.device().unwrap();
    let original = identity(&owner);
    let address = owner.address().unwrap();
    assert!(!address.active);
    assert!(owner.asynchronous.paused());
    assert_eq!(runtime.stats().unwrap().async_event_sources, 1);
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 2);
    assert_eq!(runtime.stats().unwrap().active_cqs, 0);
    assert!(runtime.paths(crate::MemoryLocation::Cpu(None)).is_err());
    assert!(
        runtime
            .allocate(8, crate::MemoryLocation::Cpu(None), 1)
            .is_err()
    );
    assert!(
        !runtime.topology().unwrap().unwrap().nic_list[0]
            .device_attrs
            .contains_key("rdma.selected")
    );
    let mut active = address.clone();
    active.active = true;
    native(&owner, |device| {
        *device.route.lock().unwrap() = Some(active.clone())
    });
    wait(runtime, |stats| stats.route_recoveries == 1);
    assert!(Arc::ptr_eq(&owner, &runtime.device().unwrap()));
    assert_eq!(identity(&owner), original);
    assert_eq!(owner.address().unwrap(), active);
    assert_eq!(
        runtime
            .paths(crate::MemoryLocation::Cpu(None))
            .unwrap()
            .len(),
        1
    );
    assert!(
        runtime
            .allocate(8, crate::MemoryLocation::Cpu(None), 1)
            .is_ok()
    );
    assert_eq!(runtime.stats().unwrap().opened_devices, 1);
    assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, 2);
    assert_eq!(runtime.stats().unwrap().active_cqs, 0);
    assert_eq!(runtime.stats().unwrap().acknowledged_async_events, 0);
    let mut grant = runtime
        .endpoint(
            &memory,
            0,
            8,
            0,
            Instant::now() + Duration::from_secs(2),
            None,
        )
        .unwrap();
    assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, 2);
    grant.close().unwrap();
    drop(grant);
    engine.close(Duration::from_secs(2)).unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
}

#[test]
fn recovery_reselects_auto_gid_only_after_drain_and_never_moves_explicit_pins() {
    for (pin, changed_field) in [(None, 0), (Some(0), 0), (None, 1), (None, 2)] {
        let mut runtime = runtime("qp_pause");
        runtime.config.gid_index = pin;
        let mut grant = runtime.staging_endpoint(8, 0).unwrap();
        let owner = runtime.device().unwrap();
        let original = identity(&owner);
        let old = owner.address().unwrap();
        let mut new = old.clone();
        match changed_field {
            0 => new.gid_index = 7,
            1 => new.port += 1,
            _ => new.device = "other-device".into(),
        }
        let release = Release(pause_device(&owner));
        native(&owner, |device| {
            device.route_queries.lock().unwrap().clear();
            *device.route.lock().unwrap() = Some(new.clone());
        });
        wait(&runtime, |stats| stats.progress_failures == 1);
        assert!(owner.asynchronous.paused());
        assert_eq!(owner.address().unwrap(), old);
        assert_eq!(runtime.stats().unwrap().route_recoveries, 0);
        drop(release);
        wait(&runtime, |stats| stats.progress_recoveries == 1);
        if pin.is_none() && changed_field == 0 {
            wait(&runtime, |stats| stats.route_recoveries == 1);
            assert_eq!(owner.address().unwrap(), new);
            assert_eq!(
                runtime.topology().unwrap().unwrap().nic_list[0].device_attrs["rdma.gid_index"],
                "7"
            );
        } else {
            let ticks = runtime.stats().unwrap().monitor_ticks;
            wait(&runtime, |stats| stats.monitor_ticks > ticks);
            assert!(owner.asynchronous.paused());
            assert_eq!(runtime.stats().unwrap().route_recoveries, 0);
            assert_eq!(owner.address().unwrap(), old);
        }
        native(&owner, |device| {
            let queries = device.route_queries.lock().unwrap();
            assert!(!queries.is_empty());
            if let Some(pin) = pin {
                assert!(
                    queries.iter().all(|query| *query == Some(pin)),
                    "{queries:?}"
                );
            } else {
                assert!(
                    queries.contains(&None),
                    "recovery did not request automatic selection: {queries:?}"
                );
                assert!(queries.iter().all(|query| query.is_none()
                    || *query == Some(old.gid_index)
                    || *query == Some(new.gid_index)));
            }
        });
        assert_eq!(identity(&owner), original);
        let _ = grant.close();
        drop(grant);
        runtime
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
}

#[test]
fn paused_port_allows_registration_but_fatal_device_and_lost_consumers_do_not() {
    for fault in 0..3 {
        let engine =
            crate::engine::tests::rdma_fixture_with_runtime(runtime("direct:initial_down"));
        let runtime = crate::engine::tests::source_runtime(&engine);
        let owner = runtime.device().unwrap();
        assert!(owner.asynchronous.paused());
        match fault {
            0 => owner
                .asynchronous
                .event(ibverbs_sys::IBV_EVENT_DEVICE_FATAL),
            1 => owner.asynchronous.lost(libc::EIO),
            _ => owner.asynchronous.lost_completion(libc::EIO),
        }
        assert!(
            engine
                .register_memory(vec![8; 8].into_boxed_slice(), crate::Permission::RemoteRead)
                .is_err()
        );
        assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, 0);
        assert_eq!(engine.stats().unwrap().retained_regions, 0);
        let _ = engine.close(Duration::from_secs(2));
    }
}

#[test]
fn active_port_rate_refresh_reseeds_selection_without_replacing_connections() {
    let runtime = runtime("ok");
    let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([1; 16]), 1, 8);
    let memory = registry
        .register(vec![0; 8].into_boxed_slice(), crate::Permission::LocalOnly)
        .unwrap();
    let mut grant = runtime
        .endpoint(
            &memory,
            0,
            8,
            0,
            Instant::now() + Duration::from_secs(30),
            None,
        )
        .unwrap();
    let owner = runtime.device().unwrap();
    let original = grant.incarnation().unwrap();
    for mbps in [Some(100_000), Some(200_000), None] {
        change_route(&runtime, |address| address.link_speed_mbps = mbps);
        let end = Instant::now() + Duration::from_secs(6);
        loop {
            let stats = runtime.nic_load_stats().unwrap();
            if stats[0].link_speed_gbps == mbps.map(|v| v as f64 / 1000.0) {
                assert_eq!(
                    stats[0].ewma_bandwidth_bps,
                    mbps.map_or(50e9, |v| v as f64 * 125_000.0)
                );
                break;
            }
            assert!(Instant::now() < end);
            thread::sleep(Duration::from_millis(5));
        }
        owner.check_address().unwrap();
        assert!(!owner.asynchronous.paused());
        assert_eq!(grant.incarnation().unwrap(), original);
        assert_eq!(runtime.stats().unwrap().route_recoveries, 0);
    }
    grant.close().unwrap();
    drop(grant);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

unsafe fn query_port_speed(
    handles: *const sys::DeviceHandles,
    _: u8,
) -> crate::Result<Option<u64>> {
    let device = unsafe { &*(*handles).context.cast::<FakeDevice>() };
    let mut state = device.effective_speed.lock().unwrap();
    state.2 += 1;
    if state.1 != 0 {
        Err(Error::Backend {
            operation: "controlled effective speed",
            code: state.1,
        })
    } else {
        Ok(state.0)
    }
}

#[test]
fn effective_rate_refresh_keeps_degraded_lag_on_query_fault_and_never_rebuilds_qps() {
    static VERBS: sys::Verbs = sys::Verbs {
        query_port_speed: Some(query_port_speed),
        ..crate::transport::rdma::tests::VERBS
    };
    let mut runtime = runtime("ok");
    runtime.verbs = &VERBS;
    let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([1; 16]), 1, 8);
    let memory = registry
        .register(vec![0; 8].into_boxed_slice(), crate::Permission::LocalOnly)
        .unwrap();
    let mut grant = runtime
        .endpoint(
            &memory,
            0,
            8,
            0,
            Instant::now() + Duration::from_secs(30),
            None,
        )
        .unwrap();
    let owner = runtime.device().unwrap();
    let original = grant.incarnation().unwrap();
    assert_eq!(owner.address().unwrap().effective_speed_mbps, Some(200_000));
    change_route(&runtime, |address| address.link_speed_mbps = Some(400_000));
    {
        let _contexts = runtime.context_set.lock().unwrap();
        let mut before = 0;
        native(&owner, |device| {
            before = device.effective_speed.lock().unwrap().2
        });
        for _ in 0..3 {
            owner.check_address().unwrap();
        }
        native(&owner, |device| {
            assert_eq!(
                device.effective_speed.lock().unwrap().2,
                before,
                "health checks must not query or learn speed"
            )
        });
    }
    for (rate, code, expected, failing) in [
        (Some(100_000), 0, 100_000, false),
        (Some(800_000), libc::EIO, 100_000, true),
        (Some(300_000), 0, 300_000, false),
        (Some(0), 0, 400_000, false),
        (None, 0, 400_000, false),
    ] {
        let before = owner.address().unwrap().effective_speed_query_failures;
        let queries = runtime.stats().unwrap().route_queries;
        native(&owner, |device| {
            let mut state = device.effective_speed.lock().unwrap();
            state.0 = rate;
            state.1 = code;
        });
        let end = Instant::now() + Duration::from_secs(6);
        loop {
            let address = owner.address().unwrap();
            let stats = runtime.nic_load_stats().unwrap();
            if runtime.stats().unwrap().route_queries > queries
                && address.effective_speed_query_failing == failing
                && address.link_speed_mbps == Some(expected)
                && stats[0].link_speed_gbps == Some(expected as f64 / 1000.0)
                && (failing || address.effective_speed_mbps == rate.filter(|v| *v != 0))
            {
                if failing {
                    assert!(address.effective_speed_query_failures > before);
                } else {
                    assert!(address.effective_speed_query_failures >= before);
                }
                assert_eq!(stats[0].ewma_bandwidth_bps, expected as f64 * 125_000.0);
                let topology = runtime.topology().unwrap().unwrap();
                assert_eq!(
                    topology.nic_list[0].device_attrs["rdma.effective_speed_query_failing"],
                    failing.to_string()
                );
                break;
            }
            assert!(Instant::now() < end);
            thread::sleep(Duration::from_millis(5));
        }
        assert!(!owner.asynchronous.paused());
        assert_eq!(grant.incarnation().unwrap(), original);
        assert_eq!(runtime.stats().unwrap().route_recoveries, 0);
    }
    assert_eq!(owner.address().unwrap().effective_speed_mbps, None);
    assert!(
        !runtime.topology().unwrap().unwrap().nic_list[0]
            .device_attrs
            .contains_key("rdma.effective_speed_mbps")
    );
    grant.close().unwrap();
    drop(grant);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}
