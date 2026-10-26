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
    assert!(runtime.allocate(8, u64::MAX).is_err());
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
    assert!(runtime.allocate(8, u64::MAX).is_err());
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
    assert!(runtime.allocate(8, 1).is_ok());
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
