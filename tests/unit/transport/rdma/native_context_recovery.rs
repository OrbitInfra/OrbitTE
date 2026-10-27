//! Opt-in real PD/MR/QP gate with process-local port-state observation injection.
use super::recovery::identity;
use super::*;
use crate::Permission;
use crate::engine::tests::{rdma_fixture_with_runtime, source_runtime};
use crate::transport::rdma::{RdmaHostMemory, buffers, discovery};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};

static PAUSED: AtomicBool = AtomicBool::new(true);

fn discover(config: &RdmaConfig) -> Result<(crate::Topology, Vec<RdmaAddress>)> {
    let mut initial = config.clone();
    initial.gid_index = Some(
        std::env::var("ORBITTE_INITIAL_GID")
            .unwrap()
            .parse()
            .unwrap(),
    );
    discovery::discover(&initial)
}

unsafe fn query_address(
    handles: *const sys::DeviceHandles,
    address: &RdmaAddress,
    gid_index: Option<u8>,
) -> Result<RdmaAddress> {
    let mut observed = unsafe { (sys::REAL.query_address)(handles, address, gid_index) }?;
    if PAUSED.load(Ordering::Acquire) {
        observed.active = false;
    }
    Ok(observed)
}

static VERBS: sys::Verbs = sys::Verbs {
    discover,
    query_address,
    ..sys::REAL
};

fn wait(runtime: &RdmaTransport, predicate: impl Fn(&crate::transport::rdma::RdmaStats) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let stats = runtime.stats().unwrap();
        if predicate(&stats) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "native recovery timed out: {stats:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "requires an authorized RNIC, existing exclusive lock and two valid GID indices"]
fn native_context_recovery_preserves_pd_mrs_and_strict_gid_selection() {
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(std::env::var("ORBITTE_RDMA_LOCK").unwrap())
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0,
        "the selected RNIC lock must be held through native close"
    );
    let device = std::env::var("ORBITTE_SHARED_DEVICE").unwrap();
    let port: u8 = std::env::var("ORBITTE_SHARED_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let initial: u8 = std::env::var("ORBITTE_INITIAL_GID")
        .unwrap()
        .parse()
        .unwrap();
    for pin in [None, Some(initial)] {
        PAUSED.store(true, Ordering::Release);
        let config = RdmaConfig {
            device: Some(device.clone()),
            port: Some(port),
            gid_index: pin,
            host_memory: RdmaHostMemory::Direct,
            ..Default::default()
        };
        let mut transport = Arc::try_unwrap(RdmaTransport::new(config).unwrap())
            .unwrap_or_else(|_| panic!("new transport unexpectedly shared"));
        transport.verbs = &VERBS;
        transport.registrations = buffers::LocalBufferManager::new(
            &transport.config,
            Arc::clone(&transport.usage),
            &VERBS,
        );
        let engine = rdma_fixture_with_runtime(transport);
        let memory = engine
            .register_memory(
                vec![0x57; 4096].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let runtime = source_runtime(&engine);
        let owner = runtime.device().unwrap();
        let original = identity(&owner);
        assert!(!owner.address().unwrap().active);
        assert!(runtime.paths().is_err());
        assert_eq!(runtime.stats().unwrap().persistent_mrs, 2);
        assert_eq!(runtime.stats().unwrap().opened_connections, 0);
        PAUSED.store(false, Ordering::Release);
        wait(runtime, |stats| stats.route_recoveries == 1);
        let selected = owner.address().unwrap().gid_index;
        match pin {
            Some(index) => assert_eq!(selected, index),
            None => assert_ne!(
                selected, initial,
                "choose a valid nonpreferred initial GID for the reselection gate"
            ),
        }
        assert_eq!(identity(&owner), original);
        assert_eq!(runtime.paths().unwrap().len(), 1);
        let mut grant = runtime
            .endpoint(
                &memory,
                0,
                4096,
                0,
                Instant::now() + Duration::from_secs(5),
                None,
            )
            .unwrap();
        let incarnation = grant.incarnation().unwrap();
        PAUSED.store(true, Ordering::Release);
        wait(runtime, |stats| {
            stats.progress_recoveries == 1 && stats.active_cqs == 0
        });
        assert!(owner.asynchronous.paused());
        assert_eq!(runtime.stats().unwrap().route_recoveries, 1);
        assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, 2);
        let _ = grant.close();
        drop(grant);
        PAUSED.store(false, Ordering::Release);
        wait(runtime, |stats| stats.route_recoveries == 2);
        assert_eq!(identity(&owner), original);
        assert_eq!(owner.address().unwrap().gid_index, selected);
        let mut replacement = runtime
            .endpoint(
                &memory,
                0,
                4096,
                0,
                Instant::now() + Duration::from_secs(5),
                None,
            )
            .unwrap();
        assert_ne!(replacement.incarnation().unwrap(), incarnation);
        replacement.close().unwrap();
        drop(replacement);
        assert_eq!(runtime.stats().unwrap().opened_devices, 1);
        assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, 2);
        engine.close(Duration::from_secs(5)).unwrap();
        let closed = runtime.stats().unwrap();
        assert_eq!(
            (
                closed.active_devices,
                closed.active_cqs,
                closed.persistent_mrs,
                closed.active_endpoints,
                closed.retained_devices,
                closed.retained_endpoints
            ),
            (0, 0, 0, 0, 0, 0)
        );
        println!(
            "ORBITTE_CONTEXT_RECOVERY {{\"automatic\":{},\"initial_gid\":{initial},\"selected_gid\":{selected},\"recoveries\":2,\"native_pd\":{},\"persistent_mrs\":2,\"physical_port_fault\":false,\"payload_tested\":false}}",
            pin.is_none(),
            original.1
        );
    }
}
