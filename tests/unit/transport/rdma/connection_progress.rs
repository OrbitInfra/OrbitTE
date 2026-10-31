//! Cold acquisition uses actual endpoint reservations; verbs remain a CPU fixture.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, Weak};
use std::time::Duration;

use super::*;
use crate::SourceEpoch;
use crate::TransferOp;
use crate::transport::rdma::tests as fixture;

static OBSERVERS: OnceLock<Mutex<BTreeMap<usize, Weak<AtomicUsize>>>> = OnceLock::new();

pub(super) fn observe_pending(endpoint: &Arc<RdmaEndpoint>) {
    if let Some(count) = OBSERVERS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(&(Arc::as_ptr(endpoint) as usize))
        .and_then(Weak::upgrade)
    {
        count.fetch_add(1, Ordering::Release);
    }
}

fn key() -> EndpointKey {
    EndpointKey {
        route: Some("127.0.0.1:1234".parse().unwrap()),
        epoch: SourceEpoch([7; 16]),
        domain: [9; 16],
        source: false,
        source_nic_id: 0,
        operation: TransferOp::Read,
    }
}

#[test]
fn pending_acquisition_preserves_the_canonical_array_and_never_reserves_extra_qps() {
    for width in [1, 3] {
        let runtime = fixture::runtime("direct:copy");
        let device = runtime.device().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut builder = runtime
            .connecting(Arc::clone(&device), key(), width, None)
            .unwrap();
        let mut reservations = Vec::new();
        for _ in 0..width {
            let Some(Connection::Fresh(Some(slot))) = builder.poll(&runtime, deadline).unwrap()
            else {
                panic!("cold slot was not reserved");
            };
            reservations.push(slot);
        }
        let mut waiter = runtime
            .connecting(Arc::clone(&device), key(), width, None)
            .unwrap();
        assert!(Arc::ptr_eq(&waiter.endpoint, &builder.endpoint));
        for _ in 0..64 {
            assert!(waiter.poll(&runtime, deadline).unwrap().is_none());
        }
        assert!(matches!(
            waiter.poll(&runtime, Instant::now()),
            Err(Error::Timeout)
        ));
        drop(waiter);
        assert!(
            builder.endpoint.reserve(None).unwrap().is_none(),
            "waiter must not cancel builders"
        );
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.opened_connections,
                stats.active_grants,
                stats.active_endpoints
            ),
            (0, 0, 0)
        );
        // Canonical lookup survives both a pending waiter and an unbound rollback.
        drop(reservations);
        let mut retry = runtime.connecting(device, key(), width, None).unwrap();
        assert!(Arc::ptr_eq(&retry.endpoint, &builder.endpoint));
        let Some(Connection::Fresh(Some(slot))) = retry.poll(&runtime, deadline).unwrap() else {
            panic!("rolled-back slot was not reusable");
        };
        drop(slot);
        runtime.close(deadline).unwrap();
        assert!(matches!(
            retry.poll(&runtime, deadline),
            Err(Error::Unavailable("RDMA runtime is closed"))
        ));
    }
}

#[cfg(feature = "cuda")]
mod staging {
    use std::ffi::{CStr, c_char};

    use super::*;
    use crate::engine::tests::source_runtime;
    use crate::transport::rdma::{RdmaConfig, buffers, discovery::RdmaAddress, sys};
    use crate::{NicEntry, NicType, Topology, TransferEngine};

    fn discover(_: &RdmaConfig) -> Result<(Topology, Vec<RdmaAddress>)> {
        let (_, candidates) = fixture::discover(&RdmaConfig::new("copy"))?;
        let names = ["rail0", "device_missing", "rail2"];
        let topology = Topology {
            nic_list: names
                .iter()
                .map(|name| NicEntry {
                    name: (*name).into(),
                    pci_bus_id: None,
                    kind: NicType::Rdma,
                    numa_node: None,
                    device_attrs: Default::default(),
                })
                .collect(),
            mem_list: vec![],
        };
        let routes = names
            .into_iter()
            .map(|name| RdmaAddress {
                device: name.into(),
                ..candidates[0].clone()
            })
            .collect();
        Ok((topology, routes))
    }

    unsafe fn open_mode(h: *mut sys::DeviceHandles, name: *const c_char, mode: &CStr) -> i32 {
        match unsafe { CStr::from_ptr(name) }.to_bytes() {
            b"rail0" => unsafe { fixture::open_device(h, mode.as_ptr()) },
            b"rail2" => unsafe { fixture::open_device(h, c"copy".as_ptr()) },
            _ => libc::ENODEV,
        }
    }
    unsafe extern "C" fn open_copy(h: *mut sys::DeviceHandles, n: *const c_char) -> i32 {
        unsafe { open_mode(h, n, c"copy") }
    }
    unsafe extern "C" fn open_hold(h: *mut sys::DeviceHandles, n: *const c_char) -> i32 {
        unsafe { open_mode(h, n, c"copy_cross_hold") }
    }
    unsafe extern "C" fn open_error(h: *mut sys::DeviceHandles, n: *const c_char) -> i32 {
        unsafe { open_mode(h, n, c"copy_cross_hold_first_error") }
    }
    unsafe extern "C" fn open_retain(h: *mut sys::DeviceHandles, n: *const c_char) -> i32 {
        unsafe { open_mode(h, n, c"copy_cross_hold_first_error_close0") }
    }

    unsafe fn open_retirement(
        h: *mut sys::DeviceHandles,
        n: *const c_char,
        failure: bool,
        error: bool,
    ) -> i32 {
        let mode = match unsafe { CStr::from_ptr(n) }.to_bytes() {
            b"rail0" if error => c"copy_cross_hold_first_error",
            b"rail0" => c"copy_cross_hold",
            b"rail2" if failure => c"copy_qp_pause_close0",
            b"rail2" => c"copy_qp_pause",
            _ => return libc::ENODEV,
        };
        unsafe { fixture::open_device(h, mode.as_ptr()) }
    }
    unsafe extern "C" fn open_retire(h: *mut sys::DeviceHandles, n: *const c_char) -> i32 {
        unsafe { open_retirement(h, n, false, false) }
    }
    unsafe extern "C" fn open_retire_error(h: *mut sys::DeviceHandles, n: *const c_char) -> i32 {
        unsafe { open_retirement(h, n, false, true) }
    }
    unsafe extern "C" fn open_retire_failure(h: *mut sys::DeviceHandles, n: *const c_char) -> i32 {
        unsafe { open_retirement(h, n, true, true) }
    }

    pub(crate) fn two_rail_runtime(mode: &str) -> RdmaTransport {
        static COPY: sys::Verbs = sys::Verbs {
            discover,
            open_device: open_copy,
            ..fixture::VERBS
        };
        static HOLD: sys::Verbs = sys::Verbs {
            discover,
            open_device: open_hold,
            ..fixture::VERBS
        };
        static ERROR: sys::Verbs = sys::Verbs {
            discover,
            open_device: open_error,
            ..fixture::VERBS
        };
        static RETAIN: sys::Verbs = sys::Verbs {
            discover,
            open_device: open_retain,
            ..fixture::VERBS
        };
        static RETIRE: sys::Verbs = sys::Verbs {
            discover,
            open_device: open_retire,
            ..fixture::VERBS
        };
        static RETIRE_ERROR: sys::Verbs = sys::Verbs {
            discover,
            open_device: open_retire_error,
            ..fixture::VERBS
        };
        static RETIRE_FAILURE: sys::Verbs = sys::Verbs {
            discover,
            open_device: open_retire_failure,
            ..fixture::VERBS
        };
        let verbs = match mode {
            "copy" => &COPY,
            "hold" => &HOLD,
            "error" => &ERROR,
            "retain" => &RETAIN,
            "retire" => &RETIRE,
            "retire_error" => &RETIRE_ERROR,
            "retire_failure" => &RETIRE_FAILURE,
            _ => panic!("unknown fixture mode"),
        };
        let mut runtime = fixture::runtime("direct:copy");
        runtime.config.device = None;
        runtime.config.max_endpoints = 2;
        runtime.config.max_grants = 4;
        runtime.config.max_cqes = 4;
        runtime.config.max_direct_bytes = 14;
        runtime.config.block_size = 64;
        if mode.starts_with("retire") {
            runtime.config.num_lanes = 2;
            runtime.config.host_memory = crate::transport::rdma::RdmaHostMemory::Staged;
            runtime.config.max_staging_bytes = 14;
        }
        runtime.verbs = verbs;
        runtime.registrations =
            buffers::LocalBufferManager::new(&runtime.config, Arc::clone(&runtime.usage), verbs);
        runtime
    }

    pub(crate) struct HeldConnection {
        endpoint: Arc<RdmaEndpoint>,
        _reservation: Construction,
        polls: Arc<AtomicUsize>,
    }

    impl HeldConnection {
        pub(crate) fn polls(&self) -> usize {
            self.polls.load(Ordering::Acquire)
        }
        pub(crate) fn assert_held(&self) {
            assert!(self.endpoint.candidates(&|_| true).unwrap().is_empty());
            assert!(self.endpoint.reserve(None).unwrap().is_none());
        }
    }

    impl Drop for HeldConnection {
        fn drop(&mut self) {
            OBSERVERS
                .get()
                .unwrap()
                .lock()
                .unwrap()
                .remove(&(Arc::as_ptr(&self.endpoint) as usize));
        }
    }

    pub(crate) fn hold_second_connection(
        requester: &TransferEngine,
        source: &TransferEngine,
        operation: TransferOp,
    ) -> HeldConnection {
        let runtime = source_runtime(requester);
        let paths = source_runtime(source).paths().unwrap();
        let remote = paths.iter().find(|p| p.nic_id == 2).unwrap();
        let device = runtime.device_at(2).unwrap();
        let connecting = runtime
            .connecting(
                device,
                EndpointKey {
                    route: Some(source.endpoint()),
                    epoch: source.source_epoch(),
                    domain: remote.domain,
                    source: false,
                    source_nic_id: 2,
                    operation,
                },
                1,
                None,
            )
            .unwrap();
        let endpoint = connecting.endpoint;
        let reservation = endpoint.reserve(None).unwrap().unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        assert!(
            OBSERVERS
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(Arc::as_ptr(&endpoint) as usize, Arc::downgrade(&polls))
                .is_none()
        );
        HeldConnection {
            endpoint,
            _reservation: reservation,
            polls,
        }
    }
}

#[cfg(feature = "cuda")]
pub(crate) use staging::{HeldConnection, hold_second_connection, two_rail_runtime};
