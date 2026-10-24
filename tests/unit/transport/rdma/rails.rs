use super::*;
use crate::engine::tests::rdma_fixture_with_runtime;
use crate::transport::rdma::tests::{self as fixture, on_requester_close};
use crate::{MemEntry, MemType, NicEntry, NicType, Topology};
use std::ffi::{CStr, c_char};
use std::sync::{Arc, Mutex};

fn discover(
    _: &RdmaConfig,
) -> Result<(Topology, Vec<super::super::super::discovery::RdmaAddress>)> {
    let (_, candidates) = fixture::discover(&RdmaConfig::new("copy"))?;
    let mut topology = Topology {
        nic_list: ["rail0", "device_missing", "rail2"]
            .iter()
            .enumerate()
            .map(|(id, name)| NicEntry {
                name: (*name).into(),
                pci_bus_id: None,
                kind: NicType::Rdma,
                numa_node: Some(u32::from(id == 2)),
                device_attrs: Default::default(),
            })
            .collect(),
        mem_list: vec![MemEntry {
            name: "*".into(),
            kind: MemType::Host,
            numa_node: None,
            device_list: Default::default(),
        }],
    };
    topology.rank();
    let routes = topology
        .nic_list
        .iter()
        .map(|nic| {
            let mut route = candidates[0].clone();
            route.device = nic.name.clone();
            route
        })
        .collect();
    Ok((topology, routes))
}

unsafe extern "C" fn open(handles: *mut sys::DeviceHandles, name: *const c_char) -> i32 {
    let name = unsafe { CStr::from_ptr(name) };
    if name.to_bytes() == b"device_missing" {
        return libc::ENODEV;
    }
    // Both independent CPU context fixtures execute the existing copy controls.
    unsafe { fixture::open_device(handles, c"copy".as_ptr()) }
}

fn runtime(mask: u64, endpoints: usize) -> RdmaTransport {
    static VERBS: sys::Verbs = sys::Verbs {
        discover,
        open_device: open,
        ..fixture::VERBS
    };
    let mut runtime = fixture::runtime("direct:copy");
    runtime.config.device = None;
    runtime.config.device_mask = mask;
    runtime.config.max_endpoints = endpoints;
    runtime.config.max_direct_bytes = 128;
    runtime.verbs = &VERBS;
    runtime.registrations =
        buffers::LocalBufferManager::new(&runtime.config, Arc::clone(&runtime.usage), &VERBS);
    runtime
}

fn exchange(
    requester: &TransferEngine,
    source: &TransferEngine,
    local: &RegisteredMemory,
    remote: &RegisteredMemory,
    op: TransferOp,
) {
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let report = requester
        .submit_and_wait(
            op,
            &peer,
            &[TransferRequest {
                transport: Transport::Rdma,
                local_address: local.base_address() + 4,
                remote_address: remote.base_address() + 4,
                length: 8,
            }],
            TIMEOUT,
        )
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert!(report.locally_drained);
    let mut bytes = [0; 16];
    let target = if op == TransferOp::Read {
        local
    } else {
        remote
    };
    target.read(0, &mut bytes).unwrap();
    assert_eq!(
        &bytes[4..12],
        if op == TransferOp::Read {
            &[0x29; 8]
        } else {
            &[0x63; 8]
        }
    );
    let guard = if op == TransferOp::Read { 0x63 } else { 0x29 };
    assert_eq!(&bytes[..4], &[guard; 4]);
    assert_eq!(&bytes[12..], &[guard; 4]);
}

#[test]
fn public_transfers_rotate_stable_contexts_reuse_only_the_selected_pair_and_retire_all_mrs() {
    for (mask, ids) in [(u64::MAX, vec![0, 2]), (1 << 2, vec![2])] {
        let source = rdma_fixture_with_runtime(runtime(mask, 8));
        let requester = rdma_fixture_with_runtime(runtime(mask, 8));
        let local = requester
            .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 16].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        for op in [TransferOp::Read, TransferOp::Write] {
            for _ in 0..4 {
                local.write(0, &[0x63; 16]).unwrap();
                remote.write(0, &[0x29; 16]).unwrap();
                let observed = Arc::clone(&observed);
                on_requester_close(move |endpoint, result| {
                    result.as_ref().unwrap();
                    observed.lock().unwrap().push(endpoint.cached_peer.unwrap());
                });
                exchange(&requester, &source, &local, &remote, op);
            }
        }
        let paths = source_runtime(&requester).paths().unwrap();
        let remote_paths = source_runtime(&source).paths().unwrap();
        assert_eq!(paths.iter().map(|p| p.nic_id).collect::<Vec<_>>(), ids);
        for (round, peer) in observed.lock().unwrap().iter().enumerate() {
            let local_path = paths
                .iter()
                .find(|p| p.domain == peer.local.domain)
                .unwrap();
            let remote_path = remote_paths
                .iter()
                .find(|p| p.domain == peer.connection.domain)
                .unwrap();
            assert_eq!(local_path.nic_id, ids[round % ids.len()]);
            assert_eq!(local_path.nic_id, remote_path.nic_id);
        }
        for engine in [&requester, &source] {
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                stats.opened_devices, 2,
                "disabled slot 1 preserves actual topology IDs"
            );
            assert_eq!(stats.progress_workers, ids.len());
            assert_eq!(stats.active_cqs, ids.len() * 2);
            assert_eq!(stats.opened_connections, (ids.len() * 2) as u64);
            assert_eq!(stats.reused_connections, (8 - ids.len() * 2) as u64);
            assert_eq!(stats.persistent_mrs, ids.len());
            assert_eq!(stats.persistent_mr_bytes, ids.len() * 16);
            assert_eq!(
                (
                    stats.direct_bytes,
                    stats.work_requests,
                    stats.receive_work_requests
                ),
                (0, 0, 0)
            );
        }
        requester.unregister_memory(&local).unwrap();
        source.unregister_memory(&remote).unwrap();
        assert_eq!(requester.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
        assert_eq!(source.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
        requester.close(TIMEOUT).unwrap();
        source.close(TIMEOUT).unwrap();
        for engine in [&requester, &source] {
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    stats.active_devices,
                    stats.active_cqs,
                    stats.progress_workers,
                    stats.active_endpoints
                ),
                (0, 0, 0, 0)
            );
        }
    }
}

#[test]
fn aggregate_connection_budget_evicts_an_idle_pair_on_another_context() {
    let source = rdma_fixture_with_runtime(runtime(u64::MAX, 1));
    let requester = rdma_fixture_with_runtime(runtime(u64::MAX, 1));
    let local = requester
        .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 16].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    for _ in 0..4 {
        exchange(&requester, &source, &local, &remote, TransferOp::Read);
    }
    for engine in [&requester, &source] {
        let stats = engine.stats().unwrap().rdma.unwrap();
        assert_eq!((stats.active_endpoints, stats.idle_endpoints), (1, 1));
        assert_eq!(
            (
                stats.opened_connections,
                stats.closed_connections,
                stats.evicted_connections
            ),
            (4, 3, 3)
        );
        assert_eq!(stats.progress_workers, 2);
    }
    requester.close(TIMEOUT).unwrap();
    source.close(TIMEOUT).unwrap();
}

#[test]
fn one_context_fault_preserves_the_other_stores_idle_count_and_fences_new_grants() {
    let source = rdma_fixture_with_runtime(runtime(u64::MAX, 4));
    let requester = rdma_fixture_with_runtime(runtime(u64::MAX, 4));
    let local = requester
        .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 16].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    for _ in 0..2 {
        exchange(&requester, &source, &local, &remote, TransferOp::Read);
    }
    let runtime = source_runtime(&requester);
    assert_eq!(runtime.stats().unwrap().idle_endpoints, 2);
    let device = runtime.device_at(0).unwrap();
    device.asynchronous.fence_route();
    device.asynchronous.notify();
    let deadline = deadline(TIMEOUT).unwrap();
    while runtime.stats().unwrap().progress_failures == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert_eq!(runtime.stats().unwrap().idle_endpoints, 1);
    let before = runtime.stats().unwrap().opened_direct_endpoints;
    let selection = runtime.allocate(8, 1 << 2).unwrap();
    assert!(matches!(
        runtime.endpoint_on(selection, &local, 4..12, 0, deadline, None),
        Err(Error::Unavailable(_))
    ));
    assert_eq!(runtime.stats().unwrap().opened_direct_endpoints, before);
    requester.close(TIMEOUT).unwrap();
    source.close(TIMEOUT).unwrap();
}
