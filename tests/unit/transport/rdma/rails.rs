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
        for (engine, published) in [(&requester, 2), (&source, 4)] {
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                stats.opened_devices, 2,
                "disabled slot 1 preserves actual topology IDs"
            );
            assert_eq!(stats.progress_lanes, ids.len());
            assert_eq!(stats.progress_workers, 1);
            assert_eq!(stats.opened_progress_workers, 1);
            assert_eq!(stats.active_cqs, ids.len() * 2);
            assert_eq!(stats.opened_connections, (ids.len() * 2) as u64);
            assert_eq!(stats.reused_connections, (8 - ids.len() * 2) as u64);
            assert_eq!(stats.persistent_mrs, published);
            assert_eq!(stats.persistent_mr_bytes, published * 16);
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
        assert_eq!(stats.progress_lanes, 2);
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

fn sliced_runtime(
    mask: u64,
    endpoints: usize,
    cqes: usize,
    policy: RdmaHostMemory,
    event: bool,
) -> RdmaTransport {
    static EVENT: sys::Verbs = sys::Verbs {
        discover,
        open_device: open,
        ..super::super::super::cq::tests::VERBS
    };
    let mut owner = runtime(mask, endpoints);
    owner.config.block_size = 4;
    owner.config.max_send_wr = 1;
    owner.config.max_cqes = cqes;
    owner.config.host_memory = policy;
    if event {
        owner.config.progress_mode = super::super::super::RdmaProgressMode::EventDriven;
        owner.verbs = &EVENT;
    }
    owner.registrations =
        buffers::LocalBufferManager::new(&owner.config, Arc::clone(&owner.usage), owner.verbs);
    owner
}

fn single_handler(runtime: RdmaTransport) -> TransferEngine {
    crate::engine::tests::rdma_fixture_with_config(
        runtime,
        crate::EngineConfig {
            handler_threads: 1,
            ..Default::default()
        },
    )
}

#[test]
fn parent_slices_use_one_source_handler_and_aggregate_read_write_receipts_across_contexts() {
    use RdmaHostMemory::{Direct, Staged};
    for event in [false, true] {
        for (source_policy, local_policy) in [
            (Direct, Direct),
            (Staged, Direct),
            (Direct, Staged),
            (Staged, Staged),
        ] {
            let source = single_handler(sliced_runtime(u64::MAX, 8, 2, source_policy, event));
            let requester = single_handler(sliced_runtime(u64::MAX, 8, 2, local_policy, event));
            let local = requester
                .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let remote = source
                .register_memory(
                    vec![0x29; 16].into_boxed_slice(),
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let mut finished = 0;
            for op in [TransferOp::Read, TransferOp::Write] {
                for _ in 0..2 {
                    local.write(0, &[0x63; 16]).unwrap();
                    remote.write(0, &[0x29; 16]).unwrap();
                    exchange(&requester, &source, &local, &remote, op);
                    finished += 2;
                    assert_eq!(
                        requester.stats().unwrap().rdma.unwrap().completed_sends,
                        finished
                    );
                    let receipts = source.stats().unwrap().rdma.unwrap();
                    assert_eq!(receipts.completed_receives, finished);
                    assert_eq!(receipts.completed_window_binds, finished);
                    assert_eq!(receipts.completed_window_invalidations, finished);
                }
            }
            for engine in [&source, &requester] {
                let stats = engine.stats().unwrap().rdma.unwrap();
                assert_eq!(stats.progress_lanes, 2);
                assert_eq!(
                    stats.completed_reusable_grants,
                    if source_policy == Direct && local_policy == Direct {
                        8
                    } else {
                        0
                    }
                );
                assert_eq!(
                    (
                        stats.active_grants,
                        stats.direct_bytes,
                        stats.staging_bytes,
                        stats.work_requests,
                        stats.receive_work_requests
                    ),
                    (0, 0, 0, 0, 0)
                );
            }
            let data = requester.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    data.completed_reads,
                    data.completed_writes,
                    data.completed_read_bytes,
                    data.completed_write_bytes
                ),
                (4, 4, 16, 16)
            );
            assert_eq!(
                data.opened_connections,
                if source_policy == Direct && local_policy == Direct {
                    4
                } else {
                    8
                }
            );
            source.unregister_memory(&remote).unwrap();
            requester.unregister_memory(&local).unwrap();
            source.close(TIMEOUT).unwrap();
            requester.close(TIMEOUT).unwrap();
        }
    }
}

#[test]
fn slice_window_intersects_remote_mask_and_single_cq_capacity_before_grants() {
    for (source_mask, cqes, expected) in [(1 << 2, 2, 1), (u64::MAX, 1, 1)] {
        let source = single_handler(sliced_runtime(
            source_mask,
            8,
            cqes,
            RdmaHostMemory::Direct,
            false,
        ));
        let requester = single_handler(sliced_runtime(
            u64::MAX,
            8,
            8,
            RdmaHostMemory::Direct,
            false,
        ));
        let local = requester
            .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 16].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        exchange(&requester, &source, &local, &remote, TransferOp::Read);
        assert_eq!(
            requester.stats().unwrap().rdma.unwrap().progress_workers,
            expected
        );
        assert_eq!(
            requester.stats().unwrap().rdma.unwrap().opened_connections,
            if cqes == 1 { 1 } else { 2 }
        );
        assert_eq!(
            source.stats().unwrap().rdma.unwrap().persistent_mrs,
            4,
            "publication retains every enabled context, including masked contexts"
        );
        source.close(TIMEOUT).unwrap();
        requester.close(TIMEOUT).unwrap();
    }
}

unsafe extern "C" fn open_paused(h: *mut sys::DeviceHandles, name: *const c_char) -> i32 {
    if unsafe { CStr::from_ptr(name) }.to_bytes() == b"device_missing" {
        return libc::ENODEV;
    }
    unsafe { fixture::open_device(h, c"pause_copy".as_ptr()) }
}

#[test]
fn both_rail_workers_can_own_payloads_before_parent_completion_and_unregister_waits_for_exact_loans()
 {
    static PAUSED: sys::Verbs = sys::Verbs {
        discover,
        open_device: open_paused,
        ..fixture::VERBS
    };
    let source = Arc::new(single_handler(sliced_runtime(
        u64::MAX,
        8,
        4,
        RdmaHostMemory::Direct,
        false,
    )));
    let mut local_runtime = sliced_runtime(u64::MAX, 8, 4, RdmaHostMemory::Direct, false);
    local_runtime.config.num_lanes = 3;
    local_runtime.verbs = &PAUSED;
    local_runtime.registrations = buffers::LocalBufferManager::new(
        &local_runtime.config,
        Arc::clone(&local_runtime.usage),
        &PAUSED,
    );
    let requester = Arc::new(single_handler(local_runtime));
    let local = requester
        .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 16].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let pauses: Vec<_> = [0, 2]
        .map(|id| {
            super::super::super::context::tests::pause_device(
                &source_runtime(&requester).device_at(id).unwrap(),
            )
        })
        .into();
    let task = {
        let requester = Arc::clone(&requester);
        let source = Arc::clone(&source);
        let local = local.clone();
        let remote = remote.clone();
        thread::spawn(move || exchange(&requester, &source, &local, &remote, TransferOp::Read))
    };
    for pause in &pauses {
        let (lock, changed) = &**pause;
        let (guard, timed) = changed
            .wait_timeout_while(lock.lock().unwrap(), TIMEOUT, |state| !state.0)
            .unwrap();
        assert!(
            !timed.timed_out(),
            "both native post owners must progress before either one is released"
        );
        drop(guard);
    }
    let stats = requester.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (stats.active_grants, stats.work_requests, stats.direct_bytes),
        (2, 2, 8)
    );
    requester.unregister_memory(&local).unwrap();
    assert_eq!(requester.stats().unwrap().rdma.unwrap().persistent_mrs, 2);
    for offset in [4, 8] {
        assert!(local.read(offset, &mut [0; 4]).is_err());
    }
    local.write(0, &[0x63; 4]).unwrap();
    assert!(matches!(
        source.close_source(Duration::from_millis(10)),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        requester.close_requester(Duration::from_millis(10)),
        Err(Error::Timeout)
    ));
    for pause in pauses {
        let (lock, changed) = &*pause;
        lock.lock().unwrap().1 = true;
        changed.notify_all();
    }
    task.join().unwrap();
    assert_eq!(requester.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
    source.close(TIMEOUT).unwrap();
    requester.close(TIMEOUT).unwrap();
}

#[test]
fn malformed_parent_plan_and_changed_slice_domain_fail_before_any_endpoint_authority() {
    let source = single_handler(sliced_runtime(
        u64::MAX,
        8,
        8,
        RdmaHostMemory::Direct,
        false,
    ));
    let remote = source
        .register_memory(
            vec![0x29; 16].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    for case in 0..8 {
        let request = Request {
            operation: Operation::RdmaReadSlices,
            epoch: source.source_epoch(),
            region: remote.id(),
            offset: 4,
            length: 8,
        };
        let mut channel =
            ControlChannel::connect(source.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
        channel.write_all(&request.encode()).unwrap();
        let paths = rail_monitor::read_paths(&mut channel, source.source_epoch()).unwrap();
        let mut limit = [0];
        channel.read_exact(&mut limit).unwrap();
        if case == 0 {
            channel.write_all(&[0]).unwrap();
        } else if case == 1 {
            channel.write_all(&[65]).unwrap();
        } else {
            channel.write_all(&[if case == 7 { 1 } else { 2 }]).unwrap();
            let a = SliceRequest {
                offset: 0,
                length: if case == 7 { 8 } else { 4 },
                domain: paths[0].domain,
            };
            a.write(&mut channel).unwrap();
            if case == 7 {
                assert_eq!(
                    Reply::read(&mut channel, source.source_epoch())
                        .unwrap()
                        .status,
                    9
                );
                Hello {
                    requester: identity(9, 8, 7),
                    source: None,
                    expected_domain: paths[1].domain,
                    direct: true,
                    previous_grant: 0,
                }
                .write(&mut channel)
                .unwrap();
            } else {
                SliceRequest {
                    offset: if case == 2 {
                        3
                    } else if case == 3 {
                        5
                    } else {
                        4
                    },
                    length: if case == 4 {
                        0
                    } else if case == 5 {
                        5
                    } else {
                        4
                    },
                    domain: if case == 6 { [99; 16] } else { paths[1].domain },
                }
                .write(&mut channel)
                .unwrap();
            }
        }
        assert!(
            matches!(
                Reply::read(&mut channel, source.source_epoch()),
                Err(Error::Protocol(_))
            ),
            "case {case}"
        );
    }
    source.close_source(TIMEOUT).unwrap();
    let stats = source.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (
            stats.opened_connections,
            stats.opened_windows,
            stats.active_grants
        ),
        (0, 0, 0)
    );
    source.close(TIMEOUT).unwrap();
}

unsafe extern "C" fn open_failed_rail(h: *mut sys::DeviceHandles, name: *const c_char) -> i32 {
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    if name == b"device_missing" {
        return libc::ENODEV;
    }
    unsafe {
        fixture::open_device(
            h,
            if name == b"rail0" {
                c"wc_close0".as_ptr()
            } else {
                c"copy".as_ptr()
            },
        )
    }
}

#[test]
fn one_unquiesced_rail_retains_its_exact_loan_and_other_rail_cleanup_cannot_report_parent_drain() {
    static FAILED: sys::Verbs = sys::Verbs {
        discover,
        open_device: open_failed_rail,
        ..fixture::VERBS
    };
    for op in [TransferOp::Read, TransferOp::Write] {
        let source = single_handler(sliced_runtime(
            u64::MAX,
            8,
            8,
            RdmaHostMemory::Direct,
            false,
        ));
        let mut failed = sliced_runtime(u64::MAX, 8, 8, RdmaHostMemory::Direct, false);
        failed.verbs = &FAILED;
        failed.registrations =
            buffers::LocalBufferManager::new(&failed.config, Arc::clone(&failed.usage), &FAILED);
        let requester = single_handler(failed);
        let local = requester
            .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 16].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
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
        assert_eq!(
            (
                report.completed_requests,
                report.transferred_bytes,
                report.locally_drained
            ),
            (0, 0, false)
        );
        let failure = report.failure.unwrap();
        assert!(matches!(failure.error, Error::Unquiesced { .. }));
        assert_eq!(
            failure.remote_effect,
            if op == TransferOp::Write {
                RemoteEffect::Unknown
            } else {
                RemoteEffect::None
            }
        );
        let stats = requester.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.active_grants,
                stats.retained_grants,
                stats.direct_bytes
            ),
            (1, 1, 4)
        );
        assert!(local.read(4, &mut [0; 4]).is_err());
        local.write(8, &[0x55; 4]).unwrap();
        source.close(TIMEOUT).unwrap();
    }
}

unsafe fn open_publication_mode(
    h: *mut sys::DeviceHandles,
    name: *const c_char,
    mode: &CStr,
) -> i32 {
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    if name == b"device_missing" {
        return libc::ENODEV;
    }
    unsafe {
        fixture::open_device(
            h,
            if name == b"rail2" {
                mode.as_ptr()
            } else {
                c"copy".as_ptr()
            },
        )
    }
}
unsafe extern "C" fn open_publication_fault(
    h: *mut sys::DeviceHandles,
    name: *const c_char,
) -> i32 {
    unsafe { open_publication_mode(h, name, c"copy_publication_fault") }
}
unsafe extern "C" fn open_publication_retained(
    h: *mut sys::DeviceHandles,
    name: *const c_char,
) -> i32 {
    unsafe { open_publication_mode(h, name, c"copy_publication_close1") }
}
unsafe extern "C" fn open_publication_parent(
    h: *mut sys::DeviceHandles,
    name: *const c_char,
) -> i32 {
    unsafe { open_publication_mode(h, name, c"copy_publication_parent") }
}
unsafe extern "C" fn fail_later_registration(
    h: *mut sys::MemoryHandles,
    d: *const sys::DeviceHandles,
    address: *mut std::ffi::c_void,
    length: usize,
    window_bind: bool,
) -> i32 {
    let device = unsafe { &*(*d).context.cast::<fixture::FakeDevice>() };
    let previous = device
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| **event == "register_local")
        .count();
    let fail = device.mode.contains("publication")
        && if device.mode.ends_with("parent") {
            window_bind
        } else {
            previous != 0
        };
    let code = unsafe { (fixture::VERBS.register_memory)(h, d, address, length, window_bind) };
    if code == 0 && fail {
        libc::ENOMEM
    } else {
        code
    }
}

fn publication_runtime(verbs: &'static sys::Verbs, capacity: usize) -> RdmaTransport {
    let mut owner = runtime(u64::MAX, 8);
    owner.config.max_cached_mrs = capacity;
    owner.verbs = verbs;
    owner.registrations =
        buffers::LocalBufferManager::new(&owner.config, Arc::clone(&owner.usage), verbs);
    owner
}

#[test]
fn failed_new_publication_preserves_published_mrs_lookup_and_reference_counts_on_every_context() {
    static FAIL: sys::Verbs = sys::Verbs {
        discover,
        open_device: open_publication_fault,
        register_memory: fail_later_registration,
        ..fixture::VERBS
    };
    for capacity in [2, 8] {
        let engine = single_handler(publication_runtime(&FAIL, capacity));
        let a = Arc::new(crate::HostMemory::new(vec![1; 16].into_boxed_slice()).unwrap());
        let b = Arc::new(crate::HostMemory::new(vec![2; 16].into_boxed_slice()).unwrap());
        let original = engine
            .register_host_memory(Arc::clone(&a), 0, 16, Permission::LocalOnly)
            .unwrap();
        let stats = engine.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (stats.opened_persistent_mrs, stats.persistent_mrs),
            (2, 2),
            "both MRs exist before the first transfer"
        );
        let error = engine
            .register_host_memory_batch(
                &[(Arc::clone(&a), 0, 16), (Arc::clone(&b), 0, 16)],
                Permission::RemoteReadWrite,
            )
            .unwrap_err();
        assert!(if capacity == 2 {
            matches!(error, Error::ResourceExhausted)
        } else {
            matches!(error, Error::Backend { .. })
        });
        assert!(matches!(
            engine.resolve_local_address(b.base_address(), 16),
            Err(Error::RegionNotFound)
        ));
        assert_eq!(engine.stats().unwrap().retained_regions, 1);
        assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 2);
        assert_eq!(
            engine
                .resolve_local_address(a.base_address(), 16)
                .unwrap()
                .0
                .id(),
            original.id()
        );
        engine.unregister_memory(&original).unwrap();
        assert!(
            matches!(
                engine.resolve_local_address(a.base_address(), 16),
                Err(Error::RegionNotFound)
            ),
            "failed batch did not increment the duplicate reference"
        );
        assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
        engine.close(TIMEOUT).unwrap();
    }
}

#[test]
fn capability_fanout_rolls_back_as_one_publication_and_failed_cleanup_keeps_unpublished_backing() {
    static PARENT: sys::Verbs = sys::Verbs {
        discover,
        open_device: open_publication_parent,
        register_memory: fail_later_registration,
        ..fixture::VERBS
    };
    let engine = single_handler(publication_runtime(&PARENT, 8));
    let backing = Arc::new(crate::HostMemory::new(vec![1; 16].into_boxed_slice()).unwrap());
    assert!(matches!(
        engine.register_host_memory(Arc::clone(&backing), 0, 16, Permission::RemoteReadWrite),
        Err(Error::Backend { .. })
    ));
    assert_eq!(engine.stats().unwrap().retained_regions, 0);
    assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
    assert_eq!(
        engine.stats().unwrap().rdma.unwrap().persistent_source_mrs,
        0
    );
    engine.close(TIMEOUT).unwrap();

    static RETAINED: sys::Verbs = sys::Verbs {
        discover,
        open_device: open_publication_retained,
        register_memory: fail_later_registration,
        ..fixture::VERBS
    };
    let engine = single_handler(publication_runtime(&RETAINED, 8));
    let original = engine
        .register_memory(vec![1; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let fresh = Arc::new(crate::HostMemory::new(vec![2; 16].into_boxed_slice()).unwrap());
    let weak = Arc::downgrade(&fresh);
    assert!(matches!(
        engine.register_host_memory(Arc::clone(&fresh), 0, 16, Permission::LocalOnly),
        Err(Error::Unquiesced { .. })
    ));
    assert!(matches!(
        engine.resolve_local_address(fresh.base_address(), 16),
        Err(Error::RegionNotFound)
    ));
    assert_eq!(engine.stats().unwrap().retained_regions, 2);
    let stats = engine.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (stats.persistent_mrs, stats.retained_persistent_mrs),
        (3, 1)
    );
    let mut bytes = [0; 16];
    original.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [1; 16]);
    assert!(matches!(
        engine.register_memory(vec![3; 16].into_boxed_slice(), Permission::LocalOnly),
        Err(Error::Unquiesced { .. })
    ));
    drop(fresh);
    assert!(
        weak.upgrade().is_some(),
        "failed native cleanup retains the real unpublished allocation"
    );
    assert!(matches!(
        engine.close(TIMEOUT),
        Err(Error::Unquiesced { .. })
    ));
}

#[test]
fn releasing_aggregate_cq_quota_wakes_another_context_without_a_local_cqe() {
    static PAUSED: sys::Verbs = sys::Verbs {
        discover,
        open_device: open_paused,
        ..super::super::super::cq::tests::VERBS
    };
    for operation in [TransferOp::Read, TransferOp::Write] {
        let mut runtime = sliced_runtime(u64::MAX, 8, 1, RdmaHostMemory::Direct, true);
        runtime.config.num_lanes = 3;
        runtime.verbs = &PAUSED;
        runtime.registrations =
            buffers::LocalBufferManager::new(&runtime.config, Arc::clone(&runtime.usage), &PAUSED);
        let engine = single_handler(runtime);
        let memory = engine
            .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let runtime = source_runtime(&engine);
        let mut endpoints = [0, 2].map(|id| {
            let selection = runtime.allocate(4, 1 << id).unwrap();
            let mut endpoint = runtime
                .endpoint_on(
                    selection,
                    &memory,
                    id * 4..id * 4 + 4,
                    0,
                    Instant::now() + TIMEOUT,
                    None,
                )
                .unwrap();
            endpoint.connect(&endpoint.descriptor()).unwrap();
            endpoint
        });
        let first =
            super::super::super::context::tests::pause_device(&runtime.device_at(0).unwrap());
        let second =
            super::super::super::context::tests::pause_device(&runtime.device_at(2).unwrap());
        second.0.lock().unwrap().1 = true;
        second.1.notify_all();
        let end = Instant::now() + Duration::from_secs(2);
        let [a, b] = &mut endpoints;
        let b_record = Arc::clone(&b.record);
        let (a_result, b_result) = thread::scope(|scope| {
            let a_task = scope.spawn(|| a.transfer(&a.descriptor(), operation, end));
            let (state, timed) = first
                .1
                .wait_timeout_while(first.0.lock().unwrap(), Duration::from_secs(1), |state| {
                    !state.0
                })
                .unwrap();
            assert!(!timed.timed_out() && state.0);
            drop(state);
            let waits = runtime.stats().unwrap().cq_event_waits;
            let b_task = scope.spawn(|| b.transfer(&b.descriptor(), operation, end));
            let wait_deadline = Instant::now() + Duration::from_secs(1);
            while b_record
                .send_namespace
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
                || runtime.stats().unwrap().cq_event_waits == waits
            {
                assert!(Instant::now() < wait_deadline);
                thread::yield_now();
            }
            first.0.lock().unwrap().1 = true;
            first.1.notify_all();
            (a_task.join().unwrap(), b_task.join().unwrap())
        });
        let closes = endpoints
            .iter_mut()
            .map(|endpoint| {
                endpoint.deadline = Instant::now() + TIMEOUT;
                endpoint.close()
            })
            .collect::<Vec<_>>();
        drop(endpoints);
        engine.unregister_memory(&memory).unwrap();
        engine.close(TIMEOUT).unwrap();
        a_result.unwrap();
        assert!(
            b_result.is_ok(),
            "a context with no local CQE missed the aggregate quota wake: {b_result:?}"
        );
        assert!(closes.iter().all(Result::is_ok), "{closes:?}");
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.peak_work_requests,
                stats.work_requests,
                stats.progress_failures
            ),
            (1, 0, 0)
        );
    }
}

#[test]
fn source_rejects_an_entire_slice_plan_before_either_peer_creates_qps_and_recovers() {
    use super::super::super::receive::ReceiveCredits;
    for event in [false, true] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let source = rdma_fixture_with_runtime(sliced_runtime(
                u64::MAX,
                16,
                4,
                RdmaHostMemory::Direct,
                event,
            ));
            let requester = rdma_fixture_with_runtime(sliced_runtime(
                u64::MAX,
                16,
                3,
                RdmaHostMemory::Direct,
                event,
            ));
            let local = requester
                .register_memory(vec![0x63; 20].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let remote = source
                .register_memory(
                    vec![0x29; 20].into_boxed_slice(),
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let peer = requester
                .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
                .unwrap();
            let credits =
                ReceiveCredits::acquire(Arc::clone(&source_runtime(&source).usage), 4, 2).unwrap();
            let request = TransferRequest {
                transport: Transport::Rdma,
                local_address: local.base_address() + 4,
                remote_address: remote.base_address() + 4,
                length: 12,
            };
            let report = requester
                .submit_and_wait(operation, &peer, std::slice::from_ref(&request), TIMEOUT)
                .unwrap();
            assert!(
                !report.is_complete() && report.locally_drained,
                "{report:?}"
            );
            let failure = report.failure.unwrap();
            assert!(matches!(failure.error, Error::ResourceExhausted));
            assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
            for engine in [&source, &requester] {
                let stats = engine.stats().unwrap().rdma.unwrap();
                assert_eq!(
                    (
                        stats.opened_connections,
                        stats.active_grants,
                        stats.receive_work_requests
                    ),
                    (0, 0, 0)
                );
            }
            let stats = source.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (stats.receive_credits, stats.receive_admission_rejections),
                (2, 1)
            );
            drop(credits);
            let report = requester
                .submit_and_wait(operation, &peer, &[request], TIMEOUT)
                .unwrap();
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            let mut bytes = [0; 20];
            let target = if operation == TransferOp::Read {
                &local
            } else {
                &remote
            };
            target.read(0, &mut bytes).unwrap();
            let guard = if operation == TransferOp::Read {
                0x63
            } else {
                0x29
            };
            assert_eq!(&bytes[..4], &[guard; 4]);
            assert_eq!(
                &bytes[4..16],
                &[if operation == TransferOp::Read {
                    0x29
                } else {
                    0x63
                }; 12]
            );
            assert_eq!(&bytes[16..], &[guard; 4]);
            requester.unregister_memory(&local).unwrap();
            source.unregister_memory(&remote).unwrap();
            requester.close(TIMEOUT).unwrap();
            source.close(TIMEOUT).unwrap();
            assert_eq!(source.stats().unwrap().rdma.unwrap().receive_credits, 0);
        }
    }
}

#[test]
fn asymmetric_cq_lane_counts_share_context_mrs_and_reuse_bilateral_qps() {
    for event in [false, true] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let make = |lanes| {
                let mut runtime = sliced_runtime(1, 16, 4, RdmaHostMemory::Direct, event);
                runtime.config.num_lanes = lanes;
                single_handler(runtime)
            };
            let source = make(2);
            let requester = make(3);
            let local = requester
                .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let remote = source
                .register_memory(
                    vec![0x29; 16].into_boxed_slice(),
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let peer = requester
                .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
                .unwrap();
            let mut previous = [Vec::new(), Vec::new()];
            for round in 0..3 {
                local.write(0, &[0x63; 16]).unwrap();
                remote.write(0, &[0x29; 16]).unwrap();
                let report = requester
                    .submit_and_wait(
                        operation,
                        &peer,
                        &[TransferRequest {
                            transport: Transport::Rdma,
                            local_address: local.base_address() + 2,
                            remote_address: remote.base_address() + 2,
                            length: 12,
                        }],
                        TIMEOUT,
                    )
                    .unwrap();
                assert!(report.is_complete() && report.locally_drained, "{report:?}");
                let mut actual = [0; 16];
                let (target, payload, guard) = if operation == TransferOp::Read {
                    (&local, 0x29, 0x63)
                } else {
                    (&remote, 0x63, 0x29)
                };
                target.read(0, &mut actual).unwrap();
                assert_eq!(&actual[2..14], &[payload; 12]);
                assert_eq!(&actual[..2], &[guard; 2]);
                assert_eq!(&actual[14..], &[guard; 2]);
                for (index, (engine, lanes, published)) in [(&requester, 3, 2), (&source, 2, 4)]
                    .into_iter()
                    .enumerate()
                {
                    let runtime = source_runtime(engine);
                    let mut pairs = Vec::new();
                    for (lane_id, lane) in runtime.context_set.lock().unwrap().slots[0]
                        .cq_list
                        .iter()
                        .enumerate()
                    {
                        let lane = lane.as_ref().unwrap();
                        let records = lane.records();
                        for record in records {
                            let state = record.lock().unwrap();
                            let resources = state.resources.as_ref().unwrap();
                            assert!(resources.grant.is_none());
                            pairs.push((
                                lane_id,
                                resources.connection.incarnation.unwrap().encode(),
                                resources.connection.handles.qp as usize,
                            ));
                        }
                    }
                    pairs.sort_unstable();
                    assert_eq!(pairs.len(), 3);
                    assert_eq!(
                        pairs
                            .iter()
                            .map(|row| row.0)
                            .collect::<std::collections::BTreeSet<_>>()
                            .len(),
                        lanes
                    );
                    if round != 0 {
                        assert_eq!(
                            pairs, previous[index],
                            "reuse must preserve actual QP and CQ lane"
                        );
                    }
                    previous[index] = pairs;
                    let stats = runtime.stats().unwrap();
                    assert_eq!(
                        (
                            stats.progress_workers,
                            stats.progress_lanes,
                            stats.active_cqs
                        ),
                        (lanes, lanes, lanes * 2)
                    );
                    assert_eq!(stats.opened_connections, 3);
                    assert_eq!(stats.reused_connections, round * 3);
                    assert_eq!(
                        stats.persistent_mrs, published,
                        "CQ lanes must share per-context MRs"
                    );
                    assert_eq!(runtime.workers.inflight_slices(), vec![0; lanes]);
                }
            }
            requester.unregister_memory(&local).unwrap();
            source.unregister_memory(&remote).unwrap();
            requester.close(TIMEOUT).unwrap();
            source.close(TIMEOUT).unwrap();
        }
    }
}
