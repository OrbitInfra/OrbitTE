use super::*;
use crate::engine::tests::rdma_fixture_with_runtime;
use crate::test_support::EVENTUAL_PROGRESS;
use crate::transport::rdma::RdmaHostMemory;
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
            pci_bus_id: None,
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
            route.link_speed_mbps = Some(if nic.name == "rail2" {
                400_000
            } else {
                100_000
            });
            route
        })
        .collect();
    Ok((topology, routes))
}

unsafe extern "C" fn open(handles: *mut sys::DeviceHandles, name: *const c_char) -> i32 {
    // SAFETY: The context constructor supplies a NUL-terminated device name that lives through
    // the open callback.
    let name = unsafe { CStr::from_ptr(name) };
    if name.to_bytes() == b"device_missing" {
        return libc::ENODEV;
    }
    // Both independent CPU context fixtures execute the existing copy controls.
    // SAFETY: The wrapper forwards writable device handles and a live NUL-terminated name to the
    // fixture opener.
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
            &[TransferRequest::new(
                Transport::Rdma,
                local.base_address() + 4,
                remote.base_address() + 4,
                8,
            )],
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
        let paths = source_runtime(&requester)
            .paths(crate::MemoryLocation::Cpu(None))
            .unwrap();
        let remote_paths = source_runtime(&source)
            .paths(crate::MemoryLocation::Cpu(None))
            .unwrap();
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
    let selection = runtime
        .allocate(8, crate::MemoryLocation::Cpu(None), 1 << 2)
        .unwrap();
    assert!(matches!(
        (move || {
            let credits = super::super::super::grant::admission::GrantCredits::acquire(
                Arc::clone(&runtime.usage),
                runtime.config.max_grants,
                1,
            )?;
            runtime.prepare_grant(
                runtime.reserve_grant(selection, &local, &credits)?,
                &local,
                4..12,
                0,
                deadline,
                crate::transport::rdma::rdma_transport::connections::Connection::Fresh(None),
            )
        })(),
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
            handler_timeout: crate::test_support::EVENTUAL_PROGRESS,
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
            1,
            "slices sharing one selected context also share its connected QP"
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
    // SAFETY: The context constructor supplies a NUL-terminated device name that lives through
    // the open callback.
    if unsafe { CStr::from_ptr(name) }.to_bytes() == b"device_missing" {
        return libc::ENODEV;
    }
    // SAFETY: The wrapper forwards writable device handles and a live NUL-terminated name to the
    // fixture opener.
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
            .wait_timeout_while(lock.lock().unwrap(), EVENTUAL_PROGRESS, |state| !state.0)
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
        let (paths, _) = path::read_paths(&mut channel, source.source_epoch()).unwrap();
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
                    qp_index: None,
                    qp_width: 1,
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
    // SAFETY: The context constructor supplies a NUL-terminated device name that lives through
    // the open callback.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    if name == b"device_missing" {
        return libc::ENODEV;
    }
    // SAFETY: The wrapper forwards writable device handles and a live NUL-terminated name to the
    // fixture opener.
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
                &[TransferRequest::new(
                    Transport::Rdma,
                    local.base_address() + 4,
                    remote.base_address() + 4,
                    8,
                )],
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
    // SAFETY: The context constructor supplies a NUL-terminated device name that lives through
    // the open callback.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    if name == b"device_missing" {
        return libc::ENODEV;
    }
    // SAFETY: The wrapper forwards writable device handles and a live NUL-terminated name to the
    // fixture opener.
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
    // SAFETY: The wrapper forwards writable device handles and a live NUL-terminated name to the
    // fixture opener.
    unsafe { open_publication_mode(h, name, c"copy_publication_fault") }
}
unsafe extern "C" fn open_publication_retained(
    h: *mut sys::DeviceHandles,
    name: *const c_char,
) -> i32 {
    // SAFETY: The wrapper forwards writable device handles and a live NUL-terminated name to the
    // fixture opener.
    unsafe { open_publication_mode(h, name, c"copy_publication_close1") }
}
unsafe extern "C" fn open_publication_parent(
    h: *mut sys::DeviceHandles,
    name: *const c_char,
) -> i32 {
    // SAFETY: The wrapper forwards writable device handles and a live NUL-terminated name to the
    // fixture opener.
    unsafe { open_publication_mode(h, name, c"copy_publication_parent") }
}
unsafe extern "C" fn fail_later_registration(
    h: *mut sys::MemoryHandles,
    d: *const sys::DeviceHandles,
    address: *mut std::ffi::c_void,
    length: usize,
    window_bind: bool,
) -> i32 {
    // SAFETY: The fixture stores a boxed FakeDevice in this handle; the context owner keeps it
    // alive for the borrow.
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
    // SAFETY: The registration owner retains the address range and PD through deregistration and
    // supplies writable MR handles.
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
            let selection = runtime
                .allocate(4, crate::MemoryLocation::Cpu(None), 1 << id)
                .unwrap();
            let credits = super::super::super::grant::admission::GrantCredits::acquire(
                Arc::clone(&runtime.usage),
                runtime.config.max_grants,
                1,
            )
            .unwrap();
            let mut endpoint = runtime
                .prepare_grant(
                    runtime.reserve_grant(selection, &memory, &credits).unwrap(),
                    &memory,
                    id * 4..id * 4 + 4,
                    0,
                    Instant::now() + TIMEOUT,
                    crate::transport::rdma::rdma_transport::connections::Connection::Fresh(None),
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
            let a_task =
                scope.spawn(|| a.transfer(&a.descriptor(), operation, crate::Priority::High, end));
            let (state, timed) = first
                .1
                .wait_timeout_while(first.0.lock().unwrap(), EVENTUAL_PROGRESS, |state| !state.0)
                .unwrap();
            assert!(!timed.timed_out() && state.0);
            drop(state);
            let waits = runtime.stats().unwrap().cq_event_waits;
            let b_task =
                scope.spawn(|| b.transfer(&b.descriptor(), operation, crate::Priority::High, end));
            let wait_deadline = Instant::now() + EVENTUAL_PROGRESS;
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
    for grant_budget in [false, true] {
        for event in [false, true] {
            for operation in [TransferOp::Read, TransferOp::Write] {
                let mut runtime = sliced_runtime(u64::MAX, 16, 4, RdmaHostMemory::Direct, event);
                runtime.config.max_grants = 4;
                let source = rdma_fixture_with_runtime(runtime);
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
                let usage = &source_runtime(&source).usage;
                let receive = (!grant_budget)
                    .then(|| ReceiveCredits::acquire(Arc::clone(usage), 4, 2).unwrap());
                let grants = grant_budget.then(|| {
                    super::super::super::grant::admission::GrantCredits::acquire(
                        Arc::clone(usage),
                        4,
                        2,
                    )
                    .unwrap()
                });
                let request = TransferRequest::new(
                    Transport::Rdma,
                    local.base_address() + 4,
                    remote.base_address() + 4,
                    12,
                );
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
                    (
                        stats.receive_credits,
                        stats.receive_admission_rejections,
                        stats.grant_credits
                    ),
                    if grant_budget { (0, 0, 2) } else { (2, 1, 0) }
                );
                drop((receive, grants));
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
                let stats = source.stats().unwrap().rdma.unwrap();
                assert_eq!((stats.receive_credits, stats.grant_credits), (0, 0));
            }
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
                        &[TransferRequest::new(
                            Transport::Rdma,
                            local.base_address() + 2,
                            remote.base_address() + 2,
                            12,
                        )],
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
                            let connection = resources.connection.lock().unwrap();
                            pairs.push((
                                lane_id,
                                connection.incarnation.unwrap().encode(),
                                connection.handles.qp as usize,
                            ));
                        }
                    }
                    pairs.sort_unstable();
                    assert_eq!(pairs.len(), 2);
                    assert_eq!(
                        pairs
                            .iter()
                            .map(|row| row.0)
                            .collect::<std::collections::BTreeSet<_>>()
                            .len(),
                        2
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
                    assert_eq!(stats.opened_connections, 2);
                    assert_eq!(stats.reused_connections, round * 3 + 1);
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

#[test]
fn periodic_source_recovery_retires_every_stale_array_entry_before_single_or_sliced_payload() {
    for (event, operation, length) in [
        (false, TransferOp::Read, 1),
        (true, TransferOp::Write, 12),
        (false, TransferOp::Write, 12),
        (true, TransferOp::Read, 1),
    ] {
        let make = || {
            let mut runtime = sliced_runtime(1, 16, 4, RdmaHostMemory::Direct, event);
            runtime.config.num_lanes = 3;
            single_handler(runtime)
        };
        let source = make();
        let requester = make();
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
        let transfer = |length| {
            local.write(0, &[0x63; 16]).unwrap();
            remote.write(0, &[0x29; 16]).unwrap();
            let report = requester
                .submit_and_wait(
                    operation,
                    &peer,
                    &[TransferRequest::new(
                        Transport::Rdma,
                        local.base_address() + 2,
                        remote.base_address() + 2,
                        length,
                    )],
                    TIMEOUT,
                )
                .unwrap();
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            let (target, payload, guard) = if operation == TransferOp::Read {
                (&local, 0x29, 0x63)
            } else {
                (&remote, 0x63, 0x29)
            };
            let mut actual = [0; 16];
            target.read(0, &mut actual).unwrap();
            let mut expected = [guard; 16];
            expected[2..2 + length].fill(payload);
            assert_eq!(actual, expected);
        };
        transfer(12);
        assert_eq!(requester.stats().unwrap().rdma.unwrap().active_endpoints, 3);
        let runtime = source_runtime(&source);
        let owner = runtime.device_at(0).unwrap();
        let domain = owner.domain().unwrap();
        let mrs = runtime.stats().unwrap().opened_persistent_mrs;
        owner.asynchronous.fence_route();
        let deadline = Instant::now() + EVENTUAL_PROGRESS;
        while runtime.stats().unwrap().route_recoveries == 0 {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(runtime.stats().unwrap().active_endpoints, 0);
        transfer(length);
        assert_eq!(
            requester.stats().unwrap().rdma.unwrap().closed_connections,
            3
        );
        assert_eq!(owner.domain().unwrap(), domain);
        assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, mrs);
        let opened = requester.stats().unwrap().rdma.unwrap().opened_connections;
        transfer(length);
        assert_eq!(
            requester.stats().unwrap().rdma.unwrap().opened_connections,
            opened
        );
        requester.close(TIMEOUT).unwrap();
        source.close(TIMEOUT).unwrap();
    }
}

#[test]
fn initially_paused_rail_keeps_registration_without_blocking_its_healthy_sibling() {
    fn paused(
        config: &RdmaConfig,
    ) -> Result<(
        Topology,
        Vec<crate::transport::rdma::discovery::RdmaAddress>,
    )> {
        let (topology, mut addresses) = discover(config)?;
        addresses[0].active = false;
        Ok((topology, addresses))
    }
    static VERBS: sys::Verbs = sys::Verbs {
        discover: paused,
        open_device: open,
        ..fixture::VERBS
    };
    let mut owner = runtime(5, 4);
    owner.verbs = &VERBS;
    owner.registrations =
        buffers::LocalBufferManager::new(&owner.config, Arc::clone(&owner.usage), &VERBS);
    let engine = rdma_fixture_with_runtime(owner);
    let memory = engine
        .register_memory(vec![3; 16].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let runtime = source_runtime(&engine);
    let paused = runtime.device_at(0).unwrap();
    let healthy = runtime.device_at(2).unwrap();
    let domain = healthy.domain().unwrap();
    assert!(paused.asynchronous.paused());
    assert!(healthy.asynchronous.check().is_ok());
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 4);
    assert_eq!(
        runtime
            .paths(crate::MemoryLocation::Cpu(None))
            .unwrap()
            .iter()
            .map(|path| path.nic_id)
            .collect::<Vec<_>>(),
        [2]
    );
    assert!(
        runtime
            .allocate(8, crate::MemoryLocation::Cpu(None), 1)
            .is_err()
    );
    assert_eq!(
        runtime
            .allocate(8, crate::MemoryLocation::Cpu(None), 4)
            .unwrap()
            .device_id(),
        2
    );
    crate::transport::rdma::context::tests::change_route(runtime, |route| route.active = true);
    let end = Instant::now() + EVENTUAL_PROGRESS;
    while runtime.stats().unwrap().route_recoveries == 0 {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        runtime
            .paths(crate::MemoryLocation::Cpu(None))
            .unwrap()
            .iter()
            .map(|path| path.nic_id)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    assert!(Arc::ptr_eq(&paused, &runtime.device_at(0).unwrap()));
    assert!(Arc::ptr_eq(&healthy, &runtime.device_at(2).unwrap()));
    assert_eq!(healthy.domain().unwrap(), domain);
    assert_eq!(runtime.stats().unwrap().opened_persistent_mrs, 4);
    assert_eq!(runtime.stats().unwrap().opened_devices, 2);
    assert_eq!(runtime.stats().unwrap().opened_connections, 0);
    memory.write(0, &[4; 16]).unwrap();
    engine.close(TIMEOUT).unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
}

#[test]
fn configured_rails_drive_public_transfers_without_compacting_failed_contexts() {
    let mut configured = runtime(u64::MAX, 8);
    configured.config.priority_matrix = Some(r#"{"*":[["device_missing"],["rail2"]]}"#.into());
    let source = rdma_fixture_with_runtime(configured);
    let mut configured = runtime(u64::MAX, 8);
    configured.config.priority_matrix = Some(r#"{"*":[["rail2"],[]]}"#.into());
    let requester = rdma_fixture_with_runtime(configured);
    let local = requester
        .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 16].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    for op in [TransferOp::Read, TransferOp::Write] {
        local.write(0, &[0x63; 16]).unwrap();
        remote.write(0, &[0x29; 16]).unwrap();
        exchange(&requester, &source, &local, &remote, op);
    }
    for engine in [&source, &requester] {
        let runtime = source_runtime(engine);
        assert_eq!(
            runtime
                .paths(crate::MemoryLocation::Cpu(None))
                .unwrap()
                .iter()
                .map(|p| p.nic_id)
                .collect::<Vec<_>>(),
            [2]
        );
        assert!(
            runtime
                .allocate(1, crate::MemoryLocation::Cpu(None), 1)
                .is_err()
        );
        let contexts = runtime.context_set.lock().unwrap();
        assert_eq!(contexts.slots.len(), 3);
        assert!(contexts.slots[1].owner.is_none());
        assert!(contexts.slots[0].cq_list.is_empty());
        assert!(!contexts.slots[2].cq_list.is_empty());
        drop(contexts);
        assert_eq!(engine.stats().unwrap().rdma.unwrap().direct_bytes, 0);
    }
    source.unregister_memory(&remote).unwrap();
    requester.unregister_memory(&local).unwrap();
    source.close(TIMEOUT).unwrap();
    requester.close(TIMEOUT).unwrap();
}

#[test]
fn invalid_topology_fails_before_context_or_mr_publication() {
    let mut runtime = runtime(u64::MAX, 8);
    runtime.config.priority_matrix = Some(r#"{"*":[["unknown_nic"],[]]}"#.into());
    assert!(matches!(
        runtime.paths(crate::MemoryLocation::Cpu(None)),
        Err(Error::InvalidConfig(_))
    ));
    let contexts = runtime.context_set.lock().unwrap();
    assert!(contexts.slots.is_empty());
    assert!(contexts.topology.is_none());
    drop(contexts);
    assert_eq!(runtime.stats().unwrap().opened_devices, 0);
}

#[test]
fn public_read_write_dispatches_to_the_faster_rail_and_publishes_payload_estimates() {
    let configured = || {
        let mut runtime = runtime(u64::MAX, 8);
        runtime.config.scheduling.smart_selection = true;
        runtime.config.scheduling.bandwidth_learning_rate = 1.0;
        runtime
    };
    let source = rdma_fixture_with_runtime(configured());
    let requester = rdma_fixture_with_runtime(configured());
    assert!(requester.nic_load_stats().unwrap().is_empty());
    let local = requester
        .register_memory(vec![0x63; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 16].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    for op in [TransferOp::Read, TransferOp::Write] {
        local.write(0, &[0x63; 16]).unwrap();
        remote.write(0, &[0x29; 16]).unwrap();
        exchange(&requester, &source, &local, &remote, op);
    }
    let stats = requester.nic_load_stats().unwrap();
    assert_eq!(
        stats.iter().map(|s| s.device_id).collect::<Vec<_>>(),
        [0, 2]
    );
    assert_eq!(stats[0].selection_samples, 0);
    assert_eq!(
        (
            stats[1].link_speed_gbps,
            stats[1].completed_bytes,
            stats[1].selection_samples
        ),
        (Some(400.0), 16, 2)
    );
    assert_eq!((stats[1].posted_bytes, stats[1].inflight_bytes), (0, 0));
    assert_eq!(stats[1].ewma_bandwidth_bps, 50e9);
    assert_eq!(
        source
            .nic_load_stats()
            .unwrap()
            .iter()
            .map(|s| s.selection_samples)
            .sum::<u64>(),
        0,
        "source receipts never teach requester payload bandwidth"
    );
    requester.unregister_memory(&local).unwrap();
    source.unregister_memory(&remote).unwrap();
    requester.close(TIMEOUT).unwrap();
    source.close(TIMEOUT).unwrap();
    assert!(requester.nic_load_stats().unwrap().is_empty());
}

#[test]
fn weighted_parent_read_write_uses_exact_tail_bytes_and_compatible_remote_rails() {
    let slots = crate::transport::rdma::shared_quota::tests::Object::new();
    for source_mask in [u64::MAX, 1, 4] {
        let configured = |mask| {
            let mut runtime = sliced_runtime(mask, 8, 16, RdmaHostMemory::Direct, false);
            runtime.config.block_size = 8;
            runtime.config.shared_quota_shm_path = Some(slots.name().to_owned());
            runtime.config.scheduling.smart_selection = true;
            runtime.config.scheduling.bandwidth_learning_rate = 1.0;
            runtime
        };
        let source = single_handler(configured(source_mask));
        let requester = single_handler(configured(u64::MAX));
        let local = requester
            .register_memory(vec![0x63; 49].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 49].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        for op in [TransferOp::Read, TransferOp::Write] {
            local.write(0, &[0x63; 49]).unwrap();
            remote.write(0, &[0x29; 49]).unwrap();
            let report = requester
                .submit_and_wait(
                    op,
                    &peer,
                    &[TransferRequest::new(
                        Transport::Rdma,
                        local.base_address() + 4,
                        remote.base_address() + 4,
                        41,
                    )
                    .with_priority(crate::Priority::Low)],
                    TIMEOUT,
                )
                .unwrap();
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            let mut bytes = [0; 49];
            let (target, payload, guard) = if op == TransferOp::Read {
                (&local, 0x29, 0x63)
            } else {
                (&remote, 0x63, 0x29)
            };
            target.read(0, &mut bytes).unwrap();
            assert_eq!(&bytes[4..45], &[payload; 41]);
            assert_eq!(&bytes[..4], &[guard; 4]);
            assert_eq!(&bytes[45..], &[guard; 4]);
        }
        let stats = requester.nic_load_stats().unwrap();
        let expected = match source_mask {
            1 => [82, 0],
            4 => [0, 82],
            _ => [16, 66],
        };
        assert_eq!(
            stats.iter().map(|s| s.completed_bytes).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            stats.iter().map(|s| s.allocated_bytes).collect::<Vec<_>>(),
            expected
        );
        for engine in [&requester, &source] {
            assert!(
                engine
                    .nic_load_stats()
                    .unwrap()
                    .iter()
                    .all(|s| s.inflight_bytes == 0 && s.posted_bytes == 0)
            );
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    stats.grant_credits,
                    stats.receive_credits,
                    stats.active_grants
                ),
                (0, 0, 0)
            );
        }
        requester.unregister_memory(&local).unwrap();
        source.unregister_memory(&remote).unwrap();
        requester.close(TIMEOUT).unwrap();
        source.close(TIMEOUT).unwrap();
        #[cfg(feature = "metrics")]
        assert_eq!(
            requester.metrics().unwrap().json()["orbite_rdma_allocated_bytes_total"],
            82
        );
    }
}

#[test]
fn named_policy_masks_every_slice_and_connects_only_its_qp_pool() -> Result<()> {
    use crate::{EngineConfig, IntentType, TransportSelectorConfig};
    let selector = TransportSelectorConfig::from_json(
        r#"{
        "policy":[
          {"name":"fast","segment_type":"memory","intent_type":"foreground_get","devices":["rail2","typo"],"qp_pool":"fg","transports":["rdma","tcp"]},
          {"name":"bulk","segment_type":"memory","devices":["rail0"],"qp_pool":"bg","service_level":9,"transports":["rdma"]},
          {"name":"masked","segment_type":"memory","devices":["device_missing"],"transports":["rdma"]}],
        "transports":{"rdma":{"endpoint":{"qp_pools":[
           {"name":"bg","num_qp":1,"service_level":3,"traffic_class":40},
           {"name":"fg","num_qp":2,"service_level":6,"traffic_class":96}]}}}}"#,
    )?;
    let make = || {
        let mut transport = runtime(u64::MAX, 12);
        transport.config.num_lanes = 2;
        transport.config.block_size = 8;
        crate::engine::tests::rdma_fixture_with_config(
            transport,
            EngineConfig {
                selector: Some(selector.clone()),
                ..Default::default()
            },
        )
    };
    let source = make();
    let requester = Arc::new(make());
    let local =
        requester.register_memory(vec![0x3a; 48].into_boxed_slice(), Permission::LocalOnly)?;
    let remote = source.register_memory(
        vec![0x71; 48].into_boxed_slice(),
        Permission::RemoteReadWrite,
    )?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let req = TransferRequest::new(
        Transport::Unspec,
        local.base_address() + 4,
        remote.base_address() + 4,
        32,
    )
    .with_intent(IntentType::ForegroundGet);
    assert_eq!(requester.stats()?.unresolved_policy_devices, 1);
    let mut batch = requester.allocate_batch(1)?;
    assert!(
        matches!(batch.submit(TransferOp::Write,&peer,&[req.clone().with_policy_name("masked")?],None,TIMEOUT),
        Err(Error::TransportPolicy {policy,reason}) if policy.as_str()=="masked" && reason.contains("device mask"))
    );
    assert_eq!(requester.stats()?.rdma.unwrap().connected_pool_indices, 0);
    let report = requester.submit_and_wait(
        TransferOp::Write,
        &peer,
        std::slice::from_ref(&req),
        TIMEOUT,
    )?;
    assert!(report.is_complete(), "{report:?}");
    let mut bytes = [0u8; 48];
    remote.read(0, &mut bytes)?;
    let mut expected = [0x71; 48];
    expected[4..36].fill(0x3a);
    assert_eq!(bytes, expected);
    let nic_bytes = |engine: &TransferEngine, id| -> Result<u64> {
        Ok(engine
            .nic_load_stats()?
            .iter()
            .find(|n| n.device_id == id)
            .unwrap()
            .completed_bytes)
    };
    assert_eq!(
        (nic_bytes(&requester, 0)?, nic_bytes(&requester, 2)?),
        (0, 32)
    );
    for engine in [&*requester, &source] {
        assert_eq!(engine.stats()?.rdma.unwrap().connected_pool_indices, 0b110);
        assert_eq!(
            super::super::super::context::tests::link_qos(source_runtime(engine), 2),
            [(6, 96); 2]
        );
    }
    remote.write(4, &[0x18; 32])?;
    let task = batch.submit(
        TransferOp::Read,
        &peer,
        &[req.with_policy_name("bulk")?],
        None,
        TIMEOUT,
    )?;
    assert_eq!(task, 0..1);
    batch.seal()?;
    assert_eq!(batch.wait(TIMEOUT)?.completed_tasks, 1);
    batch.free()?;
    local.read(0, &mut bytes)?;
    expected = [0x3a; 48];
    expected[4..36].fill(0x18);
    assert_eq!(bytes, expected);
    assert_eq!(
        (nic_bytes(&requester, 0)?, nic_bytes(&requester, 2)?),
        (32, 32)
    );
    for engine in [&*requester, &source] {
        assert_eq!(engine.stats()?.rdma.unwrap().connected_pool_indices, 7);
        assert_eq!(
            super::super::super::context::tests::link_qos(source_runtime(engine), 0),
            [(3, 40)]
        );
    }
    // Keep public task IDs and per-task routes in A/B/A order, followed by a TCP hint.
    let mut mixed = requester.allocate_batch(4)?;
    let mut requests = Vec::new();
    for (i, name) in ["fast", "bulk", "fast", "fast"].into_iter().enumerate() {
        let hint = if i == 3 {
            Transport::Tcp
        } else {
            Transport::Rdma
        };
        requests.push(
            TransferRequest::new(
                hint,
                local.base_address() + i as u64 * 4,
                remote.base_address() + i as u64 * 4,
                4,
            )
            .with_policy_name(name)?,
        );
    }
    local.write(0, &[0xa5; 16])?;
    assert_eq!(
        mixed.submit(TransferOp::Read, &peer, &requests, None, TIMEOUT)?,
        0..4
    );
    mixed.seal()?;
    assert_eq!(mixed.wait(TIMEOUT)?.completed_tasks, 4);
    for i in 0..4 {
        assert_eq!(mixed.task_status(i)?.transferred_bytes, 4);
    }
    let mut wanted = [0; 16];
    remote.read(0, &mut wanted)?;
    local.read(0, &mut bytes[..16])?;
    assert_eq!(&bytes[..16], &wanted);
    assert_eq!(
        (nic_bytes(&requester, 0)?, nic_bytes(&requester, 2)?),
        (36, 40)
    );
    mixed.free()?;
    let mismatch = crate::engine::tests::rdma_fixture_with_config(
        runtime(u64::MAX, 12),
        EngineConfig {
            selector: Some(TransportSelectorConfig::from_json(
                r#"{"transports":{"rdma":{"endpoint":{"qp_pools":[{"name":"wrong","num_qp":2}]}}}}"#,
            )?),
            ..Default::default()
        },
    );
    let untouched = mismatch.register_memory(
        vec![0xa3; 16].into_boxed_slice(),
        Permission::RemoteReadWrite,
    )?;
    let peer = requester.open_segment(mismatch.endpoint(), mismatch.source_epoch(), TIMEOUT)?;
    let bad = TransferRequest::new(
        Transport::Rdma,
        local.base_address(),
        untouched.base_address(),
        8,
    )
    .with_policy_name("bulk")?;
    let report = requester.submit_and_wait(TransferOp::Write, &peer, &[bad], TIMEOUT)?;
    let fail = report.failure.unwrap();
    assert!(matches!(
        fail.error,
        Error::Protocol("RDMA peer QP pool width mismatch")
    ));
    assert_eq!(fail.remote_effect, crate::RemoteEffect::None);
    assert_eq!(mismatch.stats()?.rdma.unwrap().connected_pool_indices, 0);
    untouched.read(0, &mut bytes[..16])?;
    assert_eq!(&bytes[..16], &[0xa3; 16]);
    Ok(())
}

#[test]
fn admission_uses_the_selected_rail_rate_and_drops_aliases_before_any_native_post() -> Result<()> {
    use crate::{EngineConfig, RuntimeQueueConfig, TaskState, TransportSelectorConfig};
    let selector = TransportSelectorConfig::from_json(
        r#"{"policy":[
        {"name":"slow","segment_type":"memory","devices":["rail0"],"transports":["rdma"]},
        {"name":"fast","segment_type":"memory","devices":["rail2"],"transports":["rdma"]},{"name":"tcp","segment_type":"memory","transports":["tcp"]}]}"#,
    )?;
    let source = rdma_fixture_with_runtime(runtime(u64::MAX, 8));
    let requester = Arc::new(crate::engine::tests::rdma_fixture_with_config(
        runtime(u64::MAX, 8),
        EngineConfig {
            selector: Some(selector),
            runtime_queue: Some(RuntimeQueueConfig {
                deadline_aware: true,
                mlu_local_threshold: 2e-11,
                max_outstanding_owners: 1,
                ..Default::default()
            }),
            ..Default::default()
        },
    ));
    let local =
        requester.register_memory((0..32u8).collect::<Vec<_>>().into(), Permission::LocalOnly)?;
    let remote = source.register_memory(vec![0x91; 32].into(), Permission::RemoteReadWrite)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let loads = requester.nic_load_stats()?;
    assert_eq!(
        loads
            .iter()
            .map(|n| (n.device_id, n.link_speed_gbps))
            .collect::<Vec<_>>(),
        [(0, Some(100.0)), (2, Some(400.0))]
    );
    // Eight bytes / 12.5 GB/s / 20 s exceeds the threshold on rail0.
    // The same eight bytes on rail2 fit. Other transports cannot borrow either rate.
    let deadline = Duration::from_secs(20);
    let req =
        |t, i| TransferRequest::new(t, local.base_address() + i, remote.base_address() + i, 4);
    for (name, drop) in [("slow", true), ("fast", false)] {
        let r = [
            req(Transport::Rdma, 4).with_policy_name(name)?,
            req(Transport::Rdma, 8).with_policy_name(name)?,
        ];
        let mut batch = requester.submit_async(TransferOp::Write, &peer, &r, None, deadline)?;
        let status = batch.wait(deadline)?;
        let tasks = batch.task_statuses()?;
        assert!(
            tasks
                .iter()
                .all(|t| t.physical_owner == 0 && t.locally_drained)
        );
        let mut got = [0; 32];
        remote.read(0, &mut got)?;
        let mut expected = [0x91; 32];
        let stats = requester.stats()?;
        assert_eq!(stats.runtime_queue.unwrap().deadline_infeasible_drops, 1);
        if drop {
            assert!(tasks.iter().all(|t| t.state == TaskState::Cancelled
                && matches!(t.failure.as_ref().unwrap().error, Error::DeadlineInfeasible)));
            let g = batch.submissions()?.remove(0);
            assert_eq!(
                (g.completed_requests, g.failure.unwrap().affected_requests),
                (0, 2)
            );
            #[cfg(feature = "metrics")]
            {
                let j = requester.metrics().unwrap().json();
                assert_eq!(j["orbite_queue_deadline_drops_total"], 1);
                assert_eq!(j["tent_deadline_infeasible_total"], 1);
                assert_eq!(j["tent_transport_attempts_total"], 0);
            }
            assert_eq!(stats.rdma.unwrap().opened_connections, 0);
            assert_eq!(source.stats()?.rdma.unwrap().receive_post_calls, 0);
        } else {
            assert_eq!(status.completed_tasks, 2);
            assert!(tasks.iter().all(|t| t.transferred_bytes == 4));
            expected[4..12].copy_from_slice(&[4, 5, 6, 7, 8, 9, 10, 11]);
            assert_eq!(stats.rdma.unwrap().completed_sends, 1);
            assert_eq!(
                requester
                    .nic_load_stats()?
                    .iter()
                    .find(|n| n.device_id == 2)
                    .unwrap()
                    .completed_bytes,
                8
            );
        }
        assert_eq!(got, expected);
        batch.free()?;
    }
    let r = [
        req(Transport::Tcp, 16).with_policy_name("tcp")?,
        req(Transport::Tcp, 20).with_policy_name("tcp")?,
    ];
    assert_eq!(
        requester
            .submit_and_wait(TransferOp::Write, &peer, &r, deadline)?
            .completed_requests,
        2
    );
    let mut got = [0; 32];
    remote.read(0, &mut got)?;
    let mut expected = [0x91; 32];
    expected[4..12].copy_from_slice(&[4, 5, 6, 7, 8, 9, 10, 11]);
    expected[16..24].copy_from_slice(&[16, 17, 18, 19, 20, 21, 22, 23]);
    assert_eq!(got, expected);
    Ok(())
}

#[test]
fn an_infeasible_merged_write_does_not_hide_a_later_feasible_tcp_owner() -> Result<()> {
    let source = rdma_fixture_with_runtime(runtime(u64::MAX, 6));
    let requester = Arc::new(crate::engine::tests::rdma_fixture_with_config(
        runtime(u64::MAX, 6),
        crate::EngineConfig {
            runtime_queue: Some(crate::RuntimeQueueConfig {
                deadline_aware: true,
                mlu_local_threshold: f64::MIN_POSITIVE,
                max_dispatch_owners: 1,
                ..Default::default()
            }),
            ..Default::default()
        },
    ));
    let local = requester.register_memory(vec![0x5a; 8].into(), Permission::LocalOnly)?;
    let remote = source.register_memory(vec![0xaa; 8].into(), Permission::RemoteReadWrite)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let r = [
        (Transport::Rdma, 1),
        (Transport::Tcp, 6),
        (Transport::Rdma, 2),
    ]
    .map(|(t, i)| TransferRequest::new(t, local.base_address() + i, remote.base_address() + i, 1));
    for asynchronous in [true, false] {
        remote.write(0, &[0xaa; 8])?;
        let f = if asynchronous {
            let mut b = requester.submit_async(TransferOp::Write, &peer, &r, None, TIMEOUT)?;
            assert_eq!(b.wait(TIMEOUT)?.completed_tasks, 1);
            assert_eq!(
                (
                    b.task_status(1)?.transferred_bytes,
                    b.task_status(2)?.physical_owner
                ),
                (1, 0)
            );
            for i in [0, 2] {
                assert!(matches!(
                    b.task_status(i)?.failure.unwrap().error,
                    Error::DeadlineInfeasible
                ));
            }
            let report = b.submissions()?.remove(0);
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (0, 0)
            );
            b.free()?;
            report.failure.unwrap()
        } else {
            let report = requester.submit_and_wait(TransferOp::Write, &peer, &r, TIMEOUT)?;
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (0, 0)
            );
            Arc::new(report.failure.unwrap())
        };
        assert!(matches!(f.error, Error::DeadlineInfeasible));
        assert_eq!(
            (f.request_index, f.affected_requests, f.remote_effect),
            (0, 3, crate::RemoteEffect::Unknown)
        );
        let mut got = [0; 8];
        remote.read(0, &mut got)?;
        assert_eq!(got, [0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0x5a, 0xaa]);
        assert_eq!(requester.stats()?.rdma.unwrap().opened_connections, 0);
    }
    Ok(())
}

#[test]
fn a_masked_pair_fault_keeps_the_other_nic_live_and_requires_one_recovery_receipt() -> Result<()> {
    use crate::engine::tests::{rdma_fixture_with_config, source_runtime};
    use crate::{EngineConfig, TransportSelectorConfig};
    use std::sync::atomic::{AtomicBool, Ordering};
    static FAIL: AtomicBool = AtomicBool::new(false);
    unsafe extern "C" fn post(
        h: *mut sys::ConnectionHandles,
        m: *const sys::MemoryHandles,
        w: *const sys::WorkRequest,
        n: usize,
        local: u64,
        len: usize,
        remote: u64,
        rkey: u32,
        write: i32,
        r: *mut sys::PostResult,
    ) -> i32 {
        if FAIL.swap(false, Ordering::AcqRel) {
            // SAFETY: the production post owner supplies its live output slot.
            unsafe {
                *r = sys::PostResult::default();
            }
            return libc::EIO;
        }
        // SAFETY: forward the live QP, registration, WR array and result from the production owner.
        unsafe { (fixture::VERBS.post)(h, m, w, n, local, len, remote, rkey, write, r) }
    }
    static VERBS: sys::Verbs = sys::Verbs {
        discover,
        open_device: open,
        post,
        ..fixture::VERBS
    };
    let mut r = runtime(5, 16);
    r.verbs = &VERBS;
    r.registrations = buffers::LocalBufferManager::new(&r.config, r.usage.clone(), &VERBS);
    r.config.block_size = 4;
    r.config.rail_error_threshold = 1;
    r.config.rail_cooldown = Duration::from_secs(5);
    r.config.rail_probe_interval = Duration::from_secs(2);
    r.config.rail_topology = Some(
        r#"{"all":[{"local":"rail0","remote":"rail0"},{"local":"rail2","remote":"rail2"}]}"#.into(),
    );
    let rails = r.workers.monitor.rails.clone();
    let start = Instant::now();
    rails.set_time(start);
    let source = rdma_fixture_with_runtime(runtime(5, 16));
    let requester = rdma_fixture_with_config(
        r,
        EngineConfig {
            selector: Some(TransportSelectorConfig::from_json(
                r#"{"policy":[{"name":"first","segment_type":"memory","devices":["rail0"],"transports":["rdma"]},{"name":"other","segment_type":"memory","devices":["rail2"],"transports":["rdma"]},{"name":"both","segment_type":"memory","transports":["rdma"]}]}"#,
            )?),
            ..Default::default()
        },
    );
    let local = requester.register_memory(vec![0xcc; 32].into(), Permission::LocalOnly)?;
    let remote = source.register_memory(
        (0..32u8).collect::<Vec<_>>().into(),
        Permission::RemoteReadWrite,
    )?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let run = |policy: &str, op: TransferOp, offset: u64| {
        requester.submit_and_wait(
            op,
            &peer,
            &[TransferRequest::new(
                Transport::Rdma,
                local.base_address() + offset,
                remote.base_address() + offset,
                4,
            )
            .with_policy_name(policy)?],
            TIMEOUT,
        )
    };
    // The requester's failed post retires locally first. The source CQ may
    // still own that grant, or be detaching its failed lane. Only its own
    // recovery receipt permits a fresh grant, even for a disjoint range.
    let wait_source = || -> Result<()> {
        let source_rdma = source_runtime(&source);
        let until = Instant::now() + EVENTUAL_PROGRESS;
        loop {
            source_rdma.recover_progress()?;
            let stats = source_rdma.stats()?;
            assert_eq!(
                (
                    stats.retained_endpoints,
                    stats.retained_grants,
                    stats.retained_cqs
                ),
                (0, 0, 0)
            );
            if source.stats()?.active_source_operations == 0
                && stats.active_grants == 0
                && stats.progress_failures == stats.progress_recoveries
            {
                return Ok(());
            }
            assert!(
                Instant::now() < until,
                "source grant/lane never retired: {stats:?}"
            );
            std::thread::yield_now();
        }
    };
    FAIL.store(true, Ordering::Release);
    assert!(run("first", TransferOp::Read, 4)?.failure.is_some());
    let stats = source_runtime(&requester).stats()?;
    assert_eq!(
        (stats.rail_errors, stats.open_rails, stats.rail_probes),
        (1, 1, 0)
    );
    assert!(matches!(
        run("first", TransferOp::Read, 4)?.failure.unwrap().error,
        Error::TransportPolicy { .. }
    ));
    assert_eq!(
        source_runtime(&requester).stats()?.post_calls,
        stats.post_calls
    );
    wait_source()?;
    let other = run("other", TransferOp::Read, 12)?;
    assert!(
        other.is_complete(),
        "healthy rail: {other:?}, source: {:?}",
        source.stats()?
    );
    let before = requester.nic_load_stats()?;
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[TransferRequest::new(
                    Transport::Rdma,
                    local.base_address() + 16,
                    remote.base_address() + 16,
                    8
                )
                .with_policy_name("both")?],
                TIMEOUT
            )?
            .is_complete()
    );
    let after = requester.nic_load_stats()?;
    for old in before {
        let delta = after
            .iter()
            .find(|n| n.device_id == old.device_id)
            .unwrap()
            .completed_bytes
            - old.completed_bytes;
        assert_eq!(delta, if old.device_name == "rail2" { 8 } else { 0 });
    }
    rails.set_time(start + Duration::from_secs(2));
    FAIL.store(true, Ordering::Release);
    assert!(run("first", TransferOp::Read, 4)?.failure.is_some());
    let open = source_runtime(&requester).stats()?;
    assert_eq!(
        (open.rail_errors, open.open_rails, open.rail_probes),
        (2, 1, 1)
    );
    wait_source()?;
    rails.set_time(start + Duration::from_secs(5));
    let trial = run("first", TransferOp::Read, 4)?;
    assert!(
        trial.is_complete(),
        "trial result {trial:?}; rail stats {:?}",
        source_runtime(&requester).stats()?
    );
    let normal = run("first", TransferOp::Read, 20)?;
    assert!(normal.is_complete(), "{normal:?}");
    let stats = source_runtime(&requester).stats()?;
    assert_eq!(
        (stats.open_rails, stats.rail_probes, stats.rail_recoveries),
        (0, 2, 1)
    );
    #[cfg(feature = "metrics")]
    {
        let j = requester.metrics().unwrap().json();
        assert_eq!(j["orbite_rdma_open_rails"], 0);
        assert_eq!(j["orbite_rdma_rail_recoveries_total"], 1);
        assert_eq!(j["orbite_rdma_rail_probes_total"], 2);
        assert_eq!(j["orbite_rdma_rail_errors_total"], 2);
    }
    let nics = requester.nic_load_stats()?;
    assert_eq!(
        nics.iter()
            .find(|n| n.device_name == "rail0")
            .unwrap()
            .rail_errors,
        2
    );
    assert_eq!(
        nics.iter()
            .find(|n| n.device_name == "rail2")
            .unwrap()
            .rail_errors,
        0
    );
    let mut actual = [0; 32];
    local.read(0, &mut actual)?;
    let mut expected = [0xcc; 32];
    for i in (4..8).chain(12..24) {
        expected[i] = i as u8;
    }
    assert_eq!(actual, expected);
    remote.read(0, &mut actual)?;
    assert_eq!(actual, std::array::from_fn(|i| i as u8));
    Ok(())
}

#[test]
fn r1b_selective_payloads_and_clamped_qps_move_full_guarded_bytes() -> Result<()> {
    let default = RdmaConfig::default();
    assert_eq!(default.signal_interval, 1);
    assert!(matches!(
        RdmaConfig {
            signal_interval: 0,
            ..default
        }
        .validate(),
        Err(Error::InvalidConfig(
            "RDMA signal_interval must be positive"
        ))
    ));
    for (interval, cqes, name, depths) in [
        (1, 8, "copy", (1, 1)),
        (3, 3, "copy_read_caps", (2, 2)),
        (usize::MAX, 2, "copy", (0, 1)),
    ] {
        let make = || {
            rdma_fixture_with_runtime(fixture::runtime_with_config(RdmaConfig {
                host_memory: RdmaHostMemory::Direct,
                block_size: 8,
                max_send_wr: 8,
                signal_interval: interval,
                max_rd_atomic: if interval == usize::MAX { 0 } else { 16 },
                max_dest_rd_atomic: 2,
                ..RdmaConfig::new(name)
            }))
        };
        let source = make();
        let requester = make();
        let src: Vec<u8> = (0..72).collect();
        let dst: Vec<u8> = (0..72).map(|i| 255 - i).collect();
        let remote = source.register_memory(src.clone().into(), Permission::RemoteReadWrite)?;
        let local = requester.register_memory(dst.clone().into(), Permission::LocalOnly)?;
        let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
        for op in [TransferOp::Read, TransferOp::Write] {
            local.write(0, &dst)?;
            remote.write(0, &src)?;
            let before = requester.stats()?.rdma.unwrap();
            let req = TransferRequest::new(
                Transport::Rdma,
                local.base_address() + 4,
                remote.base_address() + 4,
                64,
            );
            let report = requester.submit_and_wait(op, &peer, &[req], TIMEOUT)?;
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            assert_eq!(report.transferred_bytes, 64);
            let after = requester.stats()?.rdma.unwrap();
            assert_eq!(after.payload_send_cqes - before.payload_send_cqes, cqes);
            assert_eq!(
                after.completed_reads + after.completed_writes
                    - before.completed_reads
                    - before.completed_writes,
                8
            );
            assert_eq!(after.work_requests, 0);
            for engine in [&source, &requester] {
                let qp = engine.stats()?.rdma.unwrap().last_connected_qp.unwrap();
                assert_eq!((qp.initiator, qp.responder), depths);
                assert_eq!(
                    (qp.initiator_limit, qp.responder_limit),
                    if name == "copy" { (0, 0) } else { (2, 3) }
                );
                assert_eq!(qp.supports_atomics, name == "copy_read_caps");
            }
            let mut expected_local = dst.clone();
            let mut expected_remote = src.clone();
            if op == TransferOp::Read {
                expected_local[4..68].copy_from_slice(&src[4..68]);
            } else {
                expected_remote[4..68].copy_from_slice(&dst[4..68]);
            }
            let mut bytes = [0; 72];
            local.read(0, &mut bytes)?;
            assert_eq!(&bytes[..], expected_local);
            remote.read(0, &mut bytes)?;
            assert_eq!(&bytes[..], expected_remote);
        }
        requester.close(TIMEOUT)?;
        source.close(TIMEOUT)?;
    }
    Ok(())
}

#[test]
fn r1b_refreshes_the_local_snapshot_after_bootstrap_before_planning_slices() -> Result<()> {
    let make = || single_handler(sliced_runtime(5, 8, 16, RdmaHostMemory::Direct, false));
    let requester = make();
    let source = make();
    let local = requester.register_memory(vec![0xcc; 16].into(), Permission::LocalOnly)?;
    let remote =
        source.register_memory((0u8..16).collect::<Vec<_>>().into(), Permission::RemoteRead)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let mut selection = SelectionResult::plain(Transport::Rdma);
    selection.device_mask = 5;
    let request = PreparedTransfer {
        shm: None,
        hp_cancel: None,
        priority: crate::Priority::High,
        rdma_cancel: None,
        #[cfg(feature = "cuda")]
        remote_staging: false,
        local: local.clone(),
        local_offset: 4,
        remote: peer.regions()[0],
        remote_offset: 4,
        length: 8,
        selection,
    };
    let runtime = source_runtime(&requester);
    // Both PDs and their registrations stay owned while the test publishes a
    // different set of available contexts between the two halves of path exchange.
    let newly_available = runtime.context_set.lock().unwrap().slots[2]
        .owner
        .take()
        .unwrap();
    let mut setup = RdmaSetup::new(
        runtime,
        TransferOp::Read,
        &peer,
        &request,
        requester.source_epoch(),
        1,
    )?;
    let mut channel = ControlChannel::connect(source.endpoint(), deadline(TIMEOUT)?)?;
    assert!(setup.progress(runtime, &mut channel, &mut false)?.is_none());
    let retired = {
        let mut contexts = runtime.context_set.lock().unwrap();
        contexts.slots[2].owner = Some(newly_available);
        contexts.slots[0].owner.take().unwrap()
    };
    let transfer = loop {
        if let Some(t) = setup.progress(runtime, &mut channel, &mut false)? {
            break t;
        }
    };
    assert!(transfer.slices.len() > 1);
    let new_domain = runtime.device_at(2)?.domain()?;
    assert!(
        transfer
            .slices
            .iter()
            .all(|s| s.peer.local.domain == new_domain)
    );
    transfer.finish(runtime, &mut channel)?;
    runtime.context_set.lock().unwrap().slots[0].owner = Some(retired);
    let mut actual = [0; 16];
    local.read(0, &mut actual)?;
    assert_eq!(
        actual,
        [
            0xcc, 0xcc, 0xcc, 0xcc, 4, 5, 6, 7, 8, 9, 10, 11, 0xcc, 0xcc, 0xcc, 0xcc
        ]
    );
    remote.read(0, &mut actual)?;
    assert_eq!(actual, std::array::from_fn(|i| i as u8));
    requester.close(TIMEOUT)?;
    source.close(TIMEOUT)?;
    Ok(())
}

#[test]
fn r1b_failed_selective_write_preserves_guards_root_error_and_remote_uncertainty() -> Result<()> {
    for (mode, prefix, code, rail_errors) in [
        ("copy_selective_error", 16, 7, 1),
        ("copy_selective_flush", 0, sys::FLUSHED, 0),
    ] {
        let make = |mode| {
            rdma_fixture_with_runtime(fixture::runtime_with_config(RdmaConfig {
                host_memory: RdmaHostMemory::Direct,
                block_size: 8,
                max_send_wr: 8,
                signal_interval: 3,
                ..RdmaConfig::new(mode)
            }))
        };
        let requester = make(mode);
        let source = make("copy");
        let local = requester
            .register_memory((0u8..72).collect::<Vec<_>>().into(), Permission::LocalOnly)?;
        let remote = source.register_memory(vec![0xcc; 72].into(), Permission::RemoteReadWrite)?;
        let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
        let report = requester.submit_and_wait(
            TransferOp::Write,
            &peer,
            &[TransferRequest::new(
                Transport::Rdma,
                local.base_address() + 4,
                remote.base_address() + 4,
                64,
            )],
            TIMEOUT,
        )?;
        assert_eq!(
            (
                report.completed_requests,
                report.transferred_bytes,
                report.locally_drained
            ),
            (0, 0, true)
        );
        let failure = report.failure.unwrap();
        assert!(
            matches!(failure.error, Error::Backend{operation:"RDMA work completion",code:c} if c == code as i32),
            "{:?}",
            failure.error
        );
        assert_eq!(failure.remote_effect, RemoteEffect::Unknown);
        let stats = requester.stats()?.rdma.unwrap();
        assert_eq!(stats.rail_errors, rail_errors);
        assert_eq!(
            (stats.completed_write_bytes, stats.work_requests),
            (prefix as u64, 0)
        );
        let mut actual = [0; 72];
        local.read(0, &mut actual)?;
        assert_eq!(actual, std::array::from_fn(|i| i as u8));
        // The source releases its loan after receiving EOF on the failed grant.
        source.close(TIMEOUT)?;
        remote.read(0, &mut actual)?;
        let mut expected = [0xcc; 72];
        for (i, b) in expected[4..4 + prefix].iter_mut().enumerate() {
            *b = (4 + i) as u8;
        }
        assert_eq!(actual, expected);
        requester.close(TIMEOUT)?;
    }
    Ok(())
}

#[test]
fn r1b_freeing_a_batch_with_unreaped_unsignaled_wrs_keeps_its_loan_and_charge() -> Result<()> {
    let runtime = fixture::runtime_with_config(RdmaConfig {
        host_memory: RdmaHostMemory::Direct,
        block_size: 8,
        max_send_wr: 8,
        signal_interval: 3,
        ..RdmaConfig::new("copy_selective_orphan")
    });
    let requester = Arc::new(crate::engine::tests::rdma_fixture_with_config(
        runtime,
        crate::EngineConfig {
            enable_progress_worker: true,
            max_failover_attempts: 0,
            handler_timeout: TIMEOUT,
            ..Default::default()
        },
    ));
    let source = rdma_fixture_with_runtime(fixture::runtime_with_config(RdmaConfig {
        host_memory: RdmaHostMemory::Direct,
        ..RdmaConfig::new("copy")
    }));
    let local = requester.register_memory(vec![0x25; 72].into(), Permission::LocalOnly)?;
    let remote = source.register_memory(vec![0xbc; 72].into(), Permission::RemoteReadWrite)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let pause = crate::transport::rdma::context::tests::pause(source_runtime(&requester));
    let mut batch = requester.submit_async(
        TransferOp::Write,
        &peer,
        &[TransferRequest::new(
            Transport::Rdma,
            local.base_address() + 4,
            remote.base_address() + 4,
            64,
        )],
        None,
        TIMEOUT,
    )?;
    let release = batch.allocation_release_receipt();
    let (guard, timed) = pause
        .1
        .wait_timeout_while(pause.0.lock().unwrap(), EVENTUAL_PROGRESS, |v| !v.0)
        .unwrap();
    drop(guard);
    assert!(!timed.timed_out());
    let freed = batch.free();
    let held = requester.stats();
    let borrowed = local.write(4, &[0]);
    let premature = release.try_recv().is_ok();
    pause.0.lock().unwrap().1 = true;
    pause.1.notify_all();
    freed?;
    let held = held?;
    let native = held.rdma.unwrap();
    assert_eq!(
        (
            held.deferred_batches,
            held.inflight_bytes,
            native.work_requests,
            native.direct_bytes,
            native.payload_send_cqes
        ),
        (1, 64, 8, 64, 0)
    );
    assert!(borrowed.is_err() && !premature);
    release.recv_timeout(EVENTUAL_PROGRESS).unwrap();
    let end = requester.stats()?;
    assert_eq!(
        (
            end.deferred_batches,
            end.inflight_bytes,
            end.rdma.unwrap().work_requests
        ),
        (0, 0, 0)
    );
    source.close(TIMEOUT)?;
    let mut actual = [0; 72];
    remote.read(0, &mut actual)?;
    assert_eq!(actual, [0xbc; 72]);
    local.read(0, &mut actual)?;
    assert_eq!(actual, [0x25; 72]);
    requester.close(TIMEOUT)?;
    Ok(())
}

#[test]
fn r1b_masked_wildcard_moves_bytes_on_the_lower_ranked_mapped_peer() -> Result<()> {
    for (mask, name, other) in [(1, "rail0", "rail2"), (4, "rail2", "rail0")] {
        let mut target = runtime(5, 8);
        target.config.priority_matrix = Some(format!(r#"{{"*":[["{other}"],["{name}"]]}}"#));
        let mut local_runtime = runtime(mask, 8);
        local_runtime.config.rail_topology = Some(format!(
            r#"{{"all":[{{"local":"{name}","remote":"{other}"}},{{"local":"{name}","remote":"{name}"}}],"direct":[{{"local":"{name}","remote":"{name}"}}]}}"#
        ));
        let source = rdma_fixture_with_runtime(target);
        let requester = rdma_fixture_with_runtime(local_runtime);
        let local = requester.register_memory(vec![0x63; 16].into(), Permission::LocalOnly)?;
        let remote = source.register_memory(vec![0x29; 16].into(), Permission::RemoteReadWrite)?;
        let paths = source_runtime(&source).paths(crate::MemoryLocation::Cpu(None))?;
        assert_eq!(
            paths.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            [other, name]
        );
        for (round, op) in [TransferOp::Read, TransferOp::Write]
            .into_iter()
            .enumerate()
        {
            local.write(0, &[0x63; 16])?;
            remote.write(0, &[0x29; 16])?;
            exchange(&requester, &source, &local, &remote, op);
            for engine in [&source, &requester] {
                let stats = engine.nic_load_stats()?;
                assert_eq!(
                    stats
                        .iter()
                        .find(|n| n.device_name == name)
                        .unwrap()
                        .allocated_bytes,
                    ((round + 1) * 8) as u64
                );
                assert_eq!(
                    stats
                        .iter()
                        .find(|n| n.device_name == other)
                        .unwrap()
                        .allocated_bytes,
                    0
                );
            }
        }
        requester.close(TIMEOUT)?;
        source.close(TIMEOUT)?;
    }
    Ok(())
}
