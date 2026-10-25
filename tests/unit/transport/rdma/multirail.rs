//! Two actual RNICs, public parent/slice protocol and native fault/drain receipts.
use super::*;
use crate::engine::tests::{rdma_fixture_with_config, source_runtime};
use crate::transport::rdma::{RdmaProgressMode, RdmaTransport};
use crate::{EngineConfig, RemoteEffect, TransferEngine, TransferOp, TransferRequest, Transport};

#[path = "multirail_verbs.rs"]
mod native;

#[path = "concurrent.rs"]
mod concurrent;

#[path = "lifecycle.rs"]
mod lifecycle;

const PAYLOAD: usize = 12288;
const LENGTH: usize = 2 * PAYLOAD + 3 * GUARD;
const DESTINATION: usize = PAYLOAD + 2 * GUARD;

fn control(source: bool) -> TcpStream {
    let stream = if source {
        let listener = TcpListener::bind(env::var("ORBITTE_SHARED_BIND").unwrap()).unwrap();
        listener.set_nonblocking(true).unwrap();
        println!("ORBITTE_READY {}", listener.local_addr().unwrap());
        std::io::stdout().flush().unwrap();
        let end = Instant::now() + TIMEOUT;
        loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < end);
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("{error}"),
            }
        }
    } else {
        TcpStream::connect_timeout(
            &env::var("ORBITTE_SHARED_PEER").unwrap().parse().unwrap(),
            Duration::from_secs(5),
        )
        .unwrap()
    };
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream
}

fn bytes(seed: u64) -> Vec<u8> {
    let mut bytes = vec![0xa5; LENGTH];
    for index in 0..PAYLOAD {
        bytes[GUARD + index] = value(seed, index);
    }
    bytes[DESTINATION..DESTINATION + PAYLOAD].fill(0xcc);
    bytes
}

fn topology(engine: &TransferEngine, rails: &[String]) {
    let topology = engine.topology().unwrap().unwrap();
    assert_eq!(
        topology.nic_list.len(),
        2,
        "only the two locked RNICs may open"
    );
    for (id, nic) in topology.nic_list.iter().enumerate() {
        assert_eq!(nic.name, rails[id]);
        assert_eq!(nic.device_attrs["rdma.port"], "1");
        assert_eq!(
            nic.device_attrs["rdma.gid_index"],
            env::var("ORBITTE_SHARED_GID").unwrap()
        );
        assert_eq!(nic.device_attrs["rdma.gid_type"], "RoCE v2");
        println!(
            "ORBITTE_MULTI_RAIL_PATH {{\"nic_id\":{id},\"device\":\"{}\",\"pci\":\"{}\",\"gid\":\"{}\"}}",
            nic.name,
            nic.pci_bus_id.as_deref().unwrap(),
            nic.device_attrs["rdma.gid"]
        );
    }
    assert_ne!(
        topology.nic_list[0].pci_bus_id,
        topology.nic_list[1].pci_bus_id
    );
}

fn clean(stats: &RdmaStats) {
    assert!(
        [
            stats.active_devices,
            stats.retained_devices,
            stats.active_cqs,
            stats.retained_cqs,
            stats.progress_workers,
            stats.async_event_sources,
            stats.active_endpoints,
            stats.retained_endpoints,
            stats.active_grants,
            stats.retained_grants,
            stats.persistent_mrs,
            stats.retained_persistent_mrs,
            stats.active_windows,
            stats.retained_windows,
            stats.work_requests,
            stats.retained_work_requests,
            stats.receive_credits,
            stats.receive_work_requests,
            stats.retained_receive_work_requests,
            stats.direct_bytes,
            stats.staging_bytes,
            stats.active_message_mrs,
            stats.message_bytes,
            stats.retained_message_bytes,
            stats.active_completion_channels,
            stats.retained_completion_channels,
            stats.completion_event_sources
        ]
        .iter()
        .all(|value| *value == 0),
        "native resources did not drain: {stats:?}"
    );
}

fn failed_bytes(
    memory: &crate::RegisteredMemory,
    before: &[u8],
    incoming: &[u8],
    affected: bool,
    partial: bool,
    fault_rail: usize,
) {
    let mut actual = vec![0; LENGTH];
    memory.read(0, &mut actual).unwrap();
    for index in 0..LENGTH {
        let relative = index.checked_sub(DESTINATION);
        let may_change = affected
            && relative.is_some_and(|offset| {
                let bad_start = fault_rail * (PAYLOAD / 2);
                let good_start = (1 - fault_rail) * (PAYLOAD / 2);
                (good_start..good_start + PAYLOAD / 2).contains(&offset)
                    || (partial && (bad_start..bad_start + 2048).contains(&offset))
            });
        if affected && fault_rail == 1 && relative.is_some_and(|offset| offset < PAYLOAD / 2) {
            assert_eq!(
                actual[index],
                incoming[GUARD + relative.unwrap()],
                "earlier acknowledged slice lost its committed bytes"
            );
        } else if may_change {
            assert!(
                actual[index] == before[index]
                    || actual[index] == incoming[GUARD + relative.unwrap()],
                "unexpected failed-transfer byte at {index}"
            );
        } else {
            assert_eq!(
                actual[index], before[index],
                "unposted suffix or guard changed at {index}"
            );
        }
    }
}

#[test]
#[ignore = "requires two identity-pinned RNIC hosts and explicit locks for both actual devices"]
fn multirail_peer() {
    let limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_CORE, &limit) }, 0);
    println!();
    let source = env::var("ORBITTE_SHARED_ROLE").unwrap() == "source";
    let rails = env::var("ORBITTE_SHARED_RAILS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(rails.len(), 2);
    assert_ne!(rails[0], rails[1]);
    let mode = env::var("ORBITTE_RAIL_MODE").unwrap();
    if matches!(
        mode.as_str(),
        "live-reconnect" | "mr-cleanup-error" | "mw-cleanup-error"
    ) {
        lifecycle::run(&mode, source, &rails);
        return;
    }
    if matches!(
        mode.as_str(),
        "concurrent-quota"
            | "concurrent-endpoints"
            | "concurrent-close"
            | "concurrent-cq-error"
            | "rq-pressure"
            | "rq-close"
            | "rq-cq-error"
            | "large-quota"
            | "large-close"
            | "large-cq-error"
            | "folded-quota"
            | "folded-close"
            | "folded-cq-error"
            | "quarter-quota"
            | "large-qp-error"
            | "folded-qp-error"
            | "large-lost-send-cqe"
            | "folded-lost-send-cqe"
            | "large-lost-recv-cqe"
            | "folded-lost-recv-cqe"
            | "large-peer-exit"
            | "large-peer-restart"
            | "large-requester-exit"
            | "large-requester-restart"
            | "large-cq-busy"
            | "large-pd-busy"
    ) {
        concurrent::run(&mode, source, &rails);
        return;
    }
    assert!(matches!(
        mode.as_str(),
        "healthy" | "cq-error" | "partial-post" | "paused-close" | "mr-rollback"
    ));
    let mask: u64 = env::var("ORBITTE_RAIL_MASK").unwrap().parse().unwrap();
    let events = env::var("ORBITTE_RDMA_EVENTS").as_deref() == Ok("1");
    let fault_rail: usize = env::var("ORBITTE_RAIL_FAULT").unwrap().parse().unwrap();
    assert!(fault_rail < 2);
    worker_pool::reset();
    native::initialize(&mode, &rails[fault_rail], !source);
    let config = super::super::super::RdmaConfig {
        rdma_whitelist: rails.clone(),
        num_lanes: worker_pool::count(),
        port: Some(1),
        gid_index: Some(env::var("ORBITTE_SHARED_GID").unwrap().parse().unwrap()),
        device_mask: mask,
        host_memory: RdmaHostMemory::Direct,
        progress_mode: if events {
            RdmaProgressMode::EventDriven
        } else {
            RdmaProgressMode::Polling
        },
        max_endpoints: 8,
        max_cqes: if source { 2 } else { 4 },
        max_send_wr: 2,
        max_cached_mrs: if mode == "mr-rollback" { 5 } else { 8 },
        max_cached_mr_bytes: 8 * LENGTH,
        block_size: 2048,
        hop_limit: 64,
        ..Default::default()
    };
    let Ok(mut runtime) = Arc::try_unwrap(RdmaTransport::new(config).unwrap()) else {
        panic!("new runtime must have one owner")
    };
    runtime.verbs = &native::VERBS;
    runtime.registrations = buffers::LocalBufferManager::new(
        &runtime.config,
        Arc::clone(&runtime.usage),
        &native::VERBS,
    );
    let engine = Arc::new(rdma_fixture_with_config(
        runtime,
        EngineConfig {
            bind_addr: env::var("ORBITTE_SHARED_BIND").unwrap().parse().unwrap(),
            handler_threads: 1,
            handler_timeout: TIMEOUT,
            max_registered_regions: 8,
            max_registered_bytes: 1 << 20,
            ..Default::default()
        },
    ));
    let backing = Arc::new(HostMemory::new(vec![0xa5; LENGTH].into_boxed_slice()).unwrap());
    let weak = Arc::downgrade(&backing);
    let memory = engine
        .register_host_memory(
            Arc::clone(&backing),
            0,
            LENGTH,
            if mode == "mr-rollback" {
                Permission::LocalOnly
            } else {
                Permission::RemoteReadWrite
            },
        )
        .unwrap();
    topology(&engine, &rails);
    let mut stream = control(source);
    let (endpoint, epoch, remote) = super::connected::exchange(&mut stream, &engine, &memory);
    let mut retired = false;
    if mode == "mr-rollback" {
        let fresh = Arc::new(HostMemory::new(vec![0x29; LENGTH].into_boxed_slice()).unwrap());
        assert!(matches!(
            engine.register_host_memory_batch(
                &[
                    (Arc::clone(&backing), 0, LENGTH),
                    (Arc::clone(&fresh), 0, LENGTH)
                ],
                Permission::RemoteReadWrite
            ),
            Err(Error::ResourceExhausted)
        ));
        assert!(matches!(
            engine.resolve_local_address(fresh.base_address(), LENGTH),
            Err(Error::RegionNotFound)
        ));
        assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 2);
        engine.unregister_memory(&memory).unwrap();
        assert!(
            matches!(
                engine.resolve_local_address(memory.base_address(), LENGTH),
                Err(Error::RegionNotFound)
            ),
            "failed batch changed duplicate references"
        );
        retired = true;
        super::connected::barrier(&mut stream, 1);
    } else {
        let peer = if source {
            None
        } else {
            Some(engine.open_segment(endpoint, epoch, TIMEOUT).unwrap())
        };
        native::arm(&engine, &memory);
        let operation = if env::var("ORBITTE_RAIL_OPERATION").as_deref() == Ok("read") {
            TransferOp::Read
        } else {
            TransferOp::Write
        };
        let rounds = if mode == "healthy" { 2 } else { 1 };
        let mut first = None;
        for round in 0..rounds {
            let own = bytes(if source {
                0x1234 + round
            } else {
                0x9876 + round
            });
            let incoming = bytes(if source {
                0x9876 + round
            } else {
                0x1234 + round
            });
            memory.write(0, &own).unwrap();
            super::connected::barrier(&mut stream, 0x10 + round as u8);
            let offset = if operation == TransferOp::Read {
                DESTINATION
            } else {
                GUARD
            };
            if !source {
                let run = {
                    let engine = Arc::clone(&engine);
                    let peer = peer.clone().unwrap();
                    let local = memory.base_address() + offset as u64;
                    thread::spawn(move || {
                        engine
                            .submit_and_wait(
                                operation,
                                &peer,
                                &[TransferRequest {
                                    transport: Transport::Rdma,
                                    local_address: local,
                                    remote_address: remote
                                        + (if operation == TransferOp::Read {
                                            GUARD
                                        } else {
                                            DESTINATION
                                        }) as u64,
                                    length: PAYLOAD,
                                }],
                                TIMEOUT,
                            )
                            .unwrap()
                    })
                };
                if mode == "paused-close" {
                    native::wait_paused();
                    let stats = engine.stats().unwrap().rdma.unwrap();
                    if fault_rail == 0 {
                        assert_eq!((stats.active_grants, stats.direct_bytes), (2, PAYLOAD));
                    } else {
                        assert!((1..=2).contains(&stats.active_grants));
                        assert!((PAYLOAD / 2..=PAYLOAD).contains(&stats.direct_bytes));
                    }
                    assert!(stats.work_requests > 0);
                    engine.unregister_memory(&memory).unwrap();
                    retired = true;
                    assert!(
                        (1..=2).contains(&engine.stats().unwrap().rdma.unwrap().persistent_mrs)
                    );
                    assert!(matches!(
                        memory.write(offset + fault_rail * (PAYLOAD / 2), &[0xff]),
                        Err(Error::Unavailable(_))
                    ));
                    memory.write(0, &[0xa5]).unwrap();
                    assert!(matches!(
                        engine.close_requester(Duration::from_millis(10)),
                        Err(Error::Timeout)
                    ));
                    stream.write_all(&[0x40]).unwrap();
                    token(&mut stream, 0x41);
                    native::release();
                }
                let report = run.join().unwrap();
                assert!(report.locally_drained, "{report:?}");
                if matches!(mode.as_str(), "cq-error" | "partial-post") {
                    assert!(!report.is_complete());
                    assert_eq!(
                        (report.completed_requests, report.transferred_bytes),
                        (0, 0)
                    );
                    assert_eq!(
                        report.failure.as_ref().unwrap().remote_effect,
                        if operation == TransferOp::Write {
                            RemoteEffect::Unknown
                        } else {
                            RemoteEffect::None
                        }
                    );
                    let stats = engine.stats().unwrap().rdma.unwrap();
                    assert_eq!(stats.partial_posts, u64::from(mode == "partial-post"));
                    assert_eq!(
                        stats.completed_sends, fault_rail as u64,
                        "only earlier completed slices may have sent receipts"
                    );
                } else {
                    assert!(report.is_complete(), "{report:?}");
                    assert_eq!(
                        (report.completed_requests, report.transferred_bytes),
                        (1, PAYLOAD)
                    );
                }
                stream.write_all(&[0x31]).unwrap();
            } else {
                if mode == "paused-close" {
                    token(&mut stream, 0x40);
                    let source_offset = if operation == TransferOp::Read {
                        GUARD
                    } else {
                        DESTINATION
                    };
                    engine.unregister_memory(&memory).unwrap();
                    retired = true;
                    assert!(
                        (1..=2).contains(&engine.stats().unwrap().rdma.unwrap().persistent_mrs)
                    );
                    assert!(matches!(
                        memory.write(source_offset + fault_rail * (PAYLOAD / 2), &[0xff]),
                        Err(Error::Unavailable(_))
                    ));
                    memory.write(0, &[0xa5]).unwrap();
                    assert!(matches!(
                        engine.close_source(Duration::from_millis(10)),
                        Err(Error::Timeout)
                    ));
                    stream.write_all(&[0x41]).unwrap();
                }
                token(&mut stream, 0x31);
                if mode != "healthy" {
                    engine.close_source(TIMEOUT).unwrap();
                }
            }
            super::connected::barrier(&mut stream, 0x32);
            let affected = source == (operation == TransferOp::Write);
            if matches!(mode.as_str(), "cq-error" | "partial-post") {
                failed_bytes(
                    &memory,
                    &own,
                    &incoming,
                    affected,
                    mode == "partial-post",
                    fault_rail,
                );
            } else {
                let mut expected = own;
                if affected {
                    expected[DESTINATION..DESTINATION + PAYLOAD]
                        .copy_from_slice(&incoming[GUARD..GUARD + PAYLOAD]);
                }
                verify(&memory, &expected);
            }
            if mode == "healthy" {
                let stats = engine.stats().unwrap().rdma.unwrap();
                let slices = if mask == 3 || worker_pool::count() > 1 {
                    2
                } else {
                    1
                };
                assert_eq!(stats.completed_reusable_grants, slices * (round + 1));
                assert_eq!(stats.reused_connections, slices * round);
                let snapshot = connections(&engine);
                if let Some(first) = &first {
                    assert_eq!(&snapshot, first, "physical QPs must survive new grants");
                } else {
                    first = Some(snapshot);
                }
            }
            super::connected::barrier(&mut stream, 0x33);
        }
    }
    worker_pool::running(&engine);
    if !retired {
        engine.unregister_memory(&memory).unwrap();
    }
    engine.close(TIMEOUT).unwrap();
    let stats = engine.stats().unwrap().rdma.unwrap();
    clean(&stats);
    worker_pool::closed(&stats);
    assert_eq!(stats.opened_devices, 2);
    if events && mode != "mr-rollback" {
        assert!(stats.completion_events > 0);
    }
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    native::finish(&rails, mask);
    drop(memory);
    drop(backing);
    assert!(weak.upgrade().is_none());
    println!(
        "ORBITTE_MULTI_RAIL_RESULT {{\"mode\":\"{mode}\",\"source\":{source},\"mask\":{mask},\"fault_rail\":{fault_rail},\"payload\":{PAYLOAD},\"actual_rnics\":2,\"all_resources_drained\":true,\"allocation_released\":true,\"performance_measurement\":false}}"
    );
}

type Observation = (usize, u64, [u8; 24], u32, u32, u32, u32, usize);

fn connections(engine: &TransferEngine) -> Vec<Observation> {
    let runtime = source_runtime(engine);
    let contexts = runtime.context_set.lock().unwrap();
    let mut result = Vec::new();
    for (id, slot) in contexts.slots.iter().enumerate() {
        for progress in slot.cq_list.iter().flatten() {
            let mut records = Vec::new();
            progress
                .shared
                .control
                .lock()
                .unwrap()
                .qps
                .snapshot(&mut records);
            for record in records {
                let request = record.lock().unwrap();
                let resources = request.resources.as_ref().unwrap();
                assert!(resources.grant.is_none());
                let qp = unsafe {
                    &*resources
                        .connection
                        .lock()
                        .unwrap()
                        .handles
                        .qp
                        .cast::<ibverbs_sys::ibv_qp>()
                };
                assert_eq!(qp.state, ibverbs_sys::ibv_qp_state::IBV_QPS_RTS);
                let connection = resources.connection.lock().unwrap();
                assert_eq!(connection.send_queue.outstanding(), 0);
                assert_eq!(connection.receive_queue.outstanding(), 0);
                assert_eq!(connection.receive_queue.memory_usage(), (0, 0));
                assert!(!connection.send_queue.drained());
                let remote = connection.remote.unwrap();
                result.push((
                    id,
                    record.tag,
                    connection.incarnation.unwrap().encode(),
                    qp.qp_num,
                    connection.local.psn,
                    remote.qpn,
                    remote.psn,
                    connection.handles.qp as usize,
                ));
            }
        }
    }
    result.sort_unstable();
    result
}
