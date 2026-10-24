//! Concurrent public parents share actual context queues and aggregate budgets.
use super::*;

#[path = "concurrent_verbs.rs"]
mod native;

const PAYLOAD: usize = 16384;
const BANK: usize = 2 * PAYLOAD + 3 * GUARD;
const LENGTH: usize = 3 * BANK;
const DESTINATION: usize = PAYLOAD + 2 * GUARD;

fn bytes(source: bool, round: usize) -> Vec<u8> {
    let mut bytes = vec![0xa5; LENGTH];
    for parent in 0..3 {
        let begin = parent * BANK;
        for index in 0..PAYLOAD {
            bytes[begin + GUARD + index] = value(
                (if source { 0x1234 } else { 0x9876 }) + (round * 17 + parent) as u64,
                index,
            );
        }
        bytes[begin + DESTINATION..begin + DESTINATION + PAYLOAD].fill(0xcc);
    }
    bytes
}

fn owners(engine: &TransferEngine) -> Vec<(usize, Arc<Record>)> {
    let runtime = source_runtime(engine);
    let contexts = runtime.context_set.lock().unwrap();
    let mut result = Vec::new();
    for (id, slot) in contexts.slots.iter().enumerate() {
        if let Some(progress) = &slot.progress {
            let mut records = Vec::new();
            progress
                .shared
                .control
                .lock()
                .unwrap()
                .endpoints
                .snapshot(&mut records);
            result.extend(records.into_iter().map(|record| (id, record)));
        }
    }
    result
}

fn wake(engine: &TransferEngine) {
    let runtime = source_runtime(engine);
    let contexts = runtime.context_set.lock().unwrap();
    for slot in &contexts.slots {
        if let Some(progress) = &slot.progress {
            progress.shared.signal();
        }
    }
}

fn until(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            Instant::now() < end,
            "concurrent parent admission did not progress"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn transfer(
    engine: &TransferEngine,
    peer: &crate::RemoteSegment,
    base: u64,
    remote: u64,
    operation: TransferOp,
    parent: usize,
) -> crate::BatchReport {
    engine
        .submit_and_wait(
            operation,
            peer,
            &[TransferRequest {
                transport: Transport::Rdma,
                local_address: base
                    + (parent * BANK
                        + if operation == TransferOp::Read {
                            DESTINATION
                        } else {
                            GUARD
                        }) as u64,
                remote_address: remote
                    + (parent * BANK
                        + if operation == TransferOp::Read {
                            GUARD
                        } else {
                            DESTINATION
                        }) as u64,
                length: PAYLOAD,
            }],
            TIMEOUT,
        )
        .unwrap()
}

fn verify_bytes(
    memory: &crate::RegisteredMemory,
    original: &[u8],
    incoming: &[u8],
    affected: bool,
    success: &[bool; 2],
    accepted: &[[usize; 2]; 2],
) {
    let mut actual = vec![0; LENGTH];
    memory.read(0, &mut actual).unwrap();
    for index in 0..LENGTH {
        let parent = index / BANK;
        let offset = index % BANK;
        if affected && parent < 2 && (DESTINATION..DESTINATION + PAYLOAD).contains(&offset) {
            let relative = offset - DESTINATION;
            let incoming = incoming[parent * BANK + GUARD + relative];
            if success[parent] {
                assert_eq!(actual[index], incoming, "parent {parent} byte {relative}");
            } else if relative % (PAYLOAD / 2) < accepted[parent][relative / (PAYLOAD / 2)] {
                assert!(actual[index] == original[index] || actual[index] == incoming);
            } else {
                assert_eq!(actual[index], original[index], "unposted bytes changed");
            }
        } else {
            assert_eq!(
                actual[index], original[index],
                "guard/source/rejected parent changed at {index}"
            );
        }
    }
}

pub(super) fn run(mode: &str, source: bool, rails: &[String]) {
    assert_eq!(env::var("ORBITTE_RAIL_MASK").unwrap(), "3");
    let fault = matches!(mode, "concurrent-cq-error");
    let paused = mode == "concurrent-close";
    let endpoint_limit = mode == "concurrent-endpoints";
    let fault_rail: usize = env::var("ORBITTE_RAIL_FAULT").unwrap().parse().unwrap();
    let events = env::var("ORBITTE_RDMA_EVENTS").as_deref() == Ok("1");
    let operation = if env::var("ORBITTE_RAIL_OPERATION").as_deref() == Ok("read") {
        TransferOp::Read
    } else {
        TransferOp::Write
    };
    native::initialize(mode, !source, fault_rail);
    let config = super::super::super::super::RdmaConfig {
        rdma_whitelist: rails.to_vec(),
        port: Some(1),
        gid_index: Some(env::var("ORBITTE_SHARED_GID").unwrap().parse().unwrap()),
        host_memory: RdmaHostMemory::Direct,
        device_mask: 3,
        progress_mode: if events {
            RdmaProgressMode::EventDriven
        } else {
            RdmaProgressMode::Polling
        },
        max_endpoints: if !source && endpoint_limit { 4 } else { 8 },
        max_direct_bytes: if endpoint_limit {
            3 * PAYLOAD
        } else {
            2 * PAYLOAD
        },
        max_cqes: if source { 4 } else { 2 },
        max_send_wr: 1,
        max_cached_mrs: 8,
        max_cached_mr_bytes: 8 * LENGTH,
        block_size: 2048,
        hop_limit: 64,
        ..Default::default()
    };
    let Ok(mut runtime) = Arc::try_unwrap(RdmaTransport::new(config).unwrap()) else {
        panic!("one runtime owner")
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
            handler_threads: 3,
            handler_timeout: TIMEOUT,
            max_registered_regions: 8,
            max_registered_bytes: 1 << 20,
            ..Default::default()
        },
    ));
    let backing = Arc::new(HostMemory::new(vec![0xa5; LENGTH].into_boxed_slice()).unwrap());
    let weak = Arc::downgrade(&backing);
    let memory = engine
        .register_host_memory(Arc::clone(&backing), 0, LENGTH, Permission::RemoteReadWrite)
        .unwrap();
    topology(&engine, rails);
    let mut stream = control(source);
    let (endpoint, epoch, remote) =
        super::super::connected::exchange(&mut stream, &engine, &memory);
    let peer = if source {
        None
    } else {
        Some(engine.open_segment(endpoint, epoch, TIMEOUT).unwrap())
    };
    native::retain_memory(&memory);
    let rounds = if fault || paused { 1 } else { 2 };
    let mut first = None;
    let mut retired = false;
    for round in 0..rounds {
        let original = bytes(source, round);
        let incoming = bytes(!source, round);
        memory.write(0, &original).unwrap();
        super::super::connected::barrier(&mut stream, 0x10 + round as u8);
        let mut accepted = [[0usize; 2]; 2];
        let mut successful = [true; 2];
        if !source {
            native::hold();
            let spawn = |parent| {
                let engine = Arc::clone(&engine);
                let peer = peer.clone().unwrap();
                let base = memory.base_address();
                thread::spawn(move || transfer(&engine, &peer, base, remote, operation, parent))
            };
            let a = spawn(0);
            until(|| {
                native::initial_posted() && engine.stats().unwrap().rdma.unwrap().work_requests == 2
            });
            let b = spawn(1);
            until(|| {
                let records = owners(&engine);
                records.len() == 4
                    && records.iter().all(|(_, record)| {
                        let request = record.lock().unwrap();
                        request.work.as_ref().is_some_and(|work| {
                            matches!(
                                work.operation,
                                slice::Operation::Read | slice::Operation::Write
                            )
                        })
                    })
            });
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                stats.work_requests, 2,
                "aggregate SQ quota was exceeded across contexts"
            );
            assert_eq!(
                (
                    stats.active_endpoints,
                    stats.active_grants,
                    stats.direct_bytes
                ),
                (4, 4, 2 * PAYLOAD)
            );
            assert_eq!(stats.peak_work_requests, 2);
            native::admitted(owners(&engine));
            let rejected = transfer(
                &engine,
                peer.as_ref().unwrap(),
                memory.base_address(),
                remote,
                operation,
                2,
            );
            assert!(rejected.locally_drained && !rejected.is_complete());
            assert!(matches!(
                rejected.failure.as_ref().unwrap().error,
                Error::ResourceExhausted
            ));
            assert_eq!(
                rejected.failure.as_ref().unwrap().remote_effect,
                RemoteEffect::None
            );
            let after = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                after.opened_connections, stats.opened_connections,
                "rejected parent opened a native connection"
            );
            assert_eq!(
                (
                    after.active_endpoints,
                    after.active_grants,
                    after.direct_bytes
                ),
                (4, 4, 2 * PAYLOAD)
            );
            println!(
                "ORBITTE_CONCURRENT_ADMISSION {{\"round\":{round},\"sq_debt\":{},\"grants\":{},\"direct_bytes\":{},\"endpoints\":{},\"rejected_before_new_qp\":true}}",
                after.work_requests,
                after.active_grants,
                after.direct_bytes,
                after.active_endpoints
            );
            if paused {
                engine.unregister_memory(&memory).unwrap();
                retired = true;
                assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 2);
                for parent in 0..2 {
                    for slice in 0..2 {
                        assert!(matches!(
                            memory.write(
                                parent * BANK
                                    + if operation == TransferOp::Read {
                                        DESTINATION
                                    } else {
                                        GUARD
                                    }
                                    + slice * PAYLOAD / 2,
                                &[0xff]
                            ),
                            Err(Error::Unavailable(_))
                        ));
                    }
                }
                memory.write(0, &[0xa5]).unwrap();
                assert!(matches!(
                    engine.close_requester(Duration::from_millis(10)),
                    Err(Error::Timeout)
                ));
            }
            stream.write_all(&[0x40]).unwrap();
            token(&mut stream, 0x41);
            native::release();
            wake(&engine);
            let end = Instant::now() + Duration::from_secs(8);
            while (!a.is_finished() || !b.is_finished()) && Instant::now() < end {
                thread::sleep(Duration::from_millis(1));
            }
            let rescued = !a.is_finished() || !b.is_finished();
            if rescued {
                wake(&engine);
            }
            let reports = [a.join().unwrap(), b.join().unwrap()];
            for (parent, report) in reports.iter().enumerate() {
                assert!(report.locally_drained, "{report:?}");
                successful[parent] = report.is_complete();
                if report.is_complete() {
                    assert_eq!(
                        (report.completed_requests, report.transferred_bytes),
                        (1, PAYLOAD)
                    );
                } else {
                    assert!(fault, "{report:?}");
                    assert_eq!(
                        (report.completed_requests, report.transferred_bytes),
                        (0, 0)
                    );
                    let failure = report.failure.as_ref().unwrap();
                    assert!(
                        !matches!(failure.error, Error::Timeout | Error::Unquiesced { .. }),
                        "timeout is not fault/drain qualification: {report:?}"
                    );
                    assert_eq!(
                        failure.remote_effect,
                        if operation == TransferOp::Write {
                            RemoteEffect::Unknown
                        } else {
                            RemoteEffect::None
                        }
                    );
                }
            }
            if fault {
                assert!(
                    !successful[native::injected_parent()],
                    "the injected parent was reported complete"
                );
            }
            assert!(
                !rescued,
                "cross-context quota release did not resume every parent"
            );
            accepted = native::accepted();
            stream.write_all(&[0x31]).unwrap();
            stream.write_all(&successful.map(u8::from)).unwrap();
            for row in &accepted {
                for value in row {
                    stream.write_all(&(*value as u32).to_be_bytes()).unwrap();
                }
            }
        } else {
            token(&mut stream, 0x40);
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    stats.active_grants,
                    stats.direct_bytes,
                    stats.receive_work_requests
                ),
                (4, 2 * PAYLOAD, 4)
            );
            assert!(stats.peak_receive_work_requests <= 4 && stats.peak_work_requests <= 4);
            println!(
                "ORBITTE_CONCURRENT_RECEIVE {{\"round\":{round},\"rq_debt\":{},\"grants\":{},\"direct_bytes\":{}}}",
                stats.receive_work_requests, stats.active_grants, stats.direct_bytes
            );
            if paused {
                engine.unregister_memory(&memory).unwrap();
                retired = true;
                assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 2);
                for parent in 0..2 {
                    for slice in 0..2 {
                        assert!(matches!(
                            memory.write(
                                parent * BANK
                                    + if operation == TransferOp::Read {
                                        GUARD
                                    } else {
                                        DESTINATION
                                    }
                                    + slice * PAYLOAD / 2,
                                &[0xff]
                            ),
                            Err(Error::Unavailable(_))
                        ));
                    }
                }
                memory.write(0, &[0xa5]).unwrap();
                assert!(matches!(
                    engine.close_source(Duration::from_millis(10)),
                    Err(Error::Timeout)
                ));
            }
            stream.write_all(&[0x41]).unwrap();
            token(&mut stream, 0x31);
            let mut statuses = [0; 2];
            stream.read_exact(&mut statuses).unwrap();
            assert!(statuses.iter().all(|value| *value <= 1));
            successful = statuses.map(|value| value == 1);
            for row in &mut accepted {
                for value in row {
                    let mut bytes = [0; 4];
                    stream.read_exact(&mut bytes).unwrap();
                    *value = u32::from_be_bytes(bytes) as usize;
                }
            }
            if fault || paused {
                engine.close_source(TIMEOUT).unwrap();
            }
        }
        super::super::connected::barrier(&mut stream, 0x32);
        verify_bytes(
            &memory,
            &original,
            &incoming,
            source == (operation == TransferOp::Write),
            &successful,
            &accepted,
        );
        if !fault && !paused {
            let snapshot = connections(&engine);
            assert_eq!(snapshot.len(), 4);
            if let Some(first) = &first {
                assert_eq!(
                    &snapshot, first,
                    "four actual QPs must survive concurrent grant reuse"
                );
            } else {
                first = Some(snapshot);
            }
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (stats.completed_reusable_grants, stats.reused_connections),
                (4 * (round as u64 + 1), 4 * round as u64)
            );
        }
        println!(
            "ORBITTE_CONCURRENT_PARENTS {{\"round\":{round},\"successful\":{:?},\"accepted\":{:?}}}",
            successful, accepted
        );
        super::super::connected::barrier(&mut stream, 0x33);
    }
    if !retired {
        engine.unregister_memory(&memory).unwrap();
    }
    engine.close(TIMEOUT).unwrap();
    let stats = engine.stats().unwrap().rdma.unwrap();
    clean(&stats);
    assert!(stats.peak_work_requests <= if source { 4 } else { 2 });
    assert!(stats.peak_receive_work_requests <= if source { 4 } else { 2 });
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    native::finish();
    drop(memory);
    drop(backing);
    assert!(weak.upgrade().is_none());
    println!(
        "ORBITTE_CONCURRENT_RESULT {{\"mode\":\"{mode}\",\"source\":{source},\"fault_rail\":{fault_rail},\"parents\":2,\"slices_per_parent\":2,\"payload\":{PAYLOAD},\"rounds\":{rounds},\"peak_sq\":{},\"peak_rq\":{},\"all_resources_drained\":true,\"allocation_released\":true,\"performance_measurement\":false}}",
        stats.peak_work_requests, stats.peak_receive_work_requests
    );
}
