//! Concurrent public parents share actual context queues and aggregate budgets.
use super::*;

#[path = "concurrent_verbs.rs"]
mod native;

#[path = "restart.rs"]
mod restart;

#[derive(Clone, Copy, Default)]
struct Shape {
    parents: usize,
    slices: usize,
    payload: usize,
    block: usize,
    base: usize,
    bank: usize,
    length: usize,
    destination: usize,
}

impl Shape {
    fn new(mode: &str) -> Self {
        let (parents, slices, payload, block, base) = if mode.starts_with("large-") {
            (8, 5, 5 << 20, 1 << 20, 64 << 10)
        } else if mode.starts_with("folded-") {
            (8, 4, (1 << 20) + 1, 256 << 10, 64 << 10)
        } else if mode.starts_with("quarter-") {
            (8, 5, (1 << 20) + (64 << 10), 256 << 10, 64 << 10)
        } else if mode.starts_with("rq-") {
            (4, 3, 24576, 8192, 2048)
        } else {
            (2, 2, 16384, 8192, 2048)
        };
        let bank = 2 * payload + 3 * GUARD;
        Self {
            parents,
            slices,
            payload,
            block,
            base,
            bank,
            length: (parents + 1) * bank,
            destination: payload + 2 * GUARD,
        }
    }

    fn wr_lengths(self, slice: usize) -> Vec<usize> {
        let length = self.range(slice).len();
        let mut result = vec![self.base; length / self.base];
        if length % self.base != 0 {
            assert_eq!(
                length % self.base,
                1,
                "the declared folded fixture has one extra byte"
            );
            *result.last_mut().unwrap() += 1;
        }
        result
    }

    fn range(self, slice: usize) -> std::ops::Range<usize> {
        assert!(slice < self.slices);
        slice * self.block..if slice + 1 == self.slices {
            self.payload
        } else {
            (slice + 1) * self.block
        }
    }
}

fn bytes(shape: Shape, source: bool, round: usize) -> Vec<u8> {
    let mut bytes = vec![0xa5; shape.length];
    for parent in 0..=shape.parents {
        let begin = parent * shape.bank;
        for index in 0..shape.payload {
            bytes[begin + GUARD + index] = value(
                (if source { 0x1234 } else { 0x9876 }) + (round * 17 + parent) as u64,
                index,
            );
        }
        bytes[begin + shape.destination..begin + shape.destination + shape.payload].fill(0xcc);
    }
    bytes
}

fn owners(engine: &TransferEngine) -> Vec<(usize, Arc<Record>)> {
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
            result.extend(
                records
                    .into_iter()
                    .filter(|record| {
                        record
                            .lock()
                            .unwrap()
                            .resources
                            .as_ref()
                            .is_some_and(|resources| resources.grant.is_some())
                    })
                    .map(|record| (id, record)),
            );
        }
    }
    result
}

fn wake(engine: &TransferEngine) {
    let runtime = source_runtime(engine);
    let contexts = runtime.context_set.lock().unwrap();
    for slot in &contexts.slots {
        for progress in slot.cq_list.iter().flatten() {
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

fn is_fault(mode: &str) -> bool {
    mode.ends_with("cq-error")
        || mode.ends_with("qp-error")
        || mode.ends_with("lost-send-cqe")
        || mode.ends_with("lost-recv-cqe")
        || mode.ends_with("peer-exit")
        || mode.ends_with("requester-exit")
}

fn deadline_fault(mode: &str) -> bool {
    mode.ends_with("lost-send-cqe")
        || mode.ends_with("lost-recv-cqe")
        || mode.ends_with("peer-exit")
        || mode.ends_with("requester-exit")
        || matches!(
            mode,
            "large-peer-restart" | "large-requester-restart" | "large-cq-busy" | "large-pd-busy"
        )
}

fn transfer(
    shape: Shape,
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
                    + (parent * shape.bank
                        + if operation == TransferOp::Read {
                            shape.destination
                        } else {
                            GUARD
                        }) as u64,
                remote_address: remote
                    + (parent * shape.bank
                        + if operation == TransferOp::Read {
                            GUARD
                        } else {
                            shape.destination
                        }) as u64,
                length: shape.payload,
            }],
            if deadline_fault(&env::var("ORBITTE_RAIL_MODE").unwrap()) {
                Duration::from_secs(5)
            } else {
                TIMEOUT
            },
        )
        .unwrap()
}

fn verify_bytes(
    shape: Shape,
    memory: &crate::RegisteredMemory,
    original: &[u8],
    incoming: &[u8],
    affected: bool,
    success: &[bool],
    accepted: &[Vec<usize>],
) {
    let mut actual = vec![0; shape.length];
    memory.read(0, &mut actual).unwrap();
    for index in 0..shape.length {
        let parent = index / shape.bank;
        let offset = index % shape.bank;
        if affected
            && parent < shape.parents
            && (shape.destination..shape.destination + shape.payload).contains(&offset)
        {
            let relative = offset - shape.destination;
            let incoming = incoming[parent * shape.bank + GUARD + relative];
            if success[parent] {
                assert_eq!(actual[index], incoming, "parent {parent} byte {relative}");
            } else if relative - (relative / shape.block).min(shape.slices - 1) * shape.block
                < accepted[parent][(relative / shape.block).min(shape.slices - 1)]
            {
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

fn write_outcome(stream: &mut TcpStream, successful: &[bool], accepted: &[Vec<usize>]) {
    stream.write_all(&[0x31]).unwrap();
    stream
        .write_all(&successful.iter().copied().map(u8::from).collect::<Vec<_>>())
        .unwrap();
    for row in accepted {
        for value in row {
            stream.write_all(&(*value as u32).to_be_bytes()).unwrap();
        }
    }
}

fn read_outcome(stream: &mut TcpStream, successful: &mut [bool], accepted: &mut [Vec<usize>]) {
    token(stream, 0x31);
    let mut statuses = vec![0; successful.len()];
    stream.read_exact(&mut statuses).unwrap();
    for (output, status) in successful.iter_mut().zip(statuses) {
        assert!(status <= 1);
        *output = status == 1;
    }
    for row in accepted {
        for value in row {
            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes).unwrap();
            *value = u32::from_be_bytes(bytes) as usize;
        }
    }
}

pub(super) fn run(mode: &str, source: bool, rails: &[String]) {
    assert_eq!(env::var("ORBITTE_RAIL_MASK").unwrap(), "3");
    let generation = restart::generation(mode);
    let interrupted = restart::interrupted(mode, generation);
    let cleanup = restart::cleanup(mode, generation);
    let dying = restart::dying(source, generation);
    let mode = restart::mode(mode, generation);
    let shape = Shape::new(mode);
    let large = shape.parents == 8;
    let wide = large || mode.starts_with("rq-");
    let count = shape.parents * shape.slices;
    let sq_limit = if large {
        5
    } else if wide {
        3
    } else {
        2
    };
    let rq_limit = if wide { count + 1 } else { 4 };
    let fault = is_fault(mode);
    let paused = mode.ends_with("close");
    let endpoint_limit = mode == "concurrent-endpoints";
    let fault_rail: usize = env::var("ORBITTE_RAIL_FAULT").unwrap().parse().unwrap();
    let events = env::var("ORBITTE_RDMA_EVENTS").as_deref() == Ok("1");
    let operation = if env::var("ORBITTE_RAIL_OPERATION").as_deref() == Ok("read") {
        TransferOp::Read
    } else {
        TransferOp::Write
    };
    assert_eq!(
        slice::plan_rdma_slices(shape.payload, shape.base, sq_limit).unwrap(),
        (shape.block, shape.slices)
    );
    println!(
        "ORBITTE_SLICE_GEOMETRY {{\"parents\":{},\"payload\":{},\"base\":{},\"block\":{},\"slices\":{},\"lengths\":{:?},\"sq_limit\":{sq_limit},\"rq_limit\":{rq_limit},\"max_send_wr\":{}}}",
        shape.parents,
        shape.payload,
        shape.base,
        shape.block,
        shape.slices,
        (0..shape.slices)
            .map(|index| shape.range(index).len())
            .collect::<Vec<_>>(),
        if large { 2 } else { 1 }
    );
    worker_pool::reset();
    native::initialize(mode, !source, fault_rail, shape);
    let config = super::super::super::super::RdmaConfig {
        rdma_whitelist: rails.to_vec(),
        num_lanes: worker_pool::count(),
        port: Some(1),
        gid_index: Some(env::var("ORBITTE_SHARED_GID").unwrap().parse().unwrap()),
        host_memory: RdmaHostMemory::Direct,
        device_mask: 3,
        progress_mode: if events {
            RdmaProgressMode::EventDriven
        } else {
            RdmaProgressMode::Polling
        },
        max_endpoints: if wide {
            2 * count
        } else if !source && endpoint_limit {
            4
        } else {
            8
        },
        max_direct_bytes: if wide {
            (shape.parents + 1) * shape.payload
        } else if endpoint_limit {
            3 * shape.payload
        } else {
            2 * shape.payload
        },
        max_cqes: if source { rq_limit } else { sq_limit },
        max_send_wr: if large { 2 } else { 1 },
        max_cached_mrs: 8,
        max_cached_mr_bytes: 8 * shape.length,
        block_size: shape.base,
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
            bind_addr: restart::bind(generation),
            handler_threads: shape.parents + 1,
            handler_timeout: if mode.ends_with("lost-recv-cqe")
                || cleanup == Some("cq")
                || (interrupted
                    && env::var("ORBITTE_RESTART_KILL_ROLE").as_deref() == Ok("requester"))
            {
                Duration::from_secs(10)
            } else {
                TIMEOUT
            },
            max_registered_regions: 8,
            max_registered_bytes: (2 * shape.length).max(1 << 20),
            ..Default::default()
        },
    ));
    let backing = Arc::new(HostMemory::new(vec![0xa5; shape.length].into_boxed_slice()).unwrap());
    let weak = Arc::downgrade(&backing);
    let memory = engine
        .register_host_memory(
            Arc::clone(&backing),
            0,
            shape.length,
            Permission::RemoteReadWrite,
        )
        .unwrap();
    topology(&engine, rails);
    if let Some(generation) = generation {
        restart::state(&engine, &memory, generation);
    }
    let mut stream = restart::control(source, generation);
    let (endpoint, epoch, remote) =
        super::super::connected::exchange(&mut stream, &engine, &memory);
    let peer = if source {
        None
    } else {
        Some(engine.open_segment(endpoint, epoch, TIMEOUT).unwrap())
    };
    if let (Some(generation), Some(peer)) = (generation, &peer) {
        restart::remember(peer, generation);
    }
    native::retain_memory(&memory);
    let rounds = if fault || paused { 1 } else { 2 };
    let mut first: Option<Vec<Observation>> = None;
    let mut previous_opened = 0;
    let mut previous_reused = 0;
    let mut retired = false;
    for round in 0..rounds {
        let seed = round + generation.map_or(0, |g| 10 * g);
        let original = bytes(shape, source, seed);
        let incoming = bytes(shape, !source, seed);
        memory.write(0, &original).unwrap();
        super::super::connected::barrier(&mut stream, 0x10 + round as u8);
        if generation.is_some_and(|g| g > 0) && round == 0 {
            restart::stale(
                &mut stream,
                &engine,
                &memory,
                peer.as_ref(),
                shape,
                operation,
                &original,
            );
        }
        let mut accepted = vec![vec![0usize; shape.slices]; shape.parents];
        let mut successful = vec![true; shape.parents];
        if !source {
            native::hold();
            let spawn = |parent| {
                let engine = Arc::clone(&engine);
                let peer = peer.clone().unwrap();
                let base = memory.base_address();
                thread::spawn(move || {
                    transfer(shape, &engine, &peer, base, remote, operation, parent)
                })
            };
            let mut pending = vec![spawn(0)];
            until(|| {
                native::initial_posted()
                    && engine.stats().unwrap().rdma.unwrap().work_requests == sq_limit
            });
            pending.extend((1..shape.parents).map(spawn));
            until(|| {
                let records = owners(&engine);
                records.len() == count
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
                stats.work_requests, sq_limit,
                "aggregate SQ quota was exceeded across contexts"
            );
            assert_eq!(
                (
                    stats.active_endpoints,
                    stats.active_grants,
                    stats.direct_bytes
                ),
                (
                    count + stats.idle_endpoints,
                    count,
                    shape.parents * shape.payload
                )
            );
            assert!(stats.active_endpoints <= 2 * count);
            assert_eq!(stats.peak_work_requests, sq_limit);
            worker_pool::admitted(&engine, count);
            native::admitted(owners(&engine));
            stream.write_all(&[0x3e]).unwrap();
            token(&mut stream, 0x3f);
            let rejected = transfer(
                shape,
                &engine,
                peer.as_ref().unwrap(),
                memory.base_address(),
                remote,
                operation,
                shape.parents,
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
                (stats.active_endpoints, count, shape.parents * shape.payload)
            );
            assert_eq!(after.idle_endpoints, stats.idle_endpoints);
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
                for parent in 0..shape.parents {
                    for slice in 0..shape.slices {
                        assert!(matches!(
                            memory.write(
                                parent * shape.bank
                                    + if operation == TransferOp::Read {
                                        shape.destination
                                    } else {
                                        GUARD
                                    }
                                    + shape.range(slice).start,
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
            if interrupted {
                if dying {
                    successful.fill(false);
                    accepted = native::accepted();
                    write_outcome(&mut stream, &successful, &accepted);
                    token(&mut stream, 0x42);
                    println!(
                        "ORBITTE_RESTART_ACCEPTED {{\"generation\":{},\"accepted\":{:?}}}",
                        generation.unwrap(),
                        accepted
                    );
                    restart::terminate(
                        source,
                        generation.unwrap(),
                        "grants",
                        &engine.stats().unwrap().rdma.unwrap(),
                    );
                }
                restart::peer_exited(&mut stream);
            }
            native::release();
            wake(&engine);
            let end = Instant::now() + Duration::from_secs(8);
            while pending.iter().any(|task| !task.is_finished()) && Instant::now() < end {
                thread::sleep(Duration::from_millis(1));
            }
            let rescued = pending.iter().any(|task| !task.is_finished());
            if rescued {
                wake(&engine);
            }
            let reports = pending
                .into_iter()
                .map(|task| task.join().unwrap())
                .collect::<Vec<_>>();
            let retaining = cleanup == Some("cq") && dying;
            if retaining {
                assert!(
                    reports.iter().any(|report| !report.locally_drained),
                    "native cleanup failure falsely drained every parent"
                );
            }
            for (parent, report) in reports.iter().enumerate() {
                assert!(report.locally_drained || retaining, "{report:?}");
                successful[parent] = report.is_complete();
                if report.is_complete() {
                    assert_eq!(
                        (report.completed_requests, report.transferred_bytes),
                        (1, shape.payload)
                    );
                } else {
                    assert!(fault, "{report:?}");
                    assert_eq!(
                        (report.completed_requests, report.transferred_bytes),
                        (0, 0)
                    );
                    let failure = report.failure.as_ref().unwrap();
                    assert!(
                        deadline_fault(mode)
                            || !matches!(failure.error, Error::Unquiesced { .. } | Error::Timeout),
                        "an unproven cleanup is not fault/drain qualification: {report:?}"
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
            if deadline_fault(mode) && !retaining {
                let unquiesced = reports
                    .iter()
                    .filter(|report| !report.locally_drained)
                    .count();
                engine.unregister_memory(&memory).unwrap();
                retired = true;
                engine.close(TIMEOUT).unwrap();
                println!(
                    "ORBITTE_DEADLINE_DRAIN {{\"unquiesced_reports\":{unquiesced},\"fresh_close_deadline\":true,\"native_barrier_complete\":true}}"
                );
            }
            if interrupted {
                assert!(successful.iter().all(|value| !value));
            } else if fault && !mode.ends_with("lost-recv-cqe") {
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
            if !interrupted {
                write_outcome(&mut stream, &successful, &accepted);
                if cleanup == Some("cq") {
                    token(&mut stream, 0x42);
                    if dying {
                        let stats = native::retained(engine, memory, backing, weak, "cq");
                        restart::terminate(source, generation.unwrap(), "retained-cq", &stats);
                    }
                    restart::peer_exited(&mut stream);
                }
            }
        } else {
            token(&mut stream, 0x3e);
            let connections_before_rejection =
                engine.stats().unwrap().rdma.unwrap().opened_connections;
            stream.write_all(&[0x3f]).unwrap();
            token(&mut stream, 0x40);
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    stats.active_grants,
                    stats.direct_bytes,
                    stats.receive_work_requests
                ),
                (count, shape.parents * shape.payload, count)
            );
            assert!(
                stats.peak_receive_work_requests <= rq_limit
                    && stats.peak_work_requests <= rq_limit
            );
            assert_eq!(
                stats.opened_connections, connections_before_rejection,
                "source opened a QP for an atomically rejected parent"
            );
            if wide {
                assert_eq!(stats.receive_credits, count);
                assert_eq!(stats.peak_receive_credits, count);
                assert_eq!(stats.receive_admission_rejections, round as u64 + 1);
                worker_pool::admitted(&engine, count);
                native::admitted(owners(&engine));
                println!(
                    "ORBITTE_RQ_ADMISSION {{\"parents\":{},\"slices_per_parent\":{},\"capacity\":{rq_limit},\"credits\":{},\"posted_receives\":{},\"unused_capacity\":{},\"rejections\":{},\"rejected_before_source_qp\":true}}",
                    shape.parents,
                    shape.slices,
                    stats.receive_credits,
                    stats.receive_work_requests,
                    rq_limit - stats.receive_credits,
                    stats.receive_admission_rejections
                );
            }
            println!(
                "ORBITTE_CONCURRENT_RECEIVE {{\"round\":{round},\"rq_debt\":{},\"grants\":{},\"direct_bytes\":{}}}",
                stats.receive_work_requests, stats.active_grants, stats.direct_bytes
            );
            if paused {
                engine.unregister_memory(&memory).unwrap();
                retired = true;
                assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 2);
                for parent in 0..shape.parents {
                    for slice in 0..shape.slices {
                        assert!(matches!(
                            memory.write(
                                parent * shape.bank
                                    + if operation == TransferOp::Read {
                                        GUARD
                                    } else {
                                        shape.destination
                                    }
                                    + shape.range(slice).start,
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
            native::arm();
            stream.write_all(&[0x41]).unwrap();
            if interrupted && dying {
                restart::terminate(
                    source,
                    generation.unwrap(),
                    "grants",
                    &engine.stats().unwrap().rdma.unwrap(),
                );
            }
            read_outcome(&mut stream, &mut successful, &mut accepted);
            if interrupted || cleanup == Some("cq") {
                stream.write_all(&[0x42]).unwrap();
                if dying {
                    let stats = native::retained(engine, memory, backing, weak, "cq");
                    restart::terminate(source, generation.unwrap(), "retained-cq", &stats);
                }
                restart::peer_exited(&mut stream);
            }
            if fault || paused {
                engine.close_source(TIMEOUT).unwrap();
            }
        }
        if !interrupted && cleanup != Some("cq") {
            super::super::connected::barrier(&mut stream, 0x32);
        }
        verify_bytes(
            shape,
            &memory,
            &original,
            &incoming,
            source == (operation == TransferOp::Write),
            &successful,
            &accepted,
        );
        if !fault && !paused {
            let snapshot = connections(&engine);
            assert!((count..=2 * count).contains(&snapshot.len()));
            if let Some(first) = &first {
                assert!(
                    first.iter().all(|connection| snapshot.contains(connection)),
                    "a healthy cached QP was replaced without eviction"
                );
            }
            let stats = engine.stats().unwrap().rdma.unwrap();
            let opened = stats.opened_connections - previous_opened;
            let reused = stats.reused_connections - previous_reused;
            assert_eq!(
                stats.completed_reusable_grants,
                count as u64 * (round as u64 + 1)
            );
            assert_eq!(opened + reused, count as u64);
            assert_eq!(
                (stats.closed_connections, stats.evicted_connections),
                (0, 0)
            );
            if round != 0 {
                assert!(reused > 0);
            }
            if shape.range(0).len() == shape.range(shape.slices - 1).len() {
                assert_eq!(snapshot.len(), count);
                assert_eq!(
                    (opened, reused),
                    if round == 0 {
                        (count as u64, 0)
                    } else {
                        (0, count as u64)
                    }
                );
            }
            println!(
                "ORBITTE_CONNECTION_REUSE {{\"round\":{round},\"grants\":{count},\"cached_qps\":{},\"opened_delta\":{opened},\"reused_delta\":{reused},\"previous_qps_preserved\":true}}",
                snapshot.len()
            );
            previous_opened = stats.opened_connections;
            previous_reused = stats.reused_connections;
            first = Some(snapshot);
        }

        println!(
            "ORBITTE_CONCURRENT_PARENTS {{\"round\":{round},\"successful\":{:?},\"accepted\":{:?}}}",
            successful, accepted
        );
        if !interrupted && cleanup != Some("cq") {
            super::super::connected::barrier(&mut stream, 0x33);
        }
    }
    worker_pool::running(&engine);
    if !retired {
        engine.unregister_memory(&memory).unwrap();
    }
    if cleanup == Some("pd") && dying {
        assert!(matches!(
            engine.close(TIMEOUT),
            Err(Error::Unquiesced {
                operation: "ibv_dealloc_pd",
                code: libc::EBUSY
            })
        ));
        let stats = native::retained(engine, memory, backing, weak, "pd");
        restart::terminate(source, generation.unwrap(), "retained-pd", &stats);
    }
    engine.close(TIMEOUT).unwrap();
    let stats = engine.stats().unwrap().rdma.unwrap();
    clean(&stats);
    worker_pool::closed(&stats);
    assert!(stats.peak_work_requests <= if source { rq_limit } else { sq_limit });
    assert!(stats.peak_receive_work_requests <= if source { rq_limit } else { sq_limit });
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    native::finish();
    if let Some(generation) = generation {
        println!(
            "ORBITTE_RESTART_RESULT {{\"source\":{source},\"generation\":{generation},\"pid\":{},\"epoch\":\"{}\",\"endpoint\":\"{}\",\"drained\":true}}",
            std::process::id(),
            engine.source_epoch(),
            engine.endpoint()
        );
    }
    drop(memory);
    drop(backing);
    assert!(weak.upgrade().is_none());
    println!(
        "ORBITTE_CONCURRENT_RESULT {{\"mode\":\"{mode}\",\"source\":{source},\"fault_rail\":{fault_rail},\"parents\":{},\"slices_per_parent\":{},\"payload\":{},\"rounds\":{rounds},\"peak_sq\":{},\"peak_rq\":{},\"all_resources_drained\":true,\"allocation_released\":true,\"performance_measurement\":false}}",
        shape.parents,
        shape.slices,
        shape.payload,
        stats.peak_work_requests,
        stats.peak_receive_work_requests
    );
}
