//! Two-host public-protocol gate: live QPs outlast grants and registrations.
use super::*;
use crate::engine::tests::source_runtime;
use crate::{EngineConfig, TransferEngine, TransferRequest, Transport};

type Observation = (u64, [u8; 24], u32, u32, u32, u32, usize, u32);

fn connections(engine: &TransferEngine, round: usize) -> Vec<Observation> {
    if round == 0 {
        super::record_topology(engine);
    }
    let runtime = source_runtime(engine);
    let progress = runtime.progress(runtime.device().unwrap()).unwrap();
    let mut records = Vec::new();
    progress
        .shared
        .control
        .lock()
        .unwrap()
        .qps
        .snapshot(&mut records);
    let mut rows = Vec::new();
    for record in records {
        let request = record.lock().unwrap();
        assert!(request.phase == Phase::Ready && !request.close_requested);
        assert!(request.work.is_none() && request.receive.is_none() && request.finish.is_none());
        assert_eq!(record.actions.load(Ordering::Acquire), 0);
        let resources = request.resources.as_ref().unwrap();
        assert!(resources.grant.is_none());
        let connection = resources.connection.lock().unwrap();
        assert!(!connection.closed && !connection.handles.qp.is_null());
        let native = unsafe { &*connection.handles.qp.cast::<ibverbs_sys::ibv_qp>() };
        assert_eq!(native.qp_num, connection.local.qpn);
        assert_eq!(native.state, ibverbs_sys::ibv_qp_state::IBV_QPS_RTS);
        let remote = connection.remote.unwrap();
        rows.push((
            record.tag,
            connection.incarnation.unwrap().encode(),
            native.qp_num,
            connection.local.psn,
            remote.qpn,
            remote.psn,
            connection.handles.qp as usize,
            connection.permission,
        ));
    }
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        rows.len(),
        4,
        "two roles and two fixed operation capabilities"
    );
    let json = rows.iter().map(|row| {
        let id = row.1.iter().fold(String::with_capacity(48), |mut id, byte| {
            use std::fmt::Write as _;
            write!(id, "{byte:02x}").unwrap();
            id
        });
        format!("{{\"tag\":{},\"connection\":\"{id}\",\"qpn\":{},\"psn\":{},\"remote_qpn\":{},\"remote_psn\":{},\"qp_handle\":{},\"permission\":{}}}", row.0,row.2,row.3,row.4,row.5,row.6,row.7)
    }).collect::<Vec<_>>().join(",");
    println!("ORBITTE_CONNECTION_SNAPSHOT {{\"round\":{round},\"connections\":[{json}]}}");
    rows
}

pub(super) fn exchange(
    stream: &mut TcpStream,
    engine: &TransferEngine,
    memory: &crate::RegisteredMemory,
) -> (std::net::SocketAddr, SourceEpoch, u64) {
    writeln!(
        stream,
        "{} {} {}",
        engine.endpoint(),
        engine.source_epoch(),
        memory.base_address()
    )
    .unwrap();
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        if byte[0] == b'\n' {
            break;
        }
        bytes.push(byte[0]);
        assert!(bytes.len() < 512);
    }
    let text = String::from_utf8(bytes).unwrap();
    let fields = text.split_whitespace().collect::<Vec<_>>();
    assert_eq!(fields.len(), 3);
    (
        fields[0].parse().unwrap(),
        fields[1].parse().unwrap(),
        fields[2].parse().unwrap(),
    )
}

pub(super) fn barrier(stream: &mut TcpStream, phase: u8) {
    stream.write_all(&[phase]).unwrap();
    token(stream, phase);
}

#[test]
#[ignore = "requires two selected RNIC hosts and original NIC locks; uses the public v13 protocol"]
fn connected_endpoint_peer() {
    println!();
    let role = env::var("ORBITTE_SHARED_ROLE").unwrap();
    let source = role == "source";
    assert!(source || role == "requester");
    let payload: usize = env::var("ORBITTE_SHARED_PAYLOAD").unwrap().parse().unwrap();
    assert!((4096..=8 << 20).contains(&payload));
    let capacity: usize = env::var("ORBITTE_SHARED_CQES").unwrap().parse().unwrap();
    let seed: u64 = env::var("ORBITTE_SHARED_SEED").unwrap().parse().unwrap();
    let length = 2 * payload + 3 * GUARD;
    let destination = payload + 2 * GUARD;
    let bind: std::net::SocketAddr = env::var("ORBITTE_SHARED_BIND").unwrap().parse().unwrap();
    let mut config = super::rdma_config();
    config.max_endpoints = 4;
    config.max_cached_mrs = 2;
    config.max_direct_bytes = 2 * payload;
    config.max_cached_mr_bytes = 2 * length;
    config.max_send_wr = 1;
    config.max_cqes = capacity;
    config.block_size = 4096;
    let engine = TransferEngine::new(EngineConfig {
        bind_addr: bind,
        rdma: Some(config),
        handler_timeout: TIMEOUT,
        ..EngineConfig::default()
    })
    .unwrap();
    let mut backing = Arc::new(HostMemory::new(vec![0xa5; length].into_boxed_slice()).unwrap());
    let mut memory = engine
        .register_host_memory(Arc::clone(&backing), 0, length, Permission::RemoteReadWrite)
        .unwrap();
    let mut stream = if source {
        let listener = TcpListener::bind(bind).unwrap();
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
    let own_seed = seed ^ if source { 0x13579bdf } else { 0x2468ace0 };
    let peer_seed = seed ^ if source { 0x2468ace0 } else { 0x13579bdf };
    let mut first = None;
    let mut transferred = 0;
    let mut retired_backings = 0;
    for round in 0..4 {
        let (endpoint, epoch, remote) = exchange(&mut stream, &engine, &memory);
        let peer = engine.open_segment(endpoint, epoch, TIMEOUT).unwrap();
        let local_skip = 13 + round * 7;
        let remote_skip = 5 + round * 11;
        let count = payload - local_skip.max(remote_skip) - 31;
        let mut expected = vec![0xa5; length];
        expected[destination..destination + payload].fill(0xcc);
        let read_seed = own_seed ^ round as u64;
        let peer_read_seed = peer_seed ^ round as u64;
        for index in 0..payload {
            expected[GUARD + index] = value(read_seed, index);
        }
        memory.write(0, &expected).unwrap();
        barrier(&mut stream, 0x10 + round as u8);
        for operation in [TransferOp::Read, TransferOp::Write] {
            if operation == TransferOp::Write {
                expected[destination..destination + payload].fill(0xcc);
                for index in 0..payload {
                    expected[GUARD + index] = value(read_seed ^ 0xa55a5aa5, index);
                }
                memory.write(0, &expected).unwrap();
                barrier(&mut stream, 0x40 + round as u8);
            }
            let (local_offset, remote_offset) = if operation == TransferOp::Read {
                (destination + local_skip, GUARD + remote_skip)
            } else {
                (GUARD + remote_skip, destination + local_skip)
            };
            let report = engine
                .submit_and_wait(
                    operation,
                    &peer,
                    &[TransferRequest {
                        priority: Default::default(),
                        remote_staging: false,
                        transport: Transport::Rdma,
                        local_address: memory.base_address() + local_offset as u64,
                        remote_address: remote + remote_offset as u64,
                        length: count,
                    }],
                    TIMEOUT,
                )
                .unwrap();
            assert!(report.is_complete(), "{report:?}");
            barrier(
                &mut stream,
                if operation == TransferOp::Read {
                    0x20
                } else {
                    0x50
                } + round as u8,
            );
            let peer_pattern = peer_read_seed
                ^ if operation == TransferOp::Write {
                    0xa55a5aa5
                } else {
                    0
                };
            for index in 0..count {
                expected[destination + local_skip + index] =
                    value(peer_pattern, remote_skip + index);
            }
            verify(&memory, &expected);
            let stats = engine.stats().unwrap().rdma.unwrap();
            assert_eq!(
                (
                    stats.active_grants,
                    stats.active_windows,
                    stats.direct_bytes,
                    stats.message_bytes,
                    stats.active_message_mrs,
                    stats.work_requests,
                    stats.receive_work_requests
                ),
                (0, 0, 0, 0, 0, 0, 0)
            );
            barrier(
                &mut stream,
                if operation == TransferOp::Read {
                    0x30
                } else {
                    0x60
                } + round as u8,
            );
        }
        transferred += count as u64;
        let snapshot = connections(&engine, round);
        if let Some(first) = &first {
            assert_eq!(
                &snapshot, first,
                "actual QP, PSN, peer and incarnation must survive grant/registration changes"
            );
        } else {
            first = Some(snapshot);
        }
        let stats = engine.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.idle_endpoints,
                stats.opened_connections,
                stats.closed_connections
            ),
            (4, 4, 4, 0)
        );
        assert_eq!(stats.reused_connections, 4 * round as u64);
        assert_eq!(stats.completed_reusable_grants, 4 * (round as u64 + 1));
        if round == 1 {
            engine.unregister_memory(&memory).unwrap();
            assert_eq!(engine.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
            let weak = Arc::downgrade(&backing);
            drop(memory);
            drop(backing);
            let end = Instant::now() + Duration::from_secs(2);
            while weak.upgrade().is_some() && Instant::now() < end {
                thread::yield_now();
            }
            assert!(
                weak.upgrade().is_none(),
                "live QPs must not retain old registered backing"
            );
            retired_backings += 1;
            backing = Arc::new(HostMemory::new(vec![0xa5; length].into_boxed_slice()).unwrap());
            memory = engine
                .register_host_memory(Arc::clone(&backing), 0, length, Permission::RemoteReadWrite)
                .unwrap();
        }
        barrier(&mut stream, 0x70 + round as u8);
    }
    let idle = engine.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (
            idle.persistent_mrs,
            idle.opened_persistent_mrs,
            idle.reused_persistent_mrs
        ),
        (2, 4, 16)
    );
    assert_eq!(
        (
            idle.persistent_source_mrs,
            idle.opened_source_mrs,
            idle.reused_source_mrs
        ),
        (1, 2, 8)
    );
    assert_eq!(
        (
            idle.completed_sends,
            idle.completed_receives,
            idle.completed_window_binds,
            idle.completed_window_invalidations
        ),
        (8, 8, 8, 8)
    );
    assert_eq!(
        (idle.completed_read_bytes, idle.completed_write_bytes),
        (transferred, transferred)
    );
    assert!((1..=capacity).contains(&idle.peak_work_requests));
    assert!((1..=capacity).contains(&idle.peak_receive_work_requests));
    if let Ok(path) = env::var("ORBITTE_SHARED_MAPS") {
        std::fs::write(path, std::fs::read_to_string("/proc/self/maps").unwrap()).unwrap();
    }
    barrier(&mut stream, 0x7f);
    engine.close(TIMEOUT).unwrap();
    engine.unregister_memory(&memory).unwrap();
    drop(memory);
    drop(backing);
    let closed = engine.stats().unwrap().rdma.unwrap();
    super::record_completion_events(&idle, &closed, false);
    assert_eq!(
        [
            closed.active_endpoints,
            closed.idle_endpoints,
            closed.active_grants,
            closed.active_devices,
            closed.active_cqs,
            closed.progress_workers,
            closed.async_event_sources,
            closed.persistent_mrs,
            closed.window_key_indices,
            closed.retained_endpoints,
            closed.retained_grants,
            closed.retained_devices,
            closed.retained_cqs,
            closed.retained_windows,
            closed.retained_persistent_mrs,
            closed.work_requests,
            closed.receive_work_requests,
            closed.active_message_mrs,
            closed.message_bytes
        ],
        [0; 19]
    );
    assert_eq!(
        (closed.opened_connections, closed.closed_connections),
        (4, 4)
    );
    println!(
        "ORBITTE_ASYNC_STATS {{\"idle_sources\":{},\"sources\":{},\"acknowledged_events\":{}}}",
        idle.async_event_sources, closed.async_event_sources, closed.acknowledged_async_events
    );
    println!(
        "ORBITTE_OWNER_STATS {{\"idle_connections\":4,\"idle_grants\":0,\"connections\":0,\"grants\":0,\"retained_connections\":0,\"retained_grants\":0}}"
    );
    println!(
        "ORBITTE_SOURCE_MR_STATS {{\"idle_parents\":1,\"parents\":0,\"opened\":2,\"reused\":6,\"retained\":0}}"
    );
    println!(
        "ORBITTE_KEY_STATS {{\"idle_indices\":{},\"indices\":0,\"burned_keys\":8,\"exhaustions\":0}}",
        idle.window_key_indices
    );
    receive_stats(&closed);
    println!(
        "{{\"role\":\"{role}\",\"status\":\"passed\",\"connected_reuse\":true,\"duplex\":true,\"rounds\":4,\"real_qps\":4,\"opened_connections\":{},\"closed_connections\":{},\"reused_connections\":{},\"completed_grants\":{},\"retired_backings_with_live_qps\":{retired_backings},\"cq_capacity\":{capacity},\"sq_window\":1,\"peak_work_requests\":{},\"peak_receive_work_requests\":{},\"completed_read_bytes\":{transferred},\"completed_write_bytes\":{transferred},\"completed_sends\":8,\"completed_receives\":8,\"full_buffer_and_guards\":true,\"all_resources_drained\":true,\"performance_measurement\":false}}",
        closed.opened_connections,
        closed.closed_connections,
        closed.reused_connections,
        closed.completed_reusable_grants,
        idle.peak_work_requests,
        idle.peak_receive_work_requests
    );
}
