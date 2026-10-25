//! Explicit two-host shared-CQ correctness gate. The post gate delays dispatch;
//! all WRs, CQEs, MR registrations and byte movement use the native provider.
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;

use super::*;
use crate::memory::MemoryRegistry;
use crate::transport::rdma::{RdmaHostMemory, buffers};
use crate::{HostMemory, Permission, SourceEpoch};

const COUNT: usize = 4;
const GUARD: usize = 64;
const TIMEOUT: Duration = Duration::from_secs(35);
#[derive(Default)]
struct DispatchGate {
    armed: bool,
    entered: bool,
    released: bool,
}
static GATE: OnceLock<(Mutex<DispatchGate>, Condvar)> = OnceLock::new();

unsafe extern "C" fn gated_post(
    h: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    requests: *const sys::WorkRequest,
    count: usize,
    local: u64,
    length: usize,
    remote: u64,
    rkey: u32,
    write: i32,
    result: *mut sys::PostResult,
) -> i32 {
    let (lock, changed) = GATE.get_or_init(Default::default);
    let mut state = lock.lock().unwrap();
    if state.armed {
        state.armed = false;
        state.entered = true;
        changed.notify_all();
        while !state.released {
            state = changed.wait(state).unwrap();
        }
    }
    drop(state);
    unsafe {
        (sys::REAL.post)(
            h, memory, requests, count, local, length, remote, rkey, write, result,
        )
    }
}

static VERBS: sys::Verbs = sys::Verbs {
    asynchronous: sys::REAL.asynchronous,
    completion: sys::REAL.completion,
    discover: sys::REAL.discover,
    query_address: sys::REAL.query_address,
    open_device: sys::REAL.open_device,
    create_cq: sys::REAL.create_cq,
    register_memory: sys::REAL.register_memory,
    prepare_grant: sys::REAL.prepare_grant,
    open: sys::REAL.open,
    connect: sys::REAL.connect,
    post: gated_post,
    window: sys::REAL.window,
    post_receive: sys::REAL.post_receive,
    send_completion: sys::REAL.send_completion,
    dealloc_window: sys::REAL.dealloc_window,
    poll: sys::REAL.poll,
    destroy_qp: sys::REAL.destroy_qp,
    deregister: sys::REAL.deregister,
    destroy_cq: sys::REAL.destroy_cq,
    dealloc_pd: sys::REAL.dealloc_pd,
    close_device: sys::REAL.close_device,
};

#[path = "worker_pool.rs"]
mod worker_pool;

fn rdma_config() -> RdmaConfig {
    let name = env::var("ORBITTE_SHARED_DEVICE").unwrap();
    let mut config = if env::var("ORBITTE_RDMA_DISCOVERY").as_deref() == Ok("1") {
        RdmaConfig {
            rdma_whitelist: vec![name],
            ..Default::default()
        }
    } else {
        let mut config = RdmaConfig::new(name);
        config.gid_index = Some(env::var("ORBITTE_SHARED_GID").unwrap().parse().unwrap());
        config
    };
    config.hop_limit = 64;
    if env::var("ORBITTE_RDMA_EVENTS").as_deref() == Ok("1") {
        config.progress_mode = super::super::super::RdmaProgressMode::EventDriven;
    }
    config
}

fn record_completion_events(idle: &RdmaStats, closed: &RdmaStats, fault_profile: bool) {
    let enabled = env::var("ORBITTE_RDMA_EVENTS").as_deref() == Ok("1");
    let aborted = idle.progress_failures != 0;
    assert!(
        !aborted || fault_profile,
        "healthy profile unexpectedly aborted its lane"
    );
    assert_eq!(idle.progress_workers, 1);
    assert_eq!(
        (
            idle.active_completion_channels,
            idle.completion_event_workers
        ),
        (
            usize::from(enabled && !fault_profile),
            usize::from(enabled && !fault_profile)
        )
    );
    assert_eq!(closed.opened_completion_channels, u64::from(enabled));
    assert_eq!(
        (
            closed.active_completion_channels,
            closed.retained_completion_channels,
            closed.completion_event_workers
        ),
        (0, 0, 0)
    );
    if enabled {
        assert!(closed.cq_notification_arms >= 2);
        if closed.completed_reads
            + closed.completed_writes
            + closed.completed_window_binds
            + closed.completed_window_invalidations
            + closed.completed_sends
            + closed.completed_receives
            + closed.completion_errors
            > 0
        {
            assert!(closed.completion_events > 0);
        }
    }
    println!(
        "ORBITTE_CQ_EVENT_STATS {{\"opened_channels\":{},\"idle_workers\":{},\"events\":{},\"arms\":{},\"waits\":{},\"active_channels\":0,\"retained_channels\":0,\"workers\":0}}",
        closed.opened_completion_channels,
        idle.completion_event_workers,
        closed.completion_events,
        closed.cq_notification_arms,
        closed.cq_event_waits
    );
}

fn record_topology(engine: &crate::TransferEngine) {
    let topology = engine.topology().unwrap().unwrap();
    let selected = topology
        .nic_list
        .iter()
        .filter(|nic| nic.device_attrs.contains_key("rdma.selected"))
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 1);
    let nic = selected[0];
    let attrs = &nic.device_attrs;
    assert_eq!(nic.name, env::var("ORBITTE_SHARED_DEVICE").unwrap());
    assert_eq!(attrs["rdma.port"], "1");
    assert_eq!(
        attrs["rdma.gid_index"],
        env::var("ORBITTE_SHARED_GID").unwrap()
    );
    assert_eq!(attrs["rdma.gid_type"], "RoCE v2");
    assert_ne!(attrs["rdma.net_device_index"], "0");
    if env::var("ORBITTE_RDMA_DISCOVERY").as_deref() == Ok("1") {
        assert_eq!(
            topology.nic_list.len(),
            1,
            "only the locked RNIC is eligible"
        );
    }
    println!(
        "ORBITTE_TOPOLOGY {{\"device\":\"{}\",\"port\":{},\"gid_index\":{},\"gid\":\"{}\",\"gid_type\":\"{}\",\"net_device_index\":{}}}",
        nic.name,
        attrs["rdma.port"],
        attrs["rdma.gid_index"],
        attrs["rdma.gid"],
        attrs["rdma.gid_type"],
        attrs["rdma.net_device_index"]
    );
}

fn value(seed: u64, index: usize) -> u8 {
    let value = (index as u64)
        .wrapping_mul(0x9e3779b97f4a7c15)
        .wrapping_add(seed)
        .rotate_left((index % 63) as u32);
    (value ^ (value >> 17) ^ (value >> 41)) as u8
}

fn token(stream: &mut TcpStream, expected: u8) {
    let mut actual = [0];
    stream.read_exact(&mut actual).unwrap();
    assert_eq!(actual[0], expected);
}

fn verify(memory: &crate::RegisteredMemory, expected: &[u8]) {
    let mut actual = vec![0; expected.len()];
    memory.read(0, &mut actual).unwrap();
    assert!(actual == expected, "full buffer or guard comparison failed");
}

#[test]
#[ignore = "requires an explicitly selected two-host RNIC profile and original resource locks"]
fn shared_cq_peer() {
    if env::var("ORBITTE_SHARED_DUPLEX").as_deref() == Ok("1") {
        duplex_completion_peer();
        return;
    }
    if env::var("ORBITTE_RDMA_MULTI_CONTEXT").as_deref() == Ok("1") {
        multi_context::registration();
    }
    // Keep readiness and JSON on their own lines after libtest's name prefix.
    println!();
    let role = env::var("ORBITTE_SHARED_ROLE").unwrap();
    assert!(matches!(role.as_str(), "source" | "requester"));
    let source = role == "source";
    let payload: usize = env::var("ORBITTE_SHARED_PAYLOAD").unwrap().parse().unwrap();
    assert!((4096..=8 << 20).contains(&payload));
    let capacity: usize = env::var("ORBITTE_SHARED_CQES").unwrap().parse().unwrap();
    let window: usize = env::var("ORBITTE_SHARED_WINDOW").unwrap().parse().unwrap();
    assert!((1..=COUNT).contains(&capacity) && (1..=2).contains(&window));
    let seed: u64 = env::var("ORBITTE_SHARED_SEED").unwrap().parse().unwrap();
    let stride = payload + 2 * GUARD;
    let length = COUNT * stride;
    let mut config = rdma_config();
    config.host_memory = RdmaHostMemory::Direct;
    config.max_endpoints = COUNT;
    config.max_direct_bytes = length;
    config.max_cached_mr_bytes = length;
    config.block_size = 4096;
    config.max_cqes = capacity;
    config.max_send_wr = window;
    let usage = Arc::default();
    let runtime = RdmaTransport {
        registrations: buffers::LocalBufferManager::new(&config, Arc::clone(&usage), &VERBS),
        config,
        usage,
        counters: Arc::default(),
        workers: Arc::default(),
        cq_quota: Arc::default(),
        verbs: &VERBS,
        context_set: Mutex::default(),
        drained: Arc::default(),
    };
    let registry = MemoryRegistry::new(SourceEpoch::generate().unwrap(), 1, length);
    let mut original = vec![0xa5; length];
    for slot in 0..COUNT {
        for (index, byte) in original
            .iter_mut()
            .enumerate()
            .skip(slot * stride + GUARD)
            .take(payload)
        {
            *byte = value(seed, index);
        }
    }
    let mut expected = if source {
        original.clone()
    } else {
        vec![0xcc; length]
    };
    let backing = Arc::new(HostMemory::new(expected.clone().into_boxed_slice()).unwrap());
    let memory = registry
        .register_host(Arc::clone(&backing), 0, length, Permission::RemoteReadWrite)
        .unwrap();
    let mut stream = if source {
        let listener = TcpListener::bind(env::var("ORBITTE_SHARED_BIND").unwrap()).unwrap();
        listener.set_nonblocking(true).unwrap();
        println!("ORBITTE_READY {}", listener.local_addr().unwrap());
        std::io::stdout().flush().unwrap();
        let end = Instant::now() + TIMEOUT;
        loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < end);
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("{e}"),
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
    for operation in [TransferOp::Read, TransferOp::Write] {
        if !source && operation == TransferOp::Write {
            for slot in 0..COUNT {
                let offset = slot * stride + GUARD;
                let bytes = (offset..offset + payload)
                    .map(|i| value(seed ^ 0xa55a5aa5, i))
                    .collect::<Vec<_>>();
                memory.write(offset, &bytes).unwrap();
                expected[offset..offset + payload].copy_from_slice(&bytes);
            }
        }
        let end = Instant::now() + TIMEOUT;
        let mut endpoints = (0..COUNT)
            .map(|slot| {
                runtime
                    .endpoint(
                        &memory,
                        slot * stride + GUARD,
                        payload,
                        if source {
                            if operation == TransferOp::Read { 1 } else { 2 }
                        } else {
                            0
                        },
                        end,
                        None,
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut remote = Vec::new();
        if source {
            for endpoint in &endpoints {
                stream.write_all(&endpoint.connection()).unwrap();
            }
        }
        for _ in 0..COUNT {
            let mut bytes = [0; crate::transport::rdma::DESCRIPTOR_SIZE];
            stream.read_exact(&mut bytes).unwrap();
            remote.push(Descriptor::decode_connection(&bytes).unwrap());
        }
        if !source {
            for endpoint in &endpoints {
                stream.write_all(&endpoint.connection()).unwrap();
            }
        }
        for (endpoint, descriptor) in endpoints.iter_mut().zip(&remote) {
            endpoint.connect(descriptor).unwrap();
        }
        if source {
            for endpoint in &mut endpoints {
                endpoint.bind_window().unwrap();
                stream
                    .write_all(&endpoint.grant().unwrap().encode())
                    .unwrap();
            }
        } else {
            for descriptor in &mut remote {
                let mut bytes = [0; crate::transport::rdma::DESCRIPTOR_SIZE];
                stream.read_exact(&mut bytes).unwrap();
                let grant = Descriptor::decode(&bytes).unwrap();
                assert_eq!(grant.connection_bytes(), descriptor.connection_bytes());
                *descriptor = grant;
            }
        }
        if source {
            stream.write_all(&[1]).unwrap();
            token(&mut stream, 2);
        } else {
            token(&mut stream, 1);
            let (lock, changed) = GATE.get_or_init(Default::default);
            *lock.lock().unwrap() = DispatchGate {
                armed: true,
                entered: false,
                released: false,
            };
            let records = endpoints
                .iter()
                .skip(1)
                .map(|e| Arc::clone(&e.record))
                .collect::<Vec<_>>();
            let mut pairs = endpoints.into_iter().zip(remote).collect::<Vec<_>>();
            let (mut first, descriptor) = pairs.remove(0);
            let first = thread::spawn(move || {
                first.transfer(&descriptor, operation, end).unwrap();
                first
            });
            let (state, timed) = changed
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |s| !s.entered)
                .unwrap();
            assert!(
                !timed.timed_out() && state.entered,
                "first native dispatch did not reach gate"
            );
            drop(state);
            let workers = pairs
                .into_iter()
                .map(|(mut endpoint, descriptor)| {
                    thread::spawn(move || {
                        endpoint.transfer(&descriptor, operation, end).unwrap();
                        endpoint
                    })
                })
                .collect::<Vec<_>>();
            let gate_end = Instant::now() + Duration::from_secs(5);
            while !records.iter().all(|r| r.lock().unwrap().work.is_some()) {
                assert!(Instant::now() < gate_end, "other QPs did not queue");
                thread::yield_now();
            }
            lock.lock().unwrap().released = true;
            changed.notify_all();
            endpoints = vec![first.join().unwrap()];
            endpoints.extend(workers.into_iter().map(|w| w.join().unwrap()));
            // Completed QPs leave the shared CQ serving the other real QPs.
            for endpoint in &mut endpoints {
                endpoint.close().unwrap();
            }
            stream.write_all(&[2]).unwrap();
            token(&mut stream, 3);
        }
        for endpoint in &mut endpoints {
            if source {
                endpoint.invalidate_window().unwrap();
            }
            endpoint.close().unwrap();
        }
        drop(endpoints);
        if source {
            if operation == TransferOp::Write {
                for slot in 0..COUNT {
                    let offset = slot * stride + GUARD;
                    for (index, byte) in expected.iter_mut().enumerate().skip(offset).take(payload)
                    {
                        *byte = value(seed ^ 0xa55a5aa5, index);
                    }
                }
            }
            verify(&memory, &expected);
            stream.write_all(&[3]).unwrap();
        } else {
            if operation == TransferOp::Read {
                for slot in 0..COUNT {
                    let offset = slot * stride + GUARD;
                    expected[offset..offset + payload]
                        .copy_from_slice(&original[offset..offset + payload]);
                }
            }
            verify(&memory, &expected);
        }
    }
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    let idle = runtime.stats().unwrap();
    assert_eq!(
        (idle.active_cqs, idle.opened_cqs, idle.progress_workers),
        (2, 2, 1)
    );
    assert_eq!(
        (
            idle.active_endpoints,
            idle.work_requests,
            idle.progress_failures
        ),
        (0, 0, 0)
    );
    assert_eq!(idle.active_windows, 0);
    assert_eq!(
        idle.burned_window_keys,
        if source { (2 * COUNT) as u64 } else { 0 }
    );
    assert_eq!(idle.window_key_exhaustions, 0);
    assert_eq!(idle.window_key_indices != 0, source);
    assert_eq!(
        idle.completed_window_binds,
        if source { (2 * COUNT) as u64 } else { 0 }
    );
    assert_eq!(
        idle.completed_window_invalidations,
        idle.completed_window_binds
    );
    assert_eq!(
        (
            idle.opened_persistent_mrs,
            idle.reused_persistent_mrs,
            idle.persistent_mrs,
            idle.persistent_mr_bytes
        ),
        (1, 7, 1, length)
    );
    assert_eq!(
        (
            idle.persistent_source_mrs,
            idle.opened_source_mrs,
            idle.reused_source_mrs
        ),
        (
            usize::from(source),
            u64::from(source),
            if source { 7 } else { 0 }
        )
    );
    if !source {
        assert_eq!(idle.peak_work_requests, capacity);
        assert!(idle.peak_pending_qps >= capacity.div_ceil(window));

        assert_eq!(
            (idle.completed_read_bytes, idle.completed_write_bytes),
            ((COUNT * payload) as u64, (COUNT * payload) as u64)
        );
    }
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    drop(memory);
    drop(backing);
    runtime.close(Instant::now() + TIMEOUT).unwrap();
    let closed = runtime.stats().unwrap();
    record_completion_events(&idle, &closed, false);
    assert_eq!(
        (
            idle.active_grants,
            idle.retained_grants,
            closed.active_grants,
            closed.retained_grants
        ),
        (0, 0, 0, 0)
    );
    source_mr_stats(&idle, &closed);
    receive_stats(&closed);
    assert_eq!(closed.window_key_indices, 0);
    println!(
        "ORBITTE_OWNER_STATS {{\"idle_connections\":{},\"idle_grants\":{},\"connections\":{},\"grants\":{},\"retained_connections\":{},\"retained_grants\":{}}}",
        idle.active_endpoints,
        idle.active_grants,
        closed.active_endpoints,
        closed.active_grants,
        closed.retained_endpoints,
        closed.retained_grants
    );
    println!(
        "ORBITTE_KEY_STATS {{\"idle_indices\":{},\"indices\":{},\"burned_keys\":{},\"exhaustions\":{}}}",
        idle.window_key_indices,
        closed.window_key_indices,
        closed.burned_window_keys,
        closed.window_key_exhaustions
    );
    println!(
        "ORBITTE_ASYNC_STATS {{\"idle_workers\":{},\"workers\":{},\"acknowledged_events\":{}}}",
        idle.async_event_workers, closed.async_event_workers, closed.acknowledged_async_events
    );
    assert_eq!(
        (
            closed.active_devices,
            closed.active_cqs,
            closed.progress_workers,
            closed.async_event_workers,
            closed.active_endpoints,
            closed.persistent_mrs
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert_eq!(
        (
            closed.retained_endpoints,
            closed.retained_devices,
            closed.retained_cqs,
            closed.retained_persistent_mrs,
            closed.retained_work_requests
        ),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(registry.usage().unwrap(), (0, 0));
    println!(
        "{{\"role\":\"{role}\",\"status\":\"passed\",\"real_qps\":{COUNT},\"cq_capacity\":{capacity},\"sq_window\":{window},\"peak_work_requests\":{},\"peak_pending_qps\":{},\"opened_cqs\":{},\"idle_progress_workers\":{},\"opened_persistent_mrs\":{},\"reused_persistent_mrs\":{},\"completed_window_binds\":{},\"completed_window_invalidations\":{},\"completed_read_bytes\":{},\"completed_write_bytes\":{},\"full_buffer_and_guards\":true,\"all_resources_drained\":true,\"performance_measurement\":false}}",
        idle.peak_work_requests,
        idle.peak_pending_qps,
        idle.opened_cqs,
        idle.progress_workers,
        idle.opened_persistent_mrs,
        idle.reused_persistent_mrs,
        idle.completed_window_binds,
        idle.completed_window_invalidations,
        idle.completed_read_bytes,
        idle.completed_write_bytes
    );
}

#[test]
#[ignore = "requires selected physical RNIC hosts and original resource locks; intentional provider/completion error"]
fn source_window_peer() {
    println!();
    let role = env::var("ORBITTE_SHARED_ROLE").unwrap();
    assert!(matches!(role.as_str(), "source" | "requester"));
    let source = role == "source";
    let probe = env::var("ORBITTE_WINDOW_PROBE").unwrap();
    assert!(matches!(
        probe.as_str(),
        "revoked"
            | "parent"
            | "remote"
            | "range"
            | "fragment-read"
            | "fragment-write"
            | "partial-read"
            | "partial-write"
    ));
    let partial = probe.starts_with("partial-");
    let fragmented = probe.starts_with("fragment-") || partial;
    let length = if fragmented { 3 * 4096 } else { 4096 } + 2 * GUARD;
    let mut config = rdma_config();
    config.host_memory = RdmaHostMemory::Direct;
    config.max_cqes = if fragmented { 3 } else { 1 };
    config.max_send_wr = if fragmented { 3 } else { 1 };
    config.block_size = 4096;
    let mut runtime = RdmaTransport::new(config).unwrap();
    if fragmented && !source {
        Arc::get_mut(&mut runtime).unwrap().verbs = if partial {
            &partial::VERBS
        } else {
            &fragmented::VERBS
        };
    }
    let registry = MemoryRegistry::new(SourceEpoch::generate().unwrap(), 1, length);
    let mut expected = vec![if source { 0x5a } else { 0xc3 }; length];
    let backing = Arc::new(HostMemory::new(expected.clone().into_boxed_slice()).unwrap());
    let memory = registry
        .register_host(Arc::clone(&backing), 0, length, Permission::RemoteReadWrite)
        .unwrap();
    let mut stream = if source {
        let listener = TcpListener::bind(env::var("ORBITTE_SHARED_BIND").unwrap()).unwrap();
        listener.set_nonblocking(true).unwrap();
        println!("ORBITTE_READY {}", listener.local_addr().unwrap());
        std::io::stdout().flush().unwrap();
        let end = Instant::now() + TIMEOUT;
        loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < end);
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("{e}"),
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
    let mut endpoint = runtime
        .endpoint(
            &memory,
            GUARD,
            if source && !partial {
                4096
            } else {
                length - 2 * GUARD
            },
            if source {
                if matches!(probe.as_str(), "remote" | "fragment-read" | "partial-read") {
                    1
                } else {
                    2
                }
            } else {
                0
            },
            Instant::now() + TIMEOUT,
            None,
        )
        .unwrap();
    let progress = Arc::clone(&endpoint.progress);
    if source {
        stream.write_all(&endpoint.connection()).unwrap();
    }
    let mut bytes = [0; crate::transport::rdma::DESCRIPTOR_SIZE];
    stream.read_exact(&mut bytes).unwrap();
    let connection = Descriptor::decode_connection(&bytes).unwrap();
    if !source {
        stream.write_all(&endpoint.connection()).unwrap();
    }
    endpoint.connect(&connection).unwrap();
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    if source {
        endpoint.bind_window().unwrap();
        let mut grant = endpoint.grant().unwrap();
        if probe == "revoked" {
            endpoint.invalidate_window().unwrap();
        } else if probe == "parent" {
            // A deliberately hostile test peer receives the unadvertised parent key.
            // Production bootstrap never exposes either parent key or parent address.
            let request = endpoint.record.lock().unwrap();
            let h = &request
                .resources
                .as_ref()
                .unwrap()
                .grant
                .as_ref()
                .unwrap()
                .handles;
            grant.rkey = unsafe { (*h.mr.cast::<ibverbs_sys::ibv_mr>()).rkey };
        }
        let binding = [0x42; COMPLETION_MESSAGE_SIZE];
        if probe == "remote" {
            post_receive(&mut endpoint, &binding).unwrap();
        }
        stream.write_all(&grant.encode()).unwrap();
        if probe == "remote" {
            endpoint.receive_completion().unwrap();
            // Keep this actual source QP/MW alive until the hostile old-key access ends.
            stream.write_all(&[9]).unwrap();
        }
        token(&mut stream, 4);
        // The peer has observed the native error and physically closed its QP.
        let close = endpoint.close();
        assert!(!matches!(close, Err(Error::Unquiesced { .. })), "{close:?}");
        drop(endpoint);
        if probe == "fragment-write" {
            expected[GUARD..GUARD + 4096].fill(0xc3);
        }
        if probe == "partial-write" {
            partial::verify_prefix(&memory, &expected, 0xc3);
        } else {
            verify(&memory, &expected);
        }
        stream.write_all(&[5]).unwrap();
    } else {
        stream.read_exact(&mut bytes).unwrap();
        let mut grant = Descriptor::decode(&bytes).unwrap();
        assert_eq!(grant.connection_bytes(), connection.connection_bytes());
        if probe == "range" {
            // The address is inside the persistent parent but outside the exact MW.
            grant.address -= 1;
        }
        if probe == "remote" {
            endpoint
                .transfer(&grant, TransferOp::Read, Instant::now() + TIMEOUT)
                .unwrap();
            expected[GUARD..GUARD + 4096].fill(0x5a);
            endpoint
                .send_completion(&[0x42; COMPLETION_MESSAGE_SIZE], grant.rkey)
                .unwrap();
            token(&mut stream, 9);
            // The hostile test bypasses only the public one-grant submission guard.
            // The old key is still submitted through the real provider/worker/CQ.
            endpoint.record.lock().unwrap().message_sent = false;
        }
        if fragmented {
            fragmented::arm(&runtime, &endpoint, memory.clone(), partial);
        }
        let error = endpoint
            .transfer(
                &grant,
                if matches!(probe.as_str(), "remote" | "fragment-read" | "partial-read") {
                    TransferOp::Read
                } else {
                    TransferOp::Write
                },
                Instant::now() + TIMEOUT,
            )
            .unwrap_err();
        if partial {
            assert!(
                matches!(
                    error,
                    Error::Backend {
                        operation: "ibv_post_send",
                        code: libc::ENOMEM
                    }
                ),
                "expected native partial post rejection: {error:?}"
            );
        } else {
            assert!(
                matches!(
                    error,
                    Error::Backend {
                        operation: "RDMA work completion",
                        code: 10
                    }
                ),
                "expected real REM_ACCESS_ERR: {error:?}"
            );
        }
        let close = endpoint.close();
        assert!(!matches!(close, Err(Error::Unquiesced { .. })), "{close:?}");
        drop(endpoint);
        if fragmented {
            fragmented::finish(&runtime.stats().unwrap());
            if probe == "fragment-read" {
                expected[GUARD..GUARD + 4096].fill(0x5a);
            }
        }
        if probe == "partial-read" {
            partial::verify_prefix(&memory, &expected, 0x5a);
        } else {
            verify(&memory, &expected);
        }
        stream.write_all(&[4]).unwrap();
        token(&mut stream, 5);
    }
    // The request's drain receipt can precede the worker's final exit charge.
    // Join the lane before observing its retired channels and consumers.
    progress.close(Instant::now() + TIMEOUT).unwrap();
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    let idle = runtime.stats().unwrap();
    assert_eq!(
        (
            idle.active_windows,
            idle.active_endpoints,
            idle.work_requests,
            idle.retained_windows
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(idle.completed_window_binds, u64::from(source));
    assert_eq!(idle.burned_window_keys, u64::from(source));
    assert_eq!(idle.window_key_exhaustions, 0);
    assert_eq!(idle.window_key_indices, usize::from(source));
    assert_eq!(
        idle.completed_window_invalidations,
        u64::from(source && matches!(probe.as_str(), "revoked" | "remote"))
    );
    assert_eq!(idle.completion_errors, u64::from(!source && !partial));
    assert_eq!(
        (idle.completed_write_bytes, idle.completed_read_bytes),
        (
            if !source && probe == "fragment-write" {
                4096
            } else {
                0
            },
            if !source && matches!(probe.as_str(), "remote" | "fragment-read") {
                4096
            } else {
                0
            }
        )
    );
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    drop(memory);
    drop(backing);
    runtime.close(Instant::now() + TIMEOUT).unwrap();
    let closed = runtime.stats().unwrap();
    record_completion_events(&idle, &closed, true);
    assert_eq!(
        (
            idle.active_grants,
            idle.retained_grants,
            closed.active_grants,
            closed.retained_grants
        ),
        (0, 0, 0, 0)
    );
    source_mr_stats(&idle, &closed);
    receive_stats(&closed);
    assert_eq!(closed.window_key_indices, 0);
    println!(
        "ORBITTE_OWNER_STATS {{\"idle_connections\":{},\"idle_grants\":{},\"connections\":{},\"grants\":{},\"retained_connections\":{},\"retained_grants\":{}}}",
        idle.active_endpoints,
        idle.active_grants,
        closed.active_endpoints,
        closed.active_grants,
        closed.retained_endpoints,
        closed.retained_grants
    );
    println!(
        "ORBITTE_KEY_STATS {{\"idle_indices\":{},\"indices\":{},\"burned_keys\":{},\"exhaustions\":{}}}",
        idle.window_key_indices,
        closed.window_key_indices,
        closed.burned_window_keys,
        closed.window_key_exhaustions
    );
    println!(
        "ORBITTE_ASYNC_STATS {{\"idle_workers\":{},\"workers\":{},\"acknowledged_events\":{}}}",
        idle.async_event_workers, closed.async_event_workers, closed.acknowledged_async_events
    );
    assert_eq!(
        (
            closed.active_devices,
            closed.active_cqs,
            closed.progress_workers,
            closed.async_event_workers,
            closed.active_windows,
            closed.active_endpoints,
            closed.persistent_mrs,
            closed.work_requests
        ),
        (0, 0, 0, 0, 0, 0, 0, 0)
    );
    assert_eq!(
        (
            closed.retained_endpoints,
            closed.retained_devices,
            closed.retained_windows,
            closed.retained_work_requests
        ),
        (0, 0, 0, 0)
    );
    println!(
        "{{\"role\":\"{role}\",\"status\":\"passed\",\"probe\":\"{probe}\",\"real_remote_access_error\":{},\"completed_window_binds\":{},\"completed_window_invalidations\":{},\"full_buffer_and_guards\":true,\"all_resources_drained\":true,\"performance_measurement\":false}}",
        !source && !partial,
        idle.completed_window_binds,
        idle.completed_window_invalidations
    );
    if partial && !source {
        partial::finish();
    }
}

fn source_mr_stats(idle: &RdmaStats, closed: &RdmaStats) {
    assert_eq!(
        (closed.persistent_source_mrs, closed.retained_source_mrs),
        (0, 0)
    );
    println!(
        "ORBITTE_SOURCE_MR_STATS {{\"idle_parents\":{},\"parents\":{},\"opened\":{},\"reused\":{},\"retained\":{}}}",
        idle.persistent_source_mrs,
        closed.persistent_source_mrs,
        closed.opened_source_mrs,
        closed.reused_source_mrs,
        closed.retained_source_mrs
    );
}

fn receive_stats(stats: &RdmaStats) {
    assert_eq!(
        (
            stats.receive_work_requests,
            stats.retained_receive_work_requests,
            stats.active_message_mrs,
            stats.retained_message_mrs,
            stats.message_bytes,
            stats.retained_message_bytes
        ),
        (0, 0, 0, 0, 0, 0)
    );
    println!(
        "ORBITTE_RECEIVE_STATS {{\"completed_sends\":{},\"completed_receives\":{},\"receive_post_calls\":{},\"peak_receive_work_requests\":{},\"receive_work_requests\":{},\"retained_receive_work_requests\":{},\"message_mrs\":{},\"retained_message_mrs\":{},\"message_bytes\":{},\"retained_message_bytes\":{}}}",
        stats.completed_sends,
        stats.completed_receives,
        stats.receive_post_calls,
        stats.peak_receive_work_requests,
        stats.receive_work_requests,
        stats.retained_receive_work_requests,
        stats.active_message_mrs,
        stats.retained_message_mrs,
        stats.message_bytes,
        stats.retained_message_bytes
    );
}

fn duplex_binding(
    seed: u64,
    source: bool,
    slot: usize,
    operation: TransferOp,
) -> [u8; COMPLETION_MESSAGE_SIZE] {
    let mut bytes = [0xa7; COMPLETION_MESSAGE_SIZE];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes[8] = u8::from(source);
    bytes[9] = slot as u8;
    bytes[10] = u8::from(operation == TransferOp::Write);
    bytes
}

fn duplex_completion_peer() {
    println!();
    let role = env::var("ORBITTE_SHARED_ROLE").unwrap();
    assert!(matches!(role.as_str(), "source" | "requester"));
    let source = role == "source";
    let payload: usize = env::var("ORBITTE_SHARED_PAYLOAD").unwrap().parse().unwrap();
    assert!((4096..=8 << 20).contains(&payload));
    let capacity: usize = env::var("ORBITTE_SHARED_CQES").unwrap().parse().unwrap();
    assert!((1..=4).contains(&capacity));
    let seed: u64 = env::var("ORBITTE_SHARED_SEED").unwrap().parse().unwrap();
    let stride = payload + 2 * GUARD;
    let bank = COUNT * stride;
    let length = 2 * bank;
    let role_seed = seed ^ if source { 0x1234_5678 } else { 0xfedc_ba98 };
    let peer_seed = seed ^ if source { 0xfedc_ba98 } else { 0x1234_5678 };
    let mut config = rdma_config();
    config.host_memory = RdmaHostMemory::Direct;
    config.max_endpoints = 2 * COUNT;
    config.max_direct_bytes = length;
    config.max_cached_mr_bytes = 2 * length;
    config.block_size = 4096;
    config.max_cqes = capacity;
    config.max_send_wr = 1;
    let runtime = RdmaTransport::new(config).unwrap();
    let registry = MemoryRegistry::new(SourceEpoch::generate().unwrap(), 1, length);
    let mut expected = vec![0xa5; length];
    for slot in 0..COUNT {
        for index in 0..payload {
            expected[slot * stride + GUARD + index] = value(role_seed, slot * stride + index);
            expected[bank + slot * stride + GUARD + index] =
                value(role_seed ^ 0x4567, slot * stride + index);
        }
    }
    let backing = Arc::new(HostMemory::new(expected.clone().into_boxed_slice()).unwrap());
    let memory = registry
        .register_host(Arc::clone(&backing), 0, length, Permission::RemoteReadWrite)
        .unwrap();
    let mut stream = if source {
        let listener = TcpListener::bind(env::var("ORBITTE_SHARED_BIND").unwrap()).unwrap();
        listener.set_nonblocking(true).unwrap();
        println!("ORBITTE_READY {}", listener.local_addr().unwrap());
        std::io::stdout().flush().unwrap();
        let end = Instant::now() + TIMEOUT;
        loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < end);
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("{e}"),
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
    stream.set_nodelay(true).unwrap();
    for operation in [TransferOp::Read, TransferOp::Write] {
        if operation == TransferOp::Write {
            for slot in 0..COUNT {
                let bytes = (0..payload)
                    .map(|index| value(role_seed ^ 0xaabb_ccdd, slot * stride + index))
                    .collect::<Vec<_>>();
                memory.write(bank + slot * stride + GUARD, &bytes).unwrap();
                expected[bank + slot * stride + GUARD..bank + slot * stride + GUARD + payload]
                    .copy_from_slice(&bytes);
            }
        }
        let mut sources = (0..COUNT)
            .map(|slot| {
                runtime
                    .endpoint(
                        &memory,
                        slot * stride + GUARD,
                        payload,
                        if operation == TransferOp::Read { 1 } else { 2 },
                        Instant::now() + TIMEOUT,
                        None,
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut requesters = (0..COUNT)
            .map(|slot| {
                runtime
                    .endpoint(
                        &memory,
                        bank + slot * stride + GUARD,
                        payload,
                        0,
                        Instant::now() + TIMEOUT,
                        None,
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for endpoint in sources.iter().chain(&requesters) {
            stream.write_all(&endpoint.connection()).unwrap();
        }
        let mut peer_connections = Vec::new();
        for _ in 0..2 * COUNT {
            let mut bytes = [0; crate::transport::rdma::DESCRIPTOR_SIZE];
            stream.read_exact(&mut bytes).unwrap();
            peer_connections.push(Descriptor::decode_connection(&bytes).unwrap());
        }
        for (slot, endpoint) in sources.iter_mut().enumerate() {
            endpoint.connect(&peer_connections[COUNT + slot]).unwrap();
        }
        for (slot, endpoint) in requesters.iter_mut().enumerate() {
            endpoint.connect(&peer_connections[slot]).unwrap();
        }
        std::fs::write(
            env::var("ORBITTE_SHARED_MAPS").unwrap(),
            std::fs::read("/proc/self/maps").unwrap(),
        )
        .unwrap();
        let mut grants = stream.try_clone().unwrap();
        // Each process is simultaneously a source and a requester on one runtime.
        // RQ capacity one forces incremental grants while SQ data keeps progressing.
        thread::scope(|scope| {
            let source_thread = scope.spawn(move || {
                thread::scope(|sources_scope| {
                    let mut handlers = Vec::new();
                    for (slot, mut endpoint) in sources.into_iter().enumerate() {
                        endpoint.bind_window().unwrap();
                        post_receive(
                            &mut endpoint,
                            &duplex_binding(seed, source, slot, operation),
                        )
                        .unwrap();
                        grants
                            .write_all(&endpoint.grant().unwrap().encode())
                            .unwrap();
                        handlers.push(sources_scope.spawn(move || {
                            endpoint.receive_completion().unwrap();
                            endpoint.close().unwrap();
                        }));
                    }
                    for handler in handlers {
                        handler.join().unwrap();
                    }
                });
            });
            let mut transfers = Vec::new();
            for (slot, mut endpoint) in requesters.into_iter().enumerate() {
                let mut bytes = [0; crate::transport::rdma::DESCRIPTOR_SIZE];
                stream.read_exact(&mut bytes).unwrap();
                let remote = Descriptor::decode(&bytes).unwrap();
                assert_eq!(
                    remote.connection_bytes(),
                    peer_connections[slot].connection_bytes()
                );
                transfers.push(scope.spawn(move || {
                    endpoint
                        .transfer(&remote, operation, Instant::now() + TIMEOUT)
                        .unwrap();
                    endpoint
                        .send_completion(
                            &duplex_binding(seed, !source, slot, operation),
                            remote.rkey,
                        )
                        .unwrap();
                    endpoint.close().unwrap();
                }));
            }
            for transfer in transfers {
                transfer.join().unwrap();
            }
            source_thread.join().unwrap();
        });
        // Both peers finish their source and requester owners before checking backing.
        stream.write_all(&[7]).unwrap();
        token(&mut stream, 7);
        for slot in 0..COUNT {
            let target_bank = if operation == TransferOp::Read {
                bank
            } else {
                0
            };
            let content_seed = if operation == TransferOp::Read {
                peer_seed
            } else {
                peer_seed ^ 0xaabb_ccdd
            };
            for index in 0..payload {
                expected[target_bank + slot * stride + GUARD + index] =
                    value(content_seed, slot * stride + index);
            }
        }
        verify(&memory, &expected);
    }
    let idle = runtime.stats().unwrap();
    let count = (2 * COUNT) as u64;
    assert_eq!(
        (
            idle.opened_cqs,
            idle.active_cqs,
            idle.progress_workers,
            idle.async_event_workers
        ),
        (2, 2, 1, 1)
    );
    assert_eq!(
        (
            idle.completed_window_binds,
            idle.completed_window_invalidations,
            idle.completed_sends,
            idle.completed_receives,
            idle.receive_post_calls
        ),
        (count, count, count, count, count)
    );
    assert!(idle.peak_work_requests > 0 && idle.peak_work_requests <= capacity);
    assert!(idle.peak_receive_work_requests > 0 && idle.peak_receive_work_requests <= capacity);
    assert_eq!(
        (idle.completed_read_bytes, idle.completed_write_bytes),
        ((COUNT * payload) as u64, (COUNT * payload) as u64)
    );
    assert_eq!(
        (
            idle.active_endpoints,
            idle.active_grants,
            idle.active_windows,
            idle.work_requests,
            idle.completion_errors,
            idle.progress_failures
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert_eq!(
        (
            idle.persistent_mrs,
            idle.opened_persistent_mrs,
            idle.reused_persistent_mrs
        ),
        (2, 2, 2 * count - 2)
    );
    assert_eq!(
        (
            idle.persistent_source_mrs,
            idle.opened_source_mrs,
            idle.reused_source_mrs
        ),
        (1, 1, count - 1)
    );
    assert_eq!(idle.persistent_mr_bytes, 2 * length);
    receive_stats(&idle);
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    drop(memory);
    drop(backing);
    runtime.close(Instant::now() + TIMEOUT).unwrap();
    let closed = runtime.stats().unwrap();
    record_completion_events(&idle, &closed, false);
    source_mr_stats(&idle, &closed);
    receive_stats(&closed);
    assert_eq!(
        (
            closed.active_devices,
            closed.active_cqs,
            closed.progress_workers,
            closed.async_event_workers,
            closed.active_endpoints,
            closed.active_grants,
            closed.persistent_mrs,
            closed.window_key_indices
        ),
        (0, 0, 0, 0, 0, 0, 0, 0)
    );
    assert_eq!(
        (
            closed.retained_devices,
            closed.retained_cqs,
            closed.retained_endpoints,
            closed.retained_grants,
            closed.retained_windows,
            closed.retained_work_requests
        ),
        (0, 0, 0, 0, 0, 0)
    );
    println!(
        "ORBITTE_OWNER_STATS {{\"idle_connections\":{},\"idle_grants\":{},\"connections\":{},\"grants\":{},\"retained_connections\":{},\"retained_grants\":{}}}",
        idle.active_endpoints,
        idle.active_grants,
        closed.active_endpoints,
        closed.active_grants,
        closed.retained_endpoints,
        closed.retained_grants
    );
    println!(
        "ORBITTE_KEY_STATS {{\"idle_indices\":{},\"indices\":{},\"burned_keys\":{},\"exhaustions\":{}}}",
        idle.window_key_indices,
        closed.window_key_indices,
        closed.burned_window_keys,
        closed.window_key_exhaustions
    );
    println!(
        "ORBITTE_ASYNC_STATS {{\"idle_workers\":{},\"workers\":{},\"acknowledged_events\":{}}}",
        idle.async_event_workers, closed.async_event_workers, closed.acknowledged_async_events
    );
    println!(
        "{{\"role\":\"{role}\",\"status\":\"passed\",\"duplex\":true,\"qps_per_phase\":{},\"cq_capacity\":{capacity},\"sq_window\":1,\"peak_work_requests\":{},\"peak_receive_work_requests\":{},\"peak_pending_qps\":{},\"opened_cqs\":{},\"completed_read_bytes\":{},\"completed_write_bytes\":{},\"completed_sends\":{},\"completed_receives\":{},\"full_buffer_and_guards\":true,\"all_resources_drained\":true,\"performance_measurement\":false}}",
        2 * COUNT,
        idle.peak_work_requests,
        idle.peak_receive_work_requests,
        idle.peak_pending_qps,
        idle.opened_cqs,
        idle.completed_read_bytes,
        idle.completed_write_bytes,
        idle.completed_sends,
        idle.completed_receives
    );
}

#[path = "connected.rs"]
mod connected;

#[path = "fragmented.rs"]
mod fragmented;

#[path = "partial.rs"]
mod partial;

#[path = "multi_context.rs"]
mod multi_context;

#[path = "multirail.rs"]
mod multirail;
