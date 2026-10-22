//! Explicit two-host shared-CQ correctness gate. The post gate delays dispatch;
//! all WRs, CQEs, MR registrations and byte movement use the native provider.
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::OnceLock;

use super::*;
use crate::memory::MemoryRegistry;
use crate::rdma::{RdmaHostMemory, registration};
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
    h: *mut sys::Handles,
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
            h, requests, count, local, length, remote, rkey, write, result,
        )
    }
}

static VERBS: sys::Verbs = sys::Verbs {
    asynchronous: sys::REAL.asynchronous,
    open_device: sys::REAL.open_device,
    create_cq: sys::REAL.create_cq,
    register_local: sys::REAL.register_local,
    open: sys::REAL.open,
    connect: sys::REAL.connect,
    post: gated_post,
    window: sys::REAL.window,
    dealloc_window: sys::REAL.dealloc_window,
    poll: sys::REAL.poll,
    destroy_qp: sys::REAL.destroy_qp,
    deregister: sys::REAL.deregister,
    destroy_cq: sys::REAL.destroy_cq,
    dealloc_pd: sys::REAL.dealloc_pd,
    close_device: sys::REAL.close_device,
};

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
    let mut config = RdmaConfig::new(env::var("ORBITTE_SHARED_DEVICE").unwrap());
    config.gid_index = env::var("ORBITTE_SHARED_GID").unwrap().parse().unwrap();
    config.hop_limit = 64;
    config.host_memory = RdmaHostMemory::Direct;
    config.max_endpoints = COUNT;
    config.max_direct_bytes = length;
    config.max_cached_mr_bytes = length;
    config.block_size = 4096;
    config.max_cqes = capacity;
    config.max_send_wr = window;
    let usage = Arc::default();
    let runtime = Runtime {
        registrations: registration::HostRegistrations::new(&config, Arc::clone(&usage), &VERBS),
        config,
        usage,
        verbs: &VERBS,
        device: Mutex::default(),
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
            let mut bytes = [0; crate::rdma::DESCRIPTOR_SIZE];
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
                let mut bytes = [0; crate::rdma::DESCRIPTOR_SIZE];
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
        (1, 1, 1)
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
        idle.completed_window_binds,
        if source { (2 * COUNT) as u64 } else { 0 }
    );
    assert_eq!(
        idle.completed_window_invalidations,
        idle.completed_window_binds
    );
    if !source {
        assert_eq!(idle.peak_work_requests, capacity);
        assert!(idle.peak_pending_qps >= capacity.div_ceil(window));
        assert_eq!(
            (
                idle.opened_persistent_mrs,
                idle.reused_persistent_mrs,
                idle.persistent_mrs
            ),
            (1, 7, 1)
        );
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
#[ignore = "requires selected physical RNIC hosts and original resource locks; intentional remote-access error"]
fn source_window_peer() {
    println!();
    let role = env::var("ORBITTE_SHARED_ROLE").unwrap();
    assert!(matches!(role.as_str(), "source" | "requester"));
    let source = role == "source";
    let probe = env::var("ORBITTE_WINDOW_PROBE").unwrap();
    assert!(matches!(probe.as_str(), "revoked" | "parent"));
    let length = 4096 + 2 * GUARD;
    let mut config = RdmaConfig::new(env::var("ORBITTE_SHARED_DEVICE").unwrap());
    config.gid_index = env::var("ORBITTE_SHARED_GID").unwrap().parse().unwrap();
    config.hop_limit = 64;
    config.max_cqes = 1;
    config.max_send_wr = 1;
    let runtime = Runtime::new(config).unwrap();
    let registry = MemoryRegistry::new(SourceEpoch::generate().unwrap(), 1, length);
    let expected = vec![if source { 0x5a } else { 0xc3 }; length];
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
            4096,
            if source { 2 } else { 0 },
            Instant::now() + TIMEOUT,
        )
        .unwrap();
    if source {
        stream.write_all(&endpoint.connection()).unwrap();
    }
    let mut bytes = [0; crate::rdma::DESCRIPTOR_SIZE];
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
        } else {
            // A deliberately hostile test peer receives the unadvertised parent key.
            // Production bootstrap never exposes either parent key or parent address.
            let request = endpoint.record.lock().unwrap();
            let h = &request.resources.as_ref().unwrap().handles;
            grant.rkey = unsafe { (*h.mr.cast::<ibverbs_sys::ibv_mr>()).rkey };
        }
        stream.write_all(&grant.encode()).unwrap();
        token(&mut stream, 4);
        // The peer has observed REM_ACCESS_ERR and physically closed its QP.
        let close = endpoint.close();
        assert!(!matches!(close, Err(Error::Unquiesced { .. })), "{close:?}");
        drop(endpoint);
        verify(&memory, &expected);
        stream.write_all(&[5]).unwrap();
    } else {
        stream.read_exact(&mut bytes).unwrap();
        let grant = Descriptor::decode(&bytes).unwrap();
        assert_eq!(grant.connection_bytes(), connection.connection_bytes());
        let error = endpoint
            .transfer(&grant, TransferOp::Write, Instant::now() + TIMEOUT)
            .unwrap_err();
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
        let close = endpoint.close();
        assert!(!matches!(close, Err(Error::Unquiesced { .. })), "{close:?}");
        drop(endpoint);
        verify(&memory, &expected);
        stream.write_all(&[4]).unwrap();
        token(&mut stream, 5);
    }
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
    assert_eq!(
        idle.completed_window_invalidations,
        u64::from(source && probe == "revoked")
    );
    assert_eq!(idle.completion_errors, u64::from(!source));
    assert_eq!(
        (idle.completed_write_bytes, idle.completed_read_bytes),
        (0, 0)
    );
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    drop(memory);
    drop(backing);
    runtime.close(Instant::now() + TIMEOUT).unwrap();
    let closed = runtime.stats().unwrap();
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
        !source, idle.completed_window_binds, idle.completed_window_invalidations
    );
}
