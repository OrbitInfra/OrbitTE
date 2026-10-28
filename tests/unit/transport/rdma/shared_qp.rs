//! Native grant multiplexing on one actual QP; the public handshake stays exclusive.
use super::*;
use crate::runtime::control_plane::{ControlChannel, Operation, Request};
use crate::transport::rdma::protocol::{Binding, DONE, Identity};

#[derive(Default)]
struct Trace {
    payload: Vec<(u32, u64)>,
    max_namespaces: usize,
    payload_polling: bool,
    closed_qps: usize,
    lost: bool,
    barrier_checks: usize,
    records: Vec<Arc<Record>>,
    memory: Option<crate::RegisteredMemory>,
}
static TRACE: OnceLock<Mutex<Trace>> = OnceLock::new();

unsafe extern "C" fn post(
    handles: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    requests: *const sys::WorkRequest,
    count: usize,
    local: u64,
    length: usize,
    remote: u64,
    rkey: u32,
    writing: i32,
    result: *mut sys::PostResult,
) -> i32 {
    let code = unsafe {
        gated_post(
            handles, memory, requests, count, local, length, remote, rkey, writing, result,
        )
    };
    assert_eq!(code, 0);
    assert_eq!(unsafe { (*result).accepted }, count);
    let qpn = unsafe { (*(*handles).qp.cast::<ibverbs_sys::ibv_qp>()).qp_num };
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    for request in unsafe { std::slice::from_raw_parts(requests, count) } {
        trace.payload.push((qpn, request.id));
    }
    let namespaces = trace
        .payload
        .iter()
        .map(|(_, id)| (id - 1) / 128)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    trace.max_namespaces = trace.max_namespaces.max(namespaces);
    code
}

unsafe extern "C" fn poll(
    handles: *mut sys::ConnectionHandles,
    completion: *mut sys::Completion,
) -> i32 {
    // Delay CQ consumption, never fabricate completion: all four actual payload
    // posts must coexist in the shared SQ accounting before any is discharged.
    if env::var("ORBITTE_SHARED_ROLE").as_deref() == Ok("requester") {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        if !trace.payload_polling {
            if trace.payload.len() != COUNT {
                return 0;
            }
            trace.payload_polling = true;
        }
    }
    let count = unsafe { (sys::REAL.poll)(handles, completion) };
    if count != 1 {
        return count;
    }
    let wc = unsafe { &*completion };
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    if wc.status == 0 && matches!(wc.opcode, 1 | 2) {
        let index = trace
            .payload
            .iter()
            .position(|entry| *entry == (wc.qpn, wc.id))
            .unwrap();
        trace.payload.swap_remove(index);
    }
    if wc.status == 0
        && wc.opcode == 128
        && env::var("ORBITTE_SHARED_QP").as_deref() == Ok("lost-receive")
        && !trace.lost
    {
        trace.lost = true;
        println!(
            "ORBITTE_SHARED_QP_LOST {{\"qpn\":{},\"wr_id\":{},\"actual_success_cqe\":true}}",
            wc.qpn, wc.id
        );
        return 0;
    }
    count
}

unsafe extern "C" fn destroy_qp(handles: *mut sys::ConnectionHandles) -> i32 {
    let existed = unsafe { !(*handles).qp.is_null() };
    let code = unsafe { (sys::REAL.destroy_qp)(handles) };
    if existed && code == 0 {
        TRACE.get().unwrap().lock().unwrap().closed_qps += 1;
    }
    code
}

unsafe extern "C" fn destroy_cq(handles: *mut sys::CqHandles) -> i32 {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    if trace.lost {
        assert_eq!(trace.closed_qps, 1);
        for record in &trace.records {
            let state = record.lock().unwrap();
            let grant = state.resources.as_ref().unwrap().grant().unwrap();
            let connection = record.connection.lock().unwrap();
            assert!(connection.closed);
            assert_eq!(connection.receive_queue.outstanding(), COUNT);
            assert_eq!(connection.receive_queue.credited(), COUNT);
            let offset = grant.buffer.address().as_ptr() as usize
                - trace.memory.as_ref().unwrap().base_address() as usize;
            assert!(
                trace
                    .memory
                    .as_ref()
                    .unwrap()
                    .write(offset, &[0xff])
                    .is_err()
            );
        }
        trace.barrier_checks += trace.records.len();
    }
    drop(trace);
    unsafe { (sys::REAL.destroy_cq)(handles) }
}

static SHARED_VERBS: sys::Verbs = sys::Verbs {
    post,
    poll,
    destroy_qp,
    destroy_cq,
    ..sys::REAL
};

fn attach(
    runtime: &RdmaTransport,
    parent: &RdmaGrant,
    memory: &crate::RegisteredMemory,
    offset: usize,
    length: usize,
    permission: u32,
    end: Instant,
) -> RdmaGrant {
    let record = parent
        .progress
        .register(Box::new(Resources {
            connection: Arc::clone(&parent.record.connection),
            grant: None,
        }))
        .unwrap();
    let placeholder = RdmaGrant {
        record,
        progress: Arc::clone(&parent.progress),
        local: parent.local,
        incarnation: parent.incarnation,
        config: runtime.config.clone(),
        deadline: end,
        output: None,
        claim: 1,
        released: false,
        cached_peer: None,
        endpoint: None,
        construction: None,
    };
    runtime
        .endpoint(memory, offset, length, permission, end, Some(placeholder))
        .unwrap()
}

pub(super) fn run() {
    println!();
    TRACE.get_or_init(Default::default);
    let source = env::var("ORBITTE_SHARED_ROLE").unwrap() == "source";
    let lost = env::var("ORBITTE_SHARED_QP").unwrap() == "lost-receive";
    let writing = env::var("ORBITTE_SHARED_OPERATION").unwrap() == "write";
    let operation = if writing {
        TransferOp::Write
    } else {
        TransferOp::Read
    };
    let permission = if source {
        if writing { 2 } else { 1 }
    } else {
        0
    };
    let payload: usize = env::var("ORBITTE_SHARED_PAYLOAD").unwrap().parse().unwrap();
    assert!((4096..=8 << 20).contains(&payload));
    let stride = payload + 2 * GUARD;
    let length = COUNT * stride;
    let seed: u64 = env::var("ORBITTE_SHARED_SEED").unwrap().parse().unwrap();
    let epoch = SourceEpoch([if source { 1 } else { 2 }; 16]);
    let mut config = rdma_config();
    config.host_memory = RdmaHostMemory::Direct;
    config.max_endpoints = COUNT;
    config.max_send_wr = COUNT;
    config.max_cqes = COUNT;
    config.block_size = payload;
    config.max_direct_bytes = length;
    config.max_cached_mr_bytes = length;
    let mut runtime = RdmaTransport::new(
        config,
        #[cfg(feature = "cuda")]
        crate::transport::rdma::tests::proxy(),
    )
    .unwrap();
    // Use the normal owners and native provider with observable post/poll/close boundaries.
    let state = Arc::get_mut(&mut runtime).unwrap();
    state.verbs = &SHARED_VERBS;
    state.registrations =
        buffers::LocalBufferManager::new(&state.config, Arc::clone(&state.usage), &SHARED_VERBS);
    let registry = MemoryRegistry::new(epoch, 1, length);
    let mut expected = vec![0xa5; length];
    if source != writing {
        for slot in 0..COUNT {
            for (index, byte) in expected
                .iter_mut()
                .enumerate()
                .skip(slot * stride + GUARD)
                .take(payload)
            {
                *byte = value(seed, index);
            }
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
    let end = Instant::now() + TIMEOUT;
    let mut grants = vec![
        runtime
            .endpoint(&memory, GUARD, payload, permission, end, None)
            .unwrap(),
    ];
    for slot in 1..COUNT {
        grants.push(attach(
            &runtime,
            &grants[0],
            &memory,
            slot * stride + GUARD,
            payload,
            permission,
            end,
        ));
    }
    assert_eq!(runtime.stats().unwrap().opened_connections, 1);
    assert_eq!(runtime.stats().unwrap().active_grants, COUNT);
    for grant in &grants {
        assert!(Arc::ptr_eq(
            &grant.record.connection,
            &grants[0].record.connection
        ));
    }
    let local = Identity::local(&grants[0], epoch).unwrap();
    stream.write_all(&local.encode()).unwrap();
    stream.write_all(&grants[0].connection()).unwrap();
    let mut remote_identity = [0; 40];
    let mut remote_connection = [0; 40];
    stream.read_exact(&mut remote_identity).unwrap();
    stream.read_exact(&mut remote_connection).unwrap();
    let peer = Identity::decode(&remote_identity).unwrap();
    let remote = Descriptor::decode_connection(&remote_connection).unwrap();
    for grant in &mut grants {
        grant.connect(&remote).unwrap();
    }
    let mut bindings = Vec::new();
    let mut descriptors = Vec::new();
    for (slot, grant) in grants.iter_mut().enumerate() {
        if source {
            grant.bind_window().unwrap();
            let binding = Binding::new(
                &Request {
                    operation: if writing {
                        Operation::RdmaWrite
                    } else {
                        Operation::RdmaRead
                    },
                    epoch,
                    region: 1,
                    offset: (slot * stride + GUARD) as u64,
                    length: payload as u64,
                },
                local,
                peer,
                grant.grant_generation().unwrap(),
                1,
            )
            .unwrap();
            super::super::post_receive(grant, &binding.0).unwrap();
            stream.write_all(&binding.0).unwrap();
            stream.write_all(&grant.grant().unwrap().encode()).unwrap();
            bindings.push(binding);
        } else {
            let mut binding = [0; COMPLETION_MESSAGE_SIZE];
            let mut descriptor = [0; 40];
            stream.read_exact(&mut binding).unwrap();
            stream.read_exact(&mut descriptor).unwrap();
            bindings.push(Binding(binding));
            descriptors.push(Descriptor::decode(&descriptor).unwrap());
        }
    }
    if source && lost {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        trace.records = grants.iter().map(|g| Arc::clone(&g.record)).collect();
        trace.memory = Some(memory.clone());
    }
    if !source {
        *GATE.get_or_init(Default::default).0.lock().unwrap() = DispatchGate {
            armed: true,
            ..Default::default()
        };
        for (grant, descriptor) in grants.iter().zip(&descriptors) {
            grant
                .progress
                .submit(&grant.record, grant.claim, *descriptor, operation, end)
                .unwrap();
        }
        let (lock, changed) = GATE.get().unwrap();
        let (mut gate, timed) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |gate| {
                !gate.entered
            })
            .unwrap();
        assert!(!timed.timed_out());
        gate.released = true;
        changed.notify_all();
        drop(gate);
        for grant in &grants {
            grant
                .progress
                .wait(&grant.record, grant.claim, end)
                .unwrap();
        }
        assert_eq!(TRACE.get().unwrap().lock().unwrap().max_namespaces, COUNT);
    }
    let mut channel = ControlChannel {
        stream,
        deadline: end,
        stopping: None,
    };
    if lost {
        if !source {
            grants[COUNT - 1]
                .send_completion(&bindings[COUNT - 1].0, descriptors[COUNT - 1].rkey)
                .unwrap();
            grants[COUNT - 1]
                .finish_sent(&bindings[COUNT - 1].0)
                .unwrap();
            channel.read_exact(&mut [0]).unwrap();
        } else {
            while !TRACE.get().unwrap().lock().unwrap().lost {
                assert!(Instant::now() < end);
                thread::yield_now();
            }
            assert!(grants[0].close().is_err());
            assert_eq!(
                TRACE.get().unwrap().lock().unwrap().barrier_checks,
                2 * COUNT
            );
            channel.write_all(&[1]).unwrap();
        }
    } else {
        for slot in (0..COUNT).rev() {
            let pair = crate::transport::rdma::endpoint::Peer {
                route: if source {
                    None
                } else {
                    Some(channel.stream.peer_addr().unwrap())
                },
                source_nic_id: 0,
                epoch: peer.epoch,
                local: local.connection,
                connection: peer.connection,
                operation,
                source,
                generation: crate::runtime::control_plane::u64_at(
                    &bindings[slot].0,
                    COMPLETION_MESSAGE_SIZE - 9,
                ),
            };
            if grants[slot].endpoint.is_none() {
                let device = grants[slot]
                    .record
                    .connection
                    .lock()
                    .unwrap()
                    .device
                    .clone();
                let owner = runtime
                    .context_set
                    .lock()
                    .unwrap()
                    .slots
                    .iter_mut()
                    .find(|entry| {
                        entry
                            .owner
                            .as_ref()
                            .is_some_and(|d| Arc::ptr_eq(d, &device))
                    })
                    .unwrap()
                    .endpoints
                    .get_or_insert(pair.into(), 1)
                    .unwrap();
                if let Some(construction) = owner.reserve(Some(pair.connection)).unwrap() {
                    construction.attach(&mut grants[slot]).unwrap();
                    grants[slot].construction = Some(construction);
                } else {
                    grants[slot].endpoint = Some(owner);
                }
            }
            if source {
                let done = bindings[slot].done(&mut channel).unwrap();
                grants[slot].receive_completion().unwrap();
                grants[slot].finish_received(&done).unwrap();
                assert_eq!(runtime.stats().unwrap().active_grants, slot);
                assert_eq!(runtime.stats().unwrap().receive_work_requests, slot);
                for pending in 0..slot {
                    assert!(memory.write(pending * stride + GUARD, &[0xff]).is_err());
                }
                let mut released = vec![0; payload];
                memory.read(slot * stride + GUARD, &mut released).unwrap();
                grants[slot].publish_peer(pair).unwrap();
                grants[slot].cache(pair).unwrap();
                if slot + 1 < COUNT {
                    let stale = &grants[COUNT - 1];
                    stale.retire_idle().unwrap();
                    stale
                        .progress
                        .close_record(&stale.record, stale.claim, end)
                        .unwrap();
                    grants[slot].record.require_live().unwrap();
                }
                channel.write_all(&[1]).unwrap();
            } else {
                grants[slot]
                    .send_completion(&bindings[slot].0, descriptors[slot].rkey)
                    .unwrap();
                grants[slot].finish_sent(&bindings[slot].0).unwrap();
                bindings[slot].write(&mut channel, DONE).unwrap();
                let mut acknowledgement = [0];
                channel.read_exact(&mut acknowledgement).unwrap();
                assert_eq!(acknowledgement, [1]);
                grants[slot].publish_peer(pair).unwrap();
                grants[slot].cache(pair).unwrap();
            }
        }
        assert_eq!(runtime.stats().unwrap().idle_endpoints, 1);
        assert_eq!(runtime.stats().unwrap().active_endpoints, 1);
    }
    for grant in &mut grants {
        let _ = grant.close();
    }
    drop(grants);
    {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        trace.records.clear();
        trace.memory = None;
    }
    if source == writing {
        for slot in 0..COUNT {
            for (index, byte) in expected
                .iter_mut()
                .enumerate()
                .skip(slot * stride + GUARD)
                .take(payload)
            {
                *byte = value(seed, index);
            }
        }
    }
    verify(&memory, &expected);
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    drop((memory, backing));
    runtime.close(Instant::now() + TIMEOUT).unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_endpoints,
            stats.active_grants,
            stats.work_requests,
            stats.receive_work_requests
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(
        (
            stats.active_devices,
            stats.active_cqs,
            stats.active_message_mrs,
            stats.message_bytes
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(
        (
            stats.retained_grants,
            stats.retained_cqs,
            stats.retained_work_requests
        ),
        (0, 0, 0)
    );
    assert_eq!(stats.closed_connections, 1);
    assert_eq!(
        stats.completed_receives,
        if source && !lost { COUNT as u64 } else { 0 }
    );
    let trace = TRACE.get().unwrap().lock().unwrap();
    assert_eq!(trace.closed_qps, 1);
    println!(
        "ORBITTE_SHARED_QP_RESULT {{\"source\":{source},\"writing\":{writing},\"lost_receive\":{lost},\"grants\":{COUNT},\"physical_qps\":1,\"max_payload_namespaces\":{},\"barrier_loan_checks\":{},\"completed_receives\":{},\"full_buffer_and_guards\":true,\"all_resources_drained\":true,\"public_handshake_multiplexed\":false}}",
        trace.max_namespaces, trace.barrier_checks, stats.completed_receives
    );
}
