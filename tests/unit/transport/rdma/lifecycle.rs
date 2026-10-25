//! Native cleanup faults and reconnect with both peer processes kept alive.
use super::*;
use ibverbs_sys as ffi;
use std::ffi::c_int;
use std::net::SocketAddr;

#[derive(Default)]
struct Trace {
    mode: String,
    target: bool,
    armed: bool,
    rail: String,
    base: u64,
    injections: usize,
    attempts: usize,
    error: i32,
    mr: usize,
    mw: usize,
    pd: usize,
    context: usize,
    qp_errors: usize,
}

static TRACE: OnceLock<Mutex<Trace>> = OnceLock::new();

fn matches(context: *mut ffi::ibv_context, trace: &Trace) -> bool {
    let name = unsafe { ffi::ibv_get_device_name((*context).device) };
    unsafe { std::ffi::CStr::from_ptr(name) }.to_str().unwrap() == trace.rail
}

fn deny_commands(fd: c_int) {
    // Linux seccomp_data: nr at 0, args[0] at 16. This filter affects only
    // this calling thread and only write/ioctl on the selected uverbs FD.
    let mut instructions = [
        libc::sock_filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 1,
            jf: 0,
            k: libc::SYS_write as u32,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 3,
            k: libc::SYS_ioctl as u32,
        },
        libc::sock_filter {
            code: 0x20,
            jt: 0,
            jf: 0,
            k: 16,
        },
        libc::sock_filter {
            code: 0x15,
            jt: 0,
            jf: 1,
            k: fd.try_into().unwrap(),
        },
        libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: 0x0005_0000 | libc::EIO as u32,
        },
        libc::sock_filter {
            code: 0x06,
            jt: 0,
            jf: 0,
            k: 0x7fff_0000,
        },
    ];
    let program = libc::sock_fprog {
        len: instructions.len().try_into().unwrap(),
        filter: instructions.as_mut_ptr(),
    };
    assert_eq!(
        unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
        0
    );
    assert_eq!(unsafe { libc::prctl(libc::PR_SET_SECCOMP, 2, &program) }, 0);
}

unsafe extern "C" fn deregister(h: *mut sys::MemoryHandles) -> c_int {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    let mr = unsafe { (*h).mr.cast::<ffi::ibv_mr>() };
    if trace.mode != "mr-cleanup-error" || !trace.target || mr.is_null() {
        return unsafe { (sys::REAL.deregister)(h) };
    }
    assert!(
        trace.mr == 0 || trace.mr != mr as usize,
        "retained MR cleanup was retried"
    );
    let native = unsafe { &*mr };
    if !trace.armed
        || trace.injections != 0
        || native.addr as u64 != trace.base
        || !matches(native.context, &trace)
    {
        return unsafe { (sys::REAL.deregister)(h) };
    }
    trace.mr = mr as usize;
    trace.pd = native.pd as usize;
    trace.context = native.context as usize;
    deny_commands(unsafe { (*native.context).cmd_fd });
    trace.injections += 1;
    trace.attempts += 1;
    trace.error = unsafe { (sys::REAL.deregister)(h) };
    assert_eq!(trace.error, libc::EIO);
    assert_eq!(unsafe { (*h).mr } as usize, trace.mr);
    println!(
        "ORBITTE_COMMAND_ERROR {{\"kind\":\"mr\",\"code\":{},\"attempts\":{},\"mr\":{},\"pd\":{},\"syscall_fault_injection\":true}}",
        trace.error, trace.attempts, trace.mr, trace.pd
    );
    trace.error
}

unsafe extern "C" fn dealloc_window(h: *mut sys::MemoryHandles) -> c_int {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    let mw = unsafe { (*h).mw.cast::<ffi::ibv_mw>() };
    if trace.mode != "mw-cleanup-error" || !trace.target || mw.is_null() {
        return unsafe { (sys::REAL.dealloc_window)(h) };
    }
    assert!(
        trace.mw == 0 || trace.mw != mw as usize,
        "retained MW cleanup was retried"
    );
    let native = unsafe { &*mw };
    if !trace.armed || trace.injections != 0 || !matches(native.context, &trace) {
        return unsafe { (sys::REAL.dealloc_window)(h) };
    }
    trace.mr = unsafe { (*h).mr } as usize;
    trace.mw = mw as usize;
    trace.pd = native.pd as usize;
    trace.context = native.context as usize;
    deny_commands(unsafe { (*native.context).cmd_fd });
    trace.injections += 1;
    trace.attempts += 1;
    trace.error = unsafe { (sys::REAL.dealloc_window)(h) };
    assert_eq!(trace.error, libc::EIO);
    assert_eq!(unsafe { (*h).mw } as usize, trace.mw);
    println!(
        "ORBITTE_COMMAND_ERROR {{\"kind\":\"mw\",\"code\":{},\"attempts\":{},\"mr\":{},\"mw\":{},\"pd\":{},\"syscall_fault_injection\":true}}",
        trace.error, trace.attempts, trace.mr, trace.mw, trace.pd
    );
    trace.error
}

unsafe extern "C" fn post(
    h: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    requests: *const sys::WorkRequest,
    count: usize,
    local: u64,
    length: usize,
    remote: u64,
    rkey: u32,
    write: c_int,
    result: *mut sys::PostResult,
) -> c_int {
    {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        let qp = unsafe { (*h).qp.cast::<ffi::ibv_qp>() };
        if trace.mode == "live-reconnect"
            && trace.target
            && trace.armed
            && matches(unsafe { (*qp).context }, &trace)
        {
            trace.armed = false;
            let mut attr = ffi::ibv_qp_attr {
                qp_state: ffi::ibv_qp_state::IBV_QPS_ERR,
                ..Default::default()
            };
            assert_eq!(
                unsafe {
                    ffi::ibv_modify_qp(
                        qp,
                        &mut attr,
                        ffi::ibv_qp_attr_mask::IBV_QP_STATE.0 as c_int,
                    )
                },
                0
            );
            trace.qp_errors += 1;
        }
    }
    unsafe {
        (sys::REAL.post)(
            h, memory, requests, count, local, length, remote, rkey, write, result,
        )
    }
}

unsafe extern "C" fn poll(h: *mut sys::ConnectionHandles, out: *mut sys::Completion) -> i32 {
    unsafe {
        worker_pool::observe(h);
        (sys::REAL.poll)(h, out)
    }
}

static VERBS: sys::Verbs = sys::Verbs {
    poll,
    deregister,
    dealloc_window,
    post,
    ..sys::REAL
};

fn engine(rails: &[String], bind: SocketAddr) -> TransferEngine {
    worker_pool::reset();
    let config = crate::transport::rdma::RdmaConfig {
        rdma_whitelist: rails.to_vec(),
        num_lanes: worker_pool::count(),
        port: Some(1),
        gid_index: Some(env::var("ORBITTE_SHARED_GID").unwrap().parse().unwrap()),
        host_memory: RdmaHostMemory::Direct,
        device_mask: 3,
        progress_mode: if env::var("ORBITTE_RDMA_EVENTS").as_deref() == Ok("1") {
            RdmaProgressMode::EventDriven
        } else {
            RdmaProgressMode::Polling
        },
        block_size: 2048,
        max_send_wr: 2,
        max_cqes: 8,
        max_endpoints: 16,
        max_cached_mrs: 8,
        max_cached_mr_bytes: 8 * LENGTH,
        max_direct_bytes: 4 * PAYLOAD,
        hop_limit: 64,
        ..Default::default()
    };
    let Ok(mut runtime) = Arc::try_unwrap(RdmaTransport::new(config).unwrap()) else {
        panic!("one runtime owner")
    };
    runtime.verbs = &VERBS;
    runtime.registrations =
        buffers::LocalBufferManager::new(&runtime.config, Arc::clone(&runtime.usage), &VERBS);
    rdma_fixture_with_config(
        runtime,
        EngineConfig {
            bind_addr: bind,
            handler_timeout: Duration::from_secs(4),
            handler_threads: 4,
            ..Default::default()
        },
    )
}

fn wait(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(12);
    while !ready() {
        assert!(Instant::now() < deadline, "live peer did not drain");
        thread::sleep(Duration::from_millis(1));
    }
}

type Snapshot = Vec<(u64, [u8; 24], usize, u32)>;

fn snapshot(engine: &TransferEngine) -> Snapshot {
    let runtime = source_runtime(engine);
    let contexts = runtime.context_set.lock().unwrap();
    let mut rows = Vec::new();
    for progress in contexts
        .slots
        .iter()
        .flat_map(|slot| slot.cq_list.iter().flatten())
    {
        let mut records = Vec::new();
        progress
            .shared
            .control
            .lock()
            .unwrap()
            .qps
            .snapshot(&mut records);
        for record in records {
            let state = record.lock().unwrap();
            let connection = &state.resources.as_ref().unwrap().connection;
            assert!(
                state.phase == Phase::Ready && state.resources.as_ref().unwrap().grant.is_none()
            );
            let native = unsafe { &*connection.handles.qp.cast::<ffi::ibv_qp>() };
            rows.push((
                record.tag,
                connection.incarnation.unwrap().encode(),
                native.pd as usize,
                native.qp_num,
            ));
        }
    }
    rows.sort_unstable();
    rows
}

fn domains(engine: &TransferEngine) -> Vec<[u8; 16]> {
    source_runtime(engine)
        .context_set
        .lock()
        .unwrap()
        .slots
        .iter()
        .map(|slot| slot.owner.as_ref().unwrap().domain().unwrap())
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn round(
    stream: &mut TcpStream,
    engine: &TransferEngine,
    memory: &crate::RegisteredMemory,
    peer: Option<&crate::RemoteSegment>,
    remote: u64,
    source: bool,
    sequence: usize,
    fault: bool,
    retained: bool,
    operation: TransferOp,
) {
    let original = bytes(if source {
        100 + sequence as u64
    } else {
        200 + sequence as u64
    });
    let incoming = bytes(if source {
        200 + sequence as u64
    } else {
        100 + sequence as u64
    });
    memory.write(0, &original).unwrap();
    super::super::connected::barrier(stream, 0x11);
    if !source {
        let report = engine
            .submit_and_wait(
                operation,
                peer.unwrap(),
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: memory.base_address()
                        + if operation == TransferOp::Read {
                            DESTINATION
                        } else {
                            GUARD
                        } as u64,
                    remote_address: remote
                        + if operation == TransferOp::Read {
                            GUARD
                        } else {
                            DESTINATION
                        } as u64,
                    length: PAYLOAD,
                }],
                Duration::from_secs(5),
            )
            .unwrap();
        if fault {
            assert!(
                !report.is_complete(),
                "failed transfer was replayed: {report:?}"
            );
        } else {
            assert!(report.is_complete(), "{report:?}");
        }
        assert!(report.locally_drained, "{report:?}");
        println!(
            "ORBITTE_LIVE_REPORT {{\"sequence\":{sequence},\"complete\":{},\"drained\":{},\"effect\":\"{:?}\"}}",
            report.is_complete(),
            report.locally_drained,
            report.failure.as_ref().map(|failure| failure.remote_effect)
        );
    }
    super::super::connected::barrier(stream, 0x12);
    if !(source && retained) {
        wait(|| {
            let stats = engine.stats().unwrap();
            stats.active_source_operations == 0 && stats.inflight_batches == 0
        });
        let stats = engine.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.work_requests,
                stats.receive_work_requests,
                stats.receive_credits,
                stats.active_grants
            ),
            (0, 0, 0, 0)
        );
        let mut actual = vec![0; LENGTH];
        memory.read(0, &mut actual).unwrap();
        for index in 0..LENGTH {
            if source == (operation == TransferOp::Write)
                && (DESTINATION..DESTINATION + PAYLOAD).contains(&index)
            {
                let expected = incoming[GUARD + index - DESTINATION];
                if fault {
                    assert!(actual[index] == original[index] || actual[index] == expected);
                } else {
                    assert_eq!(actual[index], expected, "payload byte {index}");
                }
            } else {
                assert_eq!(
                    actual[index], original[index],
                    "sentinel/source byte {index}"
                );
            }
        }
    }
    super::super::connected::barrier(stream, 0x13);
}

fn retain(
    engine: TransferEngine,
    memory: crate::RegisteredMemory,
    backing: Arc<HostMemory>,
    mode: &str,
) -> std::sync::Weak<HostMemory> {
    let runtime = source_runtime(&engine);
    let weak = Arc::downgrade(&backing);
    let before = engine.stats().unwrap().rdma.unwrap();
    assert_eq!(TRACE.get().unwrap().lock().unwrap().error, libc::EIO);
    if mode == "mr-cleanup-error" {
        assert_eq!(before.retained_persistent_mrs, 1);
        assert_eq!(
            (
                before.work_requests,
                before.receive_work_requests,
                before.active_grants
            ),
            (0, 0, 0)
        );
    } else {
        assert!(before.retained_windows > 0 && before.retained_grants > 0);
        let loads = runtime.workers.inflight_slices();
        assert_eq!(
            loads.iter().sum::<usize>(),
            before.active_grants,
            "quarantined MW grant returned its slice load"
        );
        println!(
            "ORBITTE_RETAINED_WORKER_LOAD {{\"grants\":{},\"loads\":{loads:?}}}",
            before.active_grants
        );
        let mut blocked = 0;
        for offset in (GUARD..GUARD + PAYLOAD)
            .step_by(2048)
            .chain((DESTINATION..DESTINATION + PAYLOAD).step_by(2048))
        {
            blocked += usize::from(matches!(
                memory.write(offset, &[0xff]),
                Err(Error::Unavailable(_))
            ));
        }
        assert!(blocked > 0, "failed MW cleanup lost the actual DMA loan");
    }
    let retained = || {
        runtime
            .context_set
            .lock()
            .unwrap()
            .slots
            .iter()
            .enumerate()
            .flat_map(|(id, slot)| {
                slot.cq_list.iter().flatten().filter_map(move |progress| {
                    progress
                        .shared
                        .control
                        .lock()
                        .unwrap()
                        .retained
                        .map(|_| (id, Arc::as_ptr(progress) as usize))
                })
            })
            .collect::<Vec<_>>()
    };
    let owners = retained();
    runtime.recover_progress().unwrap();
    assert_eq!(retained(), owners, "unproven native owner was recovered");
    let recoveries = runtime.stats().unwrap().progress_recoveries;
    if mode == "mw-cleanup-error" {
        assert!(!owners.is_empty());
        assert!(before.progress_failures > recoveries);
    }
    for _ in 0..2 {
        assert!(
            engine.close(Duration::from_millis(50)).is_err(),
            "retained engine falsely drained"
        );
    }
    drop(memory);
    drop(backing);
    drop(engine);
    assert!(
        weak.upgrade().is_some(),
        "observer references masked a lost native backing owner"
    );
    let trace = TRACE.get().unwrap().lock().unwrap();
    assert_eq!(
        (trace.injections, trace.attempts, trace.error),
        (1, 1, libc::EIO)
    );
    println!(
        "ORBITTE_MEMORY_RETENTION {{\"mode\":\"{mode}\",\"attempts\":1,\"code\":5,\"backing_retained\":true,\"observer_owners\":0,\"post_drop\":true,\"retained_owner_recovered\":false,\"progress_recoveries\":{recoveries}}}"
    );
    weak
}

pub(super) fn run(mode: &str, source: bool, rails: &[String]) {
    let fault_rail: usize = env::var("ORBITTE_RAIL_FAULT").unwrap().parse().unwrap();
    let role = env::var("ORBITTE_RAIL_FAULT_ROLE").unwrap();
    let target = source == (role == "source");
    *TRACE.get_or_init(Default::default).lock().unwrap() = Trace {
        mode: mode.into(),
        target,
        rail: rails[fault_rail].clone(),
        ..Default::default()
    };
    let operation = if env::var("ORBITTE_RAIL_OPERATION").as_deref() == Ok("read") {
        TransferOp::Read
    } else {
        TransferOp::Write
    };
    let bind = env::var("ORBITTE_SHARED_BIND").unwrap().parse().unwrap();
    let mut engine = engine(rails, bind);
    let mut backing = Arc::new(HostMemory::new(vec![0xa5; LENGTH].into_boxed_slice()).unwrap());
    let mut memory = engine
        .register_host_memory(Arc::clone(&backing), 0, LENGTH, Permission::RemoteReadWrite)
        .unwrap();
    topology(&engine, rails);
    let pid = std::process::id();
    let persistent_mrs = engine.stats().unwrap().rdma.unwrap().opened_persistent_mrs;
    let endpoint = engine.endpoint();
    let epoch = engine.source_epoch();
    let region = memory.id();
    let original_domains = domains(&engine);
    let mut stream = control(source);
    let (remote_endpoint, remote_epoch, mut remote) =
        super::super::connected::exchange(&mut stream, &engine, &memory);
    let mut peer = if source {
        None
    } else {
        Some(
            engine
                .open_segment(remote_endpoint, remote_epoch, TIMEOUT)
                .unwrap(),
        )
    };
    let mut healthy: Option<Snapshot> = None;
    if mode == "live-reconnect" {
        for sequence in 0..7 {
            let fault = matches!(sequence, 1 | 4);
            TRACE.get().unwrap().lock().unwrap().armed = fault;
            round(
                &mut stream,
                &engine,
                &memory,
                peer.as_ref(),
                remote,
                source,
                sequence,
                fault,
                false,
                operation,
            );
            assert_eq!(
                (
                    std::process::id(),
                    engine.endpoint(),
                    engine.source_epoch(),
                    memory.id(),
                    domains(&engine)
                ),
                (pid, endpoint, epoch, region, original_domains.clone())
            );
            worker_pool::running(&engine);
            assert_eq!(
                engine.stats().unwrap().rdma.unwrap().opened_persistent_mrs,
                persistent_mrs,
                "reconnect replaced persistent registration owners"
            );
            if !fault {
                let rows = snapshot(&engine);
                let json = rows
                    .iter()
                    .map(|row| format!("[{}, {:?}, {}, {}]", row.0, row.1, row.2, row.3))
                    .collect::<Vec<_>>()
                    .join(",");
                println!(
                    "ORBITTE_LIVE_CONNECTIONS {{\"sequence\":{sequence},\"source\":{source},\"connections\":[{json}]}}"
                );
                assert!(!rows.is_empty());
                if matches!(sequence, 3 | 6) {
                    assert_eq!(
                        healthy.as_ref().unwrap(),
                        &rows,
                        "reconnected QPs were not reused"
                    );
                }
                if matches!(sequence, 2 | 5) {
                    let old = healthy.as_ref().unwrap();
                    assert!(
                        rows.iter().any(|row| !old
                            .iter()
                            .any(|previous: &(u64, [u8; 24], usize, u32)| previous.1 == row.1)),
                        "faulted connection incarnation was reused"
                    );
                }
                healthy = Some(rows);
            }
        }
        let stats = engine.stats().unwrap().rdma.unwrap();
        if !source {
            assert_eq!(TRACE.get().unwrap().lock().unwrap().qp_errors, 2);
            assert!(stats.progress_recoveries >= 2);
        }
        println!(
            "ORBITTE_LIVE_RECOVERY {{\"source\":{source},\"pid\":{},\"endpoint\":\"{endpoint}\",\"epoch\":\"{epoch}\",\"progress_failures\":{},\"progress_recoveries\":{},\"same_pd\":true,\"same_registration\":true,\"fault_cycles\":2,\"healthy_rounds\":5}}",
            std::process::id(),
            stats.progress_failures,
            stats.progress_recoveries
        );
    } else {
        TRACE.get().unwrap().lock().unwrap().base = memory.base_address();
        round(
            &mut stream,
            &engine,
            &memory,
            peer.as_ref(),
            remote,
            source,
            0,
            false,
            false,
            operation,
        );
        TRACE.get().unwrap().lock().unwrap().armed = true;
        round(
            &mut stream,
            &engine,
            &memory,
            peer.as_ref(),
            remote,
            source,
            1,
            mode == "mw-cleanup-error",
            mode == "mw-cleanup-error",
            operation,
        );
        let retained = if target {
            if mode == "mr-cleanup-error" {
                assert!(matches!(
                    thread::scope(|scope| scope
                        .spawn(|| engine.unregister_memory(&memory))
                        .join()
                        .unwrap()),
                    Err(Error::Unquiesced {
                        operation: "ibv_dereg_mr",
                        code: libc::EIO
                    })
                ));
            }
            Some(retain(engine, memory, backing, mode))
        } else {
            engine.unregister_memory(&memory).unwrap();
            engine.close(TIMEOUT).unwrap();
            drop(memory);
            drop(backing);
            drop(engine);
            None
        };
        super::super::connected::barrier(&mut stream, 0x21);
        engine = self::engine(rails, endpoint);
        backing = Arc::new(HostMemory::new(vec![0xa5; LENGTH].into_boxed_slice()).unwrap());
        memory = engine
            .register_host_memory(Arc::clone(&backing), 0, LENGTH, Permission::RemoteReadWrite)
            .unwrap();
        assert_eq!(std::process::id(), pid);
        assert_eq!(engine.endpoint(), endpoint);
        assert_eq!(memory.id(), region);
        assert_ne!(engine.source_epoch(), epoch);
        assert_ne!(domains(&engine), original_domains);
        let (next_endpoint, next_epoch, address) =
            super::super::connected::exchange(&mut stream, &engine, &memory);
        remote = address;
        super::super::connected::barrier(&mut stream, 0x22);
        if !source {
            assert_eq!(next_endpoint, remote_endpoint);
            assert_ne!(next_epoch, remote_epoch);
            assert!(matches!(
                engine.open_segment(remote_endpoint, remote_epoch, TIMEOUT),
                Err(Error::EpochMismatch)
            ));
            let old = peer.as_ref().unwrap();
            let stale = engine
                .submit_and_wait(
                    operation,
                    old,
                    &[TransferRequest {
                        transport: Transport::Rdma,
                        local_address: memory.base_address() + GUARD as u64,
                        remote_address: old.regions()[0].base_address() + GUARD as u64,
                        length: PAYLOAD,
                    }],
                    Duration::from_secs(5),
                )
                .unwrap();
            assert!(!stale.is_complete() && stale.locally_drained);
            assert!(matches!(
                stale.failure.as_ref().unwrap().error,
                Error::EpochMismatch
            ));
            assert_eq!(
                stale.failure.as_ref().unwrap().remote_effect,
                RemoteEffect::None
            );
            assert_eq!(engine.stats().unwrap().rdma.unwrap().opened_connections, 0);
            peer = Some(
                engine
                    .open_segment(next_endpoint, next_epoch, TIMEOUT)
                    .unwrap(),
            );
        }
        super::super::connected::barrier(&mut stream, 0x23);
        verify(&memory, &vec![0xa5; LENGTH]);
        assert_eq!(engine.stats().unwrap().rdma.unwrap().opened_connections, 0);
        let mut old = None;
        for sequence in 2..4 {
            round(
                &mut stream,
                &engine,
                &memory,
                peer.as_ref(),
                remote,
                source,
                sequence,
                false,
                false,
                operation,
            );
            let rows = snapshot(&engine);
            if let Some(old) = &old {
                assert_eq!(&rows, old);
            } else {
                old = Some(rows);
            }
            if let Some(weak) = &retained {
                assert!(
                    weak.upgrade().is_some(),
                    "replacement recycled quarantined backing"
                );
            }
        }
        println!(
            "ORBITTE_ENGINE_REPLACEMENT {{\"source\":{source},\"pid\":{},\"same_rpc\":true,\"same_registration_id\":true,\"new_epoch\":true,\"new_pd\":true,\"old_backing_retained\":{},\"stale_epoch_before_qp\":true,\"healthy_reuse_rounds\":2}}",
            std::process::id(),
            retained.is_some()
        );
    }
    worker_pool::running(&engine);
    let weak = Arc::downgrade(&backing);
    engine.unregister_memory(&memory).unwrap();
    engine.close(TIMEOUT).unwrap();
    clean(&engine.stats().unwrap().rdma.unwrap());
    worker_pool::closed(&engine.stats().unwrap().rdma.unwrap());
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    drop(memory);
    drop(backing);
    drop(engine);
    assert!(weak.upgrade().is_none());
    println!(
        "ORBITTE_LIFECYCLE_RESULT {{\"mode\":\"{mode}\",\"source\":{source},\"target\":{target},\"same_process\":true,\"fresh_engine_drained\":true,\"performance_measurement\":false}}"
    );
}
