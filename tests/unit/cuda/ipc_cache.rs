use super::ipc::owned;
use super::*;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Barrier, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::{RemoteRegion, RemoteSegment};

const TIMEOUT: Duration = Duration::from_secs(3);

// Both real engine handlers run in this process with the CUDA shim. The relay
// substitutes only process identities, and optionally drops/mutates frames.
// This exercises the public engine boundary; it is not hardware IPC evidence.
type CloseGate = Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>;

struct Relay {
    endpoint: SocketAddr,
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
    fault: Arc<AtomicUsize>,
    close_gate: Arc<Mutex<CloseGate>>,
}
impl Relay {
    fn new(source: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let fault = Arc::new(AtomicUsize::new(0));
        let close_gate = Arc::new(Mutex::new(None));
        let (stopping, faults, closing) = (stop.clone(), fault.clone(), close_gate.clone());
        let task = thread::spawn(move || {
            let mut children = Vec::new();
            while !stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((client, _)) => {
                        let faults = faults.clone();
                        let closing = closing.clone();
                        children.push(thread::spawn(move || {
                            let _ = relay(client, source, &faults, &closing);
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("relay accept: {error}"),
                }
            }
            for child in children {
                child.join().unwrap();
            }
        });
        Self {
            endpoint,
            stop,
            task: Some(task),
            fault,
            close_gate,
        }
    }
    fn peer(&self, engine: &TransferEngine, memory: &crate::RegisteredMemory) -> RemoteSegment {
        let epoch = engine.source_epoch();
        RemoteSegment {
            endpoint: self.endpoint,
            epoch,
            regions: vec![RemoteRegion {
                epoch,
                id: memory.id(),
                address: memory.base_address(),
                length: memory.len(),
                permission: Permission::RemoteReadWrite,
            }],
        }
    }
}
impl Drop for Relay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.task.take().unwrap().join().unwrap();
    }
}
fn relay(
    mut client: TcpStream,
    source: SocketAddr,
    fault: &AtomicUsize,
    close_gate: &Mutex<CloseGate>,
) -> std::io::Result<()> {
    let mut server = TcpStream::connect(source)?;
    for socket in [&client, &server] {
        socket.set_read_timeout(Some(TIMEOUT))?;
        socket.set_write_timeout(Some(TIMEOUT))?;
    }
    let mut request = [0; 52];
    client.read_exact(&mut request)?;
    server.write_all(&request)?;
    let mut hello = [0; 69];
    client.read_exact(&mut hello)?;
    assert_eq!(&hello[..4], b"NVL2");
    hello[20..36].fill(0x88);
    server.write_all(&hello)?;
    let mut reply = [0; 36];
    server.read_exact(&mut reply)?;
    if request[10] == 12 {
        let gate = close_gate.lock().unwrap().take();
        if let Some((entered, resume)) = gate {
            entered.send(()).unwrap();
            resume.recv_timeout(TIMEOUT).unwrap();
        }
        if reply[10] == 0
            && fault
                .compare_exchange(2, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return Ok(());
        }
        return client.write_all(&reply);
    }
    client.write_all(&reply)?;
    if reply[10] == 12 {
        let mut ready = [0; 1];
        client.read_exact(&mut ready)?;
        if fault
            .compare_exchange(5, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            ready[0] = 1;
        }
        server.write_all(&ready)?;
        server.read_exact(&mut reply)?;
        client.write_all(&reply)?;
    }
    if reply[10] != 0 {
        return Ok(());
    }
    let mut grant = [0; 200];
    server.read_exact(&mut grant)?;
    grant[20..36].fill(0x77);
    if fault
        .compare_exchange(4, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        client.write_all(&grant[..100])?;
        return Ok(());
    }
    client.write_all(&grant)?;
    let mut receipt = [0; 85];
    client.read_exact(&mut receipt)?;
    if fault
        .compare_exchange(3, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        receipt[68] ^= 1;
    }
    server.write_all(&receipt)?;
    server.read_exact(&mut reply)?;
    if fault
        .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        return Ok(());
    }
    client.write_all(&reply)
}
fn engine() -> Arc<TransferEngine> {
    engine_with_limit(2)
}
fn engine_with_limit(limit: usize) -> Arc<TransferEngine> {
    Arc::new(
        TransferEngine::new(EngineConfig {
            nvlink: true,
            nvlink_cache_entries: limit,
            handler_timeout: Duration::from_millis(500),
            ..EngineConfig::default()
        })
        .unwrap(),
    )
}
fn register(engine: &TransferEngine, memory: &Arc<CudaMemory>) -> crate::RegisteredMemory {
    engine
        .register_cuda_memory_batch(
            &[(memory.clone(), 0, memory.len())],
            Permission::RemoteReadWrite,
        )
        .unwrap()
        .remove(0)
}
fn request(local: &crate::RegisteredMemory, remote: &crate::RegisteredMemory) -> TransferRequest {
    TransferRequest {
        priority: Default::default(),
        remote_staging: false,
        transport: Transport::Nvlink,
        local_address: local.base_address() + 7,
        remote_address: remote.base_address() + 11,
        length: 17,
    }
}
fn event_count(device: &Device, name: &str) -> usize {
    device
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|&&event| event == name)
        .count()
}

#[test]
fn cached_ipc_reuses_import_but_releases_completed_local_owners() {
    let src = allocation(&[0x5a; 128]);
    let dst = allocation(&[0xa5; 128]);
    let remote = owned(&src, true);
    let local = owned(&dst, false);
    let source = engine();
    let requester = engine();
    let exported = register(&source, &remote);
    let relay = Relay::new(source.endpoint());
    let peer = relay.peer(&source, &exported);
    let context = local.allocation.lock().unwrap().context.clone();
    for index in 0..8 {
        let host = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
            PinnedAllocation::new(&context, 128).unwrap(),
        )));
        host.write(0, &[0xa5; 128]).unwrap();
        let registered = requester
            .register_host_memory(host.clone(), 0, 128, Permission::LocalOnly)
            .unwrap();
        let req = request(&registered, &exported);
        let read = requester
            .submit_and_wait(TransferOp::Read, &peer, &[req], TIMEOUT)
            .unwrap();
        assert!(
            read.is_complete() && read.locally_drained,
            "{index}: {read:?}"
        );
        let mut actual = [0; 128];
        host.read(0, &mut actual).unwrap();
        let mut expected = [0xa5; 128];
        expected[7..24].fill(0x5a);
        assert_eq!(actual, expected);
        let weak = Arc::downgrade(&host);
        requester.unregister_memory(&registered).unwrap();
        drop(registered);
        drop(host);
        assert!(
            weak.upgrade().is_none(),
            "idle mapping retained completed host allocation"
        );
        assert!(dst.pins.lock().unwrap().is_empty());
        let stats = requester.stats().unwrap().nvlink.unwrap();
        assert_eq!(
            (
                stats.imports,
                stats.stream_slots,
                stats.active_streams,
                stats.retained_streams
            ),
            (1, 1, 0, 0)
        );
        assert_eq!(event_count(&dst, "stream-create"), 1);
        assert_eq!(stats.progress_workers, 0, "ready groups need no worker hop");
        assert_eq!(event_count(&dst, "event-create"), 1);
        assert_eq!(event_count(&dst, "stream-destroy"), 0);
        assert_eq!(requester.stats().unwrap().inflight_bytes, 0);
        assert!(remote.read(0, &mut [0; 1]).is_err());
    }
    let registered = register(&requester, &local);
    local.write(7, &[0x39; 17]).unwrap();
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[request(&registered, &exported)],
                TIMEOUT
            )
            .unwrap()
            .is_complete()
    );
    assert_eq!(event_count(&dst, "ipc-open"), 1);
    assert_eq!(event_count(&dst, "ipc-close"), 0);
    requester.close_requester(TIMEOUT).unwrap();
    source.close_source(TIMEOUT).unwrap();
    assert_eq!(requester.stats().unwrap().nvlink.unwrap().stream_slots, 0);
    assert_eq!(event_count(&dst, "stream-destroy"), 1);
    assert_eq!(event_count(&dst, "ipc-close"), 1);
    let mut actual = [0; 128];
    remote.read(0, &mut actual).unwrap();
    let mut expected = [0x5a; 128];
    expected[11..28].fill(0x39);
    assert_eq!(actual, expected);
    assert_eq!(source.stats().unwrap().nvlink.unwrap().allocation_bytes, 0);
}

#[test]
fn cached_ipc_rechecks_source_admission_registration_and_permission() {
    for case in ["closed", "unregister", "permission"] {
        let src = allocation(&[0x5a; 128]);
        let dst = allocation(&[0xa5; 128]);
        let remote = owned(&src, true);
        let local = owned(&dst, false);
        let source = engine();
        let requester = engine();
        let exported = source
            .register_cuda_memory_batch(
                &[(remote.clone(), 0, 128)],
                if case == "permission" {
                    Permission::RemoteRead
                } else {
                    Permission::RemoteReadWrite
                },
            )
            .unwrap()
            .remove(0);
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let peer = relay.peer(&source, &exported);
        let req = request(&registered, &exported);
        assert!(
            requester
                .submit_and_wait(TransferOp::Read, &peer, std::slice::from_ref(&req), TIMEOUT)
                .unwrap()
                .is_complete()
        );
        if case == "closed" {
            assert!(matches!(
                source.close_source(Duration::from_millis(5)),
                Err(Error::Timeout)
            ));
        }
        if case == "unregister" {
            source.unregister_memory(&exported).unwrap();
        }
        let report = requester
            .submit_and_wait(TransferOp::Write, &peer, &[req], TIMEOUT)
            .unwrap();
        assert!(
            report.failure.is_some() && report.locally_drained,
            "{case}: {report:?}"
        );
        assert_eq!(event_count(&dst, "ipc-open"), 1);
        assert_eq!(
            event_count(&dst, "DtoD"),
            1,
            "rejected operation copied bytes: {case}"
        );
        requester.close_requester(TIMEOUT).unwrap();
        source.close_source(TIMEOUT).unwrap();
        assert_eq!(source.stats().unwrap().nvlink.unwrap().exports, 0);
        remote.read(0, &mut [0; 1]).unwrap();
    }
}

#[test]
fn cached_ipc_cold_open_is_single_flight_and_waiter_deadline_does_not_evict() {
    let src = allocation(&[0x5a; 128]);
    let dst = allocation(&[0xa5; 128]);
    let remote = owned(&src, true);
    let local = owned(&dst, false);
    let source = engine();
    let requester = engine_with_limit(1);
    let exported = register(&source, &remote);
    let registered = register(&requester, &local);
    let relay = Relay::new(source.endpoint());
    let peer = relay.peer(&source, &exported);
    let req = request(&registered, &exported);
    dst.query_hold.store(true, Ordering::Release);
    let context = local.allocation.lock().unwrap().context.clone();
    let waiting_host = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
        PinnedAllocation::new(&context, 128).unwrap(),
    )));
    let waiting_local = requester
        .register_host_memory(waiting_host, 0, 128, Permission::LocalOnly)
        .unwrap();
    let waiting_request = request(&waiting_local, &exported);
    let first = {
        let e = requester.clone();
        let p = peer.clone();
        let r = req.clone();
        thread::spawn(move || {
            e.submit_and_wait(TransferOp::Read, &p, &[r], TIMEOUT)
                .unwrap()
        })
    };
    super::ipc::wait_query(&dst);
    let timed_out = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&waiting_request),
            Duration::from_millis(5),
        )
        .unwrap();
    assert!(matches!(timed_out.failure.unwrap().error, Error::Timeout));
    assert_eq!(event_count(&dst, "ipc-open"), 1);
    assert_eq!(event_count(&dst, "ipc-close"), 0);
    let other_device = allocation(&[0x19; 128]);
    let other_memory = owned(&other_device, true);
    let other = register(&source, &other_memory);
    let rejected = requester
        .submit_and_wait(
            TransferOp::Read,
            &relay.peer(&source, &other),
            &[request(&waiting_local, &other)],
            TIMEOUT,
        )
        .unwrap();
    assert!(matches!(
        rejected.failure.unwrap().error,
        Error::ResourceExhausted
    ));
    assert!(matches!(
        requester.flush_nvlink_cache(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert_eq!(event_count(&other_device, "ipc-export"), 0);
    assert_eq!(event_count(&dst, "ipc-close"), 0);
    let barrier = Arc::new(Barrier::new(8));
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let (e, p, r, b) = (
                requester.clone(),
                peer.clone(),
                waiting_request.clone(),
                barrier.clone(),
            );
            thread::spawn(move || {
                b.wait();
                e.submit_and_wait(TransferOp::Read, &p, &[r], TIMEOUT)
                    .unwrap()
            })
        })
        .collect();
    dst.query_hold.store(false, Ordering::Release);
    assert!(first.join().unwrap().is_complete());
    for task in tasks {
        assert!(task.join().unwrap().is_complete());
    }
    assert_eq!(event_count(&dst, "ipc-open"), 1);
    requester.close(TIMEOUT).unwrap();
    source.close(TIMEOUT).unwrap();
}

#[test]
fn cached_ipc_completion_loss_and_close_ack_loss_preserve_one_shot_native_close() {
    for fault in [1, 2, 3, 4, 5] {
        let src = allocation(&[0x5a; 128]);
        let dst = allocation(&[0xa5; 128]);
        let remote = owned(&src, true);
        let local = owned(&dst, false);
        let source = engine();
        let requester = engine();
        let exported = register(&source, &remote);
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let peer = relay.peer(&source, &exported);
        relay.fault.store(fault, Ordering::Release);
        let report = requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[request(&registered, &exported)],
                TIMEOUT,
            )
            .unwrap();
        assert_eq!(
            report.is_complete(),
            fault == 2,
            "fault {fault}: {report:?}"
        );
        assert!(report.locally_drained);
        let close = requester.close_requester(TIMEOUT);
        if fault == 2 {
            assert!(close.is_err());
        } else {
            close.unwrap();
        }
        requester.close_requester(TIMEOUT).unwrap();
        source.close_source(TIMEOUT).unwrap();
        assert_eq!(
            event_count(&dst, "ipc-close"),
            usize::from(!matches!(fault, 4 | 5))
        );
        assert_eq!(source.stats().unwrap().nvlink.unwrap().allocation_bytes, 0);
    }
}

#[test]
fn mutually_cached_engines_close_outgoing_imports_before_incoming_exports() {
    let a = engine();
    let b = engine();
    let a_src = allocation(&[1; 128]);
    let b_src = allocation(&[2; 128]);
    let a_dst = allocation(&[0; 128]);
    let b_dst = allocation(&[0; 128]);
    let a_remote = owned(&a_src, true);
    let b_remote = owned(&b_src, true);
    let a_local = owned(&a_dst, false);
    let b_local = owned(&b_dst, false);
    let ar = register(&a, &a_remote);
    let br = register(&b, &b_remote);
    let al = register(&a, &a_local);
    let bl = register(&b, &b_local);
    let ra = Relay::new(a.endpoint());
    let rb = Relay::new(b.endpoint());
    assert!(
        a.submit_and_wait(
            TransferOp::Read,
            &rb.peer(&b, &br),
            &[request(&al, &br)],
            TIMEOUT
        )
        .unwrap()
        .is_complete()
    );
    assert!(
        b.submit_and_wait(
            TransferOp::Read,
            &ra.peer(&a, &ar),
            &[request(&bl, &ar)],
            TIMEOUT
        )
        .unwrap()
        .is_complete()
    );
    let barrier = Arc::new(Barrier::new(2));
    let close_a = {
        let a = a.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            a.close(TIMEOUT)
        })
    };
    barrier.wait();
    b.close(TIMEOUT).unwrap();
    close_a.join().unwrap().unwrap();
    assert_eq!(a.stats().unwrap().nvlink.unwrap().allocation_bytes, 0);
    assert_eq!(b.stats().unwrap().nvlink.unwrap().allocation_bytes, 0);
}

#[test]
fn cached_ipc_source_limit_rejects_before_export_and_native_close_is_not_retried() {
    for source_limit in [1, 2] {
        let dst = allocation(&[0xa5; 128]);
        let local = owned(&dst, false);
        let source = engine_with_limit(source_limit);
        let requester = engine_with_limit(3);
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let remotes: Vec<_> = (0..3)
            .map(|_| {
                let owner = allocation(&[0x5a; 128]);
                let memory = owned(&owner, true);
                let region = register(&source, &memory);
                (owner, memory, region)
            })
            .collect();
        for (i, (_, _, region)) in remotes.iter().enumerate() {
            let result = requester
                .submit_and_wait(
                    TransferOp::Read,
                    &relay.peer(&source, region),
                    &[request(&registered, region)],
                    TIMEOUT,
                )
                .unwrap();
            assert_eq!(result.is_complete(), i < source_limit);
            if i >= source_limit {
                assert!(matches!(
                    result.failure.unwrap().error,
                    Error::ResourceExhausted
                ));
            }
        }
        assert_eq!(event_count(&dst, "ipc-open"), source_limit);
        assert_eq!(
            source.stats().unwrap().nvlink.unwrap().exports,
            source_limit
        );
        let weak = Arc::downgrade(&local);
        requester.unregister_memory(&registered).unwrap();
        drop(registered);
        drop(local);
        assert!(
            weak.upgrade().is_none(),
            "idle caches retained historical local allocation"
        );
        dst.ipc_close_error.store(1, Ordering::Release);
        assert!(matches!(
            requester.close_requester(TIMEOUT),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(event_count(&dst, "ipc-close"), source_limit);
        dst.ipc_close_error.store(0, Ordering::Release);
        assert!(matches!(
            requester.close_requester(TIMEOUT),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(
            event_count(&dst, "ipc-close"),
            source_limit,
            "retried uncertain native close"
        );
        assert_eq!(
            requester.stats().unwrap().nvlink.unwrap().retained_imports,
            source_limit
        );
        assert!(matches!(
            source.close_source(Duration::from_millis(5)),
            Err(Error::Timeout)
        ));
        assert_eq!(event_count(&remotes[source_limit].0, "ipc-export"), 0);
    }
}

#[test]
fn cached_ipc_uncertain_second_submission_retains_only_its_actual_local_owner() {
    let src = allocation(&[0x5a; 128]);
    let dst = allocation(&[0xa5; 128]);
    let remote = owned(&src, true);
    let local = owned(&dst, false);
    let source = engine();
    let requester = engine();
    let exported = register(&source, &remote);
    let registered = register(&requester, &local);
    let context = local.allocation.lock().unwrap().context.clone();
    let relay = Relay::new(source.endpoint());
    let peer = relay.peer(&source, &exported);
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[request(&registered, &exported)],
                TIMEOUT
            )
            .unwrap()
            .is_complete()
    );
    let previous = Arc::downgrade(&local);
    requester.unregister_memory(&registered).unwrap();
    drop(registered);
    drop(local);
    assert!(previous.upgrade().is_none());
    let host = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
        PinnedAllocation::new(&context, 128).unwrap(),
    )));
    let current = Arc::downgrade(&host);
    let registered = requester
        .register_host_memory(host.clone(), 0, 128, Permission::LocalOnly)
        .unwrap();
    dst.sync_error.store(700, Ordering::Release);
    let result = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[request(&registered, &exported)],
            TIMEOUT,
        )
        .unwrap();
    assert!(matches!(
        result.failure.unwrap().error,
        Error::Unquiesced { .. }
    ));
    assert!(!result.locally_drained);
    requester.unregister_memory(&registered).unwrap();
    drop(registered);
    drop(host);
    assert!(current.upgrade().is_some());
    assert!(previous.upgrade().is_none());
    assert_eq!(event_count(&dst, "ipc-close"), 0);
    assert_eq!(
        requester.stats().unwrap().nvlink.unwrap().retained_imports,
        1
    );
    assert!(matches!(
        source.close_source(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
}

#[test]
fn close_receipt_overtaking_initial_hello_cannot_leave_a_late_export() {
    use crate::SourceEpoch;
    use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
    let src = allocation(&[0x5a; 128]);
    let remote = owned(&src, true);
    let source = engine();
    let exported = register(&source, &remote);
    let until = Instant::now() + TIMEOUT;
    let mut initial = ControlChannel::connect(source.endpoint(), until).unwrap();
    initial
        .write_all(
            &Request {
                operation: Operation::NvlinkReadCached,
                epoch: source.source_epoch(),
                region: exported.id(),
                offset: 11,
                length: 17,
            }
            .encode(),
        )
        .unwrap();
    while source.stats().unwrap().active_source_operations != 1 {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    let mut hello = [0; 69];
    hello[..4].copy_from_slice(b"NVL2");
    let boot: SourceEpoch = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .unwrap()
        .trim()
        .replace('-', "")
        .parse()
        .unwrap();
    hello[4..20].copy_from_slice(&boot.0);
    hello[20..36].fill(0x88);
    hello[36..52].fill(0x55);
    hello[52..68].fill(0x66);
    let mut close = ControlChannel::connect(source.endpoint(), until).unwrap();
    close
        .write_all(
            &Request {
                operation: Operation::NvlinkClose,
                epoch: source.source_epoch(),
                region: exported.id(),
                offset: 0,
                length: 0,
            }
            .encode(),
        )
        .unwrap();
    close.write_all(&hello).unwrap();
    assert_eq!(
        Reply::read(&mut close, source.source_epoch())
            .unwrap()
            .status,
        0
    );
    // Delayed hello reaches the already admitted handler after close ACK. It
    // can offer a reservation but cannot expose a handle without a new answer.
    initial.write_all(&hello).unwrap();
    let offer = Reply::read(&mut initial, source.source_epoch()).unwrap();
    assert_eq!((offer.status, offer.length), (12, 0));
    drop(initial);
    source.close_source(TIMEOUT).unwrap();
    let stats = source.stats().unwrap();
    assert_eq!(stats.active_source_operations, 0);
    assert_eq!(stats.nvlink.unwrap().allocation_bytes, 0);
    remote.read(0, &mut [0; 1]).unwrap();
}

#[test]
fn unexposed_cache_rejections_leave_no_requester_close_obligation() {
    for case in ["disabled", "epoch", "connect"] {
        let src = allocation(&[0x5a; 128]);
        let dst = allocation(&[0xa5; 128]);
        let remote = owned(&src, true);
        let local = owned(&dst, false);
        let source = TransferEngine::new(EngineConfig {
            nvlink: true,
            nvlink_cache_entries: if case == "disabled" { 0 } else { 2 },
            ..EngineConfig::default()
        })
        .unwrap();
        let requester = engine();
        let exported = register(&source, &remote);
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let mut peer = relay.peer(&source, &exported);
        if case == "epoch" {
            peer.epoch = crate::SourceEpoch([0x55; 16]);
            peer.regions[0].epoch = peer.epoch;
        }
        if case == "connect" {
            let unavailable = TcpListener::bind("127.0.0.1:0").unwrap();
            peer.endpoint = unavailable.local_addr().unwrap();
            drop(unavailable);
        }
        let report = requester
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[request(&registered, &exported)],
                TIMEOUT,
            )
            .unwrap();
        assert!(
            report.failure.is_some() && report.locally_drained,
            "{case}: {report:?}"
        );
        requester.close_requester(TIMEOUT).unwrap();
        requester.close_requester(TIMEOUT).unwrap();
        source.close_source(TIMEOUT).unwrap();
        assert_eq!(event_count(&dst, "ipc-open"), 0);
        assert_eq!(source.stats().unwrap().nvlink.unwrap().allocation_bytes, 0);
        assert_eq!(
            requester.stats().unwrap().nvlink.unwrap().allocation_bytes,
            0
        );
    }
}

#[test]
fn cached_export_keeps_actual_registration_and_allocation_until_close_receipt() {
    for drop_source in [false, true] {
        let src = allocation(&[0x5a; 128]);
        let dst = allocation(&[0xa5; 128]);
        let remote = owned(&src, true);
        let local = owned(&dst, false);
        let source = engine();
        let requester = engine();
        let exported = register(&source, &remote);
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let peer = relay.peer(&source, &exported);
        assert!(
            requester
                .submit_and_wait(
                    TransferOp::Read,
                    &peer,
                    &[request(&registered, &exported)],
                    TIMEOUT
                )
                .unwrap()
                .is_complete()
        );
        let memory = Arc::downgrade(&remote);
        let registration = Arc::downgrade(&exported.0);
        source.unregister_memory(&exported).unwrap();
        drop(exported);
        drop(remote);
        assert!(memory.upgrade().is_some());
        assert!(registration.upgrade().is_some());
        assert_eq!(event_count(&src, "free"), 0);
        if drop_source {
            assert!(matches!(
                source.close_source(Duration::from_millis(5)),
                Err(Error::Timeout)
            ));
            drop(source);
            drop(relay);
            assert!(
                requester
                    .close_requester(Duration::from_millis(30))
                    .is_err()
            );
            drop(requester);
            assert!(
                memory.upgrade().is_some(),
                "engine drop freed a live exported allocation"
            );
            assert!(
                registration.upgrade().is_some(),
                "engine drop lost the actual registration"
            );
            assert_eq!(event_count(&src, "free"), 0);
        } else {
            requester.close_requester(TIMEOUT).unwrap();
            source.close_source(TIMEOUT).unwrap();
            assert!(memory.upgrade().is_none());
            assert!(registration.upgrade().is_none());
            assert_eq!(event_count(&src, "free"), 1);
        }
    }
}

#[path = "ipc_cache/reclaim.rs"]
mod reclaim;

#[path = "ipc_cache/progress.rs"]
mod progress;
