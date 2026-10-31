use super::ipc::owned;
use super::*;
use crate::cuda::ipc::{IpcExport, IpcMapping};
use crate::memory::Allocation;

fn pinned(context: &Arc<Context>, value: u8) -> Arc<HostMemory> {
    let memory = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
        PinnedAllocation::new(context, 128).unwrap(),
    )));
    memory.write(0, &[value; 128]).unwrap();
    memory
}

#[test]
fn ipc_host_read_write_preserve_guards_reuse_stream_and_free_real_pin() {
    let source = allocation(&[0x5a; 128]);
    let destination = allocation(&[0; 128]);
    let context = memory(&destination)
        .allocation
        .lock()
        .unwrap()
        .context
        .clone();
    let host = pinned(&context, 0xa5);
    let remote = owned(&source, true);
    let export = IpcExport::new(remote.clone()).unwrap();
    let mut mapping = IpcMapping::new(&Allocation::Host(host.clone())).unwrap();
    mapping.open(export.handle).unwrap();
    for to_remote in [false, true, false] {
        mapping
            .copy_one(&Allocation::Host(host.clone()), 11, 7, 17, to_remote)
            .unwrap();
    }
    let mut actual = [0; 128];
    host.read(0, &mut actual).unwrap();
    let mut expected = [0xa5; 128];
    expected[7..24].fill(0x5a);
    assert_eq!(actual, expected);
    let weak = Arc::downgrade(&host);
    drop(host);
    assert!(weak.upgrade().is_some());
    assert_eq!(destination.pins.lock().unwrap().len(), 1);
    mapping.close().unwrap();
    assert!(weak.upgrade().is_none());
    assert!(destination.pins.lock().unwrap().is_empty());
    let events = destination.events.lock().unwrap();
    for (event, count) in [
        ("pin", 1),
        ("unpin", 1),
        ("HtoD", 1),
        ("DtoH", 2),
        ("stream-create", 1),
        ("event-sync", 3),
        ("ipc-close", 1),
    ] {
        assert_eq!(
            events.iter().filter(|&&e| e == event).count(),
            count,
            "{event}"
        );
    }
    assert!(
        events.iter().position(|e| *e == "ipc-close") < events.iter().position(|e| *e == "unpin")
    );
    assert!(!events.contains(&"DtoD"));
    export.finish();
}

#[test]
fn ipc_host_and_mixed_subbatches_keep_failure_scope_and_actual_owners() {
    for mixed in [false, true] {
        for case in [
            "success",
            "copy-second",
            "group-record",
            "group-wait",
            "group-restore",
            "close",
            "ack-loss",
            "cancel-second",
            "cancel-close",
            "ack-hold",
            "overlap",
        ] {
            let source = allocation(&[0x5a; 128]);
            let destination = allocation(&[0; 128]);
            let context = memory(&destination)
                .allocation
                .lock()
                .unwrap()
                .context
                .clone();
            let locals: Vec<_> = (1..=3)
                .map(|value| {
                    if mixed && value == 2 {
                        let owner = allocation(&[value; 128]);
                        let gpu = owned(&owner, false);
                        gpu.allocation.lock().unwrap().context = context.clone();
                        Allocation::Cuda(gpu)
                    } else {
                        Allocation::Host(pinned(&context, value))
                    }
                })
                .collect();
            match case {
                "copy-second" => destination.ipc_copy_fail_at.store(2, Ordering::Release),
                "group-record" => destination.record_error.store(1, Ordering::Release),
                "group-wait" => destination.ipc_wait_fail_at.store(2, Ordering::Release),
                "group-restore" => destination.ipc_pop_fail_at.store(2, Ordering::Release),
                "close" | "cancel-close" => destination.ipc_close_error.store(1, Ordering::Release),
                _ => (),
            }
            crate::transport::nvlink::tests::subbatch::run_case(
                locals,
                owned(&source, true),
                case,
                true,
            );
            let uncertain = matches!(
                case,
                "group-wait" | "group-restore" | "close" | "cancel-close"
            );
            assert_eq!(
                !destination.pins.lock().unwrap().is_empty(),
                uncertain,
                "{case}, mixed={mixed}"
            );
            assert_eq!(
                destination
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|&&e| e == "ipc-open")
                    .count(),
                1
            );
            STACK.with(|stack| stack.borrow_mut().clear());
        }
    }
}

#[test]
fn ipc_host_rejects_unknown_backing_and_checks_pinned_cleanup_failure() {
    let ordinary = Arc::new(HostMemory::new(vec![0; 128].into_boxed_slice()).unwrap());
    let external = unsafe {
        HostMemory::from_raw_parts(
            NonNull::new(ordinary.base_address() as usize as *mut u8).unwrap(),
            128,
            ordinary.clone(),
        )
        .unwrap()
    };
    for host in [ordinary, Arc::new(external)] {
        assert!(matches!(
            IpcMapping::new(&Allocation::Host(host)),
            Err(Error::Unavailable(_))
        ));
    }
    for fault in ["event-create", "record", "wait", "pop", "close", "free"] {
        let destination = allocation(&[0; 128]);
        let context = memory(&destination)
            .allocation
            .lock()
            .unwrap()
            .context
            .clone();
        let host = pinned(&context, 0xa5);
        let source = allocation(&[0x5a; 128]);
        let export = IpcExport::new(owned(&source, true)).unwrap();
        let mut mapping = IpcMapping::new(&Allocation::Host(host.clone())).unwrap();
        mapping.open(export.handle).unwrap();
        match fault {
            "event-create" => destination.event_create_error.store(1, Ordering::Release),
            "record" => destination.record_error.store(1, Ordering::Release),
            "wait" => destination.sync_error.store(700, Ordering::Release),
            "pop" => destination
                .pop_after_completion
                .store(true, Ordering::Release),
            "close" => destination.ipc_close_error.store(1, Ordering::Release),
            "free" => destination.free_error.store(1, Ordering::Release),
            _ => unreachable!(),
        }
        let result = mapping.copy_one(&Allocation::Host(host.clone()), 11, 7, 17, false);
        assert_eq!(
            result.is_err(),
            !matches!(fault, "close" | "free"),
            "{fault}"
        );
        let uncertain = matches!(fault, "wait" | "pop" | "close");
        assert_eq!(mapping.close().is_err(), uncertain);
        drop(mapping);
        assert_eq!(host.read(0, &mut [0; 1]).is_err(), uncertain);
        assert_eq!(host.write(0, &[1]).is_err(), uncertain);
        drop(host);
        assert_eq!(
            !destination.pins.lock().unwrap().is_empty(),
            uncertain || fault == "free",
            "{fault}"
        );
        STACK.with(|stack| stack.borrow_mut().clear());
    }
}

#[cfg(feature = "rdma")]
#[test]
fn ipc_host_completion_excludes_cpu_and_rdma_and_fence_survives_loan_drop() {
    use std::sync::mpsc;
    let destination = allocation(&[0; 128]);
    let context = memory(&destination)
        .allocation
        .lock()
        .unwrap()
        .context
        .clone();
    let host = pinned(&context, 0xa5);
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    let region = engine
        .register_host_memory(host.clone(), 0, 128, Permission::LocalOnly)
        .unwrap();
    let loan = region.dma_lease(100, 1).unwrap();
    assert!(IpcMapping::new(&Allocation::Host(host.clone())).is_err());
    drop(loan);
    let source = allocation(&[0x5a; 128]);
    let export = IpcExport::new(owned(&source, true)).unwrap();
    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::channel();
    *destination.event_gate.lock().unwrap() = Some((entered_tx, resume_rx));
    let copy_host = host.clone();
    let handle = export.handle;
    let worker = std::thread::spawn(move || {
        let owner = Allocation::Host(copy_host);
        let mut mapping = IpcMapping::new(&owner).unwrap();
        mapping.open(handle).unwrap();
        mapping.copy_one(&owner, 11, 7, 17, false).unwrap();
        mapping.close().unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (cpu_tx, cpu_rx) = mpsc::channel();
    let read_host = host.clone();
    let cpu = std::thread::spawn(move || {
        let mut bytes = [0; 17];
        read_host.read(7, &mut bytes).unwrap();
        cpu_tx.send(bytes).unwrap();
    });
    let (rdma_tx, rdma_rx) = mpsc::channel();
    let registration = region.clone();
    let rdma = std::thread::spawn(move || {
        let _loan = registration.dma_lease(64, 1).unwrap();
        rdma_tx.send(()).unwrap();
    });
    assert!(cpu_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert!(rdma_rx.try_recv().is_err());
    resume_tx.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(
        cpu_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        [0x5a; 17]
    );
    rdma_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    cpu.join().unwrap();
    rdma.join().unwrap();
    let loan = region.dma_lease(64, 1).unwrap();
    host.fence_cuda();
    drop(loan);
    assert!(host.read(0, &mut [0; 1]).is_err());
    assert!(host.write(0, &[1]).is_err());
    assert!(region.dma_lease(100, 1).is_err());
}

#[test]
fn ipc_groups_deduplicate_aliases_and_serialize_reversed_mixed_owners() {
    use crate::cuda::ipc::IpcCopy;
    use std::sync::{Barrier, mpsc};

    let source = allocation(&[0x5a; 128]);
    let destination = allocation(&[0; 128]);
    let gpu = owned(&destination, false);
    let context = gpu.allocation.lock().unwrap().context.clone();
    let host = pinned(&context, 0);
    let mut export = IpcExport::new(owned(&source, true)).unwrap();
    export.expose();
    let barrier = Arc::new(Barrier::new(2));
    let (done_tx, done_rx) = mpsc::channel();
    let workers: Vec<_> = [false, true]
        .into_iter()
        .map(|reverse| {
            let mut copies: Vec<_> = (0..4)
                .map(|index| IpcCopy {
                    memory: if index % 2 == 0 {
                        Allocation::Cuda(gpu.clone())
                    } else {
                        Allocation::Host(host.clone())
                    },
                    local_offset: index * 8,
                    remote_offset: index * 8,
                    length: 8,
                })
                .collect();
            if reverse {
                copies.reverse();
            }
            let barrier = barrier.clone();
            let done = done_tx.clone();
            let handle = export.handle;
            std::thread::spawn(move || {
                let mut mapping = IpcMapping::new(&copies[0].memory).unwrap();
                mapping.open(handle).unwrap();
                barrier.wait();
                let completion = mapping.copy_group(&copies, false, |_| Ok(()));
                completion.result.unwrap();
                assert_eq!((completion.completed, completion.attempted), (4, 4));
                // The fixture requires all copies drained before any import closes.
                barrier.wait();
                mapping.close().unwrap();
                done.send(()).unwrap();
            })
        })
        .collect();
    for _ in &workers {
        done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    }
    for worker in workers {
        worker.join().unwrap();
    }
    {
        let events = destination.events.lock().unwrap();
        for (kind, count) in [
            ("record", 2),
            ("event-sync", 2),
            ("DtoD", 4),
            ("DtoH", 4),
            ("stream-create", 2),
        ] {
            assert_eq!(
                events.iter().filter(|&&event| event == kind).count(),
                count,
                "{kind}"
            );
        }
    }
    let mut actual = [0; 128];
    gpu.read(0, &mut actual).unwrap();
    let mut expected = [0; 128];
    expected[..8].fill(0x5a);
    expected[16..24].fill(0x5a);
    assert_eq!(actual, expected);
    host.read(0, &mut actual).unwrap();
    let mut expected = [0; 128];
    expected[8..16].fill(0x5a);
    expected[24..32].fill(0x5a);
    assert_eq!(actual, expected);
    export.finish();
}
