use super::*;
use crate::cuda::ipc::{IpcExport, IpcMapping};
use crate::memory::Allocation;

pub(super) fn owned(device: &Arc<Device>, exportable: bool) -> Arc<CudaMemory> {
    let memory = memory(device);
    {
        let mut allocation = memory.allocation.lock().unwrap();
        allocation.owned = true;
        allocation.ipc_extent = exportable.then_some(2 << 20);
    }
    memory
}

#[test]
fn ipc_explicit_export_blocks_whole_allocation_and_reuses_copy_stream() {
    let source = allocation(&[0x5a; 128]);
    let destination = allocation(&[0xa5; 128]);
    let remote = owned(&source, false);
    assert!(matches!(
        IpcExport::new(remote.clone()),
        Err(Error::Unavailable(_))
    ));
    remote.allocation.lock().unwrap().ipc_extent = Some(2 << 20);
    let local = owned(&destination, false);
    let pool = CUDAStreamPool::new(1).unwrap();
    let mut export = IpcExport::new(remote.clone()).unwrap();
    export.expose();
    assert_eq!(export.extent, 2 << 20);
    assert!(
        remote.read(100, &mut [0; 1]).is_err(),
        "even disjoint bytes remain loaned"
    );
    assert!(IpcExport::new(remote.clone()).is_err());
    for to_remote in [false, true, false] {
        copy_ipc(&local, &pool, export.handle, to_remote).unwrap();
        assert!(destination.imports.lock().unwrap().is_empty());
    }
    let mut bytes = [0; 128];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[7..24], &[0x5a; 17]);
    assert!(
        bytes[..7]
            .iter()
            .chain(bytes[24..].iter())
            .all(|b| *b == 0xa5)
    );
    let events = destination.events.lock().unwrap();
    assert_eq!(events.iter().filter(|&&e| e == "stream-create").count(), 2);
    assert_eq!(pool.stats().unwrap(), (1, 0, 0));
    assert_eq!(events.iter().filter(|&&e| e == "ipc-open").count(), 3);
    assert_eq!(events.iter().filter(|&&e| e == "ipc-close").count(), 3);
    for (index, event) in events.iter().enumerate() {
        if *event == "ipc-close" {
            assert_eq!(&events[index - 3..index], &["event-query", "pop", "push"]);
        }
    }
    drop(events);
    export.finish();
    remote.write(100, &[7]).unwrap();
    assert!(remote.allocation.lock().unwrap().dma.is_empty());
}

#[test]
fn ipc_failure_controls_distinguish_clean_abort_from_retained_mapping() {
    for case in [
        "open",
        "partial-open",
        "copy",
        "record",
        "event-create",
        "wait",
        "close",
        "restore-open",
        "restore-copy",
        "restore-close",
    ] {
        let source = allocation(&[0x5a; 128]);
        let destination = allocation(&[0xa5; 128]);
        let remote = owned(&source, true);
        let local = owned(&destination, false);
        let pool = CUDAStreamPool::new(1).unwrap();
        let mut export = IpcExport::new(remote.clone()).unwrap();
        export.expose();
        match case {
            "open" | "partial-open" => {
                destination.ipc_open_error.store(1, Ordering::Release);
                destination
                    .ipc_partial_open
                    .store(case == "partial-open", Ordering::Release);
            }
            "copy" => destination.copy_error.store(1, Ordering::Release),
            "record" => destination.record_error.store(1, Ordering::Release),
            "event-create" => destination.event_create_error.store(1, Ordering::Release),
            "wait" => destination.sync_error.store(700, Ordering::Release),
            "close" => destination.ipc_close_error.store(1, Ordering::Release),
            "restore-open" => destination
                .ipc_pop_after_open
                .store(true, Ordering::Release),
            "restore-copy" => destination
                .pop_after_completion
                .store(true, Ordering::Release),
            "restore-close" => destination
                .ipc_pop_after_close
                .store(true, Ordering::Release),
            _ => unreachable!(),
        }
        let result = copy_ipc(&local, &pool, export.handle, true);
        let uncertain = matches!(
            case,
            "wait" | "close" | "restore-open" | "restore-copy" | "restore-close"
        );
        assert_eq!(
            matches!(result, Err(Error::Unquiesced { .. })),
            uncertain,
            "{case}: {result:?}"
        );
        assert!(result.is_err());
        if matches!(case, "wait" | "restore-copy") {
            assert_eq!(pool.stats().unwrap(), (1, 0, 1), "{case}");
        } else if matches!(case, "copy" | "record") {
            assert_eq!(pool.stats().unwrap(), (1, 0, 0), "{case}");
        }
        assert_eq!(
            destination.imports.lock().unwrap().is_empty(),
            !uncertain || case == "restore-close",
            "{case}"
        );
        if uncertain {
            assert!(local.validate_ipc_requester(deadline()).is_err());
            drop(export);
            assert!(remote.read(100, &mut [0; 1]).is_err());
            if !matches!(case, "close" | "restore-close") {
                assert!(!destination.events.lock().unwrap().contains(&"ipc-close"));
            }
        } else {
            export.finish();
            assert!(remote.allocation.lock().unwrap().dma.is_empty());
        }
        STACK.with(|stack| stack.borrow_mut().clear());
    }
}

#[test]
fn ipc_source_protocol_checks_scope_receipts_and_retained_admission() {
    for case in [
        "success",
        "abort",
        "nonce",
        "operation",
        "epoch",
        "partial",
        "partial-grant",
        "timeout",
        "machine",
        "same-process",
        "budget",
    ] {
        let owner = allocation(&[0x5a; 128]);
        let memory = owned(&owner, true);
        crate::transport::nvlink::tests::source_case(memory, case);
    }
}

#[test]
fn ipc_public_requester_retains_real_owners_and_reports_partial_writes() {
    for asynchronous in [false, true] {
        for case in [
            "success",
            "budget",
            "open",
            "copy",
            "wait",
            "close",
            "restore-close",
        ] {
            let source = allocation(&[0x5a; 128]);
            let destination = allocation(&[0xa5; 128]);
            let remote = owned(&source, true);
            let local = owned(&destination, false);
            match case {
                "open" => destination.ipc_open_error.store(1, Ordering::Release),
                "copy" => destination.copy_error.store(1, Ordering::Release),
                "wait" => destination.sync_error.store(700, Ordering::Release),
                "close" => destination.ipc_close_error.store(1, Ordering::Release),
                "restore-close" => destination
                    .ipc_pop_after_close
                    .store(true, Ordering::Release),
                _ => (),
            }
            crate::transport::nvlink::tests::requester_case(local, remote, case, asynchronous);
            let uncertain = matches!(case, "wait" | "close" | "restore-close");
            assert_eq!(
                destination.events.lock().unwrap().contains(&"free"),
                !uncertain,
                "{case}"
            );
            assert_eq!(
                destination.imports.lock().unwrap().is_empty(),
                !uncertain || case == "restore-close",
                "{case}"
            );
            STACK.with(|stack| stack.borrow_mut().clear());
        }
    }
}

impl IpcMapping {
    pub(super) fn copy_one(
        &mut self,
        memory: &Allocation,
        pool: &Arc<CUDAStreamPool>,
        remote_offset: usize,
        local_offset: usize,
        length: usize,
        to_remote: bool,
    ) -> Result<()> {
        let stream = pool.acquire(memory, std::time::Instant::now() + Duration::from_secs(2))?;
        self.copy_group(
            &[crate::cuda::ipc::IpcCopy {
                memory: memory.clone(),
                remote_offset,
                local_offset,
                length,
            }],
            to_remote,
            stream,
            deadline(),
            |_| Ok(()),
            complete_group,
        )
        .result
    }
}

fn copy_ipc(
    local: &Arc<CudaMemory>,
    pool: &Arc<CUDAStreamPool>,
    handle: [u8; 64],
    to_remote: bool,
) -> Result<()> {
    let local = Allocation::Cuda(local.clone());
    let mut mapping = IpcMapping::new(&local, super::ipc::deadline())?;
    let result = mapping
        .open(handle)
        .and_then(|()| mapping.copy_one(&local, pool, 11, 7, 17, to_remote));
    mapping.close()?;
    result
}

#[test]
fn ipc_subbatch_reuses_relocation_and_reports_every_affected_task() {
    for asynchronous in [false, true] {
        for case in [
            "success",
            "copy-second",
            "group-record",
            "group-wait",
            "group-restore",
            "close",
            "ack-loss",
            "prefix-ack-loss",
            "cap",
            "source-cap",
            "context",
            "region",
            "cancel-second",
            "cancel-close",
            "cancel-restore-close",
            "cancel-ack-loss",
            "overlap",
            "ack-hold",
        ] {
            if (case.starts_with("cancel-") || case == "ack-hold") && !asynchronous {
                continue;
            }
            let source = allocation(&[0x5a; 128]);
            let owners: Vec<_> = (1..=3).map(|value| allocation(&[value; 128])).collect();
            let local: Vec<_> = owners.iter().map(|owner| owned(owner, false)).collect();
            let context = local[0].allocation.lock().unwrap().context.clone();
            for (index, memory) in local.iter().enumerate().skip(1) {
                if case == "context" && index == 2 {
                    continue;
                }
                memory.allocation.lock().unwrap().context = Arc::new(Context {
                    driver: context.driver.clone(),
                    raw: context.raw,
                    device: context.device,
                    primary: false,
                    owner: Some(context.clone()),
                    poisoned: AtomicBool::new(false),
                    locality: Mutex::new(None),
                });
            }
            match case {
                "copy-second" => owners[0].ipc_copy_fail_at.store(2, Ordering::Release),
                "group-record" => owners[0].record_error.store(1, Ordering::Release),
                "group-wait" => owners[0].ipc_wait_fail_at.store(2, Ordering::Release),
                "group-restore" => owners[0].ipc_pop_fail_at.store(2, Ordering::Release),
                "close" | "cancel-close" => owners[0].ipc_close_error.store(1, Ordering::Release),
                "cancel-restore-close" => {
                    owners[0].ipc_pop_after_close.store(true, Ordering::Release)
                }
                _ => (),
            }
            crate::transport::nvlink::tests::subbatch::run_case(
                local.into_iter().map(Allocation::Cuda).collect(),
                owned(&source, true),
                case,
                asynchronous,
            );
            let events = owners[0].events.lock().unwrap();
            let expected = match case {
                "cap" | "source-cap" => 3,
                "region" | "prefix-ack-loss" => 2,
                _ => 1,
            };
            assert_eq!(
                events.iter().filter(|&&e| e == "ipc-open").count(),
                expected,
                "{case}"
            );
            if matches!(case, "success" | "overlap" | "ack-hold") {
                assert_eq!(events.iter().filter(|&&e| e == "record").count(), 1);
                assert_eq!(events.iter().filter(|&&e| e == "event-query").count(), 1);
                assert_eq!(events.iter().filter(|&&e| e == "stream-create").count(), 1);
                let record = events.iter().position(|&e| e == "record").unwrap();
                assert_eq!(events[..record].iter().filter(|&&e| e == "DtoD").count(), 3);
            }
            let closes = if matches!(case, "group-wait" | "group-restore") {
                0
            } else {
                expected
            };
            assert_eq!(
                events.iter().filter(|&&e| e == "ipc-close").count(),
                closes,
                "{case}"
            );
            drop(events);
            STACK.with(|stack| stack.borrow_mut().clear());
        }
    }
}

#[test]
fn ipc_mapping_owns_all_touched_allocations_until_explicit_close() {
    for fail_close in [false, true] {
        let source = allocation(&[0x5a; 128]);
        let destination = allocation(&[0xa5; 128]);
        let remote = owned(&source, true);
        let local = owned(&destination, false);
        let pool = CUDAStreamPool::new(1).unwrap();
        let weak = Arc::downgrade(&local);
        let mut export = IpcExport::new(remote).unwrap();
        export.expose();
        let mut mapping =
            IpcMapping::new(&Allocation::Cuda(local.clone()), super::ipc::deadline()).unwrap();
        mapping.open(export.handle).unwrap();
        mapping
            .copy_one(&Allocation::Cuda(local.clone()), &pool, 11, 7, 17, false)
            .unwrap();
        drop(local);
        assert!(weak.upgrade().is_some());
        if fail_close {
            destination.ipc_close_error.store(1, Ordering::Release);
        }
        assert_eq!(mapping.close().is_err(), fail_close);
        drop(mapping);
        assert_eq!(weak.upgrade().is_some(), fail_close);
        if !fail_close {
            export.finish();
        }
    }
}

pub(super) fn complete_group(
    mut group: crate::cuda::ipc::IpcCopyGroup,
) -> crate::cuda::ipc::IpcCopyCompletion {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match group.query() {
            Ok(false) if std::time::Instant::now() < deadline => std::thread::yield_now(),
            Ok(false) => return group.complete(Err(Error::Timeout)),
            result => return group.complete(result.map(|_| ())),
        }
    }
}

pub(super) fn wait_query(device: &Device) {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while device.event_not_ready.load(Ordering::Acquire) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "missing pending event query"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

pub(super) fn deadline() -> std::time::Instant {
    std::time::Instant::now() + Duration::from_secs(2)
}
