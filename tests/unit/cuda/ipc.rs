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
    let mut export = IpcExport::new(remote.clone()).unwrap();
    export.expose();
    assert_eq!(export.extent, 2 << 20);
    assert!(
        remote.read(100, &mut [0; 1]).is_err(),
        "even disjoint bytes remain loaned"
    );
    assert!(IpcExport::new(remote.clone()).is_err());
    for to_remote in [false, true, false] {
        copy_ipc(&local, export.handle, to_remote).unwrap();
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
    assert_eq!(events.iter().filter(|&&e| e == "stream-create").count(), 1);
    assert_eq!(events.iter().filter(|&&e| e == "ipc-open").count(), 3);
    assert_eq!(events.iter().filter(|&&e| e == "ipc-close").count(), 3);
    for (index, event) in events.iter().enumerate() {
        if *event == "ipc-close" {
            assert_eq!(&events[index - 3..index], &["event-sync", "pop", "push"]);
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
        let result = copy_ipc(&local, export.handle, true);
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
        assert_eq!(
            destination.imports.lock().unwrap().is_empty(),
            !uncertain || case == "restore-close",
            "{case}"
        );
        if uncertain {
            assert!(local.validate_ipc_requester().is_err());
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

fn copy_ipc(local: &Arc<CudaMemory>, handle: [u8; 64], to_remote: bool) -> Result<()> {
    let local = Allocation::Cuda(local.clone());
    let mut mapping = IpcMapping::new(&local)?;
    let result = mapping
        .open(handle)
        .and_then(|()| mapping.copy(&local, 11, 7, 17, to_remote));
    mapping.close()?;
    result
}

#[test]
fn ipc_subbatch_reuses_relocation_and_reports_every_affected_task() {
    for asynchronous in [false, true] {
        for case in [
            "success",
            "copy-second",
            "wait-second",
            "pop-second",
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
                });
            }
            match case {
                "copy-second" => owners[0].ipc_copy_fail_at.store(2, Ordering::Release),
                "wait-second" => owners[0].ipc_wait_fail_at.store(2, Ordering::Release),
                "pop-second" => owners[0].ipc_pop_fail_at.store(2, Ordering::Release),
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
            let closes = if matches!(case, "wait-second" | "pop-second") {
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
        let weak = Arc::downgrade(&local);
        let mut export = IpcExport::new(remote).unwrap();
        export.expose();
        let mut mapping = IpcMapping::new(&Allocation::Cuda(local.clone())).unwrap();
        mapping.open(export.handle).unwrap();
        mapping
            .copy(&Allocation::Cuda(local.clone()), 11, 7, 17, false)
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
