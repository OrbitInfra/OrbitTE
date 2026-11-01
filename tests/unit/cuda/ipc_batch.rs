use super::ipc::{complete_group, deadline, owned};
use super::*;
use crate::cuda::ipc::{IpcCopy, IpcExport, IpcMapping};
use crate::memory::Allocation;

fn local(device: &Arc<Device>, batch: bool) -> Arc<CudaMemory> {
    let mut api = driver();
    if batch {
        Arc::get_mut(&mut api).unwrap().copy_batch = Some(shim::copy_batch);
    }
    let bytes = unsafe { &*device.bytes.get() };
    let memory = Arc::new(
        CudaMemory::register(bytes.as_ptr() as u64, bytes.len(), device.clone(), api).unwrap(),
    );
    memory.allocation.lock().unwrap().owned = true;
    memory
}

fn pinned(memory: &Arc<CudaMemory>) -> Arc<HostMemory> {
    let context = memory.allocation.lock().unwrap().context.clone();
    Arc::new(HostMemory::from_cuda_pinned(Arc::new(
        PinnedAllocation::new(&context, 128).unwrap(),
    )))
}

#[test]
fn ipc_native_batch_read_write_mixed_owners_and_ordered_fallback() {
    for native in [false, true] {
        for to_remote in [false, true] {
            for alias in ["independent", "destination", "source"] {
                let overlap = alias == "destination";
                let source = allocation(&(0..128).collect::<Vec<_>>());
                let destination = allocation(&[0xa5; 128]);
                let local = local(&destination, native);
                let host = pinned(&local);
                host.write(0, &[0x5a; 128]).unwrap();
                let export = IpcExport::new(owned(&source, true)).unwrap();
                let owner = Allocation::Cuda(local.clone());
                let copies = [
                    IpcCopy {
                        memory: owner.clone(),
                        local_offset: 7,
                        remote_offset: 11,
                        length: 17,
                    },
                    IpcCopy {
                        memory: Allocation::Host(host.clone()),
                        local_offset: 40,
                        remote_offset: if overlap && to_remote { 11 } else { 40 },
                        length: 9,
                    },
                    IpcCopy {
                        memory: owner.clone(),
                        local_offset: if (overlap && !to_remote) || (alias == "source" && to_remote)
                        {
                            7
                        } else {
                            80
                        },
                        remote_offset: if alias == "source" && !to_remote {
                            11
                        } else {
                            70
                        },
                        length: 17,
                    },
                ];
                let pool = CUDAStreamPool::new(1).unwrap();
                let mut mapping = IpcMapping::new(&owner, deadline()).unwrap();
                mapping.open(export.handle).unwrap();
                let mut expected_remote = unsafe { &*source.bytes.get() }.to_vec();
                let mut expected_device = vec![0xa5; 128];
                let mut expected_host = vec![0x5a; 128];
                for copy in &copies {
                    let local = if matches!(&copy.memory, Allocation::Cuda(_)) {
                        &mut expected_device
                    } else {
                        &mut expected_host
                    };
                    let local = &mut local[copy.local_offset..copy.local_offset + copy.length];
                    let remote =
                        &mut expected_remote[copy.remote_offset..copy.remote_offset + copy.length];
                    if to_remote {
                        remote.copy_from_slice(local);
                    } else {
                        local.copy_from_slice(remote);
                    }
                }
                destination.events.lock().unwrap().clear();
                let completion = mapping.copy_group(
                    &copies,
                    to_remote,
                    pool.acquire(&owner, deadline()).unwrap(),
                    deadline(),
                    |_| Ok(()),
                    complete_group,
                );
                completion.result.unwrap();
                assert_eq!((completion.completed, completion.attempted), (3, 3));
                let events = destination.events.lock().unwrap().clone();
                let uses_batch = native && !overlap;
                assert_eq!(
                    events.iter().filter(|&&e| e == "copy-batch").count(),
                    usize::from(uses_batch)
                );
                assert_eq!(
                    events
                        .iter()
                        .filter(|&&e| matches!(e, "DtoD" | "HtoD" | "DtoH"))
                        .count(),
                    if uses_batch { 0 } else { 3 }
                );
                assert_eq!(events.iter().filter(|&&e| e == "record").count(), 1);
                assert!(!events.contains(&"stream-sync"));
                assert_eq!(&**unsafe { &*source.bytes.get() }, &expected_remote);
                assert_eq!(&**unsafe { &*destination.bytes.get() }, &expected_device);
                let mut actual_host = vec![0; 128];
                host.read(0, &mut actual_host).unwrap();
                assert_eq!(actual_host, expected_host);
                mapping.close().unwrap();
                assert!(local.allocation.lock().unwrap().dma.is_empty());
                assert_eq!(pool.stats().unwrap(), (1, 0, 0));
            }
        }
    }
}

#[test]
fn ipc_native_batch_cancel_only_submits_and_completes_admitted_prefix() {
    for accepted in 0..=2 {
        let source = allocation(&[0x5a; 128]);
        let destination = allocation(&[0xa5; 128]);
        let owner = Allocation::Cuda(local(&destination, true));
        let export = IpcExport::new(owned(&source, true)).unwrap();
        let copies = (0..3)
            .map(|i| IpcCopy {
                memory: owner.clone(),
                local_offset: i * 16,
                remote_offset: i * 16,
                length: 16,
            })
            .collect::<Vec<_>>();
        let pool = CUDAStreamPool::new(1).unwrap();
        let mut mapping = IpcMapping::new(&owner, deadline()).unwrap();
        mapping.open(export.handle).unwrap();
        let completion = mapping.copy_group(
            &copies,
            false,
            pool.acquire(&owner, deadline()).unwrap(),
            deadline(),
            |i| {
                if i < accepted {
                    Ok(())
                } else {
                    Err(Error::Cancelled)
                }
            },
            complete_group,
        );
        assert!(matches!(completion.result, Err(Error::Cancelled)));
        assert_eq!(
            (completion.completed, completion.attempted),
            (accepted, accepted)
        );
        assert_eq!(destination.ipc_copies.load(Ordering::Acquire), accepted);
        let actual = unsafe { &*destination.bytes.get() };
        assert!(actual[..accepted * 16].iter().all(|&b| b == 0x5a));
        assert!(actual[accepted * 16..].iter().all(|&b| b == 0xa5));
        mapping.close().unwrap();
    }
}

#[test]
fn ipc_native_batch_partial_error_never_replays_or_claims_a_completed_prefix() {
    for fault in ["batch", "batch-drain", "record", "query", "restore"] {
        let source = allocation(&[0x5a; 128]);
        let destination = allocation(&[0xa5; 128]);
        let local = local(&destination, true);
        let host = pinned(&local);
        let owner = Allocation::Cuda(local.clone());
        let export = IpcExport::new(owned(&source, true)).unwrap();
        let copies = [
            IpcCopy {
                memory: owner.clone(),
                local_offset: 0,
                remote_offset: 0,
                length: 16,
            },
            IpcCopy {
                memory: Allocation::Host(host.clone()),
                local_offset: 16,
                remote_offset: 16,
                length: 16,
            },
            IpcCopy {
                memory: owner.clone(),
                local_offset: 32,
                remote_offset: 32,
                length: 16,
            },
        ];
        let pool = CUDAStreamPool::new(1).unwrap();
        let mut mapping = IpcMapping::new(&owner, deadline()).unwrap();
        mapping.open(export.handle).unwrap();
        match fault {
            "batch" | "batch-drain" => destination.ipc_copy_fail_at.store(2, Ordering::Release),
            "record" => destination.record_error.store(1, Ordering::Release),
            "query" => destination.event_query_fail_at.store(1, Ordering::Release),
            "restore" => destination.ipc_pop_fail_at.store(1, Ordering::Release),
            _ => unreachable!(),
        }
        if fault == "batch-drain" {
            destination.sync_error.store(700, Ordering::Release);
        }
        destination.events.lock().unwrap().clear();
        let completion = mapping.copy_group(
            &copies,
            false,
            pool.acquire(&owner, deadline()).unwrap(),
            deadline(),
            |_| Ok(()),
            complete_group,
        );
        assert!(completion.result.is_err(), "{fault}");
        assert_eq!(
            (completion.completed, completion.attempted),
            (0, 3),
            "{fault}"
        );
        let uncertain = matches!(fault, "batch-drain" | "query" | "restore");
        assert_eq!(
            matches!(completion.result, Err(Error::Unquiesced { .. })),
            uncertain,
            "{fault}"
        );
        let events = destination.events.lock().unwrap().clone();
        assert_eq!(events.iter().filter(|&&e| e == "copy-batch").count(), 1);
        assert!(
            !events
                .iter()
                .any(|&e| matches!(e, "DtoD" | "HtoD" | "DtoH"))
        );
        assert_eq!(
            destination.submitted_copies.lock().unwrap().len(),
            if fault.starts_with("batch") { 2 } else { 3 }
        );
        assert_eq!(mapping.close().is_err(), uncertain);
        assert_eq!(host.read(0, &mut [0]).is_err(), uncertain);
        assert_eq!(local.allocation.lock().unwrap().uncertain, uncertain);
        assert_eq!(pool.stats().unwrap(), (1, 0, usize::from(uncertain)));
        if fault == "batch" {
            let bytes = unsafe { &*destination.bytes.get() };
            assert_eq!(&bytes[..16], &[0xa5; 16]);
            assert_eq!(&bytes[32..48], &[0x5a; 16]);
        }
        STACK.with(|s| s.borrow_mut().clear());
    }
}
