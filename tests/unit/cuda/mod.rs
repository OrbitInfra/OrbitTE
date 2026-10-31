#![allow(unsafe_code)]

use std::time::Duration;

use super::*;
use crate::{
    BatchState, EngineConfig, Permission, TransferEngine, TransferOp, TransferRequest, Transport,
};

mod connect_progress;
#[cfg(feature = "rdma")]
mod finish_progress;
#[cfg(feature = "rdma")]
mod grant_progress;
mod ipc;
mod ipc_cache;
mod ipc_host;
#[cfg(feature = "rdma")]
mod mixed_batches;
#[cfg(feature = "rdma")]
mod rdma;
mod remote_progress;
mod remote_staging;
mod requester_failures;
mod requester_pipeline;
mod requester_staging;
#[cfg(feature = "rdma")]
mod retirement_progress;
#[cfg(feature = "rdma")]
mod setup_progress;
mod shim;
mod socket_gate;
mod socket_progress;
mod staging;
#[cfg(feature = "rdma")]
mod staging_cross;
mod staging_events;
mod staging_location;
mod staging_pipeline;
mod staging_progress;
mod staging_shards;
use shim::*;

#[test]
fn cuda_registration_checks_type_extent_and_restores_context_on_copy_errors() {
    let owner = allocation(&[0x5a; 16]);
    let address = unsafe { &*owner.bytes.get() }.as_ptr() as u64;
    owner.memory_type.store(1, Ordering::Release);
    assert!(matches!(
        CudaMemory::register(address, 16, owner.clone(), driver()),
        Err(Error::Unavailable(_))
    ));
    owner.memory_type.store(2, Ordering::Release);
    assert!(matches!(
        CudaMemory::register(address + 8, 9, owner.clone(), driver()),
        Err(Error::InvalidRange)
    ));
    owner.managed.store(1, Ordering::Release);
    assert!(matches!(
        CudaMemory::register(address, 16, owner.clone(), driver()),
        Err(Error::Unavailable(_))
    ));
    owner.managed.store(0, Ordering::Release);
    let memory = memory(&owner);
    memory.write(4, b"orbit").unwrap();
    let mut bytes = [0; 16];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[..4], &[0x5a; 4]);
    assert_eq!(&bytes[4..9], b"orbit");
    assert_eq!(&bytes[9..], &[0x5a; 7]);
    owner.copy_error.store(1, Ordering::Release);
    let mut untouched = [0xcc; 8];
    assert!(matches!(
        memory.read(4, &mut untouched),
        Err(Error::Backend { code: 1, .. })
    ));
    assert_eq!(untouched, [0xcc; 8]);
    assert!(owner.pins.lock().unwrap().is_empty());
    assert!(STACK.with(|s| s.borrow().is_empty()));
    assert!(matches!(
        memory.write(1, &[0x19; 4]),
        Err(Error::Backend { code: 1, .. })
    ));
    owner.copy_error.store(0, Ordering::Release);
    let mut changed = [0; 16];
    memory.read(0, &mut changed).unwrap();
    assert_eq!(changed[1], 0x19);
    assert_eq!(changed[2], 0x5a);
    let events = owner.events.lock().unwrap();
    let error_copy = events
        .iter()
        .enumerate()
        .filter(|(_, e)| **e == "DtoH")
        .nth(1)
        .unwrap()
        .0;
    assert_eq!(
        &events[error_copy..error_copy + 3],
        &["DtoH", "stream-sync", "pop"]
    );
}

#[test]
fn cuda_registered_ranges_use_actual_owners_and_tcp_read_write_paths() {
    let owner = allocation(&[0x7b; 32]);
    let weak = Arc::downgrade(&owner);
    let allocation = memory(&owner);
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let source_memory = source
        .register_cuda_memory_batch(
            &[(allocation.clone(), 0, 32), (allocation.clone(), 0, 32)],
            Permission::RemoteReadWrite,
        )
        .unwrap();
    assert_eq!(source_memory[0].id(), source_memory[1].id());
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let local = requester
        .register_memory(vec![0xa5; 32].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(2),
        )
        .unwrap();
    let request = TransferRequest {
        remote_staging: false,
        transport: Transport::Tcp,
        local_address: local.base_address() + 5,
        remote_address: source_memory[0].base_address() + 7,
        length: 13,
    };
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&request),
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(report.is_complete() && report.locally_drained);
    let mut bytes = [0; 32];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[5..18], &[0x7b; 13]);
    assert_eq!(&bytes[..5], &[0xa5; 5]);
    assert_eq!(&bytes[18..], &[0xa5; 14]);
    local.write(5, &[0x39; 13]).unwrap();
    assert!(
        requester
            .submit_and_wait(TransferOp::Write, &peer, &[request], Duration::from_secs(2))
            .unwrap()
            .is_complete()
    );
    source_memory[0].read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[7..20], &[0x39; 13]);
    assert_eq!(&bytes[..7], &[0x7b; 7]);
    assert_eq!(&bytes[20..], &[0x7b; 12]);
    for region in &source_memory {
        source.unregister_memory(region).unwrap();
    }
    drop(owner);
    drop(allocation);
    assert!(weak.upgrade().is_some());
    drop(source_memory);
    // A wire ACK precedes the source handler dropping its physical owners.
    source.close_source(Duration::from_secs(2)).unwrap();
    // Idle chunks retain their actual context until whole-engine close.
    assert!(weak.upgrade().is_some());
    source.close(Duration::from_secs(2)).unwrap();
    assert!(weak.upgrade().is_none());
}

#[test]
fn unproven_cuda_copy_keeps_batch_backing_capacity_and_requester_admission() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let remote = source
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            max_allocated_batches: 1,
            max_inflight_batches: 1,
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let owner = allocation(&[0xcc; 8]);
    let weak = Arc::downgrade(&owner);
    let allocation = memory(&owner);
    let memory = requester
        .register_cuda_memory_batch(&[(allocation.clone(), 0, 8)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(2),
        )
        .unwrap();
    owner.sync_error.store(700, Ordering::Release);
    let mut batch = requester
        .submit_async(
            TransferOp::Read,
            &peer,
            &[TransferRequest {
                remote_staging: false,
                transport: Transport::Tcp,
                local_address: memory.base_address(),
                remote_address: remote.base_address(),
                length: 8,
            }],
            None,
            Duration::from_secs(2),
        )
        .unwrap();
    let status = batch.wait(Duration::from_secs(2)).unwrap();
    assert_eq!(status.state, BatchState::Failed);
    assert!(!status.locally_drained);
    assert!(!batch.task_status(0).unwrap().locally_drained);
    assert!(!batch.submissions().unwrap()[0].locally_drained);
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    requester.unregister_memory(&memory).unwrap();
    drop(memory);
    drop(allocation);
    drop(owner);
    assert!(weak.upgrade().is_some());
    drop(batch);
    assert!(matches!(
        requester.allocate_batch(1),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        requester.close_requester(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    let stats = requester.stats().unwrap();
    assert_eq!(
        (stats.staging.retained_chunks, stats.staging.reserved_bytes),
        (1, 4 << 20)
    );
    assert_eq!(
        (
            stats.allocated_batches,
            stats.inflight_batches,
            stats.retained_regions
        ),
        (1, 1, 1)
    );
}

#[test]
fn source_cuda_sync_failure_retains_source_owner_and_prevents_false_drain() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let owner = allocation(&[0x77; 8]);
    let weak = Arc::downgrade(&owner);
    let allocation = memory(&owner);
    let memory = source
        .register_cuda_memory_batch(&[(allocation.clone(), 0, 8)], Permission::RemoteRead)
        .unwrap()
        .remove(0);
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let local = requester
        .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(2),
        )
        .unwrap();
    owner.sync_error.store(700, Ordering::Release);
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[TransferRequest {
                remote_staging: false,
                transport: Transport::Tcp,
                local_address: local.base_address(),
                remote_address: memory.base_address(),
                length: 8,
            }],
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(!report.is_complete());
    assert!(report.locally_drained);
    let mut bytes = [0; 8];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0xcc; 8]);
    source.unregister_memory(&memory).unwrap();
    drop(memory);
    drop(allocation);
    drop(owner);
    assert!(matches!(
        source.close_source(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert_eq!(source.stats().unwrap().active_source_operations, 1);
    assert_eq!(source.stats().unwrap().staging.retained_chunks, 1);
    assert!(weak.upgrade().is_some());
}

#[test]
fn cuda_sync_restore_and_unpin_failures_retain_native_backing_and_poison_reuse() {
    for mode in ["sync", "restore", "copy-restore", "unpin"] {
        std::thread::spawn(move || {
            let owner = allocation(&[0x7b; 8]);
            let weak = Arc::downgrade(&owner);
            let memory = memory(&owner);
            match mode {
                "sync" => owner.sync_error.store(700, Ordering::Release),
                "restore" => owner.pop_error.store(201, Ordering::Release),
                "copy-restore" => owner.pop_after_completion.store(true, Ordering::Release),
                "unpin" => owner.free_error.store(1, Ordering::Release),
                _ => unreachable!(),
            }
            let mut bytes = [0xcc; 8];
            let result = memory.read(0, &mut bytes);
            if mode == "unpin" {
                assert!(result.is_ok());
                assert_eq!(bytes, [0x7b; 8]);
            } else {
                assert!(matches!(result, Err(Error::Unquiesced { .. })));
                assert_eq!(bytes, [0xcc; 8]);
            }
            assert!(!owner.pins.lock().unwrap().is_empty());
            assert!(matches!(
                memory.read(0, &mut bytes),
                Err(Error::Unquiesced { .. })
            ));
            drop(memory);
            drop(owner);
            assert!(weak.upgrade().is_some());
        })
        .join()
        .unwrap();
    }
}

#[test]
fn cuda_copy_reuses_its_stream_and_event_without_context_synchronization() {
    let owner = allocation(&[0x44; 32]);
    let memory = memory(&owner);
    let mut threads = Vec::new();
    for offset in 0..8 {
        let memory = Arc::clone(&memory);
        threads.push(std::thread::spawn(move || {
            memory.write(offset, &[offset as u8]).unwrap();
            let mut bytes = [0; 1];
            memory.read(offset, &mut bytes).unwrap();
            assert_eq!(bytes, [offset as u8]);
            assert!(STACK.with(|s| s.borrow().is_empty()));
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(owner.pins.lock().unwrap().is_empty());
    drop(memory);
    assert!(owner.streams.lock().unwrap().is_empty());
    assert!(owner.completions.lock().unwrap().is_empty());
    let events = owner.events.lock().unwrap();
    for name in [
        "stream-create",
        "event-create",
        "stream-destroy",
        "event-destroy",
    ] {
        assert_eq!(events.iter().filter(|e| **e == name).count(), 1, "{name}");
    }
    assert_eq!(events.iter().filter(|e| **e == "event-sync").count(), 16);
    assert!(!events.contains(&"sync"));
    assert!(!events.contains(&"stream-sync"));
}

#[test]
fn cuda_stream_partial_construction_and_record_failure_release_only_after_drain() {
    for mode in ["stream-create", "event-create", "record"] {
        let owner = allocation(&[0x55; 8]);
        let memory = memory(&owner);
        match mode {
            "stream-create" => owner.stream_create_error.store(1, Ordering::Release),
            "event-create" => owner.event_create_error.store(1, Ordering::Release),
            "record" => owner.record_error.store(1, Ordering::Release),
            _ => unreachable!(),
        }
        let mut bytes = [0xcc; 8];
        assert!(
            matches!(memory.read(0, &mut bytes), Err(Error::Backend { .. })),
            "{mode}"
        );
        assert_eq!(bytes, [0xcc; 8]);
        assert!(owner.pins.lock().unwrap().is_empty());
        assert!(!owner.events.lock().unwrap().contains(&"event-sync"));
        assert_eq!(
            owner.events.lock().unwrap().contains(&"stream-sync"),
            mode == "record"
        );
        owner.stream_create_error.store(0, Ordering::Release);
        owner.event_create_error.store(0, Ordering::Release);
        owner.record_error.store(0, Ordering::Release);
        memory.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0x55; 8]);
        drop(memory);
        assert!(owner.streams.lock().unwrap().is_empty());
        assert!(owner.completions.lock().unwrap().is_empty());
    }
}

#[test]
fn cuda_copy_completion_and_destroy_failures_retain_native_owners() {
    for mode in [
        "submit-drain",
        "record-drain",
        "event-wait",
        "event-destroy",
        "stream-destroy",
    ] {
        let owner = allocation(&[0x66; 8]);
        let weak = Arc::downgrade(&owner);
        let memory = memory(&owner);
        if mode == "record-drain" {
            // Leave an older, completed event behind. It must never stand in
            // for a failed record of the next copy.
            memory.read(0, &mut [0; 8]).unwrap();
            owner.events.lock().unwrap().clear();
        }
        let pending = matches!(mode, "submit-drain" | "record-drain" | "event-wait");
        if pending {
            owner.sync_error.store(700, Ordering::Release);
        }
        match mode {
            "submit-drain" => owner.copy_error.store(1, Ordering::Release),
            "record-drain" => owner.record_error.store(1, Ordering::Release),
            "event-wait" => (),
            "event-destroy" => owner.event_destroy_error.store(1, Ordering::Release),
            "stream-destroy" => owner.stream_destroy_error.store(1, Ordering::Release),
            _ => unreachable!(),
        }
        let mut bytes = [0xcc; 8];
        let result = memory.read(0, &mut bytes);
        if pending {
            assert!(matches!(result, Err(Error::Unquiesced { .. })), "{mode}");
            assert_eq!(bytes, [0xcc; 8]);
            assert!(matches!(
                memory.read(0, &mut bytes),
                Err(Error::Unquiesced { .. })
            ));
        } else {
            result.unwrap();
            assert_eq!(bytes, [0x66; 8]);
        }
        drop(memory);
        assert_eq!(!owner.pins.lock().unwrap().is_empty(), pending, "{mode}");
        assert!(!owner.streams.lock().unwrap().is_empty(), "{mode}");
        assert_eq!(
            owner.completions.lock().unwrap().is_empty(),
            mode == "stream-destroy"
        );
        if pending {
            let events = owner.events.lock().unwrap();
            assert!(!events.contains(&"event-destroy"));
            assert!(!events.contains(&"stream-destroy"));
        }
        assert!(STACK.with(|s| s.borrow().is_empty()));
        drop(owner);
        assert!(weak.upgrade().is_some(), "{mode}");
    }
}
