use super::*;

use crate::test_support as common;

use std::sync::atomic::Ordering;

#[test]
fn ranges_reject_empty_overflow_and_out_of_bounds() {
    for (offset, length, capacity) in [(0, 0, 8), (usize::MAX, 2, 8), (7, 2, 8), (9, 1, 8)] {
        assert!(
            matches!(
                checked_range(offset, length, capacity),
                Err(Error::InvalidRange)
            ),
            "{offset}/{length}/{capacity}"
        );
    }
    assert_eq!(checked_range(4, 4, 8).unwrap(), 4..8);
}

#[test]
fn retired_memory_keeps_budget_until_all_owners_release_it() {
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 1, 8);
    let memory = registry
        .register(vec![7; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let in_flight = registry.get(memory.id()).unwrap();
    registry.unregister(&memory).unwrap();
    assert!(matches!(
        registry.get(memory.id()),
        Err(Error::RegionNotFound)
    ));
    assert!(matches!(
        registry.register(vec![0; 8].into_boxed_slice(), Permission::RemoteRead),
        Err(Error::ResourceExhausted)
    ));
    let old_id = memory.id();
    drop(memory);
    assert_eq!(registry.usage().unwrap(), (1, 8));
    let mut observed = [0; 8];
    in_flight.read(0, &mut observed).unwrap();
    assert_eq!(observed, [7; 8]);
    drop(in_flight);
    assert_eq!(registry.usage().unwrap(), (0, 0));
    let replacement = registry
        .register(vec![0; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    assert!(replacement.id() > old_id);
}

#[test]
fn foreign_engine_registration_cannot_remove_same_numbered_region() {
    let a = MemoryRegistry::new(SourceEpoch([1; 16]), 2, 16);
    let b = MemoryRegistry::new(SourceEpoch([2; 16]), 2, 16);
    let first = a
        .register(vec![1; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let second = b
        .register(vec![2; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    assert_eq!(first.id(), second.id());
    assert!(matches!(
        b.unregister(&first),
        Err(Error::ForeignRegistration)
    ));
    assert_eq!(b.get(second.id()).unwrap().id(), second.id());
}

#[test]
fn permissions_and_export_visibility_are_distinct_from_local_access() {
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 4, 16);
    for (permission, read, write) in [
        (Permission::LocalOnly, false, false),
        (Permission::RemoteRead, true, false),
        (Permission::RemoteWrite, false, true),
        (Permission::RemoteReadWrite, true, true),
    ] {
        assert_eq!(permission.allows(false), read);
        assert_eq!(permission.allows(true), write);
        let memory = registry
            .register(vec![0; 4].into_boxed_slice(), permission)
            .unwrap();
        memory.write(1, &[1, 2]).unwrap();
        let mut bytes = [0; 4];
        memory.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0, 1, 2, 0]);
    }
    assert_eq!(registry.descriptors().unwrap().len(), 3);
}

#[test]
fn duplicate_references_keep_first_permission_and_charge_one_range() {
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 2, 16);
    let allocation = Arc::new(HostMemory::new(vec![1; 8].into_boxed_slice()).unwrap());
    let first = registry
        .register_host(Arc::clone(&allocation), 0, 8, Permission::RemoteRead)
        .unwrap();
    let duplicate = registry
        .register_host(Arc::clone(&allocation), 0, 8, Permission::RemoteReadWrite)
        .unwrap();
    assert_eq!(first.id(), duplicate.id());
    assert_eq!(duplicate.permission(), Permission::RemoteRead);
    assert_eq!(registry.usage().unwrap(), (1, 8));
    registry.unregister(&first).unwrap();
    assert_eq!(registry.get(first.id()).unwrap().id(), first.id());
    registry.unregister(&duplicate).unwrap();
    assert!(matches!(
        registry.get(first.id()),
        Err(Error::RegionNotFound)
    ));
    let replacement = registry
        .register_host(allocation, 0, 8, Permission::RemoteReadWrite)
        .unwrap();
    assert_ne!(first.id(), replacement.id());
    assert_eq!(registry.usage().unwrap(), (2, 16));
    drop(first);
    drop(duplicate);
    assert_eq!(registry.usage().unwrap(), (1, 8));
}

#[test]
fn alias_ranges_share_backing_and_resolve_like_tent() {
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 4, 64);
    let allocation = Arc::new(HostMemory::new(vec![0; 16].into_boxed_slice()).unwrap());
    let short = registry
        .register_host(Arc::clone(&allocation), 0, 8, Permission::RemoteReadWrite)
        .unwrap();
    let full = registry
        .register_host(Arc::clone(&allocation), 0, 16, Permission::RemoteReadWrite)
        .unwrap();
    let middle = registry
        .register_host(Arc::clone(&allocation), 4, 8, Permission::RemoteReadWrite)
        .unwrap();
    middle.write(0, &[7; 8]).unwrap();
    let mut bytes = [0; 8];
    short.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0, 0, 0, 0, 7, 7, 7, 7]);
    for offset in [0, 4, 8, 12] {
        let (resolved, relative) = registry
            .resolve_address(allocation.base_address() + offset, 4)
            .unwrap();
        assert_eq!((resolved.id(), relative), (full.id(), offset as usize));
    }
    for (address, length) in [
        (allocation.base_address() - 1, 1),
        (allocation.base_address() + 15, 2),
    ] {
        assert!(matches!(
            registry.resolve_address(address, length),
            Err(Error::RegionNotFound)
        ));
    }
    assert!(matches!(
        registry.resolve_address(u64::MAX, 2),
        Err(Error::InvalidRange)
    ));
}

#[test]
fn external_allocation_is_retained_by_retired_handles_and_failed_admission_is_atomic() {
    let fixture = common::external_memory(&[3; 8]);
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 1, 8);
    let memory = registry
        .register_host(
            Arc::clone(&fixture.memory),
            0,
            8,
            Permission::RemoteReadWrite,
        )
        .unwrap();
    assert!(matches!(
        registry.register(vec![0; 1].into_boxed_slice(), Permission::RemoteRead),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        registry.register_host(Arc::clone(&fixture.memory), 7, 2, Permission::RemoteRead),
        Err(Error::InvalidRange)
    ));
    registry.unregister(&memory).unwrap();
    drop(fixture.memory);
    assert!(fixture.owner.upgrade().is_some());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    memory.write(0, &[9; 8]).unwrap();
    let mut bytes = [0; 8];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [9; 8]);
    drop(memory);
    assert!(fixture.owner.upgrade().is_none());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(registry.usage().unwrap(), (0, 0));
}

#[test]
#[allow(unsafe_code)]
fn conflicting_owner_is_rejected_even_after_visibility_is_retired() {
    let fixture = common::external_memory(&[3; 8]);
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 3, 24);
    let memory = registry
        .register_host(Arc::clone(&fixture.memory), 0, 8, Permission::RemoteRead)
        .unwrap();
    let owner = fixture.owner.upgrade().unwrap();
    let address = NonNull::new(fixture.memory.base_address() as usize as *mut u8).unwrap();
    // SAFETY: this negative-control view is never dereferenced. The real owner
    // retains its stable bytes; every attempted registration below is rejected.
    let conflicting = Arc::new(unsafe { HostMemory::from_raw_parts(address, 8, owner) }.unwrap());
    assert!(matches!(
        registry.register_host(Arc::clone(&conflicting), 0, 8, Permission::RemoteRead),
        Err(Error::RegistrationConflict)
    ));
    registry.unregister(&memory).unwrap();
    assert!(matches!(
        registry.register_host(conflicting, 2, 4, Permission::RemoteRead),
        Err(Error::RegistrationConflict)
    ));
    assert_eq!(registry.usage().unwrap(), (1, 8));
}

#[test]
fn shared_host_allocation_serializes_aliases_across_engines() {
    let first = Arc::new(MemoryRegistry::new(SourceEpoch([1; 16]), 1, 4096));
    let second = Arc::new(MemoryRegistry::new(SourceEpoch([2; 16]), 1, 4096));
    let fixture = common::external_memory(&[0; 4096]);
    let a = first
        .register_host(
            Arc::clone(&fixture.memory),
            0,
            4096,
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let b = second
        .register_host(
            Arc::clone(&fixture.memory),
            0,
            4096,
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let writer = std::thread::spawn(move || {
        for index in 0..128 {
            a.write(0, &[if index % 2 == 0 { 1 } else { 2 }; 4096])
                .unwrap();
        }
    });
    for _ in 0..128 {
        let mut bytes = [0; 4096];
        b.read(0, &mut bytes).unwrap();
        assert!(
            bytes.iter().all(|byte| *byte == bytes[0]),
            "torn read across shared allocation aliases"
        );
    }
    writer.join().unwrap();
}

#[test]
fn batch_registration_preserves_order_references_and_shared_allocator_ownership() {
    let fixture = common::external_memory(&[3; 16]);
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 3, 28);
    let existing = registry
        .register_host(Arc::clone(&fixture.memory), 0, 4, Permission::RemoteRead)
        .unwrap();
    let ranges = vec![
        (Arc::clone(&fixture.memory), 0, 4),
        (Arc::clone(&fixture.memory), 4, 8),
        (Arc::clone(&fixture.memory), 4, 8),
        (Arc::clone(&fixture.memory), 0, 16),
    ];
    let batch = registry
        .register_host_batch(&ranges, Permission::RemoteReadWrite)
        .unwrap();
    assert_eq!(batch.len(), 4);
    assert_eq!(batch[0].id(), existing.id());
    assert_eq!(batch[0].permission(), Permission::RemoteRead);
    assert_eq!(batch[1].id(), batch[2].id());
    assert_eq!(batch[3].base_address(), fixture.memory.base_address());
    assert_eq!(registry.usage().unwrap(), (3, 28));
    batch[1].write(0, &[7; 8]).unwrap();
    let mut bytes = [0; 16];
    batch[3].read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [3, 3, 3, 3, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 3, 3]);
    registry.unregister(&existing).unwrap();
    registry.unregister(&batch[1]).unwrap();
    assert!(registry.get(batch[1].id()).is_ok());
    for index in [0, 2, 3] {
        registry.unregister(&batch[index]).unwrap();
    }
    assert!(registry.descriptors().unwrap().is_empty());
    drop(existing);
    drop(ranges);
    drop(fixture.memory);
    assert!(fixture.owner.upgrade().is_some());
    assert_eq!(registry.usage().unwrap(), (3, 28));
    drop(batch);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(registry.usage().unwrap(), (0, 0));
}

#[test]
fn batch_registration_failures_leave_no_new_ranges_or_duplicate_references() {
    for case in ["range", "bytes", "regions", "id", "references"] {
        let registry = MemoryRegistry::new(
            SourceEpoch([1; 16]),
            if case == "regions" { 1 } else { 4 },
            if case == "bytes" { 7 } else { 64 },
        );
        let allocation = Arc::new(HostMemory::new(vec![0; 16].into_boxed_slice()).unwrap());
        let existing = registry
            .register_host(Arc::clone(&allocation), 0, 4, Permission::RemoteRead)
            .unwrap();
        {
            let mut entries = registry.entries.lock().unwrap();
            if case == "id" {
                entries.next_id = u64::MAX;
            }
            if case == "references" {
                entries.regions.get_mut(&existing.id()).unwrap().references = usize::MAX;
            }
        }
        let next_id = registry.entries.lock().unwrap().next_id;
        let references = registry.entries.lock().unwrap().regions[&existing.id()].references;
        let ranges = [
            (Arc::clone(&allocation), 0, 4),
            (Arc::clone(&allocation), 4, 4),
            (
                Arc::clone(&allocation),
                if case == "range" { 15 } else { 8 },
                4,
            ),
        ];
        let error = registry
            .register_host_batch(&ranges, Permission::RemoteReadWrite)
            .unwrap_err();
        assert!(
            if case == "range" {
                matches!(error, Error::InvalidRange)
            } else {
                matches!(error, Error::ResourceExhausted)
            },
            "{case}: {error}"
        );
        assert_eq!(registry.usage().unwrap(), (1, 4), "{case}");
        assert_eq!(registry.descriptors().unwrap().len(), 1, "{case}");
        let mut entries = registry.entries.lock().unwrap();
        assert_eq!(entries.next_id, next_id, "{case}");
        assert_eq!(
            entries.regions[&existing.id()].references,
            references,
            "{case}"
        );
        entries.regions.get_mut(&existing.id()).unwrap().references = 1;
        drop(entries);
        registry.unregister(&existing).unwrap();
        assert!(registry.descriptors().unwrap().is_empty(), "{case}");
    }
}

#[test]
#[allow(unsafe_code)]
fn batch_rejects_foreign_alias_locks_before_publication_or_reference_changes() {
    for retired in [false, true] {
        let fixture = common::external_memory(&[0; 16]);
        let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 4, 64);
        let address = NonNull::new(fixture.memory.base_address() as usize as *mut u8).unwrap();
        // SAFETY: this negative-control wrapper retains the real owner and is
        // never dereferenced. Its registration must fail before publication.
        let conflicting = Arc::new(
            unsafe { HostMemory::from_raw_parts(address, 16, fixture.owner.upgrade().unwrap()) }
                .unwrap(),
        );
        let existing = if retired {
            let memory = registry
                .register_host(Arc::clone(&fixture.memory), 0, 8, Permission::RemoteRead)
                .unwrap();
            registry.unregister(&memory).unwrap();
            Some(memory)
        } else {
            None
        };
        let before = registry.usage().unwrap();
        let ranges = [(Arc::clone(&fixture.memory), 0, 8), (conflicting, 4, 8)];
        assert!(matches!(
            registry.register_host_batch(&ranges, Permission::RemoteRead),
            Err(Error::RegistrationConflict)
        ));
        assert!(registry.descriptors().unwrap().is_empty());
        assert_eq!(registry.usage().unwrap(), before);
        drop(existing);
    }
}

#[cfg(feature = "rdma")]
#[test]
fn dma_ranges_fence_aliases_across_engines_without_blocking_disjoint_bytes() {
    for external in [false, true] {
        let fixture = common::external_memory(&[0x55; 32]);
        let backing = if external {
            Arc::clone(&fixture.memory)
        } else {
            Arc::new(HostMemory::new(vec![0x55; 32].into_boxed_slice()).unwrap())
        };
        let first = MemoryRegistry::new(SourceEpoch([1; 16]), 4, 64);
        let second = MemoryRegistry::new(SourceEpoch([2; 16]), 4, 64);
        let full = first
            .register_host(Arc::clone(&backing), 0, 32, Permission::RemoteReadWrite)
            .unwrap();
        let alias = second
            .register_host(Arc::clone(&backing), 4, 16, Permission::RemoteReadWrite)
            .unwrap();
        let loan = alias.host_dma_lease(2, 6).unwrap().unwrap();
        assert_eq!(loan.address().as_ptr() as u64, backing.base_address() + 6);
        assert_eq!(loan.len(), 6);
        assert!(matches!(
            full.host_dma_lease(5, 2),
            Err(Error::Unavailable(_))
        ));
        assert!(matches!(
            full.host_dma_lease(11, 2),
            Err(Error::Unavailable(_))
        ));
        assert!(matches!(
            alias.read(1, &mut [0; 2]),
            Err(Error::Unavailable(_))
        ));
        assert!(matches!(
            backing.write(11, &[0; 2]),
            Err(Error::Unavailable(_))
        ));
        let disjoint = full.host_dma_lease(12, 4).unwrap().unwrap();
        full.write(0, &[9; 6]).unwrap();
        full.write(16, &[7; 16]).unwrap();
        let mut bytes = [0; 6];
        full.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [9; 6]);
        drop(loan);
        alias.write(2, &[3; 6]).unwrap();
        assert!(matches!(
            full.read(12, &mut [0; 4]),
            Err(Error::Unavailable(_))
        ));
        drop(disjoint);
        let mut expected = vec![0x55; 32];
        expected[..6].fill(9);
        expected[6..12].fill(3);
        expected[16..].fill(7);
        let mut observed = vec![0; 32];
        full.read(0, &mut observed).unwrap();
        assert_eq!(observed, expected);
    }
}

#[cfg(feature = "rdma")]
#[test]
fn direct_loan_retains_registration_charge_and_actual_external_allocation() {
    let fixture = common::external_memory(&[7; 16]);
    let registry = MemoryRegistry::new(SourceEpoch([1; 16]), 1, 8);
    let memory = registry
        .register_host(
            Arc::clone(&fixture.memory),
            4,
            8,
            Permission::RemoteReadWrite,
        )
        .unwrap();
    for (offset, length) in [(0, 0), (7, 2), (usize::MAX, 1)] {
        assert!(matches!(
            memory.host_dma_lease(offset, length),
            Err(Error::InvalidRange)
        ));
    }
    let loan = memory.host_dma_lease(1, 3).unwrap().unwrap();
    registry.unregister(&memory).unwrap();
    drop(memory);
    drop(fixture.memory);
    assert_eq!(registry.usage().unwrap(), (1, 8));
    assert!(fixture.owner.upgrade().is_some());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    assert!(matches!(
        registry.register(vec![0; 8].into_boxed_slice(), Permission::LocalOnly),
        Err(Error::ResourceExhausted)
    ));
    drop(loan);
    assert_eq!(registry.usage().unwrap(), (0, 0));
    assert!(fixture.owner.upgrade().is_none());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
}
