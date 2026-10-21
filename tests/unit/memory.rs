use super::*;

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
