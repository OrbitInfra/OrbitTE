use super::*;
use crate::{MemoryLocation, MemoryType};

#[test]
fn owner_locations_bound_registrations_without_reprobing_or_waiting_for_payloads() {
    let owner = allocation(&[0x57; 32]);
    owner.ordinal.store(3, Ordering::Release);
    let weak = Arc::downgrade(&owner);
    // SAFETY: The Device owner keeps this boxed allocation at a fixed address; the fixture never
    // replaces the box.
    let address = unsafe { &*owner.bytes.get() }.as_ptr() as u64;
    let memory = Arc::new(CudaMemory::register(address + 4, 24, owner.clone(), driver()).unwrap());
    let expected = MemoryLocation::Cuda {
        device: 3,
        context: Arc::as_ptr(&owner) as usize,
    };
    let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([4; 16]), 2, 64);
    let mut registrations = registry
        .register_cuda_batch(
            &[(memory.clone(), 2, 8), (memory.clone(), 12, 4)],
            Permission::RemoteRead,
            |_| Ok(()),
        )
        .unwrap();
    let native_calls = owner.events.lock().unwrap().len();
    // Metadata queries neither consume producer events nor grant payload access.
    let mut access = memory.allocation.lock().unwrap();
    access.uncertain = true;
    for registration in &registrations {
        assert_eq!(registration.memory_type(), MemoryType::Cuda);
        assert!(!registration.ipc_requester());
        assert!(!registration.0.descriptor.ipc_exportable);
        let location = registration.location().unwrap();
        assert_eq!(location.address, registration.base_address());
        assert_eq!(location.length, registration.len());
        assert_eq!(location.location, expected);
        assert_eq!(location.location.to_string(), "cuda:3");
    }
    for (offset, len) in [(24, 1), (0, 0), (usize::MAX, 1)] {
        assert!(matches!(
            memory.location(offset, len),
            Err(Error::InvalidRange)
        ));
    }
    drop(access);
    assert!(matches!(
        memory.read(0, &mut [0; 1]),
        Err(Error::Unquiesced { .. })
    ));
    assert_eq!(owner.events.lock().unwrap().len(), native_calls);
    assert_eq!(
        owner
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "pointer-attributes")
            .count(),
        1
    );
    memory.allocation.lock().unwrap().uncertain = false;
    for registration in &registrations {
        registry.unregister(registration).unwrap();
    }
    drop(memory);
    drop(owner);
    assert!(weak.upgrade().is_some());
    assert_eq!(
        registrations.pop().unwrap().location().unwrap().location,
        expected
    );
    drop(registrations);
    assert!(weak.upgrade().is_none());
}

#[test]
fn pointer_probe_rejects_unknown_managed_bounds_and_driver_errors_without_host_access() {
    for (kind, managed, device, code, offset, len) in [
        (0, 0, 0, 0, 0, 16), // CUDA_SUCCESS does not establish a known pointer.
        (1, 0, 0, 0, 0, 16), // Host pointers require a HostMemory owner.
        (2, 1, 0, 0, 0, 16), // Managed memory is outside this profile.
        (2, 0, -1, 0, 0, 16),
        (2, 0, 0, 201, 0, 16),
        (2, 0, 0, 700, 0, 16),
        (2, 0, 0, 0, 8, 9),
        (2, 0, 0, 0, 0, 0),
    ] {
        let owner = allocation(&[0xa5; 16]);
        owner.memory_type.store(kind, Ordering::Release);
        owner.managed.store(managed, Ordering::Release);
        owner.ordinal.store(device, Ordering::Release);
        owner.probe_error.store(code, Ordering::Release);
        // SAFETY: The Device owner keeps this boxed allocation at a fixed address; the fixture
        // never replaces the box.
        let address = unsafe { &*owner.bytes.get() }.as_ptr() as u64;
        let result = CudaMemory::register(address + offset, len, owner.clone(), driver());
        if code != 0 {
            assert!(
                matches!(result, Err(Error::Backend { operation: "cuPointerGetAttributes", code: observed }) if observed == code)
            );
        } else {
            assert!(result.is_err());
        }
        // SAFETY: The pointer query only reads metadata and submits no copy to the owned
        // allocation.
        assert_eq!(unsafe { &**owner.bytes.get() }, &[0xa5; 16]);
        assert!(
            owner
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|e| ["pointer-attributes", "push", "pop"].contains(e))
        );
        assert!(STACK.with(|s| s.borrow().is_empty()));
    }
}

#[test]
fn pinned_host_location_stays_cpu_without_consuming_producer_readiness() {
    let owner = allocation(&[0; 16]);
    let gpu = memory(&owner);
    let context = gpu.allocation.lock().unwrap().context.clone();
    let pin = Arc::new(PinnedAllocation::new(&context, 16).unwrap());
    let host = Arc::new(HostMemory::from_cuda_pinned(pin));
    let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([5; 16]), 1, 16);
    let registration = registry
        .register_host(host.clone(), 3, 7, Permission::LocalOnly)
        .unwrap();
    let native_calls = owner.events.lock().unwrap().len();
    assert_eq!(registration.memory_type(), MemoryType::Cpu);
    assert_eq!(
        registration.location().unwrap(),
        host.location(3, 7).unwrap()
    );
    assert_eq!(
        registration.location().unwrap().location,
        MemoryLocation::Cpu(None)
    );
    assert_eq!(owner.events.lock().unwrap().len(), native_calls);
}

#[test]
fn selector_describe_publishes_cuda_owner_capabilities_without_native_access() {
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    let external_device = allocation(&[0; 32]);
    let external = memory(&external_device);
    let context = external.allocation.lock().unwrap().context.clone();
    let owned_device = allocation(&[0; 32]);
    let owned = super::ipc::owned(&owned_device, false);
    let exported_device = allocation(&[0; 32]);
    let exported = super::ipc::owned(&exported_device, true);
    let host = Arc::new(HostMemory::new(vec![0; 32].into_boxed_slice()).unwrap());
    let pinned = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
        PinnedAllocation::new(&context, 32).unwrap(),
    )));
    let mut registered = engine
        .register_cuda_memory_batch(
            &[
                (external.clone(), 0, 32),
                (owned.clone(), 0, 32),
                (exported.clone(), 0, 32),
            ],
            Permission::RemoteRead,
        )
        .unwrap();
    registered.extend(
        engine
            .register_host_memory_batch(
                &[(host.clone(), 0, 32), (pinned.clone(), 0, 32)],
                Permission::RemoteRead,
            )
            .unwrap(),
    );
    let events = external_device.events.lock().unwrap().len();
    // Describe and selector metadata must remain readable with payload owners locked.
    let _external = external.allocation.lock().unwrap();
    let _owned = owned.allocation.lock().unwrap();
    let _exported = exported.allocation.lock().unwrap();
    let _pin = pinned
        .cuda_pinned(std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
    let peer = engine
        .open_segment(
            engine.endpoint(),
            engine.source_epoch(),
            Duration::from_secs(1),
        )
        .unwrap();
    for (index, (kind, requester, exportable)) in [
        (MemoryType::Cuda, false, false),
        (MemoryType::Cuda, true, false),
        (MemoryType::Cuda, true, true),
        (MemoryType::Cpu, false, false),
        (MemoryType::Cpu, true, false),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(registered[index].ipc_requester(), requester);
        assert_eq!(peer.regions()[index].memory_type(), kind);
        assert_eq!(peer.regions()[index].ipc_exportable, exportable);
    }
    assert_eq!(external_device.events.lock().unwrap().len(), events);
}
