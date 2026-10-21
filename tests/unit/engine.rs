use super::*;

#[test]
fn source_close_is_permanent_and_timeout_is_not_a_drain_receipt() {
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    let permit = engine.source.admit().unwrap();
    assert!(matches!(
        engine.close_source(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert!(matches!(engine.source.admit(), Err(Error::SourceClosed)));
    assert!(matches!(
        engine.register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteRead),
        Err(Error::SourceClosed)
    ));
    assert!(matches!(
        engine.register_host_memory_batch(&[], Permission::RemoteRead),
        Err(Error::SourceClosed)
    ));
    drop(permit);
    engine.close_source(Duration::ZERO).unwrap();
    engine.close_source(Duration::from_secs(1)).unwrap();
}

#[test]
fn registration_batch_descriptor_limit_precedes_reference_changes() {
    let engine = TransferEngine::new(EngineConfig {
        max_batch_requests: 2,
        ..EngineConfig::default()
    })
    .unwrap();
    assert!(
        engine
            .register_host_memory_batch(&[], Permission::RemoteRead)
            .unwrap()
            .is_empty()
    );
    let allocation = Arc::new(HostMemory::new(vec![0; 8].into_boxed_slice()).unwrap());
    let original = engine
        .register_host_memory(Arc::clone(&allocation), 0, 8, Permission::RemoteRead)
        .unwrap();
    let ranges = vec![(allocation, 0, 8); 3];
    assert!(matches!(
        engine.register_host_memory_batch(&ranges, Permission::RemoteReadWrite),
        Err(Error::ResourceExhausted)
    ));
    engine.unregister_memory(&original).unwrap();
    assert!(matches!(
        engine.resolve_local_address(original.base_address(), 8),
        Err(Error::RegionNotFound)
    ));
}

#[test]
fn batch_admission_bounds_concurrent_operations_and_restores_capacity() {
    let engine = TransferEngine::new(EngineConfig {
        max_inflight_batches: 1,
        max_inflight_bytes: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    assert!(matches!(
        engine.admit_batch(9),
        Err(Error::ResourceExhausted)
    ));
    let permit = engine.admit_batch(8).unwrap();
    assert!(matches!(
        engine.admit_batch(0),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        engine.open_segment(
            engine.endpoint(),
            engine.source_epoch(),
            Duration::from_secs(1)
        ),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(engine.stats().unwrap().inflight_bytes, 8);
    drop(permit);
    assert_eq!(engine.stats().unwrap().inflight_batches, 0);
    engine.admit_batch(8).unwrap();
}

#[test]
fn invalid_configuration_fails_before_binding_or_spawning_threads() {
    let mut configs = vec![EngineConfig::default(); 12];
    configs[0].handler_threads = 0;
    configs[1].max_registered_regions = 4097;
    configs[2].max_transfer_bytes = (64 << 20) + 1;
    configs[3].max_inflight_bytes = 0;
    configs[4].handler_timeout = Duration::ZERO;
    configs[5].handler_timeout = Duration::from_secs(301);
    configs[6].max_pending_notifications = 0;
    configs[7].max_notification_bytes = (64 << 10) + 1;
    configs[8].max_pending_notification_bytes = 0;
    configs[9].max_pending_notification_bytes = 1024;
    configs[10].max_allocated_batches = 0;
    configs[11].max_allocated_requests = 0;
    for config in configs {
        assert!(matches!(
            TransferEngine::new(config),
            Err(Error::InvalidConfig(_))
        ));
    }
}
