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
    drop(permit);
    engine.close_source(Duration::ZERO).unwrap();
    engine.close_source(Duration::from_secs(1)).unwrap();
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
    let mut configs = vec![EngineConfig::default(); 6];
    configs[0].handler_threads = 0;
    configs[1].max_registered_regions = 4097;
    configs[2].max_transfer_bytes = (64 << 20) + 1;
    configs[3].max_inflight_bytes = 0;
    configs[4].handler_timeout = Duration::ZERO;
    configs[5].handler_timeout = Duration::from_secs(301);
    for config in configs {
        assert!(matches!(
            TransferEngine::new(config),
            Err(Error::InvalidConfig(_))
        ));
    }
}
