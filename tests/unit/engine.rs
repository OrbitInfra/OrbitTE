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
    engine.admit_batch(0).unwrap();
    engine.close(Duration::ZERO).unwrap();
    assert!(matches!(engine.admit_batch(0), Err(Error::RequesterClosed)));
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

#[cfg(feature = "rdma")]
fn rdma_fixture(mode: &str) -> TransferEngine {
    let config = EngineConfig::default();
    let epoch = SourceEpoch::generate().unwrap();
    let source = Arc::new(Source {
        epoch,
        rdma: Some(Arc::new(crate::rdma::tests::runtime(mode))),
        registry: MemoryRegistry::new(epoch, 8, 4096),
        notifications: NotificationInbox::new(8, 4096, 1024),
        admission: Mutex::new(SourceAdmission {
            open: true,
            active: 0,
        }),
        drained: Condvar::new(),
    });
    let transport = TcpTransport::start(Arc::clone(&source), &config).unwrap();
    TransferEngine {
        config,
        source,
        transport,
        batches: Arc::default(),
        allocations: Arc::new(AllocationBudget::new()),
    }
}

#[cfg(feature = "rdma")]
#[test]
fn remote_rdma_teardown_failure_retains_source_while_requester_is_locally_drained() {
    // Actual control sockets and owners; verbs are a CPU ABI fault fixture.
    // No DMA success or physical RNIC termination is claimed.
    let source = rdma_fixture("close1");
    let requester = rdma_fixture("normal");
    let backing = Arc::new(HostMemory::new(vec![0x42; 8].into_boxed_slice()).unwrap());
    let weak = Arc::downgrade(&backing);
    let remote = source
        .register_host_memory(Arc::clone(&backing), 0, 8, Permission::RemoteRead)
        .unwrap();
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
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[TransferRequest {
                transport: Transport::Rdma,
                local_address: local.base_address(),
                remote_address: remote.base_address(),
                length: 8,
            }],
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(
        report.locally_drained,
        "only the requester QP/MR was destroyed"
    );
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (0, 0)
    );
    let failure = report.failure.unwrap().error;
    assert!(
        matches!(
            failure,
            Error::Backend {
                operation: "remote backend",
                code: -1
            }
        ),
        "unexpected remote failure: {failure:?}"
    );
    let mut bytes = [0; 8];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(
        bytes, [0xcc; 8],
        "failed source receipt must not publish READ staging"
    );
    source.unregister_memory(&remote).unwrap();
    drop(remote);
    drop(backing);
    let stats = source.stats().unwrap();
    let rdma = stats.rdma.unwrap();
    assert_eq!(
        (
            stats.active_source_operations,
            stats.retained_regions,
            stats.retained_bytes
        ),
        (1, 1, 8)
    );
    assert_eq!(
        (
            rdma.active_endpoints,
            rdma.staging_bytes,
            rdma.retained_endpoints,
            rdma.retained_bytes
        ),
        (1, 8, 1, 8)
    );
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        source.close_source(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        source.source.rdma.as_ref().unwrap().endpoint(8, 0),
        Err(Error::Unavailable(_))
    ));
    requester.close_requester(Duration::from_secs(1)).unwrap();
    let local_stats = requester.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (local_stats.active_endpoints, local_stats.retained_endpoints),
        (0, 0)
    );
    requester.unregister_memory(&local).unwrap();
    drop(local);
}

#[cfg(feature = "rdma")]
#[test]
fn whole_close_fences_both_directions_and_waits_for_previously_accepted_work() {
    let engine = Arc::new(rdma_fixture("ok"));
    let source = engine.source.admit().unwrap();
    let requester = engine.admit_batch(8).unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let closer = {
        let engine = Arc::clone(&engine);
        std::thread::spawn(move || send.send(engine.close(Duration::from_secs(5))).unwrap())
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    while engine.source.admission.lock().unwrap().open {
        assert!(Instant::now() < deadline, "close did not fence admission");
        std::thread::yield_now();
    }
    assert!(matches!(engine.source.admit(), Err(Error::SourceClosed)));
    assert!(matches!(engine.admit_batch(0), Err(Error::RequesterClosed)));
    // Backend work accepted before the fence may still open its slice. Full
    // close must not close lazy device admission while that work is draining.
    let runtime = engine.source.rdma.as_ref().unwrap();
    let mut endpoint = runtime.endpoint(8, 0).unwrap();
    drop(source);
    assert!(matches!(
        receive.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    endpoint.close().unwrap();
    drop(endpoint);
    assert_eq!(runtime.stats().unwrap().active_devices, 1);
    drop(requester);
    receive
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    closer.join().unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    engine.close(Duration::ZERO).unwrap();
}

#[cfg(feature = "rdma")]
#[test]
fn whole_close_timeout_preserves_shared_device_and_can_continue_waiting() {
    let engine = rdma_fixture("ok");
    let runtime = engine.source.rdma.as_ref().unwrap();
    runtime.endpoint(8, 0).unwrap().close().unwrap();
    let source = engine.source.admit().unwrap();
    let requester = engine.admit_batch(8).unwrap();
    assert!(matches!(engine.close(Duration::ZERO), Err(Error::Timeout)));
    assert!(!engine.source.admission.lock().unwrap().open);
    assert!(!engine.batches.usage.lock().unwrap().open);
    assert_eq!(runtime.stats().unwrap().active_devices, 1);
    drop(source);
    assert!(matches!(engine.close(Duration::ZERO), Err(Error::Timeout)));
    assert_eq!(runtime.stats().unwrap().active_devices, 1);
    drop(requester);
    engine.close(Duration::ZERO).unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
}

#[cfg(feature = "rdma")]
#[test]
fn directional_close_keeps_other_direction_and_shared_device_available() {
    for source_first in [true, false] {
        let engine = rdma_fixture("ok");
        let runtime = engine.source.rdma.as_ref().unwrap();
        runtime.endpoint(8, 0).unwrap().close().unwrap();
        if source_first {
            engine.close_source(Duration::ZERO).unwrap();
            engine.admit_batch(0).unwrap();
        } else {
            engine.close_requester(Duration::ZERO).unwrap();
            engine.source.admit().unwrap();
        }
        runtime.endpoint(8, 0).unwrap().close().unwrap();
        assert_eq!(runtime.stats().unwrap().opened_devices, 1);
        assert_eq!(runtime.stats().unwrap().active_devices, 1);
        engine.close(Duration::ZERO).unwrap();
        assert_eq!(runtime.stats().unwrap().active_devices, 0);
    }
}

#[cfg(feature = "rdma")]
#[test]
fn whole_close_reports_device_failure_without_retrying_from_another_close() {
    let engine = rdma_fixture("close3");
    let runtime = engine.source.rdma.as_ref().unwrap();
    runtime.endpoint(8, 0).unwrap().close().unwrap();
    for _ in 0..2 {
        assert!(matches!(
            engine.close(Duration::ZERO),
            Err(Error::Unquiesced {
                operation: "ibv_dealloc_pd",
                code: 16
            })
        ));
    }
    let stats = runtime.stats().unwrap();
    assert_eq!((stats.active_devices, stats.retained_devices), (1, 1));
    assert!(matches!(engine.source.admit(), Err(Error::SourceClosed)));
    assert!(matches!(engine.admit_batch(0), Err(Error::RequesterClosed)));
}
