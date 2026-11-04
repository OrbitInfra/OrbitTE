use super::*;
#[cfg(feature = "rdma")]
use crate::test_support::EVENTUAL_PROGRESS;

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

#[cfg(target_os = "linux")]
#[test]
fn source_close_fences_shm_before_an_unrelated_admission_finishes() -> Result<()> {
    let mut c = EngineConfig::default();
    c.shm.enable = true;
    c.tcp = false;
    let src = TransferEngine::new(c.clone())?;
    let dst = TransferEngine::new(c)?;
    let host = src.allocate_shm_memory(8)?;
    let remote = src.register_host_memory(host.clone(), 0, 8, Permission::RemoteReadWrite)?;
    let local = dst.register_memory(vec![0xab; 8].into(), Permission::LocalOnly)?;
    let wait = crate::test_support::EVENTUAL_PROGRESS;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), wait)?;
    let permit = src.source.admit()?;
    assert!(matches!(
        src.close_source(Duration::ZERO),
        Err(Error::Timeout)
    ));
    let q = TransferRequest::new(
        Transport::Shm,
        local.base_address() + 1,
        remote.base_address() + 2,
        4,
    );
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[q], wait),
        Err(Error::RegionNotFound)
    ));
    let mut actual = [0xff; 8];
    host.read(0, &mut actual)?;
    assert_eq!(actual, [0; 8]);
    drop(permit);
    src.close_source(wait)?;
    Ok(())
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
    let mut configs = vec![EngineConfig::default(); 14];
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
    configs[12].runtime_queue = Some(crate::RuntimeQueueConfig {
        max_dispatch_owners: 0,
        ..Default::default()
    });
    configs[13].runtime_queue = Some(crate::RuntimeQueueConfig {
        max_dispatch_bytes: 0,
        ..Default::default()
    });
    for config in configs {
        assert!(matches!(
            TransferEngine::new(config),
            Err(Error::InvalidConfig(_))
        ));
    }
}

#[cfg(feature = "rdma")]
pub(crate) fn rdma_fixture(mode: &str) -> TransferEngine {
    let runtime = if let Some(mode) = mode.strip_prefix("events:") {
        crate::transport::rdma::tests::event_runtime(mode)
    } else {
        crate::transport::rdma::tests::runtime(mode)
    };
    rdma_fixture_with_runtime(runtime)
}

#[cfg(feature = "rdma")]
pub(crate) fn rdma_fixture_with_runtime(
    runtime: crate::transport::rdma::RdmaTransport,
) -> TransferEngine {
    // These single-backend fault controls inspect the exact native failure.
    rdma_fixture_with_config(
        runtime,
        EngineConfig {
            max_failover_attempts: 0,
            handler_timeout: EVENTUAL_PROGRESS,
            ..Default::default()
        },
    )
}

#[cfg(feature = "rdma")]
pub(crate) fn rdma_fixture_with_config(
    mut runtime: crate::transport::rdma::RdmaTransport,
    config: EngineConfig,
) -> TransferEngine {
    if config
        .selector
        .as_ref()
        .is_some_and(|s| !s.qp_pools.is_empty() || s.qp_mul_factor.is_some())
    {
        runtime.qp_layout = Some(Arc::new(TransferEngine::qp_layout(&config).unwrap()));
    }
    let epoch = SourceEpoch::generate().unwrap();
    let hp_listener = config
        .hp_tcp
        .enable
        .then(|| config.hp_tcp.bind(config.bind_addr).unwrap());
    let source = Arc::new(Source {
        #[cfg(target_os = "linux")]
        shm: config.shm.enable.then(|| {
            crate::transport::shm::ShmTransport::new(
                &config.shm,
                epoch,
                config.max_registered_regions,
                config.max_registered_bytes,
            )
            .unwrap()
        }),
        metadata: SegmentMetadata {
            rdma_direct_gpu: runtime.direct_gpu_ranges(),
            tcp_disabled: !config.tcp,
            shm: config.shm.enable.then(|| {
                Arc::new(crate::transport::shm::wire::Manifest {
                    cxl: !config.shm.cxl_mount_path.as_os_str().is_empty(),
                    entries: Vec::new(),
                })
            }),
            boot: config
                .shm
                .enable
                .then(|| crate::segment::ProcessIdentity::boot_id().unwrap()),
            hp: hp_listener
                .as_ref()
                .map(|(_, endpoint)| Arc::new(*endpoint)),
            max_transfer_bytes: config.max_transfer_bytes as u64,
            rdma: true,
            #[cfg(feature = "cuda")]
            remote_staging: config.remote_staging,
            ..Default::default()
        },
        credit_issuer: config.receiver_credit.map(ReceiverCreditIssuer::new),
        #[cfg(feature = "cuda")]
        remote_staging: config.remote_staging,
        #[cfg(feature = "cuda")]
        staging_pipeline_depth: config.staging_pipeline_depth,
        #[cfg(feature = "cuda")]
        proxy: Arc::clone(&runtime.proxy),
        #[cfg(feature = "cuda")]
        nvlink: None,
        epoch,
        rdma: Some(Arc::new(runtime)),
        registry: MemoryRegistry::new(
            epoch,
            config.max_registered_regions,
            config.max_registered_bytes,
        ),
        tcp_notifications: config.tcp_notifications,
        notifications: NotificationInbox::new(8, 4096, 1024),
        admission: Mutex::new(SourceAdmission {
            open: true,
            active: 0,
        }),
        drained: Condvar::new(),
    });
    let hp_tcp = hp_listener.map(|(listener, _)| {
        crate::transport::hp_tcp::HpTcpTransport::start(&config.hp_tcp, listener, source.clone())
            .unwrap()
    });
    let control = ControlService::start(Arc::clone(&source), &config).unwrap();
    let runtime_queue = config.runtime_queue.map(LocalTransferAdmissionQueue::new);
    let receiver_credits = (config.max_receiver_credit_sessions != 0).then(|| {
        SenderCreditLedger::new(config.max_receiver_credit_sessions, runtime_queue.as_ref())
    });
    let allocations = Arc::new(AllocationBudget::new());
    let progress_worker =
        TransferEngine::start_progress(&config, &allocations, &runtime_queue, &source).unwrap();
    let engine = TransferEngine {
        hp_tcp,
        #[cfg(feature = "metrics")]
        metrics: None,
        notification_routes: NotificationRoutes::new(&config),
        progress_worker,
        files: FileSegments::default(),
        #[cfg(feature = "io-uring")]
        io_uring: None,
        #[cfg(feature = "gds")]
        gds: None,
        receiver_credits,
        qos: config
            .qos
            .as_ref()
            .map(|qos| QosContractResolver::new(qos, runtime_queue.as_ref()))
            .transpose()
            .unwrap(),
        runtime_queue,
        selector: TransferEngine::compile_selector(
            &config,
            &TransferEngine::qp_layout(&config).unwrap(),
            #[cfg(feature = "rdma")]
            source.rdma.as_deref(),
        )
        .unwrap(),
        config,
        source,
        control,
        batches: Arc::default(),
        allocations,
    };
    #[cfg(feature = "metrics")]
    let engine = {
        let cfg = engine
            .config
            .metrics
            .load(engine.config.metrics_config_file.as_deref())
            .unwrap();
        engine.install_metrics(&cfg).unwrap()
    };
    engine
}

#[cfg(feature = "rdma")]
pub(crate) fn source_runtime(engine: &TransferEngine) -> &crate::transport::rdma::RdmaTransport {
    engine.source.rdma.as_ref().unwrap()
}

#[cfg(feature = "rdma")]
#[test]
fn remote_rdma_teardown_failure_retains_source_while_requester_is_locally_drained() {
    // Actual control sockets and owners; verbs are a CPU ABI fault fixture.
    // No DMA success or physical RNIC termination is claimed.
    let source = rdma_fixture_with_config(
        crate::transport::rdma::tests::runtime("close1"),
        EngineConfig {
            receiver_credit: Some(crate::ReceiverCreditConfig {
                bytes_per_sender: 8,
                requests_per_sender: 1,
                ..Default::default()
            }),
            ..EngineConfig::default()
        },
    );
    let mut requester = rdma_fixture("normal");
    requester.receiver_credits = Some(SenderCreditLedger::new(1, None));
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
    requester
        .connect_receiver_credit(&peer, 0, Duration::from_secs(2))
        .unwrap();
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[TransferRequest::new(
                Transport::Rdma,
                local.base_address(),
                remote.base_address(),
                8,
            )],
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
    assert_eq!(
        requester
            .receiver_credit_stats(&peer)
            .unwrap()
            .completed_requests,
        0
    );
    assert!(matches!(
        requester.disconnect_receiver_credit(&peer, Duration::from_secs(1)),
        Err(Error::BatchBusy)
    ));
    assert_eq!(
        source
            .receiver_credit_issuer_stats()
            .unwrap()
            .reserved_bytes,
        8
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
            rdma.retained_bytes,
            rdma.active_grants,
            rdma.retained_grants
        ),
        (0, 8, 0, 8, 1, 1)
    );
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        source.close_source(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        source.source.rdma.as_ref().unwrap().staging_endpoint(8, 0),
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
    let deadline = Instant::now() + crate::test_support::EVENTUAL_PROGRESS;
    while engine.source.admission.lock().unwrap().open {
        assert!(Instant::now() < deadline, "close did not fence admission");
        std::thread::yield_now();
    }
    assert!(matches!(engine.source.admit(), Err(Error::SourceClosed)));
    assert!(matches!(engine.admit_batch(0), Err(Error::RequesterClosed)));
    // Backend work accepted before the fence may still open its slice. Full
    // close must not close lazy device admission while that work is draining.
    let runtime = engine.source.rdma.as_ref().unwrap();
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
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
        .recv_timeout(crate::test_support::EVENTUAL_PROGRESS)
        .unwrap()
        .unwrap();
    closer.join().unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    engine.close(Duration::ZERO).unwrap();
}

#[cfg(feature = "rdma")]
#[test]
fn requester_shared_cq_failure_cannot_report_drain_after_retaining_native_owners() {
    let source = rdma_fixture_with_config(
        crate::transport::rdma::tests::runtime("ok"),
        EngineConfig {
            receiver_credit: Some(crate::ReceiverCreditConfig {
                bytes_per_sender: 8,
                requests_per_sender: 1,
                ..Default::default()
            }),
            ..EngineConfig::default()
        },
    );
    let mut requester = rdma_fixture("post_close0");
    requester.runtime_queue = Some(LocalTransferAdmissionQueue::new(
        crate::RuntimeQueueConfig {
            max_dispatch_owners: 1,
            max_dispatch_bytes: 8,
            deadline_aware: true,
            ..Default::default()
        },
    ));
    requester.receiver_credits = Some(SenderCreditLedger::new(1, requester.runtime_queue.as_ref()));
    requester.qos = Some(
        QosContractResolver::new(
            &crate::QosConfig {
                defaults: crate::QosPolicyFields {
                    max_inflight_bytes: Some(8),
                    max_inflight_requests: Some(1),
                    allowed_degraded_actions: Some(crate::QosDegradedActions {
                        delay: true,
                        reject: true,
                    }),
                    ..Default::default()
                },
                ..Default::default()
            },
            requester.runtime_queue.as_ref(),
        )
        .unwrap(),
    );
    let qos = requester
        .resolve_qos(&crate::QosRequestContext::default())
        .unwrap();
    let captured = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&captured);
    crate::transport::rdma::tests::on_requester_close(move |endpoint, transfer| {
        assert!(matches!(
            transfer,
            Err(Error::Backend {
                operation: "ibv_post_send",
                ..
            })
        ));
        // The original logical error has already been captured. Force the
        // asynchronous native close/retention before requester reads its receipt.
        assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
        assert!(!endpoint.has_resources());
        observed.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    let remote = source
        .register_memory(vec![0x42; 8].into_boxed_slice(), Permission::RemoteRead)
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
    requester
        .connect_receiver_credit(&peer, 0, Duration::from_secs(2))
        .unwrap();
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[TransferRequest::new(
                Transport::Rdma,
                local.base_address(),
                remote.base_address(),
                8,
            )],
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(captured.load(std::sync::atomic::Ordering::SeqCst));
    assert!(!report.locally_drained);
    assert!(matches!(
        report.failure.unwrap().error,
        Error::Unquiesced { .. }
    ));
    let credit = requester.receiver_credit_stats(&peer).unwrap();
    assert!(credit.automatic);
    assert_eq!(credit.completed_requests, 0);
    assert!(matches!(
        requester.disconnect_receiver_credit(&peer, Duration::from_secs(1)),
        Err(Error::BatchBusy)
    ));
    assert_eq!(
        source
            .receiver_credit_issuer_stats()
            .unwrap()
            .reserved_bytes,
        8
    );
    let stats = requester.stats().unwrap();
    let rdma = stats.rdma.unwrap();
    assert_eq!((rdma.retained_endpoints, rdma.retained_cqs), (1, 2));
    assert_eq!(stats.inflight_batches, 1);
    assert_eq!(
        stats.runtime_queue.unwrap(),
        RuntimeQueueStats {
            deadline_infeasible_drops: 0,
            queued_owners: 0,
            queued_bytes: 0,
            dispatching_owners: 1,
            dispatching_bytes: 8,
        }
    );
    assert_eq!(
        qos.usage().unwrap(),
        crate::QosUsage {
            owners: 1,
            inflight_bytes: 8,
            inflight_requests: 1
        }
    );
    let credits = requester.receiver_credit_stats(&peer).unwrap();
    assert_eq!(
        (
            credits.consumed_bytes,
            credits.consumed_requests,
            credits.active_owners
        ),
        (8, 1, 1)
    );

    assert!(requester.close(Duration::from_millis(10)).is_err());
    source.close(Duration::from_secs(2)).unwrap();
}

#[cfg(feature = "rdma")]
#[test]
fn whole_close_timeout_preserves_shared_device_and_can_continue_waiting() {
    let engine = rdma_fixture("ok");
    let runtime = engine.source.rdma.as_ref().unwrap();
    runtime.staging_endpoint(8, 0).unwrap().close().unwrap();
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
    engine.close(Duration::from_secs(2)).unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
}

#[cfg(feature = "rdma")]
#[test]
fn directional_close_keeps_other_direction_and_shared_device_available() {
    for source_first in [true, false] {
        let engine = rdma_fixture("ok");
        let runtime = engine.source.rdma.as_ref().unwrap();
        runtime.staging_endpoint(8, 0).unwrap().close().unwrap();
        if source_first {
            engine.close_source(Duration::ZERO).unwrap();
            engine.admit_batch(0).unwrap();
        } else {
            engine.close_requester(Duration::ZERO).unwrap();
            engine.source.admit().unwrap();
        }
        runtime.staging_endpoint(8, 0).unwrap().close().unwrap();
        assert_eq!(runtime.stats().unwrap().opened_devices, 1);
        assert_eq!(runtime.stats().unwrap().active_devices, 1);
        engine.close(Duration::from_secs(2)).unwrap();
        assert_eq!(runtime.stats().unwrap().active_devices, 0);
    }
}

#[cfg(feature = "rdma")]
#[test]
fn whole_close_reports_device_failure_without_retrying_from_another_close() {
    let engine = rdma_fixture("close3");
    let runtime = engine.source.rdma.as_ref().unwrap();
    runtime.staging_endpoint(8, 0).unwrap().close().unwrap();
    for _ in 0..2 {
        assert!(matches!(
            engine.close(Duration::from_secs(2)),
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

#[cfg(feature = "rdma")]
#[test]
fn source_ack_follows_output_drop_and_native_quota_reuse() {
    for mode in ["ack_pause", "direct:ack_pause"] {
        let source = rdma_fixture(mode);
        let requester = rdma_fixture("copy");
        let runtime = source.source.rdma.as_ref().unwrap();
        let pause = crate::transport::rdma::tests::pause(runtime);
        let remote = source
            .register_memory(vec![0x42; 8].into_boxed_slice(), Permission::RemoteRead)
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
        let address = remote.base_address();
        let worker = std::thread::spawn(move || {
            let report = requester
                .submit_and_wait(
                    TransferOp::Read,
                    &peer,
                    &[TransferRequest::new(
                        Transport::Unspec,
                        local.base_address(),
                        address,
                        8,
                    )],
                    Duration::from_secs(2),
                )
                .unwrap();
            let mut bytes = [0; 8];
            local.read(0, &mut bytes).unwrap();
            requester.close(Duration::from_secs(2)).unwrap();
            (report, bytes)
        });
        let (lock, changed) = &*pause;
        let (state, timed) = changed
            .wait_timeout_while(
                lock.lock().unwrap(),
                crate::test_support::EVENTUAL_PROGRESS,
                |s| !s.0,
            )
            .unwrap();
        assert!(!timed.timed_out());
        drop(state);
        let stats = runtime.stats().unwrap();
        let mut next = runtime.endpoint(
            &remote,
            0,
            8,
            1,
            Instant::now() + Duration::from_secs(2),
            None,
        );
        if let Ok(endpoint) = &mut next {
            endpoint.close().unwrap();
        }
        let reusable = next.is_ok();
        drop(next);
        lock.lock().unwrap().1 = true;
        changed.notify_all();
        let (report, bytes) = worker.join().unwrap();
        assert_eq!(
            (
                stats.completed_window_binds,
                stats.completed_window_invalidations,
                stats.active_windows,
                stats.retained_windows
            ),
            (1, 1, 0, 0),
            "source must consume INV CQE and close the MW before ACK"
        );
        assert!(report.failure.is_none());
        assert_eq!(bytes, [0x42; 8]);
        assert_eq!(
            (
                stats.active_endpoints,
                stats.staging_bytes,
                stats.direct_bytes
            ),
            (0, 0, 0)
        );
        assert!(
            reusable,
            "source endpoint/byte quota must be reusable before ACK"
        );
        source.close(Duration::from_secs(2)).unwrap();
    }
}

#[cfg(feature = "rdma")]
#[test]
fn direct_and_staged_peers_preserve_read_write_bytes_across_all_policy_pairs() {
    // Actual control sockets with CPU mutation at the verbs boundary. This is
    // protocol/owner evidence; physical RNIC qualification is a separate gate.
    for source_mode in ["ok", "direct:ok"] {
        for requester_mode in ["copy", "direct:copy"] {
            let source = rdma_fixture(source_mode);
            let requester = rdma_fixture(requester_mode);
            let remote = source
                .register_memory(
                    vec![0x44; 32].into_boxed_slice(),
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let local = requester
                .register_memory(vec![0xcc; 32].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let peer = requester
                .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
                .unwrap();
            let request = TransferRequest::new(
                Transport::Rdma,
                local.base_address() + 3,
                remote.base_address() + 9,
                8,
            );
            let report = requester
                .submit_and_wait(
                    TransferOp::Read,
                    &peer,
                    std::slice::from_ref(&request),
                    EVENTUAL_PROGRESS,
                )
                .unwrap();
            assert!(
                report.failure.is_none(),
                "{source_mode}/{requester_mode}: {report:?}"
            );
            let mut observed = [0; 32];
            local.read(0, &mut observed).unwrap();
            let mut expected = [0xcc; 32];
            expected[3..11].fill(0x44);
            assert_eq!(observed, expected);
            local.write(3, &[0x77; 8]).unwrap();
            let report = requester
                .submit_and_wait(TransferOp::Write, &peer, &[request], EVENTUAL_PROGRESS)
                .unwrap();
            assert!(
                report.failure.is_none(),
                "{source_mode}/{requester_mode}: {report:?}"
            );
            requester.close(EVENTUAL_PROGRESS).unwrap();
            source.close(EVENTUAL_PROGRESS).unwrap();
            remote.read(0, &mut observed).unwrap();
            let mut expected = [0x44; 32];
            expected[9..17].fill(0x77);
            assert_eq!(observed, expected);
            for engine in [&source, &requester] {
                let stats = engine.stats().unwrap().rdma.unwrap();
                assert_eq!(
                    (
                        stats.active_devices,
                        stats.active_endpoints,
                        stats.direct_bytes,
                        stats.staging_bytes
                    ),
                    (0, 0, 0, 0)
                );
            }
        }
    }
}

#[cfg(feature = "rdma")]
#[test]
fn failed_direct_write_can_change_remote_bytes_without_done_and_always_reports_unknown() {
    for requester_mode in ["copy_post", "direct:copy_post"] {
        let source = rdma_fixture("direct:ok");
        let requester = rdma_fixture(requester_mode);
        let remote = source
            .register_memory(
                vec![0x44; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let local = requester
            .register_memory(vec![0x77; 8].into_boxed_slice(), Permission::LocalOnly)
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
                TransferOp::Write,
                &peer,
                &[TransferRequest::new(
                    Transport::Rdma,
                    local.base_address(),
                    remote.base_address(),
                    8,
                )],
                Duration::from_secs(2),
            )
            .unwrap();
        assert!(report.locally_drained);
        assert_eq!(report.transferred_bytes, 0);
        let failure = report.failure.unwrap();
        assert!(matches!(
            failure.error,
            Error::Backend {
                operation: "ibv_post_send",
                ..
            }
        ));
        assert_eq!(failure.remote_effect, RemoteEffect::Unknown);
        requester.close(Duration::from_secs(2)).unwrap();
        source.close(Duration::from_secs(2)).unwrap();
        let mut observed = [0; 8];
        remote.read(0, &mut observed).unwrap();
        assert_eq!(observed, [0x77; 8]);
    }
}

#[cfg(feature = "rdma")]
#[test]
fn failed_direct_read_receipt_preserves_local_changes_and_retains_remote_access_lease() {
    let source = rdma_fixture("direct:close1");
    let requester = rdma_fixture("direct:copy");
    let remote = source
        .register_memory(vec![0x44; 8].into_boxed_slice(), Permission::RemoteRead)
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
            &[TransferRequest::new(
                Transport::Rdma,
                local.base_address(),
                remote.base_address(),
                8,
            )],
            Duration::from_secs(2),
        )
        .unwrap();
    assert!(report.locally_drained);
    assert_eq!(report.transferred_bytes, 0);
    assert!(matches!(
        report.failure.unwrap().error,
        Error::Backend {
            operation: "remote backend",
            ..
        }
    ));
    let mut bytes = [0; 8];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(
        bytes, [0x44; 8],
        "direct READ is not transactional on a lost/failed receipt"
    );
    assert!(matches!(
        remote.read(0, &mut bytes),
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        source.close(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    requester.close(Duration::from_secs(2)).unwrap();
    let stats = source.stats().unwrap().rdma.unwrap();
    assert_eq!(
        (
            stats.direct_bytes,
            stats.staging_bytes,
            stats.retained_bytes,
            stats.retained_endpoints,
            stats.retained_grants
        ),
        (8, 0, 8, 1, 1)
    );
}

#[cfg(feature = "rdma")]
#[test]
fn direct_running_cancel_detach_and_close_keep_dma_lease_until_actual_worker_exit() {
    let source = rdma_fixture("direct:ok");
    let requester = Arc::new(rdma_fixture("direct:pause_copy"));
    let runtime = requester.source.rdma.as_ref().unwrap();
    let pause = crate::transport::rdma::tests::pause(runtime);
    let events = crate::transport::rdma::tests::events(runtime);
    let usage = crate::transport::rdma::tests::work_usage(runtime);
    let remote = source
        .register_memory(vec![0x44; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let fixture = crate::test_support::external_memory(&[0xcc; 16]);
    let local = requester
        .register_host_memory(fixture.memory.clone(), 4, 8, Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
        .unwrap();
    let mut batch = requester
        .submit_async(
            TransferOp::Read,
            &peer,
            &[TransferRequest::new(
                Transport::Rdma,
                local.base_address(),
                remote.base_address(),
                8,
            )],
            None,
            EVENTUAL_PROGRESS,
        )
        .unwrap();
    let (lock, ready) = &*pause;
    let state = lock.lock().unwrap();
    let (state, timeout) = ready
        .wait_timeout_while(state, crate::test_support::EVENTUAL_PROGRESS, |state| {
            !state.0
        })
        .unwrap();
    assert!(!timeout.timed_out(), "native post fixture did not start");
    drop(state);
    assert!(matches!(
        local.read(0, &mut [0; 8]),
        Err(Error::Unavailable(_))
    ));
    assert_eq!(usage.lock().unwrap().work_requests, 2);
    fixture.memory.write(0, &[9; 4]).unwrap();
    assert!(batch.cancel(0).unwrap());
    assert!(matches!(
        batch.wait(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    assert!(matches!(
        requester.close(Duration::ZERO),
        Err(Error::Timeout)
    ));
    requester.unregister_memory(&local).unwrap();
    drop(local);
    drop(fixture.memory);
    let stats = requester.stats().unwrap();
    assert_eq!(
        (
            stats.retained_regions,
            stats.retained_bytes,
            stats.rdma.unwrap().direct_bytes
        ),
        (1, 8, 8)
    );
    assert!(fixture.owner.upgrade().is_some());
    let detached = requester.allocate_batch(1);
    assert!(matches!(detached, Err(Error::RequesterClosed)));
    // The running worker retains both the batch and engine after detach/drop.
    drop(batch);
    let weak_engine = Arc::downgrade(&requester);
    drop(requester);
    assert!(weak_engine.upgrade().is_some());
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    lock.lock().unwrap().1 = true;
    ready.notify_all();
    let deadline = Instant::now() + crate::test_support::EVENTUAL_PROGRESS;
    while weak_engine.upgrade().is_some()
        || fixture.owner.upgrade().is_some()
        || !events.lock().unwrap().contains(&"close_device")
    {
        assert!(
            Instant::now() < deadline,
            "detached worker did not release its real owners"
        );
        std::thread::yield_now();
    }
    source.close(EVENTUAL_PROGRESS).unwrap();
    assert!(events.lock().unwrap().contains(&"dealloc_pd"));
    assert!(events.lock().unwrap().contains(&"close_device"));
    let stats = usage.lock().unwrap();
    assert_eq!(
        (
            stats.post_calls,
            stats.completed_reads,
            stats.completed_read_bytes,
            stats.work_requests
        ),
        (1, 2, 4, 0)
    );
    assert_eq!(fixture.drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[cfg(feature = "rdma")]
#[test]
fn expired_rdma_data_deadline_preserves_failure_and_waits_for_native_destroy() {
    for asynchronous in [false, true] {
        let source = rdma_fixture("normal");
        let requester = Arc::new(rdma_fixture("timeout"));
        let remote = source
            .register_memory(vec![0x42; 8].into_boxed_slice(), Permission::RemoteRead)
            .unwrap();
        let local = requester
            .register_memory(vec![0xcc; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
            .unwrap();
        // Start both sides' native workers before the short data deadline.
        // The timed public READ must reach a posted WR with a withheld CQE.
        for engine in [&source, requester.as_ref()] {
            source_runtime(engine)
                .staging_endpoint(8, 0)
                .unwrap()
                .close()
                .unwrap();
        }
        let captured = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&captured);
        // execute() runs in the submitting thread for the synchronous path.
        // The async path is independently checked through its public free gate.
        if !asynchronous {
            crate::transport::rdma::tests::on_requester_close(move |_, transfer| {
                assert!(matches!(transfer, Err(Error::Timeout)));
                observed.store(true, std::sync::atomic::Ordering::Release);
            });
        }
        let request = TransferRequest::new(
            if asynchronous {
                Transport::Unspec
            } else {
                Transport::Rdma
            },
            local.base_address(),
            remote.base_address(),
            8,
        );
        if asynchronous {
            let mut batch = requester.allocate_batch(1).unwrap();
            batch
                .submit(
                    TransferOp::Read,
                    &peer,
                    &[request],
                    None,
                    Duration::from_millis(250),
                )
                .unwrap();
            batch.seal().unwrap();
            let status = batch.wait(EVENTUAL_PROGRESS).unwrap();
            assert!(status.locally_drained && status.state == crate::BatchState::Failed);
            batch.free().unwrap();
        } else {
            let report = requester
                .submit_and_wait(
                    TransferOp::Read,
                    &peer,
                    &[request],
                    Duration::from_millis(250),
                )
                .unwrap();
            assert!(captured.load(std::sync::atomic::Ordering::Acquire));
            assert!(
                report.locally_drained && matches!(report.failure.unwrap().error, Error::Timeout)
            );
        }
        assert_eq!(
            requester.stats().unwrap().inflight_batches,
            0,
            "native destroy completed but requester admission escaped into quarantine"
        );
        requester.unregister_memory(&local).unwrap();
        requester.close(EVENTUAL_PROGRESS).unwrap();
        source.unregister_memory(&remote).unwrap();
        source.close(EVENTUAL_PROGRESS).unwrap();
    }
}

#[cfg(feature = "cuda")]
pub(crate) fn staging_fixture(
    config: EngineConfig,
    proxy: Arc<crate::runtime::proxy_manager::ProxyManager>,
    #[cfg(feature = "rdma")] rdma: Option<crate::transport::rdma::RdmaTransport>,
) -> TransferEngine {
    config.validate().unwrap();
    let epoch = SourceEpoch::generate().unwrap();
    let source = Arc::new(Source {
        #[cfg(target_os = "linux")]
        shm: None,
        metadata: SegmentMetadata {
            max_transfer_bytes: config.max_transfer_bytes as u64,
            #[cfg(feature = "rdma")]
            rdma: rdma.is_some(),
            remote_staging: config.remote_staging,
            ..Default::default()
        },
        credit_issuer: config.receiver_credit.map(ReceiverCreditIssuer::new),
        remote_staging: config.remote_staging,
        staging_pipeline_depth: config.staging_pipeline_depth,
        proxy,
        nvlink: None,
        epoch,
        #[cfg(feature = "rdma")]
        rdma: rdma.map(Arc::new),
        registry: MemoryRegistry::new(
            epoch,
            config.max_registered_regions,
            config.max_registered_bytes,
        ),
        tcp_notifications: config.tcp_notifications,
        notifications: NotificationInbox::new(8, 4096, 1024),
        admission: Mutex::new(SourceAdmission {
            open: true,
            active: 0,
        }),
        drained: Condvar::new(),
    });
    let control = ControlService::start(Arc::clone(&source), &config).unwrap();
    let runtime_queue = config.runtime_queue.map(LocalTransferAdmissionQueue::new);
    let allocations = Arc::new(AllocationBudget::new());
    let progress_worker =
        TransferEngine::start_progress(&config, &allocations, &runtime_queue, &source).unwrap();
    let engine = TransferEngine {
        hp_tcp: None,
        #[cfg(feature = "metrics")]
        metrics: None,
        notification_routes: NotificationRoutes::new(&config),
        progress_worker,
        files: FileSegments::default(),
        #[cfg(feature = "io-uring")]
        io_uring: None,
        #[cfg(feature = "gds")]
        gds: None,
        receiver_credits: None,
        qos: config
            .qos
            .as_ref()
            .map(|qos| QosContractResolver::new(qos, runtime_queue.as_ref()))
            .transpose()
            .unwrap(),
        runtime_queue,
        selector: TransferEngine::compile_selector(
            &config,
            &TransferEngine::qp_layout(&config).unwrap(),
            #[cfg(feature = "rdma")]
            source.rdma.as_deref(),
        )
        .unwrap(),
        config,
        source,
        control,
        batches: Arc::default(),
        allocations,
    };
    #[cfg(feature = "metrics")]
    let engine = {
        let cfg = engine
            .config
            .metrics
            .load(engine.config.metrics_config_file.as_deref())
            .unwrap();
        engine.install_metrics(&cfg).unwrap()
    };
    engine
}

impl TransferEngine {
    pub(crate) fn notification_inbox_for_test(&self) -> &NotificationInbox {
        &self.source.notifications
    }
}

#[cfg(feature = "rdma")]
#[test]
fn cancelling_a_running_rdma_alias_drains_posts_and_suppresses_the_remaining_slices() -> Result<()>
{
    let timeout = crate::test_support::EVENTUAL_PROGRESS;
    let source = rdma_fixture("direct:ok");
    let requester = Arc::new(rdma_fixture("direct:pause_copy"));
    let pause = crate::transport::rdma::tests::pause(requester.source.rdma.as_ref().unwrap());
    let remote =
        source.register_memory(vec![0x44; 8].into_boxed_slice(), Permission::RemoteRead)?;
    let local =
        requester.register_memory(vec![0xcc; 10].into_boxed_slice(), Permission::LocalOnly)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), timeout)?;
    let requests = [0, 4].map(|i| {
        TransferRequest::new(
            Transport::Rdma,
            local.base_address() + 1 + i,
            remote.base_address() + i,
            4,
        )
    });
    let mut batch = requester.submit_async(TransferOp::Read, &peer, &requests, None, timeout)?;
    let (lock, ready) = &*pause;
    let (gate, timed) = ready
        .wait_timeout_while(lock.lock().unwrap(), timeout, |s| !s.0)
        .unwrap();
    assert!(!timed.timed_out());
    drop(gate);
    assert!(batch.cancel(1)? && batch.cancel(0)?);
    assert!(
        batch
            .task_statuses()?
            .iter()
            .all(|task| task.physical_owner == 0
                && task.cancel_requested
                && task.state == crate::TaskState::Running
                && !task.locally_drained)
    );
    assert!(matches!(batch.free(), Err(Error::BatchBusy)));
    assert!(matches!(
        local.read(1, &mut [0; 8]),
        Err(Error::Unavailable(_))
    ));
    lock.lock().unwrap().1 = true;
    ready.notify_all();
    let status = batch.wait(timeout)?;
    assert_eq!(status.state, crate::BatchState::Cancelled);
    assert!(status.locally_drained);
    for task in batch.task_statuses()? {
        assert_eq!(task.state, crate::TaskState::Cancelled);
        let failure = task.failure.unwrap();
        assert!(matches!(failure.error, Error::Cancelled));
        assert_eq!((failure.request_index, failure.affected_requests), (0, 2));
    }
    assert!(!batch.cancel(1)?);
    let mut bytes = [0; 10];
    local.read(0, &mut bytes)?;
    assert_eq!(
        bytes,
        [0xcc, 0x44, 0x44, 0x44, 0x44, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc]
    );
    let rdma = requester.stats()?.rdma.unwrap();
    assert_eq!(
        (
            rdma.post_calls,
            rdma.completed_reads,
            rdma.completed_read_bytes,
            rdma.work_requests
        ),
        (1, 2, 4, 0)
    );
    Ok(())
}

#[test]
fn hp_worker_failure_preserves_an_already_completed_batch() -> Result<()> {
    let config = || EngineConfig {
        tcp: false,
        hp_tcp: crate::HpTcpConfig {
            enable: true,
            worker_count: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let src = TransferEngine::new(config())?;
    let dst = Arc::new(TransferEngine::new(config())?);
    let source = src.register_memory(vec![0xa5, 1, 2, 3, 0xa5].into(), Permission::RemoteRead)?;
    let local = dst.register_memory(vec![0xee; 5].into(), Permission::LocalOnly)?;
    let peer = dst.open_segment(
        src.endpoint(),
        src.source_epoch(),
        crate::test_support::EVENTUAL_PROGRESS,
    )?;
    let req = TransferRequest::new(
        Transport::HpTcp,
        local.base_address() + 1,
        source.base_address() + 1,
        3,
    );
    let mut batch = dst.submit_async(
        TransferOp::Read,
        &peer,
        std::slice::from_ref(&req),
        None,
        crate::test_support::EVENTUAL_PROGRESS,
    )?;
    batch.wait(crate::test_support::EVENTUAL_PROGRESS)?;
    assert_eq!(dst.hp_tcp.as_ref().unwrap().outstanding_on_lanes(), 0);
    dst.hp_tcp.as_ref().unwrap().fail_worker_for_test();
    let task = batch.task_status(0)?;
    assert_eq!(task.state, crate::TaskState::Completed);
    assert_eq!(task.transferred_bytes, 3);
    let mut got = [0; 5];
    local.read(0, &mut got)?;
    assert_eq!(got, [0xee, 1, 2, 3, 0xee]);
    source.read(0, &mut got)?;
    assert_eq!(got, [0xa5, 1, 2, 3, 0xa5]);
    batch.free()?;
    assert!(matches!(
        dst.submit_and_wait(
            TransferOp::Read,
            &peer,
            &[req],
            crate::test_support::EVENTUAL_PROGRESS
        ),
        Err(Error::Internal(_))
    ));
    assert!(matches!(
        dst.close(crate::test_support::EVENTUAL_PROGRESS),
        Err(Error::Internal(_))
    ));
    dst.close(crate::test_support::EVENTUAL_PROGRESS)?;
    Ok(())
}

impl TransferEngine {
    pub(crate) fn retired_hp_sessions_for_test(&self) -> usize {
        self.hp_tcp
            .as_ref()
            .unwrap()
            .retired_sessions
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

#[test]
fn rejected_hp_submissions_leave_no_lane_load() -> Result<()> {
    let config = EngineConfig {
        tcp: false,
        hp_tcp: crate::HpTcpConfig {
            enable: true,
            worker_count: 1,
            connections_per_peer: 2,
            max_outstanding_bytes: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    let src = TransferEngine::new(config.clone())?;
    let dst = TransferEngine::new(config)?;
    let remote = src.register_memory(vec![0x5a; 16].into(), Permission::RemoteReadWrite)?;
    let local = dst.register_memory(vec![0xee; 16].into(), Permission::LocalOnly)?;
    let wait = crate::test_support::EVENTUAL_PROGRESS;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), wait)?;
    let req = |i| {
        TransferRequest::new(
            Transport::HpTcp,
            local.base_address() + i,
            remote.base_address() + i,
            4,
        )
    };
    let load = || dst.hp_tcp.as_ref().unwrap().outstanding_on_lanes();
    for other in [9, 100] {
        assert!(
            dst.submit_and_wait(TransferOp::Read, &peer, &[req(1), req(other)], wait)
                .is_err()
        );
        assert_eq!(load(), 0);
    }
    assert!(
        dst.submit_and_wait(TransferOp::Read, &peer, &[req(1)], wait)?
            .is_complete()
    );
    assert_eq!(load(), 0);
    let mut got = [0; 16];
    local.read(0, &mut got)?;
    let mut want = [0xee; 16];
    want[1..5].fill(0x5a);
    assert_eq!(got, want);
    Ok(())
}
