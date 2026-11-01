use super::ipc::{deadline, owned};
use super::*;
use crate::cuda::ipc::{IpcExport, IpcMapping};
use crate::memory::Allocation;

struct Producer {
    input: Arc<CudaMemory>,
    context: Arc<Context>,
    stream: usize,
}

impl Producer {
    fn new(device: &Arc<Device>, per_thread: bool) -> Arc<Self> {
        let input = memory(device);
        let context = input.allocation.lock().unwrap().context.clone();
        let stream = if per_thread {
            2
        } else {
            context
                .current(|| {
                    let mut stream = std::ptr::null_mut();
                    check("producer stream", unsafe {
                        (context.driver.stream_create)(&mut stream, 1)
                    })?;
                    Ok(stream as usize)
                })
                .unwrap()
        };
        Arc::new(Self {
            input,
            context,
            stream,
        })
    }

    fn enqueue(&self, host: &HostMemory, offset: usize, length: usize) {
        self.context
            .current(|| {
                check("producer DtoH", unsafe {
                    (self.context.driver.to_host)(
                        (host.base_address() + offset as u64) as usize as *mut _,
                        self.input.base_address(),
                        length,
                        self.stream as sys::CUstream,
                    )
                })
            })
            .unwrap();
    }

    fn record(self: &Arc<Self>, host: &HostMemory) -> Result<()> {
        self.context
            .current(|| unsafe { host.record_producer_stream(self.stream as *mut _, self.clone()) })
    }

    fn pinned(&self) -> Arc<HostMemory> {
        let host = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
            PinnedAllocation::new(&self.context, 64).unwrap(),
        )));
        host.write(0, &[0xa5; 64]).unwrap();
        host
    }
}

#[test]
fn pinned_producer_generations_share_one_event_across_registration_aliases() {
    for per_thread in [false, true] {
        let device = allocation(&[0x57; 64]);
        let producer = Producer::new(&device, per_thread);
        let weak = Arc::downgrade(&producer);
        let input = Arc::downgrade(&producer.input);
        let host = producer.pinned();
        let engine = TransferEngine::new(EngineConfig::default()).unwrap();
        let views = engine
            .register_host_memory_batch(
                &[(host.clone(), 0, 64), (host.clone(), 8, 16)],
                Permission::LocalOnly,
            )
            .unwrap();
        assert!(device.completions.lock().unwrap().is_empty());
        producer.enqueue(&host, 8, 16);
        producer.record(&host).unwrap();
        producer.enqueue(&host, 40, 8);
        let view = views[1].clone();
        let bytes = std::thread::spawn(move || {
            let mut bytes = [0; 16];
            view.read(0, &mut bytes).unwrap();
            bytes
        })
        .join()
        .unwrap();
        assert_eq!(bytes, [0x57; 16]);
        let mut actual = [0; 64];
        views[0].read(0, &mut actual).unwrap();
        let mut expected = [0xa5; 64];
        expected[8..24].fill(0x57);
        assert_eq!(
            actual, expected,
            "work after the first marker stays pending"
        );
        assert_eq!(device.event_waits.load(Ordering::Acquire), 1);
        producer.record(&host).unwrap();
        drop(producer);
        assert!(weak.upgrade().is_some() && input.upgrade().is_some());
        host.read(0, &mut actual).unwrap();
        expected[40..48].fill(0x57);
        assert_eq!(actual, expected);
        assert!(weak.upgrade().is_none() && input.upgrade().is_none());
        assert_eq!(device.event_waits.load(Ordering::Acquire), 2);
        assert_eq!(device.completions.lock().unwrap().len(), 1);
        for view in &views {
            engine.unregister_memory(view).unwrap();
        }
        drop(views);
        drop(host);
        engine.close(Duration::from_secs(2)).unwrap();
        assert!(device.pins.lock().unwrap().is_empty());
        assert!(device.completions.lock().unwrap().is_empty());
        assert!(
            !device
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|&e| matches!(e, "sync" | "stream-sync"))
        );
    }
}

#[test]
fn pinned_producer_readiness_precedes_cpu_staging_ipc_dma_and_tcp_payloads() {
    let mut routes = vec![
        "cpu-write",
        "staging-bytes",
        "staging-access",
        "ipc-read",
        "ipc-write",
        "tcp-source-read",
        "tcp-source-write",
        "tcp-requester-read",
        "tcp-requester-write",
    ];
    if cfg!(feature = "rdma") {
        routes.push("dma");
    }
    for route in routes {
        let device = allocation(&[0x57; 64]);
        let producer = Producer::new(&device, false);
        let host = producer.pinned();
        let remote = allocation(&[0xcc; 64]);
        let mut ipc = if route.starts_with("ipc") {
            let export = IpcExport::new(owned(&remote, true)).unwrap();
            let mut mapping = IpcMapping::new(&Allocation::Host(host.clone()), deadline()).unwrap();
            mapping.open(export.handle).unwrap();
            Some((export, mapping, CUDAStreamPool::new(1).unwrap()))
        } else {
            None
        };
        producer.enqueue(&host, 8, 16);
        producer.record(&host).unwrap();
        match route {
            "cpu-write" => host.write(12, &[0xcc; 4]).unwrap(),
            "staging-bytes" => host
                .with_staging_bytes(24, |bytes| {
                    assert_eq!(&bytes[8..24], &[0x57; 16]);
                    Ok(())
                })
                .unwrap(),
            "staging-access" => host
                .with_staging_access(24, || {
                    let pins = device.pins.lock().unwrap();
                    assert_eq!(&pins[&(host.base_address() as usize)][8..24], &[0x57; 16]);
                    Ok(())
                })
                .unwrap(),
            "ipc-read" | "ipc-write" => {
                let (export, mut mapping, pool) = ipc.take().unwrap();
                mapping
                    .copy_one(
                        &Allocation::Host(host.clone()),
                        &pool,
                        8,
                        12,
                        4,
                        route == "ipc-write",
                    )
                    .unwrap();
                if route == "ipc-write" {
                    assert_eq!(&unsafe { &*remote.bytes.get() }[8..12], &[0x57; 4]);
                }
                mapping.close().unwrap();
                export.finish();
            }
            #[cfg(feature = "rdma")]
            "dma" => {
                let engine = TransferEngine::new(EngineConfig::default()).unwrap();
                let view = engine
                    .register_host_memory(host.clone(), 0, 64, Permission::LocalOnly)
                    .unwrap();
                let loan = view.dma_lease(8, 16).unwrap();
                assert_eq!(
                    &device.pins.lock().unwrap()[&(host.base_address() as usize)][8..24],
                    &[0x57; 16]
                );
                loan.complete().unwrap();
                drop(loan);
                engine.close(Duration::from_secs(2)).unwrap();
            }
            _ => {
                let source = TransferEngine::new(EngineConfig::default()).unwrap();
                let requester = TransferEngine::new(EngineConfig::default()).unwrap();
                let other = Arc::new(HostMemory::new(vec![0xcc; 64].into_boxed_slice()).unwrap());
                let is_source = route.contains("source");
                let source_view = source
                    .register_host_memory(
                        if is_source {
                            host.clone()
                        } else {
                            other.clone()
                        },
                        0,
                        64,
                        Permission::RemoteReadWrite,
                    )
                    .unwrap();
                let local_view = requester
                    .register_host_memory(
                        if is_source {
                            other.clone()
                        } else {
                            host.clone()
                        },
                        0,
                        64,
                        Permission::LocalOnly,
                    )
                    .unwrap();
                let peer = requester
                    .open_segment(
                        source.endpoint(),
                        source.source_epoch(),
                        Duration::from_secs(2),
                    )
                    .unwrap();
                let op = if route.ends_with("read") {
                    TransferOp::Read
                } else {
                    TransferOp::Write
                };
                let report = requester
                    .submit_and_wait(
                        op,
                        &peer,
                        &[TransferRequest {
                            priority: Default::default(),
                            transport: Transport::Tcp,
                            remote_staging: false,
                            local_address: local_view.base_address() + 8,
                            remote_address: source_view.base_address() + 8,
                            length: 16,
                        }],
                        Duration::from_secs(2),
                    )
                    .unwrap();
                assert!(report.is_complete() && report.locally_drained);
                let mut bytes = [0; 64];
                other.read(0, &mut bytes).unwrap();
                let mut expected = [0xcc; 64];
                if (is_source && op == TransferOp::Read) || (!is_source && op == TransferOp::Write)
                {
                    expected[8..24].fill(0x57);
                }
                assert_eq!(bytes, expected, "{route}");
                source.close(Duration::from_secs(2)).unwrap();
                requester.close(Duration::from_secs(2)).unwrap();
            }
        }
        assert_eq!(device.event_waits.load(Ordering::Acquire), 1, "{route}");
        let mut actual = [0; 64];
        host.read(0, &mut actual).unwrap();
        let mut expected = [0xa5; 64];
        expected[8..24].fill(0x57);
        if matches!(route, "cpu-write" | "ipc-read") {
            expected[12..16].fill(0xcc);
        }
        if matches!(route, "tcp-source-write" | "tcp-requester-read") {
            expected[8..24].fill(0xcc);
        }
        assert_eq!(actual, expected, "{route}");
    }
}

#[test]
fn pinned_producer_admission_preserves_existing_host_dma_and_ipc_ownership() {
    let device = allocation(&[0x57; 64]);
    let producer = Producer::new(&device, false);
    let host = producer.pinned();
    let ordinary = Arc::new(HostMemory::new(vec![0; 64].into_boxed_slice()).unwrap());
    let external = unsafe {
        HostMemory::from_raw_parts(
            NonNull::new(ordinary.base_address() as usize as *mut u8).unwrap(),
            64,
            ordinary.clone(),
        )
        .unwrap()
    };
    for memory in [&*ordinary, &external] {
        assert!(matches!(
            producer.record(memory),
            Err(Error::Unavailable(_))
        ));
        memory.write(0, &[0x55]).unwrap();
    }
    {
        let mut access = host.lock_cuda_pinned(deadline()).unwrap();
        access.begin_ipc();
    }
    assert!(matches!(producer.record(&host), Err(Error::Unavailable(_))));
    host.release_ipc();
    #[cfg(feature = "rdma")]
    {
        let engine = TransferEngine::new(EngineConfig::default()).unwrap();
        let view = engine
            .register_host_memory(host.clone(), 0, 64, Permission::LocalOnly)
            .unwrap();
        let loan = view.dma_lease(60, 4).unwrap();
        assert!(matches!(producer.record(&host), Err(Error::Unavailable(_))));
        drop(loan);
        engine.close(Duration::from_secs(2)).unwrap();
    }
    assert!(device.completions.lock().unwrap().is_empty());
    producer.enqueue(&host, 8, 16);
    producer.record(&host).unwrap();
    let mut bytes = [0; 16];
    host.read(8, &mut bytes).unwrap();
    assert_eq!(bytes, [0x57; 16]);
}

#[test]
fn pinned_producer_failure_retains_native_pin_and_inputs_until_proven_completion() {
    for fault in [
        "create",
        "record",
        "record-drain",
        "wait",
        "restore",
        "foreign-context",
        "drop",
        "destroy",
        "unpin",
    ] {
        let device = allocation(&[0x57; 64]);
        let producer = Producer::new(&device, false);
        let weak = Arc::downgrade(&producer);
        let input = Arc::downgrade(&producer.input);
        let host = producer.pinned();
        producer.enqueue(&host, 8, 16);
        match fault {
            "create" => device.event_create_error.store(1, Ordering::Release),
            "record" | "record-drain" => device.record_error.store(1, Ordering::Release),
            "restore" => device.pop_error.store(201, Ordering::Release),
            _ => (),
        }
        if fault == "record-drain" {
            device.sync_error.store(700, Ordering::Release);
        }
        let recorded = if fault == "foreign-context" {
            let foreign = allocation(&[0; 64]);
            let foreign_producer = Producer::new(&foreign, false);
            unsafe {
                host.record_producer_stream(foreign_producer.stream as *mut _, producer.clone())
            }
        } else {
            producer.record(&host)
        };
        let uncertain = matches!(
            fault,
            "create" | "record-drain" | "wait" | "restore" | "foreign-context"
        );
        match fault {
            "create" | "record-drain" | "restore" | "foreign-context" => assert!(
                matches!(recorded, Err(Error::Unquiesced { .. })),
                "{fault}: {recorded:?}"
            ),
            "record" => assert!(matches!(recorded, Err(Error::Backend { .. }))),
            _ => recorded.unwrap(),
        }
        if fault == "wait" {
            device.event_wait_fail_at.store(1, Ordering::Release);
        }
        if fault != "drop" {
            let result = host.read(8, &mut [0; 16]);
            assert_eq!(result.is_err(), uncertain, "{fault}");
        }
        if fault == "destroy" {
            device.event_destroy_error.store(1, Ordering::Release);
        }
        if fault == "unpin" {
            device.free_error.store(1, Ordering::Release);
        }
        drop(producer);
        assert_eq!(
            weak.upgrade().is_some(),
            uncertain || fault == "drop",
            "{fault}"
        );
        drop(host);
        assert_eq!(weak.upgrade().is_some(), uncertain, "{fault}");
        assert_eq!(input.upgrade().is_some(), uncertain, "{fault}");
        assert_eq!(
            !device.pins.lock().unwrap().is_empty(),
            uncertain || fault == "unpin",
            "{fault}"
        );
        let events = device.events.lock().unwrap();
        assert_eq!(
            events.iter().filter(|&&e| e == "unpin").count(),
            usize::from(!uncertain),
            "{fault}"
        );
        if fault == "drop" {
            assert!(
                events.iter().position(|&e| e == "event-sync").unwrap()
                    < events.iter().position(|&e| e == "unpin").unwrap()
            );
        }
        STACK.with(|s| s.borrow_mut().clear());
    }
}
