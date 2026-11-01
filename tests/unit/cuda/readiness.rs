use super::ipc::{deadline, owned};
use super::*;
use crate::cuda::ipc::{IpcExport, IpcMapping};
use crate::memory::Allocation;
use crate::runtime::proxy_manager::ProxyManager;

struct Producer {
    context: Arc<Context>,
    stream: usize,
    bytes: Box<[u8]>,
}

impl Producer {
    fn new(memory: &CudaMemory, value: u8, per_thread: bool) -> Arc<Self> {
        let context = memory.allocation.lock().unwrap().context.clone();
        let stream = if per_thread {
            2
        } else {
            context
                .current(|| {
                    let mut stream = std::ptr::null_mut();
                    check("test producer stream", unsafe {
                        (context.driver.stream_create)(&mut stream, 1)
                    })?;
                    Ok(stream as usize)
                })
                .unwrap()
        };
        Arc::new(Self {
            context,
            stream,
            bytes: vec![value; 32].into_boxed_slice(),
        })
    }

    fn enqueue(&self, address: u64, length: usize) {
        self.context
            .current(|| {
                check("test producer enqueue", unsafe {
                    (self.context.driver.to_device)(
                        address,
                        self.bytes.as_ptr().cast(),
                        length,
                        self.stream as sys::CUstream,
                    )
                })
            })
            .unwrap();
    }

    fn record(self: &Arc<Self>, memory: &CudaMemory) -> Result<()> {
        self.context.current(|| unsafe {
            memory.record_producer_stream(self.stream as *mut _, self.clone())
        })
    }
}

#[test]
fn cuda_producer_readiness_captures_calling_thread_generation_and_reuses_event() {
    for per_thread in [false, true] {
        let device = allocation(&[0xa5; 64]);
        let gpu = memory(&device);
        let producer = Producer::new(&gpu, 0x57, per_thread);
        let weak = Arc::downgrade(&producer);
        producer.enqueue(gpu.base_address() + 8, 16);
        producer.record(&gpu).unwrap();
        let mut later = Box::new([0xccu8; 16]);
        producer.enqueue(later.as_mut_ptr() as u64, 16);
        let clone = gpu.clone();
        let bytes = std::thread::spawn(move || {
            let mut bytes = [0; 64];
            clone.read(0, &mut bytes).unwrap();
            bytes
        })
        .join()
        .unwrap();
        assert_eq!(&bytes[8..24], &[0x57; 16]);
        assert!(
            bytes[..8]
                .iter()
                .chain(bytes[24..].iter())
                .all(|&b| b == 0xa5)
        );
        assert_eq!(
            *later, [0xcc; 16],
            "work after the recorded marker must not be drained"
        );
        producer.record(&gpu).unwrap();
        let event_count = device.completions.lock().unwrap().len();
        assert_eq!(event_count, 2, "one producer event and one copy event");
        drop(producer);
        assert!(weak.upgrade().is_some());
        gpu.read(8, &mut [0; 16]).unwrap();
        assert_eq!(*later, [0x57; 16]);
        assert!(weak.upgrade().is_none());
        assert!(
            !device
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|&e| matches!(e, "sync" | "stream-sync"))
        );
        drop(gpu);
        assert!(device.completions.lock().unwrap().is_empty());
        assert!(STACK.with(|s| s.borrow().is_empty()));
    }
}

#[test]
fn cuda_producer_readiness_is_consumed_by_copy_staging_ipc_and_dma_owners() {
    let mut routes = vec![
        "copy-read",
        "copy-write",
        "staging-copy",
        "staging-loan",
        "ipc-export",
        "ipc-requester",
    ];
    if cfg!(feature = "rdma") {
        routes.push("dma");
    }
    for route in routes {
        let device = allocation(&[0xa5; 64]);
        let gpu = owned(&device, true);
        let producer = Producer::new(&gpu, 0x57, false);
        producer.enqueue(gpu.base_address() + 8, 16);
        producer.record(&gpu).unwrap();
        let before = device.event_waits.load(Ordering::Acquire);
        match route {
            "copy-read" => {
                let mut bytes = [0; 16];
                gpu.read(8, &mut bytes).unwrap();
                assert_eq!(bytes, [0x57; 16]);
            }
            "copy-write" => gpu.write(12, &[0xcc; 4]).unwrap(),
            "staging-copy" => {
                let manager = ProxyManager::new(8, 1, crate::StagingLocation::Any).unwrap();
                let mut bytes = [0; 16];
                gpu.copy_staged(8, &mut bytes, None, &manager, deadline())
                    .unwrap();
                assert_eq!(bytes, [0x57; 16]);
            }
            "staging-loan" => {
                let manager = ProxyManager::new(16, 1, crate::StagingLocation::Any).unwrap();
                gpu.with_staging(8, 16, &manager, 1, |copy, mut slots| {
                    copy.copy(&mut slots[0], 0, 16, false)?;
                    assert_eq!(&slots[0].buffer()?.bytes()[..16], &[0x57; 16]);
                    Ok(())
                })
                .unwrap();
            }
            "ipc-export" => {
                let export = IpcExport::new(gpu.clone()).unwrap();
                export.finish();
            }
            "ipc-requester" => {
                let remote = allocation(&[0x77; 64]);
                let export = IpcExport::new(owned(&remote, true)).unwrap();
                let owner = Allocation::Cuda(gpu.clone());
                let pool = CUDAStreamPool::new(1).unwrap();
                let mut mapping = IpcMapping::new(&owner, deadline()).unwrap();
                mapping.open(export.handle).unwrap();
                mapping.copy_one(&owner, &pool, 0, 12, 4, false).unwrap();
                mapping.close().unwrap();
            }
            #[cfg(feature = "rdma")]
            "dma" => {
                gpu.acquire_dma(8..24).unwrap();
                gpu.complete_dma().unwrap();
                gpu.release_dma(&(8..24));
            }
            _ => unreachable!(),
        }
        let bytes = unsafe { &*device.bytes.get() };
        let mut expected = [0xa5; 64];
        expected[8..24].fill(0x57);
        if route == "copy-write" {
            expected[12..16].fill(0xcc);
        }
        if route == "ipc-requester" {
            expected[12..16].fill(0x77);
        }
        assert_eq!(&**bytes, &expected, "{route}");
        assert!(
            device.event_waits.load(Ordering::Acquire) > before,
            "{route}"
        );
        gpu.allocation.lock().unwrap().wait_producer().unwrap();
    }
}

#[test]
fn cuda_producer_readiness_orders_public_tcp_source_and_destination() {
    for op in [TransferOp::Read, TransferOp::Write] {
        let source = TransferEngine::new(EngineConfig::default()).unwrap();
        let requester = TransferEngine::new(EngineConfig::default()).unwrap();
        let device = allocation(&[0xa5; 64]);
        let gpu = memory(&device);
        let remote = source
            .register_cuda_memory_batch(&[(gpu.clone(), 0, 64)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let host = requester
            .register_memory(vec![0xcc; 16].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(2),
            )
            .unwrap();
        let producer = Producer::new(&gpu, 0x57, true);
        producer.enqueue(gpu.base_address() + 8, 16);
        producer.record(&gpu).unwrap();
        let report = requester
            .submit_and_wait(
                op,
                &peer,
                &[TransferRequest {
                    transport: Transport::Tcp,
                    remote_staging: false,
                    local_address: host.base_address(),
                    remote_address: remote.base_address() + 8,
                    length: 16,
                }],
                Duration::from_secs(2),
            )
            .unwrap();
        assert!(report.is_complete() && report.locally_drained);
        let mut actual = [0; 16];
        host.read(0, &mut actual).unwrap();
        assert_eq!(
            actual,
            [if op == TransferOp::Read { 0x57 } else { 0xcc }; 16]
        );
        let mut expected = [0xa5; 64];
        expected[8..24].fill(if op == TransferOp::Read { 0x57 } else { 0xcc });
        assert_eq!(&**unsafe { &*device.bytes.get() }, &expected);
        source.close(Duration::from_secs(2)).unwrap();
        requester.close(Duration::from_secs(2)).unwrap();
    }
}

#[test]
fn cuda_producer_failures_recover_only_original_stream_or_retain_real_owners() {
    for fault in [
        "create",
        "record",
        "record-drain",
        "wait",
        "restore",
        "capture",
        "context",
        "destroy",
        "drop",
    ] {
        let device = allocation(&[0xa5; 64]);
        let weak_device = Arc::downgrade(&device);
        let gpu = memory(&device);
        let producer = Producer::new(&gpu, 0x57, false);
        let weak_producer = Arc::downgrade(&producer);
        producer.enqueue(gpu.base_address(), 16);
        match fault {
            "create" => device.event_create_error.store(1, Ordering::Release),
            "record" | "record-drain" => device.record_error.store(1, Ordering::Release),
            "capture" => device.capture_active.store(true, Ordering::Release),
            "restore" => device.pop_error.store(201, Ordering::Release),
            _ => (),
        }
        if fault == "record-drain" {
            device.sync_error.store(700, Ordering::Release);
        }
        let recorded = if fault == "context" {
            unsafe { gpu.record_producer_stream(producer.stream as *mut _, producer.clone()) }
        } else {
            producer.record(&gpu)
        };
        let uncertain = matches!(
            fault,
            "create" | "record-drain" | "wait" | "restore" | "capture" | "context"
        );
        if matches!(fault, "wait" | "destroy" | "drop") {
            recorded.unwrap();
        } else if fault == "record" {
            assert!(matches!(recorded, Err(Error::Backend { .. })));
        } else {
            assert!(
                matches!(recorded, Err(Error::Unquiesced { .. })),
                "{fault}: {recorded:?}"
            );
        }
        if fault == "wait" {
            device.event_wait_fail_at.store(1, Ordering::Release);
        }
        if fault == "record" {
            device.record_error.store(0, Ordering::Release);
        }
        if matches!(fault, "wait" | "destroy" | "record") {
            let result = gpu.read(0, &mut [0; 16]);
            assert_eq!(result.is_err(), fault == "wait", "{fault}");
        }
        if fault == "destroy" {
            device.event_destroy_error.store(1, Ordering::Release);
        }
        drop(producer);
        assert_eq!(
            weak_producer.upgrade().is_some(),
            uncertain || fault == "drop",
            "{fault}"
        );
        drop(gpu);
        if fault == "drop" {
            assert_eq!(&unsafe { &*device.bytes.get() }[..16], &[0x57; 16]);
        }
        assert_eq!(weak_producer.upgrade().is_some(), uncertain, "{fault}");
        if uncertain {
            assert!(!device.events.lock().unwrap().contains(&"free"));
        }
        STACK.with(|s| s.borrow_mut().clear());
        drop(device);
        assert_eq!(
            weak_device.upgrade().is_some(),
            uncertain || fault == "destroy",
            "{fault}"
        );
    }
}
