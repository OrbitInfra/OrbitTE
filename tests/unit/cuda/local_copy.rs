use super::*;
use crate::cuda::local_copy::{CopyDirection::*, LocalCopyBackend::*, *};
use crate::{RegisteredMemory, test_support::EVENTUAL_PROGRESS};

struct Stream {
    ctx: Arc<Context>,
    raw: usize,
}
impl Stream {
    fn new(ctx: &Arc<Context>) -> Arc<Self> {
        let raw = ctx
            .current(|| {
                let mut raw = std::ptr::null_mut();
                // SAFETY: this context owns the new nonblocking stream until Stream::drop.
                check("create test caller stream", unsafe {
                    (ctx.driver.stream_create)(&mut raw, 1)
                })?;
                Ok(raw as usize)
            })
            .unwrap();
        Arc::new(Self {
            ctx: ctx.clone(),
            raw,
        })
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.ctx
            .current(|| {
                // SAFETY: the copy worker and all producer dependencies have released this stream.
                check("destroy test stream", unsafe {
                    (self.ctx.driver.stream_destroy)(self.raw as _)
                })
            })
            .unwrap();
    }
}
struct Fixture {
    _engine: TransferEngine,
    device: Arc<Device>,
    gpu: RegisteredMemory,
    pin: Arc<HostMemory>,
    host: RegisteredMemory,
    stream: Arc<Stream>,
}
const SIZE: usize = 4096;
const GUARD: u8 = 0x5a;
impl Fixture {
    fn new(driver: Driver) -> Self {
        let device = allocation(&[GUARD; SIZE]);
        // SAFETY: fixture owns this stationary device allocation.
        let ptr = unsafe { &*device.bytes.get() }.as_ptr() as u64;
        let g =
            Arc::new(CudaMemory::register(ptr, SIZE, device.clone(), Arc::new(driver)).unwrap());
        let ctx = g.allocation.lock().unwrap().context.clone();
        let stream = Stream::new(&ctx);
        let pin = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
            PinnedAllocation::new(&ctx, SIZE).unwrap(),
        )));
        pin.write(0, &[GUARD; SIZE]).unwrap();
        let engine = TransferEngine::new(EngineConfig::default()).unwrap();
        let gpu = engine
            .register_cuda_memory_batch(&[(g, 0, SIZE)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let host = engine
            .register_host_memory_batch(&[(pin.clone(), 0, SIZE)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        Self {
            _engine: engine,
            device,
            gpu,
            pin,
            host,
            stream,
        }
    }
    fn bind(&self, config: LocalCopyConfig) -> Result<LocalCopyEngine> {
        // SAFETY: the supplied owner controls this exact live stream and fixture accesses use registrations.
        unsafe {
            LocalCopyEngine::bind(&self.gpu, self.stream.raw as _, self.stream.clone(), config)
        }
    }
    fn desc(&self, d: usize, h: usize, n: usize) -> LocalCopyDesc {
        LocalCopyDesc::new(&self.gpu, d, &self.host, h, n)
    }
    fn calls(&self, token: &str) -> usize {
        self.device
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|&&v| v == token)
            .count()
    }
    fn guarded(
        &self,
        dir: CopyDirection,
        backend: LocalCopyBackend,
        rows: &[(usize, usize, usize)],
        op: impl FnOnce() -> Result<LocalCopyCompletion>,
    ) {
        let (src, dst) = if dir == HostToDevice {
            (&self.host, &self.gpu)
        } else {
            (&self.gpu, &self.host)
        };
        let source: Vec<_> = (0..SIZE).map(|i| (i % 239) as u8).collect();
        src.write(0, &source).unwrap();
        dst.write(0, &[GUARD; SIZE]).unwrap();
        self.device.events.lock().unwrap().clear();
        let receipt = op().unwrap();
        assert_eq!(receipt.backend(), backend);
        assert!(receipt.poll().unwrap());
        assert_eq!(
            self.calls("sync") + self.calls("stream-sync") + self.calls("event-sync"),
            0
        );
        let mut want = vec![GUARD; SIZE];
        for &(d, h, n) in rows {
            let (s, t) = if dir == HostToDevice { (h, d) } else { (d, h) };
            want[t..t + n].copy_from_slice(&source[s..s + n]);
        }
        let mut actual = vec![0; SIZE];
        dst.read(0, &mut actual).unwrap();
        assert_eq!(actual, want);
        src.read(0, &mut actual).unwrap();
        assert_eq!(actual, source);
    }
}
fn config(backend: LocalCopyBackend) -> LocalCopyConfig {
    LocalCopyConfig {
        backend,
        kernel_threshold: 3,
        max_descriptors: 320,
        max_inflight: 2,
        max_bytes: 1 << 20,
    }
}
#[test]
fn strided_dma_preserves_unrequested_bytes_both_directions() {
    for optional in [true, false] {
        let mut driver = local_driver();
        if !optional {
            driver.copy_batch = None;
            driver.local.copy_2d = None;
        }
        let f = Fixture::new(driver);
        let worker = f.bind(config(Dma)).unwrap();
        for dir in [HostToDevice, DeviceToHost] {
            let rows = [
                (2, 5, 5),
                (7, 10, 4),
                (30, 40, 3),
                (43, 57, 3),
                (56, 74, 3),
                (21, 22, 6),
                (18, 98, 2),
            ];
            let batch = [
                f.desc(2, 5, 5),
                f.desc(7, 10, 4),
                f.desc(30, 40, 3).strided(3, 13, 17),
                f.desc(21, 22, 6),
                f.desc(18, 98, 2),
            ];
            f.guarded(dir, Dma, &rows, || worker.submit(dir, &batch, None));
            assert_eq!(f.calls("copy-batch"), usize::from(optional));
            assert_eq!(f.calls("copy-2d"), usize::from(optional));
        }
    }
}
#[test]
fn kernel_matches_direct_both_directions_and_auto_counts_compiled_operations() {
    let f = Fixture::new(local_driver());
    let worker = f.bind(config(Auto)).unwrap();
    let rows: Vec<_> = (0..257)
        .map(|i| (8 + i * 14, 16 + (256 - i) * 13, 1 + i % 5))
        .collect();
    let descs: Vec<_> = rows.iter().map(|&(d, h, n)| f.desc(d, h, n)).collect();
    for dir in [HostToDevice, DeviceToHost] {
        f.guarded(dir, Kernel, &rows, || worker.submit(dir, &descs, None));
        assert_eq!(f.calls("kernel-launch"), 1);
        f.guarded(dir, Dma, &[(4, 8, 4), (12, 20, 4), (20, 32, 4)], || {
            worker.submit(dir, &[f.desc(4, 8, 4).strided(3, 8, 12)], None)
        });
        assert_eq!(f.calls("copy-2d"), 1);
    }
}
#[test]
fn pending_events_own_distinct_kernel_slots_payloads_and_abandoned_work() {
    let f = Fixture::new(local_driver());
    let worker = f.bind(config(Kernel)).unwrap();
    f.host.write(8, b"orbit").unwrap();
    f.host.write(32, b"bytes").unwrap();
    f.device.query_hold.store(true, Ordering::Release);
    let a = worker
        .submit(HostToDevice, &[f.desc(50, 8, 5)], None)
        .unwrap();
    let b = worker
        .submit(HostToDevice, &[f.desc(80, 32, 5)], None)
        .unwrap();
    assert!(matches!(a.wait(Duration::ZERO), Err(Error::Timeout)));
    drop(a);
    assert_eq!(
        worker.progress().unwrap(),
        LocalCopyStats {
            active: 2,
            retained: 0,
            payload_bytes: 10,
            descriptor_bytes: 320 * 2 * 48
        }
    );
    assert!(matches!(
        worker.submit(HostToDevice, &[f.desc(100, 100, 2)], None),
        Err(Error::ResourceExhausted)
    ));
    assert!(f.host.write(9, b"x").is_err());
    assert!(f.gpu.read(81, &mut [0]).is_err());
    f.host.write(20, b"gap").unwrap();
    assert!(matches!(worker.close(Duration::ZERO), Err(Error::Timeout)));
    f.device.query_hold.store(false, Ordering::Release);
    b.wait(EVENTUAL_PROGRESS).unwrap();
    worker.close(EVENTUAL_PROGRESS).unwrap();
    assert_eq!(worker.progress().unwrap(), LocalCopyStats::default());
    let mut actual = vec![0; SIZE];
    f.gpu.read(0, &mut actual).unwrap();
    let mut expected = vec![GUARD; SIZE];
    expected[50..55].copy_from_slice(b"orbit");
    expected[80..85].copy_from_slice(b"bytes");
    assert_eq!(actual, expected);
}
#[test]
fn missing_symbols_overlap_and_invalid_final_row_are_checked_before_payload() {
    let mut driver = local_driver();
    driver.local.kernel = None;
    let f = Fixture::new(driver);
    assert!(matches!(f.bind(config(Kernel)), Err(Error::Unavailable(_))));
    let w = f.bind(config(Auto)).unwrap();
    assert_eq!(w.progress().unwrap().descriptor_bytes, 0);
    for dir in [HostToDevice, DeviceToHost] {
        assert!(matches!(
            w.submit(dir, &[f.desc(1, 2, 3), f.desc(0, SIZE - 1, 2)], None),
            Err(Error::InvalidRange)
        ));
        assert!(matches!(
            w.submit(dir, &[f.desc(0, 0, 1).strided(321, 1, 1)], None),
            Err(Error::ResourceExhausted)
        ));
        let bad = f
            ._engine
            .register_host_memory_batch(
                &[(Arc::new(HostMemory::new(Box::new([0; 8])).unwrap()), 0, 8)],
                Permission::LocalOnly,
            )
            .unwrap()
            .remove(0);
        assert!(matches!(
            w.submit(dir, &[LocalCopyDesc::new(&f.gpu, 0, &bad, 0, 2)], None),
            Err(Error::Unavailable(_))
        ));
    }
    assert!(f.device.submitted_copies.lock().unwrap().is_empty());
    let rows = [(3, 12, 3), (3, 19, 4), (21, 14, 2)];
    let descs: Vec<_> = rows.iter().map(|&(d, h, n)| f.desc(d, h, n)).collect();
    f.guarded(HostToDevice, Dma, &rows, || {
        w.submit(HostToDevice, &descs, None)
    });
    assert_eq!(f.calls("copy-batch"), 0);
    let f = Fixture::new(local_driver());
    let w = f.bind(config(Kernel)).unwrap();
    assert!(matches!(
        w.submit(HostToDevice, &[f.desc(1, 0, 2), f.desc(1, 2, 1)], None),
        Err(Error::InvalidConfig(_))
    ));
}
#[test]
fn failed_submission_waits_for_already_enqueued_stream_work() {
    let f = Fixture::new(local_driver());
    let w = f.bind(config(Dma)).unwrap();
    f.host.write(0, b"abcd").unwrap();
    f.device.copy_error.store(700, Ordering::Release);
    f.device.query_hold.store(true, Ordering::Release);
    let h = w.submit(HostToDevice, &[f.desc(3, 0, 4)], None).unwrap();
    assert!(!h.poll().unwrap());
    assert!(f.host.write(0, b"x").is_err());
    f.device.query_hold.store(false, Ordering::Release);
    f.device.copy_error.store(0, Ordering::Release);
    assert!(matches!(h.poll(), Err(Error::Backend { code: 700, .. })));
    assert_eq!(w.progress().unwrap().active, 0);
    let mut result = [0; 6];
    f.gpu.read(2, &mut result).unwrap();
    assert_eq!(result, [GUARD, b'a', GUARD, GUARD, GUARD, GUARD]);
}
#[test]
fn query_destroy_record_or_restore_failure_quarantines_once_and_keeps_owners() {
    for fault in ["query", "destroy", "record", "restore"] {
        let f = Fixture::new(local_driver());
        let w = f.bind(config(Kernel)).unwrap();
        let (weak_pin, weak_stream) = (Arc::downgrade(&f.pin), Arc::downgrade(&f.stream));
        match fault {
            "query" => f.device.event_query_fail_at.store(1, Ordering::Release),
            "destroy" => f.device.event_destroy_error.store(700, Ordering::Release),
            "record" => f.device.record_error.store(700, Ordering::Release),
            _ => f.device.pop_after_completion.store(true, Ordering::Release),
        }
        let result = w.submit(HostToDevice, &[f.desc(5, 2, 9)], None);
        if fault == "record" {
            assert!(matches!(result, Err(Error::Unquiesced { .. })));
        } else {
            assert!(matches!(
                result.unwrap().poll(),
                Err(Error::Unquiesced { .. })
            ));
        }
        assert_eq!(
            w.progress().unwrap(),
            LocalCopyStats {
                active: 1,
                retained: 1,
                payload_bytes: 9,
                descriptor_bytes: 320 * 2 * 48
            }
        );
        assert!(f.host.write(2, b"x").is_err());
        assert!(f.gpu.read(5, &mut [0]).is_err());
        let device = f.device.clone();
        let count = || {
            device
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|&&e| matches!(e, "event-destroy" | "event-query" | "module-unload"))
                .count()
        };
        let before = count();
        assert!(matches!(
            w.close(Duration::ZERO),
            Err(Error::Unquiesced { .. })
        ));
        drop(w);
        drop(f);
        assert!(weak_pin.upgrade().is_some() && weak_stream.upgrade().is_some());
        assert_eq!(count(), before);
        assert!(!device.scratch.lock().unwrap().is_empty());
        STACK.with(|s| s.borrow_mut().clear());
    }
}

#[test]
fn caller_stream_waits_for_pinned_and_external_producers_and_restores_exact_context() {
    for with_caller in [false, true] {
        let f = Fixture::new(local_driver());
        let other = f.stream.ctx.clone();
        let producer = Stream::new(&other);
        let external = Stream::new(&other);
        let foreign = allocation(&[9; 16]);
        if with_caller {
            STACK.with(|s| s.borrow_mut().push(Arc::as_ptr(&foreign) as usize));
        }
        let expected = STACK.with(|s| s.borrow().clone());
        // Raw shim bytes can be inspected only before enqueue or after a receipt.
        f.gpu.write(100, b"ready").unwrap();
        let raw = other
            .current(|| {
                let api = &other.driver;
                // SAFETY: separate live streams and disjoint registered ranges; owners are held through the marker.
                unsafe {
                    check(
                        "producer write",
                        (api.to_host)(
                            (f.host.base_address() + 12) as _,
                            f.gpu.base_address() + 100,
                            5,
                            producer.raw as _,
                        ),
                    )?;
                    f.pin
                        .record_producer_stream(producer.raw as _, producer.clone())?;
                    check(
                        "external producer",
                        (api.to_host)(
                            (f.host.base_address() + 20) as _,
                            f.gpu.base_address() + 100,
                            5,
                            external.raw as _,
                        ),
                    )?;
                    let mut raw = std::ptr::null_mut();
                    check("external event", (api.event_create)(&mut raw, 3))?;
                    check(
                        "external marker",
                        (api.event_record)(raw, external.raw as _),
                    )?;
                    Ok(raw)
                }
            })
            .unwrap();
        let weak = Arc::downgrade(&producer);
        drop(producer);
        // SAFETY: this event will not be re-recorded or destroyed during the flight.
        let ready = unsafe { ProducerEvent::from_raw(raw.cast(), external.clone()) }.unwrap();
        let w = f.bind(config(Dma)).unwrap();
        f.device.events.lock().unwrap().clear();
        f.device.query_hold.store(true, Ordering::Release);
        let h = w
            .submit(
                HostToDevice,
                &[f.desc(2, 12, 5), f.desc(20, 20, 5)],
                Some(&ready),
            )
            .unwrap();
        drop(ready);
        assert_eq!(STACK.with(|s| s.borrow().clone()), expected);
        assert!(!h.poll().unwrap());
        assert!(weak.upgrade().is_some());
        assert_eq!(f.calls("stream-wait-event"), 2);
        assert_eq!(f.calls("event-sync") + f.calls("stream-sync"), 0);
        f.device.query_hold.store(false, Ordering::Release);
        assert!(h.poll().unwrap());
        assert!(weak.upgrade().is_none());
        assert_eq!(f.calls("event-sync") + f.calls("stream-sync"), 0);
        assert_eq!(STACK.with(|s| s.borrow().clone()), expected);
        let mut want = vec![GUARD; SIZE];
        for pos in [2, 20, 100] {
            want[pos..pos + 5].copy_from_slice(b"ready");
        }
        let mut actual = vec![0; SIZE];
        f.gpu.read(0, &mut actual).unwrap();
        assert_eq!(actual, want);
        other
            .current(|| {
                // SAFETY: both streams have retired their references to the external generation.
                check("retire external event", unsafe {
                    (other.driver.event_destroy)(raw)
                })
            })
            .unwrap();
        if with_caller {
            STACK.with(|s| assert_eq!(s.borrow_mut().pop(), Some(Arc::as_ptr(&foreign) as usize)));
        }
    }
}

#[test]
fn shared_physical_pin_coalesces_across_retained_slots_and_checks_each_boundary() {
    let f = Fixture::new(local_driver());
    let base = NonNull::new(f.pin.base_address() as *mut u8).unwrap();
    // SAFETY: external slots are disjoint and only accessed through their registrations.
    let pin = unsafe { PinnedHostRegistration::from_raw_parts(base, f.pin.len(), f.pin.clone()) }
        .unwrap();
    let mut hosts = Vec::new();
    for i in 0..2 {
        // SAFETY: two adjacent 16-byte slots in one pin, with individually retained owners.
        let host = unsafe {
            HostMemory::from_cuda_registered_parts(
                NonNull::new(base.as_ptr().add(i * 16)).unwrap(),
                16,
                Arc::new(i),
                pin.clone(),
            )
        }
        .unwrap();
        hosts.push((Arc::new(host), 0, 16));
    }
    // Keep the two logical slots in a separate registry from the fixture's whole-pool view.
    let registry = TransferEngine::new(EngineConfig::default()).unwrap();
    let slots = registry
        .register_host_memory_batch(&hosts, Permission::LocalOnly)
        .unwrap();
    slots[0].write(12, b"abcd").unwrap();
    slots[1].write(0, b"efgh").unwrap();
    let w = f.bind(config(Dma)).unwrap();
    let a = LocalCopyDesc::new(&f.gpu, 3, &slots[0], 12, 4);
    let b = LocalCopyDesc::new(&f.gpu, 7, &slots[1], 0, 4);
    assert!(matches!(
        w.submit(
            HostToDevice,
            &[LocalCopyDesc::new(&f.gpu, 3, &slots[0], 12, 8)],
            None
        ),
        Err(Error::InvalidRange)
    ));
    f.device.events.lock().unwrap().clear();
    let h = w.submit(HostToDevice, &[a, b], None).unwrap();
    assert!(slots[1].write(0, b"x").is_err());
    assert!(h.poll().unwrap());
    assert_eq!(f.calls("HtoD"), 1);
    let mut actual = [0; 10];
    f.gpu.read(2, &mut actual).unwrap();
    assert_eq!(&actual, b"ZabcdefghZ");
}

#[test]
fn auto_falls_back_before_upload_and_explicit_kernel_reports_missing_mapping_or_image() {
    unsafe extern "C" fn no_map(_: *mut u64, _: *mut std::ffi::c_void, _: u32) -> driver::Status {
        driver::Status::CUDA_ERROR_NOT_SUPPORTED
    }
    unsafe extern "C" fn no_image(
        _: *mut sys::CUmodule,
        _: *const std::ffi::c_void,
    ) -> driver::Status {
        driver::Status::CUDA_ERROR_NO_BINARY_FOR_GPU
    }
    for image in [false, true] {
        let mut d = local_driver();
        let k = d.local.kernel.as_mut().unwrap();
        if image {
            k.load = no_image;
        } else {
            k.mapped = no_map;
        }
        let f = Fixture::new(d);
        let a = [f.desc(15, 0, 2), f.desc(8, 8, 3), f.desc(2, 18, 1)];
        let k = f.bind(config(Kernel));
        if image {
            assert!(matches!(k, Err(Error::Unavailable(_))));
        } else {
            assert!(matches!(
                k.unwrap().submit(HostToDevice, &a, None),
                Err(Error::Unavailable(_))
            ));
        }
        let w = f.bind(config(Auto)).unwrap();
        f.guarded(
            HostToDevice,
            Dma,
            &[(15, 0, 2), (8, 8, 3), (2, 18, 1)],
            || w.submit(HostToDevice, &a, None),
        );
        assert_eq!(f.calls("kernel-launch"), 0);
    }
}

#[test]
fn capture_foreign_context_and_budget_rejections_do_not_enqueue() {
    let f = Fixture::new(local_driver());
    let other = Fixture::new(local_driver());
    let w = f
        .bind(LocalCopyConfig {
            max_bytes: 8,
            ..config(Dma)
        })
        .unwrap();
    let invalid = LocalCopyDesc::new(&other.gpu, 0, &f.host, 0, 4);
    assert!(matches!(
        w.submit(HostToDevice, &[invalid], None),
        Err(Error::Unavailable(_))
    ));
    f.device.capture_active.store(true, Ordering::Release);
    assert!(matches!(
        w.submit(HostToDevice, &[f.desc(0, 0, 8)], None),
        Err(Error::Unavailable(_))
    ));
    f.device.capture_active.store(false, Ordering::Release);
    assert!(matches!(
        w.submit(HostToDevice, &[f.desc(0, 0, 9)], None),
        Err(Error::ResourceExhausted)
    ));
    f.device.query_hold.store(true, Ordering::Release);
    let a = w.submit(HostToDevice, &[f.desc(0, 0, 5)], None).unwrap();
    assert!(matches!(
        w.submit(HostToDevice, &[f.desc(16, 16, 4)], None),
        Err(Error::ResourceExhausted)
    ));
    f.device.query_hold.store(false, Ordering::Release);
    a.wait(EVENTUAL_PROGRESS).unwrap();
    assert_eq!(f.device.submitted_copies.lock().unwrap().len(), 1);
}

#[test]
fn legacy_batch_abi_uses_failure_index_only_as_a_diagnostic() {
    unsafe extern "C" fn older(
        dst: *mut u64,
        src: *mut u64,
        sizes: *mut usize,
        n: usize,
        attrs: *mut sys::CUmemcpyAttributes,
        indices: *mut usize,
        count: usize,
        failed: *mut usize,
        stream: sys::CUstream,
    ) -> driver::Status {
        // SAFETY: this CUDA 12.8 shim forwards the equal-length arrays and writes the extra output.
        unsafe {
            *failed = 0;
            copy_batch(dst, src, sizes, n, attrs, indices, count, stream)
        }
    }
    let mut driver = local_driver();
    driver.copy_batch = None;
    driver.local.batch_with_fail_index = Some(older);
    let f = Fixture::new(driver);
    let w = f.bind(config(Dma)).unwrap();
    let rows = [(3, 8, 2), (12, 20, 3)];
    let ds = [f.desc(3, 8, 2), f.desc(12, 20, 3)];
    f.guarded(DeviceToHost, Dma, &rows, || {
        w.submit(DeviceToHost, &ds, None)
    });
    f.host.write(8, b"ab").unwrap();
    f.host.write(20, b"xyz").unwrap();
    f.gpu.write(0, &[GUARD; SIZE]).unwrap();
    f.device.copy_error.store(700, Ordering::Release);
    f.device.query_hold.store(true, Ordering::Release);
    let h = w.submit(HostToDevice, &ds, None).unwrap();
    assert!(!h.poll().unwrap());
    assert!(f.host.write(20, b"!").is_err());
    f.device.query_hold.store(false, Ordering::Release);
    f.device.copy_error.store(0, Ordering::Release);
    assert!(matches!(
        h.poll(),
        Err(Error::Backend {
            operation: "cuMemcpyBatchAsync",
            code: 700
        })
    ));
    let mut got = vec![0; SIZE];
    f.gpu.read(0, &mut got).unwrap();
    let mut want = vec![GUARD; SIZE];
    want[12..15].copy_from_slice(b"xyz");
    assert_eq!(got, want);
}
