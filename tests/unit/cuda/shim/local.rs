use super::*;

// Make the GPU alias deliberately different from the CPU's virtual address.
const MAPPING: usize = 1 << 62;
pub(super) enum Command {
    Wait(usize, usize),
    Kernel { triples: usize, count: usize },
}
pub(super) fn drain_prefix(
    streams: &mut BTreeMap<usize, Vec<PendingCopy>>,
    stream: usize,
    through: usize,
) {
    let pending = streams.entry(stream).or_default();
    let n = pending.partition_point(|p| p.sequence <= through);
    for p in pending.drain(..n).collect::<Vec<_>>() {
        match p.local {
            Some(Command::Wait(source, marker)) => drain_prefix(streams, source, marker),
            Some(Command::Kernel { triples, count }) => {
                // SAFETY: the kernel slot owns the uploaded triples and the flight owns every payload.
                let descs =
                    unsafe { std::slice::from_raw_parts(triples as *const [u64; 3], count) };
                for &[dst, src, len] in descs {
                    assert_ne!(dst as usize & MAPPING, src as usize & MAPPING);
                    // SAFETY: the mapping bit is the shim's translated pinned alias; all ranges remain loaned.
                    unsafe {
                        std::ptr::copy(
                            (src as usize & !MAPPING) as *const u8,
                            (dst as usize & !MAPPING) as *mut u8,
                            len as usize,
                        )
                    }
                }
            }
            None => {
                // SAFETY: the queued copy retains both backing ranges through stream progress.
                unsafe { std::ptr::copy(p.source as *const u8, p.destination as *mut u8, p.length) }
            }
        }
    }
}
fn command(stream: sys::CUstream, local: Command) {
    current()
        .streams
        .lock()
        .unwrap()
        .entry(resolve_stream(stream))
        .or_default()
        .push(PendingCopy {
            sequence: current().next_command.fetch_add(1, Ordering::AcqRel),
            local: Some(local),
            source: 0,
            destination: 0,
            length: 0,
        });
}
unsafe extern "C" fn wait(stream: sys::CUstream, event_id: sys::CUevent, flags: u32) -> Status {
    assert_eq!(flags, 0);
    event("stream-wait-event");
    let Some((s, n)) = current()
        .completions
        .lock()
        .unwrap()
        .get(&(event_id as usize))
        .copied()
        .flatten()
    else {
        return status(1);
    };
    command(stream, Command::Wait(s, n));
    SUCCESS
}
unsafe extern "C" fn attr(out: *mut i32, key: sys::CUdevice_attribute, _: i32) -> Status {
    // SAFETY: caller supplies writable attribute storage.
    unsafe {
        *out = match key {
            sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT => 16,
            sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_MAX_PITCH => 1 << 20,
            _ => return status(1),
        }
    }
    SUCCESS
}
unsafe extern "C" fn two(desc: *const sys::CUDA_MEMCPY2D, stream: sys::CUstream) -> Status {
    event("copy-2d");
    // SAFETY: caller provides a complete descriptor through the native call.
    let d = unsafe { &*desc };
    let host = sys::CUmemorytype::CU_MEMORYTYPE_HOST;
    assert_ne!(d.srcMemoryType, d.dstMemoryType);
    let src = if d.srcMemoryType == host {
        d.srcHost as usize
    } else {
        d.srcDevice as usize
    };
    let dst = if d.dstMemoryType == host {
        d.dstHost as usize
    } else {
        d.dstDevice as usize
    };
    for r in 0..d.Height {
        enqueue(
            stream,
            src + r * d.srcPitch,
            dst + r * d.dstPitch,
            d.WidthInBytes,
        );
    }
    SUCCESS
}
unsafe extern "C" fn alloc(out: *mut u64, bytes: usize) -> Status {
    let mut storage = vec![0u64; bytes.div_ceil(8)].into_boxed_slice();
    let address = storage.as_mut_ptr() as u64;
    current().scratch.lock().unwrap().insert(address, storage);
    // SAFETY: output belongs to the caller; scratch is held in this exact context.
    unsafe {
        *out = address;
    }
    SUCCESS
}
unsafe extern "C" fn free(address: u64) -> Status {
    assert!(current().scratch.lock().unwrap().remove(&address).is_some());
    SUCCESS
}
unsafe extern "C" fn mapped(out: *mut u64, host: *mut c_void, flags: u32) -> Status {
    assert_eq!(flags, 0);
    if !current()
        .pins
        .lock()
        .unwrap()
        .contains_key(&(host as usize))
    {
        return status(1);
    }
    // SAFETY: caller owns the output and retains the pin.
    unsafe { *out = (host as usize | MAPPING) as u64 }
    SUCCESS
}
unsafe extern "C" fn load(out: *mut sys::CUmodule, image: *const c_void) -> Status {
    event("module-load");
    // SAFETY: caller supplies a NUL-terminated PTX image and writable handle.
    unsafe {
        assert!(
            std::ffi::CStr::from_ptr(image.cast())
                .to_str()
                .unwrap()
                .contains(".entry orbitte_batch_copy(")
        );
        *out = 1 as _;
    }
    SUCCESS
}
unsafe extern "C" fn function(
    out: *mut sys::CUfunction,
    module: sys::CUmodule,
    name: *const std::ffi::c_char,
) -> Status {
    assert_eq!(module as usize, 1);
    // SAFETY: name is NUL-terminated and the caller owns the output handle slot.
    unsafe {
        assert_eq!(std::ffi::CStr::from_ptr(name), c"orbitte_batch_copy");
        *out = 2 as _;
    }
    SUCCESS
}
unsafe extern "C" fn unload(module: sys::CUmodule) -> Status {
    assert_eq!(module as usize, 1);
    event("module-unload");
    SUCCESS
}
unsafe extern "C" fn launch(
    f: sys::CUfunction,
    x: u32,
    y: u32,
    z: u32,
    bx: u32,
    by: u32,
    bz: u32,
    shared: u32,
    stream: sys::CUstream,
    args: *mut *mut c_void,
    extra: *mut *mut c_void,
) -> Status {
    assert_eq!(
        (f as usize, y, z, bx, by, bz, shared),
        (2, 1, 1, 256, 1, 1, 0)
    );
    assert!(extra.is_null());
    // SAFETY: the kernel entry declares three by-value args captured during launch.
    let (ptr, n, ctas) = unsafe {
        (
            *(*args).cast::<u64>(),
            *(*args.add(1)).cast::<i32>(),
            *(*args.add(2)).cast::<i32>(),
        )
    };
    assert!((1..=16).contains(&ctas));
    assert!(n > 0);
    assert_eq!(x, (n as u32 * ctas as u32).min(65535));
    event("kernel-launch");
    command(
        stream,
        Command::Kernel {
            triples: ptr as usize,
            count: n as usize,
        },
    );
    SUCCESS
}
pub(in crate::cuda::tests) fn local_driver() -> Driver {
    let mut d = Arc::try_unwrap(driver()).ok().unwrap();
    d.allocate = alloc;
    d.free = free;
    d.copy_batch = Some(copy_batch);
    d.local = driver::LocalApi {
        batch_with_fail_index: None,
        attribute: Some(attr),
        copy_2d: Some(two),
        wait_event: Some(wait),
        kernel: Some(driver::KernelApi {
            load,
            function,
            unload,
            launch,
            mapped,
        }),
    };
    d
}
