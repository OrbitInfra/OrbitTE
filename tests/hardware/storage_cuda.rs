//! Installed correctness profile for a selected GPU and a fresh local NVMe file.
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use orbit_te::cuda::{CudaMemory, allocate_pinned_host, retained_resources};
use orbit_te::{
    EngineConfig, Error, FileSegment, Permission, RegisteredMemory, Result, TransferEngine,
    TransferOp, TransferRequest, Transport,
};

const CHUNK: usize = 4096;
const TIMEOUT: Duration = Duration::from_secs(30);

fn round_trip(
    engine: &TransferEngine,
    file: &FileSegment,
    memory: &RegisteredMemory,
    path: &Path,
    expected_file: &mut [u8],
    file_offset: usize,
    length: usize,
) -> Result<()> {
    let bytes: Vec<_> = (0..memory.len())
        .map(|i| ((i * 37 + file_offset) % 251) as u8)
        .collect();
    memory.write(0, &bytes)?;
    let request = TransferRequest::new(
        Transport::IoUring,
        memory.base_address() + 3,
        file_offset as u64,
        length,
    );
    for op in [TransferOp::Write, TransferOp::Read] {
        let report = engine.submit_and_wait(op, file, std::slice::from_ref(&request), TIMEOUT)?;
        if !report.is_complete()
            || !report.locally_drained
            || report.completed_requests != 1
            || report.transferred_bytes != length
        {
            return Err(Error::Internal("incomplete installed CUDA/file transfer"));
        }
        if op == TransferOp::Write {
            expected_file[file_offset..file_offset + length].copy_from_slice(&bytes[3..3 + length]);
            if fs::read(path)? != expected_file {
                return Err(Error::Internal(
                    "installed WRITE bytes or file guards differ",
                ));
            }
            memory.write(0, &vec![0xa3; memory.len()])?;
        }
    }
    let mut actual = vec![0; memory.len()];
    memory.read(0, &mut actual)?;
    let mut expected = vec![0xa3; memory.len()];
    expected[3..3 + length].copy_from_slice(&bytes[3..3 + length]);
    if actual != expected {
        return Err(Error::Internal(
            "installed READ bytes or memory guards differ",
        ));
    }
    Ok(())
}

// The consumer owns a distinct context on the same GPU. Observe the public
// platform copy in that context and on a fresh thread with no current context.
#[allow(unsafe_code)]
fn caller_context_control(memory: &CudaMemory) -> Result<()> {
    use std::ffi::c_void;
    type Ctx = *mut c_void;
    let missing = |_| Error::Unavailable("installed CUDA context entry");
    // SAFETY: the real CUDA library owns the checked symbols for this scope.
    let lib = unsafe { libloading::Library::new("libcuda.so.1") }.map_err(missing)?;
    // SAFETY: these symbol names and signatures are the CUDA Driver C ABI.
    let (get, create, destroy, device_get) = unsafe {
        (
            *lib.get::<unsafe extern "C" fn(*mut Ctx) -> i32>(b"cuCtxGetCurrent")
                .map_err(missing)?,
            *lib.get::<unsafe extern "C" fn(*mut Ctx, u32, i32) -> i32>(b"cuCtxCreate_v2")
                .map_err(missing)?,
            *lib.get::<unsafe extern "C" fn(Ctx) -> i32>(b"cuCtxDestroy_v2")
                .map_err(missing)?,
            *lib.get::<unsafe extern "C" fn(*mut i32, i32) -> i32>(b"cuDeviceGet")
                .map_err(missing)?,
        )
    };
    let current = || -> Result<Ctx> {
        let mut ctx = std::ptr::null_mut();
        // SAFETY: ctx is writable return storage on this thread.
        if unsafe { get(&mut ctx) } != 0 {
            return Err(Error::Internal("read installed caller context"));
        }
        Ok(ctx)
    };
    let exercise = |expected: Ctx| -> Result<()> {
        if current()? != expected {
            return Err(Error::Internal("initial caller binding differs"));
        }
        memory.write(0, &[1, 3, 5, 7])?;
        if current()? != expected {
            return Err(Error::Internal("WRITE changed caller context"));
        }
        let mut bytes = [0; 4];
        memory.read(0, &mut bytes)?;
        if current()? != expected || bytes != [1, 3, 5, 7] {
            return Err(Error::Internal("READ changed caller context or bytes"));
        }
        Ok(())
    };
    exercise(std::ptr::null_mut())?;
    let mut device = 0;
    // SAFETY: the driver writes the device handle of the visible GPU to this slot.
    if unsafe { device_get(&mut device, 0) } != 0 {
        return Err(Error::Unavailable("installed CUDA device"));
    }
    let mut custom = std::ptr::null_mut();
    // SAFETY: the installed script exposes just the selected physical GPU as ordinal zero.
    if unsafe { create(&mut custom, 0, device) } != 0 || custom.is_null() {
        return Err(Error::Unavailable("create installed caller context"));
    }
    exercise(custom)?;
    // SAFETY: successful guarded copies returned the caller's context; it has no outstanding work.
    if unsafe { destroy(custom) } != 0 || !current()?.is_null() {
        return Err(Error::Internal("release installed caller context"));
    }
    Ok(())
}

fn exercise(path: &Path) -> Result<()> {
    let engine = TransferEngine::new(EngineConfig {
        io_uring: orbit_te::IoUringConfig {
            pinned_staging: true,
            staging_slot_kb: 4,
            staging_slots: 1,
        },
        staging_chunk_size: CHUNK,
        staging_chunk_count: 2,
        ..Default::default()
    })?;
    let mut image = vec![0x58; 6 * CHUNK];
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?
        .write_all(&image)?;
    let file = engine.open_file_segment(
        &format!("file://{}", path.display()),
        Permission::RemoteReadWrite,
    )?;
    let size = 3 * CHUNK;
    let gpu = Arc::new(CudaMemory::allocate(0, size)?);
    std::thread::scope(|scope| scope.spawn(|| caller_context_control(&gpu)).join())
        .map_err(|_| Error::Internal("installed caller context thread"))??;
    let device = engine
        .register_cuda_memory_batch(&[(gpu, 0, size)], Permission::LocalOnly)?
        .remove(0);
    let host = engine
        .register_host_memory_batch(
            &[(allocate_pinned_host(0, size)?, 0, size)],
            Permission::LocalOnly,
        )?
        .remove(0);
    round_trip(&engine, &file, &device, path, &mut image, 4 * CHUNK, CHUNK)?;
    let n = 2 * CHUNK + 37;
    round_trip(&engine, &file, &device, path, &mut image, 0, n)?;
    let staged = engine.stats()?.staging;
    if staged.copied_bytes != (2 * n) as u64
        || staged.completed_chunks != 6
        || staged.active_chunks != 0
        || staged.retained_chunks != 0
    {
        return Err(Error::Internal("installed CUDA/file staging accounting"));
    }
    round_trip(&engine, &file, &host, path, &mut image, 3 * CHUNK + 1, n)?;
    if engine.file_segment_info(&file)?.direct_io != Some(true)
        || engine.stats()?.staging.copied_bytes != staged.copied_bytes
    {
        return Err(Error::Internal(
            "installed ext4 direct fd or pinned host path missing",
        ));
    }
    let slots = engine
        .stats()?
        .io_uring
        .ok_or(Error::Unavailable("installed io_uring"))?;
    if !slots.pinned
        || (
            slots.slot_acquisitions,
            slots.slot_misses,
            slots.active_slots,
            slots.pageable_bytes,
        ) != (2, 2, 0, 0)
    {
        return Err(Error::Internal(
            "installed file slot acquisition or fallback accounting",
        ));
    }
    let spans = [
        (device.base_address() + 11, 3).into(),
        orbit_te::FileIoElement::Padding(3),
        (host.base_address() + 25, 2).into(),
        (device.base_address() + 7, 2).into(),
        orbit_te::FileIoElement::Padding(2),
    ];
    let vector = TransferRequest::file_vectored(Transport::IoUring, 23, &spans)?;
    device.write(6, b"!ab!!XYZ!")?;
    host.write(24, b"!HD!")?;
    for op in [TransferOp::Write, TransferOp::Read] {
        let report = engine.submit_and_wait(op, &file, std::slice::from_ref(&vector), TIMEOUT)?;
        if !report.is_complete() || report.transferred_bytes != 12 || !report.locally_drained {
            return Err(Error::Internal("installed vectored CUDA/file receipt"));
        }
        if op == TransferOp::Write {
            image[23..35].copy_from_slice(b"XYZ\0\0\0HDab\0\0");
            if fs::read(path)? != image {
                return Err(Error::Internal("installed vectored file bytes or guards"));
            }
            device.write(6, b"#########")?;
            host.write(24, b"####")?;
        }
    }
    let mut gpu_bytes = [0; 9];
    let mut host_bytes = [0; 4];
    device.read(6, &mut gpu_bytes)?;
    host.read(24, &mut host_bytes)?;
    if &gpu_bytes != b"#ab##XYZ#" || &host_bytes != b"#HD#" {
        return Err(Error::Internal(
            "installed vectored scatter bytes or guards",
        ));
    }
    engine.close_file_segment(&file, TIMEOUT)?;
    engine.unregister_memory(&device)?;
    engine.unregister_memory(&host)?;
    drop((device, host));
    engine.close(TIMEOUT)?;
    let retained = retained_resources();
    if retained.device_allocations
        + retained.pinned_allocations
        + retained.events
        + retained.streams
        + retained.contexts
        != 0
    {
        return Err(Error::Internal("CUDA file gate retained native owners"));
    }
    println!(
        "STORAGE_CUDA_OK staged_bytes={} chunks={} pinned_bytes={} direct=true retained=0 pooled_transfers=2 pool_misses=2 contexts=absent,custom",
        staged.copied_bytes,
        staged.completed_chunks,
        2 * n
    );
    Ok(())
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [flag] if flag == "--protocol" => println!("orbitte-installed-storage-cuda-v1"),
        [flag, path] if flag == "--exercise" => exercise(Path::new(path))?,
        _ => return Err("usage: orbitte-storage-cuda --protocol | --exercise NEW_FILE".into()),
    }
    Ok(())
}
