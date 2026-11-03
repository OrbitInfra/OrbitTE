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

fn exercise(path: &Path) -> Result<()> {
    let engine = TransferEngine::new(EngineConfig {
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
    let device = engine
        .register_cuda_memory_batch(&[(gpu, 0, size)], Permission::LocalOnly)?
        .remove(0);
    let host = engine
        .register_host_memory_batch(
            &[(allocate_pinned_host(0, size)?, 0, size)],
            Permission::LocalOnly,
        )?
        .remove(0);
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
        "STORAGE_CUDA_OK staged_bytes={} chunks={} pinned_bytes={} direct=true retained=0",
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
