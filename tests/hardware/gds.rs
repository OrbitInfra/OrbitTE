//! Installed native-route consumer. The driver owns the hardware locks and nvfs counters.
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use orbit_te::cuda::{CudaMemory, retained_resources};
use orbit_te::{
    EngineConfig, Error, GdsMode, Permission, Result, TransferEngine, TransferOp, TransferRequest,
    Transport,
};

const LENGTH: usize = 2 << 20;
const GUARD: usize = 4096;
const WAIT: Duration = Duration::from_secs(30);

fn exercise(path: &Path) -> Result<()> {
    let engine = TransferEngine::new(EngineConfig {
        gds_require_native: true,
        ..Default::default()
    })?;
    let gds = engine.stats()?.gds;
    if !gds.available || gds.mode != Some(GdsMode::NativeOnly) {
        return Err(Error::Unavailable(
            "installed GDS requires compatibility disabled",
        ));
    }
    let total = LENGTH + 2 * GUARD;
    let mut file_bytes = vec![0x37; total];
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(&file_bytes)?;
    file.sync_all()?;
    drop(file);
    let segment = engine.open_file_segment(
        &format!("file://{}", path.display()),
        Permission::RemoteReadWrite,
    )?;
    let gpu = Arc::new(CudaMemory::allocate(0, total)?);
    let view = engine
        .register_cuda_memory_batch(&[(gpu, 0, total)], Permission::LocalOnly)?
        .remove(0);
    let source: Vec<_> = (0..total).map(|i| ((43 * i + 19) % 251) as u8).collect();
    view.write(0, &source)?;
    let request = [TransferRequest::new(
        Transport::Gds,
        view.base_address() + GUARD as u64,
        GUARD as u64,
        LENGTH,
    )];
    for op in [TransferOp::Write, TransferOp::Read] {
        let report = engine.submit_and_wait(op, &segment, &request, WAIT)?;
        if !report.is_complete()
            || !report.locally_drained
            || report.completed_requests != 1
            || report.transferred_bytes != LENGTH
        {
            return Err(Error::Internal("incomplete GDS transfer"));
        }
        if op == TransferOp::Write {
            file_bytes[GUARD..GUARD + LENGTH].copy_from_slice(&source[GUARD..GUARD + LENGTH]);
            if fs::read(path)? != file_bytes {
                return Err(Error::Internal("GDS WRITE bytes or guards changed"));
            }
            view.write(0, &vec![0xac; total])?;
        }
    }
    let mut actual = vec![0; total];
    view.read(0, &mut actual)?;
    let mut expected = vec![0xac; total];
    expected[GUARD..GUARD + LENGTH].copy_from_slice(&source[GUARD..GUARD + LENGTH]);
    if actual != expected {
        return Err(Error::Internal("GDS READ bytes or guards changed"));
    }
    let info = engine.file_segment_info(&segment)?;
    let stats = engine.stats()?.gds;
    if info.direct_io != Some(true)
        || info.gds_mode != Some(GdsMode::NativeOnly)
        || stats.submitted_batches != 2
        || stats.quarantined_batches != 0
    {
        return Err(Error::Internal(
            "GDS direct descriptor, policy or batch accounting",
        ));
    }
    engine.unregister_memory(&view)?;
    drop(view);
    engine.close_file_segment(&segment, WAIT)?;
    engine.close(WAIT)?;
    let retained = retained_resources();
    if retained.contexts
        + retained.device_allocations
        + retained.events
        + retained.streams
        + retained.pinned_allocations
        != 0
    {
        return Err(Error::Internal("GDS gate retained CUDA owners"));
    }
    println!("GDS_NATIVE_OK read_bytes={LENGTH} write_bytes={LENGTH} batches=2");
    Ok(())
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [flag] if flag == "--protocol" => println!("orbitte-installed-gds-v1"),
        [flag, path] if flag == "--exercise" => exercise(Path::new(path))?,
        _ => return Err("usage: orbitte-gds --protocol | --exercise NEW_FILE".into()),
    }
    Ok(())
}
