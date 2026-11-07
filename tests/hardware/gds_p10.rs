//! Installed native GDS READ: held CQ receipt, concurrent unregister/close, drain or quarantine.
use orbite::{
    EngineConfig, Error, GdsMode, Permission, Result, TransferEngine, TransferOp, TransferRequest,
    Transport, cuda::CudaMemory,
};
use std::{
    fs,
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
const LEN: usize = 2 << 20;
const PAGE: usize = 4096;
const WAIT: Duration = Duration::from_secs(60);
fn require(ok: bool, why: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Internal(why))
    }
}
fn exercise(path: &Path) -> Result<()> {
    let control = std::env::var_os("ORBITE_GDS_P10_CONTROL")
        .ok_or(Error::InvalidConfig("missing installed observer control"))?;
    let control = Path::new(&control);
    let quarantine = std::env::var("ORBITE_GDS_P10_MODE").as_deref() == Ok("quarantine");
    let engine = Arc::new(TransferEngine::new(EngineConfig {
        gds_require_native: true,
        ..Default::default()
    })?);
    let mut image = vec![0x39; LEN + 2 * PAGE];
    for (i, byte) in image[PAGE..PAGE + LEN].iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }
    fs::write(path, &image)?;
    let file = engine.open_file_segment(
        &format!("file://{}", path.display()),
        Permission::RemoteReadWrite,
    )?;
    let gpu = Arc::new(CudaMemory::allocate(0, LEN + 2 * PAGE)?);
    let weak = Arc::downgrade(&gpu);
    let memory = engine
        .register_cuda_memory_batch(&[(gpu, 0, LEN + 2 * PAGE)], Permission::LocalOnly)?
        .remove(0);
    memory.write(0, &vec![0xa5; LEN + 2 * PAGE])?;
    let request = TransferRequest::new(
        Transport::Gds,
        memory.base_address() + PAGE as u64,
        PAGE as u64,
        LEN,
    );
    let batch = engine.submit_async(TransferOp::Read, &file, &[request], None, WAIT)?;
    let end = Instant::now() + WAIT;
    while !control.join("posted").exists() {
        require(Instant::now() < end, "real cuFile submit not observed")?;
        thread::sleep(Duration::from_millis(1));
    }
    require(
        engine.stats()?.gds.mode == Some(GdsMode::NativeOnly),
        "compat mode is not native GDS evidence",
    )?;
    require(
        matches!(batch.wait(Duration::ZERO), Err(Error::Timeout)),
        "held CQ is not a receipt",
    )?;
    engine.unregister_memory(&memory)?;
    require(
        matches!(
            engine.close_file_segment(&file, Duration::ZERO),
            Err(Error::Timeout)
        ),
        "file closed before held CQ drained",
    )?;
    require(
        engine.stats()?.inflight_bytes == LEN,
        "held GDS byte charge lost",
    )?;
    if !quarantine {
        fs::write(control.join("release"), [])?;
    }
    let status = batch.wait(Duration::from_secs(100))?;
    if quarantine {
        let task = batch.task_status(0)?;
        require(
            !status.locally_drained
                && matches!(
                    task.failure.as_ref().map(|f| &f.error),
                    Some(Error::Unquiesced { .. })
                ),
            "lost receipt was released",
        )?;
        require(
            engine.stats()?.gds.quarantined_batches == 1 && engine.stats()?.inflight_bytes == LEN,
            "quarantine lost charge",
        )?;
        let mut guard = vec![0; PAGE];
        memory.read(0, &mut guard)?;
        require(guard == vec![0xa5; PAGE], "leading guard modified")?;
        memory.read(PAGE + LEN, &mut guard)?;
        require(guard == vec![0xa5; PAGE], "trailing guard modified")?;
        drop(memory);
        drop(batch);
        drop(file);
        drop(engine);
        require(weak.upgrade().is_some(), "quarantine lost GPU backing")?;
    } else {
        require(
            status.locally_drained
                && status.completed_tasks == 1
                && status.transferred_bytes == LEN,
            "native GDS READ incomplete",
        )?;
        engine.close_file_segment(&file, WAIT)?;
        let mut got = vec![0; LEN + 2 * PAGE];
        memory.read(0, &mut got)?;
        image[..PAGE].fill(0xa5);
        image[PAGE + LEN..].fill(0xa5);
        require(got == image, "GDS payload or guards changed")?;
        drop(memory);
        drop(batch);
        engine.close(WAIT)?;
        require(
            engine.stats()?.inflight_bytes == 0 && weak.upgrade().is_none(),
            "drained GDS owners not released",
        )?;
    }
    println!(
        "P10_GDS_OK mode={}",
        if quarantine { "quarantine" } else { "drain" }
    );
    Ok(())
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [s] if s == "--protocol" => println!("orbite-installed-gds-p10"),
        [s, path] if s == "--exercise" => exercise(Path::new(path))?,
        _ => return Err("--protocol | --exercise FILE".into()),
    }
    Ok(())
}
