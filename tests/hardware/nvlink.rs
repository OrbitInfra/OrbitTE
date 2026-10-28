//! Installed, independent-process GPU IPC gate. Requires explicitly selected GPUs.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orbit_te::cuda::{CudaMemory, retained_resources};
use orbit_te::{
    EngineConfig, Error, Permission, Result, TransferEngine, TransferOp, TransferRequest, Transport,
};

const GUARD: usize = 67;
const TIMEOUT: Duration = Duration::from_secs(30);

fn pattern(length: usize, salt: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index * 37 + salt) % 251) as u8)
        .collect()
}

fn verify(memory: &CudaMemory, payload: usize, salt: usize) -> Result<()> {
    let mut bytes = vec![0; memory.len()];
    memory.read(0, &mut bytes)?;
    if bytes[GUARD..GUARD + payload] != pattern(payload, salt)
        || bytes[..GUARD]
            .iter()
            .chain(bytes[GUARD + payload..].iter())
            .any(|&byte| byte != 0xa5)
    {
        return Err(Error::Internal("NVLink full allocation/guard mismatch"));
    }
    Ok(())
}

fn released(engine: &TransferEngine) -> Result<()> {
    let stats = engine.stats()?;
    let ipc = stats.nvlink.ok_or(Error::Internal("NVLink not enabled"))?;
    let cuda = retained_resources();
    if stats.retained_regions != 0
        || stats.inflight_bytes != 0
        || stats.active_source_operations != 0
        || ipc.allocation_bytes != 0
        || ipc.exports != 0
        || ipc.imports != 0
        || ipc.retained_exports != 0
        || ipc.retained_imports != 0
        || cuda.device_allocations != 0
        || cuda.pinned_allocations != 0
        || cuda.contexts != 0
        || cuda.streams != 0
        || cuda.events != 0
        || cuda.ipc_mappings != 0
        || cuda.ipc_exports != 0
    {
        return Err(Error::Internal("NVLink healthy close retained resources"));
    }
    Ok(())
}

fn source(device: i32, payload: usize, root: &Path, mode: &str) -> Result<()> {
    let engine = TransferEngine::new(EngineConfig {
        nvlink: true,
        handler_timeout: TIMEOUT,
        ..EngineConfig::default()
    })?;
    let memory = Arc::new(CudaMemory::allocate_ipc(device, payload + 2 * GUARD)?);
    memory.write(0, &vec![0xa5; memory.len()])?;
    memory.write(GUARD, &pattern(payload, 11))?;
    let registered = engine
        .register_cuda_memory_batch(
            &[(memory.clone(), 0, memory.len())],
            Permission::RemoteReadWrite,
        )?
        .remove(0);
    let ready = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        std::process::id(),
        engine.endpoint(),
        engine.source_epoch(),
        registered.base_address(),
        memory.len()
    );
    fs::write(root.join("ready.tmp"), ready)?;
    fs::rename(root.join("ready.tmp"), root.join("ready"))?;
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut pending = false;
    while !root.join("finish").exists() {
        if mode == "graceful" && !pending && root.join("close-source").exists() {
            if !matches!(
                engine.close_source(Duration::from_millis(10)),
                Err(Error::Timeout)
            ) {
                return Err(Error::Internal("source close ignored a live IPC mapping"));
            }
            if engine.stats()?.nvlink.map(|stats| stats.exports) != Some(1) {
                return Err(Error::Internal("close lost the exported allocation"));
            }
            if memory.read(0, &mut [0; 1]).is_ok() {
                return Err(Error::Internal("live export released its allocation loan"));
            }
            fs::write(root.join("close-pending"), b"retained\n")?;
            pending = true;
        }
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    if mode == "requester-exit" {
        if !matches!(
            engine.close_source(Duration::from_millis(100)),
            Err(Error::Timeout)
        ) {
            return Err(Error::Internal(
                "requester exit became a false close receipt",
            ));
        }
        let stats = engine.stats()?;
        let ipc = stats.nvlink.ok_or(Error::Internal("missing IPC stats"))?;
        let extent = (memory.len() + (2 << 20) - 1) & !((2 << 20) - 1);
        if stats.active_source_operations != 1
            || ipc.exports != 1
            || ipc.retained_exports != 1
            || ipc.allocation_bytes != extent
            || memory.read(0, &mut [0; 1]).is_ok()
        {
            return Err(Error::Internal(
                "lost IPC receipt did not retain source ownership/budget",
            ));
        }
        println!(
            "NVLINK_SOURCE_RETAINED pid={} extent={extent} source_ops=1",
            std::process::id()
        );
        return Ok(());
    }
    engine.close(TIMEOUT)?;
    if mode == "graceful" && !pending {
        return Err(Error::Internal("graceful close control never armed"));
    }
    verify(&memory, payload, if mode == "stale" { 11 } else { 53 })?;
    engine.unregister_memory(&registered)?;
    drop(registered);
    drop(memory);
    released(&engine)?;
    println!(
        "NVLINK_SOURCE_OK pid={} payload={payload} full_bytes={} retained=0",
        std::process::id(),
        payload + 2 * GUARD
    );
    Ok(())
}

fn requester(device: i32, payload: usize, root: &Path, mode: &str) -> Result<()> {
    let ready = fs::read_to_string(root.join("ready"))?;
    let fields: Vec<_> = ready.lines().collect();
    if fields.len() != 5 {
        return Err(Error::Protocol("NVLink fixture source descriptor"));
    }
    let endpoint = fields[1]
        .parse()
        .map_err(|_| Error::Protocol("source endpoint"))?;
    let epoch = fields[2].parse()?;
    let address: u64 = fields[3]
        .parse()
        .map_err(|_| Error::Protocol("source address"))?;
    let engine = TransferEngine::new(EngineConfig {
        nvlink: true,
        ..EngineConfig::default()
    })?;
    if mode == "stale" {
        let stale = "00000000000000000000000000000000".parse()?;
        if !matches!(
            engine.open_segment(endpoint, stale, TIMEOUT),
            Err(Error::EpochMismatch)
        ) {
            return Err(Error::Internal("stale source generation was accepted"));
        }
        engine.close(TIMEOUT)?;
        released(&engine)?;
        println!("NVLINK_STALE_REJECTED pid={} imports=0", std::process::id());
        return Ok(());
    }
    let peer = engine.open_segment(endpoint, epoch, TIMEOUT)?;
    let memory = Arc::new(CudaMemory::allocate(device, payload + 2 * GUARD)?);
    memory.write(0, &vec![0xa5; memory.len()])?;
    let registered = engine
        .register_cuda_memory_batch(&[(memory.clone(), 0, memory.len())], Permission::LocalOnly)?
        .remove(0);
    let request = TransferRequest {
        transport: Transport::Nvlink,
        local_address: registered.base_address() + GUARD as u64,
        remote_address: address + GUARD as u64,
        length: payload,
    };
    if mode == "healthy" {
        let report = engine.submit_and_wait(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&request),
            TIMEOUT,
        )?;
        if !report.is_complete() || !report.locally_drained {
            return Err(Error::Internal("NVLink READ incomplete"));
        }
        verify(&memory, payload, 11)?;
    }
    memory.write(GUARD, &pattern(payload, 53))?;
    let report = engine.submit_and_wait(TransferOp::Write, &peer, &[request], TIMEOUT)?;
    if !report.is_complete() || !report.locally_drained {
        eprintln!("NVLink WRITE report: {report:?}");
        return Err(Error::Internal("NVLink WRITE incomplete"));
    }
    verify(&memory, payload, 53)?;
    engine.close(TIMEOUT)?;
    engine.unregister_memory(&registered)?;
    drop(registered);
    drop(memory);
    released(&engine)?;
    println!(
        "NVLINK_REQUESTER_OK pid={} payload={payload} retained=0",
        std::process::id()
    );
    Ok(())
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--protocol"] {
        println!("orbitte-installed-nvlink-v1");
        return Ok(());
    }
    if args.len() != 5 {
        return Err("nvlink source|requester <device> <payload> <external-root> healthy|graceful|requester-exit|stale".into());
    }
    let device = args[1].parse()?;
    let payload: usize = args[2].parse()?;
    if payload == 0 || payload > 8 << 20 {
        return Err("payload out of bounds".into());
    }
    let mode = args[4].as_str();
    if !["healthy", "graceful", "requester-exit", "stale"].contains(&mode) {
        return Err("unknown mode".into());
    }
    match args[0].as_str() {
        "source" => source(device, payload, Path::new(&args[3]), mode)?,
        "requester" => requester(device, payload, Path::new(&args[3]), mode)?,
        _ => return Err("unknown role".into()),
    }
    Ok(())
}
