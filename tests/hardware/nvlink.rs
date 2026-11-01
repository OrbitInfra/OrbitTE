//! Installed, independent-process GPU IPC gate. Requires explicitly selected GPUs.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orbit_te::cuda::{CudaMemory, allocate_pinned_host, retained_resources};
use orbit_te::{
    EngineConfig, Error, HostMemory, Permission, RegisteredMemory, Result, TransferEngine,
    TransferOp, TransferRequest, Transport,
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

enum LocalMemory {
    Device(Arc<CudaMemory>),
    Host(Arc<HostMemory>),
}

impl LocalMemory {
    fn len(&self) -> usize {
        match self {
            Self::Device(m) => m.len(),
            Self::Host(m) => m.len(),
        }
    }
    fn read(&self, bytes: &mut [u8]) -> Result<()> {
        match self {
            Self::Device(m) => m.read(0, bytes),
            Self::Host(m) => m.read(0, bytes),
        }
    }
    fn write(&self, offset: usize, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Device(m) => m.write(offset, bytes),
            Self::Host(m) => m.write(offset, bytes),
        }
    }
    fn register(&self, engine: &TransferEngine) -> Result<RegisteredMemory> {
        match self {
            Self::Device(m) => Ok(engine
                .register_cuda_memory_batch(&[(m.clone(), 0, m.len())], Permission::LocalOnly)?
                .remove(0)),
            Self::Host(m) => {
                engine.register_host_memory(m.clone(), 0, m.len(), Permission::LocalOnly)
            }
        }
    }
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
    let cached = mode.starts_with("cache-");
    let mode = mode
        .strip_prefix("cache-")
        .or_else(|| mode.strip_prefix("batch-"))
        .unwrap_or(mode);
    let engine = TransferEngine::new(EngineConfig {
        nvlink: true,
        nvlink_cache_entries: if cached { 2 } else { 0 },
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
        if stats.active_source_operations != usize::from(!cached)
            || ipc.exports != 1
            || ipc.retained_exports != usize::from(!cached)
            || ipc.allocation_bytes != extent
            || memory.read(0, &mut [0; 1]).is_ok()
        {
            return Err(Error::Internal(
                "lost IPC receipt did not retain source ownership/budget",
            ));
        }
        println!(
            "NVLINK_SOURCE_RETAINED pid={} extent={extent} source_ops={}",
            std::process::id(),
            stats.active_source_operations
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

fn requester(
    device: i32,
    payload: usize,
    root: &Path,
    mode: &str,
    memory_kind: &str,
) -> Result<()> {
    let cached = mode.starts_with("cache-");
    let batched = cached || mode.starts_with("batch-");
    let mode = mode
        .strip_prefix("cache-")
        .or_else(|| mode.strip_prefix("batch-"))
        .unwrap_or(mode);
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
        nvlink_cache_entries: if cached { 2 } else { 0 },
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
    let memories: Vec<_> = (0..if batched { 2 } else { 1 })
        .map(|index| {
            if memory_kind == "host" || (memory_kind == "mixed" && index == 0) {
                Ok(LocalMemory::Host(allocate_pinned_host(
                    device,
                    payload + 2 * GUARD,
                )?))
            } else {
                Ok(LocalMemory::Device(Arc::new(CudaMemory::allocate(
                    device,
                    payload + 2 * GUARD,
                )?)))
            }
        })
        .collect::<Result<_>>()?;
    for memory in &memories {
        memory.write(0, &vec![0xa5; memory.len()])?;
    }
    let registered: Vec<_> = memories
        .iter()
        .map(|m| m.register(&engine))
        .collect::<Result<_>>()?;
    let third = payload / 3;
    let slices = if batched {
        vec![(2 * third, payload - 2 * third), (0, third), (third, third)]
    } else {
        vec![(0, payload)]
    };
    let requests: Vec<_> = slices
        .iter()
        .enumerate()
        .map(|(index, &(offset, length))| TransferRequest {
            priority: Default::default(),
            remote_staging: false,
            transport: Transport::Nvlink,
            local_address: registered[index % memories.len()].base_address()
                + (GUARD + offset) as u64,
            remote_address: address + (GUARD + offset) as u64,
            length,
        })
        .collect();
    if mode == "healthy" {
        if cached {
            let barrier = std::sync::Barrier::new(4);
            std::thread::scope(|copies| -> Result<()> {
                let tasks: Vec<_> = (0..4)
                    .map(|_| {
                        copies.spawn(|| {
                            barrier.wait();
                            engine.submit_and_wait(TransferOp::Read, &peer, &requests, TIMEOUT)
                        })
                    })
                    .collect();
                for task in tasks {
                    let report = task
                        .join()
                        .map_err(|_| Error::Internal("cached requester panicked"))??;
                    if !report.is_complete() || !report.locally_drained {
                        eprintln!("cached READ: {report:?}");
                        return Err(Error::Internal("cached READ incomplete"));
                    }
                }
                Ok(())
            })?;
        } else {
            let report = engine.submit_and_wait(TransferOp::Read, &peer, &requests, TIMEOUT)?;
            if !report.is_complete() || !report.locally_drained {
                return Err(Error::Internal("NVLink READ incomplete"));
            }
        }
        for (storage, memory) in memories.iter().enumerate() {
            let mut expected = vec![0xa5; memory.len()];
            let data = pattern(payload, 11);
            for (index, &(offset, length)) in slices.iter().enumerate() {
                if index % memories.len() == storage {
                    expected[GUARD + offset..GUARD + offset + length]
                        .copy_from_slice(&data[offset..offset + length]);
                }
            }
            let mut actual = vec![0; memory.len()];
            memory.read(&mut actual)?;
            if actual != expected {
                return Err(Error::Internal("NVLink READ slices/gaps/guards mismatch"));
            }
        }
    }
    for memory in &memories {
        memory.write(GUARD, &pattern(payload, 53))?;
    }
    let report = engine.submit_and_wait(TransferOp::Write, &peer, &requests, TIMEOUT)?;
    if !report.is_complete() || !report.locally_drained {
        eprintln!("NVLink WRITE report: {report:?}");
        return Err(Error::Internal("NVLink WRITE incomplete"));
    }
    for memory in &memories {
        let mut bytes = vec![0; memory.len()];
        memory.read(&mut bytes)?;
        if bytes[GUARD..GUARD + payload] != pattern(payload, 53)
            || bytes[..GUARD]
                .iter()
                .chain(bytes[GUARD + payload..].iter())
                .any(|&b| b != 0xa5)
        {
            return Err(Error::Internal("requester WRITE changed source/guards"));
        }
    }
    if cached {
        let stats = engine.stats()?;
        let ipc = stats.nvlink.ok_or(Error::Internal("missing cache stats"))?;
        if ipc.imports != 1 || stats.inflight_bytes != 0 {
            return Err(Error::Internal(
                "cached mapping or completed byte accounting",
            ));
        }
        println!(
            "NVLINK_CACHE_IDLE imports=1 completed_submissions={}",
            if mode == "healthy" { 5 } else { 1 }
        );
    }
    engine.close(TIMEOUT)?;
    for region in &registered {
        engine.unregister_memory(region)?;
    }
    drop(registered);
    drop(memories);
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
    if args.len() != 6 {
        return Err("nvlink source|requester <device> <payload> <external-root> healthy|graceful|requester-exit|stale device|host|mixed".into());
    }
    let device = args[1].parse()?;
    let payload: usize = args[2].parse()?;
    if payload == 0 || payload > 8 << 20 {
        return Err("payload out of bounds".into());
    }
    let mode = args[4].as_str();
    if ![
        "healthy",
        "graceful",
        "requester-exit",
        "stale",
        "batch-healthy",
        "batch-graceful",
        "batch-requester-exit",
        "cache-healthy",
        "cache-graceful",
        "cache-requester-exit",
    ]
    .contains(&mode)
    {
        return Err("unknown mode".into());
    }
    let grouped = mode.starts_with("batch-") || mode.starts_with("cache-");
    if grouped && payload < 3 {
        return Err("batch requires at least three bytes".into());
    }
    let memory_kind = args[5].as_str();
    if !["device", "host", "mixed"].contains(&memory_kind) || (memory_kind == "mixed" && !grouped) {
        return Err("invalid requester memory profile".into());
    }
    match args[0].as_str() {
        "source" => source(device, payload, Path::new(&args[3]), mode)?,
        "requester" => requester(device, payload, Path::new(&args[3]), mode, memory_kind)?,
        _ => return Err("unknown role".into()),
    }
    Ok(())
}
