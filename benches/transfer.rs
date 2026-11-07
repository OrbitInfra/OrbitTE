//! Public-API, byte-checked CPU benchmark. Raw samples go to the external runner output.
use orbite::{
    EngineConfig, Error, Permission, RegisteredMemory, Result, TransferEngine, TransferOp,
    TransferRequest, TransferTarget, Transport,
};
use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

const PAGE: usize = 4096;
const LIMIT: usize = 64 << 20;
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct Parameters {
    backend: String,
    op: TransferOp,
    block: usize,
    batch: usize,
    gap: bool,
    warmup: usize,
    samples: usize,
    workers: usize,
    path: String,
}
impl Parameters {
    fn parse() -> Result<Self> {
        let a: Vec<_> = std::env::args()
            .skip(1)
            .filter(|a| a != "--bench")
            .collect();
        let bad = || {
            Error::InvalidConfig(
                "backend read|write block batch contiguous|gapped warmup samples workers file",
            )
        };
        if a.len() != 9 {
            return Err(bad());
        }
        let n = |i: usize| a[i].parse::<usize>().map_err(|_| bad());
        let p = Self {
            backend: a[0].clone(),
            op: match a[1].as_str() {
                "read" => TransferOp::Read,
                "write" => TransferOp::Write,
                _ => return Err(bad()),
            },
            block: n(2)?,
            batch: n(3)?,
            gap: a[4] == "gapped",
            warmup: n(5)?,
            samples: n(6)?,
            workers: n(7)?,
            path: a[8].clone(),
        };
        if !matches!(a[4].as_str(), "contiguous" | "gapped")
            || !matches!(
                p.backend.as_str(),
                "cpu" | "tcp" | "hp_tcp" | "shm" | "io_uring"
            )
            || p.block < PAGE
            || p.block % PAGE != 0
            || p.block > LIMIT
            || !(1..=64).contains(&p.batch)
            || p.block * p.batch > LIMIT
            || !(1..=10000).contains(&p.samples)
            || !(1..=100).contains(&p.warmup)
            || !(1..=16).contains(&p.workers)
        {
            return Err(bad());
        }
        Ok(p)
    }
    fn stride(&self) -> usize {
        self.block * if self.gap { 2 } else { 1 }
    }
    fn extent(&self) -> usize {
        (self.batch - 1) * self.stride() + self.block
    }
    fn image(&self, iteration: usize) -> Vec<u8> {
        let mut image = vec![0xa5; self.extent()];
        for i in 0..self.batch {
            for j in 0..self.block {
                image[i * self.stride() + j] = ((j * 17 + i * 37 + iteration * 13) % 251) as u8;
            }
        }
        image
    }
}

#[cfg(unix)]
fn cpu_ns() -> Result<u64> {
    let mut t = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the writable timespec on success.
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, t.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: the call above succeeded.
    let t = unsafe { t.assume_init() };
    Ok(t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64)
}
#[cfg(not(unix))]
fn cpu_ns() -> Result<u64> {
    Err(Error::Unavailable("benchmark CPU clock requires Unix"))
}

fn check(ok: bool, message: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Internal(message))
    }
}
fn threads() -> usize {
    fs::read_dir("/proc/self/task")
        .map(|d| d.count())
        .unwrap_or(0)
}
fn result(
    p: &Parameters,
    ns: Vec<u64>,
    cpu: Vec<u64>,
    owners: usize,
    process_threads: usize,
    direct: Option<bool>,
) {
    println!(
        "{}",
        serde_json::json!({"verified": true, "samples_ns": ns, "cpu_ns": cpu, "physical_owners": owners, "process_threads": process_threads, "direct_io": direct, "payload_bytes": p.block * p.batch})
    );
}
fn cpu(p: &Parameters) -> Result<()> {
    let mut src = vec![0xa5; p.extent() + 3 * PAGE];
    let mut dst = src.clone();
    let aligned = |data: &[u8]| {
        let base = data.as_ptr() as usize;
        base.next_multiple_of(PAGE) - base + PAGE
    };
    let so = aligned(&src);
    let to = aligned(&dst);
    let mut ns = Vec::new();
    let mut cpus = Vec::new();
    for i in 0..p.warmup + p.samples {
        let image = p.image(i);
        src[so..so + p.extent()].copy_from_slice(&image);
        dst.fill(0xa5);
        let c = cpu_ns()?;
        let start = Instant::now();
        for b in 0..p.batch {
            let off = b * p.stride();
            dst[to + off..to + off + p.block].copy_from_slice(&src[so + off..so + off + p.block]);
        }
        let elapsed = start.elapsed().as_nanos() as u64;
        let c = cpu_ns()? - c;
        for (buffer, offset) in [(&src, so), (&dst, to)] {
            check(
                buffer[offset..offset + p.extent()] == image
                    && buffer[..offset]
                        .iter()
                        .chain(buffer[offset + p.extent()..].iter())
                        .all(|b| *b == 0xa5),
                "CPU bytes or guards differ",
            )?;
        }
        if i >= p.warmup {
            ns.push(elapsed);
            cpus.push(c);
        }
    }
    result(p, ns, cpus, p.batch, threads(), None);
    Ok(())
}
fn memory(
    e: &TransferEngine,
    size: usize,
    shm: bool,
    permission: Permission,
) -> Result<(RegisteredMemory, usize)> {
    let m = if shm {
        let m = e.allocate_shm_memory(size)?;
        e.register_host_memory(m, 0, size, permission)?
    } else {
        e.register_memory(vec![0xa5; size].into(), permission)?
    };
    let offset =
        (m.base_address().next_multiple_of(PAGE as u64) - m.base_address()) as usize + PAGE;
    Ok((m, offset))
}
fn verify(m: &RegisteredMemory, offset: usize, image: &[u8]) -> Result<()> {
    let mut got = vec![0; m.len()];
    m.read(0, &mut got)?;
    check(
        got[..offset]
            .iter()
            .chain(got[offset + image.len()..].iter())
            .all(|b| *b == 0xa5)
            && got[offset..offset + image.len()] == *image,
        "payload or registration guards differ",
    )
}
fn run(p: &Parameters) -> Result<()> {
    if p.backend == "cpu" {
        return cpu(p);
    }
    let transport = match p.backend.as_str() {
        "tcp" => Transport::Tcp,
        "hp_tcp" => Transport::HpTcp,
        "shm" => Transport::Shm,
        _ => Transport::IoUring,
    };
    let config = EngineConfig {
        handler_threads: p.workers,
        merge_requests: true,
        max_failover_attempts: 0,
        max_transfer_bytes: LIMIT,
        max_inflight_bytes: LIMIT,
        hp_tcp: orbite::HpTcpConfig {
            enable: transport == Transport::HpTcp,
            worker_count: p.workers,
            rail_addresses: vec![
                "127.0.0.1".parse().map_err(|_| Error::InvalidRange)?,
                "127.0.0.2".parse().map_err(|_| Error::InvalidRange)?,
            ],
            connections_per_peer: 2,
            ..Default::default()
        },
        tcp: transport != Transport::HpTcp,
        shm: orbite::ShmConfig {
            enable: transport == Transport::Shm,
            ..Default::default()
        },
        ..Default::default()
    };
    let source = TransferEngine::new(config.clone())?;
    let requester = Arc::new(TransferEngine::new(config)?);
    let (local, lo) = memory(
        &requester,
        p.extent() + 3 * PAGE,
        false,
        Permission::LocalOnly,
    )?;
    let (remote, ro) = memory(
        &source,
        p.extent() + 3 * PAGE,
        transport == Transport::Shm,
        Permission::RemoteReadWrite,
    )?;
    let is_file = transport == Transport::IoUring;
    let file = if is_file {
        fs::write(&p.path, vec![0xa5; p.extent() + 2 * PAGE])?;
        Some(
            requester
                .open_file_segment(&format!("file://{}", p.path), Permission::RemoteReadWrite)?,
        )
    } else {
        None
    };
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let target = file
        .as_ref()
        .map_or(TransferTarget::Memory(&peer), TransferTarget::File);
    let base = if is_file {
        PAGE as u64
    } else {
        remote.base_address() + ro as u64
    };
    let requests: Vec<_> = (0..p.batch)
        .map(|i| {
            TransferRequest::new(
                transport,
                local.base_address() + (lo + i * p.stride()) as u64,
                base + (i * p.stride()) as u64,
                p.block,
            )
        })
        .collect();
    let blank = vec![0xa5; p.extent()];
    let mut ns = Vec::new();
    let mut cpus = Vec::new();
    let mut owners = 0;
    for i in 0..p.warmup + p.samples {
        let image = p.image(i);
        local.write(0, &vec![0xa5; local.len()])?;
        remote.write(0, &vec![0xa5; remote.len()])?;
        if p.op == TransferOp::Write {
            local.write(lo, &image)?;
        } else if !is_file {
            remote.write(ro, &image)?;
        }
        if is_file {
            let mut bytes = vec![0xa5; p.extent() + 2 * PAGE];
            if p.op == TransferOp::Read {
                bytes[PAGE..PAGE + p.extent()].copy_from_slice(&image);
            }
            fs::write(&p.path, bytes)?;
        }
        let c = cpu_ns()?;
        let start = Instant::now();
        let mut batch = requester.allocate_batch(p.batch)?;
        batch.submit(p.op, target, &requests, None, TIMEOUT)?;
        batch.seal()?;
        let mut polls = 0usize;
        loop {
            let s = batch.status()?;
            if s.is_terminal() {
                check(
                    s.completed_tasks == p.batch
                        && s.transferred_bytes == p.batch * p.block
                        && s.locally_drained,
                    "incomplete benchmark batch",
                )?;
                break;
            }
            check(start.elapsed() < TIMEOUT, "benchmark deadline expired")?;
            if polls >= 128 {
                std::thread::sleep(Duration::from_micros(
                    (1u64 << (polls - 128).min(8)).min(200),
                ));
            }
            polls = polls.saturating_add(1);
        }
        // Inspect aliases only in the discarded warmup. Samples time allocate/submit/poll/free.
        if i == 0 {
            owners = batch
                .task_statuses()?
                .iter()
                .enumerate()
                .filter(|(id, s)| s.physical_owner == *id)
                .count();
        }
        batch.free()?;
        let elapsed = start.elapsed().as_nanos() as u64;
        let c = cpu_ns()? - c;

        if std::env::var_os("ORBITE_BENCH_CORRUPT_BYTE").is_some() {
            local.write(0, &[0])?;
        }
        verify(&local, lo, &image)?;
        if is_file {
            let mut expected = vec![0xa5; p.extent() + 2 * PAGE];
            expected[PAGE..PAGE + p.extent()].copy_from_slice(&image);
            check(
                fs::read(&p.path)? == expected,
                "file bytes or guards differ",
            )?;
            verify(&remote, ro, &blank)?;
        } else {
            verify(&remote, ro, &image)?;
        }
        if i >= p.warmup {
            ns.push(elapsed);
            cpus.push(c);
        }
    }
    #[cfg(feature = "io-uring")]
    let direct = file
        .as_ref()
        .map(|f| requester.file_segment_info(f))
        .transpose()?
        .and_then(|i| i.direct_io);
    #[cfg(not(feature = "io-uring"))]
    let direct = None;
    if let Some(file) = &file {
        requester.close_file_segment(file, TIMEOUT)?;
    }
    let process_threads = threads();
    requester.close(TIMEOUT)?;
    source.close(TIMEOUT)?;
    result(p, ns, cpus, owners, process_threads, direct);
    Ok(())
}
fn main() -> Result<()> {
    run(&Parameters::parse()?)
}
