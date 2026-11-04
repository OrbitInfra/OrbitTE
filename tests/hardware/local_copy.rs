//! Packaged public-API comparison with OrbitKV's native_kernel_copy custom-fragments case.
use cudarc::driver::CudaContext;
use orbit_te::cuda::{
    CudaMemory, allocate_pinned_host,
    local_copy::{
        CopyDirection, LocalCopyBackend, LocalCopyConfig, LocalCopyDesc, LocalCopyEngine,
    },
};
use orbit_te::{EngineConfig, Permission, TransferEngine};
use serde_json::json;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

const GUARD: usize = 32;
type GateResult<T> = Result<T, Box<dyn Error>>;
fn trial(
    worker: &LocalCopyEngine,
    dir: CopyDirection,
    rows: &[LocalCopyDesc],
    backend: LocalCopyBackend,
) -> GateResult<f64> {
    let start = Instant::now();
    let flight = worker.submit(dir, rows, None)?;
    if flight.backend() != backend {
        return Err("wrong executing backend".into());
    }
    // Match CPU wall time from submit entry to a physical receipt. The baseline
    // synchronizes its stream. OrbitTE observes the private event on that stream.
    let deadline = start + Duration::from_secs(30);
    while !flight.poll()? {
        if Instant::now() >= deadline {
            return Err("local copy missed gate deadline".into());
        }
        std::hint::spin_loop();
    }
    Ok(start.elapsed().as_secs_f64() * 1000.0)
}
fn main() -> GateResult<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--protocol"] {
        println!("orbitte-installed-local-copy");
        return Ok(());
    }
    let [blocks, width, samples] = args.as_slice() else {
        return Err("expected BLOCKS FRAGMENT_BYTES SAMPLES".into());
    };
    let (blocks, width, samples) = (
        blocks.parse::<usize>()?,
        width.parse::<usize>()?,
        samples.parse::<usize>()?,
    );
    if !(1..=4096).contains(&blocks)
        || !(1..=65536).contains(&width)
        || !(9..=200).contains(&samples)
    {
        return Err("outside bounded gate dimensions".into());
    }
    let stride = width.div_ceil(16) * 16 + 2 * GUARD;
    let total = blocks * stride + 2 * GUARD + 16;
    if total > 64 << 20 {
        return Err("fixture exceeds 64 MiB".into());
    }
    let ctx = CudaContext::new(0)?;
    let uuid = ctx
        .uuid()?
        .bytes
        .map(|b| format!("{:02x}", b.to_ne_bytes()[0]))
        .concat();
    let uuid = format!(
        "GPU-{}-{}-{}-{}-{}",
        &uuid[0..8],
        &uuid[8..12],
        &uuid[12..16],
        &uuid[16..20],
        &uuid[20..]
    );
    let stream = ctx.default_stream();
    let engine = TransferEngine::new(EngineConfig::default())?;
    let device = Arc::new(CudaMemory::allocate(0, total)?);
    let host = allocate_pinned_host(0, total)?;
    let g = engine
        .register_cuda_memory_batch(&[(device, 0, total)], Permission::LocalOnly)?
        .remove(0);
    let h = engine
        .register_host_memory_batch(&[(host, 0, total)], Permission::LocalOnly)?
        .remove(0);
    let host_guard = vec![0xa5; total];
    let device_guard = vec![0xcd; total];
    let mut pattern = vec![0xa5; total];
    let mut expected = vec![0xcd; total];
    let mut rows = Vec::with_capacity(blocks);
    for id in 0..blocks {
        let off = GUARD + stride * id;
        for j in 0..width {
            let byte = ((j * 31) ^ (j >> 8) ^ (id * 17)) as u8;
            pattern[off + j] = byte;
            expected[off + j] = byte;
        }
        rows.push(LocalCopyDesc::new(&g, off, &h, off, width));
    }
    rows.reverse();
    let backends = [LocalCopyBackend::Kernel, LocalCopyBackend::Dma];
    let workers: Vec<_> = backends
        .iter()
        .map(|&backend| {
            ctx.bind_to_thread()?;
            // SAFETY: this caller owns the retained primary context and its default
            // stream. No capture, mutation or external access overlaps a flight.
            Ok::<_, Box<dyn Error>>(unsafe {
                LocalCopyEngine::bind(
                    &g,
                    stream.cu_stream().cast(),
                    stream.clone(),
                    LocalCopyConfig {
                        backend,
                        max_descriptors: blocks,
                        max_inflight: 1,
                        ..Default::default()
                    },
                )
            }?)
        })
        .collect::<Result<_, _>>()?;
    for dir in [CopyDirection::HostToDevice, CopyDirection::DeviceToHost] {
        let h2d = dir == CopyDirection::HostToDevice;
        let mut times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
        let mut actual = vec![0; total];
        for id in 0..2 {
            h.write(0, if h2d { &pattern } else { &host_guard })?;
            g.write(0, if h2d { &device_guard } else { &expected })?;
            trial(&workers[id], dir, &rows, backends[id])?;
            h.read(0, &mut actual)?;
            if actual != pattern {
                return Err("host source, payload or guard mismatch".into());
            }
            g.read(0, &mut actual)?;
            if actual != expected {
                return Err("GPU source, payload or guard mismatch".into());
            }
            for _ in 0..5 {
                trial(&workers[id], dir, &rows, backends[id])?;
            }
        }
        for sample in 0..samples {
            let order = if sample % 4 < 2 { [0, 1] } else { [1, 0] };
            for id in order {
                times[id].push(trial(&workers[id], dir, &rows, backends[id])?);
            }
        }
        println!(
            "{}",
            json!({"case":"custom-fragments","direction":if h2d{"h2d"}else{"d2h"},"cuda_device_uuid":uuid,"descriptors":blocks,"sizes":vec![width;blocks],"samples":samples,"warmup":5,"bytes":blocks*width,"byte_source_guard_oracle":"passed","kernel_submit_to_drain_ms":times[0],"dma_submit_to_drain_ms":times[1]})
        );
    }
    for w in workers {
        w.close(Duration::from_secs(30))?;
    }
    Ok(())
}
