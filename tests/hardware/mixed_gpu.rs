//! Packaged two-process GPU Direct/staging gate, driven by tests/hardware/run.py.
use orbite::cuda::CudaMemory;
use orbite::transport::rdma::{RdmaConfig, RdmaCudaMemory, RdmaHostMemory};
use orbite::{
    EngineConfig, Notification, NotificationStatus, Permission, RegisteredMemory, RemoteSegment,
    SourceEpoch, TaskState, TransferEngine, TransferOp, TransferRequest, Transport,
};
use std::error::Error;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const WAIT: Duration = Duration::from_secs(20);
const LEN: usize = 128;
// Direct ranges abut in address order. The staged pair abuts in public order.
const SPANS: [(usize, usize, usize); 4] = [(2, 4, 5), (20, 23, 6), (26, 29, 7), (7, 9, 9)];

fn image(seed: usize) -> [u8; LEN] {
    std::array::from_fn(|i| ((37 * i + seed) % 251) as u8)
}

fn check(view: &RegisteredMemory, expected: &[u8; LEN]) -> Result<()> {
    let mut got = [0; LEN];
    view.read(0, &mut got)?;
    if got != *expected {
        return Err("GPU payload or guards changed".into());
    }
    Ok(())
}

fn source(engine: &TransferEngine, view: &RegisteredMemory, leg: &str) -> Result<()> {
    println!(
        "ORBITE_READY {} {}",
        engine.endpoint(),
        engine.source_epoch()
    );
    std::io::stdout().flush()?;
    let until = Instant::now() + WAIT + WAIT;
    let mut phases = Vec::new();
    while phases.len() < 2 {
        for note in engine.receive_notifications()? {
            if note.message != leg
                || Some(&*note.name) != ["read", "write"].get(phases.len()).copied()
            {
                return Err("unexpected phase notification".into());
            }
            phases.push(note.name);
        }
        if Instant::now() >= until {
            return Err("mixed batch notifications timed out".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut expected = image(3);
    let writes = image(121);
    for (l, r, n) in SPANS {
        expected[r..r + n].copy_from_slice(&writes[l..l + n]);
    }
    check(view, &expected)
}

fn requester(
    engine: &Arc<TransferEngine>,
    view: &RegisteredMemory,
    peer: &RemoteSegment,
    leg: &str,
) -> Result<()> {
    let [region] = peer.regions() else {
        return Err("expected one public GPU registration".into());
    };
    let requests = SPANS
        .iter()
        .enumerate()
        .map(|(i, &(l, r, n))| {
            let middle = i == 1 || i == 2;
            let transport = if middle && leg != "rdma-pin" {
                Transport::Tcp
            } else {
                Transport::Rdma
            };
            TransferRequest::new(
                transport,
                view.base_address() + l as u64,
                region.base_address() + r as u64,
                n,
            )
            .with_remote_staging(middle && leg != "tcp-local")
        })
        .collect::<Vec<_>>();
    let mut expected = image(77);
    for (op, name) in [(TransferOp::Read, "read"), (TransferOp::Write, "write")] {
        if op == TransferOp::Write {
            expected = image(121);
            view.write(0, &expected)?;
        }
        let note = Notification {
            name: name.into(),
            message: leg.into(),
        };
        let mut batch = engine.submit_async(op, peer, &requests, Some(&note), WAIT)?;
        let done = batch.wait(WAIT)?;
        if !done.locally_drained || done.completed_tasks != 4 || done.transferred_bytes != 27 {
            return Err(format!("mixed batch failed: {done:?}").into());
        }
        for (i, owner) in [0, 1, 1, 3].into_iter().enumerate() {
            let task = batch.task_status(i)?;
            if task.state != TaskState::Completed
                || !task.locally_drained
                || task.physical_owner != owner
                || task.transferred_bytes != SPANS[i].2
                || task.transport != requests[i].transport
                || task.failover_count != 0
            {
                return Err(format!("unexpected task receipt: {task:?}").into());
            }
        }
        let reports = batch.submissions()?;
        let [receipt] = &reports[..] else {
            return Err("expected one submission receipt".into());
        };
        if receipt.completed_requests != 4
            || receipt.transferred_bytes != 27
            || receipt.failure.is_some()
            || !matches!(*receipt.notification, NotificationStatus::Delivered)
        {
            return Err("incomplete submission or notification".into());
        }
        if op == TransferOp::Read {
            let remote = image(3);
            for (l, r, n) in SPANS {
                expected[l..l + n].copy_from_slice(&remote[r..r + n]);
            }
        }
        check(view, &expected)?;
        batch.free()?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--protocol"] {
        println!("orbite-installed-mixed-gpu");
        return Ok(());
    }
    let [role, bind, device, gid, leg, peer @ ..] = &args[..] else {
        return Err("expected ROLE BIND RNIC GID STAGED_LEG [PEER EPOCH]".into());
    };
    if !matches!(leg.as_str(), "tcp-local" | "tcp-pin" | "rdma-pin") {
        return Err("unknown staged leg".into());
    }
    let mut rdma = RdmaConfig::new(device);
    rdma.gid_index = Some(gid.parse()?);
    rdma.cuda_memory = RdmaCudaMemory::Direct;
    rdma.host_memory = RdmaHostMemory::Direct;
    rdma.gpu_source_reuse = false;
    let engine = Arc::new(TransferEngine::new(EngineConfig {
        bind_addr: bind.parse()?,
        rdma: Some(rdma),
        remote_staging: role == "source",
        max_failover_attempts: 0,
        staging_chunk_count: 2,
        staging_chunk_size: 64,
        ..Default::default()
    })?);
    let gpu = Arc::new(CudaMemory::allocate(0, LEN)?);
    let permission = if role == "source" {
        Permission::RemoteReadWrite
    } else {
        Permission::LocalOnly
    };
    let view = engine
        .register_cuda_memory_batch(&[(gpu, 0, LEN)], permission)?
        .remove(0);
    match (role.as_str(), peer) {
        ("source", []) => {
            view.write(0, &image(3))?;
            source(&engine, &view, leg)?;
        }
        ("requester", [endpoint, epoch]) => {
            view.write(0, &image(77))?;
            let peer = engine.open_segment(
                endpoint.parse::<SocketAddr>()?,
                epoch.parse::<SourceEpoch>()?,
                WAIT,
            )?;
            requester(&engine, &view, &peer, leg)?;
        }
        _ => return Err("invalid role or peer identity".into()),
    }
    engine.unregister_memory(&view)?;
    engine.close(WAIT)?;
    let stats = engine.stats()?;
    let rdma = stats.rdma.ok_or("missing native RDMA counters")?;
    let stage = stats.staging;
    let direct_bytes = if leg == "rdma-pin" { 27 } else { 14 };
    let pins = if role == "source" && leg != "tcp-local" {
        2
    } else {
        0
    };
    if role == "requester"
        && (rdma.completed_read_bytes, rdma.completed_write_bytes) != (direct_bytes, direct_bytes)
        || role == "source" && (rdma.opened_grant_pds, rdma.closed_grant_pds) != (4, 4)
        || (stage.remote_pins, stage.remote_unpins) != (pins, pins)
        || stage.copied_bytes != 26
        || stage.active_chunks != 0
        || stage.retained_chunks != 0
        || rdma.retained_work_requests != 0
        || rdma.active_grants != 0
        || rdma.retained_grants != 0
        || stats.inflight_bytes != 0
    {
        return Err(format!("unexpected route or retained owners: {rdma:?} {stage:?}").into());
    }
    std::fs::write(
        std::env::var("ORBITE_HARDWARE_MAPS")?,
        std::fs::read("/proc/self/maps")?,
    )?;
    println!(
        "ORBITE_MIXED_OK {role} {leg} rdma_read={} rdma_write={} private_pds={} staged_bytes={} pins={}",
        rdma.completed_read_bytes,
        rdma.completed_write_bytes,
        rdma.closed_grant_pds,
        stage.copied_bytes,
        stage.remote_pins
    );
    Ok(())
}
