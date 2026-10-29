//! Explicitly invoked byte/lifetime runner; never part of automatic device tests.

use std::env;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use orbit_te::cuda::{self, CudaMemory};
use orbit_te::transport::rdma::{RdmaConfig, RdmaCudaMemory, RdmaHostMemory, RdmaProgressMode};
use orbit_te::{
    BatchState, EngineConfig, Error, HostMemory, Notification, NotificationStatus, Permission,
    RegisteredMemory, Result, SourceEpoch, TransferEngine, TransferOp, TransferRequest, Transport,
};

const GUARD: usize = 64;
const TIMEOUT: Duration = Duration::from_secs(15);

enum Memory {
    Host(Arc<HostMemory>),
    Cuda(Arc<CudaMemory>),
}
impl Memory {
    fn new(kind: &str, bytes: &[u8]) -> Result<Self> {
        if kind == "cpu" {
            return Ok(Self::Host(Arc::new(HostMemory::new(
                bytes.to_vec().into_boxed_slice(),
            )?)));
        }
        let (kind, device) = kind
            .split_once(':')
            .ok_or(Error::InvalidConfig("memory: cpu, cuda:N or pinned:N"))?;
        let device = device
            .parse()
            .map_err(|_| Error::InvalidConfig("CUDA ordinal"))?;
        match kind {
            "cuda" => {
                let memory = Arc::new(CudaMemory::allocate(device, bytes.len())?);
                memory.write(0, bytes)?;
                Ok(Self::Cuda(memory))
            }
            "pinned" => {
                let memory = cuda::allocate_pinned_host(device, bytes.len())?;
                memory.write(0, bytes)?;
                Ok(Self::Host(memory))
            }
            _ => Err(Error::InvalidConfig("unknown memory kind")),
        }
    }
    fn register(&self, engine: &TransferEngine) -> Result<RegisteredMemory> {
        match self {
            Self::Host(memory) => engine.register_host_memory(
                memory.clone(),
                0,
                memory.len(),
                Permission::RemoteReadWrite,
            ),
            Self::Cuda(memory) => engine
                .register_cuda_memory_batch(
                    &[(memory.clone(), 0, memory.len())],
                    Permission::RemoteReadWrite,
                )?
                .pop()
                .ok_or(Error::Internal("CUDA registration is empty")),
        }
    }
}
fn pattern(seed: u64, index: usize) -> u8 {
    let mixed = (index as u64)
        .wrapping_mul(0x9e3779b97f4a7c15)
        .wrapping_add(seed)
        .rotate_left((index % 63) as u32);
    (mixed ^ (mixed >> 17) ^ (mixed >> 41)) as u8
}
fn initial(seed: u64, payload: usize) -> Vec<u8> {
    let mut bytes = vec![0xa5; payload + 2 * GUARD];
    for (index, byte) in bytes[GUARD..GUARD + payload].iter_mut().enumerate() {
        *byte = pattern(seed, index);
    }
    bytes
}
fn slices(payload: usize) -> Vec<(usize, usize, usize)> {
    let quarter = payload / 4;
    let length = env::var("ORBITTE_RDMA_SLICE_BYTES")
        .ok()
        .map(|value| value.parse::<usize>().expect("slice length"));
    (0..3)
        .map(|i| {
            (
                GUARD + i * quarter + 5,
                GUARD + i * quarter + 11,
                length.unwrap_or(quarter - 23),
            )
        })
        .collect()
}
fn verify(memory: &RegisteredMemory, expected: &[u8]) -> Result<()> {
    let mut actual = vec![0; expected.len()];
    memory.read(0, &mut actual)?;
    if let Some(index) = actual.iter().zip(expected).position(|(a, b)| a != b) {
        eprintln!(
            "byte mismatch at {index}: actual={} expected={}",
            actual[index], expected[index]
        );
        return Err(Error::Internal("full buffer/guard comparison failed"));
    }
    Ok(())
}
fn finish(
    engine: Arc<TransferEngine>,
    memory: RegisteredMemory,
    backing: Memory,
    role: &str,
    transport: Transport,
    payload: usize,
    bytes: usize,
) -> Result<()> {
    if let Some(path) = std::env::var_os("ORBITTE_HARDWARE_MAPS") {
        std::fs::write(path, std::fs::read_to_string("/proc/self/maps")?)?;
    }
    let gpu_direct = transport == Transport::Rdma
        && matches!(&backing, Memory::Cuda(_))
        && env::var("ORBITTE_RDMA_CUDA_MEMORY").as_deref() == Ok("direct");
    let host_direct = transport == Transport::Rdma
        && matches!(&backing, Memory::Host(_))
        && env::var("ORBITTE_RDMA_HOST_MEMORY").as_deref() != Ok("staged");
    let remote_staging = env::var("ORBITTE_REMOTE_STAGING").as_deref() == Ok("1");
    let remote_rdma = remote_staging && transport == Transport::Rdma;
    let expect_direct = host_direct || gpu_direct || remote_rdma;
    let contexts = env::var("ORBITTE_RDMA_RAILS").map_or(1, |v| v.split(',').count());
    let grants = env::var("ORBITTE_EXPECT_GRANTS")
        .map_or(Ok(6u64), |v| v.parse())
        .map_err(|_| Error::InvalidConfig("expected grant count"))?;
    let persistent = contexts * (2 * usize::from(host_direct) + usize::from(gpu_direct));
    let source_cached = host_direct;
    let remote_depth: u64 = if remote_staging && role == "source" {
        env::var("ORBITTE_EXPECT_STAGE_DEPTH")
            .map_or(Ok(1), |s| s.parse())
            .map_err(|_| Error::InvalidConfig("expected staging depth"))?
    } else {
        1
    };
    let private_mrs = if remote_rdma { 6 * remote_depth } else { 0 };
    let reused_mrs = if remote_rdma {
        grants - private_mrs
    } else if host_direct || (gpu_direct && role == "requester") {
        grants
    } else {
        0
    };
    let opened_source_mrs = contexts as u64 * u64::from(source_cached)
        + if remote_rdma && role == "source" {
            private_mrs
        } else {
            0
        };
    let reused_source_mrs = if remote_rdma && role == "source" {
        grants - private_mrs
    } else if source_cached && role == "source" {
        grants
    } else {
        0
    };
    let idle = engine
        .stats()?
        .rdma
        .ok_or(Error::Internal("missing RDMA counters"))?;
    let receives = if role == "source" && transport == Transport::Rdma {
        grants
    } else {
        0
    };
    let windows = if gpu_direct { 0 } else { receives };
    let private_pds = if gpu_direct { receives } else { 0 };
    if idle.active_grant_pds != 0
        || idle.opened_grant_pds != private_pds
        || idle.closed_grant_pds != private_pds
    {
        return Err(Error::Internal("private GPU grant PD owners did not drain"));
    }
    let expected_sends = if role == "requester" && transport == Transport::Rdma {
        grants
    } else {
        0
    };
    if idle.completed_sends != expected_sends
        || idle.completed_receives != receives
        || idle.receive_post_calls != receives
        || idle.receive_work_requests != 0
        || idle.retained_receive_work_requests != 0
        || idle.active_message_mrs != 0
        || idle.retained_message_mrs != 0
        || idle.message_bytes != 0
        || idle.retained_message_bytes != 0
    {
        return Err(Error::Internal(
            "source receive/message owners did not drain",
        ));
    }
    let reused = env::var("ORBITTE_EXPECT_QP_REUSE").as_deref() == Ok("1");
    let idle_qps = if reused {
        env::var("ORBITTE_RDMA_MAX_ENDPOINTS").map_or(2 * contexts, |s| {
            s.parse::<usize>().unwrap_or(0).min(2 * contexts)
        })
    } else {
        0
    };
    let opened_qps = if transport == Transport::Rdma {
        if reused {
            (2 * contexts) as u64
        } else {
            grants
        }
    } else {
        0
    };
    if idle.opened_connections != opened_qps
        || idle.closed_connections != opened_qps - idle_qps as u64
        || idle.idle_endpoints != idle_qps
        || idle.reused_connections != if reused { grants - opened_qps } else { 0 }
        || idle.completed_reusable_grants != if reused { grants } else { 0 }
    {
        return Err(Error::Internal("actual QP reuse counters differ"));
    }
    if idle.active_grants != 0 || idle.retained_grants != 0 || idle.active_endpoints != idle_qps {
        return Err(Error::Internal("connection/grant owners did not drain"));
    }
    if idle.active_windows != idle.exhausted_windows
        || idle.exhausted_windows as u64 != idle.window_key_rollovers
        || idle.retained_windows != 0
        || idle.opened_windows != windows + idle.window_key_rollovers
        || idle.completed_window_binds != windows
        || idle.completed_window_invalidations != windows
        || idle.burned_window_keys != windows
        || idle.window_key_exhaustions != 0
        || (windows == 0 && idle.window_key_indices != 0)
        || (windows != 0 && !(1..=windows as usize).contains(&idle.window_key_indices))
    {
        return Err(Error::Internal("source MW lifecycle counters differ"));
    }
    if idle.active_cqs != 2 * contexts * usize::from(transport == Transport::Rdma)
        || idle.opened_cqs != 2 * contexts as u64 * u64::from(transport == Transport::Rdma)
        || idle.progress_workers != usize::from(transport == Transport::Rdma)
        || idle.async_event_sources != contexts * usize::from(transport == Transport::Rdma)
        || idle.progress_failures != 0
        || idle.persistent_mrs != persistent
        || idle.persistent_mr_bytes != persistent * memory.len()
        || idle.opened_persistent_mrs != persistent as u64 + private_mrs
        || idle.reused_persistent_mrs != reused_mrs
        || idle.evicted_persistent_mrs != 0
        || idle.retained_persistent_mrs != 0
        || idle.persistent_source_mrs != contexts * usize::from(source_cached)
        || idle.opened_source_mrs != opened_source_mrs
        || idle.reused_source_mrs != reused_source_mrs
        || idle.retained_source_mrs != 0
    {
        return Err(Error::Internal("persistent MR reuse counters differ"));
    }
    if transport == Transport::Rdma {
        let topology = engine
            .topology()?
            .ok_or(Error::Internal("missing RDMA topology"))?;
        let nic = topology
            .nic_list
            .iter()
            .find(|nic| nic.device_attrs.contains_key("rdma.selected"))
            .ok_or(Error::Internal("missing selected RDMA NIC"))?;
        let attrs = &nic.device_attrs;
        println!(
            "ORBITTE_TOPOLOGY {{\"device\":\"{}\",\"port\":{},\"gid_index\":{},\"gid\":\"{}\",\"gid_type\":\"{}\",\"net_device_index\":{}}}",
            nic.name,
            attrs["rdma.port"],
            attrs["rdma.gid_index"],
            attrs["rdma.gid"],
            attrs["rdma.gid_type"],
            attrs["rdma.net_device_index"]
        );
    }
    engine.close_requester(TIMEOUT)?;
    engine.close_source(TIMEOUT)?;
    let staging = engine.stats()?.staging;
    if remote_staging {
        let chunk: usize = env::var("ORBITTE_EXPECT_STAGE_CHUNK_SIZE")
            .map_err(|_| Error::InvalidConfig("remote staging chunk size"))?
            .parse()
            .map_err(|_| Error::InvalidConfig("remote staging chunk size"))?;
        let length = slices(payload)[0].2;
        let steps = 6 * length.div_ceil(chunk) as u64;
        let expected = if role == "source" {
            (6, steps, steps, 6)
        } else {
            (0, 0, 0, 0)
        };
        if (
            staging.remote_pins,
            staging.remote_delegates,
            staging.remote_crosses,
            staging.remote_unpins,
        ) != expected
        {
            return Err(Error::Internal("remote staging operation receipts differ"));
        }
        println!(
            "ORBITTE_REMOTE_STAGE_STATS {{\"pins\":{},\"delegates\":{},\"crosses\":{},\"unpins\":{}}}",
            staging.remote_pins,
            staging.remote_delegates,
            staging.remote_crosses,
            staging.remote_unpins
        );
    }
    if env::var("ORBITTE_STAGING_OBSERVE").as_deref() == Ok("1") {
        let expected = u64::from(matches!(&backing, Memory::Cuda(_)) && !gpu_direct);
        let chunk: usize = env::var("ORBITTE_STAGING_CHUNK_SIZE")
            .unwrap()
            .parse()
            .unwrap();
        let transfer_chunk = if remote_staging {
            env::var("ORBITTE_EXPECT_STAGE_CHUNK_SIZE")
                .map_err(|_| Error::InvalidConfig("expected staging chunk"))?
                .parse::<usize>()
                .map_err(|_| Error::InvalidConfig("expected staging chunk"))?
        } else {
            chunk
        };
        let chunks: usize = slices(payload)
            .iter()
            .map(|(_, _, length)| length.div_ceil(transfer_chunk))
            .sum();
        let slots = expected * remote_depth;
        if staging.allocations != slots
            || staging.reuses != 5 * slots
            || staging.copied_bytes != bytes as u64 * expected
            || staging.completed_chunks != (chunks * 2) as u64 * expected
            || staging.idle_chunks != slots as usize
            || staging.active_chunks != 0
            || staging.retained_chunks != 0
            || staging.reserved_bytes != chunk * slots as usize
        {
            return Err(Error::Internal(
                "staging chunk ownership or byte accounting",
            ));
        }
        println!(
            "ORBITTE_STAGING_STATS {{\"allocations\":{},\"reuses\":{},\"chunks\":{},\"bytes\":{},\"reserved_bytes\":{}}}",
            staging.allocations,
            staging.reuses,
            staging.completed_chunks,
            staging.copied_bytes,
            staging.reserved_bytes
        );
    }
    engine.close(TIMEOUT)?;
    if engine.stats()?.staging.reserved_chunks != 0 {
        return Err(Error::Internal("staging pool did not close"));
    }
    engine.unregister_memory(&memory)?;
    drop(memory);
    drop(backing);
    let stats = engine.stats()?;
    let rdma = stats.rdma.ok_or(Error::Internal("missing RDMA counters"))?;
    if stats.retained_regions != 0
        || stats.inflight_batches != 0
        || stats.allocated_batches != 0
        || stats.requester_workers != 0
        || stats.active_source_operations != 0
        || rdma.active_windows != 0
        || rdma.exhausted_windows != 0
        || rdma.retained_windows != 0
        || rdma.active_completion_channels != 0
        || rdma.retained_completion_channels != 0
        || rdma.completion_event_sources != 0
        || rdma.active_cqs != 0
        || rdma.retained_cqs != 0
        || rdma.progress_workers != 0
        || rdma.async_event_sources != 0
        || rdma.progress_failures != 0
        || rdma.active_devices != 0
        || rdma.window_key_indices != 0
        || rdma.retained_devices != 0
        || rdma.persistent_mrs != 0
        || rdma.persistent_mr_bytes != 0
        || rdma.retained_persistent_mrs != 0
        || rdma.persistent_source_mrs != 0
        || rdma.retained_source_mrs != 0
        || rdma.active_grant_pds != 0
        || rdma.opened_grant_pds != rdma.closed_grant_pds
        || rdma.active_endpoints != 0
        || rdma.idle_endpoints != 0
        || rdma.opened_connections != rdma.closed_connections
        || rdma.active_grants != 0
        || rdma.retained_grants != 0
        || rdma.staging_bytes != 0
        || rdma.direct_bytes != 0
        || rdma.work_requests != 0
        || rdma.retained_work_requests != 0
        || rdma.partial_posts != 0
        || rdma.uncertain_posts != 0
        || rdma.completion_errors != 0
        || rdma.opened_direct_endpoints != if expect_direct { grants } else { 0 }
        || rdma.retained_endpoints != 0
        || rdma.retained_bytes != 0
        || rdma.opened_devices != contexts as u64 * u64::from(transport == Transport::Rdma)
    {
        return Err(Error::Internal("resource counters did not drain"));
    }
    if rdma.receive_work_requests != 0
        || rdma.retained_receive_work_requests != 0
        || rdma.active_message_mrs != 0
        || rdma.retained_message_mrs != 0
        || rdma.message_bytes != 0
        || rdma.retained_message_bytes != 0
    {
        return Err(Error::Internal("receive/message resources were retained"));
    }
    println!(
        "ORBITTE_GPU_MR_STATS {{\"direct\":{},\"opened_private_pds\":{},\"closed_private_pds\":{},\"active_private_pds\":{}}}",
        gpu_direct, rdma.opened_grant_pds, rdma.closed_grant_pds, rdma.active_grant_pds
    );
    println!(
        "ORBITTE_RECEIVE_STATS {{\"completed_sends\":{},\"completed_receives\":{},\"receive_post_calls\":{},\"peak_receive_work_requests\":{},\"receive_work_requests\":{},\"retained_receive_work_requests\":{},\"message_mrs\":{},\"retained_message_mrs\":{},\"message_bytes\":{},\"retained_message_bytes\":{}}}",
        rdma.completed_sends,
        rdma.completed_receives,
        rdma.receive_post_calls,
        rdma.peak_receive_work_requests,
        rdma.receive_work_requests,
        rdma.retained_receive_work_requests,
        rdma.active_message_mrs,
        rdma.retained_message_mrs,
        rdma.message_bytes,
        rdma.retained_message_bytes
    );
    let retained = cuda::retained_resources();
    if retained.device_allocations != 0
        || retained.pinned_allocations != 0
        || retained.contexts != 0
        || retained.streams != 0
        || retained.events != 0
    {
        return Err(Error::Internal("CUDA resources were retained"));
    }
    println!(
        "ORBITTE_SOURCE_MR_STATS {{\"idle_parents\":{},\"parents\":{},\"opened\":{},\"reused\":{},\"retained\":{}}}",
        idle.persistent_source_mrs,
        rdma.persistent_source_mrs,
        rdma.opened_source_mrs,
        rdma.reused_source_mrs,
        rdma.retained_source_mrs
    );
    println!(
        "ORBITTE_ASYNC_STATS {{\"idle_sources\":{},\"sources\":{},\"acknowledged_events\":{}}}",
        idle.async_event_sources, rdma.async_event_sources, rdma.acknowledged_async_events
    );
    println!(
        "ORBITTE_OWNER_STATS {{\"idle_connections\":{},\"idle_grants\":{},\"connections\":{},\"grants\":{},\"retained_connections\":{},\"retained_grants\":{}}}",
        idle.active_endpoints,
        idle.active_grants,
        rdma.active_endpoints,
        rdma.active_grants,
        rdma.retained_endpoints,
        rdma.retained_grants
    );
    println!(
        "ORBITTE_CONNECTION_STATS {{\"idle\":{},\"opened\":{},\"closed\":{},\"reused\":{},\"completed_grants\":{}}}",
        idle.idle_endpoints,
        rdma.opened_connections,
        rdma.closed_connections,
        rdma.reused_connections,
        rdma.completed_reusable_grants
    );
    println!(
        "ORBITTE_KEY_STATS {{\"idle_indices\":{},\"indices\":{},\"burned_keys\":{},\"exhaustions\":{},\"rollovers\":{},\"idle_exhausted_windows\":{},\"exhausted_windows\":{}}}",
        idle.window_key_indices,
        rdma.window_key_indices,
        rdma.burned_window_keys,
        rdma.window_key_exhaustions,
        rdma.window_key_rollovers,
        idle.exhausted_windows,
        rdma.exhausted_windows
    );
    println!(
        "ORBITTE_WINDOW_STATS {{\"opened_windows\":{},\"completed_binds\":{},\"completed_invalidations\":{},\"active_windows\":0,\"retained_windows\":0}}",
        rdma.opened_windows, rdma.completed_window_binds, rdma.completed_window_invalidations
    );
    let event_mode =
        transport == Transport::Rdma && env::var("ORBITTE_RDMA_EVENTS").as_deref() == Ok("1");
    if idle.active_completion_channels != contexts * usize::from(event_mode)
        || idle.completion_event_sources != contexts * usize::from(event_mode)
        || rdma.opened_completion_channels != contexts as u64 * u64::from(event_mode)
        || (event_mode && (rdma.completion_events == 0 || rdma.cq_notification_arms < 2))
    {
        return Err(Error::Internal(
            "completion event mode/owner proof mismatch",
        ));
    }
    println!(
        "ORBITTE_CQ_EVENT_STATS {{\"opened_channels\":{},\"idle_sources\":{},\"events\":{},\"arms\":{},\"waits\":{},\"active_channels\":0,\"retained_channels\":0,\"sources\":0}}",
        rdma.opened_completion_channels,
        idle.completion_event_sources,
        rdma.completion_events,
        rdma.cq_notification_arms,
        rdma.cq_event_waits
    );
    println!(
        "ORBITTE_SHARED_STATS {{\"opened_cqs\":{},\"idle_progress_workers\":{},\"peak_pending_qps\":{},\"active_cqs\":0,\"progress_workers\":0,\"retained_cqs\":0,\"progress_failures\":0}}",
        rdma.opened_cqs, idle.progress_workers, rdma.peak_pending_qps
    );
    println!(
        "{{\"role\":\"{role}\",\"transport\":\"{transport:?}\",\"status\":\"passed\",\"payload_bytes\":{payload},\"completed_bytes\":{bytes},\"full_buffer_and_guards\":true,\"locally_drained\":true,\"registered_regions\":0,\"active_devices\":0,\"retained_devices\":0,\"persistent_mrs\":0,\"persistent_mr_bytes\":0,\"retained_persistent_mrs\":0,\"idle_persistent_mrs\":{},\"idle_persistent_mr_bytes\":{},\"opened_persistent_mrs\":{},\"reused_persistent_mrs\":{},\"evicted_persistent_mrs\":{},\"opened_rdma_devices\":{},\"active_endpoints\":0,\"staging_bytes\":0,\"direct_bytes\":0,\"opened_direct_endpoints\":{},\"completed_rdma_reads\":{},\"completed_rdma_writes\":{},\"completed_rdma_read_bytes\":{},\"completed_rdma_write_bytes\":{},\"work_requests\":0,\"retained_work_requests\":0,\"peak_work_requests\":{},\"posted_work_requests\":{},\"post_calls\":{},\"partial_posts\":0,\"uncertain_posts\":0,\"completion_errors\":0}}",
        idle.persistent_mrs,
        idle.persistent_mr_bytes,
        rdma.opened_persistent_mrs,
        rdma.reused_persistent_mrs,
        rdma.evicted_persistent_mrs,
        rdma.opened_devices,
        rdma.opened_direct_endpoints,
        rdma.completed_reads,
        rdma.completed_writes,
        rdma.completed_read_bytes,
        rdma.completed_write_bytes,
        rdma.peak_work_requests,
        rdma.posted_work_requests,
        rdma.post_calls
    );
    Ok(())
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().collect();
    if args.len() == 2 && args[1] == "--protocol" {
        println!("orbitte-hardware-peer-v1");
        return Ok(());
    }
    if args.len() != 9 && args.len() != 12 {
        return Err("usage: hardware_peer source|requester BIND RNIC GID cpu|cuda:N|pinned:N PAYLOAD SEED tcp|rdma [PEER EPOCH REMOTE_ADDRESS]".into());
    }
    let role = &args[1];
    let bind_addr: SocketAddr = args[2].parse()?;
    let mut rdma = if env::var("ORBITTE_RDMA_DISCOVERY").as_deref() == Ok("1") {
        RdmaConfig {
            rdma_whitelist: vec![args[3].clone()],
            ..Default::default()
        }
    } else {
        let mut config = RdmaConfig::new(&args[3]);
        config.gid_index = Some(args[4].parse()?);
        config
    };
    if let Ok(rails) = env::var("ORBITTE_RDMA_RAILS") {
        rdma.device = None;
        rdma.rdma_whitelist = rails.split(',').map(str::to_owned).collect();
    }
    rdma.hop_limit = 64;
    rdma.progress_mode = match env::var("ORBITTE_RDMA_EVENTS").as_deref() {
        Ok("1") => RdmaProgressMode::EventDriven,
        Ok("0") | Err(env::VarError::NotPresent) => RdmaProgressMode::Polling,
        _ => return Err("ORBITTE_RDMA_EVENTS must be 0 or 1".into()),
    };
    rdma.host_memory = match env::var("ORBITTE_RDMA_HOST_MEMORY").as_deref() {
        Ok("staged") => RdmaHostMemory::Staged,
        Ok("direct") | Err(env::VarError::NotPresent) => RdmaHostMemory::Direct,
        _ => return Err("ORBITTE_RDMA_HOST_MEMORY must be direct or staged".into()),
    };
    rdma.cuda_memory = match env::var("ORBITTE_RDMA_CUDA_MEMORY").as_deref() {
        Ok("direct") => RdmaCudaMemory::Direct,
        Ok("staged") | Err(env::VarError::NotPresent) => RdmaCudaMemory::Staged,
        _ => return Err("ORBITTE_RDMA_CUDA_MEMORY must be direct or staged".into()),
    };
    if let Ok(value) = env::var("ORBITTE_RDMA_BLOCK_SIZE") {
        rdma.block_size = value.parse()?;
    }
    if let Ok(value) = env::var("ORBITTE_RDMA_MAX_SEND_WR") {
        rdma.max_send_wr = value.parse()?;
    }
    if let Ok(value) = env::var("ORBITTE_RDMA_MAX_CQES") {
        rdma.max_cqes = value.parse()?;
    }
    if let Ok(value) = env::var("ORBITTE_RDMA_MAX_ENDPOINTS") {
        rdma.max_endpoints = value.parse()?;
    }
    let kind = &args[5];
    let payload: usize = args[6].parse()?;
    let seed: u64 = args[7].parse()?;
    if !(128..=32 << 20).contains(&payload) {
        return Err("payload must be 128..32MiB".into());
    }
    let transport = match args[8].as_str() {
        "tcp" => Transport::Tcp,
        "rdma" => Transport::Rdma,
        _ => return Err("transport must be tcp or rdma".into()),
    };
    if transport == Transport::Tcp {
        rdma.host_memory = RdmaHostMemory::Staged;
    }
    let config = EngineConfig {
        bind_addr,
        remote_staging: env::var("ORBITTE_REMOTE_STAGING").as_deref() == Ok("1"),
        handler_threads: if env::var("ORBITTE_REMOTE_STAGING").as_deref() == Ok("1") {
            1
        } else {
            4
        },
        max_inflight_batches: 1,
        staging_chunk_size: env::var("ORBITTE_STAGING_CHUNK_SIZE")
            .map_or(Ok(4 << 20), |s| s.parse())?,
        staging_chunk_count: env::var("ORBITTE_STAGING_CHUNK_COUNT")
            .map_or(Ok(64), |s| s.parse())?,
        staging_pipeline_depth: env::var("ORBITTE_STAGING_PIPELINE_DEPTH")
            .map_or(Ok(1), |s| s.parse())?,
        rdma: Some(rdma),
        handler_timeout: TIMEOUT,
        ..EngineConfig::default()
    };
    if env::var("ORBITTE_STAGING_OBSERVE").as_deref() == Ok("1") {
        println!(
            "ORBITTE_STAGING_CONFIG {{\"depth\":{},\"chunk_count\":{},\"chunk_size\":{}}}",
            config.staging_pipeline_depth, config.staging_chunk_count, config.staging_chunk_size
        );
    }
    let engine = Arc::new(TransferEngine::new(config)?);
    let source_bytes = initial(seed, payload);
    let initial_bytes = if role == "source" {
        source_bytes.clone()
    } else {
        vec![0xcc; source_bytes.len()]
    };
    let backing = Memory::new(kind, &initial_bytes)?;
    let memory = backing.register(&engine)?;
    println!(
        "ORBITTE_MEMORY {{\"base\":{},\"length\":{},\"cuda\":{}}}",
        memory.base_address(),
        memory.len(),
        matches!(&backing, Memory::Cuda(_))
    );
    let entries = slices(payload);
    if entries.iter().any(|(local, remote, length)| {
        *length == 0 || local + length > GUARD + payload || remote + length > GUARD + payload
    }) {
        return Err("explicit slices exceed payload".into());
    }
    let transferred: usize = entries.iter().map(|(_, _, length)| *length).sum();
    if role == "source" && args.len() == 9 {
        println!(
            "ORBITTE_READY {} {} {} {}",
            engine.endpoint(),
            engine.source_epoch(),
            memory.base_address(),
            memory.len()
        );
        io::stdout().flush()?;
        let deadline = Instant::now() + Duration::from_secs(45);
        let mut phases = Vec::new();
        while phases.len() < 2 {
            for notification in engine.receive_notifications()? {
                if notification.message != seed.to_string()
                    || !["read_done", "write_done"].contains(&notification.name.as_str())
                    || phases.contains(&notification.name)
                {
                    return Err("unexpected or duplicate phase notification".into());
                }
                phases.push(notification.name);
            }
            if Instant::now() >= deadline {
                return Err("requester completion deadline".into());
            }
            thread::sleep(Duration::from_millis(2));
        }
        if phases != ["read_done", "write_done"] {
            return Err("phase notifications are out of order".into());
        }
        let mut expected = source_bytes;
        for (local, remote, length) in &entries {
            for index in 0..*length {
                expected[remote + index] = pattern(seed ^ 0xa55a5aa5, local + index);
            }
        }
        verify(&memory, &expected)?;
    } else if role == "requester" && args.len() == 12 {
        let peer =
            engine.open_segment(args[9].parse()?, args[10].parse::<SourceEpoch>()?, TIMEOUT)?;
        let remote: u64 = args[11].parse()?;
        let requests: Vec<_> = entries
            .iter()
            .map(|(local, source, length)| TransferRequest {
                transport,
                local_address: memory.base_address() + *local as u64,
                remote_address: remote + *source as u64,
                length: *length,
            })
            .collect();
        let mut batch = engine.allocate_batch(requests.len())?;
        batch.submit(TransferOp::Read, &peer, &requests[..1], None, TIMEOUT)?;
        let first_deadline = Instant::now() + TIMEOUT;
        loop {
            let first = batch.submissions()?.remove(0);
            if first.failure.is_some() {
                return Err(format!("first READ submission failed: {:?}", first.failure).into());
            }
            if first.locally_drained {
                if first.completed_requests != 1 {
                    return Err("first READ submission did not complete".into());
                }
                break;
            }
            if Instant::now() >= first_deadline {
                return Err("first READ submission drain deadline".into());
            }
            thread::sleep(Duration::from_millis(1));
        }
        batch.submit(
            TransferOp::Read,
            &peer,
            &requests[1..],
            Some(&Notification {
                name: "read_done".into(),
                message: seed.to_string(),
            }),
            TIMEOUT,
        )?;
        batch.seal()?;
        let status = batch.wait(TIMEOUT)?;
        if status.state != BatchState::Completed
            || !status.locally_drained
            || status.transferred_bytes != transferred
        {
            return Err(format!(
                "READ batch result: {status:?}; submissions: {:?}",
                batch.submissions()?
            )
            .into());
        }
        for submission in batch.submissions()? {
            if submission.failure.is_some()
                || !matches!(
                    *submission.notification,
                    NotificationStatus::Delivered | NotificationStatus::NotRequested
                )
            {
                return Err("READ submission/notification failure".into());
            }
        }
        let mut expected = vec![0xcc; memory.len()];
        for (local, remote, length) in &entries {
            expected[*local..local + length]
                .copy_from_slice(&source_bytes[*remote..remote + length]);
        }
        verify(&memory, &expected)?;
        batch.free()?;
        for (local, _, length) in &entries {
            let bytes: Vec<_> = (0..*length)
                .map(|i| pattern(seed ^ 0xa55a5aa5, local + i))
                .collect();
            memory.write(*local, &bytes)?;
            expected[*local..local + length].copy_from_slice(&bytes);
        }
        let mut batch = engine.submit_async(
            TransferOp::Write,
            &peer,
            &requests,
            Some(&Notification {
                name: "write_done".into(),
                message: seed.to_string(),
            }),
            TIMEOUT,
        )?;
        let status = batch.wait(TIMEOUT)?;
        if status.state != BatchState::Completed
            || !status.locally_drained
            || status.transferred_bytes != transferred
            || !matches!(
                *batch.submissions()?[0].notification,
                NotificationStatus::Delivered
            )
        {
            return Err(format!(
                "WRITE batch/notification result: {status:?}; submissions: {:?}",
                batch.submissions()?
            )
            .into());
        }
        verify(&memory, &expected)?;
        batch.free()?;
        let rdma = engine.stats()?.rdma.ok_or("missing RDMA stats")?;
        if transport == Transport::Rdma
            && (rdma.completed_reads < 3
                || rdma.completed_writes < 3
                || rdma.completed_read_bytes != transferred as u64
                || rdma.completed_write_bytes != transferred as u64
                || rdma.posted_work_requests
                    != rdma.completed_reads + rdma.completed_writes + rdma.completed_sends
                || rdma.peak_work_requests == 0)
        {
            return Err("no actual RDMA completion counters".into());
        }
    } else {
        return Err("invalid role/argument count".into());
    }
    finish(
        engine,
        memory,
        backing,
        role,
        transport,
        payload,
        2 * transferred,
    )?;
    Ok(())
}
