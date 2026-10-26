//! Installed consumer resources: context selection, registration and drain.

use std::env;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use orbit_te::transport::rdma::{RdmaConfig, RdmaProgressMode, RdmaStats};
use orbit_te::{EngineConfig, Permission, RegisteredMemory, TransferEngine};

use super::{Case, Result, TIMEOUT};

pub(super) fn idle(engine: &TransferEngine) -> Result<RdmaStats> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let stats = engine.stats()?;
        let rdma = stats.rdma.ok_or("missing RDMA counters")?;
        if stats.active_source_operations == 0
            && stats.inflight_batches == 0
            && stats.requester_workers == 0
            && rdma.active_grants == 0
            && rdma.grant_credits == 0
            && rdma.receive_credits == 0
            && rdma.work_requests == 0
            && rdma.receive_work_requests == 0
            && rdma.active_message_mrs == 0
            && rdma.active_windows == 0
        {
            return Ok(rdma);
        }
        if Instant::now() >= deadline {
            return Err(format!("owners did not become idle: {stats:?}").into());
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn clean(engine: &TransferEngine) -> Result<()> {
    let stats = engine.stats()?;
    let rdma = stats.rdma.ok_or("missing RDMA counters")?;
    if [
        stats.allocated_batches,
        stats.allocated_requests,
        stats.requester_workers,
        stats.retained_regions,
        stats.retained_bytes,
        stats.active_source_operations,
        stats.inflight_batches,
        stats.inflight_bytes,
        stats.retained_notifications,
        rdma.active_devices,
        rdma.retained_devices,
        rdma.active_cqs,
        rdma.retained_cqs,
        rdma.active_endpoints,
        rdma.idle_endpoints,
        rdma.retained_endpoints,
        rdma.active_grants,
        rdma.grant_credits,
        rdma.retained_grants,
        rdma.persistent_mrs,
        rdma.persistent_mr_bytes,
        rdma.retained_persistent_mrs,
        rdma.persistent_source_mrs,
        rdma.retained_source_mrs,
        rdma.window_key_indices,
        rdma.active_windows,
        rdma.retained_windows,
        rdma.work_requests,
        rdma.retained_work_requests,
        rdma.receive_credits,
        rdma.receive_work_requests,
        rdma.retained_receive_work_requests,
        rdma.active_message_mrs,
        rdma.retained_message_mrs,
        rdma.message_bytes,
        rdma.retained_message_bytes,
        rdma.active_completion_channels,
        rdma.retained_completion_channels,
        rdma.progress_workers,
        rdma.progress_lanes,
        rdma.monitor_workers,
        rdma.async_event_sources,
        rdma.completion_event_sources,
        rdma.direct_bytes,
        rdma.staging_bytes,
        rdma.retained_bytes,
    ]
    .iter()
    .any(|n| *n != 0)
        || rdma.opened_connections != rdma.closed_connections
        || rdma.progress_failures != 0
        || rdma.completion_errors != 0
    {
        return Err(format!("close retained resources: {stats:?}").into());
    }
    Ok(())
}

pub(super) fn create(case: &Case, bind: SocketAddr) -> Result<(TransferEngine, RegisteredMemory)> {
    let engine = TransferEngine::new(EngineConfig {
        bind_addr: bind,
        max_registered_bytes: case.length(),
        max_transfer_bytes: case.payload,
        max_inflight_bytes: case.parents * case.payload,
        handler_threads: case.parents + 2,
        handler_timeout: if env::var("ORBITTE_HOST_MODE").as_deref() == Ok("installed-interruption")
        {
            Duration::from_secs(8)
        } else {
            TIMEOUT
        },
        rdma: Some(RdmaConfig {
            rdma_whitelist: case.rails.clone(),
            port: Some(1),
            gid_index: Some(env::var("ORBITTE_SHARED_GID")?.parse()?),
            device_mask: case.mask,
            hop_limit: 64,
            num_lanes: case.lanes,
            block_size: case.block,
            max_endpoints: 2 * case.lanes,
            max_grants: 2048,
            max_cqes: 2048,
            max_send_wr: 8,
            max_direct_bytes: case.parents * case.payload,
            max_cached_mrs: 4,
            max_cached_mr_bytes: 4 * case.length(),
            progress_mode: if case.events {
                RdmaProgressMode::EventDriven
            } else {
                RdmaProgressMode::Polling
            },
            ..Default::default()
        }),
        ..Default::default()
    })?;
    let memory = engine.register_memory(
        vec![0; case.length()].into_boxed_slice(),
        Permission::RemoteReadWrite,
    )?;
    let topology = engine.topology()?.ok_or("missing topology")?;
    if topology.nic_list.len() != 2
        || topology.nic_list[0].pci_bus_id == topology.nic_list[1].pci_bus_id
    {
        return Err("two distinct physical RNIC contexts are required".into());
    }
    for nic in &topology.nic_list {
        if !case.rails.contains(&nic.name) || !nic.device_attrs.contains_key("rdma.selected") {
            return Err("selected topology escaped the locked RNIC list".into());
        }
        println!(
            "ORBITTE_INSTALLED_NIC {{\"name\":{:?},\"pci\":{:?},\"gid\":{:?},\"gid_type\":{:?}}}",
            nic.name,
            nic.pci_bus_id.as_deref().ok_or("missing PCI identity")?,
            nic.device_attrs.get("rdma.gid").ok_or("missing GID")?,
            nic.device_attrs
                .get("rdma.gid_type")
                .ok_or("missing GID type")?
        );
    }
    Ok((engine, memory))
}

pub(super) fn maps() -> Result<()> {
    std::fs::write(
        env::var_os("ORBITTE_SHARED_MAPS").ok_or("missing maps path")?,
        std::fs::read("/proc/self/maps")?,
    )?;
    Ok(())
}

pub(super) fn close(engine: &TransferEngine, memory: RegisteredMemory) -> Result<()> {
    engine.close(TIMEOUT)?;
    engine.unregister_memory(&memory)?;
    drop(memory);
    clean(engine)
}
