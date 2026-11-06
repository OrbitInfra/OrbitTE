//! Installed consumer resources: context selection, registration and drain.

use std::env;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use orbite::transport::rdma::{RdmaConfig, RdmaProgressMode, RdmaStats};
use orbite::{EngineConfig, Permission, RegisteredMemory, TransferEngine};

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
        || rdma.progress_failures != rdma.progress_recoveries
        || (!matches!(
            env::var("ORBITE_HOST_MODE").as_deref(),
            Ok("installed-interruption"
                | "installed-lost-completion"
                | "installed-cleanup-failure"
                | "installed-inflight-dma"
                | "installed-memory-cleanup"
                | "installed-rail-health")
        ) && rdma.progress_failures != 0)
        || rdma.completion_errors
            > u64::from(env::var("ORBITE_HOST_MODE").as_deref() == Ok("installed-inflight-dma"))
    {
        return Err(format!("close retained resources: {stats:?}").into());
    }
    Ok(())
}

pub(super) fn engine(case: &Case, bind: SocketAddr) -> Result<TransferEngine> {
    let automatic = env::var("ORBITE_HOST_DISCOVERY").as_deref() == Ok("auto");
    let mut rail_topology = None;
    let mut priority_matrix = None;
    if env::var("ORBITE_HOST_MODE")? == "installed-rail-health" {
        rail_topology = Some(format!(
            r#"{{"all":[{{"local":"{0}","remote":"{0}"}},{{"local":"{1}","remote":"{1}"}}]}}"#,
            case.rails[0], case.rails[1]
        ));
    } else if case.selective {
        let index = usize::from(case.mask == 2);
        let (direct, other) = (&case.rails[index], &case.rails[1 - index]);
        if case.source {
            priority_matrix = Some(format!(r#"{{"*":[["{other}"],["{direct}"]]}}"#));
        } else {
            rail_topology = Some(format!(
                r#"{{"all":[{{"local":"{direct}","remote":"{other}"}},{{"local":"{direct}","remote":"{direct}"}}],"direct":[{{"local":"{direct}","remote":"{direct}"}}]}}"#
            ));
        }
    }
    let config = EngineConfig {
        selector: if env::var("ORBITE_HOST_MODE")?.as_str() == "installed-rail-health" {
            Some(super::rail_health::policy(case)?)
        } else {
            case.selector
                .then(|| super::selector::config(case))
                .transpose()?
        },
        runtime_queue: case.selector.then_some(orbite::RuntimeQueueConfig {
            max_outstanding_owners: 1,
            max_dispatch_owners: 1,
            deadline_aware: true,
            mlu_local_threshold: 1.0,
            ..Default::default()
        }),
        bind_addr: bind,
        max_registered_bytes: case.length(),
        max_transfer_bytes: case.payload,
        max_inflight_bytes: case.parents * case.payload,
        handler_threads: case.parents + 2,
        handler_timeout: if matches!(
            env::var("ORBITE_HOST_MODE").as_deref(),
            Ok("installed-interruption"
                | "installed-lost-completion"
                | "installed-cleanup-failure"
                | "installed-inflight-dma"
                | "installed-memory-cleanup")
        ) {
            Duration::from_secs(8)
        } else {
            TIMEOUT
        },
        rdma: Some(RdmaConfig {
            rail_cooldown: Duration::from_secs(2),
            rail_probe_interval: Duration::from_secs(1),
            rail_topology,
            priority_matrix,
            rdma_whitelist: case.rails.clone(),
            rdma_blacklist: if automatic {
                case.rails.clone()
            } else {
                Vec::new()
            },
            port: if automatic { None } else { Some(1) },
            gid_index: if automatic {
                None
            } else {
                Some(env::var("ORBITE_SHARED_GID")?.parse()?)
            },
            device_mask: if case.selective && case.source {
                3
            } else {
                case.mask
            },
            hop_limit: 64,
            num_lanes: case.lanes,
            block_size: case.block,
            max_endpoints: 2 * case.lanes,
            max_grants: 2048,
            max_cqes: 2048,
            max_send_wr: 8,
            signal_interval: if case.selective { 3 } else { 1 },
            max_rd_atomic: if case.selective { u8::MAX } else { 16 },
            max_dest_rd_atomic: if case.selective { u8::MAX } else { 16 },
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
    };
    if automatic {
        let mut pinned = config.clone();
        pinned
            .rdma
            .as_mut()
            .ok_or("missing RDMA configuration")?
            .gid_index = Some(255);
        let engine = TransferEngine::new(pinned)?;
        let registration =
            engine.register_memory(vec![0; 64].into_boxed_slice(), Permission::RemoteReadWrite);
        engine.close(TIMEOUT)?;
        match registration {
            Err(orbite::Error::Unavailable(_)) => {
                clean(&engine)?;
                println!(
                    "ORBITE_INSTALLED_DISCOVERY_PIN {{\"gid_index\":255,\"unavailable\":true}}"
                );
            }
            Err(error) => return Err(error.into()),
            Ok(memory) => {
                engine.unregister_memory(&memory)?;
                drop(memory);
                clean(&engine)?;
                return Err("absent strict GID unexpectedly fell back".into());
            }
        }
    }
    let engine = TransferEngine::new(config)?;
    Ok(engine)
}

pub(super) fn inventory(case: &Case, engine: &TransferEngine) -> Result<()> {
    let topology = engine.topology()?.ok_or("missing topology")?;
    if topology.nic_list.len() != 2
        || topology.nic_list[0].pci_bus_id == topology.nic_list[1].pci_bus_id
    {
        return Err("two distinct physical RNIC contexts are required".into());
    }
    let automatic = env::var("ORBITE_HOST_DISCOVERY").as_deref() == Ok("auto");
    if automatic {
        println!(
            "ORBITE_INSTALLED_DISCOVERY_CONFIG {{\"device\":null,\"port\":null,\"gid_index\":null,\"whitelist\":{:?},\"blacklist\":{:?}}}",
            case.rails, case.rails
        );
        for (id, nic) in topology.nic_list.iter().enumerate() {
            println!(
                "ORBITE_INSTALLED_TOPOLOGY_NIC {{\"id\":{id},\"name\":{:?},\"pci\":{:?},\"numa\":{},\"attrs\":{:?}}}",
                nic.name,
                nic.pci_bus_id.as_deref().ok_or("missing PCI identity")?,
                nic.numa_node
                    .map_or_else(|| "null".into(), |n| n.to_string()),
                nic.device_attrs
            );
        }
        for memory in &topology.mem_list {
            println!(
                "ORBITE_INSTALLED_TOPOLOGY_MEMORY {{\"name\":{:?},\"numa\":{},\"ranks\":{:?}}}",
                memory.name,
                memory
                    .numa_node
                    .map_or_else(|| "null".into(), |n| n.to_string()),
                memory.device_list
            );
        }
    }
    for nic in &topology.nic_list {
        if !case.rails.contains(&nic.name) || !nic.device_attrs.contains_key("rdma.selected") {
            return Err("selected topology escaped the locked RNIC list".into());
        }
        println!(
            "ORBITE_INSTALLED_NIC {{\"name\":{:?},\"pci\":{:?},\"gid\":{:?},\"gid_type\":{:?}}}",
            nic.name,
            nic.pci_bus_id.as_deref().ok_or("missing PCI identity")?,
            nic.device_attrs.get("rdma.gid").ok_or("missing GID")?,
            nic.device_attrs
                .get("rdma.gid_type")
                .ok_or("missing GID type")?
        );
    }
    Ok(())
}

pub(super) fn create(case: &Case, bind: SocketAddr) -> Result<(TransferEngine, RegisteredMemory)> {
    let engine = engine(case, bind)?;
    let memory = engine.register_memory(
        vec![0; case.length()].into_boxed_slice(),
        Permission::RemoteReadWrite,
    )?;
    inventory(case, &engine)?;
    Ok((engine, memory))
}

pub(super) fn maps() -> Result<()> {
    std::fs::write(
        env::var_os("ORBITE_SHARED_MAPS").ok_or("missing maps path")?,
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
