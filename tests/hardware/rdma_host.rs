//! Installed-crate host qualification through public APIs only.

use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use orbit_te::transport::rdma::{RdmaConfig, RdmaProgressMode, RdmaStats};
use orbit_te::{
    EngineConfig, Permission, SourceEpoch, TransferEngine, TransferOp, TransferRequest, Transport,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const GUARD: usize = 64;
const ROUNDS: usize = 3;
const TIMEOUT: Duration = Duration::from_secs(20);
const PROTOCOL: &str = "orbitte-installed-host-v1";

fn number(name: &str) -> Result<usize> {
    Ok(env::var(name)?.parse()?)
}

struct Case {
    source: bool,
    read: bool,
    parents: usize,
    payload: usize,
    block: usize,
    lanes: usize,
    mask: u64,
    events: bool,
    rails: Vec<String>,
}

impl Case {
    fn load() -> Result<Self> {
        let case = Self {
            source: match env::var("ORBITTE_SHARED_ROLE")?.as_str() {
                "source" => true,
                "requester" => false,
                _ => return Err("invalid peer role".into()),
            },
            read: match env::var("ORBITTE_RAIL_OPERATION")?.as_str() {
                "read" => true,
                "write" => false,
                _ => return Err("invalid operation".into()),
            },
            parents: number("ORBITTE_HOST_PARENTS")?,
            payload: number("ORBITTE_HOST_PAYLOAD")?,
            block: number("ORBITTE_HOST_BLOCK")?,
            lanes: number("ORBITTE_RDMA_LANES")?,
            mask: env::var("ORBITTE_RAIL_MASK")?.parse()?,
            events: match env::var("ORBITTE_RDMA_EVENTS")?.as_str() {
                "0" => false,
                "1" => true,
                _ => return Err("invalid progress mode".into()),
            },
            rails: env::var("ORBITTE_SHARED_RAILS")?
                .split(',')
                .map(str::to_owned)
                .collect(),
        };
        if !(1..=32).contains(&case.parents)
            || !(128..=32 << 20).contains(&case.payload)
            || !(64 << 10..=4 << 20).contains(&case.block)
            || !(1..=3).contains(&case.lanes)
            || !(1..=3).contains(&case.mask)
            || case.rails.len() != 2
            || case.rails[0] == case.rails[1]
            || case.rails.iter().any(|name| {
                name.is_empty()
                    || !name
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
            })
        {
            return Err("unsupported installed-host profile".into());
        }
        if case.length() > 256 << 20 {
            return Err("installed-host backing exceeds 256 MiB".into());
        }
        Ok(case)
    }

    fn bank(&self) -> usize {
        2 * self.payload + 3 * GUARD
    }
    fn length(&self) -> usize {
        self.parents * self.bank()
    }
    fn destination(&self) -> usize {
        self.payload + 2 * GUARD
    }

    fn bytes(&self, source: bool, round: usize) -> Vec<u8> {
        let mut bytes = vec![0xa5; self.length()];
        for parent in 0..self.parents {
            let start = parent * self.bank();
            for index in 0..self.payload {
                bytes[start + GUARD + index] = pattern(source, round, parent, index);
            }
            bytes[start + self.destination()..start + self.destination() + self.payload].fill(0xcc);
        }
        bytes
    }

    fn signature(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}",
            self.parents, self.payload, self.block, self.lanes, self.mask, self.events, self.read
        )
    }
}

fn pattern(source: bool, round: usize, parent: usize, index: usize) -> u8 {
    let seed = 0x9e3779b97f4a7c15u64
        .wrapping_mul((1 + round * 71 + parent * 7 + usize::from(source)) as u64);
    let value = seed.wrapping_add((index as u64).wrapping_mul(0x517cc1b727220a95));
    (value ^ (value >> 17) ^ (value >> 41)) as u8
}

fn control(source: bool, bind: SocketAddr) -> Result<TcpStream> {
    let stream = if source {
        let listener = TcpListener::bind(bind)?;
        listener.set_nonblocking(true)?;
        println!("ORBITTE_READY {}", listener.local_addr()?);
        std::io::stdout().flush()?;
        let deadline = Instant::now() + TIMEOUT;
        loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(10))
                }
                Err(error) => return Err(error.into()),
            }
        }
    } else {
        TcpStream::connect_timeout(&env::var("ORBITTE_SHARED_PEER")?.parse()?, TIMEOUT)?
    };
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    Ok(stream)
}

fn exchange(stream: &mut TcpStream, text: &str) -> Result<String> {
    if text.len() > 4096 {
        return Err("oversized control frame".into());
    }
    stream.write_all(&(text.len() as u32).to_be_bytes())?;
    stream.write_all(text.as_bytes())?;
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > 4096 {
        return Err("oversized peer control frame".into());
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(String::from_utf8(bytes)?)
}

fn barrier(stream: &mut TcpStream, phase: &str) -> Result<()> {
    if exchange(stream, phase)? != phase {
        return Err("peer phase mismatch".into());
    }
    Ok(())
}

fn idle(engine: &TransferEngine) -> Result<RdmaStats> {
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

fn main() -> Result<()> {
    if env::args().skip(1).eq(["--protocol"]) {
        println!("{PROTOCOL}");
        return Ok(());
    }
    if env::args().len() != 1 {
        return Err("only --protocol or explicit profile environment is supported".into());
    }
    let case = Case::load()?;
    let bind: SocketAddr = env::var("ORBITTE_SHARED_BIND")?.parse()?;
    let engine = TransferEngine::new(EngineConfig {
        bind_addr: bind,
        max_registered_bytes: case.length(),
        max_transfer_bytes: case.payload,
        max_inflight_bytes: case.parents * case.payload,
        handler_threads: case.parents + 2,
        handler_timeout: TIMEOUT,
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
    let mut stream = control(case.source, bind)?;
    let peer = exchange(
        &mut stream,
        &format!(
            "{} {} {} {} {} {}",
            PROTOCOL,
            case.signature(),
            engine.endpoint(),
            engine.source_epoch(),
            memory.base_address(),
            memory.len()
        ),
    )?;
    let fields: Vec<_> = peer.split_whitespace().collect();
    if fields.len() != 6
        || fields[0] != PROTOCOL
        || fields[1] != case.signature()
        || fields[5].parse::<usize>()? != case.length()
    {
        return Err("peer profile/registration mismatch".into());
    }
    let segment = engine.open_segment(
        fields[2].parse()?,
        fields[3].parse::<SourceEpoch>()?,
        TIMEOUT,
    )?;
    let remote: u64 = fields[4].parse()?;
    let mut last_reused = 0;
    let mut last_completed = 0;
    for round in 0..ROUNDS {
        let mut expected = case.bytes(case.source, round);
        memory.write(0, &expected)?;
        barrier(&mut stream, &format!("ready-{round}"))?;
        if !case.source {
            let start = Barrier::new(case.parents);
            thread::scope(|scope| -> Result<()> {
                let mut joins = Vec::new();
                for parent in 0..case.parents {
                    let (case, engine, memory, segment, start) =
                        (&case, &engine, &memory, &segment, &start);
                    joins.push(scope.spawn(move || -> Result<()> {
                        start.wait();
                        let report = engine.submit_and_wait(
                            if case.read {
                                TransferOp::Read
                            } else {
                                TransferOp::Write
                            },
                            segment,
                            &[TransferRequest {
                                transport: Transport::Rdma,
                                local_address: memory.base_address()
                                    + (parent * case.bank()
                                        + if case.read { case.destination() } else { GUARD })
                                        as u64,
                                remote_address: remote
                                    + (parent * case.bank()
                                        + if case.read { GUARD } else { case.destination() })
                                        as u64,
                                length: case.payload,
                            }],
                            TIMEOUT,
                        )?;
                        if report.failure.is_some()
                            || !report.locally_drained
                            || report.transferred_bytes != case.payload
                        {
                            return Err(format!("parent {parent} failed: {report:?}").into());
                        }
                        Ok(())
                    }));
                }
                for join in joins {
                    join.join().map_err(|_| "parent thread panicked")??;
                }
                Ok(())
            })?;
        }
        barrier(&mut stream, &format!("transferred-{round}"))?;
        let stats = idle(&engine)?;
        if case.source != case.read {
            for parent in 0..case.parents {
                for index in 0..case.payload {
                    expected[parent * case.bank() + case.destination() + index] =
                        pattern(!case.source, round, parent, index);
                }
            }
        }
        let mut actual = vec![0; case.length()];
        memory.read(0, &mut actual)?;
        if let Some(index) = actual.iter().zip(&expected).position(|(a, b)| a != b) {
            return Err(format!("byte/sentinel mismatch at round {round}, byte {index}").into());
        }
        if stats.opened_devices != 2
            || stats.persistent_mrs != 4
            || stats.opened_persistent_mrs != 4
            || stats.persistent_source_mrs != 2
            || stats.opened_connections == 0
            || stats.opened_connections > (case.mask.count_ones() as usize * case.lanes) as u64
            || stats.closed_connections != 0
            || stats.completed_reusable_grants <= last_completed
            || (round > 0 && stats.reused_connections <= last_reused)
            || stats.retained_grants != 0
            || stats.progress_failures != 0
            || stats.staging_bytes != 0
        {
            return Err(format!("persistent MR/QP contract failed: {stats:?}").into());
        }
        let bytes = ((round + 1) * case.parents * case.payload) as u64;
        if !case.source
            && (stats.completed_read_bytes != if case.read { bytes } else { 0 }
                || stats.completed_write_bytes != if case.read { 0 } else { bytes })
        {
            return Err(format!("native RDMA byte counters disagree: {stats:?}").into());
        }
        println!(
            "ORBITTE_INSTALLED_ROUND {{\"round\":{round},\"opened_qps\":{},\"reused_qps\":{},\"completed_grants\":{},\"opened_mrs\":{},\"read_bytes\":{},\"write_bytes\":{}}}",
            stats.opened_connections,
            stats.reused_connections,
            stats.completed_reusable_grants,
            stats.opened_persistent_mrs,
            stats.completed_read_bytes,
            stats.completed_write_bytes
        );
        last_reused = stats.reused_connections;
        last_completed = stats.completed_reusable_grants;
        barrier(&mut stream, &format!("verified-{round}"))?;
    }
    std::fs::write(
        env::var_os("ORBITTE_SHARED_MAPS").ok_or("missing maps path")?,
        std::fs::read("/proc/self/maps")?,
    )?;
    engine.close(TIMEOUT)?;
    engine.unregister_memory(&memory)?;
    drop(memory);
    clean(&engine)?;
    barrier(&mut stream, "closed")?;
    println!(
        "ORBITTE_INSTALLED_RESULT {{\"protocol\":\"{PROTOCOL}\",\"status\":\"passed\",\"role\":\"{}\",\"operation\":\"{}\",\"parents\":{},\"payload\":{},\"rounds\":{ROUNDS},\"full_bytes_and_guards\":true,\"locally_drained\":true}}",
        if case.source { "source" } else { "requester" },
        if case.read { "read" } else { "write" },
        case.parents,
        case.payload
    );
    Ok(())
}
