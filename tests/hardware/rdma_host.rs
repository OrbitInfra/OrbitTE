//! Installed-crate host qualification through public APIs only.

use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use orbit_te::{
    RegisteredMemory, RemoteSegment, SourceEpoch, TransferEngine, TransferOp, TransferRequest,
    Transport,
};

#[path = "rdma_host/gate.rs"]
mod gate;
#[path = "rdma_host/interruption.rs"]
mod interruption;
#[path = "rdma_host/resources.rs"]
mod resources;
#[path = "rdma_host/restart.rs"]
mod restart;

use resources::{close, create, idle, maps};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const GUARD: usize = 64;
const ROUNDS: usize = 3;
const TIMEOUT: Duration = Duration::from_secs(20);
const PROTOCOL: &str = "orbitte-installed-host-v3";

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

fn control(source: bool, bind: SocketAddr, peer: Option<SocketAddr>) -> Result<TcpStream> {
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
        TcpStream::connect_timeout(
            &match peer {
                Some(address) => address,
                None => env::var("ORBITTE_SHARED_PEER")?.parse()?,
            },
            TIMEOUT,
        )?
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

fn open_peer(
    case: &Case,
    engine: &TransferEngine,
    memory: &RegisteredMemory,
    stream: &mut TcpStream,
    advertised: SocketAddr,
) -> Result<RemoteSegment> {
    let peer = exchange(
        stream,
        &format!(
            "{} {} {} {} {} {}",
            PROTOCOL,
            case.signature(),
            advertised,
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
    if segment.regions().len() != 1
        || segment.regions()[0].base_address() != fields[4].parse::<u64>()?
        || segment.regions()[0].len() != case.length()
    {
        return Err("remote registration does not match rendezvous".into());
    }
    Ok(segment)
}

fn rounds(
    case: &Case,
    engine: &TransferEngine,
    memory: &RegisteredMemory,
    segment: &RemoteSegment,
    stream: &mut TcpStream,
    phase: usize,
) -> Result<()> {
    let remote = segment.regions()[0].base_address();
    let baseline = idle(engine)?;
    let mut last_reused = baseline.reused_connections;
    let mut last_completed = baseline.completed_reusable_grants;
    for round in 0..ROUNDS {
        let mut expected = case.bytes(case.source, phase * ROUNDS + round);
        memory.write(0, &expected)?;
        barrier(stream, &format!("ready-{round}"))?;
        if !case.source {
            let start = Barrier::new(case.parents);
            thread::scope(|scope| -> Result<()> {
                let mut joins = Vec::new();
                for parent in 0..case.parents {
                    let (case, engine, memory, segment, start) =
                        (case, engine, memory, segment, &start);
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
        barrier(stream, &format!("transferred-{round}"))?;
        let stats = idle(engine)?;
        if case.source != case.read {
            for parent in 0..case.parents {
                for index in 0..case.payload {
                    expected[parent * case.bank() + case.destination() + index] =
                        pattern(!case.source, phase * ROUNDS + round, parent, index);
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
            || stats.opened_connections - baseline.opened_connections
                > (case.mask.count_ones() as usize * case.lanes) as u64
            || stats.active_endpoints > 2 * case.lanes
            || stats.opened_connections - stats.closed_connections > (2 * case.lanes) as u64
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
            && (stats.completed_read_bytes - baseline.completed_read_bytes
                != if case.read { bytes } else { 0 }
                || stats.completed_write_bytes - baseline.completed_write_bytes
                    != if case.read { 0 } else { bytes })
        {
            return Err(format!("native RDMA byte counters disagree: {stats:?}").into());
        }
        println!(
            "ORBITTE_INSTALLED_ROUND {{\"phase\":{phase},\"round\":{round},\"opened_qps\":{},\"closed_qps\":{},\"reused_qps\":{},\"completed_grants\":{},\"opened_mrs\":{},\"read_bytes\":{},\"write_bytes\":{}}}",
            stats.opened_connections,
            stats.closed_connections,
            stats.reused_connections,
            stats.completed_reusable_grants,
            stats.opened_persistent_mrs,
            stats.completed_read_bytes,
            stats.completed_write_bytes
        );
        last_reused = stats.reused_connections;
        last_completed = stats.completed_reusable_grants;
        barrier(stream, &format!("verified-{round}"))?;
    }
    Ok(())
}

fn result(case: &Case) -> Result<()> {
    println!(
        "ORBITTE_INSTALLED_RESULT {{\"protocol\":\"{PROTOCOL}\",\"status\":\"passed\",\"role\":\"{}\",\"operation\":\"{}\",\"parents\":{},\"payload\":{},\"rounds\":{ROUNDS},\"full_bytes_and_guards\":true,\"locally_drained\":true}}",
        if case.source { "source" } else { "requester" },
        if case.read { "read" } else { "write" },
        case.parents,
        case.payload
    );
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
    match env::var("ORBITTE_HOST_MODE")?.as_str() {
        "installed-restart" | "installed-interruption" => restart::run(&case),
        "installed-host" => {
            let bind = env::var("ORBITTE_SHARED_BIND")?.parse()?;
            let (engine, memory) = create(&case, bind)?;
            let mut stream = control(case.source, bind, None)?;
            let peer = open_peer(&case, &engine, &memory, &mut stream, engine.endpoint())?;
            rounds(&case, &engine, &memory, &peer, &mut stream, 0)?;
            maps()?;
            close(&engine, memory)?;
            barrier(&mut stream, "closed")?;
            result(&case)
        }
        _ => Err("unsupported installed host mode".into()),
    }
}
