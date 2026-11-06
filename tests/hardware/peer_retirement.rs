//! Installed selective retirement, with real native payloads and a source restart.
use orbite::cuda::CudaMemory;
use orbite::transport::rdma::{RdmaConfig, RdmaHostMemory};
use orbite::{
    EngineConfig, Error, MnnvlConfig, Permission, RegisteredMemory, RemoteSegment, SourceEpoch,
    TransferEngine, TransferOp, TransferRequest, Transport,
};
use std::{
    error::Error as StdError,
    io::{BufRead, Read, Write},
    sync::Arc,
    time::Duration,
};

type Result<T> = std::result::Result<T, Box<dyn StdError>>;
const WAIT: Duration = Duration::from_secs(60);
const LEN: usize = (1 << 20) + 2;

fn check(m: &RegisteredMemory, edge: u8, body: u8) -> Result<()> {
    let mut actual = vec![0; LEN];
    m.read(0, &mut actual)?;
    if actual[0] != edge || actual[LEN - 1] != edge || actual[1..LEN - 1].iter().any(|b| *b != body)
    {
        return Err("payload or guard mismatch".into());
    }
    Ok(())
}
fn say(line: &str) -> Result<()> {
    println!("{line}");
    std::io::stdout().flush()?;
    Ok(())
}
fn line() -> Result<String> {
    let mut line = String::new();
    if std::io::stdin().lock().take(1025).read_line(&mut line)? == 0 {
        return Err("coordinator closed input".into());
    }
    if line.len() > 1024 {
        return Err("coordinator command too long".into());
    }
    Ok(line.trim().into())
}
fn transfer(
    e: &TransferEngine,
    l: &RegisteredMemory,
    p: &RemoteSegment,
    t: Transport,
    op: TransferOp,
) -> Result<()> {
    let [r] = p.regions() else {
        return Err("expected one source registration".into());
    };
    let q = TransferRequest::new(t, l.base_address() + 1, r.base_address() + 1, LEN - 2);
    let report = e.submit_and_wait(op, p, &[q], WAIT)?;
    if !report.is_complete() || !report.locally_drained || report.transferred_bytes != LEN - 2 {
        return Err(format!("incomplete native receipt: {report:?}").into());
    }
    Ok(())
}
fn exchange(
    e: &TransferEngine,
    l: &RegisteredMemory,
    p: &RemoteSegment,
    t: Transport,
) -> Result<()> {
    l.write(0, &vec![0xcd; LEN])?;
    transfer(e, l, p, t, TransferOp::Read)?;
    check(l, 0xcd, 0x5a)?;
    l.write(1, &vec![0x6b; LEN - 2])?;
    transfer(e, l, p, t, TransferOp::Write)?;
    check(l, 0xcd, 0x6b)
}
fn retired(
    e: &TransferEngine,
    p: &RemoteSegment,
    l: &RegisteredMemory,
    t: Transport,
) -> Result<()> {
    if !matches!(
        e.open_segment(p.endpoint(), p.source_epoch(), WAIT),
        Err(Error::PeerClosed)
    ) {
        return Err("old epoch reopened".into());
    }
    let q = TransferRequest::new(
        t,
        l.base_address() + 1,
        p.regions()[0].base_address() + 1,
        LEN - 2,
    );
    if !matches!(
        e.submit_and_wait(TransferOp::Read, p, &[q], WAIT),
        Err(Error::PeerClosed)
    ) {
        return Err("stale descriptor admitted".into());
    }
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--protocol"] {
        return say("orbite-installed-peer-retirement");
    }
    let [role, profile, bind, device, domain, peers @ ..] = &args[..] else {
        return Err("ROLE PROFILE BIND RNIC FABRIC_DOMAIN [PEER EPOCH OTHER EPOCH]".into());
    };
    let mut c = EngineConfig {
        tcp: false,
        max_failover_attempts: 0,
        handler_timeout: WAIT,
        bind_addr: bind.parse()?,
        ..Default::default()
    };
    let transport = match profile.as_str() {
        "rdma" => {
            let mut r = RdmaConfig::new(device);
            r.host_memory = RdmaHostMemory::Direct;
            c.rdma = Some(r);
            Transport::Rdma
        }
        "nvlink" => {
            c.nvlink = true;
            c.nvlink_cache_entries = 8;
            Transport::Nvlink
        }
        "mnnvl" => {
            c.mnnvl = Some(MnnvlConfig {
                fabric_domain: Some(domain.parse()?),
                ..Default::default()
            });
            c.nvlink_cache_entries = 8;
            Transport::Mnnvl
        }
        _ => return Err("unknown native profile".into()),
    };
    let e = Arc::new(TransferEngine::new(c)?);
    let permission = if role == "source" {
        Permission::RemoteReadWrite
    } else {
        Permission::LocalOnly
    };
    let memory = match transport {
        Transport::Rdma => e.register_memory(vec![0x5a; LEN].into(), permission)?,
        _ => {
            let m = if transport == Transport::Mnnvl {
                e.allocate_mnnvl(0, LEN)?
            } else {
                Arc::new(CudaMemory::allocate(0, LEN)?)
            };
            e.register_cuda_memory_batch(&[(m, 0, LEN)], permission)?
                .remove(0)
        }
    };
    if role == "source" {
        memory.write(0, &vec![0x5a; LEN])?;
        say(&format!("READY {} {}", e.endpoint(), e.source_epoch()))?;
        if line()? != "VERIFY" {
            return Err("expected VERIFY".into());
        }
        check(&memory, 0x5a, 0x6b)?;
        e.unregister_memory(&memory)?;
        drop(memory);
        e.close(WAIT)?;
        return say("PASS source");
    }
    let [endpoint, epoch, other_endpoint, other_epoch] = peers else {
        return Err("expected two authoritative peers".into());
    };
    let p = e.open_segment(endpoint.parse()?, epoch.parse()?, WAIT)?;
    let other = e.open_segment(other_endpoint.parse()?, other_epoch.parse()?, WAIT)?;
    exchange(&e, &memory, &p, transport)?;
    exchange(&e, &memory, &other, transport)?;
    e.close_segment(p.endpoint(), p.source_epoch(), WAIT)?;
    retired(&e, &p, &memory, transport)?;
    transfer(&e, &memory, &other, transport, TransferOp::Read)?;
    check(&memory, 0xcd, 0x6b)?;
    if let Some(n) = e.stats()?.nvlink {
        if n.imports != 1 || n.retained_imports != 0 {
            return Err("unrelated IPC cache entry lost or retained".into());
        }
    }
    say("RETIRED")?;
    let new = line()?;
    let parts: Vec<_> = new.split_whitespace().collect();
    let ["NEW", endpoint, epoch] = parts.as_slice() else {
        return Err("expected NEW endpoint epoch".into());
    };
    let epoch: SourceEpoch = epoch.parse()?;
    if endpoint.parse::<std::net::SocketAddr>()? != p.endpoint() || epoch == p.source_epoch() {
        return Err("restart must reuse endpoint with a fresh epoch".into());
    }
    let fresh = e.open_segment(p.endpoint(), epoch, WAIT)?;
    exchange(&e, &memory, &fresh, transport)?;
    retired(&e, &p, &memory, transport)?;
    e.close_segment(fresh.endpoint(), fresh.source_epoch(), WAIT)?;
    e.close_segment(other.endpoint(), other.source_epoch(), WAIT)?;
    let s = e.stats()?;
    if s.active_peer_operations != 0 || s.inflight_bytes != 0 {
        return Err("peer admission did not drain".into());
    }
    if let Some(r) = s.rdma {
        if r.active_endpoints != 0 || r.retained_endpoints != 0 {
            return Err("requester QPs did not retire".into());
        }
    }
    if let Some(n) = s.nvlink {
        if n.imports != 0 || n.retained_imports != 0 {
            return Err("requester imports did not retire".into());
        }
    }
    e.unregister_memory(&memory)?;
    drop(memory);
    e.close(WAIT)?;
    say("PASS requester")
}
