//! Packaged host-DRAM libfabric consumer for the unqualified EFA profile.
use orbite::{
    EngineConfig, Error, FabricConfig, FabricProvider, Permission, Result, TransferEngine,
    TransferOp, TransferRequest, Transport,
};
use std::{
    io::{self, BufRead, Write},
    net::{IpAddr, SocketAddr},
    time::Duration,
};
const LENGTH: usize = (2 << 20) + 3;
const WAIT: Duration = Duration::from_secs(30);
fn expected() -> Vec<u8> {
    (0..LENGTH).map(|i| (i % 239) as u8).collect()
}
fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() == ["--protocol"] {
        println!("orbite-installed-fabric");
        return Ok(());
    }
    let usage =
        || Error::InvalidConfig("use serve BIND_IP DOMAIN or run BIND_IP DOMAIN PEER EPOCH");
    let mode = args.first().ok_or_else(usage)?;
    let bind: IpAddr = args
        .get(1)
        .ok_or_else(usage)?
        .parse()
        .map_err(|_| usage())?;
    let domain = args.get(2).ok_or_else(usage)?;
    if bind.is_loopback() || bind.is_unspecified() {
        return Err(usage());
    }
    let engine = TransferEngine::new(EngineConfig {
        tcp: false,
        bind_addr: (bind, 0).into(),
        fabric: Some(FabricConfig {
            provider: FabricProvider::Efa,
            devices: vec![domain.clone()],
            max_mr_size: 1 << 20,
            ..Default::default()
        }),
        ..Default::default()
    })?;
    let pattern = expected();
    match mode.as_str() {
        "serve" if args.len() == 3 => {
            let remote = engine
                .register_memory(vec![0xa5; LENGTH + 2].into(), Permission::RemoteReadWrite)?;
            println!("READY {} {}", engine.endpoint(), engine.source_epoch());
            io::stdout().flush()?;
            if io::stdin().lock().lines().next().transpose()?.as_deref() != Some("verify") {
                return Err(Error::Protocol("missing requester receipt"));
            }
            engine.close_source(WAIT)?;
            let mut bytes = vec![0; LENGTH + 2];
            remote.read(0, &mut bytes)?;
            if bytes[0] != 0xa5 || bytes[LENGTH + 1] != 0xa5 || bytes[1..=LENGTH] != pattern {
                return Err(Error::Protocol("source data or guards"));
            }
        }
        "run" if args.len() == 5 => {
            let endpoint = args[3].parse::<SocketAddr>().map_err(|_| usage())?;
            let remote = engine.open_segment(endpoint, args[4].parse()?, WAIT)?;
            let descriptor = remote.regions().first().ok_or(Error::RegionNotFound)?;
            let local =
                engine.register_memory(vec![0xee; LENGTH + 2].into(), Permission::LocalOnly)?;
            let request = TransferRequest::new(
                Transport::Fabric,
                local.base_address() + 1,
                descriptor.base_address() + 1,
                LENGTH,
            );
            local.write(1, &pattern)?;
            for op in [TransferOp::Write, TransferOp::Read] {
                if op == TransferOp::Read {
                    local.write(1, &vec![0; LENGTH])?;
                }
                let report =
                    engine.submit_and_wait(op, &remote, std::slice::from_ref(&request), WAIT)?;
                if !report.is_complete() || report.transferred_bytes != LENGTH {
                    return Err(Error::Protocol("incomplete libfabric payload"));
                }
            }
            let mut bytes = vec![0; LENGTH + 2];
            local.read(0, &mut bytes)?;
            if bytes[0] != 0xee || bytes[LENGTH + 1] != 0xee || bytes[1..=LENGTH] != pattern {
                return Err(Error::Protocol("requester data or guards"));
            }
            let stats = engine
                .stats()?
                .fabric
                .ok_or(Error::Unavailable("EFA owner"))?;
            if stats.read_bytes != LENGTH as u64 || stats.written_bytes != LENGTH as u64 {
                return Err(Error::Protocol("missing native fabric completions"));
            }
            println!("FABRIC {stats:?}");
        }
        _ => return Err(usage()),
    }
    engine.close(WAIT)?;
    println!("PASS {mode} {LENGTH}");
    Ok(())
}
