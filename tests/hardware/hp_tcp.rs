//! Public two-process HP-TCP rail and byte-integrity consumer.
use orbit_te::{
    EngineConfig, Error, HpTcpConfig, Permission, Result, TransferEngine, TransferOp,
    TransferRequest, Transport,
};
use std::{
    io::{self, BufRead, Write},
    net::{Ipv4Addr, SocketAddr},
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
        println!("orbitte-installed-hp-tcp");
        return Ok(());
    }
    let usage = || Error::InvalidConfig("use serve RAIL_A,RAIL_B or run RAIL_A,RAIL_B PEER EPOCH");
    let (mode, rails) = args.first().zip(args.get(1)).ok_or_else(usage)?;
    let rails = rails
        .split(',')
        .map(|s| s.parse::<Ipv4Addr>().map_err(|_| usage()))
        .collect::<Result<Vec<_>>>()?;
    if rails.len() != 2 || rails[0] == rails[1] {
        return Err(usage());
    }
    let engine = TransferEngine::new(EngineConfig {
        tcp: false,
        bind_addr: (rails[0], 0).into(),
        hp_tcp: HpTcpConfig {
            enable: true,
            worker_count: 2,
            connections_per_peer: 2,
            rail_addresses: rails,
            ..Default::default()
        },
        ..Default::default()
    })?;
    let pattern = expected();
    match mode.as_str() {
        "serve" if args.len() == 2 => {
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
        "run" if args.len() == 4 => {
            let endpoint = args[2].parse::<SocketAddr>().map_err(|_| usage())?;
            let remote = engine.open_segment(endpoint, args[3].parse()?, WAIT)?;
            let descriptor = remote.regions().first().ok_or(Error::RegionNotFound)?;
            let local =
                engine.register_memory(vec![0xee; LENGTH + 2].into(), Permission::LocalOnly)?;
            let request = TransferRequest::new(
                Transport::HpTcp,
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
                    return Err(Error::Protocol("incomplete HP-TCP payload"));
                }
            }
            let mut bytes = vec![0; LENGTH + 2];
            local.read(0, &mut bytes)?;
            if bytes[0] != 0xee || bytes[LENGTH + 1] != 0xee || bytes[1..=LENGTH] != pattern {
                return Err(Error::Protocol("requester data or guards"));
            }
            engine.close(WAIT)?;
        }
        _ => return Err(usage()),
    }
    println!("PASS {mode} {LENGTH}");
    Ok(())
}
