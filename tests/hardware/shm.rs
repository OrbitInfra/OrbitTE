//! Independent public-API source and requester for the installed same-host SHM profile.
use orbite::{
    EngineConfig, Error, Permission, Result, ShmConfig, TransferEngine, TransferOp,
    TransferRequest, Transport,
};
use std::{
    io::{self, BufRead, Write},
    path::PathBuf,
    time::Duration,
};
const N: usize = (1 << 20) + 7;
const BASE: usize = 19;
const RO: usize = BASE + N + 17;
const TOTAL: usize = RO + 64;
const WAIT: Duration = Duration::from_secs(30);
fn pattern(i: usize) -> u8 {
    (i % 241) as u8
}
fn verify(mem: &orbite::RegisteredMemory, expected: &[u8]) -> Result<()> {
    let mut got = vec![0; expected.len()];
    mem.read(0, &mut got)?;
    if got != expected {
        return Err(Error::Protocol("SHM bytes or guards differ"));
    }
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.as_slice() == ["--protocol"] {
        println!("orbite-installed-shm");
        return Ok(());
    }
    let mode = args
        .first()
        .ok_or(Error::InvalidConfig("expected source or requester"))?;
    let directory = args
        .get(1)
        .ok_or(Error::InvalidConfig("expected POSIX or a mount path"))?;
    let cxl = if directory == "posix" {
        PathBuf::new()
    } else {
        PathBuf::from(directory)
    };
    let engine = TransferEngine::new(EngineConfig {
        tcp: false,
        shm: ShmConfig {
            enable: true,
            cxl_mount_path: cxl,
        },
        ..Default::default()
    })?;
    match mode.as_str() {
        "source" => {
            let host = engine.allocate_shm_memory(TOTAL)?;
            let mut expected = vec![0xa5; TOTAL];
            for (i, v) in expected[BASE..BASE + N].iter_mut().enumerate() {
                *v = pattern(i);
            }
            host.write(0, &expected)?;
            let remote =
                engine.register_host_memory(host.clone(), BASE, N, Permission::RemoteReadWrite)?;
            let alias =
                engine.register_host_memory(host.clone(), BASE, N, Permission::RemoteReadWrite)?;
            let readonly =
                engine.register_host_memory(host.clone(), RO, 32, Permission::RemoteRead)?;
            engine.unregister_memory(&alias)?;
            drop(alias);
            let address = host.base_address();
            let maps = std::fs::read_to_string("/proc/self/maps")?;
            let path = maps
                .lines()
                .find_map(|line| {
                    let (start, end) = line.split_whitespace().next()?.split_once('-')?;
                    let start = u64::from_str_radix(start, 16).ok()?;
                    let end = u64::from_str_radix(end, 16).ok()?;
                    (start <= address && address < end)
                        .then(|| line.split_once('/').map(|(_, path)| format!("/{path}")))
                        .flatten()
                })
                .ok_or(Error::Protocol("source mapping has no path"))?;
            println!(
                "READY {} {} {} {} {}",
                engine.endpoint(),
                engine.source_epoch(),
                remote.base_address(),
                readonly.base_address(),
                path
            );
            io::stdout().flush()?;
            if io::stdin().lock().lines().next().transpose()?.as_deref() != Some("verify") {
                return Err(Error::Protocol("missing SHM requester receipt"));
            }
            engine.close_source(WAIT)?;
            for v in &mut expected[BASE..BASE + N] {
                *v ^= 0x5a;
            }
            let mut got = vec![0; TOTAL];
            host.read(0, &mut got)?;
            if got != expected {
                return Err(Error::Protocol("source SHM data or guards"));
            }
            engine.unregister_memory(&remote)?;
            engine.unregister_memory(&readonly)?;
            drop((remote, readonly));
            engine.free_shm_memory(host)?;
            if std::path::Path::new(&path).exists() {
                return Err(Error::Protocol("source SHM name survived release"));
            }
        }
        "requester" if args.len() == 6 => {
            let peer = engine.open_segment(
                args[2]
                    .parse()
                    .map_err(|_| Error::InvalidConfig("peer address"))?,
                args[3].parse()?,
                WAIT,
            )?;
            let addr = args[4].parse().map_err(|_| Error::InvalidRange)?;
            let ro = args[5].parse().map_err(|_| Error::InvalidRange)?;
            let l = engine.register_memory(vec![0xee; N + 2].into(), Permission::LocalOnly)?;
            let mut want = vec![0xee; N + 2];
            let req = TransferRequest::new(Transport::Unspec, l.base_address() + 1, addr, N);
            for op in [TransferOp::Read, TransferOp::Write] {
                let rep = engine.submit_and_wait(op, &peer, std::slice::from_ref(&req), WAIT)?;
                if !rep.is_complete() || rep.completed_requests != 1 || rep.transferred_bytes != N {
                    return Err(Error::Protocol("SHM payload incomplete"));
                }
                if op == TransferOp::Read {
                    for (i, v) in want[1..=N].iter_mut().enumerate() {
                        *v = pattern(i);
                    }
                    verify(&l, &want)?;
                    for v in &mut want[1..=N] {
                        *v ^= 0x5a;
                    }
                    l.write(1, &want[1..=N])?;
                }
            }
            let denied = TransferRequest::new(Transport::Shm, l.base_address() + 1, ro, 32);
            if !matches!(
                engine.submit_and_wait(TransferOp::Write, &peer, &[denied], WAIT),
                Err(Error::PermissionDenied)
            ) {
                return Err(Error::Protocol("read-only SHM accepted WRITE"));
            }
            verify(&l, &want)?;
            engine.close(WAIT)?;
        }
        _ => {
            return Err(Error::InvalidConfig(
                "use source MOUNT or requester MOUNT ENDPOINT EPOCH ADDRESS READONLY",
            ));
        }
    }
    println!("PASS {mode} {N}");
    Ok(())
}
