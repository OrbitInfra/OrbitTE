//! Public-API mixed-peer consumer. The companion script supplies an authoritative
//! test epoch; the frozen peer cannot attest to it over its RPC wire.
use orbite::{
    EngineConfig, Permission, RemoteEffect, SourceEpoch, TransferEngine, TransferOp,
    TransferRequest, Transport,
};
use std::{
    io::{self, Write},
    time::Duration,
};
const WAIT: Duration = Duration::from_secs(30);
fn pattern(i: usize, written: bool) -> u8 {
    ((i * 17 + if written { 91 } else { 23 }) % 251) as u8
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let source = args.get(1).is_some_and(|s| s == "source");
    let n: usize = args
        .get(if source { 2 } else { 5 })
        .ok_or("missing size")?
        .parse()?;
    if n == 0 || n > 1 << 20 {
        return Err("size bound".into());
    }
    let mut c = EngineConfig::default();
    if source {
        c.tent_bind_addr = Some("127.0.0.1:0".parse()?);
    }
    let engine = std::sync::Arc::new(TransferEngine::new(c)?);
    let mut initial = vec![0xa1; n + 2];
    if source {
        for i in 0..n {
            initial[i + 1] = pattern(i, false);
        }
    }
    let local = engine.register_memory(
        initial.into(),
        if source {
            Permission::RemoteReadWrite
        } else {
            Permission::LocalOnly
        },
    )?;
    if source {
        let ro = engine.register_memory(vec![0xb2; n + 2].into(), Permission::RemoteRead)?;
        println!(
            "READY {} {} {} {}",
            engine.tent_endpoint().ok_or("no endpoint")?,
            local.base_address(),
            ro.base_address(),
            n
        );
        io::stdout().flush()?;
        let mut command = String::new();
        io::stdin().read_line(&mut command)?;
        if command.trim() != "verify" {
            return Err("missing verification".into());
        }
        let mut b = vec![0; n + 2];
        local.read(0, &mut b)?;
        assert_eq!((b[0], b[n + 1]), (0xa1, 0xa1));
        for i in 0..n {
            assert_eq!(b[i + 1], pattern(i, true));
        }
        ro.read(0, &mut b)?;
        assert_eq!(b, vec![0xb2; n + 2]);
        engine.unregister_memory(&ro)?;
    } else {
        let endpoint = args[2].parse()?;
        let remote: u64 = args[3].parse()?;
        let readonly: u64 = args[4].parse()?;
        let epoch: SourceEpoch = "11111111111111111111111111111111".parse()?;
        let peer = engine.open_tent_segment(endpoint, epoch, WAIT)?;
        assert_eq!(peer.regions().len(), 2);
        let req = TransferRequest::new(Transport::Tcp, local.base_address() + 1, remote + 1, n);
        let mut batch = engine.allocate_batch(1)?;
        batch.submit(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&req),
            None,
            WAIT,
        )?;
        batch.seal()?;
        let status = batch.wait(WAIT)?;
        assert_eq!(status.completed_tasks, 1);
        batch.free()?;
        let mut b = vec![0; n + 2];
        local.read(0, &mut b)?;
        assert_eq!((b[0], b[n + 1]), (0xa1, 0xa1));
        for i in 0..n {
            assert_eq!(b[i + 1], pattern(i, false));
            b[i + 1] = pattern(i, true);
        }
        local.write(0, &b)?;
        let report = engine.submit_and_wait(TransferOp::Write, &peer, &[req], WAIT)?;
        assert!(report.is_complete() && report.locally_drained);
        let req = TransferRequest::new(Transport::Tcp, local.base_address() + 1, readonly + 1, n);
        let report = engine.submit_and_wait(TransferOp::Write, &peer, &[req], WAIT)?;
        assert!(!report.is_complete());
        assert_eq!(
            report.failure.ok_or("missing rejection")?.remote_effect,
            RemoteEffect::Unknown
        );
        engine.close_segment(endpoint, epoch, WAIT)?;
    }
    engine.unregister_memory(&local)?;
    engine.close(WAIT)?;
    println!("PASS {}", if source { "source" } else { "requester" });
    Ok(())
}
