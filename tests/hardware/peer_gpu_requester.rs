//! Installed requester built without native features.
use orbit_te::{
    EngineConfig, MemoryType, Permission, TransferEngine, TransferOp, TransferRequest, Transport,
};
use std::{error::Error, time::Duration};
fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let first = args.next().ok_or("expected endpoint")?;
    if first == "--protocol" {
        println!("orbitte-peer-gpu-requester");
        return Ok(());
    }
    let wait = Duration::from_secs(30);
    let engine = TransferEngine::new(EngineConfig::default())?;
    let peer = engine.open_segment(
        first.parse()?,
        args.next().ok_or("expected epoch")?.parse()?,
        wait,
    )?;
    let region = peer.regions().first().ok_or("no GPU range")?;
    if region.memory_type() != MemoryType::Cuda {
        return Err("expected a peer GPU".into());
    }
    let host = engine.register_memory(vec![0xee; 12].into(), Permission::LocalOnly)?;
    let req = TransferRequest::new(
        Transport::Unspec,
        host.base_address() + 1,
        region.base_address() + 2,
        8,
    );
    for op in [TransferOp::Read, TransferOp::Write] {
        let report = engine.submit_and_wait(op, &peer, std::slice::from_ref(&req), wait)?;
        if !report.is_complete() || !report.locally_drained || report.transferred_bytes != 8 {
            return Err("incomplete TCP transfer".into());
        }
        let mut got = [0; 12];
        host.read(0, &mut got)?;
        let mut expected = [0xee; 12];
        expected[1..9].copy_from_slice(if op == TransferOp::Read {
            &[1, 2, 3, 4, 5, 6, 7, 8]
        } else {
            &[8, 7, 6, 5, 4, 3, 2, 1]
        });
        if got != expected {
            return Err("host payload or guards".into());
        }
        if op == TransferOp::Read {
            host.write(1, &[8, 7, 6, 5, 4, 3, 2, 1])?;
        }
    }
    engine.close(wait)?;
    Ok(())
}
