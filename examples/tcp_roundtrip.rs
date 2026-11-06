use std::time::Duration;

use orbite::{EngineConfig, Permission, TransferEngine, TransferOp, TransferRequest};

fn main() -> orbite::Result<()> {
    let source = TransferEngine::new(EngineConfig::default())?;
    let exported = source.register_memory(
        b"hello from OrbiTE".to_vec().into_boxed_slice(),
        Permission::RemoteRead,
    )?;
    let requester = TransferEngine::new(EngineConfig::default())?;
    let destination = requester.register_memory(
        vec![0; exported.len()].into_boxed_slice(),
        Permission::LocalOnly,
    )?;
    let peer = requester.open_segment(
        source.endpoint(),
        source.source_epoch(),
        Duration::from_secs(1),
    )?;
    let remote = *peer
        .regions()
        .iter()
        .find(|region| region.id() == exported.id())
        .ok_or(orbite::Error::RegionNotFound)?;
    let request = TransferRequest::new(
        orbite::Transport::Tcp,
        destination.base_address(),
        remote.base_address(),
        exported.len(),
    );
    let report =
        requester.submit_and_wait(TransferOp::Read, &peer, &[request], Duration::from_secs(1))?;
    if let Some(failure) = report.failure {
        return Err(failure.error);
    }
    let mut bytes = vec![0; destination.len()];
    destination.read(0, &mut bytes)?;
    println!(
        "{} bytes: {}",
        report.transferred_bytes,
        String::from_utf8_lossy(&bytes)
    );
    source.close_source(Duration::from_secs(1))?;
    source.unregister_memory(&exported)?;
    Ok(())
}
