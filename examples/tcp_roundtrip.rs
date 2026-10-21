use std::time::Duration;

use orbit_te::{EngineConfig, Permission, TransferEngine, TransferOp, TransferRequest};

fn main() -> orbit_te::Result<()> {
    let source = TransferEngine::new(EngineConfig::default())?;
    let exported = source.register_memory(
        b"hello from OrbitTE".to_vec().into_boxed_slice(),
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
        .ok_or(orbit_te::Error::RegionNotFound)?;
    let request = TransferRequest {
        local_address: destination.base_address(),
        remote_address: remote.base_address(),
        length: exported.len(),
    };
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
