use std::sync::Arc;
use std::time::Duration;

use orbit_te::{BatchState, EngineConfig, Permission, TransferEngine, TransferOp, TransferRequest};

fn main() -> orbit_te::Result<()> {
    let timeout = Duration::from_secs(2);
    let source = TransferEngine::new(EngineConfig::default())?;
    let exported = source.register_memory(
        (0u8..32).collect::<Vec<_>>().into_boxed_slice(),
        Permission::RemoteRead,
    )?;
    let requester = Arc::new(TransferEngine::new(EngineConfig::default())?);
    let destination =
        requester.register_memory(vec![0xee; 32].into_boxed_slice(), Permission::LocalOnly)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), timeout)?;
    let mut batch = requester.allocate_batch(2)?;
    for (local_offset, remote_offset) in [(0, 8), (16, 0)] {
        batch.submit(
            TransferOp::Read,
            &peer,
            &[TransferRequest {
                remote_staging: false,
                transport: orbit_te::Transport::Tcp,
                local_address: destination.base_address() + local_offset,
                remote_address: exported.base_address() + remote_offset,
                length: 8,
            }],
            None,
            timeout,
        )?;
    }
    batch.seal()?;
    let status = batch.wait(timeout)?;
    assert_eq!(status.state, BatchState::Completed);
    let mut bytes = [0; 32];
    destination.read(0, &mut bytes)?;
    assert_eq!(&bytes[..8], &(8u8..16).collect::<Vec<_>>());
    assert_eq!(&bytes[8..16], &[0xee; 8]);
    assert_eq!(&bytes[16..24], &(0u8..8).collect::<Vec<_>>());
    assert_eq!(&bytes[24..], &[0xee; 8]);
    println!(
        "batch {}: {} tasks, {} bytes",
        batch.id()?.as_u64(),
        status.completed_tasks,
        status.transferred_bytes
    );
    batch.free()?;
    requester.close_requester(timeout)?;
    source.close_source(timeout)?;
    Ok(())
}
