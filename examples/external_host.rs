use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orbit_te::{
    EngineConfig, Error, HostMemory, Permission, TransferEngine, TransferOp, TransferRequest,
};

fn main() -> orbit_te::Result<()> {
    let allocation = Arc::new(Mutex::new(vec![0; 64].into_boxed_slice()));
    let address = {
        let mut bytes = allocation
            .lock()
            .map_err(|_| Error::Internal("allocation lock poisoned"))?;
        NonNull::new(bytes.as_mut_ptr()).ok_or(Error::InvalidRange)?
    };
    let owner: Arc<dyn Send + Sync> = allocation.clone();
    // SAFETY: the box is stable and owner retains it. All byte accesses use this
    // HostMemory until source drain below; no other code can reallocate it.
    let memory = Arc::new(unsafe { HostMemory::from_raw_parts(address, 64, owner)? });
    memory.write(16, b"orbit")?;
    let source = TransferEngine::new(EngineConfig::default())?;
    let exported = source.register_host_memory_batch(
        &[(Arc::clone(&memory), 0, 32), (Arc::clone(&memory), 32, 32)],
        Permission::RemoteRead,
    )?;
    let requester = TransferEngine::new(EngineConfig::default())?;
    let destination =
        requester.register_memory(vec![0; 5].into_boxed_slice(), Permission::LocalOnly)?;
    let peer = requester.open_segment(
        source.endpoint(),
        source.source_epoch(),
        Duration::from_secs(1),
    )?;
    let transfer = TransferRequest {
        local_address: destination.base_address(),
        remote_address: memory.base_address() + 16,
        length: 5,
    };
    let report =
        requester.submit_and_wait(TransferOp::Read, &peer, &[transfer], Duration::from_secs(1))?;
    if let Some(failure) = report.failure {
        return Err(failure.error);
    }
    let mut observed = [0; 5];
    destination.read(0, &mut observed)?;
    assert_eq!(&observed, b"orbit");
    source.close_source(Duration::from_secs(1))?;
    for registration in &exported {
        source.unregister_memory(registration)?;
    }
    drop(exported);
    drop(memory);
    let bytes = allocation
        .lock()
        .map_err(|_| Error::Internal("allocation lock poisoned"))?;
    assert_eq!(&bytes[16..21], b"orbit");
    println!(
        "{} bytes from external host allocation",
        report.transferred_bytes
    );
    Ok(())
}
