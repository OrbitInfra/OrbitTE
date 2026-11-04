//! Compiled as an independent consumer whose only direct dependency is orbit-te.
use orbit_te::cuda::local_copy::{LocalCopyConfig, LocalCopyEngine, ProducerEvent};
use orbit_te::{RegisteredMemory, Result};
use std::ffi::c_void;
use std::sync::Arc;

/// # Safety
/// The owner retains the caller's stream in the device context, its recorded
/// producer generation and inputs. The caller coordinates all external access.
pub unsafe fn bind(
    device: &RegisteredMemory,
    stream: *mut c_void,
    producer: *mut c_void,
    owner: Arc<dyn Send + Sync>,
) -> Result<(LocalCopyEngine, ProducerEvent)> {
    // SAFETY: the caller retains both native handles through their last receipt.
    unsafe {
        Ok((
            LocalCopyEngine::bind(device, stream, owner.clone(), LocalCopyConfig::default())?,
            ProducerEvent::from_raw(producer, owner)?,
        ))
    }
}
