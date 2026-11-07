//! Optional host RMA through libfabric, following TENT's domain workers.
#[cfg(feature = "fabric")]
mod config;
#[cfg(any(feature = "fabric", test))]
pub(crate) mod fabric_attrs;
#[cfg(feature = "fabric")]
mod fabric_context;
#[cfg(feature = "fabric")]
mod fabric_transport;
#[cfg(feature = "fabric")]
mod sys;
#[cfg(feature = "fabric")]
pub use config::{FabricConfig, FabricProvider};
#[cfg(feature = "fabric")]
pub use fabric_context::FabricStats;
#[cfg(feature = "fabric")]
pub(crate) use fabric_transport::FabricTransport;

// Checked IDs must never wrap and reuse a live native key or grant nonce.
#[cfg(feature = "fabric")]
fn next_id(counter: &std::sync::atomic::AtomicU64) -> crate::Result<u64> {
    use std::sync::atomic::Ordering;
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current
            .checked_add(1)
            .ok_or(crate::Error::ResourceExhausted)?;
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Ok(current),
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/fabric.rs"]
mod tests;
