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

#[cfg(test)]
#[path = "../../../tests/unit/transport/fabric.rs"]
mod tests;
