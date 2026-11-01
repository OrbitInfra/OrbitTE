//! Owned CPU/NVIDIA memory, fenced TCP/direct or staged RDMA transfers and notifications.
//!
//! This first implementation follows TENT's engine, segment/control-plane and
//! transport ownership boundaries. Its versioned development protocol is
//! OrbitTE-only; full hardware coverage and TENT wire/ABI compatibility remain open.
//! See the repository's completion plan for qualification and integration gates.
//!
//! ```
//! use std::time::Duration;
//! use orbit_te::{EngineConfig, Permission, TransferEngine, TransferOp, TransferRequest};
//!
//! # fn main() -> orbit_te::Result<()> {
//! let source = TransferEngine::new(EngineConfig::default())?;
//! let exported = source.register_memory(b"orbit".to_vec().into_boxed_slice(), Permission::RemoteRead)?;
//! let requester = TransferEngine::new(EngineConfig::default())?;
//! let destination = requester.register_memory(vec![0; 5].into_boxed_slice(), Permission::LocalOnly)?;
//! // Obtain this epoch from the authoritative source owner, not untrusted discovery.
//! let peer = requester.open_segment(source.endpoint(), source.source_epoch(), Duration::from_secs(1))?;
//! let remote = peer.regions().iter().find(|r| r.id() == exported.id()).ok_or(orbit_te::Error::RegionNotFound)?;
//! let request = TransferRequest {
//!     remote_staging: false,
//!     transport: orbit_te::Transport::Tcp,
//!     local_address: destination.base_address(),
//!     remote_address: remote.base_address(), length: 5,
//! };
//! let report = requester.submit_and_wait(TransferOp::Read, &peer, &[request], Duration::from_secs(1))?;
//! assert!(report.is_complete());
//! let mut bytes = [0; 5];
//! destination.read(0, &mut bytes)?;
//! assert_eq!(&bytes, b"orbit");
//! source.close_source(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//! ```

#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

mod batch;
mod config;
mod engine;
mod error;
mod memory;
mod notification;
#[cfg(any(feature = "rdma", feature = "cuda"))]
mod platform;
mod runtime;
mod segment;
pub mod transport;

pub use batch::{Batch, BatchId, BatchState, BatchStatus, SubmissionStatus, TaskState, TaskStatus};
#[cfg(feature = "cuda")]
pub mod cuda;

#[cfg(feature = "cuda")]
pub use runtime::proxy_manager::StagingStats;
#[cfg(feature = "cuda")]
pub use transport::nvlink::NVLinkStats;

pub use config::EngineConfig;
#[cfg(feature = "cuda")]
pub use config::StagingLocation;
pub use engine::{
    BatchReport, EngineStats, RemoteEffect, TransferEngine, TransferFailure, TransferOp,
    TransferRequest, Transport,
};
pub use error::{Error, Result};
pub use memory::{
    HostMemory, MemoryLocation, MemoryType, Permission, RangeLocation, RegisteredMemory,
};
pub use notification::{Notification, NotificationEffect, NotificationFailure, NotificationStatus};
pub use segment::{RemoteRegion, RemoteSegment, SourceEpoch};

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod test_support;

#[cfg(feature = "rdma")]
pub use runtime::topology::{
    DEVICE_PRIORITY_RANKS, MemEntry, MemType, NicEntry, NicType, Topology,
};
