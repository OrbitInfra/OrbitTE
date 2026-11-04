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
//! let request = TransferRequest::new(
//!     orbit_te::Transport::Tcp,
//!     destination.base_address(),
//!     remote.base_address(),
//!     5,
//! );
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

#[cfg(unsupported_native_target)]
compile_error!("OrbitTE's `cuda`, `rdma`, `io-uring` and `gds` features require a Linux target");

#[cfg(not(unsupported_native_target))]
mod batch;
#[cfg(not(unsupported_native_target))]
mod config;
#[cfg(not(unsupported_native_target))]
mod engine;
#[cfg(not(unsupported_native_target))]
mod error;
#[cfg(not(unsupported_native_target))]
mod memory;
#[cfg(not(unsupported_native_target))]
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(not(unsupported_native_target))]
mod notification;
#[cfg(all(target_os = "linux", any(feature = "rdma", feature = "cuda")))]
mod platform;
#[cfg(not(unsupported_native_target))]
mod runtime;
#[cfg(not(unsupported_native_target))]
mod segment;
#[cfg(not(unsupported_native_target))]
pub mod transport;

#[cfg(all(target_os = "linux", feature = "gds"))]
pub use transport::gds::{GdsMode, GdsStats};
#[cfg(all(target_os = "linux", feature = "cuda"))]
pub mod cuda;
#[cfg(all(target_os = "linux", feature = "rdma"))]
pub use runtime::topology::{
    DEVICE_PRIORITY_RANKS, MemEntry, MemType, NicEntry, NicType, Topology,
};
#[cfg(not(unsupported_native_target))]
pub use transport::{hp_tcp::HpTcpConfig, shm::ShmConfig};
#[cfg(all(target_os = "linux", feature = "cuda"))]
pub use {
    config::StagingLocation, runtime::proxy_manager::StagingStats, transport::nvlink::NVLinkStats,
};

#[cfg(not(unsupported_native_target))]
pub use {
    batch::{Batch, BatchId, BatchState, BatchStatus, SubmissionStatus, TaskState, TaskStatus},
    config::{EngineConfig, RuntimeQueueConfig},
    engine::{
        BatchReport, EngineStats, Priority, RemoteEffect, TransferEngine, TransferFailure,
        TransferOp, TransferRequest, Transport,
    },
    error::{Error, Result},
    memory::{HostMemory, MemoryLocation, MemoryType, Permission, RangeLocation, RegisteredMemory},
    notification::{Notification, NotificationEffect, NotificationFailure, NotificationStatus},
    runtime::{
        admission_queue::RuntimeQueueStats,
        progress_worker::{next_poll_delay, wait_before_next_poll},
        qos_contract::{
            EffectiveQosPolicy, QosConfig, QosDegradedActions, QosPolicyFields, QosRequestContext,
            QosTenantContract, QosUsage,
        },
        receiver_credit::{
            CreditAmount, CreditBalance, CreditResource, CreditUpdateDisposition,
            ReceiverCreditConfig, ReceiverCreditIssuerStats, ReceiverCreditStats,
            ReceiverCreditUpdateV1,
        },
        transport_selector::{
            IntentType, PolicyName, QpPool, SegmentType, SelectionPolicy, TransportSelectorConfig,
        },
    },
    segment::{
        FileSegment, FileSegmentInfo, RemoteRegion, RemoteSegment, SourceEpoch, TransferTarget,
    },
};

#[cfg(all(test, not(unsupported_native_target)))]
#[path = "../tests/common/ports.rs"]
mod test_ports;
#[cfg(all(test, not(unsupported_native_target)))]
#[path = "../tests/common/mod.rs"]
mod test_support;
