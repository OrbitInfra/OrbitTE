//! Same-host shared mappings. Describe carries allocation identity and offsets;
//! the source retains authority for each individual registration.
use std::path::PathBuf;

#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShmConfig {
    pub enable: bool,
    /// Empty selects POSIX shared memory. Otherwise both peers specify their local mount.
    pub cxl_mount_path: PathBuf,
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod mapping;
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod sys;
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod transport;
pub(crate) mod wire;
#[cfg(target_os = "linux")]
pub(crate) use mapping::{Local, RangeLock};
#[cfg(target_os = "linux")]
pub(crate) use transport::{Lease, ShmTransport};

#[cfg(all(test, target_os = "linux"))]
#[path = "../../../tests/unit/transport/shm/mod.rs"]
mod tests;
