#[cfg(feature = "gds")]
pub(crate) mod gds;
#[cfg(feature = "io-uring")]
pub(crate) mod io_uring;
#[cfg(feature = "cuda")]
pub(crate) mod nvlink;
#[cfg(feature = "rdma")]
pub mod rdma;
pub(crate) mod tcp;

#[cfg(any(feature = "rdma", feature = "io-uring", feature = "gds"))]
const DESTRUCTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
