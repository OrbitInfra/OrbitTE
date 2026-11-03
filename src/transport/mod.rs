#[cfg(feature = "io-uring")]
pub(crate) mod io_uring;
#[cfg(feature = "cuda")]
pub(crate) mod nvlink;
#[cfg(feature = "rdma")]
pub mod rdma;
pub(crate) mod tcp;

#[cfg(any(feature = "rdma", feature = "io-uring"))]
const DESTRUCTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
