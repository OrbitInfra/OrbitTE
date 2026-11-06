//! Frozen TENT C transfer-engine boundary. See the installed header and C API guide.
#![deny(unsafe_code, unsafe_op_in_unsafe_fn)]
#[cfg(not(panic = "unwind"))]
compile_error!("libtent requires panic unwinding for its C entry-point barriers");
mod abi;
#[cfg(all(feature = "device-plugin", not(target_os = "linux")))]
compile_error!("OrbiTE device-plugin features require a Linux target");
mod batch;
#[allow(unsafe_code)]
mod boundary;
#[cfg(all(feature = "device-plugin", target_os = "linux"))]
#[allow(unsafe_code)]
mod device_plugin;
mod engine;
#[allow(unsafe_code)]
mod memory;
#[cfg(feature = "python")]
#[allow(unsafe_code)]
mod python;
mod segments;

// Only the ABI entry points and raw-storage adapters admit unsafe operations.
#[allow(unsafe_code)]
mod cpp_v1;
#[allow(unsafe_code)]
mod exports;

#[cfg(test)]
#[allow(unsafe_code)]
#[path = "../tests/unit/contract.rs"]
mod tests;
