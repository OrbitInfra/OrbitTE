//! Frozen TENT C transfer-engine boundary. See the installed header and C API guide.
#![deny(unsafe_code, unsafe_op_in_unsafe_fn)]
#[cfg(not(panic = "unwind"))]
compile_error!("libtent requires panic unwinding for its C entry-point barriers");
mod abi;
mod batch;
#[allow(unsafe_code)]
mod boundary;
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
