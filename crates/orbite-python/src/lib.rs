//! Python packaging boundary. Its retained owners delegate transfers to the Rust core.
#![deny(unsafe_code, unsafe_op_in_unsafe_fn)]
#[cfg(not(panic = "unwind"))]
compile_error!("orbite-python requires panic unwinding for its Python entry-point barriers");
mod batch;
mod boundary;
mod engine;
#[allow(unsafe_code)]
mod memory;
#[allow(unsafe_code)]
mod python;
mod records;
mod segments;
#[allow(unsafe_code)]
mod support;
