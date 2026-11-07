# OrbiTE Python bindings

This unpublished Rust crate is the packaging boundary for `orbite._native`.
It retains Python buffer and CUDA exporters through the Rust engine's physical
drain. See [the Python guide](../../docs/python.md). It exports no C transfer
engine ABI, C++ adapter or device-plugin interface.
