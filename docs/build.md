# Build Guide

OrbitTE builds as a Rust library. Provider features are optional and independent.
The supported source baseline is Linux and Rust 1.85 or later.

## CPU and TCP

```sh
cargo build --locked
cargo test --locked
cargo run --locked --example tcp_roundtrip
```

No CUDA or RDMA SDK is required. The runtime does not build or load Mooncake's
C++ engine. Keep `Cargo.lock` when reproducing a source build.

## Host RDMA

On Debian or Ubuntu, install the standard native build prerequisites:

```sh
sudo apt-get install g++ cmake pkg-config clang-18 libclang-18-dev libibverbs-dev
CC=gcc CXX=g++ LIBCLANG_PATH=/usr/lib/llvm-18/lib \
  cargo build --locked --features rdma
```

`ibverbs-sys` generates its bindings and links against the host's system
`libibverbs.so.1`. Each execution node also needs its RNIC provider, accessible
uverbs character nodes and a valid GID. A usable context can register memory
while its port is paused; actual transfers require an active, compatible route.

With a configured RDMA engine and host Direct policy, host registration opens
the usable contexts, including paused ports, and prepares all physical MR copies
before returning a visible range. Native/device or MR-budget errors can therefore
occur during registration. Each local-only view uses one full-view MR per context;
remotely accessible views use two per context (local-only and MW-bind parent).
Published copies are not evicted while their registration is active. Size
`max_cached_mrs` and `max_cached_mr_bytes` for this fan-out. Staged host policy
and CUDA device registration do not eagerly register application backing.

The current CI baseline is Ubuntu 24.04 with Clang 18. A different libclang can
change generated bindings; use the declared baseline when reproducing a
binding-layout failure. See [troubleshooting](troubleshooting.md).

## NVIDIA allocations and host staging

```sh
cargo build --locked --features cuda
cargo build --locked --features rdma,cuda
```

The CUDA feature uses generated `cudarc` Driver API types and selective dynamic
loading. The generated binding baseline is CUDA 13.0; it does not require a
CUDA toolkit or eagerly load every CUDA 13 symbol. Runtime calls need a working
NVIDIA driver. Native NVLink batch dispatch requires reported Driver API version
13000 or later and the exact `cuMemcpyBatchAsync_v2` symbol. Without that optional
capability, groups keep the existing ordered-copy path; required driver symbols
still return an explicit error if unavailable. Device and pinned-host
owners, synchronized copies and host staging are implemented. GPUDirect RDMA,
CUDA IPC and NVLink transfer paths remain separate qualification work.

Compiling these features does not establish hardware execution. See
[qualification](qualification.md) and the [RDMA/CUDA contract](rdma-cuda.md).

## Run source checks

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

With native build prerequisites installed, run the same checks with
`--features rdma` or `--features rdma,cuda`. Native hardware tests are explicitly
ignored by default and require selected devices, original resource locks and
external artifact directories.

## Build the documentation website

The website is generated from this `docs/` tree using Mooncake's Sphinx Book
Theme. See [the contributor instructions](contributing.md#documentation-site)
for the exact build and preview commands.
