<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/_static/orbitte-logo-dark.svg">
    <img src="docs/_static/orbitte-logo.svg" width="60%" alt="OrbitTE">
  </picture>
  <h2>A Rust-native transfer engine for AI infrastructure</h2>
  <a href="https://orbitinfra.github.io/OrbitTE/"><strong>Documentation</strong></a>
  | <a href="https://orbitinfra.github.io/OrbitTE/getting-started.html"><strong>Quick Start</strong></a>
  | <a href="https://orbitinfra.github.io/OrbitTE/architecture.html"><strong>Architecture</strong></a>
  | <a href="docs/tent-parity.md"><strong>TENT Reference</strong></a>
  <br><br>

  [![CI](https://github.com/OrbitInfra/OrbitTE/actions/workflows/ci.yml/badge.svg)](https://github.com/OrbitInfra/OrbitTE/actions/workflows/ci.yml)
  [![Crates.io](https://img.shields.io/crates/v/orbit-te.svg)](https://crates.io/crates/orbit-te)
  [![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](Cargo.toml)
  [![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
</div>

## Overview

OrbitTE moves bytes between CPU memory and NVIDIA GPU allocations, within and
across machines. It follows Mooncake TENT's engine, topology, context, buffer,
endpoint and worker architecture, with resource ownership and transport state
machines implemented in Rust.

The runtime has no Mooncake C++ dependency. TCP and RDMA are separate payload
transports; a shared control service handles discovery, grants and receipts.
Applications retain cache identity, recovery policy and replica ownership.

**Under development.** TCP, host RDMA and synchronized NVIDIA host staging are
implemented with scoped validation. Two physical RNICs per host pass a declared
CPU DRAM/RoCEv2 profile for cross-rail READ/WRITE, concurrent parents, aggregate
quotas, whole-parent source RQ admission, connected reuse, registration rollback
and native fault/drain ownership. The extended profile includes eight parents,
up to 5 MiB each, folded tails, 32/40 active slice grants and connected QP reuse. Native QP ERR, lost SQ/RQ
completions, source/requester death and repeated fresh-process restart pass
scoped checks with separate endpoint destruction deadlines. Real CQ/PD cleanup
errors preserve the affected native owners through caller Drop. Completely
drained progress owners recover for later requests; MR/MW syscall-failure tests
keep the old owners quarantined while fresh engines recover in live processes.
Per-context CQ lanes and load-based slice admission pass scoped 1/2/3/6-lane
checks; context PD/MR owners and aggregate limits remain shared.
Peer QP arrays, verified NUMA worker binding and a unified event monitor are
validated in the declared two-host CPU DRAM/RoCEv2 profile. Fixed-width QP
multiplexing, physical
port/device loss, independent P4 acceptance, full TENT parity, GPUDirect RDMA
and optional SSD transports remain open.
The published `orbit-te` alpha reserves the package identity; use the source
checkout for the implementation described here.

## Highlights

- **Owned memory.** CPU and NVIDIA allocations retain their actual owners;
  direct host DMA leases exclude conflicting access across aliases.
- **Batched transfers.** READ/WRITE, asynchronous batches, cancellation and
  notifications share bounded admission and explicit completion contracts.
- **Persistent RDMA resources.** Context-specific MRs, connected QP reuse,
  per-context CQ lanes and a transport-level worker pool keep resources with
  their physical owners. `num_lanes` aligns workers and CQ parallelism; new
  slices use worker load while cached QPs retain their CQ lane. Host Direct registration prepares all enabled contexts
  before exposing a range. Peer endpoint arrays, physical QPs and per-slice
  grants have separate owners; data workers bind to allowed NUMA CPU sets and
  one transport monitor consumes device/CQ events.
- **Topology-aware device selection.** Stable NIC IDs, device masks, NUMA
  priorities and in-flight byte charges select compatible paths per RDMA slice.
  Parent completion requires every slice and its source receipt.
- **Shared quotas.** Concurrent parents share SQ/RQ, byte and connection limits.
  Source admission reserves every slice's receive credit together before
  granting authority. Returning completion capacity wakes the worker assigned to each waiting CQ lane.
- **Explicit drain.** Timeouts and logical failures do not release memory still
  owned by native work. Failed cleanup retains its resources and charges.
  Completely drained progress owners can be replaced for later requests while
  preserving the context's PD and persistent MRs. Scoped live-process recovery
  and MR/MW syscall-failure retention checks pass; full P4 acceptance remains open.

<div align="center">
  <img src="docs/_static/architecture.svg" width="95%" alt="OrbitTE engine, control service and sibling TCP/RDMA transports; planned paths are dashed">
</div>

See [capabilities](docs/capabilities.md) for current behavior and
[qualification](docs/qualification.md) for the evidence and its limits.

## Quick Start

On Linux with Rust 1.85 or later:

```sh
git clone https://github.com/OrbitInfra/OrbitTE.git
cd OrbitTE
cargo run --locked --example tcp_roundtrip
```

Expected output:

```text
18 bytes: hello from OrbitTE
```

The CPU/TCP example needs no CUDA or RDMA SDK. Optional `rdma` and `cuda`
features enable the native providers; device transfers currently use declared
host staging. See the [build guide](docs/build.md) and
[RDMA/CUDA guide](docs/rdma-cuda.md).

## Documentation

| Start here | Reference and development |
| --- | --- |
| [Quick Start](docs/getting-started.md) | [Rust API guide](docs/rust-api.md) |
| [Build Guide](docs/build.md) | [TCP and ownership contracts](docs/cpu-tcp.md) |
| [Architecture](docs/architecture.md) | [Asynchronous batches](docs/async-batches.md) |
| [Capabilities](docs/capabilities.md) | [RDMA and CUDA](docs/rdma-cuda.md) |
| [Troubleshooting](docs/troubleshooting.md) | [Qualification](docs/qualification.md) |
| [Contributing](docs/contributing.md) | [Frozen TENT parity contract](docs/tent-parity.md) |

The [completion plan](docs/completion-plan.md) is the only execution queue.
OrbitTE's native P4 stage comes before renewed OrbitKV integration. Historical
implementation details remain in the [RDMA contract](docs/rdma-cuda.md) and the
[earlier README](https://github.com/OrbitInfra/OrbitTE/blob/0ec2a88ba01511f40445d498249e947486034aa4/README.md#status).

## Development

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

The documentation site uses the same Sphinx Book Theme as Mooncake, with Orbit
branding. Its pages render directly from `docs/`; see
[building the website](docs/contributing.md#documentation-site).

## License and Provenance

OrbitTE is an independent [OrbitInfra](https://github.com/OrbitInfra) project,
licensed under [Apache-2.0](LICENSE).
[Mooncake TENT](https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent)
is the frozen capability and architecture reference. Rust API, native ABI and
wire compatibility are tracked separately in the parity contract.
