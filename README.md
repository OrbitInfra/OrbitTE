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
endpoint and worker architecture, with resource ownership and transfer state
machines implemented in Rust. The runtime has no Mooncake C++ dependency.

TCP and RDMA are separate payload transports. A shared control service handles
peer discovery, memory grants and receipts. Applications own cache identity,
recovery policy and replica placement.

<div align="center">
  <img src="docs/_static/architecture.svg" width="95%" alt="OrbitTE engine, control service and sibling TCP/RDMA transports; planned paths are dashed">
</div>

## Highlights

- **Owned memory and explicit completion.** Registrations retain their actual
  allocations; asynchronous READ/WRITE batches, cancellation and notifications
  share bounded admission. Failed native cleanup retains unresolved memory.
- **Persistent host RDMA.** Context-owned MRs, reusable QP arrays, shared CQ
  lanes and a transport worker pool serve concurrent requests. A periodic
  monitor fences failed routes and resumes drained contexts.
- **Topology-aware multirail transfers.** Device masks, NUMA priorities and
  in-flight byte accounting select paths for bounded request slices. Parent
  completion requires every slice and its source receipt.
- **Optional NVIDIA support.** Device and pinned-host allocations use the CUDA
  Driver API. Current device transfers use synchronized host staging.

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

The CPU/TCP example needs no CUDA or RDMA SDK. Select optional `rdma` and `cuda`
features with the [build guide](docs/build.md), then follow the
[Rust API guide](docs/rust-api.md) and [RDMA/CUDA guide](docs/rdma-cuda.md).

## Project Status

**Host RDMA is implemented; P4 acceptance is pending.** The packaged host API
passes the declared two-host RoCE concurrency, resource-cleanup, restart,
engine-replacement and in-flight DMA profiles. Native WR/CQE accounting verifies
shared QP limits and exact bytes on two physical rails. Remaining P4 acceptance
requires physical link/device-loss evidence, InfiniBand and independent review; see the fixed
[acceptance queue](docs/completion-plan.md#p4-remaining-delivery).

OrbitTE remains under development. GPUDirect RDMA, CUDA IPC/NVLink, optional SSD
transports and full TENT API/wire parity belong to later stages.
[Capabilities](docs/capabilities.md) describes supported paths;
[qualification](docs/qualification.md) records exact artifacts and limits.

The published `orbit-te` alpha reserves the package identity. Use the source
checkout for the implementation described here.

## Documentation

| Topic | Guides |
| --- | --- |
| Getting started | [Quick Start](docs/getting-started.md), [Build](docs/build.md), [Capabilities](docs/capabilities.md) |
| Using the engine | [TCP and memory ownership](docs/cpu-tcp.md), [Async batches](docs/async-batches.md), [RDMA and CUDA](docs/rdma-cuda.md) |
| Design and API | [Architecture](docs/architecture.md), [Rust API](docs/rust-api.md), [Frozen TENT reference](docs/tent-parity.md) |
| Development | [Contributing](docs/contributing.md), [Qualification](docs/qualification.md), [Completion plan](docs/completion-plan.md) |
| Operations | [Troubleshooting](docs/troubleshooting.md), [Installed host validation](docs/installed-host.md), [Native cleanup](docs/native-cleanup.md) |

Technical documentation lives in `docs/`. The website uses Mooncake's Sphinx
Book Theme with Orbit branding and deploys automatically from `main` through
GitHub Pages. See [building the website](docs/contributing.md#documentation-site).

## Contributing

Start with the [contributor guide](docs/contributing.md) and the
[source reading order](docs/architecture.md#source-reading-order). The completion
plan is the only execution queue; complete P4 before renewing OrbitKV integration.

## License and Provenance

OrbitTE is an independent [OrbitInfra](https://github.com/OrbitInfra) project,
licensed under [Apache-2.0](LICENSE).
[Mooncake TENT](https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent)
is the frozen capability and architecture reference. Rust API, native ABI and
wire compatibility are tracked separately in the parity contract.
