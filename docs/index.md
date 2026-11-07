# Welcome to OrbiTE

:::{raw} html
<div class="orbit-hero">
  <img class="orbit-logo-light dark-light" src="_static/orbite-logo.svg" alt="OrbiTE" width="600" height="144">
  <img class="orbit-logo-dark dark-light" src="_static/orbite-logo-dark.svg" alt="OrbiTE" width="600" height="144">
</div>
<p class="orbit-tagline"><strong>A Rust-native transfer engine for AI infrastructure.</strong></p>
<p class="orbit-links">
  <a href="https://github.com/OrbitInfra/OrbiTE">GitHub</a> ·
  <a href="getting-started.html">Quick Start</a> ·
  <a href="architecture.html">Architecture</a>
</p>
:::

## Overview

OrbiTE is the transfer engine for OrbitKV's KV cache, replacing its earlier
Mooncake TENT dependency. It moves bytes between CPU memory, NVIDIA GPUs and
local SSDs, within and across machines, so every end-to-end cache path,
including GPU↔DRAM and GPU↔SSD, runs through one engine. The design follows
Mooncake TENT's engine, transport, context, buffer, endpoint and worker
boundaries, implemented in Rust with no Mooncake C++ dependency.

:::{image} _static/architecture.svg
:alt: OrbiTE network transports and the implemented DRAM and GPU paths to local SSD
:class: dark-light
:::

## Updates

- **Nov 3, 2026**: Local file segments support DRAM transfers through io_uring,
  CUDA staging through pinned DRAM, and GDS through cuFile. The DRAM path is
  qualified on ext4 and tmpfs in CI. [Local storage](storage.md)
- **Nov 3, 2026**: TENT-style transport selection picks NVLink, RDMA or TCP
  per request.
- **Nov 2, 2026**: FIFO/EDF admission windows, TENT-format QoS contracts and
  receiver credit windows.
- **Nov 1, 2026**: Bandwidth-aware multirail selection from CUDA/NUMA topology
  and TENT priority matrices; CUDA 13 native batch copies for NVLink groups.
- **Oct 28, 2026**: Bounded GPUDirect RDMA, CUDA IPC and remote CUDA staging
  profiles pass installed H20/RoCE tests.
- **Oct 27, 2026**: The bounded host RDMA RoCE profile is independently accepted.

## Status

OrbiTE is in alpha. TCP, RDMA, CUDA IPC and local file segment handles are
implemented, along with Linux io_uring and GDS file transfers. HP-TCP persistent lanes and same-host SHM are implemented. MNNVL and EGM are implemented with CUDA shim coverage. Their installed fabric profiles await qualification. Libfabric host DRAM is implemented with real software-provider controls; EFA is unqualified and MPComm is deferred: no consumer; provider unavailable.
[Capabilities](capabilities.md) lists each path and its limits,
[qualification](qualification.md) records the evidence, and the
[completion plan](completion-plan.md) is the work queue.

## Documentation

:::{toctree}
:caption: Getting Started
:maxdepth: 1

getting-started
build
capabilities
:::

:::{toctree}
:caption: User Guides
:maxdepth: 1

cpu-tcp
hp-tcp
fabric
shm
storage
transport-selection
async-batches
metrics
diagnostics
rdma-cuda
cuda-ipc
local-copy
staging
:::

:::{toctree}
:caption: Design Documents
:maxdepth: 1

architecture
tent-parity
:::

:::{toctree}
:caption: API Reference
:maxdepth: 1

rust-api
python
:::

:::{toctree}
:caption: Troubleshooting
:maxdepth: 1

troubleshooting
:::

:::{toctree}
:caption: Development and Validation
:maxdepth: 1

contributing
qualification
installed-host
native-cleanup
cpu-oracle
benchmarks
completion-plan
:::
