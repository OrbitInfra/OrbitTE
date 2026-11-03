# Welcome to OrbitTE

:::{raw} html
<div class="orbit-hero">
  <img class="orbit-logo-light dark-light" src="_static/orbitte-logo.svg" alt="OrbitTE" width="600" height="144">
  <img class="orbit-logo-dark dark-light" src="_static/orbitte-logo-dark.svg" alt="OrbitTE" width="600" height="144">
</div>
<p class="orbit-tagline"><strong>A Rust-native transfer engine for AI infrastructure.</strong></p>
<p class="orbit-links">
  <a href="https://github.com/OrbitInfra/OrbitTE">GitHub</a> ·
  <a href="getting-started.html">Quick Start</a> ·
  <a href="architecture.html">Architecture</a>
</p>
:::

## Overview

OrbitTE is the transfer engine for OrbitKV's KV cache, replacing its earlier
Mooncake TENT dependency. It moves bytes between CPU memory, NVIDIA GPUs and
local SSDs, within and across machines, so every end-to-end cache path,
including GPU↔DRAM and GPU↔SSD, runs through one engine. The design follows
Mooncake TENT's engine, transport, context, buffer, endpoint and worker
boundaries, implemented in Rust with no Mooncake C++ dependency.

:::{image} _static/architecture.svg
:alt: OrbitTE engine and sibling TCP/RDMA transports; dashed paths are planned
:class: dark-light
:::

## Updates

- **Nov 3, 2026**: TENT-style transport selection picks NVLink, RDMA or TCP
  per request; local SSD paths through io_uring and GDS become core targets.
- **Nov 2, 2026**: FIFO/EDF admission windows, TENT-format QoS contracts and
  receiver credit windows.
- **Nov 1, 2026**: Bandwidth-aware multirail selection from CUDA/NUMA topology
  and TENT priority matrices; CUDA 13 native batch copies for NVLink groups.
- **Oct 28, 2026**: Bounded GPUDirect RDMA, CUDA IPC and remote CUDA staging
  profiles pass installed H20/RoCE tests.
- **Oct 27, 2026**: The bounded host RDMA RoCE profile is independently accepted.

## Status

OrbitTE is in alpha. TCP, RDMA, CUDA IPC and local file segment handles are
implemented. Storage transports, HP-TCP, SHM, MNNVL and the optional network
providers are planned.
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
storage
transport-selection
async-batches
rdma-cuda
cuda-ipc
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
completion-plan
:::
