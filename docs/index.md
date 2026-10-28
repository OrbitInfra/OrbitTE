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

OrbitTE provides owned, batched data movement across CPU memory, NVIDIA host
staging and network transports. Its design follows Mooncake TENT's resource
and runtime boundaries, implemented in Rust without a Mooncake C++ dependency.

The engine owns registration, batches and completion. TCP, RDMA and opt-in CUDA IPC are separate
payload transports, with a shared control service for peer discovery, grants
and receipts. Allocations remain owned until native work has drained.

:::{image} _static/architecture.svg
:alt: OrbitTE engine and sibling TCP/RDMA transports; dashed paths are planned
:class: dark-light
:::

## Getting started

Run the [CPU example](getting-started.md), select optional providers with the
[build guide](build.md), and use the [Rust API guide](rust-api.md) to register
memory and submit transfers. The [RDMA/CUDA guide](rdma-cuda.md) explains device
selection, persistent resources, completion and shutdown.

## Development status

CPU/TCP, persistent host RDMA, multirail slicing and synchronized NVIDIA host
staging with owned copy streams/events, and explicit GPU direct RDMA are implemented. Native qualification applies to the exact profiles
in [qualification](qualification.md). The published alpha reserves the crate
name; the source implementation remains under development.

The host RDMA implementation and declared installed cleanup fixtures are complete.
[Physical rail accounting](qualification.md#installed-physical-rail-accounting)
verifies concurrent parents, shared QP limits and exact native payload bytes.
[Automatic discovery](qualification.md#installed-automatic-discovery) now passes
two-host READ/WRITE against a separately captured PCI, NUMA and route inventory.
The [P4 exit queue](completion-plan.md#p4-remaining-delivery) tracks physical-fault,
fabric qualification before full acceptance. Source and the bounded RoCE profile
passed independent review with four fresh live repeats; P5 CUDA work now proceeds
with available hardware. The [GPU Direct profile](qualification.md#gpudirect-rdma)
passes 72 installed H20/RoCE cases with an explicit host-wide context inventory
limit. The cooperative [CUDA IPC/NVLink path](cuda-ipc.md) is implemented, with
48 installed H20 cases passing. Bounded subbatch import reuse is implemented;
its additional qualification is tracked separately. OrbitKV integration remains deferred.
[Capabilities](capabilities.md) distinguishes current paths from later work,
including broader GPU/IPC qualification and full TENT compatibility. For code
navigation, use the [architecture reading order](architecture.md#source-reading-order).

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
async-batches
rdma-cuda
cuda-ipc
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
