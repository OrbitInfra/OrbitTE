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

OrbitTE provides owned, batched data movement across CPU memory, NVIDIA host
staging and network transports. Its design follows Mooncake TENT's resource
and runtime boundaries, implemented in Rust.

## Overview

The engine owns registration, batches and completion. TCP and RDMA move payloads
through separate transports, with a shared control service for peer discovery,
grants and receipts. Native resources retain their allocation owners until
physical work has drained.

:::{image} _static/architecture.svg
:alt: OrbitTE engine and sibling TCP/RDMA transports; dashed paths are planned
:class: dark-light
:::

The source implements CPU/TCP, persistent host RDMA resources, cross-rail
request slicing, eager multi-context MR publication and synchronized NVIDIA
host staging. Qualification is scoped
to the profiles recorded in the [capability matrix](capabilities.md). The
published alpha reserves the crate name; the source implementation remains
under development.

## Getting started

Run the [CPU example](getting-started.md), select optional providers with the
[build guide](build.md), or read the [architecture](architecture.md) to understand
registration, transfer and completion ownership.

## Project updates

- **October 25, 2026 — larger requests and folded tails:** Eight concurrent
  parents exercise 5 MiB, MiB-plus-one and exact-quarter-tail plans, 32/40-QP
  reuse and whole-plan RQ rejection. Stable/Rust 1.85 pass 288 two-RNIC native
  cases, including 144 new geometry cases. Two-WR fault windows preserve the
  unresolved sibling until CQ drain; four native mutation runs reject early
  debt/credit return. Sixteen valid plans also match the isolated frozen TENT
  function. Production and wire v12 are unchanged. Full P4 and independent
  acceptance remain open.
- **October 25, 2026 — source RQ admission:** Wire v12 reserves each complete
  slice plan before constructing QPs. The declared two-RNIC profile passes 296
  native cases across stable/Rust 1.85, including 64 new four-parent/three-slice
  cases. Residual RQ capacity, twelve-QP reuse, paused close and either-rail CQ
  errors retain actual receive credits through completion/drain. One CPU and
  two native mutation controls reject broken admission or early credit return.
  Full P4 and independent acceptance remain open.
- **October 24, 2026:** Cross-context quota returns now wake waiting event
  workers. The declared two-RNIC CPU DRAM/RoCEv2 profile passes 232 native
  cases across stable/Rust 1.85, including 80 new concurrent-parent cases.
  Shared SQ/RQ pressure, byte/connection rejection, four-QP reuse, faults and
  concurrent unregister/close are validated. One CPU and four native negative
  controls reject missing wakeups, quota overruns or early WR-debt repayment.
  Broader physical faults and independent P4 acceptance remain open.
  Documentation publishes automatically to GitHub Pages from `main`.
- **Current focus:** Complete OrbitTE's native RDMA stage using the frozen TENT
  architecture. OrbitKV integration resumes after that stage is accepted.

See [qualification](qualification.md) for evidence and the
[completion plan](completion-plan.md) for the delivery sequence.

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
:caption: Development
:maxdepth: 1

contributing
qualification
cpu-oracle
completion-plan
:::
