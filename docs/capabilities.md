# Capabilities

This page summarizes the current source on `main`. The `0.1.0-alpha.1` crate on
crates.io only reserves the package name.

| Status | Meaning |
| --- | --- |
| **Accepted** | A bounded installed hardware profile passed and an independent reviewer accepted it. Only that recorded profile is covered |
| **Qualified** | A bounded installed hardware profile passed; independent acceptance is still open |
| **Implemented** | Source and CPU controls pass; the current source has no installed hardware result |
| **Planned** | Not implemented yet; the stage is listed in the [completion plan](completion-plan.md) |

[Qualification](qualification.md) records the exact artifacts, cases and limits
behind every hardware result.

## Transports

| Path | Status | Limits |
| --- | --- | --- |
| CPU DRAM over TCP | Implemented | Loopback and multi-process byte, restart and lifetime controls; no physical two-host profile. See [CPU and TCP](cpu-tcp.md) |
| Host DRAM over RDMA | Accepted (RoCE) | Two-host RoCEv2 READ/WRITE with restart, cleanup and in-flight DMA faults. InfiniBand and physical link or device loss are unqualified. See [RDMA and CUDA](rdma-cuda.md) |
| GPUDirect RDMA | Accepted (H20/RoCE, with limits) | Host/GPU READ/WRITE and two-rail slices. Host-wide context non-growth is unqualified; GPU source MRs and QPs are per grant |
| CUDA IPC / NVLink on one host | Accepted (H20, cooperative profiles) | GPU/GPU, pinned host/GPU, subbatch reuse and a fixed-working-set import cache. Grouped completion, stream pool, native batch copy and cache reclamation are implemented only. See [CUDA IPC](cuda-ipc.md) |
| Local CUDA staging over TCP/RDMA | Accepted (H20, healthy) | Shared pinned chunks; physical CUDA faults are unqualified. See [staging](staging.md) |
| Remote CUDA staging | Accepted (H20, serial and pipeline) | Pin/Delegate/Unpin with up to 16 slots. Later nonblocking progress work is implemented only; full ProxyManager parity is open. See [remote pipeline](staging.md#bounded-remote-pipeline) |
| Mixed GPU Direct and staged batches | Implemented | Per-request remote staging next to GPU Direct in one ordered batch. See [batch contract](async-batches.md#mixed-gpu-direct-and-staged-submissions) |
| DRAM or CUDA ↔ local file (io_uring) | Implemented (Linux) | Real ext4/tmpfs DRAM transfers and CUDA shim staging through pinned host chunks. Installed GPU/NVMe profile unqualified. See [file transfers](storage.md) |
| GDS / cuFile | Implemented, installed profile unqualified | Host and CUDA batch I/O, registered buffers, direct file handles, terminal receipts and quarantine. See [GDS](storage.md#gds-with-cufile) |
| HP-TCP, SHM | Planned (P3) | |
| MNNVL / EGM | Planned (P5) | |
| MPComm, libfabric/EFA | Planned (P8) | Optional network providers |

## Memory and batches

| Capability | Status | Limits |
| --- | --- | --- |
| Request construction | Implemented | `TransferRequest::new` with builders. The `rdma` and `cuda` features require Linux. See [Rust API](rust-api.md) |
| Owned and external host registration | Implemented | Transactional batches, duplicate references, aliases and retained backing |
| Local file segments | Implemented | Core `file://` descriptors, checked offsets and close with pending I/O. Real-file controls cover fd retention and release. See [file segments](storage.md) |
| Async batches and notifications | Implemented | Bounded admission, cancellation and detached handles. See [async batches](async-batches.md) |
| NVIDIA allocations, pinned host memory and copy streams | Qualified (H20 local copy) | Stream and event reuse; failure retention has CPU controls only |
| GPU and pinned-host producer readiness | Implemented | Caller-stream capture before payload access, with a CPU-blocking wait. See [pinned handoff](rdma-cuda.md#pinned-host-producer-handoff) |
| Memory type and location | Implemented | Owner-backed CPU/CUDA locations; unknown pointers never select CPU access. See [memory location](rust-api.md#memory-type-and-location) |
| Host staging location | Implemented | GPU-near or explicit NUMA staging buffers; residency and performance are unqualified. See [staging location](staging.md#host-staging-location) |

## Routing and scheduling

| Capability | Status | Limits |
| --- | --- | --- |
| Transport selection | Implemented | First-match memory, size and priority rules with strict hints. Named policies, intents, device and QP-pool filters and file segments are open. See [transport selection](transport-selection.md) |
| Discovery, port recovery and multirail slicing | Accepted (RoCE) | Automatic discovery matches a separate host inventory; two-rail accounting verifies exact bytes. Physical loss is unqualified |
| CUDA topology, custom priority matrices, bandwidth-aware rails | Implemented | Installed routing and throughput are unqualified. See [bandwidth-aware selection](rdma-cuda.md#bandwidth-aware-rdma-selection) |
| Request priority and NIC arbitration | Implemented | RDMA orders High/Medium/Low with aging and opt-in deadline urgency; TCP and NVLink treat priority as advisory. See [priority](rdma-cuda.md#request-priority-and-bandwidth-arbitration) |
| Runtime FIFO/EDF admission | Implemented | Whole-submission dispatch windows. Staging reserves and deadline-infeasible cancellation are open. See [dispatch window](async-batches.md#runtime-admission-and-dispatch-window) |
| QoS contracts | Implemented | TENT-format global, tenant and intent policies with byte/request caps and delay/reject. Hierarchical quotas are open. See [QoS](async-batches.md#qos-contracts) |
| Receiver credits | Implemented | Cumulative peer grants and automatic receiver windows. They authorize requests; they are not a rate limiter. See [credits](async-batches.md#receiver-credit-gating) |
| Failover, explicit progress, metrics endpoints | Planned (P6) | |

## Failure handling

| Capability | Status | Limits |
| --- | --- | --- |
| Native failure retention | Accepted (RoCE fault profiles) | QP errors, lost CQEs, QP/MR/MW faults and CQ/channel/PD cleanup failures keep their owners and charges. Device-close errors have CPU controls only. See [native cleanup](native-cleanup.md) |

## APIs and compatibility

| Capability | Status | Limits |
| --- | --- | --- |
| Rust API | Implemented | See [Rust API](rust-api.md) |
| TENT C, C++ and Python APIs | Planned (P9) | |
| TENT configuration and metadata stores | Planned (P9) | `MC_TENT_CONF`, etcd, Redis and HTTP |
| TENT wire interoperability | Planned (P9) | OrbitTE peers use a pre-release protocol and must run the same build. See the [wire policy](tent-parity.md#wire-and-consumer-boundaries) |

## Provider selection

Explicit CPU/TCP routing works without configuration. `Transport::Unspec` uses
[eligible transport selection](transport-selection.md). `rdma` and `cuda` are
independent Cargo features. Enabling a feature makes its provider code
available; runtime use still requires the actual device, driver and resources.

`RdmaConfig::new("mlx5_0")` strictly pins that device, port 1 and GID index 0.
`RdmaConfig::default()` enables automatic discovery with optional strict pins
and whitelist/blacklist filters. `device_mask` limits dispatch by stable NIC
IDs. See [RDMA configuration](rdma-cuda.md#device-port-gid-and-memory-topology-discovery)
for selection and resource ownership.

## Scope of support

The target is CPU DRAM, NVIDIA GPUs and local SSDs: OrbitKV hands every
end-to-end cache transfer, including GPU↔DRAM and GPU↔SSD, to OrbitTE. Storage
transports will be opt-in builds like `rdma` and `cuda`, so a network/memory-only
build loads no storage libraries. Other accelerators are outside the selected
scope. Architectural correspondence with frozen TENT does not establish full
behavior or wire compatibility.

Hardware gates are paused while no GPU or RDMA machine is available. New work
is implemented with CPU controls, and its installed gates wait in the
[qualification](qualification.md) verification queue. OrbitKV integration
resumes in [P11](completion-plan.md#p11--consumers-performance-and-release).
