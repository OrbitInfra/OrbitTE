# Capabilities

This page summarizes the current source on `main`. The `0.1.0-alpha.1` crate on
crates.io is published for early development; its API is unstable.

| Status | Meaning |
| --- | --- |
| **Accepted** | A bounded installed hardware profile passed and an independent reviewer accepted it. Only that recorded profile is covered |
| **Qualified** | A bounded installed hardware profile passed; independent acceptance is still open |
| **Implemented** | Source and CPU controls pass; the current source has no installed hardware result |
| **Planned** | Not implemented yet; the stage is listed in the [completion plan](completion-plan.md) |

[Qualification](qualification.md) records the exact artifacts, cases and limits
behind every hardware result. The [v0.3.14 delta](tent-parity.md#v0314-delta)
records the source alignment. The [S16 acceptance inventory](https://github.com/OrbitInfra/OrbiTE/tree/main/tests/reference/inventory)
lists the six native direction flags and their provider conditions. Wire
fixtures and CPU codec checks support the independent frozen oracle.

## Transports

| Path | Status | Limits |
| --- | --- | --- |
| CPU DRAM over TCP | Implemented | Loopback and multi-process byte, restart and lifetime controls; no physical two-host profile. See [CPU and TCP](cpu-tcp.md) |
| Host DRAM over RDMA | Accepted (RoCE) | Two-host RoCEv2 READ/WRITE with restart, cleanup and in-flight DMA faults. InfiniBand and physical link or device loss are unqualified. See [RDMA and CUDA](rdma-cuda.md) |
| GPUDirect RDMA | Accepted (H20/RoCE, with limits) | Host/GPU READ/WRITE and two-rail slices. The H20 evidence covers private GPU source grants. Opt-in GPU source QP/MR reuse is implemented; installed profile unqualified. Host-wide context non-growth is unqualified |
| CUDA IPC / NVLink on one host | Accepted (H20, cooperative profiles) | GPU/GPU, pinned host/GPU, subbatch reuse and a fixed-working-set import cache. Grouped completion, stream pool, native batch copy, cache reclamation and the single-task synchronous path await installed verification. See [CUDA IPC](cuda-ipc.md) |
| Local CUDA staging over TCP/RDMA | Accepted (H20, healthy) | Shared pinned chunks; physical CUDA faults are unqualified. See [staging](staging.md) |
| Remote CUDA staging | Accepted (H20, serial and pipeline) | Pin/Delegate/Unpin with up to 16 slots. Source control isolation and later nonblocking progress are implemented; their installed profiles are unqualified. See [remote pipeline](staging.md#bounded-remote-pipeline) |
| Local CUDA ↔ pinned DRAM on a caller stream | Implemented; installed profile unqualified | Coalesced 1D/2D and batched DMA, mapped-host kernel and event-owned range loans. [Local copies](local-copy.md) |
| Mixed GPU Direct and staged batches | Implemented; installed profile unqualified | GPU Direct and local or remote staged legs keep public order. Compatible aliases within a leg share one dispatch and release receipt. See [batch contract](async-batches.md#mixed-gpu-direct-and-staged-submissions) |
| DRAM ↔ local file (io_uring) | Qualified on Linux CI | Direct ext4 and buffered tmpfs I/O, including ordered host gather/scatter with explicit file padding. See [qualification evidence](qualification.md#local-file-segments) |
| CUDA ↔ local file (io_uring) | Implemented; installed profile unqualified | CUDA shim and real files exercise multi-registration gather/scatter, bounded proxy chunks and optional pinned slots. See [file staging](storage.md#cuda-transfers-through-pinned-host-memory) |
| GDS / cuFile | Implemented, installed native gate unrun | Lazy, runtime-disablable cuFile, aligned vector I/O, terminal cookies and quarantine. See [GDS](storage.md#gds-with-cufile) |
| [HP-TCP](hp-tcp.md) | Qualified on Linux loopback | Least-loaded single-operation lanes and striped READs on multiple loopback IPv4 rails. Physical rail gate unrun |
| [SHM](shm.md) | Qualified on same-host Linux CI | Guarded two-process READ/WRITE over `/dev/shm`. The file-backed path is tested on tmpfs. Physical CXL is unqualified |
| [MNNVL / EGM](cuda-ipc.md#multi-node-nvlink-and-egm) | Implemented; installed profile unqualified | CUDA VMM fabric handles, GPU and host-NUMA exports, bounded imports and checked close. The multi-node device and EGM gates are unrun |
| [Libfabric host DRAM](fabric.md) | Implemented (P8.2); software TCP provider tested in Linux CI | Runtime-loaded opt-in provider, bounded RMA grants and sticky native retention. EFA is implemented; installed profile unqualified |
| MPComm | Deferred (P8.1) | No consumer; provider unavailable. Constant recognized and returns unavailable |

## Memory and batches

| Capability | Status | Limits |
| --- | --- | --- |
| Request construction | Implemented | `TransferRequest::new` with builders. The `rdma` and `cuda` features require Linux. See [Rust API](rust-api.md) |
| Owned and external host registration | Implemented | Transactional batches, duplicate references, aliases and retained backing |
| Local file segments | Implemented | Core `file://` descriptors, checked offsets and close with pending I/O. Real-file controls cover fd retention and release. See [file segments](storage.md) |
| Async batches and notifications | Implemented | Explicit progress, event-driven batch wakes, deferred free, bounded TCP/RPC notifications and per-target delivery. See [async batches](async-batches.md#explicit-progress) |
| NVIDIA allocations, pinned host memory and copy streams | Qualified (H20 local copy) | Stream and event reuse; failure retention has CPU controls only |
| GPU and pinned-host producer readiness | Implemented | Caller-stream capture before payload access, with a CPU-blocking wait. See [pinned handoff](rdma-cuda.md#pinned-host-producer-handoff) |
| Memory type and location | Implemented | Owner-backed CPU/CUDA locations; unknown pointers never select CPU access. See [memory location](rust-api.md#memory-type-and-location) |
| Host staging location | Implemented | GPU-near or explicit NUMA staging buffers; residency and performance are unqualified. See [staging location](staging.md#host-staging-location) |

## Routing and scheduling

| Capability | Status | Limits |
| --- | --- | --- |
| Transport selection | Implemented | First-match and named rules consume intent, boot identity, priority and size. RDMA dispatch honors per-request NIC masks and endpoint QP pools. Physical routing awaits the installed two-RNIC gate. See [transport selection](transport-selection.md) |
| Discovery, port recovery and multirail slicing | Accepted (RoCE) | Automatic discovery matches a separate host inventory; two-rail accounting verifies exact bytes. Physical loss is unqualified |
| RDMA rail health | Implemented; installed profile unqualified | Policy-masked slices refresh local paths and prefer a healthy mapped peer for wildcard targets. A real link-fault and two-RNIC gate is queued. See [rail health](rdma-cuda.md#rail-health) |
| CUDA topology, custom priority matrices, bandwidth-aware rails | Implemented | Installed routing and throughput are unqualified. See [bandwidth-aware selection](rdma-cuda.md#bandwidth-aware-rdma-selection) |
| Request priority and NIC arbitration | Implemented | RDMA orders High/Medium/Low with aging and opt-in deadline urgency; TCP and NVLink treat priority as advisory. See [priority](rdma-cuda.md#request-priority-and-bandwidth-arbitration) |
| RDMA selective signaling and HCA READ depth | Implemented; installed profile unqualified | Per-post signal groups, QP depth clamps and exact flush accounting. See [RDMA ownership](rdma-cuda.md#failure-drain-and-resource-ownership) |
| Adaptive congestion core | Implemented, CPU controls | Opt-in callable device and route domains with off/observe/enforce. TENT and OrbiTE workers leave live policy to the caller. |
| RDMA quota accounting | Implemented, installed gate unrun | Per-NIC smart inflight bytes and exact slice charges. A POSIX shared-memory name enables host-wide priority slots. See [shared slots](rdma-cuda.md#priority-promotion-and-shared-slots) |
| Runtime FIFO/EDF admission | Implemented | Merged memory requests, public alias status, physical dispatch windows, staging reserves and transport-aware deadline drops. See [dispatch window](async-batches.md#runtime-admission-and-dispatch-window) |
| QoS contracts | Implemented | Global, tenant and intent fields resolve to one local byte/request budget. Delay and reject execute. Unsupported actions return a field-specific error. See [QoS](async-batches.md#qos-contracts) |
| Receiver credits | Implemented | Four named cumulative grants, configured receiver windows and drain-fenced epoch retirement. See [credits](async-batches.md#receiver-credit-gating) |
| Transport failover | Implemented | Bounded physical retries, epoch-pinned Describe and release evidence. Installed RNIC profile unqualified. See [transport attempts](async-batches.md#transport-attempts) |
| RDMA rail cooldown and recovery | Implemented; installed gate unrun | [Rail health](rdma-cuda.md#rail-health) |
| Offline version diagnostics | Implemented | Packaged CLI and Rust API. See [diagnostics](diagnostics.md). |
| TENT metrics, request/attempt accounting and exporter health | Implemented (P6, opt-in) | [Metrics](metrics.md) |

## Failure handling

[CPU benchmark and fault fixtures](benchmarks.md) cover verified size/batch
sweeps and the software backend lifetime matrix. They add no hardware
qualification; GPU/RDMA/GDS installed fault fixtures remain queued.

| Capability | Status | Limits |
| --- | --- | --- |
| Native failure retention | Accepted (RoCE fault profiles) | QP errors, lost CQEs, QP/MR/MW faults and CQ/channel/PD cleanup failures keep their owners and charges. Device-close errors have CPU controls only. See [native cleanup](native-cleanup.md) |

## APIs and compatibility

| Capability | Status | Limits |
| --- | --- | --- |
| Rust API | Implemented | See [Rust API](rust-api.md) |
| TENT C ABI (S19) | Removed; out of scope | Frozen declarations retained as reference inventory only |
| TENT C++ and classic TE (S20) | Removed; out of scope | No source adapters or classic-TE shim |
| Device plugin ABI (S22/F03) | Removed; out of scope | CPU and CUDA use built-in owners |
| TENT Python API | Implemented (F21) | [Installed wheels](python.md) for CPython 3.10–3.14, Linux x86_64/aarch64. Guarded TCP, GIL release and retained host/CAI/DLPack exporters; native GPU/RDMA Python profiles remain unqualified |
| TENT configuration and P2P self-description | Configuration implemented (F01), P2P available (F06) | File or inline `MC_TENT_CONF`, legacy aliases and explicit constructor precedence resolve into [one engine configuration](rust-api.md#loading-tent-configuration). Discovery, membership, KV indexes and external metadata stores belong to the consumer; non-P2P configuration is rejected. Rust `close_segment(endpoint, epoch, timeout)` fences and drains one peer; live descriptor/operation references bound the identity table, while failed native cleanup stays charged. CPU sockets/SHM are exercised on Linux; RNIC/CUDA retirement is shim-only, installed profile unqualified |
| TENT wire interoperability | Out of scope by design (S23) | See the [wire and consumer boundary](tent-parity.md#wire-and-consumer-boundaries) |

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
end-to-end cache transfer, including GPU↔DRAM and GPU↔SSD, to OrbiTE. Storage
transports use the opt-in Linux `io-uring` and `gds` features. A network/memory-only
build loads no storage libraries. Other accelerators are outside the selected
scope. Architectural correspondence with frozen TENT does not establish full
behavior or wire compatibility.

Hardware gates are paused while no GPU or RDMA machine is available. New work
is implemented with CPU controls, and its installed gates wait in the
[qualification](qualification.md) verification queue. OrbitKV integration
resumes in [P11](completion-plan.md#p11--consumers-performance-and-release).
