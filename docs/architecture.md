# Architecture

OrbitTE follows the frozen Mooncake TENT architecture around engines, topology,
contexts, registered buffers, endpoints and workers. Rust ownership retains the
actual allocation and native resources through physical completion or teardown.
The current protocol is OrbitTE-specific; API and wire parity are tracked
separately.

:::{image} _static/architecture.svg
:alt: OrbitTE engine and sibling TCP/RDMA transports; dashed paths are planned
:class: dark-light
:::

## Engine and control plane

`TransferEngine` owns registration, batches, transfer admission and separate
source/requester shutdown. A registered range retains its backing allocation;
duplicate registrations retain references to the same range, and overlapping
views share allocation-level access coordination.

`runtime::control_plane::ControlService` handles peer descriptors, grants and
receipts over bounded TCP control connections. TCP payload I/O lives in
`transport::tcp::TcpTransport`. RDMA payload I/O lives in
`transport::rdma::RdmaTransport`. The transports share engine ownership and
completion contracts.

## RDMA ownership

| Owner | Responsibility | Native TENT correspondence |
| --- | --- | --- |
| `Topology`, host discovery | Inventory NICs and NUMA locations; retain stable NIC IDs | `Topology`, `CpuPlatform::probe` |
| `RdmaTransport`, context set | Initialize permitted NICs and route each request or slice to a compatible context | `RdmaTransport::initializeContexts`, `context_set_` |
| `DeviceSelector` | Intersect masks, topology tiers and in-flight byte charges | `DeviceSelector` in `quota.h` |
| `RdmaContext` | Own actual context/PD, frozen route and persistent key history | `RdmaContext` |
| `LocalBufferManager` | Prepare all-context host MRs before publication; retain physical copies by context, range and capability | Context-keyed `mem_reg_map` |
| Connection resources, `EndpointStore` | Own QPs, immutable connection identities and idle reuse | `RdmaEndPoint`, endpoint store |
| Grant resources | Own one slice's exact loan, source MW and completion buffers | Rust-specific exact-range authority over TENT's registered-region model |
| `Workers`, CQ and queue owners | Post bounded WR windows, validate real completions and retire resources | `Workers`, RDMA CQ and slice owners |

The `platform/cpu_probe.rs` filename comes directly from TENT's
`platform/cpu_probe.cpp`. It discovers host NUMA/RNIC locality. Native verbs
access remains in the RDMA discovery module; allocation page-location probing
and GPU topology are still separate work.

## Device selection and rail routing

Each permitted NIC retains its topology slot, even if native construction
fails cleanly. An enabled slot owns its own PD, progress worker, shared SQ/RQ
CQs and endpoint store. Physical MR copies across contexts share aggregate
registration budgets.

The control bootstrap advertises bounded NIC/PD paths before granting memory
authority. The requester selects a compatible path using its device mask,
NUMA hint and in-flight byte load. A cached pair remains bound to the selected
source NIC/PD, both process/connection identities and the previous grant.

Host Direct registration prepares a local-only MR in every enabled context,
plus a separate MW-bind parent for remotely accessible ranges. A batch becomes
visible only after all copies succeed. Dispatch masks select paths; they do not
remove enabled contexts from publication. Published copies remain pinned until
unregister or close, with aggregate context/capability byte and count limits.

Large multi-context requests use TENT's bounded, short-tail-folding slice
geometry and select a path per slice. One source control handler owns the parent
and prepares every slice grant before payload submission. Existing per-context
workers post slices concurrently; each slice retains its own PD, connection,
MR borrow, exact-range DMA loan, WR identity and bound completion receipts.
The parent succeeds only after every slice completes. No thread is created per
slice. Small or single-context requests retain the single-path protocol.

A declared two-host CPU DRAM/IPv4 RoCEv2 profile validates two actual RNICs per
host: masked and cross-rail READ/WRITE, persistent QP reuse, all-context MR
publication/rollback, and either-rail CQ/partial-post faults. Test-only native
observers bind each receipt to the actual device, PD, QP, CQ and WR owner while
public engine operations drive the existing transport. They add no production
callback API, worker or hot-path layer. See the
[native profile](rdma-cuda.md#two-rnic-native-qualification-and-failure-drain).
Physical link/device loss, broader concurrency/topology, cross-rail recovery
and independent acceptance remain in the [completion plan](completion-plan.md).

## Completion and failure

Frozen TENT publishes context-specific region rkeys from its registered
buffers. OrbitTE adds exact-range memory-window grants while retaining TENT's
context, buffer and endpoint ownership boundaries. The MW handshake is an
OrbitTE authority contract.

A source MR is a local parent registration. A fresh one-use memory window
authorizes only the granted range. Data completion, same-QP fenced completion
messaging, exact source receive/invalidation and bound DONE/ACK/READY receipts
establish the negotiated grant-drain proof before connected reuse.

Logical cancellation and timeouts do not prove native quiescence. Failed
cleanup retains the actual QP/MR/allocation owners and their charges. An
ambiguous WRITE is not replayed automatically. See the detailed
[RDMA lifetime contracts](rdma-cuda.md) for the state transitions and evidence.

## Source layout

| Location | What it owns |
| --- | --- |
| `engine.rs`, `memory.rs`, `batch.rs` | Public operations, backing allocations and batch lifetime |
| `runtime/` | Control service and shared topology descriptions |
| `platform/` | Host platform discovery |
| `transport/tcp.rs` | TCP payload transfer |
| `transport/rdma/` | RDMA context, buffers, connections, queues, scheduling and progress |
| `cuda/` | NVIDIA allocation and synchronized host-staging foundations |

The [parity contract](tent-parity.md) pins the reference revision and capability
inventory. Architecture correspondence does not establish native ABI, wire or
performance equivalence.
