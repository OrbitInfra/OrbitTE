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
| `ReceiveCredits` | Atomically reserve a parent's source RQ capacity; transfer credits to actual receive owners | OrbitTE source-completion admission alongside TENT's bounded slice queues |
| `CqQuota` | Wake blocked context workers after actual aggregate SQ/RQ capacity returns | Rust notification adaptation for TENT's worker iteration across context CQs; separate from native cross-process QoS |
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

## Aggregate quota progress

All contexts share the existing SQ/RQ debt, byte and connection limits. Each
native CQ retains its configured capacity. In event mode, a context with no
posted WR has no local CQE to wake it when another context returns shared
capacity. `CqQuota` registers weak worker subscribers and wakes only workers
that reported quota pressure. Enrollment precedes a budget recheck, so a
concurrent release leaves either a notification or an available-capacity wake.

Native TENT's `Workers::asyncPollCq` visits the entire context set while its
worker has active slices. OrbitTE retains its existing per-context progress
owners and explicitly notifies across them. This adds no per-slice thread,
transport facade, bandwidth policy or cross-process quota implementation.
Capacity returns through validated data/control completions, proven unposted
suffixes or the existing native QP/CQ drain barrier. Unproven cleanup retains
its charges. See the [concurrent native profile](rdma-cuda.md#cross-context-quota-wakeups-and-concurrent-parents).

## Source receive admission

Each parent prepares every grant before payload submission. Reserving RQ slots
one slice at a time can leave concurrent parents holding incomplete plans with
no payload able to complete. `ReceiveCredits` acquires the whole validated plan
under the aggregate budget lock, then moves one credit into each `ReceiveQueue`.
Insufficient capacity returns `ResourceExhausted` before either peer creates a
new QP for that parent. This is immediate bounded admission; callers decide when
to retry. Single-path source requests acquire one credit through the same owner.

Wire v12 adds a source admission reply after the multirail plan. Requesters wait
for it before preparing endpoints. Previous wire versions are rejected. Unused
credits roll back on preparation failure; accepted or uncertain receives retain
their own credits until validated CQ completion or successful native QP/CQ drain.
`receive_credits` includes unposted reservations; `receive_work_requests` counts
actual reserved/possibly posted WR debt. Neither is payload-byte progress.

TENT's `Workers`, `RdmaSlice`, `DeviceSelector`, context set and endpoint store
remain the architectural reference. This additional RQ credit owner belongs to
OrbitTE's exact source receive/invalidation protocol, rather than a native TENT
wire contract or its cross-process bandwidth arbitration.

The extended native fixture exercises eight concurrent parents: 5 MiB,
MiB-plus-one folded and exact-quarter-tail requests over 32/40 QPs. TENT's
`planRdmaSlices` rounds to the base quantum and folds tails strictly below a
quarter block; an exact-quarter tail remains separate. Both outer routing and
inner WR queues use that rule. A native fixture checks each submitted WR offset
and length, including the folded final byte. `DeviceSelector` charges bytes,
so unequal tails need not create equal QP counts on the two RNICs.

Five shared SQ credits bound actual outstanding requester WRs, with at most two
WRs per post. The 5 MiB profile submits eighty payload WRs per parent, 640 per
eight-parent round, while retaining that five-slot aggregate bound. One source
RQ credit is reserved per outer slice's completion receive, independently of
the number of payload WRs. A ninth whole plan is
rejected before new QPs despite one residual RQ credit. Native observers bind
the injected fault to its exact CQ/QPN/WR and reconcile unresolved siblings,
DMA loans, MRs and receive credits through shared CQ teardown. Four native
mutation runs reject premature debt/credit return. This extends qualification
of existing owners without adding a production layer or changing wire v12.
See the [geometry evidence](rdma-cuda.md#larger-requests-folded-tails-and-eight-parent-admission)
and [source RQ contract](rdma-cuda.md#whole-parent-source-rq-admission).

## Completion and failure

RDMA data and endpoint destruction have separate deadlines. `close_slices`
shares one fresh thirty-second destruction window across a parent's slices,
matching TENT's `RdmaEndPoint::finishDestroy` timeout. Exhausting the data
deadline preserves the original failure. Only actual QP/CQ teardown permits
resource return; unproven destruction retains admission and backing. The failed
call may wait up to thirty additional seconds for this barrier, and native
provider calls remain non-preemptible. This adds no retry or WRITE replay.

The declared native profile now includes actual process-owned QP ERR/flush,
lost successful SQ/RQ CQEs and source SIGKILL with leases held through waitpid
and replacement exit. Fresh sources rebind the same RPC address and numeric
registration ID; old source epochs fail before new QPs or remote effects.
Healthy transfers resume with explicit new descriptors and fresh engine
processes. Physical port/device-down and recovery of a poisoned live engine
remain separate open profiles. See the
[destruction and restart evidence](rdma-cuda.md#native-endpoint-faults-lost-completions-and-restart).

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
