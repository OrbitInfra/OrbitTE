# RDMA and CUDA

This guide describes the current source. Start with [Build](build.md) for
provider dependencies and [Architecture](architecture.md#source-reading-order)
for the code layout. Executed profiles and their limits live in
[Qualification](qualification.md); the [completion plan](completion-plan.md#p4-remaining-delivery)
is the only P4 execution queue.

## Build and select a path

The default crate supports CPU/TCP without a native SDK. The independent
`rdma` and `cuda` Cargo features enable libibverbs and CUDA Driver API bindings.
Runtime use also requires the actual driver, provider and accessible devices.
OrbitTE's engine, connection protocol, progress and resource owners are Rust;
there is no Mooncake runtime dependency or C bridge in the production crate.

Each `TransferRequest` explicitly selects `Transport::Tcp` or `Transport::Rdma`.
An unavailable RDMA path fails preflight. Host policy selects the backing used
by RDMA:

| Memory and policy | Payload path |
| --- | --- |
| Host, `RdmaHostMemory::Direct` (default) | Application host range ↔ RC RDMA READ/WRITE ↔ peer registered range |
| Host, `RdmaHostMemory::Staged` | Application bytes ↔ private registered host staging ↔ RDMA |
| NVIDIA device, `RdmaCudaMemory::Staged` (default) | Device ↔ owned host staging ↔ RDMA |
| NVIDIA device, `RdmaCudaMemory::Direct` | Device MR ↔ RC RDMA READ/WRITE ↔ peer registered range |
| NVIDIA device over TCP | Device ↔ owned host staging ↔ socket |
| Host over TCP | Application bytes ↔ owned host buffers ↔ socket |

Each peer selects its host and device policies independently. Failed direct MR
registration returns an error; it never changes the request to staging. CUDA
IPC and NVLink use a separate [cooperative transport](cuda-ipc.md). Direct GPU support requires actual
PeerDirect-capable GPU/RNIC registration and the prerequisites below.

Opt-in [remote CUDA staging](staging.md#serial-remote-staging) coordinates
Pin/Delegate/Unpin over the same control channel, while TCP and RDMA retain
payload ownership. The serial profile passes 16 installed H20 cases; the bounded
up-to-16-slot pipeline passes 24 fresh cases on stable and Rust 1.85. See
[qualification](qualification.md#bounded-remote-cuda-pipeline) for exact artifacts,
review status and limits.

TCP and staged RDMA use the shared [local CUDA staging pool](staging.md) for
GPU copies. The pool bounds reusable pinned chunks; existing payload buffers,
MRs and RDMA quotas keep their own lifetimes.

## Device, port, GID and memory topology discovery

Set `EngineConfig.rdma` to a configured `RdmaConfig`:

| Configuration | Behavior |
| --- | --- |
| `RdmaConfig::default()` | Discover permitted devices, ports and valid GIDs |
| `RdmaConfig::new("mlx5_0")` | Strictly pin that device, port 1 and GID index 0 |
| `device`, `port`, `gid_index` | `Some(...)` is a strict pin; `None` enables selection for that field |
| `rdma_whitelist`, `rdma_blacklist` | Filter devices before opening; a nonempty whitelist takes precedence |
| `device_mask` | Restrict dispatch by stable topology NIC IDs, up to 64 devices |
| `preferred_numa_node` | Explicit CPU NUMA hint overriding automatic owner location |
| `strict_local_numa` | Reject known cross-NUMA NICs; default `false`; unknown locality stays eligible |
| `priority_matrix` | Optional inline TENT matrix JSON; takes precedence over `custom_json_path` |
| `custom_json_path` | Optional TENT matrix or native `nics`/`mems` JSON file, read at context initialization |
| `scheduling` | TENT `SchedulingParams`: bandwidth-aware selection, NUMA weights, EWMA coefficients and transmit-meter intervals |

Discovery prefers active routes and retains otherwise usable paused ports.
Each selected context owns its actual device, protection domain (PD), key
history and address. A failed slot never renumbers the other NIC IDs. A partial
construction whose cleanup fails retains its owners and fences admission.

With both `rdma` and `cuda`, discovery enumerates CUDA-visible devices without
creating contexts or allocating device memory. Each `cuda:N` entry publishes its
validated PCI bus ID, optional NUMA node and three GPU-to-RNIC tiers: nearest
known PCI bridge distance, other same-NUMA NICs, then the remaining NICs. Missing
sysfs distance never wins the nearest tier. No CUDA driver/device leaves the
host inventory usable; other native query errors fail discovery before any GPU
entries are published. CUDA ordinals are process-local, including visibility remapping.

The existing `DeviceSelector` compiles location priorities once. A registered
owner supplies `cpu:N`, `cuda:N`, or unknown DRAM (`*`) for each request; aliases
and contexts on the same GPU share its topology row. Selection intersects the
device mask, healthy contexts, compatible paths and strict-NUMA eligibility,
then chooses the lowest estimated service time with NUMA weights and rotating
ties. [Bandwidth scheduling](#bandwidth-aware-rdma-selection) describes the
formula and the optional baseline round-robin mode.
Missing location metadata keeps unknown locality; it does not establish CPU
access to GPU bytes. Explicit `preferred_numa_node` takes precedence.

Source advertisements use the source owner's eligibility and priority order.
Compatible same-name NIC pairs still take precedence; otherwise the peer keeps
the advertised locality order. A matching name or address family does not prove
physical reachability. Strict locality excludes a known cross-NUMA NIC even if
it is the nearest PCI path, but does not exclude unknown placement. A known CPU node remains known when its sysfs
memory row is missing. Masks never override this rejection. Physical page placement and bandwidth estimates are
not inferred from hints or PCI distance. See [qualification](qualification.md#cuda-topology-and-rdma-selection).

The control protocol publishes bounded NIC/PD path metadata without memory
authority. Actual connection descriptors must match the selected path. Explicit
pins never silently fall back to another device, port or GID. See
[initially paused contexts](#initially-paused-contexts-and-gid-reselection) for
the recovery rules.

## Bandwidth-aware RDMA selection

`RdmaConfig::scheduling` exposes `SchedulingParams`, following frozen TENT's
`DeviceSelector`. Smart selection is enabled by default. For each eligible NIC:

```text
score = (charged_inflight_bytes + slice_bytes) / selection_bandwidth_bytes_per_second
        * numa_tier_weight
```

For one slice the lowest score wins; equal scores rotate. For a multi-slice
parent, candidates are scored once using its planned block size. TENT's
`selectMultiPath` assigns `floor(weight / total_weight * slice_count)` slices per
NIC with `weight = 1 / (score + score_epsilon)`, then gives leftover slices to
the best NIC. `score_epsilon` defaults to `1e-12`; weights are normalized to the
best candidate before summing to keep finite configuration values bounded.
Tier weights default to `[1, 5, 10]`.

Every 100th successful smart multi-slice allocation probes eligible NICs in
round-robin order. The counter belongs to the transport's selector, rather than
TENT's thread-local counter, so work split across submission threads still
reaches the interval. Probe starts rotate across parents even when a parent has
fewer slices than NICs. A probe can use only currently available, compatible
paths; it cannot recover or test a failed rail. Baseline mode does not probe.

All parent byte charges commit under one selector lock, including an exact final
slice that can be larger than the block when a short tail is folded. Overflow
rejects the entire reservation. A bounded `DeviceAllocation` owns unconsumed
charges and transfers each to the existing grant's `DeviceLease`; abandonment
returns only unconsumed charges. No new protocol frame or payload retry is added.
Single-grant/source paths use the same allocation owner; the source still binds
each grant to the requester's selected advertised protection domain.

Strict NUMA rules, configured candidates, masks and compatible paths remain hard
eligibility limits. Weights can therefore favor an idle lower-ranked NIC, but
cannot admit an excluded NIC. Set `scheduling.smart_selection = false` for
round-robin selection within the first eligible tier. Both modes retain exact
physical grant charges through source acknowledgement, native drain or failed
cleanup; collecting a payload sample never releases that ownership.

Native context initialization and periodic monitor queries obtain negotiated
port speed/width through `ibv_query_port`. The selector seeds estimates from
that rate; an unknown or out-of-range rate uses `default_bandwidth_gbps` (400).
The default accepted range is 10–800 Gbit/s. An unknown actual rate remains
`None` in load snapshots. A speed change reseeds the estimates and discards old
sampling generations, while repeated unchanged queries preserve learning.
Speed alone is not connection identity and does not recreate QPs. This provider
currently decodes legacy active-speed/width fields; newer effective LAG rate
queries and extended XDR encoding remain open.

Two independent estimates share one per-NIC owner:

- **Selection EWMA:** successful READ/WRITE payload bytes divided by the interval
  from the first accepted native post to the last valid payload CQE. This
  includes waits between WR windows, but excludes registration, handshake and
  later source receipts. The old estimate has coefficient 0.01 by default.
- **Transmit bandwidth:** valid payload completion bytes divided by time with
  outstanding posted payload debt. Idle intervals are excluded. The old estimate
  has coefficient 0.9; the meter updates after 10 ms and drops intervals older
  than 50 ms. Each CQ polling pass accounts all its completions before sampling
  against their shared timestamp. Control WRs do not contribute payload bytes.

Both estimates clamp to 0.1–10 times their seed. Invalid configuration is rejected
before device construction. Partial/uncertain posts, failed completions and
fenced outstanding work invalidate affected samples; possible native debt
remains charged until validated completion or physical drain. The transmit
estimate never replaces the selection EWMA.

`TransferEngine::nic_load_stats()` returns available initialized NICs without
probing or opening devices. It is empty before initialization or after close.
Snapshots expose stable IDs/names, actual optional link rate, charged grant bytes,
currently posted payload bytes, cumulative valid completion bytes, both estimates
in **bytes/s**, sample counts and cumulative busy time. Completed bytes can
include a successful prefix of an attempt that later fails; only successful
complete attempts update selection EWMA. These are local requester observations,
not wire-level throughput or source-side remote DMA measurements.

Selection reuses precompiled topology and an atomic per-NIC estimate; it adds no
driver queries or allocation. Sampling uses the existing SQ/CQ owners, a per-NIC
meter lock and one timestamp per CQ polling pass. No additional global completion
lock is introduced. Whole-parent selection scans context health and locks the
selector once, using fixed arrays of at most 64 candidates/slices without an
additional heap allocation; each eligible NIC estimate is read once. Actual
grant acquisition still rechecks native health. Hardware throughput and
comparison with TENT remain unqualified; see [bandwidth evidence](qualification.md#rdma-bandwidth-selection)
and [multipath evidence](qualification.md#rdma-weighted-multipath).

## Custom topology and priority matrices

The RDMA configuration accepts TENT's classic location matrix. Each row is
`[preferred NIC names, available NIC names]`, corresponding to rank 0 and rank 1:

```rust
let rdma = orbit_te::transport::rdma::RdmaConfig {
    priority_matrix: Some(r#"{
        "cpu:0": [["mlx5_0"], ["mlx5_1"]],
        "cuda:0": [["mlx5_1"], ["mlx5_0"]]
    }"#.into()),
    ..Default::default()
};
```

Names must exist in this process's discovered inventory after device/port/GID
filters. `cuda:N` uses the CUDA-visible ordinal, and requires CUDA discovery.
Unsupported accelerator locations, absent memory rows and absent NICs are errors.
Only listed NICs are eligible for that row. Empty ranks deliberately disable a
location. Masks, context availability and strict physical NUMA eligibility
further restrict the candidates; they never introduce an omitted NIC.

An explicit `*` row supplies ranks for locations without their own configured
row, while retaining each discovered location's physical metadata. If omitted,
the classic matrix synthesizes `*` from the union of preferred and available
NICs. A NIC preferred in any row appears only in the wildcard's preferred tier.
Duplicate NICs within one row, or duplicate locations, are configuration errors.
A known CPU node absent from discovery still applies strict NUMA filtering to
the wildcard candidates; it cannot become unknown to bypass locality.

Alternatively, set `custom_json_path: Some(path.into())`. The file accepts the
same matrix or TENT's native format:

```json
{
  "nics": [{"name": "mlx5_1", "type": 0}, {"name": "mlx5_0", "type": 0}],
  "mems": [{"name": "cuda:0", "type": 1,
            "device_list": {"rank0": [0], "rank1": [1], "rank2": []}}]
}
```

Native-format indices refer to the file's `nics` order and are resolved to the
existing stable NIC IDs. Memory type is optional; when present it must match
discovery (`0` host, `1` CUDA; `4` is also accepted for `*`). This format does not
synthesize a wildcard: omitted memory rows retain automatic ranks unless the
file supplies `*`. JSON PCI/NUMA and NIC attributes never replace discovered
physical metadata, addresses or provider capabilities. Rank overrides do not
reduce persistent MR publication across otherwise usable contexts.

Both inputs are limited to 1 MiB. Inline input wins even when invalid; malformed
or unreadable explicit configuration fails initialization without opening RDMA
contexts or publishing MRs. This deliberately differs from frozen TENT's
warning-and-autodiscovery fallback and replacement of the inventory. Successful
initialization freezes the resolved ranks: changing the file does not reload
them. Transfers and monitor recovery perform no JSON or filesystem work.
The names and two JSON forms follow frozen TENT `runtime/topology.cpp`; legacy
environment mappings and the general TENT config object remain F01 work.
See [source qualification](qualification.md#custom-topology-input).

## Eager publication and cross-rail slice completion

With host Direct policy, registration prepares physical MRs on every usable
context, including paused ports, before exposing the registered range. A
local-only view uses one MR per context; a remotely accessible view uses two:
a local-only MR and a separate `LOCAL_WRITE|MW_BIND` parent. Dispatch masks do
not reduce this registration fan-out. Neither parent key is exported.

Batch visibility and duplicate-reference increments commit only after every
required MR is ready. Failure retires the new copies while preserving previously
published registrations. Failed rollback retains its actual MR, backing, PD
and budget. Published active copies cannot be evicted. Final unregister hides
lookup visibility and retires every copy; already admitted work retains its
exact allocation and MR borrow through native drain.

The MR cache is keyed by actual context/PD, registered view identity and
capability. Two roles may therefore charge two full-view MRs for the same
allocation. An idle MR holds no DMA exclusion lease. Each grant separately
locks its exact allocation-relative range, so disjoint ranges may proceed and
conflicting CPU/DMA access through the same owner is rejected across aliases.

Requests use TENT's bounded slice geometry: a 64 KiB base block by default,
at most 64 slices for host-local READ and 32 for WRITE or device-local READ.
The block adapts for large requests; a tail shorter than one quarter of the
chosen block folds into its predecessor. CUDA-pinned host memory counts as
host memory. Peer limits may reduce the effective slice count.

`DeviceSelector` selects a compatible rail for each slice. The source validates
exact ordered coverage and reserves the whole parent's grant and receive
capacity before publishing authority. Every child grant must be ready before
payload submission. The parent reports success and its byte count only after
all slices complete, including their source receipts. Cleanup visits every
slice even after one rail fails; an unquiesced child prevents a parent drain
claim. Earlier slices may already have changed bytes, so a multi-rail WRITE
is not atomic.

## Connections, grants and completion

Host Direct peers negotiate a fixed reusable QP width equal to the smaller
`num_lanes` value. `EndpointStore` provides a canonical peer array, and each
cold builder reserves a slot before creating a native QP. A slot stays occupied
through construction, use and unresolved native destruction. Waiters use their
own deadlines without cancelling the builder or holding store locks across I/O.

Arrays are scoped by local context, remote process/PD/NIC, role and READ/WRITE
capability. Concurrent grants share published QPs; successful records can
retire independently. The physical QP becomes idle only after all affiliated
records finish. A direct GPU requester can reuse a direct host source QP. If either peer uses
staging, or the source uses direct GPU memory, connections remain per-grant and
require physical close barriers.

The current OrbitTE protocol is v21. Every phase binds both process and
connection incarnations, the exact request and an independent grant generation.
This protocol is separate from native TENT API/wire compatibility.

For host/staged source memory, the source binds a one-use Type-2B memory window (MW) to the exact range and
direction, consumes its successful BIND completion, and posts a real completion
receive before publishing the grant. Type-2B MW support is required; a remotely
enabled parent MR is not a fallback. The PD retains a bounded, non-evicting key
history so an old grant key cannot regain authority within that domain.
When a native MW index has no fresh low-byte tag, the context keeps that newly
allocated, never-bound MW alive and allocates a replacement from the same PD.
Keeping the exhausted index allocated prevents immediate provider reuse without
clearing key history. One parked MW per index and the historical index count
are both bounded by `max_window_key_indices`; duplicate live indices, exhausted
capacity or allocation failure reject admission. `exhausted_windows` counts
these PD-owned MWs inside `active_windows`. Close destroys them before the PD;
failed destruction retains the remaining MWs and PD without a Drop retry.

The requester submits bounded, signaled READ/WRITE work. After every payload
CQE succeeds, it posts a fenced `SEND_WITH_INV` on the same QP. The source must
receive the exact 141-byte binding and invalidated key through its actual RQ
CQE. A SEND CQE or control-plane DONE alone cannot establish source completion.
Only after the bound DONE and native receipt agree does the source clean its
MW and grant, send ACK, and publish READY for reuse.

`RdmaQp` owns SQ depth, RQ buffers and exact outstanding WR identities.
Independent grant records borrow queue capacity. A received buffer identifies
the native slot; the message binding identifies the completed grant, which may
be a different record. Checked WR namespaces and CQ/QPN validation prevent
old or duplicate completions from paying current debt.

Only an explicit pre-authority cache miss permits bounded connection bootstrap
retry on the selected PDs. An ambiguous WRITE or payload failure is never
replayed automatically.

## Workers, CQ lanes and budgets

One transport-level `Workers` pool progresses all contexts. `num_lanes`
controls the data worker count and the SQ/RQ CQ pairs per context. New slices
use worker load; a reused QP stays on its original CQ lane. Each CQ has one
polling owner. Data workers bind to allowed NUMA CPU sets; synchronous native
calls can still delay other work on that worker.

`RdmaProgressMode::Polling` is the default. `EventDriven` arms both CQs before
polling and uses native events to wake the existing data worker. One transport
`Monitor` consumes and acknowledges device/CQ events and scans contexts each
second. Native data work and fallible destruction stay with the data owners.

| Budget | Default | Charged resource |
| --- | --- | --- |
| `max_endpoints` | 64 | Aggregate physical QPs, including retained connections |
| `max_grants` | 64 | Whole-parent grant reservations, independent of QP count |
| `max_send_wr` | 16 | Unresolved signaled sends per physical QP |
| `max_cqes` | 1024 | Each CQ's capacity; aggregate SQ and RQ debt are bounded separately |
| `max_direct_bytes`, `max_staging_bytes` | 64 MiB each | Active direct loans and private staging respectively |
| `max_cached_mrs`, `max_cached_mr_bytes` | 64 / 64 MiB | All full-view physical MR copies across contexts and capabilities |
| `max_window_key_indices` | 65,536 | Historical MW key indices and parked exhausted MWs in each PD domain |

Creating, closing and retained resources remain charged. Native CQ completion
repays its exact WR debt; receive admission remains charged until the actual
message MR is reaped. Returning aggregate capacity wakes the owning lanes.
Adding a context does not multiply the aggregate posted-work budget. These
limits describe admission, not a throughput or fairness guarantee.

## Failure, drain and resource ownership

Post failure releases only a proven unaccepted suffix. An unknown `bad_wr`,
invalid CQE, timeout or CQ error leaves unresolved work charged and fences
affected admission. Physical cancellation includes all grants affiliated with
the failed QP; uncertain debt requires the native QP/CQ drain barriers.
Healthy sibling owners can finish their own cleanup.

Parent cleanup has one shared thirty-second destruction window, separate from
the data deadline. A timeout does not prove physical termination. Native
teardown failure retains the remaining resources, backing and charges and is
not retried by another close or Drop. A fully drained and detached progress
owner can be replaced while preserving its context's PD and persistent MRs;
quarantined native failures cannot be healed this way.

`TransferEngine::close(timeout)` fences source and requester admission, waits
for accepted work, retires connections and MRs, drains CQs and workers, then
destroys PDs and contexts. Its shared deadline can expire while the native
owner remains live; a later close can continue waiting. `close_source` and
`close_requester` retain their directional contracts and do not supply a
whole-engine PD/context teardown receipt.

`Error::Unquiesced` means physical completion or destruction is unproven.
Logical failure can therefore coexist with `locally_drained == false`; batch
capacity, registration owners and DMA loans remain retained. Successful engine
close does not free caller-held batch handles or unregister caller-held memory.
Native provider calls are synchronous and cannot be forcibly interrupted by an
API deadline. See [native cleanup](native-cleanup.md) for tested failure owners
and [async batches](async-batches.md) for caller-visible completion.

## Periodic monitor recovery

The monitor queries selected routes even without new submissions, covering
missed port events. A changed or inactive address fences route admission. Old
builders, QPs and CQs must physically drain and detach before a validated route
is published under the unchanged fault generation. Recovery preserves PD
identity, key history and persistent MRs.

`monitor_ticks`, `route_queries` and `route_recoveries` distinguish scans,
native queries and resumed routes. `progress_recoveries` counts reclaimed,
fully drained lane owners. Busy owners can defer a tick; the one-second scan
interval is not a real-time recovery guarantee. Fatal device events, lost event
consumers and retained cleanup failures remain fenced.

### Initially paused contexts and GID reselection

An initially inactive port with valid provider/GID metadata retains a paused
context and PD. Host Direct registration prepares its MR copies even when all
selected ports are paused, while path selection and QP creation reject that
route. A healthy sibling rail remains eligible. Stable NIC IDs and MR fan-out
do not change during recovery.

The context retains configured `gid_index` separately from its selected address.
Healthy ticks query the selected index. Paused automatic recovery (`None`)
reranks valid GIDs on the same device and port; `Some(index)` remains strict.
This follows frozen TENT's `RdmaContext::enable` and `refreshAddress`.
OrbitTE's ranking also includes real IPv6 RoCEv2. New-device hotplug and general
cross-rail retry policy remain outside this behavior.

CPU controls cover these transitions. The focused native gate passes with
stable 1.99.1 and Rust 1.85.0 on one mlx5 RoCE RNIC: automatic recovery
selects GID 3 from initial GID 0, while the explicit pin stays at 0. It uses
real PD/MR/QP/CQ operations and native GID queries with a process-local
override of the observed port state. It sends no payload and does not
change a physical port. See [the frozen evidence](qualification.md#focused-native-recovery)
for source equivalence, provider maps, supervision and remaining acceptance.

On an authorized RNIC, inspect its GID table and choose a valid
`ORBITTE_INITIAL_GID` different from the automatically preferred index. Supply
the existing exclusive resource-lock file and an external log destination:

```sh
ORBITTE_RDMA_LOCK=/path/to/authorized-rnic.lock \
ORBITTE_SHARED_DEVICE=mlx5_0 ORBITTE_SHARED_PORT=1 ORBITTE_INITIAL_GID=0 \
CC=gcc CXX=g++ cargo +stable test --locked --features rdma \
  transport::rdma::context::tests::native_context_recovery::native_context_recovery_preserves_pd_mrs_and_strict_gid_selection \
  -- --exact --ignored --nocapture --test-threads=1 \
  > /path/to/external-artifacts/context-recovery.log 2>&1
```

Both automatic and pinned cases register two persistent MRs while paused,
resume through monitor queries, drain a native QP/CQ on the next pause, and
resume with a fresh connection incarnation. Receipts require unchanged native
context/PD/domain and MR counts, then zero live or retained native owners at
close. This gate cannot replace two-host payload or physical-loss evidence.

## NVIDIA memory ownership

`CudaMemory::allocate` owns device memory and its primary-context reference.
Unsafe `CudaMemory::from_raw_parts` retains the supplied allocation and context
owner, validates device type/context/ordinal/extent, and rejects host or managed
pointers. Aliases must share one `Arc<CudaMemory>`. CUDA batch registration uses
the same transactional bounds, permission and retired-owner accounting as host
registration.

`cuda::allocate_pinned_host` returns an owned `HostMemory` allocation with its
actual CUDA deallocator and context. Ordinary host registration does not pin
memory through CUDA. The feature uses generated Driver API signatures and
selective fallible loading from `libcuda.so.1`; it does not require nvcc,
libcudart, cuBLAS or NVRTC to build.

Callers exclude conflicting external access before handoff and coordinate
consumers through their own stream/lifecycle contract. GPU producers can use
the explicit readiness handoff below for device or owned CUDA-pinned host
allocations, or finish their work before handoff. Failed copy synchronization,
context restore or unpin retains the actual owner and is
observable through `cuda::retained_resources`.

### Producer-stream readiness

Unsafe `CudaMemory::record_producer_stream(stream, owner)` captures work already
queued on the actual producer stream. Call it on the producer thread, before
making the allocation available to a transfer. The supplied `Arc<dyn Send + Sync>`
must retain the live stream and all producer inputs through completion, without
forming an ownership cycle with this allocation. Coordinate exclusive external
access before queuing producer work; no transfer loan may exist, and subsequent
external access must wait for engine completion. Registration alone is neither
a readiness handoff nor a payload access.

An explicit stream must belong to the allocation's actual native context;
the caller can have a different current context or none. Special default-stream
handles resolve on the calling thread before OrbitTE pushes any context.
Graph-capturing streams are rejected. OrbitTE does not infer a framework stream
from an unrelated transfer worker. See NVIDIA's
[stream context and capture contract](https://docs.nvidia.com/cuda/archive/13.1.0/cuda-driver-api/group__CUDA__STREAM.html).

`cuda/readiness.rs::CallerSyncEvent` belongs to the actual allocation and is
shared by its registration aliases. A successful capture does not wait for the newly recorded
work. The first local copy, staging loan/copy, GPUDirect DMA loan or IPC
source/requester access consumes the recorded generation. A subsequent record
consumes its predecessor before reuse; final allocation drop also drains a
pending producer. The event is reused with timing disabled and blocking wait
enabled. Each generation is consumed once, rather than once per transfer slice.
Later work on the producer stream is not part of that event's receipt.

Readiness consumption uses `cuEventSynchronize` and can block the accessing
thread. It is not a device-side stream dependency, and a request deadline cannot
interrupt the native wait. If record fails, only the original producer stream
on the calling thread can supply a recovery drain; an older or unrecorded event
cannot. Unproven record, wait or context restoration fences the allocation and
retains its actual producer inputs/context. An admission error before capture
leaves producer work with the caller. Native event destruction is attempted
once after completion; uncertain cleanup retains the native handle/context.

Frozen TENT's
[`CallerSyncEventPool` / `syncWithCallerStream`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_transport.cpp)
records a reusable event on `cudaStreamPerThread` and waits for it before
submission. OrbitTE retains that scoped CPU event-wait boundary while accepting
the caller's explicit stream and attaching its generation to the allocation
across threads and transports. Producer readiness and transfer completion use
separate events. Consumer-stream chaining is not exposed; consumers start after
the engine's completion receipt. See [source qualification](qualification.md#cuda-producer-readiness).

### Pinned host producer handoff

With `cuda` enabled, unsafe `HostMemory::record_producer_stream(stream, owner)`
accepts memory returned by `cuda::allocate_pinned_host`. It uses the same caller
stream, producer-input retention and exclusive external-access contract above.
Ordinary owned host memory and external `from_raw_parts` wrappers are rejected;
the call does not register or copy arbitrary host bytes through CUDA.

The stream must belong to the native context selected by
`allocate_pinned_host(device, length)`. Portable pinned allocation makes bytes
usable from other CUDA contexts, but this API deliberately retains a marker in
the allocation's own context. It does not infer a foreign producer context.
See NVIDIA's [portable host allocation contract](https://docs.nvidia.com/cuda/archive/13.1.0/cuda-driver-api/group__CUDA__MEM.html).

`PinnedAllocation` lazily owns the existing `CallerSyncEvent`; registration
aliases share it. `HostMemory` keeps its access lock through capture and rejects
any existing DMA or IPC loan first. CPU read/write, staging byte/access loans,
host RDMA admission and local pinned-host IPC access consume readiness before
payload access. TCP therefore consumes the same marker on either side of
READ/WRITE. Each generation waits once and reuses its event. Internal staging
allocations without a producer skip this optional state and create no new event.

Final native release consumes pending readiness before `cuMemFreeHost` or
`cuMemHostUnregister`. Failed readiness keeps the actual pinned backing and
producer inputs alive; release is attempted once. Successful readiness allows
input release even if later event destruction or unpin fails. The shared wait
remains CPU-blocking and cannot be interrupted by transfer deadlines.
This extends TENT's caller-sync event boundary to OrbitTE's actual pinned owner;
it adds neither a new transport nor a separate readiness state machine.
See [source qualification](qualification.md#pinned-host-producer-readiness).

### Copy completion and reuse

Each `CudaMemory` lazily owns one nonblocking CUDA stream and completion event.
READ/WRITE staging uses async Driver copies, records the event after the copy,
and waits for that event before exposing host bytes or releasing staging. The
allocation mutex serializes aliases and event reuse. Other streams are not part
of that completion receipt; caller producer handoff remains explicit.

Failed copy submission or event recording attempts to drain this same stream.
If drain cannot be proven, the operation returns `Unquiesced` and retains its
actual allocation, pinned staging and stream/event owners. Later access cannot
reuse them. `cuda::retained_resources` includes `streams` and `events`; cleanup
failure preserves the corresponding context and unresolved handles. Initial
allocation zeroing still uses context synchronization.

This follows CUDA's [event capture and completion semantics](https://docs.nvidia.com/cuda/archive/13.0.2/cuda-driver-api/group__CUDA__EVENT.html):
only an event successfully recorded after the copy proves its completion.
Destroying an event or stream is cleanup, not a DMA completion receipt.

Run the explicit byte/guard gate only on allocated, locked GPUs:

```sh
cargo build --locked --release --features cuda --example cuda_copy
# Ordinals refer to the selected process-visible devices.
target/release/examples/cuda_copy 0 1
```

The runner checks three unaligned sizes, eight concurrent callers and four
rounds per caller, then releases all owners. It qualifies local staging only;
GPUDirect and interprocess GPU sharing use their own separate installed gates.

## GPUDirect RDMA ownership

Build with `--features rdma,cuda`, set `MLX5_SCATTER_TO_CQE=0` before process
startup, and select `RdmaConfig.cuda_memory = RdmaCudaMemory::Direct`. Keep the
provider environment unchanged while any engine is alive. Initialization rejects
an absent or different setting: mlx5 scatter-to-CQE can otherwise copy small
RDMA READ results into the SGE from the CPU. This is incompatible with ordinary
device pointers ([provider guidance](https://github.com/linux-rdma/perftest#readme)).
No process-wide environment mutation or automatic staging fallback occurs.

`CudaMemory` validates the device allocation and context. Before native
registration it sets `CU_POINTER_ATTRIBUTE_SYNC_MEMOPS` once and checks device
GPUDirect write-ordering/host-flush capabilities. `LocalBufferManager` publishes
one local-only GPU MR per selected context transactionally, under the owning
CUDA context. Requester slices borrow these persistent MRs. Allocation-wide
`DmaLease` ranges reject overlapping copies or grants across registration views
and engines; they never expose a GPU pointer as a CPU byte slice.

NVIDIA [PeerDirect does not support Memory Windows](https://docs.nvidia.com/networking/display/mlnxofedv23102131201lts/optimized%2Bmemory%2Baccess).
A GPU source grant therefore owns a fresh PD, exact-range MR with only the
requested remote permission, and a fresh QP. Its RQ message MR uses that same
PD. The device context, CQ lanes and transport Workers remain shared. Protocol
profile 2 binds the separate private-PD connection identity and uses fenced
ordinary SEND completion; it never pretends that a normal MR supports remote
invalidation. The public NIC path still selects and validates the device route.

Source ACK follows actual RQ completion and matching DONE, QP destruction,
payload/message MR deregistration, private PD deallocation, and GPU write
visibility. Requester completion likewise establishes visibility before
releasing its DMA range. Hardware owner ordering is used when advertised;
otherwise `cuFlushGPUDirectRDMAWrites` targets the current owning context.
Any uncertain native cleanup, flush or context restoration retains the real
allocation, range loan and associated admission. GPU producer readiness must be
handed off through `record_producer_stream` or completed by the caller before
DMA admission. Consumers still coordinate through their own lifecycle;
SYNC_MEMOPS does not wait for arbitrary kernels ([CUDA ordering contract](https://docs.nvidia.com/cuda/gpudirect-rdma/#synchronization-and-memory-ordering)).

This follows frozen TENT's `LocalBufferManager` → `RdmaContext::registerMemReg`
registration and `Workers`/slice progress boundaries. TENT publishes native MR
keys; OrbitTE's exact per-request authorization additionally needs private PDs
for GPU source grants. GPU source MRs/QPs are currently per-grant and are not
cached. Host source MW/QP reuse is retained; a GPU requester can reuse a pair
when the source is direct host memory. Registration failure on any context fails
the batch publication. The [bounded installed H20/RoCE profile](qualification.md#gpudirect-rdma)
covers two contexts and preserves an unqualified host-wide context inventory
limit. Other GPU/provider, native GPU fault and performance claims remain open.

The explicit `hardware_peer` gate accepts `ORBITTE_RDMA_CUDA_MEMORY=direct` and
optional `ORBITTE_RDMA_SLICE_BYTES` for 1/17/32/64-byte controls. The existing
supervisor owns GPU/RNIC locks and process reaping. Its passive native observer
records real MR type/flags, QP PD and payload WR addresses; `gdr.py` checks those
against declared allocations and full-buffer/guard results. Evidence belongs
outside the checkout; see [qualification](qualification.md).

Build the installed GPU consumer from a clean commit, then pass both its
manifest and passive observer to the controller:

```sh
CC=gcc CXX=g++ python3 tests/hardware/build_installed.py --consumer gpu \
  --toolchain stable --output /external/installed-gpu-stable
python3 -B tests/hardware/run.py --profile /external/gpu-profile.json \
  --binary /external/installed-gpu-stable/installed/bin/orbitte-hardware-peer \
  --installation /external/installed-gpu-stable/manifest.json \
  --gdr-observer /external/installed-gpu-stable/liborbitte_cq_fault.so \
  --output /external/runs/gpu-direct-001
```

Use `--toolchain 1.85.0` and a new output directory for the MSRV consumer. Each
GPU case declares `cuda_memory: "direct"`, `transport: "rdma"`, and memory
kinds `cpu`/`cuda:0`, with at least one GPU. The controller sets the required
scatter environment before spawning the native process. Nodes must have
distinct pinned hostnames and boot IDs. Small cases use `payload: 4096` and
`slice_bytes` of 1, 17, 32 or 64. The two-rail folded-tail profile adds `rails`
and both original NIC locks, `payload: 8388768`, `rdma_block_size: 1048576`,
and `expected_grants: 12`. Each native process runs three READ requests followed
by three WRITE requests. Run both physical directions and both values of
`rdma_events`; preserve failed runs. The observer does not simulate transfers
or change provider return values. This profile proves correctness and ownership,
not bandwidth or GPU-native fault recovery.

## Run a native profile

Resolve the current platform attempt and collect fresh host/provider/device
inventory before choosing execution nodes. Use an external JSON profile with
[`tests/hardware/run.py`](../tests/hardware/run.py):

```sh
python3 tests/hardware/run.py --profile /external/profile.json \
  --binary /external/build/release/examples/hardware_peer \
  --output /external/runs/run-001
```

The basic profile contains `ssh_config`, `rnic`, `gid_index`, `nodes` and
`cases`. Each node supplies an existing SSH `alias`, exact `hostname` and
`boot_id`, a `bind` address and the original `nic_lock`. GPU cases additionally
supply `gpu_lock` and `gpu_uuid`; only that UUID is exposed, so the memory kind
uses ordinal 0. Optional node `env` freezes native library paths.

Each basic case selects `source`, `requester`, `source_kind`, `requester_kind`,
`transport`, `payload` and `seed`. The `hardware_peer` example supports `cpu`,
`pinned:0` and `cuda:0`, compares complete bytes and guards, and checks drain
after noncontiguous READ/WRITE operations. Optional `host_memory` and per-peer
overrides select Direct or Staged. This correctness pilot is separate from
the installed and fault profiles.

For `orbitte-multirail` and `orbitte-installed-host`, profiles supply exactly
two distinct physical `rails` and each node's `nic_locks` map covering both.
The runner validates each mode's fields and exact native test selector before
deployment. Use the maintained [installed host recipe](installed-host.md) for
packaged public consumers and [native cleanup recipe](native-cleanup.md) for
instrumented libtests. Their artifacts and proof obligations are separate.

The controller verifies host/boot identity and binary hashes, deploys into a
unique `/var/tmp/orbitte-runs` directory, and collects actual provider mappings.
Supervisors hold nonblocking resource locks until `waitpid` confirms their
owned children's exit. A missed reap, broken supervision or missing receipt
fails cleanup qualification. Only owned children can be terminated; the runner
does not change physical NIC configuration or platform resource allocation.
Keep raw runs, manifests and failed controls outside the source checkout.

## Reference and historical records

The frozen TENT reference remains
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`. See the
[owner mapping](architecture.md#rdma-resource-owners) for `RdmaTransport`,
contexts, buffer managers, endpoints, workers and monitor responsibilities.
Exact-range MW authorization, source receipts and conservative failed-cleanup
retention are explicit Rust contracts; architectural correspondence does not
establish TENT wire compatibility.

The [pre-cleanup guide](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/rdma-cuda.md)
preserves all earlier stage-specific recipes, source revisions and evidence
locations, including historical OrbitKV experiments. Those records describe
their original revisions. [Qualification](qualification.md) indexes the current
evidence without treating old filenames, protocols or open-item lists as current
contracts.
