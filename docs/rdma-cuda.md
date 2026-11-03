# RDMA and CUDA

This guide describes the current source. See [Build](build.md) for provider
dependencies, [Architecture](architecture.md#source-reading-order) for the code
layout and [Qualification](qualification.md) for executed hardware profiles.

## Build and select a path

The default crate supports CPU/TCP without a native SDK. The independent `rdma`
and `cuda` features enable libibverbs and CUDA Driver API bindings; runtime use
still needs the actual driver, provider and devices. All engine, protocol,
progress and resource owners are Rust. A small C shim only queries port rates
from the installed libibverbs headers.

A request names `Transport::Tcp`, `Transport::Rdma` or `Transport::Nvlink`, or
uses `Transport::Unspec` for [transport selection](transport-selection.md). Each
peer's memory policy then picks the backing:

| Memory and policy | Payload path |
| --- | --- |
| Host, `RdmaHostMemory::Direct` (default) | Application range ↔ RC RDMA ↔ peer registered range |
| Host, `RdmaHostMemory::Staged` | Application bytes ↔ private registered host staging ↔ RDMA |
| NVIDIA device, `RdmaCudaMemory::Staged` (default) | Device ↔ owned host staging ↔ RDMA |
| NVIDIA device, `RdmaCudaMemory::Direct` | Device MR ↔ RC RDMA ↔ peer registered range |
| NVIDIA device over TCP | Device ↔ owned host staging ↔ socket |
| Host over TCP | Application bytes ↔ owned host buffers ↔ socket |

A failed direct MR registration returns an error; it never switches to
staging. Same-host GPU sharing uses the [CUDA IPC transport](cuda-ipc.md).
TCP and staged RDMA share the [CUDA staging pool](staging.md), and
[remote staging](staging.md#serial-remote-staging) adds Pin/Delegate/Unpin over
the same control channel.

## Device, port, GID and memory topology discovery

Set `EngineConfig.rdma` to an `RdmaConfig`:

| Configuration | Behavior |
| --- | --- |
| `RdmaConfig::default()` | Discover permitted devices, ports and valid GIDs |
| `RdmaConfig::new("mlx5_0")` | Strictly pin that device, port 1 and GID index 0 |
| `device`, `port`, `gid_index` | `Some(...)` pins the field; `None` lets discovery choose |
| `rdma_whitelist`, `rdma_blacklist` | Filter devices before opening; a nonempty whitelist wins |
| `device_mask` | Restrict dispatch to stable NIC IDs (up to 64) |
| `preferred_numa_node` | CPU NUMA hint overriding the owner's location |
| `strict_local_numa` | Reject known cross-NUMA NICs (default `false`); unknown locality stays eligible |
| `priority_matrix` | Inline TENT matrix JSON; wins over `custom_json_path` |
| `custom_json_path` | TENT matrix or native `nics`/`mems` file, read at initialization |
| `scheduling` | TENT `SchedulingParams` for bandwidth-aware selection |

Discovery prefers active routes and keeps usable paused ports. Each context owns
its device, protection domain (PD), key history and address; a failed slot never
renumbers other NIC IDs. Explicit pins never fall back to another device, port
or GID.

With `cuda` as well, discovery lists CUDA-visible devices without creating
contexts. Each `cuda:N` entry carries its PCI bus ID, optional NUMA node and
three GPU-to-RNIC tiers: nearest known PCI bridge, other same-NUMA NICs, then the
rest. Missing sysfs distance never counts as nearest. No CUDA driver leaves the
host inventory usable.

`DeviceSelector` compiles location priorities once. Each request supplies its
owner's location (`cpu:N`, `cuda:N` or unknown `*`). Selection intersects the
mask, healthy contexts, compatible peer paths and strict-NUMA eligibility, then
picks the lowest estimated service time. A matching NIC name or address family
does not prove physical reachability, and strict locality cannot be overridden
by a mask. See [qualification](qualification.md#cuda-topology-and-rdma-selection).

## Bandwidth-aware RDMA selection

Smart selection, following TENT's `DeviceSelector`, is on by default. For each
eligible NIC:

```text
score = (charged_inflight_bytes + slice_bytes) / selection_bandwidth_bytes_per_second
        * numa_tier_weight
```

A single slice takes the lowest score, with rotating ties. A multi-slice parent
follows TENT's `selectMultiPath`: each NIC gets `floor(weight / total_weight *
slice_count)` slices with `weight = 1 / (score + 1e-12)`, and leftovers go to the
best NIC. Tier weights default to `[1, 5, 10]`. Every 100th multi-slice parent
probes eligible NICs round-robin; probes never reach failed rails. Set
`scheduling.smart_selection = false` for plain round-robin within the first tier.

All of a parent's byte charges commit under one selector lock and move to each
grant's `DeviceLease`; abandonment returns only unconsumed charges. Eligibility
rules stay hard limits: weights can favor an idle lower-ranked NIC but never
admit an excluded one.

Port rates come from `ibv_query_port` at initialization and on each monitor
scan, preferring the extended speed encoding (including XDR) and the optional
`ibv_query_port_speed` LAG rate. A failed query keeps the last effective value.
Unknown or out-of-range rates use `default_bandwidth_gbps` (400). Topology
attributes `rdma.link_speed_mbps` and `rdma.effective_speed_mbps` expose the
result. See [qualification](qualification.md#rdma-effective-link-rate).

Each NIC keeps two independent estimates, both clamped to 0.1–10× the seed:

- **Selection EWMA:** successful payload bytes over the time from first post to
  last payload CQE (old-value weight 0.01).
- **Transmit bandwidth:** completion bytes over time with posted payload debt
  outstanding (old-value weight 0.9, 10 ms updates, 50 ms window).

Failed or uncertain posts invalidate their samples; possible native debt stays
charged until completion or drain. `TransferEngine::nic_load_stats()` returns
these estimates (bytes/s), link rates, charged, posted and completed bytes for
initialized NICs without probing devices. They are local requester observations.
Selection adds no driver query or heap allocation per slice. See
[bandwidth](qualification.md#rdma-bandwidth-selection) and
[multipath](qualification.md#rdma-weighted-multipath) evidence.

## Completion and transport retry

RDMA reports a replayable READ failure only after all affected QPs and WRs
drain and their loans are released. A CQE error followed by failed retirement
retains the backing and admission. A partially posted WRITE keeps its unknown
effect even after local QP retirement. A transport switch consumes the same
physical owner’s retry budget. See [transport attempts](async-batches.md#transport-attempts).

## Request priority and bandwidth arbitration

`request.with_priority(...)` selects `High` (the default, as in TENT), `Medium`
or `Low`. TCP and NVLink accept every priority without scheduling by it. On
RDMA, the effective priority follows ordinary grants, multirail slices and
remote staging CROSS work into the worker queues.

Each RDMA worker collects ready payloads from all its contexts and posts High,
then Medium, then Low, FIFO within a tier. Requests within a batch keep their
order, posted DMA is never preempted, and completion, receive, MW and cleanup
work bypass the priority queue. A full QP does not block another ready QP.

With `RdmaConfig::deadline_bw_arbitration = true` (default `false`), candidates
sharing a NIC and priority are ordered by
`(posted_bytes + unposted_grant_bytes) / transmit_bandwidth / remaining_deadline`.
As in TENT, the first 64 picks are greedy and the tail uses a fixed backlog with
FIFO ties; unknown bandwidth keeps FIFO. This orders work; it is not a rate cap
or a deadline guarantee. OrbitTE groups by local NIC across peers because its
workers share one CQ budget, whereas TENT also groups by remote segment. Sorting
buffers are reused, with no per-dispatch allocation. See
[qualification](qualification.md#rdma-request-priority).

Admission uses the same `DeadlineMlu` formula before posting. It counts
dispatched, unfinished RDMA payload bytes and uses the sum of current transmit
estimates for healthy NICs allowed by the physical owner’s mask. An effective
link rate supplies the initial estimate. Measured transmit bandwidth can
subsequently replace it. If an eligible NIC has no rate or transmit sample,
or recovery owns the route snapshot, admission leaves the owner eligible.
The RDMA worker continues to arbitrate its own posted backlog. A pre-dispatch
drop creates no grant, connection or WR. See
[queue configuration](async-batches.md#runtime-admission-and-dispatch-window).

## Priority promotion and shared slots

`priority_promotion_timeout` defaults to 10 ms, as in TENT. Workers check
lower-priority ready payloads at most once per millisecond and promote one tier
per pass, keeping the original enqueue age. By default
(`priority_promotion_per_entry = false`, TENT's behavior) an aged head promotes
its whole tier; set it to `true` to promote only entries that individually aged
out. Aging lets old work progress against new High arrivals; it does not
guarantee deadlines or isolate tenants.

Cooperating processes on one host can share priority slots:

```rust
use std::time::Duration;
use orbit_te::RdmaConfig;

let config = RdmaConfig {
    priority_promotion_per_entry: true,
    shared_quota_shm_path: Some("/orbitte-rdma-slots".into()),
    slot_rotation_interval: Duration::from_millis(2),
    ..Default::default()
};
```

The 2 ms slots repeat High-only, High+Medium, then all priorities, matching
TENT's `SharedSlotManager::canSend`. Only payload posts are gated. This is a
worker-pass gate, not a bandwidth cap or exclusive turn. All participants need
the same name, interval, effective UID, `/dev/shm` mount and time namespace.
OrbitTE derives slots from the monotonic clock instead of TENT's rotation thread,
and uses its own object format rather than TENT's ABI. The object persists after
exit: stop **all** participants before removing
`/dev/shm/<name-without-leading-slash>`. See
[qualification](qualification.md#rdma-promotion-and-shared-slots).

## Custom topology and priority matrices

`priority_matrix` accepts TENT's classic matrix. Each row is
`[preferred NIC names, available NIC names]` (rank 0, rank 1):

```rust
let rdma = orbit_te::transport::rdma::RdmaConfig {
    priority_matrix: Some(r#"{
        "cpu:0": [["mlx5_0"], ["mlx5_1"]],
        "cuda:0": [["mlx5_1"], ["mlx5_0"]]
    }"#.into()),
    ..Default::default()
};
```

Names must exist in the discovered inventory after filters; `cuda:N` uses the
CUDA-visible ordinal. Only listed NICs are eligible for a row, an empty rank
disables a location, and masks or strict NUMA can only narrow the candidates. An
explicit `*` row covers unlisted locations; otherwise the classic matrix
synthesizes `*` from all named NICs.

`custom_json_path` also accepts TENT's native format:

```json
{
  "nics": [{"name": "mlx5_1", "type": 0}, {"name": "mlx5_0", "type": 0}],
  "mems": [{"name": "cuda:0", "type": 1,
            "device_list": {"rank0": [0], "rank1": [1], "rank2": []}}]
}
```

Indices refer to the file's `nics` order. Memory type, when given, must match
discovery (`0` host, `1` CUDA, `4` for `*`). JSON never replaces discovered
PCI/NUMA metadata or capabilities. Both inputs are limited to 1 MiB, and invalid
explicit configuration fails initialization (TENT warns and falls back to
autodiscovery). Ranks are frozen after initialization. See
[qualification](qualification.md#custom-topology-input).

## Eager publication and cross-rail slice completion

With host Direct policy, registration prepares MRs on every usable context,
including paused ports, before the range becomes visible: one local-only MR, or
for remote access a local-only MR plus a separate `LOCAL_WRITE|MW_BIND` parent.
Neither parent key is exported. Visibility commits only after every MR is ready;
failure retires the new copies, and failed rollback retains its MR, backing and
budget. Final unregister hides the range while admitted work keeps its MR borrow
through drain. Each grant locks its exact range, so disjoint ranges proceed and
conflicting CPU or DMA access is rejected across aliases.

Slicing follows TENT: a 64 KiB base block, at most 64 slices for host-local READ
and 32 for WRITE or device-local READ, adaptive blocks for large requests and a
short tail folded into its predecessor. Each slice selects its own rail. The
source reserves the whole parent's grant and receive capacity before publishing
authority, and the parent succeeds only after every slice and its source receipt
complete. Earlier slices may already have changed bytes, so a multirail WRITE is
not atomic.

## Connections, grants and completion

Peers share a fixed-width reusable QP array per local context, remote
process/PD/NIC, role and capability; width is the smaller `num_lanes`. Each cold
builder reserves a slot before creating a QP, and waiters never hold store locks
across I/O. Concurrent grants share published QPs. If either peer stages, or the
source uses direct GPU memory, connections are per grant. Every RDMA phase binds
process and connection incarnations, the exact request and a grant generation.

For host or staged source memory, the source binds a one-use Type-2B memory
window to the exact range and direction and posts a completion receive before
publishing the grant; Type-2B support is required. The PD keeps a bounded key
history so an old key cannot regain authority, parking exhausted MW indices up
to `max_window_key_indices`.

The requester posts signaled READ/WRITE work and, after every payload CQE
succeeds, a fenced `SEND_WITH_INV`. The source accepts completion only when its
RQ receives the exact 141-byte binding and invalidated key, then cleans the MW
and grant, sends ACK and publishes READY. `RdmaQp` tracks exact outstanding WR
identities, so old or duplicate completions cannot repay current debt. Only an
explicit pre-authority cache miss permits a reconnect; ambiguous writes are never
replayed. This protocol is OrbitTE's own, not TENT wire compatibility.

## Workers, CQ lanes and budgets

One transport-level `Workers` pool serves all contexts. `num_lanes` sets the data
worker count and SQ/RQ CQ pairs per context; a reused QP stays on its lane.
Workers bind to allowed NUMA CPUs. `RdmaProgressMode::Polling` is the default;
`EventDriven` arms both CQs and wakes on native events. One `Monitor` consumes
device and CQ events and scans contexts each second.

| Budget | Default | Charged resource |
| --- | --- | --- |
| `max_endpoints` | 64 | Physical QPs, including retained connections |
| `max_grants` | 64 | Whole-parent grant reservations |
| `max_send_wr` | 16 | Unresolved signaled sends per QP |
| `max_cqes` | 1024 | Each CQ's capacity |
| `max_direct_bytes`, `max_staging_bytes` | 64 MiB each | Active direct loans and private staging |
| `max_cached_mrs`, `max_cached_mr_bytes` | 64 / 64 MiB | Full-view MR copies across contexts |
| `max_window_key_indices` | 65,536 | MW key history per PD |

Creating, closing and retained resources stay charged. These limits govern
admission; they are not throughput or fairness guarantees.

## Failure, drain and resource ownership

A failed post releases only a proven unaccepted suffix. An unknown `bad_wr`,
invalid CQE, timeout or CQ error keeps the work charged and fences admission;
healthy siblings still finish their own cleanup. Parent cleanup has one
30-second destruction window, separate from the data deadline, and a timeout
proves nothing. Failed native teardown keeps its resources, backing and charges
and is never retried by another close or Drop. A fully drained progress owner can
be replaced while keeping the context's PD and MRs.

`TransferEngine::close(timeout)` fences admission, waits for accepted work,
retires connections and MRs, drains CQs and workers, then destroys PDs and
contexts; a later close can keep waiting. `close_source` and `close_requester`
are directional and do not tear down PDs or contexts. `Error::Unquiesced` means
physical completion or destruction is unproven, so batch capacity, registrations
and DMA loans stay retained. See [native cleanup](native-cleanup.md) and
[async batches](async-batches.md).

## Rail health

The requester tracks errors per peer epoch and local/remote NIC pair. The
registered memory's path table, static `rail_topology` (`all` and `direct`),
the policy device mask and current health determine which NIC can carry a
slice. An empty allowed set fails before payload. Port generations from the
existing monitor fence old observations. QP retirement and grant drain still
belong to the original CQ lane.

Defaults are three failures inside ten seconds, a 30-second cooldown and a
one-second exploratory probe interval. Set `RdmaConfig::rail_error_threshold`,
`rail_error_window`, `rail_cooldown` and `rail_probe_interval` to change them.
Zero probe interval allows an immediate trial. Looking up a mask never arms a
probe. Only one admitted trial can run on a paused pair. Early failure leaves
the cooldown intact. Expiry admits one half-open trial. A failed half-open
trial doubles the cooldown up to five minutes. Only a current trial’s positive
receipt reopens the pair. Cancelling an unposted trial returns its slot.

`RdmaStats` exposes errors, open pairs, probes and recoveries. NIC load stats
include each local NIC’s rail errors, probes and open-pair count. A process
retains at most `max_rail_pairs` pair identities. An exhausted table preserves
active and unproven owners and returns `ResourceExhausted`. See the
[installed verification queue](qualification.md#verification-queue).

## Periodic monitor recovery

The monitor queries selected routes even without traffic. A changed or inactive
address fences the route; old builders, QPs and CQs must drain before a validated
route is published again. Recovery keeps the PD, key history and MRs.
`monitor_ticks`, `route_queries`, `route_recoveries` and `progress_recoveries`
count scans, queries, resumed routes and reclaimed lanes. Fatal device events and
retained cleanup failures stay fenced.

### Initially paused contexts and GID reselection

An initially inactive port with valid GID metadata keeps a paused context and
PD. Host Direct registration still prepares its MRs, while path selection
rejects that route and healthy rails stay eligible. On recovery, automatic GID
configuration (`gid_index: None`) reranks valid GIDs on the same device and port;
`Some(index)` stays pinned. This follows TENT's `RdmaContext::enable` and
`refreshAddress`; hotplug and cross-rail retry are out of scope.

The focused native gate runs on one authorized mlx5 RoCE RNIC with real
PD/MR/QP/CQ operations and an overridden port state; it changes no physical port
and is not a two-host or physical-loss test. Choose an `ORBITTE_INITIAL_GID`
different from the automatically preferred index:

```sh
ORBITTE_RDMA_LOCK=/path/to/authorized-rnic.lock \
ORBITTE_SHARED_DEVICE=mlx5_0 ORBITTE_SHARED_PORT=1 ORBITTE_INITIAL_GID=0 \
CC=gcc CXX=g++ cargo +stable test --locked --features rdma \
  transport::rdma::context::tests::native_context_recovery::native_context_recovery_preserves_pd_mrs_and_strict_gid_selection \
  -- --exact --ignored --nocapture --test-threads=1 \
  > /path/to/external-artifacts/context-recovery.log 2>&1
```

See [qualification](qualification.md#focused-native-recovery).

## NVIDIA memory ownership

`CudaMemory::allocate` owns device memory and its primary-context reference.
Unsafe `CudaMemory::from_raw_parts` adopts an existing allocation after
validating type, context, ordinal and extent; host and managed pointers are
rejected, and aliases must share one `Arc<CudaMemory>`. CUDA batch registration
uses the same transactional rules as host registration.
`cuda::allocate_pinned_host` returns owned pinned `HostMemory`. The feature
loads Driver API symbols from `libcuda.so.1` at runtime and needs no nvcc,
libcudart or other CUDA libraries to build.

Callers exclude conflicting external access before handoff. GPU producers either
finish their work first or use the readiness handoff below. Failed copy
synchronization, context restore or unpin keeps the owner, visible through
`cuda::retained_resources`.

### Producer-stream readiness

Unsafe `CudaMemory::record_producer_stream(stream, owner)` captures work already
queued on the producer's stream. Call it on the producer thread before handing
the allocation to a transfer. `owner` must keep the stream and all producer
inputs alive until completion, without an ownership cycle. The stream must
belong to the allocation's native context; default-stream handles resolve on the
calling thread, and graph-capturing streams are rejected.

`cuda/readiness.rs::CallerSyncEvent` belongs to the allocation and is shared by
its aliases. The first local copy, staging loan, GPUDirect DMA loan or IPC access
waits for the recorded generation once, with `cuEventSynchronize`. That wait
blocks the CPU and cannot be interrupted by a request deadline; it is not a
device-side stream dependency. An unproven record, wait or context restore fences
the allocation and keeps its producer inputs. TENT's `CallerSyncEventPool` /
`syncWithCallerStream` waits on `cudaStreamPerThread`; OrbitTE accepts the
caller's explicit stream and carries the generation across threads and
transports. Consumers start after the engine's completion receipt; consumer
stream chaining is not exposed. See
[qualification](qualification.md#cuda-producer-readiness).

### Pinned host producer handoff

With `cuda`, unsafe `HostMemory::record_producer_stream(stream, owner)` accepts
memory from `cuda::allocate_pinned_host` under the same contract; ordinary or
external host memory is rejected. The stream must belong to the allocation's own
context. CPU read/write, staging, host RDMA and local pinned IPC access consume
readiness before touching payload, and final `cuMemFreeHost` or
`cuMemHostUnregister` waits for any pending generation. Internal staging buffers
without a producer create no event. See
[qualification](qualification.md#pinned-host-producer-readiness).

### Copy completion and reuse

Each `CudaMemory` lazily owns one nonblocking stream and completion event.
Staging copies are asynchronous, record the event after the copy and wait for it
before exposing host bytes or releasing staging. If submission or recording
fails, OrbitTE drains that stream; if drain is unproven, it returns `Unquiesced`
and keeps the allocation, staging and handles. Destroying an event or stream is
cleanup, not completion evidence.

Run the byte/guard gate only on allocated, locked GPUs:

```sh
cargo build --locked --release --features cuda --example cuda_copy
# Ordinals refer to the selected process-visible devices.
target/release/examples/cuda_copy 0 1
```

It checks three unaligned sizes with eight concurrent callers for four rounds
each. It covers local staging only.

## GPUDirect RDMA ownership

Build with `--features rdma,cuda`, set `MLX5_SCATTER_TO_CQE=0` before the process
starts, and select `RdmaConfig.cuda_memory = RdmaCudaMemory::Direct`.
Initialization rejects any other setting, because mlx5 scatter-to-CQE can copy
small READ results with the CPU, which is incompatible with device pointers
([provider guidance](https://github.com/linux-rdma/perftest#readme)). There is no
automatic fallback to staging.

Before registration, OrbitTE sets `CU_POINTER_ATTRIBUTE_SYNC_MEMOPS` and checks
GPUDirect write-ordering support. `LocalBufferManager` publishes one local-only
GPU MR per context under the owning CUDA context, and requester slices borrow
these persistent MRs. Allocation-wide `DmaLease` ranges reject overlapping copies
or grants and never expose GPU pointers as CPU slices.

[PeerDirect does not support memory windows](https://docs.nvidia.com/networking/display/mlnxofedv23102131201lts/optimized%2Bmemory%2Baccess),
so each GPU source grant owns a fresh PD, an exact-range MR with only the
requested permission and a fresh QP, and completes with an ordinary fenced SEND.
Source ACK follows RQ completion, QP and MR teardown, PD deallocation and GPU write
visibility (hardware ordering, or `cuFlushGPUDirectRDMAWrites`). Uncertain
cleanup or flush keeps the allocation, range loan and admission. TENT publishes
native MR keys directly; OrbitTE's exact per-request authorization needs private
PDs for GPU source grants, which are therefore not cached. See
[qualification](qualification.md#gpudirect-rdma).

The `hardware_peer` gate accepts `ORBITTE_RDMA_CUDA_MEMORY=direct` and optional
`ORBITTE_RDMA_SLICE_BYTES` (1, 17, 32 or 64). Its passive observer records MR
flags, QP PDs and payload WR addresses, which `gdr.py` checks against the
declared allocations and full buffers:

```sh
CC=gcc CXX=g++ python3 tests/hardware/build_installed.py --consumer gpu \
  --toolchain stable --output /external/installed-gpu-stable
python3 -B tests/hardware/run.py --profile /external/gpu-profile.json \
  --binary /external/installed-gpu-stable/installed/bin/orbitte-hardware-peer \
  --installation /external/installed-gpu-stable/manifest.json \
  --gdr-observer /external/installed-gpu-stable/liborbitte_cq_fault.so \
  --output /external/runs/gpu-direct-001
```

Use `--toolchain 1.85.0` and a new output directory for the MSRV build. Each case
declares `cuda_memory: "direct"`, `transport: "rdma"` and `cpu`/`cuda:0` memory
kinds with at least one GPU. Small cases use `payload: 4096`; the two-rail
folded-tail case adds `rails`, both NIC locks, `payload: 8388768`,
`rdma_block_size: 1048576` and `expected_grants: 12`. Run both directions and
both `rdma_events` values, and keep failed runs.

## Run a native profile

Collect fresh host, provider and device inventory first, then pass an external
JSON profile to [`tests/hardware/run.py`](../tests/hardware/run.py):

```sh
python3 tests/hardware/run.py --profile /external/profile.json \
  --binary /external/build/release/examples/hardware_peer \
  --output /external/runs/run-001
```

A profile contains `ssh_config`, `rnic`, `gid_index`, `nodes` and `cases`. Each
node has an SSH `alias`, exact `hostname` and `boot_id`, a `bind` address and its
`nic_lock`; GPU cases add `gpu_lock` and `gpu_uuid`, and optional `env` freezes
library paths. Each case sets `source`, `requester`, `source_kind`,
`requester_kind` (`cpu`, `pinned:0` or `cuda:0`), `transport`, `payload` and
`seed`, with optional `host_memory` overrides. Multirail and installed-host modes
need two physical `rails` and a `nic_locks` map; see the
[installed host recipe](installed-host.md) and
[native cleanup recipe](native-cleanup.md).

The controller checks host identity and binary hashes, deploys to a unique
`/var/tmp/orbitte-runs` directory and records provider mappings. Supervisors hold
resource locks until `waitpid` confirms their children exited; a missed reap or
missing receipt fails cleanup. The runner never changes NIC configuration. Keep
runs and failed controls outside the checkout.

## Reference

The frozen TENT reference is `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`; see the
[owner mapping](architecture.md#rdma-resource-owners). Exact-range MW
authorization, source receipts and failed-cleanup retention are OrbitTE
contracts. The
[pre-cleanup guide](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/rdma-cuda.md)
keeps the earlier stage-specific recipes and evidence locations.
