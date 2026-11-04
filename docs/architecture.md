# Architecture

OrbitTE follows frozen Mooncake TENT's engine, transport, context, registered
buffer, endpoint and worker boundaries, implemented as Rust resource owners.
Architectural alignment does not by itself establish behavior, ABI, wire or
performance parity. See [capabilities](capabilities.md) for support and the
[completion plan](completion-plan.md#p4-remaining-delivery) for open work.

:::{image} _static/architecture.svg
:alt: OrbitTE network transports and the implemented DRAM and GPU paths to local SSD
:class: dark-light
:::

## Engine and transport boundary

Callers build a `TransferRequest` with a transport (or `Transport::Unspec`) and
two absolute addresses; the operation is chosen at submission, and an optional
engine-resolved QoS handle sets priority and caps. Public request, transport and
error types are non-exhaustive.

`TransferEngine` owns registration, batches, admission and directional shutdown.
`RegisteredMemory` retains the actual backing, and aliases share allocation-level
access coordination. `runtime::control_plane::ControlService` owns peer lookup,
request framing and source handlers, and dispatches payload work to the sibling
TCP, RDMA and NVLink transports. RDMA negotiates paths, connections and exact
memory grants over the control connection and moves bytes through verbs; see
[wire boundaries](tent-parity.md#wire-and-consumer-boundaries).

HP-TCP is built into the CPU transport set and is disabled by default. When
enabled, it replaces the ordinary TCP payload and retains the control service.
Each configured worker owns a single-thread Tokio reactor, the Rust counterpart to TENT’s per-worker Asio context. Persistent lanes
keep their worker affinity and use readiness-driven sockets with TCP_NODELAY.
Physical owners retain registration and task/byte admission through the last
slice receipt. The typed configuration, frame layout, static rail limits and
generation fence are described in [HP-TCP](hp-tcp.md).

SHM follows the shared allocator and synchronous copy in frozen TENT. TENT
can also copy a caller-supplied path from `MemoryOptions`. OrbitTE publishes
only allocations whose mapping and fd it owns, so an ordinary host buffer
cannot grant access to an unrelated inode. OrbitTE also publishes the inode,
allocation offset and registration grant. The source holds Linux OFD locks for
the permitted directions, and every requester holds a separate activity lock
through its copy. Those receipts fence a cached mapping across unregister or
process death. Local/native loans and peer copies coordinate on inode byte
ranges. Mapping failure precedes payload I/O, and failed unmap quarantines the
mapping and its charge. POSIX and directory-backed paths use the same owners.
See [shared memory](shm.md).

## Local file segment ownership

TENT's `SegmentManager` maps a name to one handle, `makeFileRemote` describes one
regular file with base zero, and io_uring passes the request offset straight to
positioned I/O. `segment/file.rs` follows that split: a bounded per-engine table
shares one entry per name, the size is cached for TENT's one hour, and refresh and
range admission read the size from the open fd, so replacing the pathname cannot
change the file an admitted operation uses. A lease keeps the fd until its I/O
finishes; close fences all aliases before waiting, and a timeout keeps the entry
charged until close is retried. See [file segments](storage.md).

An idle engine retains no storage thread or ring after the setup probe.
For DRAM/file payloads the caller or existing batch worker owns the io_uring,
iovecs, host range loans and bounce storage through CQE drain. File and peer
targets share admission and batch progress. CUDA/file submissions use the same
worker and ring for successive rounds of pinned chunks from `ProxyManager`.
The whole GPU range stays loaned through file receipts and CUDA copy events.
GDS binds a cuFile handle to the retained file entry and pins the CUDA
registration across queued and running batches. Its caller owns status polling
and quarantine. See the [file guide](storage.md) for completion and retention.

## Transport selector ownership

`runtime/transport_selector.rs` owns TENT-style first-match selection and
`transport_selector/config.rs` compiles bounded typed or JSON policies.
`segment/metadata.rs` owns the placement and provider metadata published in
Describe, plus the process identity shared with CUDA IPC. `TransferEngine::prepare`
resolves the rule index, transport, NIC mask and QP range before admission.
Every network request, local file operation and staged CROSS child carries that
decision. RDMA filters paths before allocating slices. Its endpoint owns QP
indices, native SL/TC and cache claims independently of CQ worker placement.
The pool layout is immutable across admitted work. See
[transport selection](transport-selection.md).

Library diagnostics use the pinned `log` facade. OrbitTE installs no logger.
Consumers can enable selector warnings and errors through their own logger.

## Batch progress and notification ownership

The batch executor runs each admitted transport to its receipt. Submitting
work and completing an execution signal the same per-batch state that
`progress_batch` advances. TENT uses progress-lock shards. OrbitTE uses a lock
per live batch and a bounded weak-ID registry for admission and deferred
retirement. The optional engine worker owns a bounded wake set and joins on
drop. The typed `progress_fallback_interval` corresponds to TENT's
`runtime_queue/progress_fallback_interval_us`, and `notification_rpc_fallback`
corresponds to `notification/rpc_fallback`. A sweep probes retired owners independently and skips permanently
quarantined IDs. A source staging session continues to own its chunk and CUDA
loan after its socket disappears. Only a confirmed event and restored context
return the chunk. See [batch progress](async-batches.md#explicit-progress).

Notification routing visits installed channels before the control RPC. TCP and
self share the bounded control inbox. An async hook retains a charged set of
outstanding destinations. OrbitTE omits RDMA from notification routing because
its data grant QPs have no notification receive owner. An RPC-only engine can
send and receive even without an installed TCP notification channel. A lost
receipt precludes automatic replay. See [notifications](cpu-tcp.md#notification-contracts).

## Runtime queue ownership

| Owner | Responsibility | TENT counterpart |
| --- | --- | --- |
| `runtime/admission_queue.rs::LocalTransferAdmissionQueue` | Ordered eligible owners, per-batch dependency chains, dispatch counters | `LocalTransferAdmissionQueue`, `refillDispatchWindow` |
| `runtime/qos_contract.rs::QosContractResolver`, `qos_contract/admission.rs::QosQuota` | Compiled global/tenant/intent contracts and fixed per-contract counters | `QosContractResolver` (resolution only in TENT) |
| `runtime/receiver_credit.rs::SenderCreditLedger` | Cumulative grant/consumed accounting per peer binding | `SenderCreditLedger` (no runtime consumer in TENT) |
| `receiver_credit/issuer.rs::ReceiverCreditIssuer`, `sync.rs` | Fixed per-sender windows, tombstones and single-flight cumulative sync | No counterpart |

`runtime/request_merge.rs` maps original task IDs to physical owners after
registration, permission and route resolution. TENT sorts by hint and address.
OrbitTE also fences overlapping destinations and self-copy dependencies to
preserve public byte order. One source epoch, target and operation apply to the
whole submit. The peer advertises its transfer limit in Describe so a merge
cannot create an oversized grant on a smaller receiver.

Each physical owner holds its QoS permit, queue window and receiver credit.
Dispatch takes queue → QoS → credit locks. Credit sync and payload I/O run
outside them. EDF ranks independent batch heads. Reserving the only slot for
a successor would deadlock its FIFO worker. The worker advances its eligible
prefix while independent batches share the window. Files have no TENT memory-buffer key.
The direct file path keeps its native batches. When the runtime queue is
enabled, each file owner enters separately, as in TENT’s queued dispatch.

The source describes one exact location per registered view. TENT’s
`coalesceRegions` buckets multiple NUMA probe entries within one buffer.
OrbitTE has no such per-page location list. Adjacent registered views keep
their separate IDs, permissions and lifetimes. The queue reserves capacity
for a real proxy session, whose private CROSS tasks already have a bounded
lifetime inside that owner. This avoids reacquiring the parent’s window from
its worker. See [admission](async-batches.md#runtime-admission-and-dispatch-window).

### Retry ownership

The compiled selection fixes candidate order before merge. The existing batch
worker keeps a private, bounded decision slot for a drained completion. Poll,
explicit progress, cancel and free arbitrate that slot under the batch lock.
The same worker starts the next attempt. Public terminal states have no path
back to pending.

Frozen TENT permits duplicate same-content WRITEs after partial enqueue.
OrbitTE requires proof that a WRITE posted nothing. It has no general remote
revocation receipt for replay. A lost TCP ACK or a failed native WRITE can
therefore make the effect unknown. A drained READ can replay after QP retirement
or a confirmed CUDA copy and stage release. An unquiesced destination, an
overlap with the source, a short file result or an expired deadline prevents
that replay. The controls hold a real posting call, fail teardown, drop a WRITE
ACK and check the untouched guards. The file owners keep confirmed siblings.
A Describe RPC alone retries a connection error once, with the original
deadline and epoch. A replaced registration cannot inherit an admitted WRITE.

## Metrics ownership

The optional per-engine registry preallocates the frozen transport, operation
and reason domains, including the failover pairs. Recording uses indexed
relaxed atomics. The physical admission owner carries timestamps and the first
failure stage across retries. Its aliases and subsequent status polls add no
samples. Registration gauges change at publication and final unregistration.
The exporter samples existing queue, QoS, receiver-credit and rail owners only
on a scrape. Benchmark-only QoS reports have no runtime series.

Frozen TENT uses process-wide ylt metrics. OrbitTE engines own separate
registries and use a bounded standard-library listener, so no C++ runtime or
HTTP dependency is needed. Each worker serves one connection with a 4 KiB
request-header limit and an absolute 500 ms I/O deadline. Dropping the engine
joins the HTTP workers and the optional periodic logger. A busy port degrades
to log-only recording. The loader rejects malformed values and caps the pool
at 64 threads. Tests exercise a busy port, incomplete and oversized headers,
per-engine isolation and toggle-safe gauges. See [metrics](metrics.md).

## RDMA resource owners

A bounded rail table is shared by requester workers and the existing port
monitor. TENT keeps one table per worker. OrbitTE serializes only pair
accounting so two workers cannot arm concurrent trials. A receipt identifies
the admitted generation and trial. A late completion from before a port
fault cannot close the new circuit. `an_open_pair_admits_one_probe_and_only_its_receipt_can_reopen_it`
exercises that distinction. Discovery keeps peer epoch and NIC/PD identities,
rather than borrowing remote topology pointers. The grant path advertises the
remote memory owner's eligible NICs directly. A multi-slice submission uses
healthy pairs, or one trial grant when every permitted pair is paused.

The endpoint store fences admission before context shutdown. A record is
reclaimable after both native QP close and terminal grant retirement. An
external handle cannot construct into a fenced array. A finished logical grant
can still point to a live physical QP shared by other grants. Candidates check
the physical fence and claim the exact generation under the lane lock. The bounded store
retains a live or uncertain entry under pressure.


| Owner | Resource and lifetime | Frozen TENT correspondence |
| --- | --- | --- |
| `RdmaTransport` | Context slots, registrations, workers and parent dispatch | `RdmaTransport`, `context_set_`, `local_buffer_manager_`, `workers_` |
| `ContextSet`, `RdmaContext` | Stable NIC IDs, actual context/PD, route and persistent key domain | `RdmaContextSet`, `RdmaContext` |
| `LocalBufferManager` | Persistent host MRs keyed by actual context, allocation view and capability | Context-keyed `mem_reg_map` |
| `EndpointStore`, `RdmaEndpoint` | Peer identity, fixed-width construction slots and persistent QPs across CQ lanes | Context endpoint store, `RdmaEndPoint::qp_list_` |
| `RdmaQp`, `ConnectionResources`, `QpStore` | One QP shared by independently retained grant records | Endpoint QPs and affiliated slice ownership |
| `qp::send_queue::SendQueue` | Physical send depth, exact outstanding WR IDs and fenced admission | Per-QP `wr_depth_list_` and `slice_queue_` |
| `qp::receive_queue::ReceiveQueue` | Receive buffers/MRs, RQ debt and binding/key dispatch | QP-owned receive WRs; exact receipts are OrbitTE's |
| `RdmaGrant`, `GrantResources` | One exact-range DMA loan, source MW, SEND storage and grant charge | OrbitTE authority extension over registered regions |
| `RdmaSlice`, `SendWork`, `ReceiveWork` | Per-grant geometry, WR windows and completion identities | `RdmaSlice` work and completion state |
| `CqLane` | SQ/RQ CQ pair, grant records, submission, drain and recovery | Per-context CQs consumed by workers |
| `Workers`, `PostQueue` | Data threads, lane assignment, priority promotion and urgency ordering | `Workers::submit`, `asyncPostSend`, `promoteTimedOutRequests`, `orderByDeadline`, `asyncPollCq` |
| `SharedSlotManager` | Optional cross-process priority slots on existing workers | `SharedSlotManager::canSend` |
| `Monitor` | Event consumption and periodic context reclaim and recovery | `Workers::monitorThread`, `reclaimEndpoints`, `resumePausedContexts` |
| `DeviceSelector`, `DeviceInfo`, `SchedulingParams` | Compiled NIC eligibility, grant byte charges, bandwidth-weighted selection and estimates | TENT's classes of the same names |
| `CqQuota`, `GrantCredits`, `ReceiveCredits` | Aggregate SQ/RQ wakes and whole-parent grant/receive admission | Bounded slice admission; receive credits are OrbitTE's |

Each context slot owns its PD, CQ lanes and peer endpoint registry. Several
grant records can share one QP, which keeps its send and receive queues across
grants; peer arrays hold weak lane references to avoid ownership cycles. Native
cleanup is fallible: a failed QP, MR, MW, CQ or channel teardown keeps the
remaining owner, backing and charges, and a timeout never substitutes for
physical drain. See [native cleanup](native-cleanup.md) and the
[rail accounting gate](installed-host.md#physical-rail-accounting).

## Fixed-width cold construction

`EndpointStore::get_or_insert` gives every caller the same `RdmaEndpoint`, with
`min(local.num_lanes, remote.num_lanes)` slots keyed by local context, remote
process/PD/NIC, role and capability; staged and device paths use per-grant
connections instead. An uncloneable `Construction` reserves a slot, attaches the
QP owner before native creation and survives until the pair is published.
Waiters release all locks and use their own deadlines, and a unique token stops
an old guard from releasing a replacement slot. The source publishes its pair
before GRANT, so later handshakes share it; a fresh bootstrap may retire an old
**idle** pair but never an active sibling. TENT's
`getOrInsert` / `construct` / `EP_HANDSHAKING` follow the same lock discipline,
with an eager bidirectional QP list instead of OrbitTE's lazy per-capability
arrays.

## QP send ownership

`SendQueue` admits at most `max_send_wr` unresolved WRs across all work on a QP,
also charging the aggregate CQ budget. Payload, MW bind and completion SEND share
it. A failed post returns only a proven unaccepted suffix; an unknown post keeps
the whole possible debt and fences admission, and a validated CQE repays its exact
WR once. Dropping a work handle never returns SQ credit. CQ lookup resolves the
exact record before checking QPN, WR identity and operation, and native access
follows grant → QP → context lock order. GPU source grants use a private PD and
exact-range MR because PeerDirect cannot bind MWs; see
[GPU ownership](rdma-cuda.md#gpudirect-rdma-ownership).

## QP receive ownership

`ReceiveQueue` owns registered receive slots and their admission credits. A
successful RQ CQE names the consumed buffer by WR ID, while its 141-byte binding
and invalidated rkey identify the completed grant, which may differ from the
buffer's donor. Duplicate, stale or mismatched messages cannot complete another
receipt. RQ depth uses `max_cqes`, so a parent can prepare all receives even when
its grants exceed the SQ width, and receive credits stay charged until the
consumed buffer's MR is reaped. TENT also owns WRs at the QP and dispatches CQEs
to work identities; the bound source receipt is OrbitTE's authority extension.

## Follow one transfer

1. **Register.** Host Direct prepares local-only MRs and MW-bind parents in every
   usable context, including paused ones, before the range becomes visible.
2. **Plan.** TENT's slice geometry splits the parent; `DeviceSelector` picks a
   rail per slice from the owner's location, masks, strict NUMA, bandwidth
   estimates and byte charges.
3. **Authorize.** Both peers reserve the parent's grants, and the source reserves
   all receive credits before exposing the first grant. A fresh one-use MW
   authorizes only the requested range, and every slice is prepared before
   payload submission.
4. **Progress.** Worker `i` polls lane `i` across contexts; new grants pick the
   least-loaded lane, and reused QPs keep theirs.
5. **Complete.** Payload completion, the fenced completion SEND, the exact source
   receive and bound DONE/ACK/READY establish grant drain; the parent succeeds
   only when every slice has. Ambiguous WRITEs are never replayed.

All contexts share aggregate byte, QP, grant and SQ/RQ limits. Fully drained
faulted lanes can be replaced while the PD, MRs and worker pool stay alive.
Workers bind to NUMA CPU sets; this does not move application pages.

The frozen RDMA quota has two scopes. The selector charges exact slice bytes
in one process. A configured POSIX shm name shares priority eligibility among
processes on the same host. OrbitTE uses a same-UID header, bounded nonblocking
file locks and an aligned atomic slot. A process killed under the lock cannot
strand its peers. A stale Low slot cannot extend eligibility past its clock
boundary. The Linux controls kill a publisher with a live device lease, check
the surviving NIC charge and reject a foreign header without modifying it.
Pool shutdown joins the publisher and consults the single unmap receipt. An
unconfirmed unmap retains the mapping and fd and returns `Unquiesced`.

TENT's receiver ledger names four resources and assumes its caller has fenced
the old epoch. OrbitTE checks its physical permits and their completion
receipts before retiring even a manual epoch. The lost TCP WRITE acknowledgement
control checks that an applied payload with no receipt cannot free credit.
Manual grant updates never release automatic Pin leases. QoS uses one resolved
budget per contract. The parser controls reject unexecutable actions by name
at every policy layer.

## Periodic monitor reclaim and port recovery

The monitor scans contexts once per second, even without traffic, using
nonblocking inspection so a destructor waiting for an event ACK cannot stall it.
Faulted lanes and endpoint slots are reclaimed only after native drain.
Discovery keeps an initially down port as a paused context that accepts MR
registration but rejects routes. `RdmaContext::refresh_address` queries the
selected port and GID; paused automatic configuration reranks GIDs on the same
port, while pins stay strict. A monotonic route generation stops an old query
from clearing a newer fault, and recovery needs drained lanes, a successful query
and an unchanged generation. This follows TENT's one-second reclaim/resume pass
and `activateContext`, while keeping physical destruction on the data workers.
See [the recovery contract](rdma-cuda.md#initially-paused-contexts-and-gid-reselection).

## Source reading order

Start at an operation, then follow the resource it borrows. Child modules extend
their resource owner; submission and cleanup stay beside the resources they protect.

| Read | Location | Responsibility |
| --- | --- | --- |
| Public request | `engine.rs`, `batch.rs`, `memory.rs`, `segment/` | Engine, tasks, backing and file segments |
| Runtime | `runtime/transport_selector.rs`, `admission_queue.rs`, `qos_contract/`, `receiver_credit/` | Selection, admission, QoS and credits |
| Transport entry | `transport/rdma/rdma_transport.rs` | Registration, parent dispatch and source serving |
| Context orchestration | `rdma_transport/contexts.rs` | Initialization, selection, CQ attachment, recovery and shutdown |
| Context owners | `context_set.rs`, `context.rs`, `context/key_domain.rs`, `discovery.rs`, `quota.rs` | Stable slots, PD/key history, discovery and NIC admission |
| Peer rendezvous | `rdma_transport/connections.rs`, `rdma_transport/handshake.rs` | Fixed-width connection claims and bilateral publication |
| Grant construction | `grant/construction.rs`, `rdma_transport/requester.rs` | Admitted buffer, credit and connection ownership across MR waits |
| Wire data | `protocol.rs`, `path.rs` | Grant receipts and static NIC/PD path metadata |
| Persistent owners | `buffers.rs`, `endpoint.rs`, `endpoint_store.rs`, `qp.rs`, `qp_store.rs`, `grant.rs` | MR, peer, QP and grant lifetime |
| QP work ownership | `qp/send_queue.rs`, `qp/receive_queue.rs`, `slice.rs`, `receive.rs` | Physical SQ/RQ ownership and receipt dispatch |
| CQ execution | `cq_lane.rs`, `cq_lane/` | Lane lifecycle, admission, reuse, retirement, dispatch and polling |
| Scheduling and events | `workers.rs`, `workers/post_queue.rs`, `promotion_policy.rs`, `shared_quota.rs`, `bw_arbitration.rs`, `monitor.rs`, `events.rs`, `cq.rs` | Data threads, priority and urgency ordering, events and CQ/channel owners |
| Verbs boundary | `sys.rs`, `sys/` | Fallible function table and native calls by resource |
| Staging | `runtime/proxy_manager.rs`, `runtime/proxy_manager/`, `cuda/staging.rs`, `platform/numa.rs` | Pinned chunks, remote operations, rings and CUDA copy/drain |
| Host and GPU | `platform/cpu_probe.rs`, `cuda/`, `transport/nvlink/` | Host discovery, NVIDIA copy, GPU DMA and IPC owners |

Paths after the transport entry are relative to `src/transport/rdma/`, except
the public, runtime, staging and host/GPU rows, which are relative to `src/`.
The installed fixture `tests/hardware/rdma_host.rs` crosses these owners through
the public API; see [installed host](installed-host.md). Private tests live under
`tests/unit/`, mirroring `src/`.

## P4 source audit

The audit compared OrbitTE `f2c7e7f` (PR #55) with frozen TENT
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`. At that point both RDMA
implementations were about 11k physical lines (OrbitTE 38 Rust files, TENT 28
`.cpp`/`.h` files, tests excluded). Per-request MW authority, exact source
receipts, bilateral epochs and conservative retention are deliberate OrbitTE
contracts beyond TENT's registered-region path. The audit found two gaps
against the [context implementation][context], both since implemented:

| Behavior | Frozen TENT | OrbitTE at `f2c7e7f` | Current source |
| --- | --- | --- | --- |
| Initially down port with valid device/GID | `RdmaContext::enable` keeps it as `DEVICE_PAUSED` | Discovery filtered non-active ports | Paused context/PD kept; MR publication includes it, route admission excludes it |
| Automatic GID reselection on recovery | `refreshAddress` reruns `getBestGidIndex` | Refresh queried the existing index | Paused refresh reranks GIDs on the same port; pins stay strict |

Both pass CPU transitions and the
[focused native gate](qualification.md#focused-native-recovery), with
[independent review](qualification.md#p4-independent-review). Physical and
fabric qualification stays in the [P4 queue](completion-plan.md#p4-remaining-delivery).

## Correspondence and remaining differences

The reference is frozen at `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`; see
TENT's [transport header][transport], [endpoint][endpoint] and [workers][workers].

| Area | Current OrbitTE | Difference from TENT |
| --- | --- | --- |
| QP arrays | Fixed-width, lazily populated, concurrent grants, drain-aware rollback | TENT builds its QP list eagerly and shares it across directions |
| Monitor | Unified get/ACK, periodic reclaim, paused contexts, GID reselection, rate refresh | OrbitTE keeps retained-resource quarantine |
| Path metadata | `path.rs` validates NIC/PD descriptions and compatible pairs; `rail_monitor.rs` gates selection and tracks native receipts | TENT `RailMonitor` |
| Grant authority | Exact-range host MW or private GPU PD/MR, with a bound source receipt | TENT publishes native MR keys; not wire parity |
| CQ layout | Separate SQ and RQ CQ per lane | TENT uses one lane CQ for data QPs |
| Worker load | Unfinished grant charge, including retained native work | Failover and remaining deadline policy are P6 |

[transport]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/transport/rdma/rdma_transport.h
[endpoint]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/endpoint.cpp
[context]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/context.cpp
[workers]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/workers.cpp

## CUDA copy stream ownership

| Owner | Responsibility |
| --- | --- |
| `cuda::CudaMemory` | Allocation lock and primary-context reference |
| `cuda::stream::CopyStream` | Lazy nonblocking stream, reusable completion event and pending-work state |
| `cuda::stream_pool::CUDAStreamPool` / `CUDAStreamHandle` | Bounded context-matched streams shared by NVLink groups |
| `cuda::driver` | Fallibly loaded Driver API functions |
| `cuda/readiness.rs::CallerSyncEvent` | Producer event generation, its context and retained producer inputs |
| `cuda/pinned.rs::PinnedAllocation` | Pinned host allocation, context and lazily created readiness event |
| `runtime/proxy_manager.rs`, `proxy_manager/` | Shared staging chunks, remote sessions, rings and worker shards |

Each copy holds its staging until the recorded event completes. A failed enqueue
or record requires a proven stream drain; otherwise the allocation, staging,
stream/event and context are retained. Destroying a stream or event never proves
DMA completion. This follows TENT's
[CUDAStreamPool](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/platform/cuda/cuda_stream_pool.cpp)
handles and NVLink's event-after-copy completion. The staging owners and their
progress states are described in [CUDA staging](staging.md#tent-correspondence),
and the readiness handoff in
[producer readiness](rdma-cuda.md#producer-stream-readiness).

## Platform pointer and location ownership

`memory/location.rs` defines TENT-style memory types and bounded `RangeLocation`
metadata supplied by the actual owners; CUDA-pinned host memory stays CPU memory.
`cuda/probe.rs` validates external CUDA pointers with one batched attribute query
and a mapped-range check, and caches each context's PCI/NUMA discovery. This
follows TENT's `Platform` / `MemoryProber` responsibilities without a global
raw-pointer registry; see [memory location](rust-api.md#memory-type-and-location).

`cuda/probe/topology.rs` supplies TENT's `CudaProbe` inventory to RDMA discovery,
`platform/pci.rs` resolves PCI/NUMA and bridge distance, and
`runtime/topology.rs::MemEntry` stores host and CUDA locations with three NIC
tiers. `runtime/topology/config.rs` implements TENT's `loadFromConfig`,
`parsePriorityMatrix` and `parseCustomTopology`. `DeviceSelector` compiles all of
this into rank arrays and masks once, so slice allocation does no driver, sysfs
or string work, preserving TENT's `CudaProbe → Topology → DeviceSelector` chain.

| Owner | TENT responsibility |
| --- | --- |
| `quota/scheduling.rs::SchedulingParams`, `quota/bandwidth.rs::DeviceInfo` | Bandwidth policy, selection EWMA and busy-time meter (`setDeviceBandwidth`, `noteCompleted`, `maybeSampleTransmit`, `getNicLoadStats`) |
| `quota/allocation.rs` | `buildCandidates`, `selectSinglePath`, `selectMultiPath` |
| `sys/port.c`, `sys/port.rs`, `RdmaContext` | `recordPortSpeed`, `queryEffectiveSpeed`, `refreshPortAttributes` |
| `workers/post_queue.rs`, `bw_arbitration.rs`, `promotion_policy.rs` | Priority tiers, `OrderByUrgency`, request promotion |
| `shared_quota.rs::SharedSlotManager` | Mapped current priority slot and joined monotonic publisher; per-NIC leases remain in DeviceSelector |

Physical grant release stays separate from payload sampling, and route identity
excludes link speed, so a rate change keeps existing QPs. See
[bandwidth-aware selection](rdma-cuda.md#bandwidth-aware-rdma-selection).

## CUDA IPC transport ownership

| Owner | Responsibility |
| --- | --- |
| `transport/nvlink.rs::NVLinkTransport` | Same-host identity checks, grant and close bindings, mapping quotas, stream pool and completion progress |
| `cuda/ipc.rs::IpcExport`, `IpcMapping` | Allocation-wide source loan; one imported reference with its context and touched local allocations |
| `transport/nvlink/mapping.rs::OpenedMapping` | Mapping charge and explicit native close |
| `transport/nvlink/subbatch.rs::NVLinkSubBatch` | Consecutive same-region, same-context copies |
| `cuda/ipc/copy.rs`, `cuda/ipc/batch.rs::CopyBatch` | Deduplicated owner locking, ordered enqueue and native batch arrays |
| `transport/nvlink/progress.rs` | Inline ready check, then one lazy worker querying pending events |
| `transport/nvlink/cache.rs::RelocationCache`, `export.rs::ExportCache` | Optional import cache and persistent source export lifetime |

The stream handle is acquired before memory locks and returned before close and
control I/O; loans survive event polls without holding allocation mutexes. TENT
keeps a transport-wide relocation cache and queries the task event in
`getTransferStatus`; OrbitTE closes imports per subbatch by default and adds
explicit close receipts. See [CUDA IPC](cuda-ipc.md).
