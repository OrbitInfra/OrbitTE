# Architecture

OrbitTE follows frozen Mooncake TENT's engine, transport, context, registered
buffer, endpoint and worker boundaries. The host TCP/RDMA data path exists;
full TENT behavior, native wire compatibility and complete P4 acceptance remain
open. Read [capabilities](capabilities.md) for support and
[the P4 exit queue](completion-plan.md#p4-remaining-delivery) for missing work.

:::{image} _static/architecture.svg
:alt: OrbitTE engine and sibling TCP/RDMA transports; dashed paths are planned
:class: dark-light
:::

## Engine and transport boundary

`TransferEngine` owns registration, batches, admission and directional shutdown.
`RegisteredMemory` retains actual backing; aliases share allocation-level
access coordination. `runtime::control_plane::ControlService` owns peer lookup,
request framing and source handlers. It dispatches payload work to sibling
`TcpTransport` and `RdmaTransport` implementations.

RDMA uses the control connection to negotiate paths, connections and exact
memory grants. Payload bytes go through verbs. `rdma::protocol` defines the
OrbitTE v21 messages; moving them into their own module does not make this
protocol compatible with TENT's native RPC or metadata.

## RDMA resource owners

| Owner | Resource and lifetime | Frozen TENT correspondence |
| --- | --- | --- |
| `RdmaTransport` | Context slots, registrations, workers and parent dispatch | `RdmaTransport`, `context_set_`, `local_buffer_manager_`, `workers_` |
| `ContextSet`, `RdmaContext` | Stable NIC IDs, actual context/PD, route and persistent key domain | `RdmaContextSet`, `RdmaContext` |
| `LocalBufferManager` | Persistent host MRs keyed by actual context, allocation view and capability | Context-keyed `mem_reg_map` |
| `EndpointStore`, `RdmaEndpoint` | Peer identity, fixed-width construction slots and persistent QPs across CQ lanes | Context endpoint store, `RdmaEndPoint::qp_list_` |
| `RdmaQp`, `ConnectionResources`, `QpStore` | One actual QP shared by independently retained grant records; CQ-local lookup and retirement | Endpoint QPs and affiliated slice ownership |
| `qp::send_queue::SendQueue` | Physical QP send depth, exact outstanding WR IDs and fenced admission | Per-QP `wr_depth_list_` and `slice_queue_` accounting |
| `qp::receive_queue::ReceiveQueue` | QP-owned receive buffers/MRs, RQ debt and binding/key dispatch | QP-owned receive WRs; exact grant receipts are a Rust extension |
| `RdmaGrant`, `GrantResources` | One exact-range DMA loan, source MW, SEND message storage and grant charge | Rust authority extension over TENT's registered-region model |
| `RdmaSlice`, `SendWork`, `ReceiveWork` | Per-grant geometry, WR windows and independent completion identities | `RdmaSlice` work and completion state; source receipts are a Rust extension |
| `CqLane` | SQ/RQ CQ pair, registered grant records, submission, drain and recovery | Per-context CQs consumed by workers |
| `Workers` | Transport data threads, lane assignment and unfinished-grant load | `Workers::submit`, `workerThread`, `asyncPollCq` |
| `Monitor` | One transport thread consuming events and driving periodic context reclaim/recovery | `Workers::monitorThread`, `reclaimEndpoints`, `resumePausedContexts` |
| `DeviceSelector`, `CqQuota`, `GrantCredits`, `ReceiveCredits` | NIC byte load, aggregate SQ/RQ wakes and whole-parent grant/receive admission | Device selection and bounded slice admission; OrbitTE's receive-credit protocol is additional |

Each context slot owns its PD/context, CQ lanes and peer endpoint registry.
`RdmaQp` serializes one physical connection; each CQ-lane `Record` owns one
grant's state, namespaces, work and waiters. Multiple records can retain the
same QP. The connection retains its `SendQueue` and `ReceiveQueue` across grants. `SendWork` borrows
the shared SQ; `ReceiveWork` tracks an exact binding/key receipt independently
of the buffer that receives it. A live grant
retains its lane, worker pool and peer endpoint. Peer arrays retain QP records
and weak lane references, avoiding an endpoint/lane ownership cycle.

Native cleanup is fallible. A failed QP/MR/MW/CQ/channel teardown retains the
remaining owner, backing and charges; a timeout or terminal task state cannot
substitute for physical drain. A separate shared thirty-second parent destruction window
preserves the original data failure unless cleanup reports unquiesced work.
The [native cleanup gate](native-cleanup.md) checks these owners after failed
destruction and explains the difference from TENT's retry policy.

The [installed physical rail gate](installed-host.md#physical-rail-accounting)
checks these boundaries outside the crate: native context identity selects the
physical RNIC, accepted WRs consume the shared QP, and successful CQEs account
for exact slice bytes. TENT's `Workers::markPosted` and `releaseSliceQuota`
similarly associate completion with posted work. OrbitTE additionally keeps
its grant and backing charges through the source receipt; a payload CQE alone
does not retire that ownership.

## Fixed-width cold construction

`EndpointStore::get_or_insert` gives every caller the same `RdmaEndpoint`, even
while the first builder has not created a QP. Each array has
`min(local.num_lanes, remote.num_lanes)` slots, negotiated before any slice is
prepared. Its key includes the local context, remote process/PD and NIC path,
requester/source role and READ/WRITE capability. Opposite roles do not wait on
one another. Staged/device paths advertise zero reusable width and retain
their existing per-grant physical connection budget.

An uncloneable `Construction` reserves a slot under the endpoint mutex. It
attaches the actual QP owner before native creation, then survives until the
bilateral connection is published. An unpublished slot cannot be shared.
Waiters release all store/endpoint locks and use their own deadlines; they
cannot cancel the builder. Cancellation releases an unbound slot immediately,
but a created QP occupies its slot until native destruction succeeds. A unique
reservation token prevents an old guard from releasing a replacement slot.

The source publishes its connected pair before sending GRANT. A subsequent
handshake therefore finds that exact pair as soon as the requester can share
it. Duplicate cold bootstraps join the same remote connection identity. Fresh
bootstrap after a peer discarded a QP can retire an old **idle** pair in that
array, using the existing all-affiliates-finished check; active siblings are
never evicted to make a slot. Malformed path metadata rejects the new request
without replacing an established connection. Whole-parent grant/RQ admission
remains separate, so more slices than QPs can be prepared without waiting for
payload completion.

Frozen TENT uses `EndpointStore::getOrInsert`, `RdmaEndPoint::construct` and
`EP_HANDSHAKING`/`EP_READY`; its connect path releases the endpoint lock for
bootstrap RPC and revalidates on return. OrbitTE expresses that ownership with
bounded slots and Rust guards. TENT's eager, bidirectional QP list and native
bootstrap format differ from OrbitTE's lazy, capability-specific grant protocol.

## QP send ownership

`ConnectionResources::send_queue` owns the physical SQ's unresolved WR IDs.
It admits at most `max_send_wr` across all work sharing that QP while also
charging the aggregate CQ budget. Each grant's `SendWork` keeps its
geometry, window and completion state; payload, MW bind and completion SEND
all use the connection's same queue. Post receipts return only a proven
unaccepted suffix. Unknown posts retain the entire possible debt and fence new
admission. A validated CQE discharges its exact posted WR once.

Dropping a work handle cannot return SQ credits. QP close fences admission;
cancellation clears unresolved debt only after all QPs and both CQs have
closed. Failed native destruction retains the queue with its connection and
backing. The native fault fixtures compare this queue against observed
accepted-minus-delivered WRs before CQ destruction.

Concurrent runtime records now share this queue. Each record publishes its own
send namespace, and CQ lookup resolves the exact record before checking QPN,
WR identity and operation. Submission borrows the SQ directly so a sibling's
native QP call cannot block logical work admission. Native access follows
grant → QP → context lock order; connection identity is published under the
QP creation lock. The public v21 handshake can attach independent grants to a
connected QP. The observed previous grant is a lower bound, while both process
and connection identities still match exactly. Endpoint observations only move
forward when concurrent handshakes arrive out of order. Cold construction
reserves the same fixed array used for established pairs.

GPU source grants use a private PD and exact-range MR because PeerDirect cannot
bind MWs. Their fresh QPs still use context CQ lanes and transport Workers.
`cuda/rdma.rs` owns SYNC_MEMOPS preparation, range exclusion and consumer-visible
completion; `GrantResources` and `ConnectionResources` own native MR/PD teardown.
See [GPU ownership](rdma-cuda.md#gpudirect-rdma-ownership) for the TENT correspondence
and the separate source/requester reuse policies.

## QP receive ownership

`ConnectionResources::receive_queue` owns registered receive slots and their
parent admission credits. A successful RQ CQE names the consumed buffer by WR
ID; its full 141-byte binding and invalidated rkey identify the grant receipt.
The buffer's donor is not used to identify the completed grant. `ReceiveWork`
holds immutable binding/key/deadline metadata and independent posted/completed
state. Dropping that handle cannot release an unresolved receive slot.

Completing grant B through buffer A releases A's RQ debt. B's donated buffer
remains QP-owned until its own CQE or physical drain, even after B is released.
Reaping completed buffers preserves prepared but unposted slots. Duplicate,
stale or mismatched messages cannot complete another receipt. Ambiguous posts
retain buffer/MR/credit ownership; cancellation requires QP/CQ destruction.
Failed message-MR cleanup retains the actual queue along with its connection.

Native SQ depth uses `max_send_wr`; RQ depth uses `max_cqes` so an entire
parent can prepare receives even when its grants exceed the physical SQ width.
Each queue has a separate aggregate CQ budget. Receive credits remain charged
until the consumed buffer's MR is reaped, including native cleanup failure. TENT likewise owns WRs at the physical
endpoint/QP and dispatches CQEs to work identities. This full-binding source
receipt protocol is an OrbitTE authority extension, not TENT wire parity.
The lane locates the physical RQ by slot identity, releases its lock, then
locks the grant named by the receipt. It clears that grant's bound authority
before publishing completion or waking its waiter. The donor record may already
have left lookup: its pending slot/MR stays with `RdmaQp`.

Successful grants can retire independently. Physical cancellation fences every
affiliated record and drains the connection once; unresolved work still requires
all QPs and both CQs to close. Retention counts each physical QP/SQ/RQ once and
each grant/loan separately. A stale claim or replaced idle record has no authority
to retire a sibling or later grant. The native shared-QP fixture exercises these
owners directly. The public handshake uses the same owners and fixed-width
construction reservations. See [qualification](qualification.md) for the tested scope.

## Follow one transfer

1. **Register.** Host Direct publication prepares local-only MRs and separate
   MW-bind parents in every usable context before making the range visible,
   including contexts whose selected port is paused. Partial failures roll back
   only newly created copies. Device masks restrict dispatch, not MR publication;
   paused contexts cannot supply a transfer path.
2. **Plan.** The transport partitions large parents with TENT's bounded,
   short-tail-folding geometry. Static `path` metadata checks compatible peers;
   `DeviceSelector` applies device masks, topology ranks and byte charges.
3. **Authorize.** Both peers reserve the whole parent's `max_grants` budget;
   the source also reserves all RQ credits before exposing the first grant.
   Each grant owns a reservation slot; the parent's reservation survives until
   all its actual children drain. Failed source RQ admission rolls back its
   grant reservation before any QP or authority is created. Rendezvous verifies both process/connection identities,
   source NIC/PD and grant generation. A fresh one-use MW authorizes only the
   requested range. All slices are prepared before payload submission.
4. **Progress.** `num_lanes` sets both transport worker count and each context's
   CQ-pair count. Worker `i` polls lane `i` across contexts. New grants choose
   the least-loaded lane with rotating ties; reused QPs keep their original
   lane. Grant-owned `SliceLease` remains charged through drain or quarantine.
5. **Complete.** Native data completion, same-QP fenced completion messaging,
   the exact source receive/invalidation and bound DONE/ACK/READY receipts
   establish grant drain. Every slice must finish before its parent succeeds.
   Staged output is copied only after the required proof. Ambiguous WRITEs
   are not replayed automatically.

All contexts share aggregate byte, physical QP, grant and SQ/RQ limits. A QP
is idle and evictable only when every affiliated grant is finished. Cancelling
an unattached claim discards that record without fencing an active sibling. Quota return wakes
waiting lanes even when they have no local CQE to wake them. Completely drained
faulted lanes can be replaced while the PD, persistent MRs and worker pool stay
alive. Workers bind before admission to NUMA CPU sets intersected with the
creator's allowed mask; this does not place application pages near an RNIC.

The monitor serializes native get/ACK with event-source detachment. CQ channels
stay lane-owned; async FDs stay device-owned. Native owners close only after
detachment. Lost event-consumption proof fences the affected device and retains
unproven native resources. See [the lifetime contracts](rdma-cuda.md) for exact
failure transitions and [qualification](qualification.md) for scoped evidence.

## Periodic monitor reclaim and port recovery

The transport's existing monitor scans its registered context set once per
second, including when applications submit no new work. It removes faulted CQ
lanes only after data workers have closed native QPs/CQs and detached. Endpoint
slots are reclaimed only when their physical QP is closed; retained cleanup
failures remain charged. The monitor uses nonblocking context, lane and QP
inspection so a native destructor waiting for event ACK cannot stall its ACK
consumer. Transport shutdown detaches the weak context registration before
dropping resource owners; the monitor cannot become their last owner.

Discovery retains a context with valid device/GID metadata even when its port
starts down. `events::State::check_device` permits persistent MR registration
on its usable PD; route operations also require `check`, which rejects paused
ports. Device-fatal or lost-consumer faults still block registration.

`RdmaContext::refresh_address` queries the selected native port/GID tuple while
healthy. Paused recovery repeats automatic GID selection on that same port;
explicit device/port/GID pins stay strict. Down ports and changed GID/LID/MTU
fence admission and wake every old lane.
PORT_ERR, PORT_ACTIVE, GID_CHANGE and LID_CHANGE apply only to the selected
port, though all native events are acknowledged. A monotonic route generation
prevents an older query result from clearing a newer fault. Recovery requires
all old lanes and endpoint construction slots to drain, a successful native
query and unchanged fault generation. It publishes the new address and restores
`DeviceSelector` availability under the context-set lock. Missing PORT_ACTIVE
events therefore do not require an application request to trigger recovery.
Device-fatal events, lost event consumers and retained owners cannot be healed
by a successful port query.

Frozen TENT's monitor performs a similar one-second reclaim/resume pass;
`activateContext` pauses selection, refreshes the address and evicts old
endpoints before enabling. OrbitTE keeps physical destruction in its data
workers and waits for their native drain proof. It preserves PD/key domains and
persistent MRs. When the peer discarded several cached QPs, bootstrap may
consume one explicit CACHE_MISS per array slot before a fresh handshake;
the original deadline still applies and payload submission has not begun.
This does not retry an ambiguous data transfer.

Initially paused context retention and automatic GID reselection have CPU
coverage and a scoped one-RNIC native gate; source and recorded evidence passed
[independent review](qualification.md#p4-independent-review) with fresh live
RoCE repeats. New-device hotplug,
link-bandwidth policy and physical fabric/device-loss qualification remain open.
See [the recovery contract](rdma-cuda.md#initially-paused-contexts-and-gid-reselection)
and [qualification](qualification.md).

## Source reading order

Start at an operation, then follow the resource it borrows. Child modules extend
their resource owner; submission and cleanup stay beside the resources they protect.

| Read | Location | Responsibility |
| --- | --- | --- |
| Public request | `engine.rs`, `batch.rs`, `memory.rs` | Engine, tasks and actual backing |
| Transport entry | `transport/rdma/rdma_transport.rs` | Registration, parent dispatch and source serving |
| Context orchestration | `rdma_transport/contexts.rs` | Transport initialization, selection, CQ attachment, recovery and shutdown |
| Context owners | `context_set.rs`, `context.rs`, `context/key_domain.rs`, `discovery.rs`, `quota.rs` | Stable slots and reclaim, PD/key history and exhausted MWs, device discovery and NIC admission |
| Peer rendezvous | `rdma_transport/connections.rs`, `rdma_transport/handshake.rs` | Fixed-width connection claims and bilateral publication |
| Grant construction | `grant/construction.rs`, `rdma_transport/requester.rs` | Admitted buffer/credit/connection ownership across MR publication waits; native QP handoff and explicit abort |
| Wire data | `protocol.rs`, `path.rs` | OrbitTE grant receipts and static NIC/PD path metadata |
| Persistent owners | `buffers.rs`, `endpoint.rs`, `endpoint_store.rs`, `qp.rs`, `qp_store.rs`, `grant.rs` | MR, peer, QP and exclusive grant lifetime |
| QP work ownership | `qp/send_queue.rs`, `qp/receive_queue.rs`, `slice.rs`, `receive.rs` | Physical SQ/RQ ownership, independent grant work and binding/key receive dispatch |
| CQ execution | `cq_lane.rs`, `cq_lane/submit.rs`, `cq_lane/reuse.rs`, `cq_lane/retirement.rs`, `cq_lane/completion.rs`, `cq_lane/poll.rs` | Lane lifecycle, admission/waits, drained reuse, pollable retirement, CQE dispatch and native progress |
| Scheduling and events | `workers.rs`, `monitor.rs`, `events.rs`, `cq.rs` | Data threads, event consumer and actual CQ/channel owners |
| Verbs boundary | `sys.rs`, `sys/device.rs`, `sys/qp.rs`, `sys/memory.rs`, `sys/cq.rs` | Fallible function table and native calls grouped by resource |
| Staging | `runtime/proxy_manager.rs`, `runtime/proxy_manager/`, `cuda/staging.rs`, `platform/numa.rs` | Shared pinned chunks, scoped remote operations, independent requester/source rings, GPU range loans and CUDA copy/drain |
| Host and GPU | `platform/cpu_probe.rs`, `platform/cpu_probe/affinity.rs`, `cuda/` | Host discovery/worker binding, NVIDIA copy and GPU DMA owners |

`rdma_transport/contexts.rs` extends `RdmaTransport`; `context_set.rs` owns only
the slot state and its monitor-driven reclaim. The methods retain their existing
lock scopes, call order and visibility to the RDMA module. This mirrors TENT's
transport-owned `RdmaContextSet` and context-owned PD/CQ/endpoint resources
without introducing a forwarding owner or changing cleanup semantics.

`LocalBufferManager` retains the actual MR creator and physical registration
charge. A requester waiting for publication owns no MR borrow yet.
`GrantConstruction` retains its admitted buffer, worker credit and connection
claim until that borrow is ready, then hands the complete grant to the existing
CQ owner. Failed cleanup retains actual native resources; an unused waiter's
credit can return independently. This follows TENT's buffer/context/endpoint
boundaries; the explicit suspended Rust owner is described in
[MR publication progress](staging.md#persistent-mr-publication-progress).

Paths after the transport entry are relative to `src/transport/rdma/`, except
the public, host and GPU rows, which are relative to `src/`.
The installed host fixture in `tests/hardware/rdma_host.rs` crosses these owners
through the public engine API. `build_installed.py` builds it as a separate
consumer of an extracted `.crate`. `rdma_host/resources.rs` owns registration
and drain; `rdma_host/restart.rs` owns repeated peer death while the other
engine stays live. `rdma_host/interruption.rs` owns pending public batches and
drain assertions; `rdma_host/gate.rs` owns the bounded test control relay that
holds GRANT after source RQ posting and rejects TCP payload opcodes.
`run.py`/`supervise.py` own exact-child supervision; `installed.py`,
`installed_restart.py` and `installed_interruption.py` independently reconcile
native resource, byte and process receipts. `rdma_host/completion.rs` controls
the separate packaged native CQ-loss tool. `installed_completion.py` joins
accepted WRs, actual successful CQEs and real QP destruction before accepting
the public drain result. This explicit test profile intercepts provider calls
in the requester only; normal package consumers do not load the tool.
Failed lanes are reclaimed before replacement CQs are created on demand; subsequent healthy rounds must preserve
all persistent MRs and have no new progress failures. No production owner or
transport layer is added.

Private tests remain under `tests/unit/`; moving methods preserves the existing
transport and hardware test selectors. `BoundDone` can still be constructed
only by the protocol's successful bound DONE read.

## P4 source audit

The audit compares OrbitTE `f2c7e7f` (PR #55 merged) with frozen TENT
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`. Count the complete RDMA implementation
and headers when comparing size: OrbitTE has 38 Rust source files / 11,387
physical lines in `src/transport/rdma`; TENT has 28 `.cpp`/`.h` files / 11,133
physical lines across its RDMA source and include directories. Counts include
comments and blank lines, exclude tests and are not measures of feature parity.
The subsequent context-method relocation adds one source file, not a new owner.
These counts describe the audited commit, before the recovery changes below.

The basic context/MR/endpoint/worker/slice decomposition is present. Per-request
MW authority, exact source receipts, bilateral epochs and conservative retention
add work beyond TENT's registered-region path. These are deliberate OrbitTE
contracts, not automatic requirements of writing Rust. Removing them would
require a separately reviewed public lifetime/authority change.

The audit identified two gaps against the frozen [context implementation][context].
The current source implements both; the historical finding and current evidence
remain distinct:

| Behavior | Frozen TENT | OrbitTE at `f2c7e7f` | Current source |
| --- | --- | --- | --- |
| Initially down port with otherwise valid device/GID | `RdmaContext::enable` retains the context as `DEVICE_PAUSED` | Discovery filtered non-active ports | Discovery retains a paused context/PD; persistent MR publication includes it, while route admission excludes it |
| Automatic GID reselection on port recovery | `RdmaContext::refreshAddress` reruns `getBestGidIndex` for automatic configuration | Refresh queried the existing index only | Paused refresh reranks valid GIDs on the selected device/port; explicit pins stay strict; native drain precedes publication |

CPU transitions and the [focused native recovery gate](qualification.md#focused-native-recovery)
cover these paths. [Installed RoCE profiles](qualification.md#installed-automatic-discovery)
have bounded qualification and [independent live repetition](qualification.md#p4-independent-review).
Physical/fabric qualification remains open; the single [P4 queue](completion-plan.md#p4-remaining-delivery)
keeps them separate. Selected-port address refresh, periodic reclaim and
fixed-width QP reuse already exist. Broader bandwidth/QoS/retry policy remains
in P6. GPU Direct now uses the same context/worker owners with the private-MR
authority described in [RDMA/CUDA](rdma-cuda.md#gpudirect-rdma-ownership); native
wire compatibility remains a separate stage.

## Correspondence and remaining differences

The reference is frozen at `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`.
TENT's [transport header][transport], [endpoint implementation][endpoint] and
[workers implementation][workers] are the source references, not a moving main.

| Area | Current OrbitTE | Remaining correspondence |
| --- | --- | --- |
| QP arrays | Fixed-width, lazily populated arrays with concurrent grants and native-drain-aware rollback | TENT constructs its whole QP list eagerly and can use one array for both directions/operations; OrbitTE keeps role/capability-specific arrays |
| Monitor | Unified get/ACK, periodic reclaim, initially paused contexts and automatic GID reselection | TENT also refreshes effective link bandwidth. OrbitTE preserves conservative retained-resource quarantine; focused native recovery passes, with source and bounded RoCE evidence independently accepted |
| Path metadata | `path.rs` validates frozen NIC/PD descriptions and compatible pairs | Selected-port health lives in `Monitor`/`RdmaContext`; native `RailMonitor` also supplies broader failover/retry policy, which remains open |
| Grant authority | Exact-range host MW or private GPU PD/MR, and bound source receipt | Deliberate Rust authorization contract; TENT publishes native MR keys. GPU source grants are not cached; this is not native wire parity |
| CQ layout | Separate SQ/RQ CQ pair per lane | TENT uses a lane CQ for its data QPs; ownership and accounting are preserved with a different CQ layout |
| Worker load | Unfinished grant charge including retained native work | Priority queues, QoS, retry/failover and deadline policies remain P6 work |

The `cpu_probe` filename does match TENT's host-probing source. Names that match
native concepts retain that meaning; new Rust ownership or protocol mechanisms
are identified explicitly. Architecture alignment alone does not establish
behavioral, ABI, wire or performance parity.

[transport]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/transport/rdma/rdma_transport.h
[endpoint]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/endpoint.cpp
[context]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/context.cpp
[workers]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/workers.cpp

## CUDA copy stream ownership

`cuda::CudaMemory` owns the allocation lock; `cuda::stream::CopyStream` owns a
lazy nonblocking stream, reusable completion event and pending-work state.
Public allocation copies and staging slots retain their own `CopyStream`;
NVLink groups borrow one through `cuda::stream_pool::CUDAStreamHandle`.
`cuda::driver` supplies fallibly loaded Driver API functions. Each copy holds
pinned staging until its event completes. Engine TCP/RDMA paths consume the
shared `runtime/proxy_manager.rs` chunk owner through `cuda/staging.rs`; public
allocation read/write retains private one-shot staging. Engine chunks can opt into
actual-owner GPU/NUMA location selection. `platform/numa.rs` owns fresh mapped
host backing and per-range placement; `cuda/pinned.rs` pairs that mapping with
CUDA host registration. The pool keys reuse by context and resolved location,
including page padding in its global budget. Registration/unregister/restoration
or unmap uncertainty retains backing and capacity. This is separate from the
RDMA rail selector and has no measured page-residency claim. See [local staging](staging.md). An enqueue/record failure
requires a successful stream drain; an ambiguous wait retains the allocation,
staging, stream/event and context. Native destroy success alone never proves
DMA complete. Handle cleanup failures retain the unresolved handles/context.

This follows the frozen TENT
[CUDAStreamPool](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/platform/cuda/cuda_stream_pool.cpp)
pattern of lazy nonblocking streams and owning handles, and
[NVLinkTransport](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_transport.cpp)
records an event after async copy work. OrbitTE reuses one stream/event
per serialized public allocation copy and one per physical staging slot for
remote source and requester staging. NVLink has a transport-owned bounded
`CUDAStreamPool` shared across actual local allocations and contexts; an exclusive
handle spans group completion/recovery and restoration. Idle slots retain only
native resources/context. Creation and destruction happen outside the pool mutex;
uncertain slots remain charged and cannot be reused. `StagingCopy` owns an exclusive GPU range
loan and actual allocation reference while the allocation mutex is released
between metadata updates; disjoint sessions can progress together.
`runtime/proxy_manager/stage_buffer_cache.rs::StageBufferCache` owns the requester ring's private registrations
and drain. Each `PinnedStage` owns its accepted pending copy; completion is
consumed once before the slot is reused or retired. Local WRITE slots release
after CROSS and local READ slots after POST, matching TENT's separate local and
remote stage ownership. `requester_task.rs` owns the bounded local chunk event loop and CROSS
queues. Each PRE/POST queries its recorded CUDA event; NOT_READY retains its
physical owners, while success requires context restoration. A ready slot can
refill without waiting for the rest of a window. `staging_task.rs::StagingTask`
owns one session's connection and bounded concurrent RDMA CROSS owners.
`rdma_transport/transfer.rs::RdmaTransfer` retains the submitted slices and
registration, queries exact payload work, then runs the existing bilateral
finish/cleanup. The synchronous RDMA entry point consumes the same owner.
CROSS admission is ordered; completion can reorder across slots. A scoped
`FinishCross` selects the native SEND/RQ/ACK/READY exchange before the terminal
receipt. Pin negotiates CROSS capacity separately from CUDA ring width;
per-CROSS slice caps and actual native credits prevent self-exhaustion. `worker_shard.rs::Workers` assigns sessions to at most eight shared
`WorkerShard`s, following TENT's per-shard task queue. Threads start on demand;
each queue holds at most 64 sessions. Pin starts only after dequeue. A task
terminal receipt follows destruction of its closure, including all private
registration and source references. The caller waits for this receipt before
retiring registrations, including cancellation and unwind. `source_task.rs` owns
source phases, including `InflightRemote` for each accepted CUDA Delegate.
`QueryDelegate` polls that slot's recorded event and returns a fully scoped
pending or terminal result. The requester keeps its generation occupied until
terminal confirmation, while querying and finishing other native CROSS owners.
This follows TENT's `submitRemoteStage` / `INFLIGHT_REMOTE` / `tryTakeResult`
ownership. Final Unpin waits for every local copy and terminal remote result.
`tcp/task.rs::TcpTask` owns a TCP payload snapshot, partial request/reply positions
and its actual local registration. Ordinary TCP and staging use the same bounded
nonblocking progress. `ControlOperation` retains one scoped partial response;
`TcpCross` sequences its outer header, TCP task and CROSS receipt. The session
never interleaves another operation into a partial frame.
`control_plane/connection.rs::Connecting` owns one nonblocking connect attempt,
checks both socket error and connected peer, and transfers that same socket into
`ControlChannel`. Its original deadline and typed `Stop::Task` follow the channel
through Pin and native RDMA handshake I/O; source handlers use `Stop::Source`.
Pin validates the full success response before publishing chunk geometry. Native
abort/close still drains accepted work, with uncertain cleanup retaining owners.
`rdma_transport/finish.rs` owns requester completion SEND, sealed grant drain and
partial DONE/Reply/ACK/READY progress. The active FinishCross stays in its inflight
slot through the outer scoped receipt; other native payloads remain queryable.
CqLane shares the same SEND submission and grant-drain validation with synchronous
callers. `rdma_transport/setup.rs::RdmaSetup` owns the parent plan, grant credits,
selected devices, pending requester handshake and prepared slices.
`rdma_transport/requester.rs::RequesterSetup` owns a slice's pending grant lease,
then its actual grant, partial frames and validated connection/binding.
`rdma_transport/connections.rs::Connecting` retains the canonical endpoint array
and CQ lanes across cold-construction polls. It uses the same claim/reserve/share
order as synchronous source acquisition. Pending requester polls retain admission
without constructing extra QPs or waiting for another builder. Staging keeps one outer CROSS
framing owner through setup and its scoped receipt while observing other submitted
payloads. Ordinary synchronous RDMA drives the same setup states and validators.
`cq_lane/retirement.rs` fences all physical-QP affiliates before publishing CLOSE.
Requester finish polls the same retirement state used by synchronous close;
busy record/store locks yield before mutation. Only Closed transfers the drained
grant output; Retained waits for the complete lane receipt and worker detachment.
Native cleanup remains on the existing Workers pool, so a blocked provider also
blocks other lanes assigned to that worker. See [retirement progress](staging.md#nonreusable-qp-retirement-progress).
Pin, native construction/connect/copy, aggregate failure drain and source handlers
retain synchronous boundaries; full asynchronous control and location scheduling remain open. The slot retains both handles across sessions; close
releases them before its pinned allocation. The separate NVLink transport shares the copy
completion owner; NVLink shares context-matched streams separately through
`CUDAStreamPool`. `cuda/readiness.rs::CallerSyncEvent` separately owns a GPU
producer event generation, actual context and retained producer inputs. An
explicit caller-thread record publishes readiness to the allocation; shared
`DeviceAllocation::prepare_access` consumes it before copy, staging, DMA or IPC
access. Aliases consume each generation once and reuse its event. Final drop
drains any pending generation. Unproven readiness fences the real allocation.
This follows TENT's caller-sync event boundary, extended across transport and
thread ownership; see [producer readiness](rdma-cuda.md#producer-stream-readiness).
`PinnedAllocation` reuses this same event owner in a lazy `OnceLock` slot. Its
existing access preparation and final native release consume pending readiness;
ordinary staging without a producer skips the optional event mutex. The
`HostMemory` lock excludes CPU/native access during explicit capture, and all
aliases share the actual pinned backing. Host DMA/IPC admission checks existing
loans before consuming readiness. See [pinned handoff](rdma-cuda.md#pinned-host-producer-handoff).
Public read/write and producer event waits remain synchronous; initial allocation
zeroing still synchronizes its context. Consumer-stream chaining is not exposed;
consumer access follows engine completion. Broader scheduling remains open.

## CUDA IPC transport ownership

`transport/nvlink.rs::NVLinkTransport` owns same-host identity checks, grant/close
bindings, physical mapping quotas, the shared stream pool and completion
progress. It is a sibling of TCP and RDMA.
`cuda/ipc.rs::IpcExport` owns the allocation-wide source loan; `IpcMapping` owns
one imported native reference, its context and every touched local allocation.
`transport/nvlink/mapping.rs::OpenedMapping` owns the mapping charge and explicit
native close. `subbatch.rs::NVLinkSubBatch` owns resolved consecutive
same-region/context copies. `cuda/ipc/copy.rs` acquires actual GPU/pinned host
owners in one deduplicated lock order, then submits the group on its exclusive
`CUDAStreamHandle`. `cuda/ipc/batch.rs::CopyBatch` owns native descriptor arrays
and checks unordered-member eligibility. `cuda/driver.rs` probes the optional
CUDA 13 versioned entry; independent groups use one native call, while dependent
groups or unavailable capability retain ordered enqueue. `NVLinkTransport` acquires the handle before memory access
locks and returns it before mapping-close/control I/O. The pool matches the
retained actual context and survives changes of first allocation. Memory guards
protect publication of whole-allocation loans and enqueue. `IpcCopyGroup` owns
those loans and the stream handle across completion polls without holding the
allocation mutexes. `progress.rs` first queries on the caller, so already-ready
events avoid a channel allocation and worker hop. Pending events use one lazy
worker, bounded by stream admission; it queries every pending group in its retained native context.
`NOT_READY` yields to other groups. Proven completion plus restoration releases
loans/stream before the caller performs source receipt or mapping-close I/O.
Conflicting NVLink allocation waits obey the request deadline; CPU/RDMA access
to an in-flight owner fails until its loan ends.
The optional `cache.rs::RelocationCache` owns bounded single-flight imports.
At capacity it selects an idle LRU owner, reserves both old and replacement
keys, and closes outside the map lock before committing the replacement.
`flush_nvlink_cache` uses the same exclusive leases and close receipts without
closing outgoing admission. Uncertain native owners remain charged.
`export.rs::ExportCache` owns the independent persistent source registration,
whole-allocation loan and physical charge. Each cached submission revalidates
source authorization, then releases drained local owners at operation completion.
`cuda/stream.rs` owns actual copy completion. ControlService keeps cached close
receipts available after source admission closes; Engine drains outgoing imports
and stops completion progress/closes the stream pool before incoming exports. Legacy uncertain handlers and current uncertain
requester DMA still retain their admissions.

This corresponds to TENT's NVLink allocation export, address relocation and
stream/event completion. One grant covers a bounded ordered group, with one
trailing event and a final import close/source receipt. A clean ordered enqueue
failure can confirm a recovered prefix; a failed native batch never identifies
such a prefix and is never replayed; shared record/query/restore failure covers the
whole attempted group. Native uncertainty retains and fences actual owners. Native TENT keeps a transport-wide relocation cache;
OrbitTE closes imports at each subbatch boundary by default. With
`nvlink_cache_entries` enabled, imports survive until idle reclamation, explicit
cache flush or requester close. The initial
ready-offer handshake separates unexposed reservations from exposed exporter obligations; a late first request
cannot expose a handle after its requester already closed the original socket.

`cuda/pinned.rs::PinnedAllocation` owns real host allocation, primary context,
and its staging copy resources. NVLink uses the separate shared stream pool.
`HostMemory` owns the exclusive IPC loan through event/context completion and
fences CPU/RDMA access on uncertainty.
`IpcMapping` retains the existing `Allocation::Host` / `Allocation::Cuda` owners;
mixed groups still share only their actual native context and remote GPU grant.
The shared query worker follows TENT's `NVLinkSubBatch` completion-event lifetime;
TENT queries it in `getTransferStatus`. NVLink setup and receipts remain caller
work, and submissions queued inside one Batch remain FIFO.
See [CUDA IPC](cuda-ipc.md)
for whole-allocation permission, exporter lifetime and remaining parity limits.
