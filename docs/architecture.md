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
OrbitTE v14 messages; moving them into their own module does not make this
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
QP creation lock. The public v14 handshake can attach independent grants to a
connected QP. The observed previous grant is a lower bound, while both process
and connection identities still match exactly. Endpoint observations only move
forward when concurrent handshakes arrive out of order. Cold construction
reserves the same fixed array used for established pairs.

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
| Context owners | `context_set.rs`, `context.rs`, `discovery.rs`, `quota.rs` | Stable slots and reclaim, PDs, device discovery and NIC admission |
| Peer rendezvous | `rdma_transport/connections.rs`, `rdma_transport/handshake.rs` | Fixed-width connection claims, grant preparation and bilateral publication |
| Wire data | `protocol.rs`, `path.rs` | OrbitTE grant receipts and static NIC/PD path metadata |
| Persistent owners | `buffers.rs`, `endpoint.rs`, `endpoint_store.rs`, `qp.rs`, `qp_store.rs`, `grant.rs` | MR, peer, QP and exclusive grant lifetime |
| QP work ownership | `qp/send_queue.rs`, `qp/receive_queue.rs`, `slice.rs`, `receive.rs` | Physical SQ/RQ ownership, independent grant work and binding/key receive dispatch |
| CQ execution | `cq_lane.rs`, `cq_lane/submit.rs`, `cq_lane/reuse.rs`, `cq_lane/completion.rs`, `cq_lane/poll.rs` | Lane lifecycle, admission/waits, drained reuse, CQE dispatch and native progress |
| Scheduling and events | `workers.rs`, `monitor.rs`, `events.rs`, `cq.rs` | Data threads, event consumer and actual CQ/channel owners |
| Verbs boundary | `sys.rs`, `sys/device.rs`, `sys/qp.rs`, `sys/memory.rs`, `sys/cq.rs` | Fallible function table and native calls grouped by resource |
| Host and GPU | `platform/cpu_probe.rs`, `platform/cpu_probe/affinity.rs`, `cuda/` | Host discovery/worker binding and NVIDIA staging owners |

`rdma_transport/contexts.rs` extends `RdmaTransport`; `context_set.rs` owns only
the slot state and its monitor-driven reclaim. The methods retain their existing
lock scopes, call order and visibility to the RDMA module. This mirrors TENT's
transport-owned `RdmaContextSet` and context-owned PD/CQ/endpoint resources
without introducing a forwarding owner or changing cleanup semantics.

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
in P6; GPU direct paths and native wire compatibility remain separate stages.

## Correspondence and remaining differences

The reference is frozen at `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`.
TENT's [transport header][transport], [endpoint implementation][endpoint] and
[workers implementation][workers] are the source references, not a moving main.

| Area | Current OrbitTE | Remaining correspondence |
| --- | --- | --- |
| QP arrays | Fixed-width, lazily populated arrays with concurrent grants and native-drain-aware rollback | TENT constructs its whole QP list eagerly and can use one array for both directions/operations; OrbitTE keeps role/capability-specific arrays |
| Monitor | Unified get/ACK, periodic reclaim, initially paused contexts and automatic GID reselection | TENT also refreshes effective link bandwidth. OrbitTE preserves conservative retained-resource quarantine; focused native recovery passes, with source and bounded RoCE evidence independently accepted |
| Path metadata | `path.rs` validates frozen NIC/PD descriptions and compatible pairs | Selected-port health lives in `Monitor`/`RdmaContext`; native `RailMonitor` also supplies broader failover/retry policy, which remains open |
| Grant authority | Exact-range MW and source receipt protocol | Deliberate Rust authority contract, not native TENT wire parity |
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

`cuda::CudaMemory` owns the allocation lock; `cuda::stream::CopyStream` owns its
lazy nonblocking stream, reusable completion event and pending-work state.
`cuda::driver` supplies fallibly loaded Driver API functions. Each copy holds
private pinned staging until its event completes. An enqueue/record failure
requires a successful stream drain; an ambiguous wait retains the allocation,
staging, stream/event and context. Native destroy success alone never proves
DMA complete. Handle cleanup failures retain the unresolved handles/context.

This follows the frozen TENT
[CUDAStreamPool](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/platform/cuda/cuda_stream_pool.cpp)
pattern of lazy nonblocking streams and owning handles, and
[NVLinkTransport](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_transport.cpp)
records an event after async copy work. OrbitTE currently reuses one stream/event
per serialized allocation, rather than implementing TENT's device-wide pool or
NVLink transport here. External producers still synchronize before handing over
memory. Public read/write completion remains synchronous; initial allocation
zeroing still synchronizes its context. Transport-level scheduling and caller
stream readiness remain P5 work.
