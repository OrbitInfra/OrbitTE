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
OrbitTE v12 messages; moving them into their own module does not make this
protocol compatible with TENT's native RPC or metadata.

## RDMA resource owners

| Owner | Resource and lifetime | Frozen TENT correspondence |
| --- | --- | --- |
| `RdmaTransport` | Context slots, registrations, workers and parent dispatch | `RdmaTransport`, `context_set_`, `local_buffer_manager_`, `workers_` |
| `ContextSet`, `RdmaContext` | Stable NIC IDs, actual context/PD, route and persistent key domain | `RdmaContextSet`, `RdmaContext` |
| `LocalBufferManager` | Persistent host MRs keyed by actual context, allocation view and capability | Context-keyed `mem_reg_map` |
| `EndpointStore`, `RdmaEndpoint` | Peer identity and persistent QP array across CQ lanes | Context endpoint store, `RdmaEndPoint::qp_list_` |
| `RdmaQp`, `ConnectionResources`, `QpStore` | One actual QP shared by independently retained grant records; CQ-local lookup and retirement | Endpoint QPs and affiliated slice ownership |
| `qp::send_queue::SendQueue` | Physical QP send depth, exact outstanding WR IDs and fenced admission | Per-QP `wr_depth_list_` and `slice_queue_` accounting |
| `qp::receive_queue::ReceiveQueue` | QP-owned receive buffers/MRs, RQ debt and binding/key dispatch | QP-owned receive WRs; exact grant receipts are a Rust extension |
| `RdmaGrant`, `GrantResources` | One exact-range DMA loan, source MW, SEND message storage and grant charge | Rust authority extension over TENT's registered-region model |
| `RdmaSlice`, `SendWork`, `ReceiveWork` | Per-grant geometry, WR windows and independent completion identities | `RdmaSlice` work and completion state; source receipts are a Rust extension |
| `CqLane` | SQ/RQ CQ pair, registered grant records, submission, drain and recovery | Per-context CQs consumed by workers |
| `Workers` | Transport data threads, lane assignment and unfinished-grant load | `Workers::submit`, `workerThread`, `asyncPollCq` |
| `Monitor` | One transport thread consuming context and CQ events | Event-consumption portion of `Workers::monitorThread` |
| `DeviceSelector`, `CqQuota`, `ReceiveCredits` | NIC byte load, aggregate SQ/RQ wakes and whole-parent receive admission | Device selection and bounded slice admission; OrbitTE's receive-credit protocol is additional |

Each context slot owns its PD/context, CQ lanes and peer endpoint registry.
`RdmaQp` serializes one physical connection; each CQ-lane `Record` owns one
grant's state, namespaces, work and waiters. Multiple records can retain the
same QP. The connection retains its `SendQueue` and `ReceiveQueue` across grants. `SendWork` borrows
the shared SQ; `ReceiveWork` tracks an exact binding/key receipt independently
of the buffer that receives it. A live grant
retains its lane, worker pool and peer endpoint. Peer arrays retain QP records
and weak lane references, avoiding an endpoint/lane ownership cycle.

Native cleanup is fallible. A failed QP/MR/MW/CQ teardown retains the actual
owner, backing and charges; a timeout or terminal task state cannot substitute
for physical drain. A separate shared thirty-second parent destruction window
preserves the original data failure unless cleanup reports unquiesced work.

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
QP creation lock. The public v12 handshake still claims a QP exclusively.
Its all-slices-first preparation and fixed-width construction remain in the
first P4 exit item; the internal concurrent path does not activate that protocol.

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

Native SQ and RQ depths are both provisioned from `max_send_wr`; each queue
has a separate aggregate CQ budget. TENT likewise owns WRs at the physical
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
owners directly; public v12 multiplexing and fixed-width peer activation remain
open. See [qualification](qualification.md) for the exact tested scope.

## Follow one transfer

1. **Register.** Host Direct publication prepares local-only MRs and separate
   MW-bind parents in every enabled context before making the range visible.
   Partial failures roll back only newly created copies. Device masks restrict
   dispatch, not the set of enabled contexts receiving registration.
2. **Plan.** The transport partitions large parents with TENT's bounded,
   short-tail-folding geometry. Static `path` metadata checks compatible peers;
   `DeviceSelector` applies device masks, topology ranks and byte charges.
3. **Authorize.** The source reserves all receive credits for the parent before
   creating grants. Rendezvous verifies both process/connection identities,
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

All contexts share aggregate byte, QP and SQ/RQ limits. Quota return wakes
waiting lanes even when they have no local CQE to wake them. Completely drained
faulted lanes can be replaced while the PD, persistent MRs and worker pool stay
alive. Workers bind before admission to NUMA CPU sets intersected with the
creator's allowed mask; this does not place application pages near an RNIC.

The monitor serializes native get/ACK with event-source detachment. CQ channels
stay lane-owned; async FDs stay device-owned. Native owners close only after
detachment. Lost event-consumption proof fences the affected device and retains
unproven native resources. See [the lifetime contracts](rdma-cuda.md) for exact
failure transitions and [qualification](qualification.md) for scoped evidence.

## Source reading order

Start at an operation, then follow the resource it borrows. Child modules extend
their resource owner; submission and cleanup stay beside the resources they protect.

| Read | Location | Responsibility |
| --- | --- | --- |
| Public request | `engine.rs`, `batch.rs`, `memory.rs` | Engine, tasks and actual backing |
| Transport entry | `transport/rdma/rdma_transport.rs` | Registration, parent dispatch and source serving |
| Context selection | `context_set.rs`, `context.rs`, `discovery.rs`, `quota.rs` | Stable slots, PDs, devices and NIC admission |
| Peer rendezvous | `rdma_transport/handshake.rs` | Existing transport's grant preparation and bilateral handshake |
| Wire data | `protocol.rs`, `path.rs` | OrbitTE grant receipts and static NIC/PD path metadata |
| Persistent owners | `buffers.rs`, `endpoint.rs`, `endpoint_store.rs`, `qp.rs`, `qp_store.rs`, `grant.rs` | MR, peer, QP and exclusive grant lifetime |
| QP work ownership | `qp/send_queue.rs`, `qp/receive_queue.rs`, `slice.rs`, `receive.rs` | Physical SQ/RQ ownership, independent grant work and binding/key receive dispatch |
| CQ execution | `cq_lane.rs`, `cq_lane/submit.rs`, `cq_lane/reuse.rs`, `cq_lane/completion.rs`, `cq_lane/poll.rs` | Lane lifecycle, admission/waits, drained reuse, CQE dispatch and native progress |
| Scheduling and events | `workers.rs`, `monitor.rs`, `events.rs`, `cq.rs` | Data threads, event consumer and actual CQ/channel owners |
| Verbs boundary | `sys.rs`, `sys/device.rs`, `sys/qp.rs`, `sys/memory.rs`, `sys/cq.rs` | Fallible function table and native calls grouped by resource |
| Host and GPU | `platform/cpu_probe.rs`, `platform/cpu_probe/affinity.rs`, `cuda/` | Host discovery/worker binding and NVIDIA staging owners |

Paths after the transport entry are relative to `src/transport/rdma/`, except
the public, host and GPU rows, which are relative to `src/`.
Private tests remain under `tests/unit/`; moving methods preserves the existing
transport and hardware test selectors. `BoundDone` can still be constructed
only by the protocol's successful bound DONE read.

## Correspondence and remaining differences

The reference is frozen at `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`.
TENT's [transport header][transport], [endpoint implementation][endpoint] and
[workers implementation][workers] are the source references, not a moving main.

| Area | Current OrbitTE | Remaining correspondence |
| --- | --- | --- |
| QP arrays | Demand-grown peer arrays; QP-owned send depth/debt, but one active wire grant per QP | Concurrent runtime records/CQ lookup exist; public handshake admission and fixed-width peer arrays remain open |
| Monitor | Unified async/CQ get and ACK; lane workers drain retired QPs | TENT also performs periodic endpoint/orphan reclaim, paused-port recovery and address/link-speed refresh |
| Path metadata | `path.rs` validates frozen NIC/PD descriptions and compatible pairs | Native `RailMonitor` additionally owns dynamic rail health and recovery; a static codec is not that owner |
| Grant authority | Exact-range MW and source receipt protocol | Deliberate Rust authority contract, not native TENT wire parity |
| CQ layout | Separate SQ/RQ CQ pair per lane | TENT uses a lane CQ for its data QPs; ownership and accounting are preserved with a different CQ layout |
| Worker load | Unfinished grant charge including retained native work | Priority queues, QoS, retry/failover and deadline policies remain P6 work |

The `cpu_probe` filename does match TENT's host-probing source. Names that match
native concepts retain that meaning; new Rust ownership or protocol mechanisms
are identified explicitly. Architecture alignment alone does not establish
behavioral, ABI, wire or performance parity.

[transport]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/transport/rdma/rdma_transport.h
[endpoint]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/endpoint.cpp
[workers]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/workers.cpp
