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
| `ConnectionResources`, `QpStore` | Actual QP, CQ-local lookup and idle/retiring records | Endpoint QPs, WR depth and retirement |
| `RdmaGrant`, `GrantResources` | One exact-range DMA loan, source MW, message storage and grant charge | Rust authority extension over TENT's registered-region model |
| `RdmaSlice`, send/receive queues | Payload geometry, WR IDs, accepted work and completion debt | `RdmaSlice`, per-QP slice queues |
| `CqLane` | SQ/RQ CQ pair, registered QP records, submission, drain and recovery | Per-context CQs consumed by workers |
| `Workers` | Transport data threads, lane assignment and unfinished-grant load | `Workers::submit`, `workerThread`, `asyncPollCq` |
| `Monitor` | One transport thread consuming context and CQ events | Event-consumption portion of `Workers::monitorThread` |
| `DeviceSelector`, `CqQuota`, `ReceiveCredits` | NIC byte load, aggregate SQ/RQ wakes and whole-parent receive admission | Device selection and bounded slice admission; OrbitTE's receive-credit protocol is additional |

Each context slot owns its PD/context, CQ lanes and peer endpoint registry.
Physical QP records own connection and active grant resources. A live grant
retains its lane, worker pool and peer endpoint. Peer arrays retain QP records
and weak lane references, avoiding an endpoint/lane ownership cycle.

Native cleanup is fallible. A failed QP/MR/MW/CQ teardown retains the actual
owner, backing and charges; a timeout or terminal task state cannot substitute
for physical drain. A separate shared thirty-second parent destruction window
preserves the original data failure unless cleanup reports unquiesced work.

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
their existing owner; they do not introduce forwarding objects or extra locks.

| Read | Location | Responsibility |
| --- | --- | --- |
| Public request | `engine.rs`, `batch.rs`, `memory.rs` | Engine, tasks and actual backing |
| Transport entry | `transport/rdma/rdma_transport.rs` | Registration, parent dispatch and source serving |
| Context selection | `context_set.rs`, `context.rs`, `discovery.rs`, `quota.rs` | Stable slots, PDs, devices and NIC admission |
| Peer rendezvous | `rdma_transport/handshake.rs` | Existing transport's grant preparation and bilateral handshake |
| Wire data | `protocol.rs`, `path.rs` | OrbitTE grant receipts and static NIC/PD path metadata |
| Persistent owners | `buffers.rs`, `endpoint.rs`, `endpoint_store.rs`, `qp.rs`, `qp_store.rs`, `grant.rs` | MR, peer, QP and exclusive grant lifetime |
| CQ execution | `cq_lane.rs`, `cq_lane/submit.rs`, `cq_lane/reuse.rs`, `cq_lane/poll.rs` | Lane lifecycle, work admission/waits, drained reuse and native progress |
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
| QP arrays | Demand-grown peer arrays; one active grant owns one QP | TENT's fixed-width arrays admit concurrent slices per QP using separate depth and slice queues |
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
