# CUDA staging

`runtime/proxy_manager.rs::ProxyManager` owns an engine-wide pool of reusable
CUDA-pinned host chunks. TCP and staged RDMA share that pool for device-to-host
and host-to-device copies. Each payload transport still owns its network buffers,
MRs, grants and completion receipts.

## Configuration and lifetime

With the `cuda` feature, `EngineConfig.staging_chunk_size` defaults to 4 MiB and
`staging_chunk_count` to 64. Allocation is lazy: one local logical GPU copy checks out
one chunk and processes its full range, including the final short chunk. The
actual `CudaMemory` allocation lock remains held across all chunks. Public
`CudaMemory::read/write` retain their existing one-shot staging behavior.

The pool bounds pinned copy bytes across source and requester operations and
all local CUDA contexts. Size/count must be positive, size at most 64 MiB, count
at most 4096, and their physical allocation product at most 1 GiB.
NUMA-backed slots include page padding in that product and in `reserved_bytes`. Active, constructing, closing and
retained chunks count against the bound. Capacity exhaustion returns
`ResourceExhausted`; it does not create an uncharged allocation or wait for a
nested request. TCP payload buffers and RDMA staging/MR quotas remain separate.

Idle chunks reuse their retained native CUDA context and resolved host location.
A different context or location may
replace an idle chunk only after its native release and caller-context restore
both succeed. Native allocation, copy and cleanup never run under the pool map
lock. The retained context prevents address reuse while a chunk is cached.

A lease becomes unsafe to return before native submission. Only event completion
(or recovery stream drain) and caller-context restoration permit return. Failed
completion retains the actual chunk, native context, GPU owner and parent
admission. Partial native allocation and failed native cleanup also keep their
slot charged. Successful free followed by a failed context pop is recorded
separately; another close or Drop never repeats the free.

Deadlines are checked at each drained chunk boundary. They cannot preempt a
native CUDA call. A partially copied logical request fails with no completed
request bytes; partial destination effects are possible and must not be replayed
automatically. Cancellation keeps the existing task boundary: queued requests
may be cancelled; a running logical copy continues through drain.

Directional `close_source` and `close_requester` leave the shared pool available
to the other direction. Full `TransferEngine::close` fences and drains both,
then closes idle chunks. A timeout permits another drain wait; uncertain native
cleanup remains retained. `EngineStats.staging` exposes reserved/idle/active/
retained chunk counts and allocation, reuse, completed-chunk and byte counters.
Chunk byte counters describe local copy progress, including a completed prefix
of a failed logical request; batch completion remains a separate contract.

## Host staging location

`EngineConfig.staging_location` applies to engine-owned CUDA staging chunks in
local copies and both roles of a remote session. Each endpoint resolves its own
actual GPU context; a peer's CUDA ordinal is never used as a local device id.
CPU requester scratch buffers and public `allocate_pinned_host` / standalone
`CudaMemory::read/write` retain their existing allocation paths.

| Setting | Behavior |
| --- | --- |
| `StagingLocation::Any` (default) | Preserve CUDA `cuMemHostAlloc`; no NUMA placement claim |
| `StagingLocation::Auto` | Query the actual context device's PCI BDF, read its sysfs NUMA node, prefer that eligible node, otherwise choose the lowest eligible node |
| `StagingLocation::Cpu(N)` | Require host location `cpu:N`; unavailable nodes fail before allocation |

Eligible nodes have online memory and appear in the calling thread's allowed
memory-node set. Missing or negative GPU locality stays unknown; it does not
mean node zero. Auto then selects the first eligible host node, as TENT's
`Topology::findNearMem` does after failing to find a matching NUMA location.
Discovery errors, an empty eligible set and a selected node's allocation/binding
failure are errors. They do not silently switch to the wildcard allocator.
No topology is exchanged in the v21 wire protocol.

`Any` preserves OrbitTE's existing driver policy. TENT also spells a wildcard
location `*`, but its frozen host allocator resolves that wildcard to node zero;
the two default allocation policies are not claimed equivalent.

The NUMA backing owns a fresh, page-rounded anonymous private mapping. It applies
`mbind(MPOL_BIND | MPOL_F_STATIC_NODES)` before touching pages, then registers the
whole mapping with `cuMemHostRegister`. Logical chunk lengths remain unchanged.
This follows Linux's [range-policy semantics](https://man7.org/linux/man-pages/man2/mbind.2.html)
and CUDA's [registered-host lifetime](https://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__MEM.html).
No thread memory policy or existing application page is changed. The pool still
has one global slot/byte bound across contexts and locations; moving an idle
slot frees the old allocation before constructing its replacement.

Private MRs retire before a drained chunk returns to the pool. Final pool cleanup
closes its copy streams/events, calls `cuMemHostUnregister`, restores the CUDA
context, and only then unmaps the host pages. A failed registration or uncertain
unregister/context restoration/unmap retains the actual mapping, context and
slot charge. Repeated close/Drop cannot repeat a native release. The original
`cuMemHostAlloc` backing continues to pair with `cuMemFreeHost`.

The platform boundary lives in `platform/numa.rs`, CUDA registration/backing in
`cuda/pinned.rs`, selection beside the actual context in `cuda/staging.rs`, and
reuse/budgets in `runtime/proxy_manager.rs`. This matches TENT's
`findStagingPolicy` → `Topology::findNearMem` → per-location `StageBuffers` →
platform allocator responsibilities. OrbitTE limits this stage to host staging
for its current CUDA owners. It does not yet add arbitrary GPU staging locations,
automatic transport choice, topology publication or GPU-near RDMA rail routing.

CPU fixtures exercise real mapped bytes and public TCP/RDMA paths with simulated
CUDA and NUMA binding. They do not establish physical page residency, actual
CUDA registration, DMA overlap or performance. A successful binding policy is
not a lasting residency guarantee if the OS/cpuset changes. Hardware requests
remain paused; see [qualification](qualification.md#host-staging-location).

## TENT correspondence

The frozen [TENT ProxyManager](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/runtime/proxy_manager.h)
owns location-specific `StageBuffers`, chunk Pin/Unpin and staged work. Its
[implementation](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/proxy_manager.cpp)
separates PRE/CROSS/POST work, remote staging operations and deferred cleanup.

P5.5a implements the local chunk owner and consumes it in TCP/RDMA CUDA copies.
It uses one global engine bound with retained-context chunks rather than an
independent budget per location. `cuda/staging.rs` owns the local copy loop;
`cuda/stream.rs` retains the allocation's stream/event completion authority.
There is no extra Proxy worker pool in this substage.

P5.5b adds an opt-in remote operation with serial and bounded pipeline modes. The names and phases follow
TENT's `pinStageBuffer`, `delegateAsync`, `submitCrossStage` and
`RemoteStageOperation`: the latter distinguishes an observed remote result from
an unconfirmed operation before deferred cleanup. OrbitTE uses scoped receipts
and an inline source handler to preserve that distinction with bounded resources.
The new bounded remote mode follows `transferEventLoop`'s ring of at most 16
stage buffers (`id % kStageBuffers`). `source_task.rs` owns source `Chunk`
scheduling; `requester_task.rs` reserves the local ring and full GPU range loan.
`requester_task.rs` also owns local PRE/POST scheduling and bounded CROSS queues.
`staging_task.rs::StagingTask` owns the connection and bounded CROSS progress;
`worker_shard.rs` queues these sessions on shared `WorkerShard`s. `StageBufferCache` retains the request's private
registrations and buffers, following the resource role of TENT's cache of the
same name. `stage_registration.rs::PinnedStage` owns the actual pinned lease,
private registration, pending copy and ordered retirement on either side.
`remote_stage_operation.rs` owns scoped messages and receipts. Per-slot CUDA completion supplies the
PRE/POST completion boundary, while TCP/RDMA still own CROSS.

The current implementation can keep both source and requester CUDA copies
pending during CROSS. Independent local and remote rings follow TENT's
`local_locked` / `remote_locked` ownership: local WRITE slots become reusable
after CROSS, and local READ slots after POST. The requester event loop tracks at
most `L` chunks and submits CROSS commands in logical order, while querying each
local PRE/POST independently. A ready slot immediately starts its next chunk;
there is no whole-window refill barrier. Local progress continues while a CROSS
or Delegate receipt is pending. The worker submits multiple RDMA CROSS chunks
on one connection and queries their payload completion independently.
`rdma_transport/transfer.rs::RdmaTransfer` owns the actual slices and registration,
matching TENT's submitted internal batch and `progressBatch` boundary. The
existing transport workers still own CQ progress. Each control exchange is
serialized. `transport/tcp/task.rs::TcpTask` owns TCP payload progress; the
staging `TcpCross` also retains its outer header and scoped CROSS receipt.
Frozen TENT creates eight shared worker shards; each dequeues one task and runs
its chunk event loop to completion. OrbitTE starts at most eight shards on demand,
reuses an idle shard and otherwise selects the smallest queue with rotating ties.
Each shard still executes one session at a time. TENT's `transferEventLoop`
polls PRE/CROSS/POST batches and remote operations, releasing each local buffer
at its own phase boundary. OrbitTE uses the same chunk phases for requester
progress, with `CopyStream` querying the recorded CUDA event and the connection
worker publishing CROSS receipts. Idle sweeps follow TENT's 128 yields followed
by exponential delays from one to 200 microseconds, bounded by the deadline.
Source CUDA Delegate submits an event at every ring width. Scoped
`QueryDelegate` polls its completion once, so NOT_READY can leave other
submitted CROSS owners progressing. TCP payload and Delegate/query/Unpin socket
progress are bounded and nonblocking. Connect uses a single pending socket;
worker stop also interrupts Pin and native setup socket waits. Requester native
finish polls SEND and reusable-grant cleanup, retaining partial completion frames
while other CROSS payloads remain queryable. Requester native setup also retains
partial negotiation frames and yields to other submitted CROSS payload queries.
Waiting for a cold requester QP builder also yields, preserving one grant lease
and the canonical endpoint array until publication or rollback.
Nonreusable requester QP retirement also polls the existing native owner.
Pin, native construction and cleanup calls, copyback, source handlers and ordinary local GPU snapshot/commit
retain synchronous boundaries.
Remaining control progress and broader location scheduling stay in the P5/P6 queue. CPU controls establish scheduling and owner
lifetime; physical DMA overlap and throughput require hardware measurement.

## Serial remote staging

Build with `cuda` and set `EngineConfig.remote_staging = true` on the source
whose CUDA registrations may accept Pin. Select
`TransferRequest.remote_staging = true` for each outgoing TCP/RDMA request that
should use Pin/Delegate/Unpin.
The requester need not accept incoming Pin itself. Both CUDA Direct registration
and incoming Pin may be enabled in one engine.

| Setting | Responsibility |
| --- | --- |
| `EngineConfig.remote_staging` | Source capability: allow Pin against actual CUDA ranges; defaults to false |
| `TransferRequest.remote_staging` | Per-request choice: true selects remote staging, false uses ordinary transport/memory policy |

Every request literal now supplies `remote_staging`. Migrate ordinary requests
with `false` and old remote-staging submissions with `true`; enabling only the
engine capability no longer redirects outgoing traffic. False does not force
GPUDirect: `RdmaCudaMemory` still selects Direct or local staging for ordinary
RDMA, and ordinary GPU-over-TCP still uses local staging.

Remote host ranges and an opted-out source are rejected by the source. Its
control owner checks epoch, registration, range and permission before Pin checks
the actual allocation type. Discovery descriptors do not contain remote memory
type, so this rejection is authoritative at the source, not a local type guess.
Requester ranges may be CPU or CUDA. Remote staging with NVLink, or without the
`cuda` feature, fails during whole-submission preparation before earlier requests
can execute. TCP/RDMA remain the payload transports; selection is explicit.

One ordered batch can mix GPU Direct RDMA, remote TCP/RDMA staging and ordinary
host transfers. Prepared owners retain each request's choice. Nested CROSS and
RDMA slices clear remote-staging intent and call their payload transport directly.
The private chunks are host memory: `RdmaHostMemory` controls their grants even
when `RdmaCudaMemory::Direct` is enabled. Direct host MRs are acquired lazily in
the used context, reused across chunks and retired at session completion.
GPU MRs may stay cached, while each request releases its active range loan before
the next staging or direct operation. Staging chunks, copy streams/events and
worker shards keep their existing bounded reuse.

Frozen TENT's `PreparedSubmit::Owner` stores a route and staging plan;
`commitPreparedSubmit` dispatches staging owners to `ProxyManager` and direct
owners to their transport. OrbitTE follows that ownership split with explicit
request choices; TENT's automatic selector and subbatch scheduling remain P6.
See [source qualification](qualification.md#per-request-remote-staging).

| Phase | READ | WRITE |
| --- | --- | --- |
| Local preparation | Reserve one bounded private slot and borrow the full local GPU range | Same; no full-request snapshot |
| Pin | Validate source epoch/region/range/permission; borrow its GPU range and pin its slots | Same |
| PRE | Source GPU → pinned source slot, with event/context completion before CROSS | Local range → requester slot, with event/context completion before CROSS |
| CROSS | TCP/RDMA source slot → requester slot | Requester slot → TCP/RDMA source slot |
| POST | After the scoped FinishCross receipt, requester slot → local range | Delegate source slot → source GPU |
| Unpin | Drain accepted copies, retire private registrations/MRs and release source slots/range, then acknowledge | Same |
| Request completion | Require final Unpin receipt and requester slot/MR retirement | Same |

One control connection and one source admission own the entire session. At depth
one, CROSS commands are sequential, including the short tail. The local next PRE
or current POST can start while the worker awaits a remote Delegate receipt. Nested CROSS
calls the existing transport directly; it never recursively submits an ordinary
batch or waits for a second handler. One handler, one parent batch slot and one
pinned source chunk per engine are sufficient. A GPU requester also needs one
local pinned chunk; self-peer and opposite-direction sessions share their
engine pool. With pipelining enabled, each engine needs its outgoing local ring
plus its incoming source ring; self-peer capacity is `L + R` below. Insufficient
capacity fails immediately, without waiting for a slot held by the same session.
Native RDMA grant/CQ/MR budgets still apply.
RDMA uses its slices protocol even for a single chunk, retaining its bounded
same-connection QP-cache-miss retries and actual verbs payload path.

Each CROSS uses local offset zero in its selected private host slot. At the
default depth of one, CUDA requesters reserve one physical pool slot; larger
depths reserve a bounded local ring before Pin. Each physical slot retains its
stream/event across sessions. CPU requesters still allocate at most
`min(request.length, staging_chunk_size)` host bytes under the parent admission;
they do not spend CUDA pool slots. No full-request snapshot or commit payload
is allocated. TCP/RDMA retain their independent bounded transport resources.

Pin negotiates `min(source chunk size, requester chunk size)` bytes per logical
chunk. Each side still reserves and reports its actual configured physical slot
size. A larger source allocation does not enlarge the negotiated CROSS grant.
The tail uses its exact remaining byte count.

`cuda::StagingCopy` retains the actual `Arc<CudaMemory>` and an exclusive logical
range loan. Its allocation mutex protects only metadata, not network I/O or
slot event waits. Nonoverlapping source/requester ranges on the same allocation
can progress concurrently. Overlapping staging, CPU or RDMA access fails before
new work; whole-allocation IPC export conflicts with every staging range.
Self-peer overlap is rejected rather than providing snapshot-copy semantics.
A busy metadata mutex fails admission with `ResourceExhausted`. Pool exhaustion
also fails explicitly; no hidden pool or silent reduction of source depth is used.

Wire version 17 introduced PinRead/PinWrite, Delegate, CROSS and Unpin; version
18 added pipeline width and version 19 added the requester chunk cap.
Version 20 introduced a separate CROSS capacity and `FinishCross`.
Current version **21** adds `QueryDelegate`: Delegate acceptance no longer
permits READ CROSS, WRITE slot reuse or final Unpin. Both peers must use
version 21; older headers are rejected.
Each frame binds a fresh nonce, original registration, phase and logical range;
success receipts echo that full scope. A repeated prior receipt cannot stand in
for Unpin. Private chunk descriptors never appear in Describe and cannot be used
outside their session. These identities fence reuse; they are not authentication.

The CROSS receipt confirms acceptance of its native owner. A later scoped
`FinishCross` selects that owner's original SEND/RQ/ACK/READY exchange; its
terminal receipt follows actual transport grant cleanup. Local progress cannot
consume acceptance as completion. Unpin first unregisters
the private view, retires every native MR and verifies no private registration
borrower remains, then explicitly releases the chunk and GPU range loan. A transport
ACK alone never completes a remote GPU WRITE. Lost POST/Unpin replies leave WRITE
with `RemoteEffect::Unknown`. READ commits each completed chunk after its scoped
FinishCross receipt; a lost Unpin receipt still fails the request even when all chunks
are visible. A failed request contributes zero completed requests/bytes, but may
have changed bytes. Independent local POST streams can complete out of order;
a native failure does not establish an exact visible prefix. `locally_drained`
proves physical local completion, not rollback, and failed groups do not send
attached notifications. Successful local native drain allows cleanup after EOF, while uncertain
CUDA or RDMA cleanup retains actual backing and the responsible parent/source
admission. Ordinary confirmed errors remain errors even when locally drained.
There is no automatic retry of a possibly executed WRITE.

`EngineStats.staging.remote_pins`, `remote_delegates`, `remote_crosses` and
`remote_unpins` count source-side completed operations. Unpin returns the drained
chunk to the pool before its reply; native host unpin normally occurs when the
pool closes.
A lost reply can coexist with a completed source Unpin. These observations do
not replace requester completion receipts.

## Bounded remote pipeline

Set `EngineConfig.staging_pipeline_depth` to 2–16 on both peers to opt in.
It defaults to 1 and cannot exceed `staging_chunk_count`. For a request of `N`
bytes, physical chunk sizes `C_local` / `C_source` and configured depth caps
`D_local` / `D_source`, the rings are:

| Quantity | Bound |
| --- | --- |
| Logical chunk size `C` | `min(C_local, C_source)` |
| CUDA requester width `L`, reserved before Pin | `min(D_local, ceil(N / C_local))` |
| Source width `R`, negotiated by Pin | `min(D_local, D_source, ceil(N / C))` |
| CPU requester width | One ordinary host buffer, no CUDA pool charge |
| Concurrent CROSS window `W` | RDMA: minimum of both peers' native limits and `R`; TCP: one |

The requester reserves its entire local ring and full GPU range before opening
the control connection. The source checks out its entire negotiated ring before
acknowledging Pin. Insufficient capacity fails with `ResourceExhausted`; neither
side silently narrows its ring. A large local physical chunk can conservatively
limit `L` even when the smaller negotiated chunks would permit more concurrency.
There is no late expansion after Pin. The requester uses asynchronous copies
and queries even at width one. The source now also submits an asynchronous
copy at width one; all widths require terminal `QueryDelegate` confirmation.

RDMA advertises a separate native capacity in Pin. `W` is bounded by grant,
CQ/receive, endpoint and byte limits, up to 16. Each peer divides its per-CROSS
slice cap by `W`; the peer-advertised cap is already divided. A chunk may still
use multiple rails/slices, and every actual slice consumes the existing grant,
receive and byte credits. Competing requests can fail admission explicitly;
this session never waits for credits held by its own unfinished CROSS. The
requester may conservatively advertise fewer CROSS owners when its physical
chunk is larger than the negotiated chunk. CUDA ring sizes do not shrink.

Logical chunk `i` uses local slot `i % L` and source slot `i % R`. Each CUDA slot
owns its actual pinned allocation, a persistent stream/event and a private
registration for the current session. Scoped CROSS grants bind the source slot.
Source registrations retire at Unpin; requester registrations retire after its
receipt or after draining an errored session. Drained native allocations and
stream/events remain in the pool for later sessions. Full engine close destroys
those handles before freeing pins. Failed construction or destruction remains
charged without a Drop retry.

| Direction | Scheduling and completion |
| --- | --- |
| READ | Delegate prefills `R` source PREs. CROSS requires a terminal QueryDelegate result for its source PRE and completion of any previous POST in its local slot. Its terminal FinishCross receipt permits local POST to begin; the freed source slot queues the next PRE at `i + R`. Other local POSTs can remain pending until local slot reuse. |
| WRITE | Prefill `L` local PREs. CROSS waits for its local PRE and terminal QueryDelegate confirmation of any previous POST in the source slot. After FinishCross's terminal receipt and the Delegate send, the freed local slot queues PRE at `i + L` without waiting for that Delegate receipt; other PREs and source POSTs can remain pending. |
| Final Unpin | Drain all local POSTs and confirm every remote Delegate before sending Unpin. The source rejects premature Unpin, retires private registrations/MRs and releases its range before replying. Only the matching receipt and local retirement complete the request. |

Every Delegate receipt confirms **acceptance**, not CUDA completion.
`remote_delegates` advances after source event-query success (or failure-path
sibling drain) and successful context restoration.
Requester completed-chunk counters advance once after event query (or error-drain
wait) and successful context restoration. `remote_crosses` still follows actual transport
cleanup, and `remote_unpins` follows full session cleanup. WRITE becomes
`RemoteEffect::Unknown` before sending Delegate and is never automatically
replayed after an ambiguous reply.

### Remote Delegate progress

The source keeps each accepted Delegate in `InflightRemote` with its original
offset, generation, pinned bytes, private registration and GPU range loan.
QueryDelegate is legal only in this state. Its successful reply contains the
full echoed QueryDelegate frame plus one byte: `0` for pending, `1` for complete.
Both results bind the session nonce, original region, absolute offset and length.
Only event-query success and context restoration advance the state. Early,
duplicate, stale or mismatched queries fail before another CUDA query or copy.

The requester stores at most one pending Delegate per remote slot. Each sweep
queries it once; NOT_READY does not count as progress and does not hold the sweep
inside a native wait. A command that would reuse that slot is deferred while
other submitted RDMA CROSS owners can still finish. A WRITE's old generation
stays occupied until POST is confirmed. Unpin cannot substitute for any missing
terminal result. Lost replies are not replayed; WRITE retains Unknown once its
Delegate may have been sent.

Frozen TENT uses `submitRemoteStage`, `INFLIGHT_REMOTE` and
`RemoteStageOperation::tryTakeResult` with asynchronous RPC callbacks. OrbitTE
uses explicit scoped query exchanges on its existing connection to establish
the same separate completion/owner boundary. It does not yet provide TENT's
fully asynchronous RPC mechanism: each session still has one framing owner,
and Pin still occupies its session worker. Requester native
[setup](#native-rdma-setup-progress) and
[finish](#native-rdma-finish-progress) now have explicit socket progress. The
[cancellable connection](#connection-and-pin-lifetime) preserves the original
deadline and task stop through those socket waits.
Error/EOF cleanup still closes all accepted RDMA work, drains
all accepted CUDA siblings and only then retires private registrations. An
unconfirmed remote reply is never a native drain proof.

### Socket progress

One `TcpTask` owns its request frame, partial reply, bounded payload snapshot,
local registration and WRITE effect. Each turn attempts one socket operation of
at most 64 KiB; WouldBlock preserves its position and returns pending. READ commits
to local memory only after all payload bytes arrive. WRITE becomes Unknown before
its first possible payload send and requires the terminal ACK; neither partial
progress nor failure is replayed. The ordinary synchronous API drives this same
task. Its local GPU snapshot/commit remains synchronous, and public batch cancel
still does not interrupt an already running ordinary TCP operation.

For remote staging, `TcpCross` sequences outer CROSS header, inner `TcpTask` and
scoped CROSS receipt. `ControlOperation` separately owns Delegate, QueryDelegate,
FinishCross and Unpin request/response bytes. Sending a request and receiving its
scoped receipt are separate so RDMA can own its intervening native exchange.
Socket work is bounded to the small header and at most two small reads per turn; complete response framing and scope validation
precede any state transition. An incomplete socket result differs from a fully
received QueryDelegate NOT_READY. Initial queries follow Delegate acceptance
before admission of more CROSS work; later queries rotate across pending slots.
The wire remains v21.

The single socket never interleaves two exchanges. Native payload completion can
still be observed during a pending control response, but a bilateral finish must
wait for that response. A staging local failure or shard stop is checked between
polls, closes the interrupted session and joins its owner. It never appends Unpin
into a partial frame. Existing native drain and uncertain-owner retention still
precede private registration retirement. Public `Batch::cancel` retains its
existing queued-versus-running contract; this substage does not add an active
batch cancellation token to the transport API.

Frozen TENT's `TcpTask` names the ordinary TCP request owner; its ordinary TCP
workers use synchronous send/receive. Its separate `hp_tcp` provider has async
header/body/response state. OrbitTE uses explicit socket progress in its existing
TCP provider, and scoped control results corresponding to `delegateAsync` and
`RemoteStageOperation::tryTakeResult`. This neither adds an hp_tcp provider nor
claims wire compatibility. Native connection construction, provider-side cleanup, source handlers,
readiness notification and full asynchronous RPC parity remain open.

### Connection and Pin lifetime

`Connecting` creates one nonblocking socket and issues connect once. A pending
result retains that socket and the original deadline. Success requires both no
socket error and a connected peer; a terminal error or stop drops the socket.
There is no retry, second connection or replay. The successful channel carries
`Stop::Task` through every later control read/write. Source shutdown uses a
separate `Stop::Source`, preserving `Cancelled` versus `SourceClosed`.

Pin stays on the same worker. Its existing synchronous read/write loops retain
partial positions and recheck stop/deadline with at most 25 ms socket timeouts.
Decode the complete 36-byte Reply before reading its 18-byte success tail:
source errors have no tail. Only a valid chunk size, remote ring width and CROSS
width, followed by a final stop/deadline check, permit chunk publication. Before
that boundary no local PRE, Delegate or CROSS can start. Failure drops the
connection; it cannot append Unpin to an incomplete frame. Source EOF cleanup
owns any remote pin already allocated. Requester terminal is not a source drain
receipt, and WRITE still has no user-data effect before Delegate.

The same task stop reaches native RDMA setup socket waits and the requester
[finish event loop](#native-rdma-finish-progress). An
interrupted handshake still aborts its constructed grant and closes every
accepted CROSS; CUDA siblings and private registrations keep their existing
drain order. A native drain error may supersede cancellation, and `Unquiesced`
retains actual owners and admission. Native provider calls and synchronous
setup/source CQ waits are not preempted by this socket token. Each shard still
runs one session at a time.
Public active `Batch::cancel` is unchanged, and whole-engine close still drains
accepted batches before stopping the staging pool.

Frozen TENT's `StageBufferCache::allocateRemote` calls synchronous
`ControlClient::pinStageBuffer` before using that remote buffer in its event loop.
OrbitTE follows this admission boundary and TENT's separation between shared
ProxyManager workers and transport-owned native batches. The single cancellable
socket is OrbitTE's control implementation, not a claim of identical RPC or wire
behavior. See [source qualification](qualification.md#connection-and-pin-lifetime).

### Native RDMA finish progress

The staging session keeps one framing owner: outer FinishCross request, native
completion exchange, then the scoped FinishCross receipt. `RdmaTransfer` remains
in its original inflight slot throughout. Its finish cursor owns one slice and
one bounded partial frame; no other exchange enters that socket while pending.
Other submitted CROSS payloads are still queried every sweep, so their errors
can close the session while the peer withholds an ACK. A completed chunk's
POST/next PRE takes priority over starting another ready chunk's finish.

`CqLane::submit_completion` validates the exact claim, completed payload and key,
submits the completion SEND once and wakes the shared Workers pool. Its separate
query requires SEND work, not the prior payload completion. Reusable Direct
grants then enter the same sealed `GrantDrain` validation as synchronous callers.
The drain query requires the current claim and Drained phase, with no remaining
finish operation, work or native grant. Native cleanup still belongs to CqLane.

Only then does the requester send bound DONE and consume the full Reply, bound
ACK and, for reusable grants, bound READY. Partial reads/writes retain position;
malformed bindings fail without replay. A connection becomes cacheable only
after READY. The outer scoped receipt still gates slot reuse and local POST or
remote WRITE Delegate. An interrupted finish closes every accepted native owner;
failed retirement retains actual memory and admission. A native cleanup error
may supersede worker cancellation; neither proves remote EOF cleanup occurred.

This follows frozen TENT `e2d6e8e`'s `ProxyManager::transferEventLoop`: `INFLIGHT`
queries `progressBatch`, leaves pending chunks queued, and advances other owners.
TENT's transport Workers own native CQ progress. OrbitTE keeps that resource
split; its grant-specific completion SEND and bilateral release protocol are
OrbitTE contracts, not TENT wire compatibility.

This boundary covers reusable host Direct grants. Nonreusable QPs use the
[retirement progress](#nonreusable-qp-retirement-progress) below. Copyback, cache
fallback retirement, failure drain, Pin and source handlers retain synchronous
boundaries. Requester setup sockets are advanced by
the [subsequent setup boundary](#native-rdma-setup-progress). Native provider calls
and locks are not preemptible. One shard still executes one session at a time.
No public active-cancel or engine-close ordering changes are introduced.
[Source qualification](qualification.md#native-rdma-finish-progress) is separate
from installed v21 execution, real DMA overlap and performance.

### Nonreusable QP retirement progress

After its completion SEND, a nonreusable requester slice enters
`FinishProgress::Retire`. `RdmaGrant::poll_close` retains the same record, buffer,
grant credit and native connection while `CqLane::poll_close_record` requests
retirement. It does not send DONE, copy back READ bytes or consume the outer
FinishCross receipt until the native owner has drained.

Retirement tries the record, store and affiliated-record locks without waiting.
A busy sibling yields before detachment; all affiliated close actions publish
under the same store critical section. Stale claims cannot close a successor.
An unattached shared claim can leave without fencing its live physical sibling.
Repeated polls neither reserve another QP nor repeat native cleanup. Payload,
SEND and reusable-grant queries also yield when cleanup holds their record lock.

The existing CQ worker closes the QP, grant MR/MW and receive resources in their
established order. Closed alone permits output ownership to move to the grant
handle. A Retained record is not a complete receipt: the observer waits for the
lane's finished state and worker detachment before returning its native failure.
Cleanup deadline expiry reports `Unquiesced`, preserving unresolved owners.
Synchronous callers drive the same state with bounded polling waits.

A requester can now observe another CROSS error, worker stop or its original
deadline while the provider is still closing a nonreusable QP. Aggregate failure
cleanup still waits for native drain before returning a safe terminal. CPU
controls explicitly hold `destroy_qp` while observing entry to failure cleanup;
they do not assert early terminal, copyback, POST, Unpin or buffer release.
The held context and other CROSS use verified distinct worker IDs. A provider
call still blocks its own worker, including other lanes assigned to that worker.
There is no per-QP thread, provider preemption or wire change (v21).

Frozen TENT `e2d6e8e` provides the corresponding split: `EndpointStore` unpublishes
an endpoint and queues it for reclamation; `Workers::reclaimEndpoints` visits
context stores from its monitor loop; `RdmaEndPoint::deconstruct` owns synchronous
native QP/resource destruction. OrbitTE preserves endpoint/QP, context/CQ and
Workers ownership. Its explicit grant drainage/retention receipts and requester
polling adapt that split to Rust lifetimes and OrbitTE's bilateral protocol.
See [source qualification](qualification.md#nonreusable-qp-retirement-progress).

### Persistent MR publication progress

After connection acquisition, `RequesterSetup::Construct` owns one
`GrantConstruction`: the admitted range loan, grant/device and worker credits,
the fresh array slot or reused connection claim, and any physical connection
reservation. `LocalBufferManager::poll_acquire` returns pending when the same
registration/context/capability entry is still being created. Repeated polls
neither borrow its unpublished handle nor reserve another MR, QP or grant.
The staging loop can query other accepted CROSS payloads and observe its
original deadline or worker stop before publication. No HELLO or payload from
the waiting grant is sent at this point.

Publication produces one `MrLease`. Only then does construction attach the
complete grant to its CQ record and open the QP. A fresh empty record closed by
a lane fault cannot reopen; a provider call returning after the deadline cannot
start new QP construction. Abort explicitly closes an owned reuse claim and
releases an acquired MR borrow, with uncertain native cleanup taking precedence
over cancellation or timeout. An unused fresh reservation returns without
cancelling another MR creator. A shared unattached claim cannot close its live
sibling's QP. Waiting for a private staging MR does not prove that private
backing has drained: an external creator still owning it forces retention of
the pinned chunks and parent admission, even after the waiter's credit returns.

The synchronous source helper drives the same construction owner. Direct cache
publication keeps its condition-variable predicate under the cache mutex.
There is no per-request thread or new wire frame. Native registration runs on
its actual creator, outside the cache lock; it is not moved onto CQ workers.
Provider calls and their context locks, CQ/QP creation, eviction, connect/fill,
failure drain and source handlers remain synchronous. Nonreusable requester
retirement uses the [pollable retirement state](#nonreusable-qp-retirement-progress).

Frozen TENT's `LocalBufferManager` indexes registered buffers by range and
`RdmaContext`; `RdmaContext::registerMemReg` performs native registration.
OrbitTE preserves these owners with an additional exact registration identity
and capability key. TENT's synchronous registration is the reference boundary;
its optional parallel per-context registration still joins before publication.
This requester wait does not claim asynchronous provider execution.
See [source qualification](qualification.md#persistent-mr-publication-progress).

### Cold RDMA connection acquisition

Before a requester can send HELLO, `RequesterSetup::Acquire` owns exactly one
`GrantLease`, including its parent grant credit and selected-device byte charge.
`Connecting` retains the same `RdmaEndpoint` array and CQ lanes across polls. It
tries idle claims, fixed-width construction reservations and published-pair
sharing in the existing order. If another builder occupies every unpublished
slot, it returns pending without sending HELLO or creating another QP.
The staging loop can then query other accepted CROSS payloads and check its
original deadline and worker stop flag. Batch cancellation semantics are unchanged.

Publication makes the pair shareable; an unbound builder rollback makes only its
slot available again. A pending waiter cannot release another builder's slot or
recreate the canonical array. Its own lease returns on abort, even if another
CROSS must retain a native QP, MR, host chunk and parent admission. A constructed
grant still follows the existing abort/drain path. All parent grants must validate
before that parent's first payload; the outer CROSS framing owner stays exclusive.

Ordinary synchronous requester transfers drive the same `RequesterSetup` states.
Passive/source connection acquisition drives the same `Connecting` selection
logic with the existing endpoint condition-variable wait. No per-attempt thread,
new wire frame, transport fallback or renewed deadline is introduced.

Frozen TENT's `Workers::getEndpoint` / `RdmaTransport::getEndpoint`, context-owned
`EndpointStore` and `RdmaEndPoint::connect` provide the corresponding resource
boundaries. TENT performs a synchronous bootstrap outside its endpoint lock,
then revalidates the peer and local QPs. This change makes OrbitTE's requester
construction-slot wait pollable; it does not claim TENT bootstrap is asynchronous.
Native context/CQ/QP/MR creation, connect/fill and eviction/failure drain remain
synchronous and are not preempted by this state. Requester retirement uses the
[pollable close state](#nonreusable-qp-retirement-progress).
See [source qualification](qualification.md#cold-rdma-connection-acquisition).

### Native RDMA setup progress

`RdmaSetup` owns a parent's device selections, grant credits, planned slices,
actual registration and partial requester grant. `RequesterSetup` owns each
slice's HELLO/OFFER/ACCEPT/READY/GRANT exchange. The outer CROSS header, embedded
native exchanges and scoped CROSS receipt remain one exclusive framing operation.
Every socket step attempts bounded I/O and retains its byte position. A pending
setup yields to deadline/worker-stop checks and queries of previously submitted
CROSS payloads; FinishCross, Delegate and another setup cannot interleave frames.
The original session deadline is never restarted.

Path tables and parent RQ admission are validated before per-slice construction.
OFFER binds source epoch, PD domain, connection generation, path and release
profile. GRANT must match the exact request binding and offered connection.
All slices must pass these checks before the first payload WR is submitted.
Ordinary synchronous RDMA callers drive the same parent and slice states.

Only an explicit zero-length CACHE_MISS for a cached pair permits another
handshake. The old grant must close successfully first. Retry keeps the selected
path, original deadline and negotiated width-plus-one bound, and happens before
payload submission. No partially executed payload is replayed. A WRITE to remote
staging retains the existing exported-GPU effect boundary at Delegate.

Failure first explicitly aborts the partial handshake, then closes all prepared
and submitted slices before private registration retirement. Cleanup uncertainty
overrides ordinary errors and retains the actual native/backing owners and their
admission. Source EOF cleanup is a separate obligation; requester terminal does
not prove that the peer has observed EOF. Completed chunks retain POST/next-PRE
priority over admitting another ready FinishCross.

This follows frozen TENT `e2d6e8e`'s ProxyManager/transport/Workers ownership split
and submitted-batch observation. TENT's Workers can themselves perform endpoint
bootstrap outside the endpoint lock; this change does not imply that TENT has
universally asynchronous connection setup. OrbitTE retains its own v21 framing
and exact-grant completion protocol. Passive/source cold-builder waits in
`connection()`, native resource creation/open/connect/fill, failure drain and
source handlers remain synchronous; locks and provider calls are not preemptible.
Each shard still runs one session. [Source qualification](qualification.md#native-rdma-setup-progress)
does not qualify installed v21 execution, real DMA overlap or performance.

### Session progress and completion

The requester reserves its physical ring and GPU range loan before submitting
a session to the shared pool. Existing parent admission remains charged through
queueing and completion. At most eight `orbitte-staging-{id}` threads exist per
`ProxyManager`, with at most 64 waiting sessions per shard. Full queues return
`ResourceExhausted`; no nested public batch is admitted. Queue time consumes the
original request deadline. A cancelled/expired queued session is removed and its
actual captures are destroyed before terminal publication, without Pin, payload
I/O or waiting for an unrelated active native call. An active session negotiates
Pin on its shard, then publishes the negotiated chunk size to the caller.
Command and CROSS completion queues each hold at most `L` entries. The event
loop retains one chunk state per slot and references at most `L` registrations. The shard owns the control connection and at most `W` active RDMA transfers;
local CUDA owners remain in the caller. TCP still transports TCP payload; RDMA uses its existing shared
verbs workers/CQ lanes and uses TCP only for control.

The caller sweeps `Pre`, `Cross`, `Post` and their in-flight states without
blocking on an individual CUDA event or CROSS completion. A WRITE's PRE must
complete before its ordered CROSS; a READ's POST must complete before its slot
can receive another CROSS. Other slots continue to progress. CROSS results are
consumed individually and start the corresponding local POST or next PRE.
The shard admits CROSS offsets in order, but can finalize any payload-ready
owner. If the next offset maps to an occupied remote slot, it defers that command
and continues querying active owners. Each result must match the local slot's
current absolute offset and phase; a previous slot generation cannot release it.
A CROSS result is published only after the exact payload WRs finish, actual
transport grant cleanup and the scoped FinishCross receipt; the worker drops
its local registration clone first. Source MW Bind completion is never queried
as payload completion. It sends the required Delegate
before publishing CROSS completion, records WRITE uncertainty before that send,
then waits for the remote receipt independently of local CUDA progress. CROSS
ready releases the local slot, not the remote operation or the whole request.

`cuEventQuery` returning `CUDA_ERROR_NOT_READY` retains the exact pending length,
event generation, GPU range loan, pinned lease and private MR. Only query success
followed by successful context restoration clears that pending copy and advances
completion counters. A query error or failed context restoration quarantines the
actual slot and makes range failure sticky. Error cleanup still waits for every
other accepted copy; success on a sibling cannot clear uncertainty. Ordinary
healthy requester progress performs no event synchronization. The CUDA Driver
API [event contract](https://docs.nvidia.com/cuda/cuda-driver-api/cuda_driver_api/group__CUDA__EVENT.html)
defines the query result; event destruction is not completion evidence.

After consuming all CROSS results, the caller drains every accepted local copy
and queues Unpin. The worker defers Unpin until every preceding Delegate
acceptance and terminal QueryDelegate result succeed. A missing final result therefore fails the request
even when every CROSS result was received. On failure, the caller stops queue
admission and cancels commands that have not started. Every accepted RDMA owner
is closed before private registrations retire; one failed cleanup cannot skip
healthy siblings or erase an earlier `Unquiesced`. Partial TCP payload and scoped
control receipts retain the original deadline and yield to staging cancellation.
Blocking Pin, native construction, aggregate failure drain and provider calls keep their existing
completion/drain obligations.
On source EOF/error, all native CROSS owners close before CUDA drain and private
registration retirement. The caller drops its queue
handles and waits for a separate task terminal receipt before private MR
retirement. That receipt is sent only after the task closure, command receiver,
registration clones and connection have been destroyed. It cannot be blocked by
a full CROSS completion queue. Caller unwind drops both chunk channel handles
before its task guard cancels and waits. Task panic becomes `Unquiesced` while
the shard remains available; uncertain native errors take precedence over an
ordinary local failure and keep actual owners and admission charged.

`EngineStats.staging.worker_shards` counts live threads, including idle ones;
`opened_worker_shards` records total starts. `active_tasks` counts executing
sessions and `queued_tasks` counts waiting sessions. The active count can reach
zero before terminal delivery; it is not a drain receipt. `cancelled_tasks` counts
local scheduling exits that stop queued work. Counters are independently sampled.
`close_requester` fences admission and waits for accepted calls, leaving idle
shards available to the engine owner. `close_source` does not stop outgoing
shards. Successful whole-engine `close` joins all shards before releasing pooled
buffers. If requester/source drain times out, the pool remains available for
already accepted work; retry observes the same owners. Native uncertainty can
keep that engine and its idle shards retained. At the pool shutdown boundary,
admission and lazy creation are fenced together, queued tasks are cancelled and
active tasks retain their original drain obligation. A pool close timeout can be
waited again without restarting threads. Idle workers hold only their shard
state, not a strong engine reference. Timeout never substitutes for CUDA/DMA drain.

EOF, a protocol error or deadline expiry stops new work but drains every accepted
copy once, including siblings of a failed copy. `PinnedStage` takes its pending
length exactly once; an absent pending copy performs no native wait. The range
owner tracks aggregate accepted copies, while each physical slot owns its
stream/event and refuses retirement while a copy is still pending. Ordinary pending work does not fence the entire allocation.
Native uncertainty is published as a sticky allocation flag after private
host/lease/stream locks are released. A successful sibling never clears it.
An uncertain CUDA copy retains the actual GPU owner and range loan. Private MR
retirement failure retains the host stage and responsible admission even when
CUDA has drained and its GPU range loan can be released. A recovered submission error can release resources after
all other slots drain. Unproven completion retains the actual native buffers,
registrations and responsible admission. No extra handler or nested batch slot
is required.

This source implementation has deterministic CPU controls for multiple pending
copies during network progress, depth negotiation, short tails, stream reuse,
partial admission, wrong slot/scope, EOF, lost final receipts and failed
completion/construction/destruction. CROSS setup, FinishCross and PRE drain
errors use the nested transport's source epoch; Delegate and Unpin use the
staging nonce.
An error after a partial native wire exchange may still require closing the
connection with an unknown remote effect. A requester still writing CROSS may
observe a socket error before it can read the source's error receipt. Long RDMA sessions rotate exhausted
MW key indices through the [PD-owned key domain](rdma-cuda.md).
The earlier v18 pipeline profile passes 24 two-host cases on stable/MSRV
plus four independent repeats. That exact source and healthy profile are accepted;
see [qualification](qualification.md#bounded-remote-cuda-pipeline).
Scheduling overlap and native completion receipts do not establish GPU execution
overlap or a performance improvement.

## Validation

CPU native-call shims cover TCP/RDMA bytes and guards, chunk tails, context reuse,
capacity, cancellation, deadline prefixes, close races and allocation/copy/free
failures. Combined CUDA and ordinary CQ failures must preserve `Unquiesced` so
source/requester admission cannot be returned early.

The installed gate extends the existing `hardware_peer` consumer. Build with
`python3 tests/hardware/build_installed.py --consumer gpu --toolchain stable
--output /external/installed`. A fresh two-host profile specifies TCP or staged
RDMA, CPU/GPU memory kinds, and `staging_chunk_size` smaller than each logical
request with a nonzero tail. Run `tests/hardware/run.py` with `--installation`,
`--binary`, `--staging-observer` (the manifest's `cuda_observer`), `--profile` and
an external `--output`. The packaged passive CUDA observer records actual pins,
chunk copies, event waits, context pops and frees; it never fabricates CUDA
completion. Both peers compare full buffers and guards, close all owners, and
run under inventoried GPU/RNIC locks and process supervision.

The installed stable/MSRV profile passes 24 two-host H20 cases and four fresh
independent repeats, with native chunk allocation, copy and free receipts.
Source and this bounded healthy profile are independently accepted. See
[Qualification](qualification.md#local-cuda-staging-chunks) for the exact
artifacts and limits.
CPU failure controls do not qualify physical GPU faults or remote proxy staging.

The serial remote path has CPU controls through the public engine for TCP and
RDMA, both memory directions, private MR cleanup failure, CUDA uncertainty,
source authorization, wrong scope/order, abandoning a staged WRITE, lost final
receipts, self-peer and opposite-direction sessions. These native-call shims do
not qualify real GPU/RNIC failures. The installed consumer accepts
`remote_staging: true` in the existing staging profile, requiring a CUDA source,
one RDMA slice per chunk and the same passive observer. It additionally checks
real remote-operation counters and private MR retirement. The consumer waits for
`SubmissionStatus.locally_drained` before its next incremental submit reuses the
one-slot parent admission; task byte completion alone does not release that slot.
The CUDA ledger checks all asynchronous copies, including setup/readback, against
actual live allocations and completion before cross-thread reuse/free.

Stable/MSRV installed consumers pass 16 two-host H20 cases for this healthy serial
profile, with four fresh independent repeats. Source and this bounded profile
are independently accepted. The profile and artifact binding are in
[Qualification](qualification.md#serial-remote-cuda-staging). The pipeline has
separate evidence below; physical faults and full ProxyManager parity remain open.

### Installed pipeline validation

Set `staging_pipeline_depth` in the hardware profile, or use
`source_pipeline_depth` / `requester_pipeline_depth` for asymmetric depths.
`source_chunk_size` / `requester_chunk_size` override the shared
`staging_chunk_size` for physical slots; native copy/grant checks use the
negotiated minimum while pinned-byte accounting uses each physical size.
The consumer reports its actual configuration; the harness requires the
independent widths `R` and `L` to match the physical pool slots and persistent
stream/event owners on each side. Source private MR counts scale by `R`;
requester private MR counts scale by `L` (one for CPU requesters). Six sessions
therefore create `6 * R` and `6 * L` private MRs, respectively. Physical pinned
bytes use each side's own chunk size and width. Byte and chunk counts do not
scale with width.

The staging-only observer adds context push and stream/event create/destroy
receipts plus paired entry/return records for copy, query, wait and cleanup calls.
`staging_native.py` joins each event record generation to its actual stream,
thread/context and live GPU/pinned allocations. Completion requires both the
matching successful query/wait return and context restoration. NOT_READY keeps
the same live generation; stale or repeated terminal receipts fail the gate.
Distinct slots may submit and finish in different orders; reusing a slot or entering cleanup on any
thread before its completion fails the gate. Pool streams/events must be
destroyed before their pinned allocation is freed. The observer forwards all
native arguments, outputs and return codes without inserting driver waits.

The frozen bounded profile covers TCP/RDMA, CPU/GPU requesters, both host
orientations, asymmetric caps, widths 1/2/3/4/16 and shortening a requested width
to the chunk count. Main pipeline cases turn the ring more than three times and
include a tail. All six sessions reuse physical slots; setup/readback copies
are also checked. Native slot copies are matched by exact address/length
multiplicity within each payload phase; setup/update/readback ordering and
per-slot generations remain strict. Tests distinguish unconfirmed submissions from physical DMA
execution: native receipts do not establish execution overlap or throughput.
The historical 24-case installed matrix uses wire v18 and the generation-aware
observer. Four fresh independent repeats and their cleanup audit also pass;
earlier serial evidence remains bound to its original observer and wire version.
The current v21 requester ring, concurrent RDMA CROSS and remote Delegate queries require their own artifact and evidence. The
controller supports independent widths and conservative local capacity, but
hardware execution is deferred while new resource requests are paused. The
shared worker shards and requester query event loop also require fresh installed
evidence; the CPU controls prove ownership and scheduling only. The shared-QP
control holds the first native payload completion while a second CROSS finishes,
with exact bytes, guards and retained grant/RQ/range owners. Native fault injection
selects an actual private MR and exact WR identity, independently of posting
batch sizes. A serialized CROSS control fails that progress condition. Expiring
the first CROSS after a later chunk finishes preserves exact partial effects;
unproven cleanup retains the real owners. Remote query controls also hold one
source PRE/POST while another submitted CROSS completes; query/context errors
compose with native cleanup failure and preserve actual owners. Socket controls
hold a real partial reply/payload through local failure and task join, test real
TCP write backpressure and preserve READ guards and uncertain GPU owners.
Host staging location selection is implemented with the limits above.
Remaining blocking control boundaries and current-profile qualification stay in P5.5b.
