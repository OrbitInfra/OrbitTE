# CUDA staging

`runtime/proxy_manager.rs::ProxyManager` owns an engine-wide pool of reusable
CUDA-pinned host chunks. TCP and staged RDMA share it for device-to-host and
host-to-device copies. Each payload transport still owns its network buffers,
MRs, grants and completion receipts.

## Configuration and lifetime

With the `cuda` feature, `EngineConfig.staging_chunk_size` defaults to 4 MiB and
`staging_chunk_count` to 64. Size must be at most 64 MiB, count at most 4096, and
their physical product (including NUMA page padding) at most 1 GiB. Chunks are
allocated lazily. A local GPU copy checks out one chunk and walks its whole range,
holding the `CudaMemory` allocation lock across chunks. Public
`CudaMemory::read/write` keep their one-shot staging.

The pool bounds pinned copy bytes across source and requester operations and
all CUDA contexts. Active, constructing, closing and retained chunks all count.
Exhaustion returns `ResourceExhausted`; it never creates an uncharged allocation
or waits for a nested request. Idle chunks keep their native context and host
location; a chunk moves to another context or location only after its native
release and context restore succeed. Native calls never run under the pool lock.

A chunk returns to the pool only after event completion (or recovery stream
drain) and caller-context restoration. Failed completion, partial allocation and
failed cleanup keep the actual chunk, context, GPU owner and admission charged,
and a repeated close never repeats a native free.

Deadlines are checked at drained chunk boundaries; they cannot preempt a native
CUDA call. A partially copied request fails with zero completed bytes, may have
changed destination bytes, and is never replayed automatically. Queued requests
can be cancelled; a running copy continues through drain.

`close_source` and `close_requester` leave the pool to the other direction. Full
`TransferEngine::close` drains both, then frees idle chunks; a timeout permits
another wait and uncertain cleanup stays retained. Pool close fences new pins
before waiting for a requester shard, including when that wait times out. `EngineStats.staging` reports
chunk states and allocation, reuse and byte counters. Byte counters track local
copy progress, not batch completion.

## Host staging location

`EngineConfig.staging_location` places engine-owned staging chunks for local
copies and both roles of a remote session. Each endpoint uses its own GPU
context; a peer's CUDA ordinal is never used locally. CPU scratch buffers,
`allocate_pinned_host` and standalone `CudaMemory::read/write` are unchanged.

| Setting | Behavior |
| --- | --- |
| `StagingLocation::Any` (default) | Plain `cuMemHostAlloc`; no NUMA placement claim |
| `StagingLocation::Auto` | Prefer the NUMA node of the context's GPU (PCI BDF → sysfs), otherwise the lowest eligible node |
| `StagingLocation::Cpu(N)` | Require `cpu:N`; an unavailable node fails before allocation |

Eligible nodes have online memory and are in the calling thread's allowed set,
which is re-read on every selection. GPU locality is cached per context; an
unknown locality never means node zero. Discovery errors, an empty eligible set
or a failed binding are errors, not a silent fallback to `Any`. TENT spells its
wildcard `*` but resolves it to node zero, so the two defaults are not claimed
equivalent.

NUMA backing is a fresh page-rounded anonymous mapping bound with
`mbind(MPOL_BIND | MPOL_F_STATIC_NODES)` before first touch, then registered with
`cuMemHostRegister`. Cleanup retires private MRs, destroys copy streams/events,
unregisters, restores the context and only then unmaps; any uncertain step keeps
the mapping and its slot charged. Owners: `platform/numa.rs` (binding),
`cuda/pinned.rs` (registration), `cuda/probe.rs` (locality), `cuda/staging.rs`
(selection) and `runtime/proxy_manager.rs` (reuse and budgets), matching TENT's
`findStagingPolicy` → `Topology::findNearMem` → per-location `StageBuffers`.

CPU fixtures use real mapped bytes with simulated CUDA and NUMA binding. Page
residency, real registration, DMA and performance are unqualified; see
[qualification](qualification.md#host-staging-location).

## Serial remote staging

Remote staging lets a requester read or write a peer's CUDA memory through the
peer's pinned chunks (Pin/Delegate/Unpin), with TCP or RDMA carrying the bytes.

| Setting | Responsibility |
| --- | --- |
| `EngineConfig.remote_staging` (source) | Allow Pin against actual CUDA ranges; defaults to false |
| `request.with_remote_staging(true)` (requester) | Use remote staging for this TCP/RDMA request; otherwise the ordinary path is used |

The requester need not accept Pin itself, and one engine can enable both CUDA
Direct and incoming Pin. Opting out does not force GPUDirect: ordinary RDMA still
chooses Direct or local staging. The source reads the complete fixed-size Pin
hello before replying to a rejection. It checks epoch, registration, range,
permission and the actual allocation type, so a remote host range or opted-out
source is rejected there. Remote staging over NVLink or without `cuda` fails
during whole-submission preparation. One ordered batch can mix GPU Direct RDMA,
remote staging and host transfers; private staging chunks are host memory and
follow `RdmaHostMemory` policy.

| Phase | READ | WRITE |
| --- | --- | --- |
| Local preparation | Reserve a private slot and borrow the full local GPU range | Same |
| Pin | Source validates scope, borrows its GPU range and pins its slots | Same |
| PRE | Source GPU → source slot | Local range → requester slot |
| CROSS | TCP/RDMA source slot → requester slot | Requester slot → source slot |
| POST | After FinishCross, requester slot → local range | Delegate: source slot → source GPU |
| Unpin | Drain copies, retire private registrations/MRs, release slots and range, then acknowledge | Same |

One control connection and one source admission own the session. Pin
negotiates `min(source, requester)` chunk size; each side still reserves its own
physical slot size, and the tail uses its exact length. `cuda::StagingCopy`
holds the actual allocation and an exclusive range loan, so non-overlapping
ranges progress concurrently and overlapping staging, CPU, RDMA or IPC access
fails before new work.

Every frame binds a fresh session nonce, registration, phase and range, and each
receipt echoes that scope; private chunk descriptors never appear in Describe.
These fence reuse; they are not authentication. A CROSS receipt confirms
acceptance only; the scoped `FinishCross` receipt follows actual grant cleanup.
A transport ACK never completes a remote GPU WRITE: a lost POST or Unpin reply
leaves `RemoteEffect::Unknown`, and a possibly executed WRITE is never retried.
A failed request reports zero completed bytes but may have changed bytes.
`EngineStats.staging.remote_*` counters count source-side operations and do not
replace requester receipts.

## Bounded remote pipeline

Set `EngineConfig.staging_pipeline_depth` to 2–16 on both peers to opt in (the
default 1 is serial; it cannot exceed `staging_chunk_count`). For an `N`-byte
request:

| Quantity | Bound |
| --- | --- |
| Logical chunk size `C` | `min(C_local, C_source)` |
| CUDA requester width `L`, reserved before Pin | `min(D_local, ceil(N / C_local))` |
| Source width `R`, negotiated by Pin | `min(D_local, D_source, ceil(N / C))` |
| CPU requester width | One host buffer, no CUDA pool charge |
| Concurrent CROSS window `W` | RDMA: both peers' native limits and `R`, up to 16; TCP: one |

Both sides reserve their entire ring before Pin is acknowledged and never
narrow it silently; insufficient capacity returns `ResourceExhausted`. RDMA
divides each peer's per-CROSS slice cap by `W`, and every slice still consumes
grant, receive and byte credits. Chunk `i` uses local slot `i % L` and source
slot `i % R`; each slot owns its pinned allocation, a persistent stream/event and
a per-session private registration.

| Direction | Scheduling |
| --- | --- |
| READ | Delegate prefills `R` source PREs. A CROSS needs its source PRE confirmed by QueryDelegate and the previous POST in its local slot done. FinishCross starts the local POST and frees the source slot for PRE `i + R` |
| WRITE | Prefill `L` local PREs. A CROSS needs its local PRE and confirmation of the previous source POST. After FinishCross and the Delegate send, the local slot takes PRE `i + L` |
| Final Unpin | Drain all local POSTs and confirm every Delegate first. Only the matching receipt and local retirement complete the request |

A Delegate receipt confirms acceptance, not CUDA completion. WRITE becomes
`RemoteEffect::Unknown` before Delegate is sent.

### Session progress and completion

The requester reserves its ring and GPU range loan, then queues the session on a
shared worker pool: at most eight `orbite-staging-{id}` threads per
`ProxyManager`, each running one session at a time with up to 64 queued sessions.
Full queues return `ResourceExhausted`. Queue time consumes the request deadline,
and a cancelled queued session is destroyed before Pin.

The caller drives PRE, CROSS and POST per slot without blocking on any single
CUDA event or CROSS. CROSS offsets are admitted in order but can finish out of
order; each result must match its slot's current offset and phase.
`cuEventQuery` NOT_READY keeps the exact pending copy, event generation, range
loan, lease and MR. Only query success plus context restoration advances a slot;
an error quarantines it and makes the range failure sticky.

On failure the caller stops admission and cancels unstarted commands. Every
accepted RDMA owner closes and every accepted CUDA copy drains before private
registrations retire; one failed cleanup never skips healthy siblings. A task
panic becomes `Unquiesced` and keeps actual owners and admission charged.
`EngineStats.staging` adds shard, active, queued and cancelled task counts; they
are samples, not drain receipts. Whole-engine `close` joins the shards before
releasing pooled buffers.

### Remote Delegate progress

The source keeps each accepted Delegate in an inflight state with its offset,
generation, pinned bytes, registration and range loan. `QueryDelegate` is legal
only there and replies pending or complete for exactly that scope; early,
duplicate or mismatched queries fail before another CUDA call. The requester
holds at most one pending Delegate per remote slot, queries it once per sweep and
defers reuse of that slot while other CROSS owners progress. TENT uses
asynchronous RPC callbacks (`RemoteStageOperation::tryTakeResult`); OrbiTE uses
scoped query exchanges on its single session connection.

TENT runs Delegate on the RPC blocking executor so unrelated control requests
can finish. With incoming remote staging enabled, OrbiTE gives each admitted
Pin connection one source session worker through Unpin or disconnect.
The control worker reads the full Pin hello, checks the epoch, range and
permissions, and hands over the original socket, deadline, registration and
source admission. Describe and credit messages use the ordinary control workers.
The requester shards are independent, including for a self-peer transfer.

The source has `min(handler_threads, staging_chunk_count)` session slots.
A full set rejects a further Pin with `ResourceExhausted` before allocating
staging memory. The session worker performs native allocation, Delegate and
failure drain synchronously. It returns its slot before replying to a clean
Unpin or error. An unconfirmed drain keeps the slot, source admission and
backing charged. Engine destruction stops the sockets and joins both sets of
workers. The [installed control gate](qualification.md#verification-queue)
exercises this boundary on a GPU.

### Socket progress

`TcpTask` owns a TCP request frame, partial reply, bounded payload snapshot,
registration and WRITE effect. Each turn performs at most one 64 KiB socket
operation and keeps its position on WouldBlock. READ commits only after all
bytes arrive; WRITE becomes Unknown before its first send and needs the final
ACK. `TcpCross` sequences the CROSS header, inner `TcpTask` and scoped receipt;
`ControlOperation` owns Delegate, QueryDelegate, FinishCross and Unpin framing.
Pin carries a sender-session binding for
[owner credits](async-batches.md#staging-and-consumer-owner-credits). The single
socket never interleaves two exchanges, and a local failure closes the session
instead of appending Unpin to a partial frame. TENT's ordinary TCP workers are
synchronous; this progress is an OrbiTE choice, not wire compatibility.

### Connection and Pin lifetime

`Connecting` issues one nonblocking connect and keeps the socket and original
deadline while pending; there is no retry or second connection. The connected
channel carries the task stop through every later read and write, separately
from source shutdown. Pin stays on the shard worker with 25 ms socket timeouts
and must deliver a complete, valid reply (chunk size, ring width, CROSS width)
before any PRE, Delegate or CROSS starts. TENT's `StageBufferCache::allocateRemote`
also pins synchronously before its event loop. See
[qualification](qualification.md#connection-and-pin-lifetime).

### Native RDMA setup progress

`RdmaSetup` owns a parent's device selections, grant credits, slices and
registration; `RequesterSetup` owns each slice's HELLO/OFFER/ACCEPT/READY/GRANT
exchange inside the exclusive CROSS frame. Each step does bounded I/O and keeps
its position, so a pending setup yields to deadline and stop checks and to
queries of other CROSS payloads. Every slice's grant must validate before the
first payload WR. Only an explicit CACHE_MISS on a cached pair permits one more
handshake, after the old grant closes. Failure aborts the partial handshake and
closes all slices before registration retirement. Native resource creation,
connect, failure drain and source handlers stay synchronous. See
[qualification](qualification.md#native-rdma-setup-progress).

### Native RDMA finish progress

After a payload completes, `CqLane::submit_completion` sends the completion SEND
once; reusable Direct grants then pass the same sealed drain check as synchronous
callers. Only then does the requester send DONE and read the reply, ACK and, for
reusable grants, READY, keeping partial frames across polls. Other CROSS payloads
are still queried meanwhile, and a completed chunk's POST or next PRE goes first.
This follows TENT's `transferEventLoop`, which leaves pending batches queued
while other owners advance; the grant-specific completion protocol is
OrbiTE's own. See [qualification](qualification.md#native-rdma-finish-progress).

### Cold RDMA connection acquisition

Before HELLO, `RequesterSetup::Acquire` owns one grant lease and its device byte
charge. If another builder holds every unpublished QP slot, it returns pending
without creating a QP, so the session can query other CROSS payloads and check
its deadline. Publication makes the pair shareable; a waiter can never release
another builder's slot. Synchronous callers drive the same states. TENT's
bootstrap is synchronous; only OrbiTE's slot wait is pollable. See
[qualification](qualification.md#cold-rdma-connection-acquisition).

### Persistent MR publication progress

`GrantConstruction` holds the admitted range loan, credits and connection claim
while `LocalBufferManager::poll_acquire` waits for another creator to publish the
same MR. Repeated polls reserve nothing new and send nothing. Publication yields
one `MrLease`, after which the grant attaches to its CQ record. Abort releases an
owned claim and MR borrow, with uncertain cleanup taking precedence. Native
registration still runs synchronously on its creator, as in TENT's
`RdmaContext::registerMemReg`. See
[qualification](qualification.md#persistent-mr-publication-progress).

### Nonreusable QP retirement progress

A nonreusable requester slice enters `FinishProgress::Retire` after its
completion SEND. `RdmaGrant::poll_close` keeps the record, buffer, credit and
connection while the existing CQ worker closes the QP, MR/MW and receive
resources in order; busy locks yield instead of blocking. DONE, READ copyback and
the FinishCross receipt wait for that drain, and a retained record reports its
native failure only after the lane finishes. TENT's
`EndpointStore` / `Workers::reclaimEndpoints` provide the same split. See
[qualification](qualification.md#nonreusable-qp-retirement-progress).

## TENT correspondence

| OrbiTE owner | Frozen TENT owner |
| --- | --- |
| `ProxyManager` chunk pool | `allocateStageBuffers`, `pinStageBuffer`, `unpinStageBuffer`, `freeStageBuffers`. OrbiTE claims a slot under the pool lock and runs native calls outside it. Concurrent locations share the engine-wide physical budget. |
| `cuda/staging.rs`, `cuda/stream.rs` | Local staged copy and stream completion |
| `control_plane/source_sessions.rs`, `source_task.rs` | `ControlService::onPinStageBuffer` / `onUnpinStageBuffer` run inline in TENT, while `onDelegate` is offloaded. OrbiTE moves the scoped connection to a bounded source worker so blocking source work leaves control RPCs serviceable. |
| `remote_stage_operation.rs` and the source slot drain | `RemoteStageOperation::tryTakeResult`, `abandonForCleanup` and `flushPendingCleanups`. Disconnect ends admission for the session. Only a native completion or confirmed recovery drain can release its bytes. |
| `source_task.rs`, `requester_task.rs` | `transferEventLoop` rings of up to 16 stage buffers |
| `staging_task.rs::StagingTask`, `worker_shard.rs` | Eight shared worker shards |
| `stage_registration.rs::PinnedStage`, `StageBufferCache` | `StageBufferCache` private registrations |
| `rdma_transport/transfer.rs::RdmaTransfer` | Submitted internal batch and `progressBatch` |

Requester shards start lazily. Control socket exchanges, setup and finish,
connection acquisition, MR publication waits and QP retirement can yield.
Pin allocation, provider calls, copyback and failure drain stay synchronous on
their owning worker. Private stage registrations remain outside Describe.
TENT uses location pools and raw addresses for Pin/Unpin. OrbiTE checks the
session, range, generation and phase before a slot can be reused.

## Validation

CPU native-call shims cover bytes and guards, chunk tails, context reuse,
capacity, cancellation, deadline prefixes, close races and allocation, copy and
free failures. Combined CUDA and CQ failures must preserve `Unquiesced`. Remote
staging controls cover both directions over TCP and RDMA, wrong scope or order,
lost final receipts, self-peer and opposite-direction sessions, and retained
CUDA and MR failures. They do not qualify physical GPU or RNIC faults.

The installed gate extends the `hardware_peer` consumer:

```bash
python3 tests/hardware/build_installed.py --consumer gpu --toolchain stable --output /external/installed
```

Run `tests/hardware/run.py` with `--installation`, `--binary`,
`--staging-observer` (the manifest's `cuda_observer`), `--profile` and an
external `--output`. A profile selects TCP or staged RDMA, CPU/GPU memory and a
`staging_chunk_size` smaller than each request, with a tail. The passive CUDA
observer records actual pins, copies, event waits, context pops and frees; both
peers compare full buffers and guards under GPU/RNIC locks and process
supervision. Set `remote_staging: true` to run the serial remote path.

Accepted profiles: local chunks (24 H20 cases), serial remote staging (16 cases)
and the bounded pipeline (24 cases), each with four independent repeats; see
[local chunks](qualification.md#local-cuda-staging-chunks),
[serial remote](qualification.md#serial-remote-cuda-staging) and
[pipeline](qualification.md#bounded-remote-cuda-pipeline). The later requester
ring, concurrent RDMA CROSS, Delegate queries, shared shards and pollable setup
work have CPU controls only and need a fresh installed run.

### Installed pipeline validation

Set `staging_pipeline_depth`, or `source_pipeline_depth` /
`requester_pipeline_depth` for asymmetric depths, and optionally
`source_chunk_size` / `requester_chunk_size`. The harness requires widths `R` and
`L` to match each side's physical slots and persistent stream/event owners;
private MR counts scale by `R` (source) and `L` (requester), so six sessions
create `6 * R` and `6 * L`. The staging observer adds context push and
stream/event create/destroy receipts plus paired entry/return records.
`staging_native.py` joins every event generation to its stream, thread, context
and live allocations: completion needs a successful query or wait and context
restoration, NOT_READY keeps the same generation, and slot reuse or cleanup
before completion fails the gate. Streams and events must be destroyed before
their pinned allocation is freed. Native receipts do not establish execution
overlap or throughput.
