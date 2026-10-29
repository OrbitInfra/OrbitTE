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
at most 4096, and their product at most 1 GiB. Active, constructing, closing and
retained chunks count against the bound. Capacity exhaustion returns
`ResourceExhausted`; it does not create an uncharged allocation or wait for a
nested request. TCP payload buffers and RDMA staging/MR quotas remain separate.

Idle chunks reuse their retained native CUDA context. A different context may
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
stage buffers (`id % kStageBuffers`). `staging_task.rs` owns the source task and
its source `Chunk` scheduling; `requester_task.rs` owns bounded local PRE/POST
and CROSS scheduling. `stage_registration.rs::PinnedStage` owns the actual
pinned lease, its private registration and ordered retirement on either side.
`remote_stage_operation.rs` owns scoped messages and receipts. Per-slot CUDA completion supplies the
PRE/POST completion boundary, while TCP/RDMA still own CROSS.

The current implementation overlaps remote CUDA work with CROSS in one source
handler. TENT's full event loop also schedules the requester's local stage and
tracks asynchronous remote operations across chunks. OrbitTE now reuses one
bounded requester slot with synchronous local PRE/POST. Multiple asynchronous
requester slots, location selection and broader remote scheduling remain
in the existing P5/P6 queue. This is a scoped step toward that architecture.

## Serial remote staging

Set `EngineConfig.remote_staging = true` on both peers, built with `cuda`.
This explicitly selects remote GPU staging for TCP/RDMA submissions; the remote
registered range must be CUDA. Requester ranges may be CPU or CUDA. NVLink keeps
its own path. Remote host ranges, an opted-out source and a config combining
this flag with `RdmaCudaMemory::Direct` are rejected. No automatic fallback or
new payload transport is introduced. The flag defaults to false.

| Phase | READ | WRITE |
| --- | --- | --- |
| Local preparation | Reserve one bounded private slot and borrow the full local GPU range | Same; no full-request snapshot |
| Pin | Validate source epoch/region/range/permission; borrow its GPU range and pin its slots | Same |
| PRE | Source GPU → pinned source slot, with event/context completion before CROSS | Local range → requester slot, with event/context completion before CROSS |
| CROSS | TCP/RDMA source slot → requester slot | Requester slot → TCP/RDMA source slot |
| POST | After the scoped CROSS receipt, requester slot → local range | Delegate source slot → source GPU |
| Unpin | Drain accepted copies, retire private registrations/MRs and release source slots/range, then acknowledge | Same |
| Request completion | Require final Unpin receipt and requester slot/MR retirement | Same |

One control connection and one source admission own the entire session. Each
chunk completes before the next starts, including the short tail. Nested CROSS
calls the existing transport directly; it never recursively submits an ordinary
batch or waits for a second handler. One handler, one parent batch slot and one
pinned source chunk per engine are sufficient. A GPU requester also needs one
local pinned chunk; self-peer and opposite-direction sessions share their
engine pool and need source width plus one requester slot. Native RDMA grant/CQ/MR budgets still apply.
RDMA uses its slices protocol even for a single chunk, retaining its bounded
same-connection QP-cache-miss retries and actual verbs payload path.

The requester reuses one private host slot across all CROSS legs, always at
local offset zero. CUDA requesters reserve one physical pool slot and retain its
stream/event across sessions. CPU requesters allocate at most
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
18 added pipeline width. Current version **19** also carries the requester chunk
cap in Pin. Both peers must use version 19; older headers are rejected.
Each frame binds a fresh nonce, original registration, phase and logical range;
success receipts echo that full scope. A repeated prior receipt cannot stand in
for Unpin. Private chunk descriptors never appear in Describe and cannot be used
outside their session. These identities fence reuse; they are not authentication.

CROSS receipts follow actual transport grant cleanup. Unpin first unregisters
the private view, retires every native MR and verifies no private registration
borrower remains, then explicitly releases the chunk and GPU range loan. A transport
ACK alone never completes a remote GPU WRITE. Lost POST/Unpin replies leave WRITE
with `RemoteEffect::Unknown`. READ commits each completed chunk after its scoped
CROSS receipt; a lost Unpin receipt still fails the request even when all chunks
are visible. A failed request contributes zero completed requests/bytes, but may
have a visible prefix. `locally_drained` proves physical local completion, not
rollback, and failed groups do not send attached notifications. Successful local native drain allows cleanup after EOF, while uncertain
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
It defaults to 1 and cannot exceed `staging_chunk_count`. Pin selects the minimum
of the two depths and the request's chunk count. The source checks out that
entire width before acknowledging Pin. Insufficient capacity fails with
`ResourceExhausted`; it never silently narrows the negotiated window. A short
request may negotiate one slot and use the synchronous path.

Each slot owns its pinned allocation, a persistent CUDA stream/event and a
private registration for the current session. Logical chunk `i` uses slot
`i % depth`. Scoped CROSS grants also bind that slot. Registrations retire at
Unpin; the drained native allocation and stream/event remain in the pool for
later sessions. Full engine close destroys those handles before freeing the
pin. Failed construction or destruction remains charged without a Drop retry.

| Direction | Scheduling and completion |
| --- | --- |
| READ | Delegate initially queues PRE for each slot. CROSS waits for its slot's CUDA event, then transfers bytes. The freed slot queues the next PRE while other chunks are in flight. |
| WRITE | CROSS fills a drained slot. Delegate queues POST and returns an acceptance receipt. The next CROSS may use another slot; reuse of the same slot waits for its earlier POST. |
| Final Unpin | Drain every accepted PRE/POST, retire private registrations/MRs and release the GPU range loan before replying. Earlier completed READ chunks may already be visible locally. |

A pipelined Delegate receipt confirms **acceptance**, not CUDA completion.
`remote_delegates` and completed-chunk counters advance only after event wait and
context restoration succeed. `remote_crosses` still follows actual transport
cleanup, and `remote_unpins` follows full session cleanup. WRITE becomes
`RemoteEffect::Unknown` before sending Delegate and is never automatically
replayed after an ambiguous reply.

EOF, a protocol error or deadline expiry stops new work but drains every accepted
copy once. Each range owner tracks accepted copies; each physical slot owns
its stream/event. Ordinary pending work does not fence the entire allocation.
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
completion/construction/destruction. CROSS setup and PRE drain errors use the
nested transport's source epoch; Delegate and Unpin use the staging nonce.
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
negotiated width to match the physical pool slots and persistent stream/event
owners. Source private MR counts scale by negotiated width, while the requester
has one reusable private slot MR per session. Byte and chunk counts do not scale
with width.

The staging-only observer adds context push and stream/event create/destroy
receipts plus paired entry/return records for copy, wait and cleanup calls.
`staging_native.py` joins each event record generation to its actual stream,
thread/context and live GPU/pinned allocations. Completion requires both the
matching wait return and context restoration. Distinct slots may interleave
and finish in different orders; reusing a slot or entering cleanup on any
thread before its completion fails the gate. Pool streams/events must be
destroyed before their pinned allocation is freed. The observer forwards all
native arguments, outputs and return codes without inserting driver waits.

The frozen bounded profile covers TCP/RDMA, CPU/GPU requesters, both host
orientations, asymmetric caps, widths 1/2/3/4/16 and shortening a requested width
to the chunk count. Main pipeline cases turn the ring more than three times and
include a tail. All six sessions reuse physical slots; setup/readback copies
are also checked. Tests distinguish unconfirmed submissions from physical DMA
execution: native receipts do not establish execution overlap or throughput.
The historical 24-case installed matrix uses wire v18 and the generation-aware
observer. Four fresh independent repeats and their cleanup audit also pass;
earlier serial evidence remains bound to its original observer and wire version.
Requester bounded staging requires its own v19 artifact and evidence. Broader
asynchronous remote scheduling remains in P5.5b.
