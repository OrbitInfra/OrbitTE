# CUDA staging

`runtime/proxy_manager.rs::ProxyManager` owns an engine-wide pool of reusable
CUDA-pinned host chunks. TCP and staged RDMA share that pool for device-to-host
and host-to-device copies. Each payload transport still owns its network buffers,
MRs, grants and completion receipts.

## Configuration and lifetime

With the `cuda` feature, `EngineConfig.staging_chunk_size` defaults to 4 MiB and
`staging_chunk_count` to 64. Allocation is lazy: one logical GPU copy checks out
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

P5.5b now adds an opt-in serial remote operation. The names and phases follow
TENT's `pinStageBuffer`, `delegateAsync`, `submitCrossStage` and
`RemoteStageOperation`: the latter distinguishes an observed remote result from
an unconfirmed operation before deferred cleanup. OrbitTE uses scoped receipts
and an inline source handler to preserve that distinction with bounded resources.
Native TENT's overlapping pipeline, location selection and asynchronous remote
operation scheduler remain in the existing P5/P6 queue.

## Serial remote staging

Set `EngineConfig.remote_staging = true` on both peers, built with `cuda`.
This explicitly selects remote GPU staging for TCP/RDMA submissions; the remote
registered range must be CUDA. Requester ranges may be CPU or CUDA. NVLink keeps
its own path. Remote host ranges, an opted-out source and a config combining
this flag with `RdmaCudaMemory::Direct` are rejected. No automatic fallback or
new payload transport is introduced. The flag defaults to false.

| Phase | READ | WRITE |
| --- | --- | --- |
| Local preparation | Reserve private host payload | Snapshot local bytes into private host payload |
| Pin | Validate source epoch/region/range/permission; pin one chunk and lock the GPU allocation | Same |
| Delegate PRE | Source GPU → pinned chunk, wait for event and context restore | — |
| CROSS | TCP/RDMA chunk → requester private payload | Requester private payload → TCP/RDMA chunk |
| Delegate POST | — | Pinned chunk → source GPU, wait for event and context restore |
| Unpin | Retire private registration/MRs and release the actual chunk, then acknowledge | Same |
| Local completion | Commit the accumulated private payload into the local range | Complete only after the final Unpin receipt |

One control connection and one source admission own the entire session. Each
chunk completes before the next starts, including the short tail. Nested CROSS
calls the existing transport directly; it never recursively submits an ordinary
batch or waits for a second handler. One handler, one parent batch slot and one
pinned chunk are sufficient. Native RDMA grant/CQ/MR budgets still apply.
RDMA uses its slices protocol even for a single chunk, retaining its bounded
same-connection QP-cache-miss retries and actual verbs payload path.

The requester owns one full-request private host payload under the parent
inflight byte charge; transport buffers and the pinned pool retain their own
bounds. READ POST borrows that private payload under its access lock, without
a second full-request allocation. WRITE releases its local GPU lock before
Pin; READ acquires its local GPU lock only after confirmed Unpin. The source
uses a nonblocking allocation lock and fails busy admission with
`ResourceExhausted`. This avoids self-peer and opposite-direction GPU lock cycles.
The current serial implementation is a correctness boundary, not a bounded-chunk
pipeline or a performance-parity claim.

Wire version 17 adds PinRead/PinWrite, Delegate, CROSS and Unpin operations.
Each frame binds a fresh nonce, original registration, phase and logical range;
success receipts echo that full scope. A repeated prior receipt cannot stand in
for Unpin. Private chunk descriptors never appear in Describe and cannot be used
outside their session. These identities fence reuse; they are not authentication.

CROSS receipts follow actual transport grant cleanup. Unpin first unregisters
the private view, retires every native MR and verifies no private registration
borrower remains, then explicitly releases the chunk and GPU guard. A transport
ACK alone never completes a remote GPU WRITE. Lost POST/Unpin replies leave WRITE
with `RemoteEffect::Unknown`; READ does not commit local bytes without confirmed
Unpin. Successful local native drain allows cleanup after EOF, while uncertain
CUDA or RDMA cleanup retains actual backing and the responsible parent/source
admission. Ordinary confirmed errors remain errors even when locally drained.
There is no automatic retry of a possibly executed WRITE.

`EngineStats.staging.remote_pins`, `remote_delegates`, `remote_crosses` and
`remote_unpins` count source-side completed operations. The Unpin counter records
physical release before its reply, so a lost reply can coexist with a completed
source Unpin. These observations do not replace requester completion receipts.

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
real remote-operation counters and private MR retirement. Its new hardware
profile remains unqualified until the exact installed artifacts run successfully.
