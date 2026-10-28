# Local CUDA staging

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
There is no extra Proxy worker pool in this substage. Remote Delegate/Pin/Unpin,
nested admission, PRE/CROSS/POST pipelines and topology-aware location selection
remain in the existing [P5/P6 queue](completion-plan.md#p5--cuda-gpudirect-and-staged-transfers).

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

See [Qualification](qualification.md) for executed artifacts and limits.
CPU failure controls do not qualify physical GPU faults or remote proxy staging.
