# CUDA IPC and NVLink

Build with `--features cuda`, set `EngineConfig { nvlink: true, ..Default::default() }`
and select `Transport::Nvlink` (or let [transport selection](transport-selection.md)
choose it). This payload transport sits beside TCP and RDMA: the shared control
socket exchanges descriptors, CUDA IPC handles and receipts, and the bytes move
as CUDA device/device or pinned host/device copies. Both peers must be different
cooperating processes on the same physical host, with compatible NVIDIA devices
and driver.

## Multi-node NVLink and EGM

With `--features cuda`, select `EngineConfig::mnnvl = Some(MnnvlConfig { .. })`
on both engines. The default is off. As in the frozen loader, the **presence**
of `MC_ENABLE_MNNVL` opts the default configuration into MNNVL. An explicit
`None` disables it. Legacy `nvlink` and MNNVL are mutually exclusive.
Use `Transport::Mnnvl` or the policy candidate `"mnnvl"`. Fabric symbols are resolved only when this profile is requested. A missing symbol leaves other transports available.

| Setting | Default | Meaning |
| --- | --- | --- |
| `handle_type` | `MnnvlHandleType::Fabric` | CUDA fabric handle. `PosixFd` uses a same-host exporter PID and fd |
| `fabric_domain` | `None` | Restricts the default to this boot. For cross-host fabric, configure the same nonzero 16-byte domain on peers in the same NVLink partition. Each side advertises it through Describe. The actual CUDA import checks reachability |
| `host_register` | `true` | Registers ordinary local host backing with CUDA through the last registration owner. Existing CUDA pins are reused. With `false`, the caller and driver must support the local host pointer |
| `egm` | `false` | Enables HOST_NUMA allocation and remote host grants |
| `async_memcpy_threshold` | `1024` units of 128 KiB | MNNVL uses the async stream and completion event at every size. The v0.3.14 sync fast path applies only to legacy NVLink |

Call `allocate_mnnvl(device, length)` for GPU storage or
`allocate_egm(numa_node, length)` for host storage. Register the returned
owner with the usual CUDA or host registration API. CUDA rounds the physical
extent to its reported granularity. Logical lengths, permissions and offsets
still apply to each registration. Explicit VMM allocation returns a typed
error when CUDA cannot supply the requested storage.

For existing VMM storage, the unsafe `wrap_mnnvl` and `wrap_egm` constructors
require the actual whole-allocation owner, initialized padding, exact mapped
extent and explicit sharing consent. The driver checks the handle properties,
granularity and any available address-range result. The external owner
controls final unmap. The ordinary raw-pointer registration API grants no
fabric export permission.

A requester can READ or WRITE a remote device allocation from host or GPU
memory. A remote host range must have an EGM export and both peers must opt
in. The local EGM mapping grants CPU access and the copy stream runs on a GPU
attached to its NUMA node. An unknown or incompatible socket placement cannot
select that owner. Imported mappings grant access to the visible **local**
GPUs. Peer CUDA ordinals never select a requester context.

The source consumes producer readiness before granting the actual backing.
The requester consumes its recorded producer event before submitting copies.
Both use the existing NVLink subbatch, stream pool and completion owner.
A close receipt follows physical retirement of the import. Failed unmap,
release or VA cleanup returns `Unquiesced` and retains charges and owners.
An unposted import rejection can select a later candidate. A WRITE with an
uncertain effect is terminal. Use `flush_nvlink_cache` or `close_requester`
to close cached imports and `free_mnnvl` / `free_egm` on the last allocation
handle for a checked local release. See the [installed verification queue](qualification.md#multi-node-nvlink-fabric).

## Memory and permission contract

The source opts in with `CudaMemory::allocate_ipc(device, length)`, registers it
with `register_cuda_memory_batch` and publishes its endpoint and epoch through
trusted discovery. The requester uses owned `CudaMemory::allocate` or
`allocate_ipc`, or `cuda::allocate_pinned_host` memory registered with
`register_host_memory`. Requests use absolute registered addresses with the
normal bounds and permissions. GPU producers can hand off with
[`record_producer_stream`](rdma-cuda.md#producer-stream-readiness) (or the
[pinned host variant](rdma-cuda.md#pinned-host-producer-handoff)); consumers
start after engine completion.

An exported CUDA IPC handle grants access to the **whole underlying
allocation**. The driver does not enforce a registered subrange or read-only
access, and a receipt cannot revoke a copied handle, so this requires trusted
peers and is not an isolation boundary. External or managed GPU allocations and
remote host exports are unsupported. The exporter must stay alive until every
import is closed; killing it with a live import is unqualified.

`allocate_ipc` rounds the physical size up to 2 MiB and zeroes it; the logical
length stays as requested. An export exclusively borrows the whole allocation,
including disjoint aliases, until its close receipt, and local access, other
exports and RDMA wait. Exports and imports charge the **physical extent** against
a mapping budget bounded by `max_inflight_bytes`, so a 17-byte transfer from a
2 MiB allocation charges 2 MiB.

## Local pinned host directions

A local owned pinned allocation can READ from or WRITE to a remote GPU export
with `cuMemcpyDtoHAsync_v2` / `cuMemcpyHtoDAsync_v2` (or a native batch), copying
the host pointer directly with no temporary staging. Pageable and externally
supplied `HostMemory` are rejected; there is no implicit registration or staging
fallback. While the group holds its whole-allocation loan, CPU access and new
RDMA leases return `Unavailable` and other NVLink groups wait within their
deadline. Uncertain completion fences the whole host owner and keeps the pinned
allocation, context and charges.

## Subbatch import reuse

Consecutive requests in one submission share an import when they name the same
remote region and the same local **native CUDA context**, even across different
local GPU and pinned host allocations. Results keep input order, including
overlapping ranges; dependent copies enqueue in order and independent copies may
use a native batch, all on one stream with one trailing event.

`OpenedMapping` holds one mapping and its physical charge. The source grants the
smallest enclosing range up to `max_transfer_bytes`; byte accounting counts only
requested bytes. A new region, context or oversized envelope ends the group. An
explicit `ResourceExhausted` reply **before any grant** permits one retry as a
single request; nothing else is retried. With the default
`nvlink_cache_entries: 0`, the import closes at the group boundary.

## Single small copies

`nvlink_async_memcpy_threshold_kb` defaults to 2048. One legacy NVLink task
strictly below that size uses `cuMemcpy` on the caller or batch worker.
Pinned-host transfers complete on the calling thread. A device-to-device
copy can return before DMA completes, so OrbitTE drains the legacy stream
before releasing its import. A failed drain or context restoration keeps the
loan, mapping and admission. The stream/event path handles groups and copies
at or above the threshold. Setting it to zero disables the fast path. MNNVL
uses its existing async path for every size.
[CUDA synchronization rules](https://docs.nvidia.com/cuda/cuda-driver-api/api-sync-behavior.html)

## Native CUDA batch copies

Groups with at least two independent copies can use `cuMemcpyBatchAsync_v2`
(Driver API 13000+, exact v2 symbol; CUDA 12.8's different signature is never
called). `cuda/ipc/batch.rs::CopyBatch` builds the destination, source and length
arrays. CUDA does not order members inside a batch, so OrbitTE sorts the actual
address intervals in O(n log n) and sends any group with overlapping destinations
or a source/destination dependency through ordered enqueue instead.

Admission and cancellation are checked before submission, so only an admitted
prefix is submitted. A native batch error gives no completed-prefix evidence:
every passed descriptor counts as attempted, the stream is drained, and nothing
is replayed. TENT's `NVLinkTransport::startTransfer` builds the same arrays for
`cudaMemcpyBatchAsync` and falls back to individual copies. See
[qualification](qualification.md#cuda-ipc-native-batch-copy).

## Shared CUDA stream pool

`cuda/stream_pool.rs::CUDAStreamPool` lends each asynchronous group an exclusive
`CUDAStreamHandle` matched to its native context; completed groups return the
stream and event for reuse, independent of the import cache.
`nvlink_stream_count` (default 8, 1–4096) bounds slots across all contexts. At
capacity, an idle slot in another context is replaced; otherwise callers wait
until their deadline. Native calls run outside the pool mutex. Uncertain DMA,
restoration or cleanup quarantines a slot without retrying destroy, and a fully
quarantined pool returns `ResourceExhausted`.

`EngineStats.nvlink` reports `stream_slots`, `active_streams`,
`retained_streams`, `pending_groups` and `progress_workers`. TENT's
`getStreamFromPool` returns the same move-only handle; OrbitTE adds the
cross-context bound and quarantine. See
[qualification](qualification.md#cuda-ipc-stream-pool).

## Event completion progress

After enqueue and the trailing event record, the caller restores its context and
queries the event once; an already-ready group completes without a worker hop.
Otherwise the `IpcCopyGroup`, with its stream handle and allocation loans, moves
to one lazy worker per transport that polls pending groups with `cuEventQuery` in
their retained context, as TENT's `getTransferStatus` does. A ready query plus
context restoration releases the loans and stream. Deadline expiry with a pending
event, a failed query or failed restoration returns `Unquiesced` and keeps every
touched owner and charge.

Setup, mapping operations and source receipts still run synchronously on the
caller, and a Batch's submissions still run in FIFO order. See
[qualification](qualification.md#cuda-ipc-event-progress).

## Cross-submission relocation cache

Both engines can opt in with `nvlink_cache_entries: N` (1–4096). `RelocationCache`
is keyed by peer endpoint, source epoch, region ID and local native context;
concurrent first submissions for a key share one slot. Every submission still
asks the source to validate admission, epoch, registration, range and
permission, so a cached pointer grants nothing by itself. The source
`ExportCache` keeps the registration, whole-allocation loan and physical charge,
so **source-side local access and RDMA stay excluded for the whole cache
lifetime**, including idle time. This is cooperative caching, not transparent
access to mutable buffers.

The first request receives an offer without a handle; only the requester's answer
exposes it, so a close that overtakes a queued first request cannot leave an
exposed export behind. Each subbatch drains its event and sends an
**operation-complete receipt**; completed local owners are released there, while
the export and mapping stay cached.

At capacity, a new key reclaims the least recently used idle mapping; active,
constructing and uncertain entries are never evicted. The old key keeps its slot
until native close **and** the source acknowledgement both succeed; a lost ACK
permits a receipt-only retry, never a second native close. A full cache with no
idle entry returns `ResourceExhausted`. `flush_nvlink_cache(timeout)` closes the
current keys through the same receipts while admission stays open, so concurrent
submissions can refill the cache.

`close_requester` fences outgoing work, waits for accepted submissions, closes
cached imports, then stops completion progress and the stream pool. `close()`
releases outgoing imports before waiting for incoming exports, so two peers can
close together. Dropping an engine does not prove remote closure. TENT's cache
closes in `uninstall`; the explicit receipts, LRU and continuing flush are
OrbitTE extensions. See
[qualification](qualification.md#cuda-ipc-cache-reclamation).

## Completion and shutdown

1. The source validates epoch, bounds, permission, boot identity and requester
   process identity, then retains the allocation and quota before exposing the
   handle.
2. The requester validates the grant (operation, range, both epochs, fresh
   token), opens the handle in its primary context and relocates the offset.
3. `NVLinkSubBatch` resolves local owners, leases a stream, locks each owner once
   in address order, enqueues every slice and records one trailing event.
   Destination bytes may change before the final report, even on failure.
4. Only a successful native close and context restoration permit the success
   receipt; the source then releases its loan and quota and acknowledges.

Only acknowledged requests count as completed. If a shared record, query, restore,
native close or final ACK fails, every attempted request in the group is listed in
`TransferFailure.affected_requests`, and WRITE may have `RemoteEffect::Unknown`.
`close_source` waits for existing exports; a timeout, vanished process or lost
receipt does not prove an import closed, so the allocation stays retained.
Inspect `EngineStats.nvlink` and `cuda::retained_resources()`.

Boot and process identities and epochs detect stale peers; they are not
authentication. The protocol is OrbitTE's own, not TENT wire compatibility.

## Correspondence to TENT

| OrbitTE owner | Frozen TENT counterpart |
| --- | --- |
| `transport/nvlink.rs`, `cuda/ipc.rs` | `NVLinkTransport` export, relocation and copies |
| `transport/nvlink/cache.rs::RelocationCache` | `relocateSharedMemoryAddress` relocation map |
| `transport/nvlink/subbatch.rs::NVLinkSubBatch` | `NVLinkSubBatch` with task stream and event |
| `cuda/stream_pool.rs` | `getStreamFromPool` / `CUDAStreamHandle` |
| `transport/nvlink/export.rs::ExportCache` | No counterpart; explicit source lifetime |

TENT optionally registers local host buffers (`transports/nvlink/host_register`)
but relocates only CUDA buffers remotely; OrbitTE keeps that split, so remote
grants are GPU-only. Legacy IPC remote host exports and exporter-crash safety remain open. See the
[CUDA IPC lifetime rules](https://docs.nvidia.com/cuda/archive/13.1.0/cuda-driver-api/group__CUDA__MEM.html).

## Installed qualification

Build an independent public-API consumer from the packaged crate:

```sh
CC=gcc CXX=g++ python3 tests/hardware/build_installed.py \
  --consumer nvlink --toolchain stable --output /var/tmp/orbitte-ipc-install
```

It needs no RDMA libraries or CUDA toolkit; runtime needs the CUDA driver. On an
allocated host, run `nvlink_run.py` with `--binary`, `--observer`, their manifest
hashes, `--devices 0,1`, `--lock-root`, `--output` (outside the checkout),
`--payload`, `--mode` and `--memory device|host|mixed`. The controller locks the
GPU UUIDs and requires an NVLink topology edge.

Modes cover healthy READ/WRITE, source close with an open import, requester exit
and stale identity rejection; `batch-*` modes use three out-of-order slices over
two allocations, and `cache-*` modes run four concurrent READs and a WRITE through
one import and one final close. Checks cover full bytes and guards, exact handles
and copy ranges, matched event drains and IPC close, process reaping, lock release
and GPU allocation baselines; the observer forwards real Driver API results. See
[cooperative IPC](qualification.md#cooperative-cuda-ipc-and-nvlink),
[subbatches](qualification.md#bounded-cuda-ipc-subbatches),
[pinned host](qualification.md#local-pinned-host-cuda-ipc) and
[cache](qualification.md#bounded-cross-submission-cuda-ipc-cache) evidence.
