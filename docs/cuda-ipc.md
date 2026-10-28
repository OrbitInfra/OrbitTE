# CUDA IPC and NVLink

Build with `--features cuda`, set `EngineConfig { nvlink: true, ..Default::default() }`,
and select `Transport::Nvlink`. This is a separate payload transport alongside
TCP and RDMA. The shared control socket exchanges descriptors, CUDA handles and
receipts; payload bytes use CUDA device/device or pinned host/device copies. It requires different cooperating
processes on the same physical host, with compatible NVIDIA devices and driver.

## Memory and permission contract

The source explicitly opts in with `CudaMemory::allocate_ipc(device, length)`.
Register that allocation with `register_cuda_memory_batch`, publish its engine
endpoint/epoch through your application's trusted discovery, and open that
segment from the requester. The requester uses owned `CudaMemory::allocate`
or `allocate_ipc`, or `cuda::allocate_pinned_host` registered through
`register_host_memory` / `register_host_memory_batch`. Requests use absolute
registered addresses and retain the
existing READ/WRITE bounds and permissions.

An exported legacy CUDA IPC handle grants access to its **whole underlying
allocation**. Driver hardware does not enforce a registered subrange or a
READ-only grant, and a receipt cannot revoke a copied handle. This interface
requires trusted, cooperating peers that obey the protocol. It is not an
isolation boundary. External/managed GPU allocations and remote host exports are unsupported
in this profile. The exporter process and context must remain alive until every
import is closed. Killing the exporter with a live import is unqualified;
generation checks do not make that CUDA operation safe.

`allocate_ipc` rounds physical allocation size up to 2 MiB and zeroes the entire
extent, including padding. Its public length remains the requested logical size.
An export exclusively borrows the whole logical allocation, including disjoint
registered aliases, until the matching close receipt. Local access and other
exports/RDMA uses cannot reuse it during that loan. Source exports and requester
imports charge the **physical extent** against the transport's shared mapping
budget, bounded by `max_inflight_bytes`, separately from normal request-byte
admission. A 17-byte transfer from a 2 MiB allocation therefore charges 2 MiB of
mapping quota. Configure enough space for the simultaneous mappings you allow.

## Local pinned host directions

A local owned pinned allocation can READ from or WRITE to the remote GPU export.
READ uses `cuMemcpyDtoHAsync_v2`; WRITE uses `cuMemcpyHtoDAsync_v2`. The actual
host pointer is copied directly, with no temporary staging allocation. The
allocation retains the primary context selected by `allocate_pinned_host` and
an allocation-owned reusable copy stream/event. Ordinary/pageable and externally
supplied `HostMemory` are rejected by this bounded profile; there is no implicit
registration or staging fallback. This is an OrbitTE ownership restriction,
not a claim that CUDA or native TENT always rejects pageable memory.

The `HostMemory` access lock stays held through copy, event drain and context
restoration. CPU access and new RDMA leases wait for that critical section.
Requester preflight conservatively rejects any existing RDMA loan on that host
allocation, including disjoint ranges, as the existing device requester does.
The actual slice is checked again at copy time. Uncertain completion or cleanup
fences the whole host owner against subsequent CPU and RDMA access; dropping
an unrelated RDMA lease cannot remove that fence. The native pinned allocation,
context and transport charges remain retained when drain cannot be proven.

## Subbatch import reuse

Consecutive requests in one submission reuse an import when they name the same
remote region and the same local **native CUDA context**. Separate `CudaMemory`
and pinned `HostMemory` allocations on the same primary context can share it,
including a mixed host/GPU group. The transport preserves
input order, including overlapping or out-of-order ranges; each copy drains its
allocation-owned event before the next begins. It does not reorder addresses.

`OpenedMapping` holds one mapping and its physical-extent charge. The source
grants the smallest enclosing remote range, bounded by `max_transfer_bytes`;
logical byte accounting still adds only requested bytes, excluding gaps. A new
region, context, transport or oversized envelope ends the group. If the source
has a smaller limit, an explicit `ResourceExhausted` reply **before any grant**
permits one retry as a single request. No ambiguous I/O, received grant or copy
failure is retried.

With the default `nvlink_cache_entries: 0`, the import closes at the group
boundary. The optional cache below extends that lifetime explicitly.

## Cross-submission relocation cache

Both cooperating engines can opt in with `nvlink_cache_entries: N` (1–4096).
`NVLinkTransport` owns a bounded `RelocationCache`, keyed by peer endpoint,
authoritative source epoch, region ID and the actual local CUDA context.
Concurrent first submissions for the same key share one slot, including during
initial grant/open. Waiting for a busy slot uses the submission deadline. The
cache retains the native context, so its address cannot be recycled under a live
mapping. Different contexts/regions never alias a cached entry.

Every submission still asks the source to validate admission, epoch, current
registration, range and permission. A cached pointer alone grants no new
operation. The source `ExportCache` retains the actual `RegisteredMemory`,
`IpcExport` whole-allocation loan and physical mapping charge. One allocation
therefore supports only one live export token/context in this profile, and
**source-side local access and RDMA remain excluded for the entire cache
lifetime**, including idle time. This is suited to a fixed, cooperating working
set; it is not transparent caching of mutable source buffers.

The first request reserves an active source entry and receives an offer with no
handle. Only the requester's answer permits handle exposure. This ensures that
an explicit close which overtakes a queued first request cannot leave a later
exposed export behind: the requester closes its original socket before issuing
close, so a delayed offer cannot receive its answer. An unexposed reservation is
cleaned up on EOF. Once exposed, missing grant/completion bytes retain the entry.

Each subbatch drains its copy events and restores contexts before a bound
**operation-complete receipt**. Its ACK determines the completed prefix; it does
not release the export or mapping. Completed local GPU/pinned-host owners are
released at this drain boundary, so an idle import cannot accumulate every prior
requester's allocation. Uncertain current copies still retain their real owners.

`close_requester` permanently fences new outgoing work, waits for accepted
submissions, then closes cached imports. Native close and context restoration
must succeed before an independent **mapping-close receipt** releases the source
entry. The close endpoint remains available after source admission closes or the
region is unregistered. A lost close ACK leaves a non-reusable close obligation;
a later `close_requester` retries only the idempotent receipt, never the native
close. An uncertain native close poisons the slot and retains its native owner
and physical quota without retry. `close()` releases outgoing imports before
waiting for incoming exports, permitting two cooperating peers to close together.

Both import and export entry counts are bounded by `nvlink_cache_entries`, and
their physical extents share the existing mapping byte budget. Full caches
reject new keys. There is no automatic eviction, idle expiry or public flush
that permits further submissions in this substage. Dropping the engine does not
prove remote closure: explicit shutdown is required, and uncertain owners stay
retained. A source-close timeout does not revoke an outstanding handle.

## Completion and shutdown

The default per-subbatch profile proceeds as follows:

1. The source validates the request's authoritative source epoch, memory bounds,
   permission, boot identity and requester process identity. It retains the
   whole allocation and quota before exposing the opaque CUDA handle.
2. The requester validates a grant binding the operation/range, source and
   requester epochs, and fresh grant token. It opens the handle in its local
   primary context with lazy peer access, then relocates the granted offset.
3. Each slice's allocation-owned stream issues `cuMemcpyAsync` for device/device
   or the direction-specific host/device API above, records and waits for a
   completion event, and restores the calling context. The mapping retains all
   touched local allocations until close. Both READ and WRITE may
   modify destination bytes before their final report, including on failure.
4. Only a successful native IPC close and context restoration permit a matching
   success/clean-abort receipt. The source releases its loan and quota, then
   acknowledges. WRITE failure may have `RemoteEffect::Unknown`; do not replay it
   automatically. An unproven local drain reports `locally_drained = false`.

Only source-acknowledged requests enter the completed prefix. If a copy fails
cleanly or a later task is cancelled, a successful shared close/ACK confirms the
copied prefix. If native cleanup or the final ACK fails, every attempted request
in that group belongs to `TransferFailure.affected_requests`, starting at
`request_index`. Async tasks in that span share the failure; they are never
reported as skipped. Only requests after the span were not attempted. Uncertain
native cleanup retains all touched owners and marks the whole span undrained.
A lost ACK after a proven local close remains locally drained, with unknown
remote WRITE effects. Earlier acknowledged groups retain their success.

`close_source` stops new source admission and waits for existing exports. A
deadline, process disappearance or malformed/lost receipt does not prove that
an import closed. The source retains the real allocation and its quota on that
ambiguity. Likewise, uncertain import/copy/close/context cleanup retains the
actual requester allocation, context and admission; dropping a Batch or public
memory handle cannot release it. Inspect `EngineStats.nvlink` and
`cuda::retained_resources()` for retained resources. There is no automatic
reclamation after a missing receipt in this substage.

Boot/process identities and epochs detect stale or incompatible peers; they are
not authentication. Wire version 16 keeps per-subbatch IPC opcodes 8/9 (`NVL1`) and adds
cached READ/WRITE/close opcodes 10/11/12 (`NVL2` hello). Initial cached offers use
ready status 12 with no handle payload. TCP/RDMA layouts are unchanged; older
wire versions are rejected rather than interpreted as cached completion.
This is OrbitTE's protocol, not native TENT wire compatibility.

## Correspondence to TENT

Frozen TENT's
[NVLinkTransport](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_transport.cpp)
exports the CUDA allocation base/extent, relocates imported addresses, and uses
streams/events for peer-copy completion. OrbitTE keeps those responsibilities
in `transport/nvlink.rs`, `cuda/ipc.rs` and `cuda/stream.rs`. Rust-owned source
loans, explicit close receipts and retained budgets make the lifetime contract
visible to callers.

TENT's `relocateSharedMemoryAddress` caches imports in its transport relocation
map, while `NVLinkSubBatch` owns task streams/events. OrbitTE's
`transport/nvlink/cache.rs::RelocationCache` follows that transport-level
placement. `mapping.rs::OpenedMapping` owns one native import and its charge;
`export.rs::ExportCache` separately owns source lifetime. `subbatch.rs` only
selects consecutive same-region/context slices. `cuda/ipc.rs::IpcMapping` owns
the native reference, retained context and current touched allocations. Each
local allocation reuses its stream/event. TENT's cache closes in `uninstall`;
OrbitTE additionally requires explicit cooperating source close receipts.
Device-wide stream pooling and broader scheduling remain separate work.

TENT advertises DRAM/GPU directions and optionally registers local host buffers
(`transports/nvlink/host_register`, default false), but its remote relocation
accepts only CUDA buffers. OrbitTE follows that distinction: `cuda/pinned.rs`
owns local pinned backing/stream, `HostMemory` owns CPU/RDMA exclusion, and
`IpcMapping` retains the concrete host or device allocation. Remote grants
remain GPU-only. Remote host exports, dynamic cache eviction, MNNVL/VMM and
exporter-crash safety remain open in P5.4.
These limits follow the native
[CUDA IPC lifetime rules](https://docs.nvidia.com/cuda/archive/13.1.0/cuda-driver-api/group__CUDA__MEM.html).

## Installed qualification

Build an independent public-API consumer from a clean packaged crate:

```sh
CC=gcc CXX=g++ python3 tests/hardware/build_installed.py \
  --consumer nvlink --toolchain stable --output /var/tmp/orbitte-ipc-install
```

The package needs neither RDMA libraries nor the CUDA toolkit. Runtime needs the
CUDA driver. The output includes an installed binary, native observer, bounded
controller and manifest with hashes. On an allocated host, run `nvlink_run.py`
with explicit `--binary`, `--observer`, their manifest hashes, `--devices 0,1`,
`--lock-root`, `--output`, `--payload`, `--mode` and
`--memory device|host|mixed` (mixed requires a batch or cache mode). Output must stay outside the
source checkout. The controller locks the selected GPU UUIDs and requires an
NVLink topology edge; it never claims measured fabric bandwidth from that fact.

Modes cover healthy READ/WRITE, source-close waiting for an open import,
requester exit before close, and stale source identity rejection. The
`batch-healthy`, `batch-graceful` and `batch-requester-exit` modes use three
out-of-order slices across two requester allocations on one native context.
They require one native import per group, distinct nonoverlapping local
allocations before use and a separate matching event drain for every slice. Checks include
full logical bytes/guards, native allocation initialization, exact imported
handles/copy ranges, matched event drain and IPC close, process reaping, lock
release and GPU allocation baselines. The observer forwards real Driver API
results, including exact pinned allocation/free and HtoD/DtoH pointer ABIs.
Host/mixed controls require native pinned owners and no temporary allocation
inside an import/copy cycle; initialization/readback staging outside the cycle
is accounted separately. CPU negative controls check its transparency and reject incomplete
evidence. See [local pinned host qualification](qualification.md#local-pinned-host-cuda-ipc),
[subbatch qualification](qualification.md#bounded-cuda-ipc-subbatches)
and [the initial profile](qualification.md#cooperative-cuda-ipc-and-nvlink) for
exact artifacts and profile limits.

The `cache-healthy` mode starts four concurrent READ submissions, then a WRITE
submission over the same three-slice mixed/host/device group: all fifteen payload
copies must share one native import/export and one final close. `cache-graceful`
and `cache-requester-exit` exercise the distinct persistent export lifetime.
The latter is killed after event drain and before native close, and verifies
retention without final source payload readback. Source admission is no longer
held by an idle handler; the export table remains charged instead. Native ledger
checks distinguish IPC payload from owned-device setup/readback, require exact
copy directions/ranges and event/context completion, and validate final cleanup.
A payload copy cannot borrow an event, wait or restoring context pop from later
owned-device setup/readback. See [cache qualification](qualification.md#bounded-cross-submission-cuda-ipc-cache)
for exact artifacts and the accepted profile.
