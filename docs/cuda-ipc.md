# CUDA IPC and NVLink

Build with `--features cuda`, set `EngineConfig { nvlink: true, ..Default::default() }`,
and select `Transport::Nvlink`. This is a separate payload transport alongside
TCP and RDMA. The shared control socket exchanges descriptors, CUDA handles and
receipts; device bytes use CUDA peer copies. It requires different cooperating
processes on the same physical host, with compatible NVIDIA devices and driver.

## Memory and permission contract

The source explicitly opts in with `CudaMemory::allocate_ipc(device, length)`.
Register that allocation with `register_cuda_memory_batch`, publish its engine
endpoint/epoch through your application's trusted discovery, and open that
segment from the requester. The requester uses owned `CudaMemory::allocate`
or `allocate_ipc`. Requests use absolute registered addresses and retain the
existing READ/WRITE bounds and permissions.

An exported legacy CUDA IPC handle grants access to its **whole underlying
allocation**. Driver hardware does not enforce a registered subrange or a
READ-only grant, and a receipt cannot revoke a copied handle. This interface
requires trusted, cooperating peers that obey the protocol. It is not an
isolation boundary. External/managed allocations and host IPC are unsupported
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

## Completion and shutdown

1. The source validates the request's authoritative source epoch, memory bounds,
   permission, boot identity and requester process identity. It retains the
   whole allocation and quota before exposing the opaque CUDA handle.
2. The requester validates a grant binding the operation/range, source and
   requester epochs, and fresh grant token. It opens the handle in its local
   primary context with lazy peer access, then relocates the granted offset.
3. Its allocation-owned stream issues `cuMemcpyAsync`, records and waits for a
   completion event, and restores the calling context. Both READ and WRITE may
   modify destination bytes before their final report, including on failure.
4. Only a successful native IPC close and context restoration permit a matching
   success/clean-abort receipt. The source releases its loan and quota, then
   acknowledges. WRITE failure may have `RemoteEffect::Unknown`; do not replay it
   automatically. An unproven local drain reports `locally_drained = false`.

`close_source` stops new source admission and waits for existing exports. A
deadline, process disappearance or malformed/lost receipt does not prove that
an import closed. The source retains the real allocation and its quota on that
ambiguity. Likewise, uncertain import/copy/close/context cleanup retains the
actual requester allocation, context and admission; dropping a Batch or public
memory handle cannot release it. Inspect `EngineStats.nvlink` and
`cuda::retained_resources()` for retained resources. There is no automatic
reclamation after a missing receipt in this substage.

Boot/process identities and epochs detect stale or incompatible peers; they are
not authentication. The existing wire version 15 gains IPC opcodes 8/9 with an
`NVL1` payload. TCP/RDMA layouts are unchanged; older peers reject these opcodes.
This is OrbitTE's protocol, not native TENT wire compatibility.

## Correspondence to TENT

Frozen TENT's
[NVLinkTransport](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/nvlink/nvlink_transport.cpp)
exports the CUDA allocation base/extent, relocates imported addresses, and uses
streams/events for peer-copy completion. OrbitTE keeps those responsibilities
in `transport/nvlink.rs`, `cuda/ipc.rs` and `cuda/stream.rs`. Rust-owned source
loans, explicit close receipts and retained budgets make the lifetime contract
visible to callers.

TENT caches exported/imported handles and pools streams. This first cooperative
GPU-to-GPU path opens/closes one import per grant and reuses one stream/event per
local allocation. Host/GPU IPC directions, mapping-cache ownership, broader
stream scheduling, MNNVL/VMM and exporter-crash safety remain open in P5.4.
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
`--lock-root`, `--output`, `--payload`, and `--mode`. Output must stay outside the
source checkout. The controller locks the selected GPU UUIDs and requires an
NVLink topology edge; it never claims measured fabric bandwidth from that fact.

Modes cover healthy READ/WRITE, source-close waiting for an open import,
requester exit before close, and stale source identity rejection. Checks include
full logical bytes/guards, native allocation initialization, exact imported
handles/copy ranges, matched event drain and IPC close, process reaping, lock
release and GPU allocation baselines. The observer forwards real Driver API
results; CPU negative controls check its transparency and reject incomplete
evidence. See [qualification](qualification.md#cooperative-cuda-ipc-and-nvlink) for
exact artifacts and profile limits.
