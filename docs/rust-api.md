# Rust API Guide

The library package is `orbit-te`; the Rust crate is `orbit_te`. This guide
describes the current source API. Generate its full type documentation with:

```sh
cargo doc --locked --no-deps --open
```

Add `--features rdma,cuda` when documenting native providers in an environment
with the [build prerequisites](build.md).

## Engine and registration

`TransferEngine::new(EngineConfig)` starts a source control service and owns
requester/source admission. Its default bind address is loopback. Configuration
and budgets are immutable for that engine instance.

| API | Contract |
| --- | --- |
| `register_memory` | Transfer ownership of host bytes into a registered range |
| `register_host_memory` | Register a range of an existing `Arc<HostMemory>` while retaining its real allocation owner |
| `register_host_memory_batch` | Preflight a set of host ranges before publishing new registration references |
| `unregister_memory` | Retire a registration reference; admitted users retain their actual backing |
| `resolve_local_address` | Resolve a contained registered address range without adopting an arbitrary pointer |

`Permission` distinguishes local-only, remote READ and remote READ/WRITE
authority. Repeated registration of the same range keeps its original
permission. CPU access coordination is shared across aliases.

For a configured RDMA engine with host Direct policy, host registration prepares
every usable context's MR copies before lookup/descriptor visibility and before
duplicate reference increments commit. This includes paused ports: their PDs
retain memory registration, while transfer admission waits for recovery. A failed batch retires only its newly
created copies. Previously published ranges remain valid; failed native cleanup
retains unpublished backing and its charges. This can fail at registration time
when native resources or aggregate MR capacity are unavailable. See the
[publication contract](rdma-cuda.md#eager-publication-and-cross-rail-slice-completion).

## Memory type and location

`RegisteredMemory::memory_type()` returns `MemoryType::Cpu` or `Cuda` from the
retained backing without a lock or driver call. `RegisteredMemory::location()`
returns `RangeLocation { address, length, location }` clipped to exactly that
registration. `HostMemory::location(offset, length)` and
`CudaMemory::location(offset, length)` check the requested owner subrange first.
Queries read immutable metadata without taking the payload lock; they do not
wait for producer readiness or grant
permission to access payload bytes.

`MemoryLocation::Cuda { device, context }` contains the process-visible CUDA
ordinal and native context identity. Its display form is `cuda:N`. The context
number is diagnostic metadata, not a retained or usable native handle. Retain
the actual owner/registration and follow its transfer completion contract.
`MemoryLocation::Cpu(Some(N))` displays `cpu:N`; `Cpu(None)` displays `*`.
Ordinary/external host memory and portable CUDA-pinned allocations have unknown
NUMA placement. Pinned DRAM remains CPU memory. A NUMA-bound staging mapping
reports its selected node, not a new physical-residency measurement.

External CUDA adoption performs one `cuPointerGetAttributes` call for type,
managed status, actual context and device, then checks the whole range with
`cuMemGetAddressRange` inside that retained context. Unknown/default attributes,
managed/host pointers, invalid context/device and native errors fail adoption.
They never choose a CPU dereference. The range check uses the mapped allocation,
not a potentially larger virtual-address reservation. See the
[CUDA attribute contract](https://docs.nvidia.com/cuda/archive/13.1.0/cuda-driver-api/group__CUDA__UNIFIED.html).

These APIs implement the owner-backed part of frozen TENT's `Platform`
`getMemoryType` / `getPointerDeviceId` / `getLocation` and `RangeLocation`
responsibilities. They do not provide a general arbitrary-pointer classifier,
CPU page-residency scan or device-plugin ABI. Native RDMA discovery now publishes
`MemType::Cuda` entries with validated `MemEntry::pci_bus_id`, optional NUMA
placement and GPU-to-RNIC priority tiers. Transfer selection consumes the actual
owner's location; see [topology selection](rdma-cuda.md#device-port-gid-and-memory-topology-discovery).
Current hardware/performance limits are in [qualification](qualification.md#cuda-pointer-and-location).

## Peer authority

```rust
let peer = requester.open_segment(
    source.endpoint(),
    source.source_epoch(),
    Duration::from_secs(1),
)?;
```

The expected `SourceEpoch` must come from the authoritative source owner.
Descriptors identify registered regions but do not replace that authority.
Restarting a process at the same endpoint does not authorize an old peer handle
to use the new process's allocations.

## Requests and reports

`TransferRequest` names the transport, local address, remote address and length.
Both ranges must be contained in one applicable registration. `TransferOp`
selects READ or WRITE. Submission resolves and retains each backing allocation
before transfer effects.

`submit_and_wait` returns `BatchReport` with completed request/byte counts,
failure information, notification status and local drain status. The completed
prefix is not a transaction. `RemoteEffect::Unknown` means an attempted WRITE
may have changed peer bytes; a failed report does not authorize automatic replay.
`TransferFailure.affected_requests` is the number of consecutive tasks sharing
the failure from `request_index`. Shared NVLink import close/ACK failure excludes
the entire attempted group from the completed prefix; async tasks in this span
are failed, not skipped. Only later requests were never attempted.
For a cross-rail request, all slice receipts must complete before its one request
and full byte count are added. Earlier slices may already have changed bytes if
a later slice fails, including source-staged WRITE slices copied back after
their own completion. Completion is not an atomic multi-rail commit.
Remote staging READ starts each local POST after its scoped CROSS receipt.
Independent slot copies may complete out of order. A later failure, including
lost Unpin, leaves changed bytes visible but counts the failed request as zero
completed requests/bytes; native uncertainty does not imply an exact prefix.
The CUDA requester uses a bounded ring; WRITE does not snapshot the entire
request. Keep input bytes stable while a request is in flight. GPU staging range loans reject
conflicting access, including overlapping self-peer copies.

For asynchronous ownership, see [`Batch`](async-batches.md): submit groups,
seal, inspect or wait for completion, and retain the handle until resource
ownership is understood. Dropping a handle cannot free live native work.

## Native providers

`transport::rdma::RdmaConfig` selects discovery/pins, host direct or staged
memory, progress mode and bounded native resource budgets.
`RdmaProgressMode::Polling` is the default; `EventDriven` consumes native CQ
events while preserving the same data worker and completion validation.

The `cuda` module owns NVIDIA device/pinned-host allocations and synchronized
copies. Device transfers select declared host staging or explicit GPUDirect RDMA.
Unsafe `CudaMemory::record_producer_stream(stream, owner)` records already-queued
GPU producer work on the caller's actual stream. Retain the live stream and all
producer inputs in the supplied `Arc<dyn Send + Sync>`; aliases share one reusable
event generation. Transfer access consumes readiness before reading or writing
the allocation. `HostMemory::record_producer_stream` provides the same handoff
for owned CUDA-pinned host memory, using the context selected at allocation.
CPU/staging, host RDMA and local IPC access consume that marker. Ordinary or
externally wrapped host memory is rejected; neither API establishes a
consumer-stream dependency. See the
[producer ownership contract](rdma-cuda.md#producer-stream-readiness) before using
the unsafe API, and [RDMA and CUDA](rdma-cuda.md) for range, grant and drain rules.

## Close and drain

With CUDA enabled, `flush_nvlink_cache(timeout)` closes a snapshot of cached
import keys while keeping requester admission open. It waits for each captured
key's current borrower and then uses the native-close/source-receipt protocol.
Concurrent submissions may refill the cache; stop submitting first if it must
be completely empty. Flush also retries outstanding safe release receipts.
See [the cache contract](cuda-ipc.md#cross-submission-relocation-cache).

`close_source` fences source admission; `close_requester` fences requester
admission. Whole-engine `close` fences both, drains accepted work and closes
shared native resources. A timeout does not complete pending native work.
`Error::Unquiesced` retains the real resources and their charges after physical
cleanup could not be proved.

The runnable [TCP example](../examples/tcp_roundtrip.rs),
[external allocation example](../examples/external_host.rs) and
[async example](../examples/async_batch.rs) use the public source API.
