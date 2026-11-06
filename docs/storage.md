# Local file transfers

File segments are available in every build. Linux offers three paths for
registered memory to read or write a local file.

| Path | Cargo features | Qualification |
| --- | --- | --- |
| DRAM ↔ file | `io-uring` | Qualified on real ext4 and tmpfs files in CI |
| CUDA ↔ pinned DRAM ↔ file | `cuda,io-uring` | Implemented. Installed GPU/NVMe gate unrun |
| CUDA or DRAM ↔ file through cuFile | `gds` | Implemented. Installed native GDS gate unrun |

With both executors enabled, `Transport::Unspec` prefers eligible GDS, then
io_uring. With neither feature, opening and inspecting a segment still works.
File READ and WRITE return `Unavailable` before payload I/O.
[Qualification and gate commands](qualification.md#local-file-segments)

CI runs `cargo tree --locked --features "$FEATURES" -e normal,build` for
builds without `io-uring` and checks that the io-uring crate is absent.
The GNU/Linux runtime control checks shared-library lookups, threads and live
ring fds after engine construction and file inspection. An idle engine has no
storage worker or ring. File submission probes the kernel when a policy first
considers io_uring. File inspection and memory registration do not load cuFile.

## File segments

`TransferEngine::open_file_segment` reserves an engine-local name. The
literal text after `file://` is a filesystem path. An absolute path starts
with a third slash, as in `file:///tmp/cache.bin`. A relative path resolves
in the process working directory. The first descriptor lookup opens the
existing regular file. Opening a segment never creates or truncates a file.

This example checks the four-byte range starting at byte 8 of an existing file
and closes the segment.

```rust
let segment = engine.open_file_segment("file:///tmp/cache.bin", Permission::RemoteRead)?;
let info = engine.file_segment_info(&segment)?;
assert_eq!(info.offset, 0);
let offset = engine.resolve_file_offset(&segment, TransferOp::Read, 8, 4)?;
assert_eq!(offset, 8);
engine.close_file_segment(&segment, Duration::from_secs(1))?;
```

A file segment describes one buffer with base zero. The request offset counts
bytes from the start of the file. Range checks reject zero length, arithmetic
overflow and an end beyond the current file size. Choose `RemoteRead`,
`RemoteWrite` or `RemoteReadWrite` at open. A disallowed operation returns
`Error::PermissionDenied`.

Opening the same name with the same permission shares the entry.
`file_segment_info` caches size for up to one hour. Use `refresh_file_segment`
after a resize to get a fresh snapshot. Range checks always read the current
size. Once resolved, the handle stays bound to the same file across rename or
unlink. Close and reopen the name to bind a replacement file.

`EngineConfig::max_file_segments` bounds open and draining names (default
1024, maximum 4096). A timeout from `close_file_segment` leaves all aliases
closed to new operations. Retry with the same handle after admitted users
finish. A successful close frees the slot and invalidates the old handle.
Requester and whole-engine shutdown also drain file entries.

## Gather and scatter across registrations

Use `TransferRequest::file_vectored` when scattered local bytes map to one
contiguous file extent. List elements in file order. A
`FileIoElement::Range { address, length }` names one registered host or CUDA
range. `FileIoElement::Padding(n)` writes `n` zero bytes on WRITE and
discards `n` file bytes on READ. Padding needs no local address or
registration. Tuple-only slices of `(u64, usize)` remain supported.

```rust
use orbite::{FileIoElement, TransferOp, TransferRequest, Transport};

let mut elements = vec![FileIoElement::Range {
    address: layer_1.base_address() + offset_1,
    length: layer_bytes,
}];
if gap_bytes != 0 {
    elements.push(FileIoElement::Padding(gap_bytes));
}
elements.push((layer_2.base_address() + offset_2, layer_bytes).into());
if tail_bytes != 0 {
    elements.push(FileIoElement::Padding(tail_bytes));
}
let request = TransferRequest::file_vectored(Transport::Unspec, file_offset, &elements)?;
let batch = engine.submit_async(TransferOp::Write, &segment, &[request], None, timeout)?;
batch.wait(timeout)?;
let receipt = batch.task_status(0)?;
```

Use the same list with `TransferOp::Read` to restore the layers. Leading,
consecutive and trailing padding and padding-only extents are valid. Omit
zero-length elements. The limit is 1024 elements per vector and
`max_batch_requests` descriptors per submission. Padding counts toward those
limits, `max_transfer_bytes` and in-flight byte admission. Overlapping ranges
within a registration share a loan. Any CUDA range selects the CUDA
local-memory filter.

One vector takes one admission charge and produces one task receipt.
`receipt.transferred_bytes` includes the contiguous file prefix, even when
EOF falls inside padding. READ changes only the range bytes in that prefix.
A failed vector contributes zero bytes to the synchronous batch report.
A vectored WRITE is never replayed. An unconfirmed drain retains the file,
loans, staging memory and admission charge.

Host io_uring submits one `readv` or `writev` and uses one private scratch
lease for all padding in that vector. It zeroes the lease before each WRITE.
The lease uses the existing file staging pool or its byte-charged pageable
fallback. For CUDA, the worker zeroes or discards padding in the pinned
chunks it already holds and confirms each device copy. Both paths release
scratch under the same completion rules as data.

Aligned vectors containing only registered ranges can use cuFile directly.
WRITEs use bounded batches. Direct READ issues the next entry only after a
full receipt. Padding and unaligned vectors use io_uring when the file policy
permits it. An explicit `Transport::Gds` request with padding returns
`Error::Unavailable`, even for a page-aligned gap.

## DRAM transfers with io_uring

Enable the Linux `io-uring` Cargo feature. Pass `&segment` to
`submit_and_wait`, `submit_async` or `Batch::submit`. The local address is
registered DRAM and the remote address is a file offset. This reads four bytes
at offset 8.

```rust
let request = TransferRequest::new(Transport::Unspec, local.base_address(), 8, 4);
let report = engine.submit_and_wait(TransferOp::Read, &segment, &[request], timeout)?;
```

An unavailable kernel makes io_uring ineligible. QoS, the runtime window
and the submission deadline apply to files. Local files require no receiver
credit and reject attached notifications.

The caller or existing batch worker owns one ring per submission. Before
publishing any SQE it validates every request, acquires exclusive host ranges,
allocates aligned bounce buffers and checks SQ space. An overlapping host
loan fails the group before I/O. The synchronous report counts only the
consecutive fully successful requests before the first failure, and only their
bytes. The failure span includes later SQEs submitted to the kernel. For a
WRITE, any submitted request in that span makes the remote effect `Unknown`.
Async tasks publish their individual CQEs. A short READ or WRITE fails with
its exact byte count, including zero at EOF. A later task may succeed. Only
the returned READ prefix is copied back.

OrbiTE reopens the retained inode with the segment's access rights.
`file_segment_info().direct_io` records whether a direct fd was obtained.
TENT repairs an unaligned address with a 4 KiB bounce but passes the offset
and length through. OrbiTE uses the buffered fd for requests that violate
the filesystem's DIO alignment. It reads `STATX_DIOALIGN` where available
and otherwise assumes 4096 bytes. tmpfs uses the buffered fd.

After a short submit, the worker flushes only the remaining SQEs. On a
persistent submit failure it drains the consumed prefix and fails the unsent
tail. Freeing an active batch keeps the ring and its buffers alive. If the
blocking wait fails, the owner polls the mapped CQ for the same consumed SQEs.
Cancellation stops queued work. All waits for consumed SQEs share one
cutoff, 30 seconds after the submission deadline. If a CQE is still missing
or the mapped CQ itself cannot be read, the group reports `Error::Unquiesced` at the first unsuccessful index
with `locally_drained = false`.
The ring, fds, iovecs, outstanding bounce buffers, memory loans and admission
charges stay retained. See [real-file controls](qualification.md#local-file-segments).

## CUDA transfers through pinned host memory

With the Linux `cuda,io-uring` features, use the same submission APIs with a
registered CUDA address. The caller or batch worker holds the whole device
range and consumes its producer event before access. By default the engine
leases page-aligned pinned chunks from its `ProxyManager`, bounded by
`staging_chunk_count` and `staging_chunk_size`. Set
`EngineConfig.io_uring.pinned_staging` to enable TENT's optional file pool.
`staging_slot_kb` defaults to 4096 and rounds up to a 4 KiB boundary.
`staging_slots` defaults to 32. The checked total is at most 1 GiB.
At engine creation, `MC_IOURING_PINNED_STAGING`, `MC_IOURING_STAGING_SLOT_KB`
and `MC_IOURING_STAGING_SLOTS` override the corresponding typed settings.
The size overrides apply only when enabled. Invalid boolean values preserve
the configured switch, as in TENT. Invalid enabled sizes return a configuration
error before allocating a slab.
The first eligible request creates one slab. Its slots can serve an entire
GPU request or an unaligned DRAM request. CUDA uses portable pinned memory.
The CPU platform uses ordinary page-aligned memory.

A GPU with an explicit or GPU-local NUMA placement can use the slab only if
its node matches. A request that cannot lease a suitable slot immediately uses the existing
GPU chunk pool or an aligned pageable host buffer charged against
`max_inflight_bytes`. Exhausting both budgets fails before submitting that
round. `EngineStats.io_uring` exposes slot size, capacity, active slots, hits,
misses, pinned backing and outstanding pageable bytes. A slot stays owned
until its CQE and any GPU copy are complete and the caller context is restored. The ring is sized for the device chunks and unsplit host
requests, up to `max_batch_requests` and the Linux submission queue limit.
Each round fills that width from the remaining pieces while chunks are
available. If none can be leased the request fails with `ResourceExhausted`.

WRITE confirms each device-to-host copy and restores the CUDA context before
submitting the round. READ drains the SQEs first and copies back consecutive
file bytes in offset order, including a short final prefix. A gap stops the
byte count even if a later SQE succeeds. A submitted WRITE can still affect
the file beyond that gap. The same worker releases its chunks after both I/O
and CUDA completion before starting another round. An uncertain copy, event
or context restore retains the affected loan, chunks, ring and admission.

Memory from `cuda::allocate_pinned_host` consumes its producer event and goes
directly through the host file path. Each chunk or pinned view uses the direct
fd when its address and extent satisfy DIO alignment, and the buffered fd
otherwise. The installed GPU and local NVMe profile is [queued for verification](qualification.md#verification-queue).

## GDS with cuFile

Enable the Linux `gds` feature and pass a file segment to the existing batch
APIs. `EngineConfig::gds_enabled` defaults to true in a GDS build. Set it
to false to prevent all cuFile loading and registration by that engine.
When enabled, the first file submission whose policy considers eligible GDS
probes cuFile. Before discovery, `stats()?.gds.mode` is `None`.
`Transport::Gds` supports host and CUDA READ/WRITE. CUDA ranges register
on first GDS use in their owning context and consume producer readiness
before I/O. A clean cuFile buffer registration failure increments
`EngineStats::gds.buffer_registration_failures` and leaves the region available
to other transports. An explicit GDS hint for that region returns `Unavailable`.
Automatic selection and file policies can choose io_uring staging when allowed.
An uncertain registration still fences and retains the CUDA allocation.
Host ranges use their existing exclusive loan. A file entry owns
one cuFile handle and its `O_DIRECT` descriptor until close drains its users.
A failure after GDS selection is returned to the caller.

The caller or batch worker sets up one native batch at `gds_batch_depth`
(default 32). Requests split at 16 MiB. A group exceeding the configured
slice count fails before submission for scalar requests. Vectors span bounded batches. Each terminal cookie identifies one
slice. The group reports the completed request prefix. Each async task's byte
count advances as contiguous receipts arrive. Its final status also accounts
for cleanup of its registration. Cancel requests native batch cancellation
and can affect other slices. Only terminal receipts authorize destruction.
An ambiguous submit drains without replay. After the deadline plus the
30-second destruction window, unresolved I/O returns `Unquiesced` and keeps
its registrations, loans and admission. A later submission or close polls
quarantined batches. A failed destroy or deregister retains its owners and
is never retried. Each native batch has its own handle.

`EngineStats::gds.mode` reports whether the driver disables, permits or
forces compatibility I/O. `FileSegmentInfo::gds_mode` records that policy
when the file handle is registered. `gds_require_native` excludes GDS when
fallback is permitted or forced and rejects unaligned I/O or a slice exceeding
cuFile's per-buffer cache. Physical route verification uses before/after
nvidia-fs counters in the [installed gate](qualification.md#verification-queue).

The runtime loads `libcufile.so.0`, including its effective boolean configuration
API. Missing symbols, failed open or unreadable properties leave GDS unavailable.
Engines share one process-wide driver open. The PID guard reopens on first use
in a child and prevents reuse of parent handles. The fork control uses a shim.
NVIDIA specifies undefined behavior after fork from an initialized process.
Use a fresh executable for native child work. [cuFile API reference](https://docs.nvidia.com/gpudirect-storage/api-reference-guide/)
