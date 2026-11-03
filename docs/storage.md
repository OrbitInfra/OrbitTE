# Local file segments

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

## DRAM transfers with io_uring

Enable the Linux `io-uring` Cargo feature. Pass `&segment` to
`submit_and_wait`, `submit_async` or `Batch::submit`. The local address is
registered DRAM and the remote address is a file offset. This reads four bytes
at offset 8.

```rust
let request = TransferRequest::new(Transport::Unspec, local.base_address(), 8, 4);
let report = engine.submit_and_wait(TransferOp::Read, &segment, &[request], timeout)?;
```

Engine construction probes ring setup. An unavailable kernel rejects file
submissions. QoS, the runtime window and the submission deadline apply to
files. Local files require no receiver credit and reject attached notifications.

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

OrbitTE reopens the retained inode with the segment's access rights.
`file_segment_info().direct_io` records whether a direct fd was obtained.
TENT repairs an unaligned address with a 4 KiB bounce but passes the offset
and length through. OrbitTE uses the buffered fd for requests that violate
the filesystem's DIO alignment. It reads `STATX_DIOALIGN` where available
and otherwise assumes 4096 bytes. tmpfs uses the buffered fd.

After a short submit, the worker flushes only the remaining SQEs. On a
persistent submit failure it drains the consumed prefix and fails the unsent
tail. Cancellation stops queued work. All waits for consumed SQEs share one
cutoff, 30 seconds after the submission deadline. If a CQE is still missing
then, the group reports `Error::Unquiesced` at the first unsuccessful index
with `locally_drained = false`.
The ring, fds, iovecs, outstanding bounce buffers, memory loans and admission
charges stay retained. See [real-file controls](qualification.md#local-file-segments).

## CUDA transfers through pinned host memory

With the Linux `cuda,io-uring` features, use the same submission APIs with a
registered CUDA address. The caller or batch worker holds the whole device
range and consumes its producer event before access. TENT allocates a host
buffer per request. OrbitTE leases page-aligned pinned chunks from the
engine's `ProxyManager`, bounded by `staging_chunk_count` and
`staging_chunk_size`. The ring is sized for the device chunks and unsplit host
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
