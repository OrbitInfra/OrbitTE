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
CUDA device memory and CUDA-owned pinned allocations are outside this profile.

The caller or existing batch worker owns one ring per submission. Before
publishing any SQE it validates every request, acquires exclusive host ranges,
allocates aligned bounce buffers and checks SQ space. An overlapping host
loan fails the group before I/O. Each CQE publishes its task result while other file I/O can still be running.
A short READ or WRITE fails with the exact byte count, including zero at EOF.
Only the returned READ prefix is copied back. Inspect async task results for
individual outcomes when another request in the group fails.

OrbitTE reopens the retained inode with the segment's access rights.
`file_segment_info().direct_io` records whether a direct fd was obtained.
TENT repairs an unaligned address with a 4 KiB bounce but passes the offset
and length through. OrbitTE uses the buffered fd for requests that violate
the filesystem's DIO alignment. It reads `STATX_DIOALIGN` where available
and otherwise assumes 4096 bytes. tmpfs uses the buffered fd.

After a short submit, the worker flushes only the remaining SQEs. On a
persistent submit failure it drains the consumed prefix and fails the unsent
tail. Cancellation stops queued work. Started I/O drains even past a deadline.
If the worker cannot account for its CQEs, `Unquiesced` retains the ring, fds,
memory and admission charges. See [real-file controls](qualification.md#local-file-segments).
