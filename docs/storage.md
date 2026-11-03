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
`Error::PermissionDenied`. Payload submission and file selector rules await S2.

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
