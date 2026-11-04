# Shared-memory transfers

SHM copies registered CPU bytes between processes on the same Linux host. The requester maps the source's object and copies on the submitting thread or its existing batch worker. The source publishes shared storage allocated by its SHM transport. Ordinary host allocations can serve as the requester's local buffer.

## Allocate and register

Both engines enable SHM. The default leaves it off. The source can register multiple ranges of one allocation, including ranges that begin between page boundaries.

```rust
use orbit_te::{EngineConfig, Permission, ShmConfig, TransferEngine};

fn main() -> orbit_te::Result<()> {
    let source = TransferEngine::new(EngineConfig {
        shm: ShmConfig { enable: true, ..Default::default() },
        ..Default::default()
    })?;
    let host = source.allocate_shm_memory(8192)?;
    host.write(19, b"orbit")?;
    let region = source.register_host_memory(host.clone(), 19, 4096, Permission::RemoteRead)?;
    source.unregister_memory(&region)?;
    drop(region);
    source.free_shm_memory(host)
}
```

The requester opens the segment using the source endpoint and epoch, then submits an ordinary batch with `Transport::Shm` or an `"shm"` policy. The request address is the published registration base plus its byte displacement. The copy uses the registration's allocation offset plus that displacement. Boot identity, the backing namespace, the registration and its permissions must all match.

## Mapping lifetime

The allocator creates a unique object with exclusive creation, reserves its extent, and maps it shared. With an empty `cxl_mount_path` it uses POSIX SHM. A configured absolute directory selects a regular file under that directory. Both peers configure the mount locally. The descriptor carries the object identity and offset; the requester opens an existing object, checks its inode and size, and caches the mapping. A read-only grant opens a read-only descriptor and maps the pages with read protection. SHM does not grant access to CUDA device memory.

Each registration has separate source-owned read and write grants. A physical request holds its own open-file-description lock before it checks the grant. Final unregister fences new copies; an admitted copy retains its mapping, backing and charges until completion. Process exit releases the kernel lock. A range lock coordinates the payload with other SHM, local host and native accesses to the same backing. Overlapping aliases use memmove on a common virtual base and preserve public request order when their byte ranges intersect. Relocation of the whole SHM sub-batch precedes the first payload copy.

`close_source(timeout)` revokes grants and drains admitted borrowers. A timeout leaves ownership in place for a later close. `close_requester(timeout)` drains local work before retiring cached mappings. A failed unmap returns `Error::Unquiesced` and retains the native mapping, descriptor and capacity charge. The source allocation remains owned by its public host and registration handles. After unregistering and dropping those handles, pass the last host handle to `free_shm_memory` for a checked release.

Close, registration, allocation release and stats collect revoked grants whose borrowers have finished. The engine does not need a background sweeper.

Allocation and import caches each use `max_registered_regions` and `max_registered_bytes`. Unlink leaves an already mapped inode alive. New installs scan a bounded number of entries for names left by dead creators in their own PID namespace. An existing peer can also remove the exact orphan it mapped. Existing mappings are never rebound to a replacement inode. These permissions and file locks assume cooperating processes with access to the namespace.

## Qualified paths

CI runs independent source and requester processes on `/dev/shm` and exercises the directory-backed code on tmpfs. The physical CXL profile requires a DAX inode on a pmem namespace backed by the selected CXL region. Its installed gate remains in the [verification queue](qualification.md#shared-memory-and-cxl-verification). The [Describe layout](cpu-tcp.md#development-wire-contract) specifies the SHM metadata trailer.
