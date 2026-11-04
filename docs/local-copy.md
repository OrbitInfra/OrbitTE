# Local CUDA copies

Use `orbit_te::cuda::local_copy` to move retained layer ranges between a GPU
and pinned host memory. Enable the Linux `cuda` feature. One worker binds to
one caller-owned CUDA stream. Submit several descriptors in either direction
and keep the completion until the bytes can be reused. The caller chooses the
blocks, layer layout and cache tier. OrbitTE owns the copies and their receipts.

## Bind and submit

In this example `gpu` and `host` are `RegisteredMemory` views, and `stream` is
the consumer's `Arc<cudarc::driver::CudaStream>`. Bind once for the lifetime
of the worker. For a default stream, make its context current before binding.
A per-thread default stream also requires submission on the originating thread.

```rust
use orbit_te::cuda::local_copy::{
    CopyDirection, LocalCopyConfig, LocalCopyDesc, LocalCopyEngine,
};

// SAFETY: stream owns this noncapturing stream in gpu's context. The caller
// coordinates its other GPU work and retains each external allocation.
let worker = unsafe {
    LocalCopyEngine::bind(&gpu, stream.cu_stream().cast(), stream.clone(),
                          LocalCopyConfig::default())?
};
let layers = [
    LocalCopyDesc::new(&gpu, 128, &host, 64, 32),
    LocalCopyDesc::new(&gpu, 512, &host, 128, 16).strided(4, 64, 32),
];
let done = worker.submit(CopyDirection::HostToDevice, &layers, None)?;
// Further work may be queued on the same stream immediately.
done.wait(std::time::Duration::from_secs(5))?;
```

Use `CopyDirection::DeviceToHost` to copy back. Offsets are relative to their
registered views. Each explicit row must fit both views. `strided` takes the
row count, device pitch and host pitch in bytes. Empty batches still get a
stream marker. Allocation-level producer events become `cuStreamWaitEvent`
dependencies. To pass a caller-recorded event, construct
`unsafe { ProducerEvent::from_raw(event, owner)? }` and pass `Some(&ready)`.
The owner keeps the event generation and producer inputs alive. Stream capture
is rejected. Submission restores the exact previous context, including an
empty binding.

## Register an arena

Register one retained `CudaMemory` per GPU allocation. Use
`cuda::allocate_pinned_host` for engine-owned host memory. For a consumer-owned
CUDA host arena, create one `PinnedHostRegistration` per physical pin, then
wrap each disjoint logical slot using its own allocator lease. A pin owner must
keep the physical registration live. A slot owner must prevent its allocator
from recycling that slot. Share the same `Arc<HostMemory>` for overlapping
views. Both constructors are unsafe because only the allocator can promise
these lifetimes and exclude external writes.

```rust
use orbit_te::{HostMemory, Permission};
use orbit_te::cuda::local_copy::PinnedHostRegistration;
use std::sync::Arc;

// SAFETY: these addresses belong to the stated physical pin and live slot.
let pin = unsafe { PinnedHostRegistration::from_raw_parts(arena_base, arena_len, arena_owner)? };
let slot = Arc::new(unsafe {
    HostMemory::from_cuda_registered_parts(slot_base, slot_len, slot_owner, pin.clone())?
});
let host = engine.register_host_memory_batch(&[(slot, 0, slot_len)], Permission::LocalOnly)?
    .remove(0);
```

## DMA and kernel selection

DMA preserves descriptor order. Contiguous source and destination ranges can
become a single 1D copy within one device allocation and physical host pin.
Equal-width rows with constant pitches within the device pitch limit can
become one `cuMemcpy2DAsync`. Gaps stay untouched. For independent 1D copies
on a nonlegacy stream, the driver can use the CUDA 13 batch entry or the
CUDA 12.8 entry with its failure-index argument. Missing optional entries
leave the directional asynchronous calls available. Overlapping destinations
use ordered DMA. Forced kernel submission rejects such a batch.

`LocalCopyConfig::default()` selects `Auto`. At 128 or more compiled DMA
operations, independent fragments are eligible for the mapped-memory kernel.
Set `kernel_threshold` to change the crossover, or set `backend` to `Dma`
or `Kernel`. Auto can choose DMA before upload if the image or mapped alias
is unavailable. The kernel resolves the actual device alias for each physical
host pin, uploads one descriptor array and launches a grid-strided copy with
an aligned vector path and byte tails. The crate ships embedded PTX generated
from the included CUDA source. The driver loads the module during bind.
Consumers need neither a CUDA compiler nor NVRTC.

## Budgets and retirement

The defaults allow 4,096 expanded rows per submission, four in-flight
submissions and 256 MiB per worker. The byte limit covers the sum of live
payload lengths plus the kernel's reserved descriptor storage. Each kernel
slot reserves 24 bytes per possible row on each side of the upload. Each
occupied slot owns one completion event and all of its source and destination
loans. Descriptor and slot limits also bound CPU planning memory. Exhaustion
returns `Error::ResourceExhausted`.

`poll()` queries the completion. `wait(timeout)` bounds the CPU wait.
Dropping a handle leaves its slot owned by the worker. Subsequent submission
or `progress()` polls abandoned slots. `close(timeout)` stops admission and
drains, and can be called again after a timeout. A failed payload enqueue may
already have scheduled a subset. Its marker must complete before the handle
reports the enqueue error. An uncertain record, query, destroy or context
restoration fences the slot with `Error::Unquiesced`. The worker retains its
loans, stream, scratch and charges. Final drop bounds its drain and retains
unconfirmed owners for process lifetime.

The installed comparison checks both directions against OrbitKV on the same
GPU. The local copy profile is implemented and awaits that hardware run.
[Verification queue](qualification.md#caller-stream-local-cuda-copy)
