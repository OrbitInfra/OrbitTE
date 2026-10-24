# Troubleshooting

Start with the [capability matrix](capabilities.md) and the path you selected.
Build availability, device discovery, request completion and physical drain
are separate checks.

## The crates.io package does not expose the documented API

The published `0.1.0-alpha.1` is the original package reservation. Build the
current repository source for the APIs in this documentation. The
[Quick Start](getting-started.md) uses a source example.

## RDMA bindings fail to compile

Reproduce with Ubuntu 24.04, GCC/G++ and Clang/libclang 18, matching CI.
Confirm that `LIBCLANG_PATH` resolves the selected libclang and that an inherited
`CLANG_PATH` does not select a conflicting installation. The build needs CMake,
pkg-config and libibverbs development files. See the [build guide](build.md).

## No RDMA device or compatible path is selected

Check provider visibility and read/write access to the matching uverbs
character node. Confirm the device's port is active and the selected GID is
valid for the intended link layer, GID type and network interface.

An explicit device/port/GID pin is strict. `RdmaConfig::new` selects port 1 and
GID 0 explicitly; use `Default` and optional selectors for discovery. A nonempty
whitelist takes precedence over the blacklist and still intersects a device
pin. `device_mask` uses topology NIC IDs, including inert slots after failed
construction. Inspect `TransferEngine::topology()` after first RDMA use.

Matching GID families or NIC names does not prove that two fabric ports can
connect. The current implementation does not replay a failed transfer through
another rail.

## A peer is rejected after restart or protocol change

Use the source owner's current authoritative epoch. Reopening the same TCP
address with an old epoch cannot adopt a replacement source. Both peers must
use the current OrbitTE protocol version; legacy protocol fallback and native
TENT mixed-peer wire compatibility are not implemented.

## A request times out or returns an uncertain WRITE

Inspect completed-prefix counts, failure information, remote effect and local
drain status. An uncertain WRITE may have changed remote bytes. Logical
cancellation and timeout do not establish physical DMA completion or rollback.
Keep application ownership consistent with the [transfer contract](cpu-tcp.md)
and [RDMA drain requirements](rdma-cuda.md).

## Native cleanup returns Unquiesced

The actual native owners and memory remain retained with their charges. New
admission is fenced; dropping a handle cannot establish a successful native
destroy or safely reset the PD's key history. Preserve the source/artifact,
device identity, error and cleanup evidence for diagnosis.

## CUDA allocation works but direct GPU RDMA does not

NVIDIA allocation and synchronized host staging are the current source paths.
Direct GPU registration, GPUDirect RDMA, IPC and NVLink transfer require separate
implementation and hardware qualification. A successful small CUDA compute
probe does not establish those transport paths.
