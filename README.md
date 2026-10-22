# OrbitTE

A Rust-native transfer engine for AI infrastructure.

OrbitTE is an independent OrbitInfra project using Mooncake TENT as its
capability baseline. It is intended to provide byte movement for OrbitKV and
other systems while leaving cache identity, recovery planning and replica
ownership with those consumers.

## Status

**Early development.** The source tree implements owned/external CPU memory registration,
epoch-fenced P2P descriptors, bounded synchronous/asynchronous TCP READ/WRITE batches and
separate requester/source close and drain, plus bounded local/remote and
batch-completion notifications. Async batches retain real owners through
cancel, timeout and detached-handle cleanup.
Real loopback and independent-process tests cover bytes,
restart fencing and failure ownership. The published `0.1.0-alpha.1` is still
the original identity-only package; this implementation has not been released.

External host backing retains its actual allocation owner. Duplicate ranges
share registration references; overlapping views share one allocation lock.
Host registration batches preflight every range before publishing changes.
Transfers accept absolute addresses and retain the resolved backing during I/O;
local and remote lookup follows TENT's containing-buffer order.

Opt-in `rdma` and `cuda` features add single-rail RC verbs with owned host
staging, retained NVIDIA device/pinned allocations and synchronized CUDA copies.
They use the existing `ibverbs-sys` and `cudarc` Rust bindings, share batch
owners and conservatively retain unquiesced failures.
Each engine lazily reuses one RNIC context/PD while slices keep exclusive
CQ/QP/MR/staging owners. Whole-engine `close(timeout)` fences both directions,
drains accepted work and explicitly destroys shared device resources; failed
teardown stays retained. This new ownership substage has separate qualification
from the earlier consumer profiles.
See the [RDMA/CUDA contract and physical runner](docs/rdma-cuda.md).
An opt-in [OrbitKV allocation bridge](https://github.com/OrbitInfra/OrbitKV/pull/303)
retains real CUDA-pinned allocator slots. The TCP and RDMA consumer candidates
connect source grants, epochs and generation-scoped completion to actual
Manager GPU Save/Restore. Independent CUDA 13.1 source gates and an installed
CPython 3.11 client/Manager fixture now pass in their explicit H20 profiles.
The installed fixture uses one selected Torch CUDA 13.0.96 runtime and checks
cross-process IPC Publish plus TCP/same-RNIC RDMA bytes and normal ownership
retirement. See the [runtime profiles](docs/rdma-cuda.md) and
[OrbitKV #307](https://github.com/OrbitInfra/OrbitKV/pull/307).
The default OrbitKV provider remains TENT.

The protocol remains OrbitTE-only. Full TENT API/wire compatibility, RDMA/GPU
coverage, GPUDirect, NVLink, SSD, installed inference serving and physical fault
qualification remain open. Accepted evidence is scoped in the completion plan;
no complete numbered parity stage is qualified.
See the [CPU TCP contract and recipe](docs/cpu-tcp.md) for behavior and limits.

## Try the source

On Linux with Rust 1.85+ and permission to bind loopback sockets:

```sh
cargo run --locked --example tcp_roundtrip
cargo run --locked --example external_host
cargo run --locked --example notifications
cargo run --locked --example async_batch
cargo test --locked
```

The example prints `18 bytes: hello from OrbitTE`. The test suite includes a
source in a separate OS process, verifies READ/WRITE bytes, restarts it on the
same address and rejects requests carrying its old source epoch.
The external-host example batch-registers ranges of a pre-existing allocation
without copying it and submits an absolute-address transfer. It does not pin pages.
The notification example verifies READ bytes and a single completion message.
The [async example and contract](docs/async-batches.md) cover allocated capacity,
append, status, seal/wait/free and local owner retention.
Data completion and notification delivery have separate outcomes; a lost
notification receipt does not authorize replaying completed data or the message.
The [frozen CPU comparison](docs/cpu-oracle.md) runs one shared byte/registration/
notification trace against unmodified native TENT in a separate test build.
It adds selected behavioral evidence while full API/wire compatibility remains open.

## Implementation plan

The target is functional parity with the frozen latest Mooncake TENT baseline
for CPU DRAM and NVIDIA GPUs. Network and memory movement form the core;
local file/SSD movement through io_uring and cuFile/GDS is optional. Other
accelerator platforms are outside the implementation scope.

- [Completion plan](docs/completion-plan.md): delivery order, dependencies and
  acceptance gates; the project's only execution queue.
- [TENT parity contract](docs/tent-parity.md): reference revision, API inventory,
  runtime behavior, transport matrix and compatibility boundaries.

## Package identity

- Project: **OrbitTE**
- Repository: <https://github.com/OrbitInfra/OrbitTE>
- Cargo package: `orbit-te`
- Rust crate: `orbit_te`

## Development

```sh
cargo fmt --check
cargo check --locked
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo publish --dry-run --locked
```

## License and provenance

Licensed under Apache-2.0. See [LICENSE](LICENSE).

[Mooncake TENT](https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent)
is the capability and architecture baseline. The Rust runtime has no Mooncake
C++ dependency. The implementation and its development protocol are original
Rust code; source correspondence is recorded in the [CPU contract](docs/cpu-tcp.md).
