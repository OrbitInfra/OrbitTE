# RDMA and CUDA staging foundation

P4.1/P5.1 add opt-in source implementations, pending independent acceptance and
complete hardware qualification. The [completion plan](completion-plan.md) is
the only execution queue. This is a first bounded, single-rail staging profile;
it does not establish full B04/B01 or native TENT compatibility.

## Build and select a path

The default crate still needs no CUDA/RDMA SDK or native dependency.
`--features rdma` uses the published
[`ibverbs-sys 0.3.2+55.0`](https://docs.rs/ibverbs-sys/0.3.2+55.0/ibverbs_sys/)
release for the standard libibverbs ABI. It links directly to the system
`libibverbs.so.1`. Building needs GCC with the GNU linker, CMake, libclang and
libibverbs development/linker files; install the runtime library and the actual
RNIC provider on each RDMA host. The dependency configures and builds vendored
rdma-core while generating bindings; runtime linking still uses the system
library. No Mooncake source is used.

```sh
# Debian / Ubuntu prerequisites
sudo apt-get install g++ cmake pkg-config libclang-dev libibverbs-dev
CC=gcc CXX=g++ cargo test --locked --all-features
```

The published dependency's symbol-version configure test failed with this
host's Clang 20/LLD combination; GCC/GNU ld passes. The command and CI explicitly
select that tested C toolchain. Only Linux native builds are tested; cross
compilation remains unqualified.

The standard post/poll inline operations dispatch through the provider function
table exposed by the generated bindings. Missing callbacks return an unsupported
error. Port queries use libibverbs' stable compatibility symbol; the engine
consumes fields from its common output prefix. CQ errors preserve their status
and vendor syndrome without reading success-only opcode/length fields.
OrbitTE carries no C bridge or build script. Engine, protocol, RC setup,
progress, admission and backing owners remain Rust.

`--features cuda` uses [`cudarc 0.19.0`](https://docs.rs/cudarc/0.19.0/cudarc/driver/sys/)
for generated Driver API types and ABI signatures, and `libloading 0.8` for
fallible loading of the 16 required versioned entry points from `libcuda.so.1`.
The signatures are inferred from cudarc's generated fields. Its eager loader
is deliberately not called: a missing unrelated CUDA symbol must not panic an
engine that only needs allocation and copy. The library is retained through
all native owners and failed cleanup. The dependency is pinned to preserve
Rust 1.85; newer cudarc releases using libloading 0.9 need Rust 1.88. The
`cuda-12000` binding selection is a compile-time API baseline, not a requirement
to install a CUDA toolkit. No libcudart, nvcc, cuBLAS or NVRTC is needed.
Unsupported platforms/drivers remain unavailable rather than selecting CPU access.

OrbitTE owns fallible QP/MR/context destruction and DMA quiescence explicitly.
The higher-level wrappers' automatic `Drop` behavior does not supply our
failure-retention contract. Reusing bindings does not establish GPUDirect,
NVLink, device MR support or a new completed parity row.

Set `EngineConfig.rdma = Some(RdmaConfig::new("mlx5_0"))` and explicitly choose
the port/GID index, traffic class, hop limit and endpoint/staging budgets. An
Ethernet GID must be nonzero; select a routable GID/type from the real device.
There is no automatic device, rail or topology discovery/selection yet.
Each `TransferRequest` now includes `transport: Transport::Tcp` or `Rdma`.
An uncompiled/unconfigured RDMA request fails preflight, with no fallback.
The peer also rejects unavailable RDMA before exposing a descriptor.

`CudaMemory::allocate` owns device memory and its primary-context reference.
Unsafe `CudaMemory::from_raw_parts` retains the supplied actual allocation AND
context owner, probes device type/context/ordinal/allocation extent, and rejects
host/managed pointers. All aliases must share one `Arc<CudaMemory>`.
`register_cuda_memory_batch` uses the same transactional range, duplicate,
permission and retired-owner accounting as host registration.
`cuda::allocate_pinned_host` returns a `HostMemory` backed by a real CUDA-pinned
allocation and its deallocator/context; ordinary host registration does not pin.

Before handing off an external GPU range, the caller must finish producers and
stop conflicting external accesses. After completion, the caller must hand off
ready bytes to consumers through its own stream/lifecycle contract. These APIs
do not infer arbitrary framework readiness or generation leases.

## Actual data paths and completion

```text
TCP:  CPU/GPU -> owned host bytes -> socket -> owned host bytes -> CPU/GPU
RDMA: CPU/GPU -> owned host MR -> RC RDMA READ/WRITE -> owned host MR -> CPU/GPU
CUDA copy: device <-> private CUDA-pinned staging <-> caller host slice
```

Each RDMA slice owns a PD/CQ/QP/MR and stable host staging. READ/WRITE use real
signaled verbs work requests and CQ completions; no application/GPU pointer is
registered with the RNIC in this profile. This is not zero-copy host RDMA or
GPUDirect. Endpoint caching, multirail, registered application buffers and
pipelined chunks remain open.

Source admission, authoritative epoch, registration bounds and permissions are
checked before bootstrap. READ snapshots source bytes into its staging. WRITE
preloads requester staging. The requester exchanges validated descriptors,
connects its RC QP and posts one WR. It checks completion identity/opcode/status
and READ byte count, then destroys QP and MR before sending a commit marker.
The source destroys QP/MR before exposing staging to CPU/CUDA or committing a
WRITE to registered backing. A final ACK follows that commit. A requester READ
copies into its destination only after local close and the source ACK.
A failed/partial transfer sends no marker and cannot commit source WRITE staging.
A lost marker/ACK receipt can make a WRITE's remote effect unknown; never replay it.

CUDA copies serialize per allocation, push its retained context, use private
pinned staging, synchronize, and pop/verify the context even on copy errors.
Caller host slices are never submitted to CUDA. A normal copy error followed by
successful synchronization returns an error without publishing partial READ
host-slice bytes. A failed H2D/registered-GPU commit can have changed GPU bytes
even when synchronization proves quiescence; a failure does not establish rollback
or authorize consumers to publish those bytes. Synchronization/restoration ambiguity retains device owner, pinned
staging and context. Failed native frees retain resources and their driver.

## Failure, drain and resource ownership

Endpoint count and registered staging bytes are bounded per engine. Teardown
runs QP -> MR -> CQ -> PD -> device, stopping on the first error. Failed teardown
retains the remaining handles, actual staging and budget; later RDMA admission
is rejected. `RdmaStats` exposes retained endpoints/bytes and successful requester
READ/WRITE CQ counts independently of application commit/notification results.

`Error::Unquiesced` means a backend could not prove termination/teardown. A
synchronous report or async task/group can be logically failed with
`locally_drained == false`. Requester registrations/admission, and async batch
capacity/engine references, stay retained even after handle drop. Explicit free
returns `BatchBusy`; requester close times out. A source with unquiesced work
similarly retains its actual registered owner and source permit, so source close
cannot report a false drain. Ordinary remote backend errors do not assert local
physical ambiguity; the remote engine owns that retention.

Retention is conservative protection, not implemented recovery. There is no
reclaim proof/API for quarantined operations yet; process-scoped retained CUDA
resources are observable through `cuda::retained_resources`. A context with
failed restore/unpin is poisoned. GPU/RNIC loss, hardware abort/drain, concurrent
shutdown and reuse qualification remain open. Synchronous native calls/teardown
can exceed an API deadline; no hard real-time or provider interrupt is claimed.

## Verification and physical execution

CPU-only ABI shims cover verbs descriptor layout, malformed fields, construction,
post/poll/CQ errors, deadline and every teardown boundary; they assert exact
retained budgets and admission rejection. CUDA shims cover pointer extent/type,
copy/sync/restore/unpin, private staging, whole-buffer guards, shared registration,
TCP directions and retained source/requester/batch ownership. They do not use
CUDA stub libraries or count as GPU/RNIC qualification.

```sh
cargo fmt --check
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked --all-features
cargo build --locked --release --all-features --example hardware_peer
```

The explicit native [`hardware_peer`](../examples/hardware_peer.rs) runner
performs three noncontiguous READs across two appended submissions, then three
WRITEs, generation-specific notifications and complete source/destination/guard
comparisons. Each run varies a deterministic seed. It checks exact completion
bytes and successful requester WR counts, then unregisters, drains and checks
zero remaining registrations, workers, endpoints, staging and retained CUDA
resources. It supports `cpu`, `pinned:N` and `cuda:N` memory on either peer.
This is a correctness pilot, not a benchmark or a full fault qualification.

Use [`tests/hardware/run.py`](../tests/hardware/run.py) with an externally stored
JSON profile and a fresh external output directory:

```sh
python3 tests/hardware/run.py --profile /external/profile.json \
  --binary /external/build/release/examples/hardware_peer \
  --output /external/runs/run-001
```

The profile supplies `ssh_config`, `rnic`, `gid_index`, `nodes` and `cases`.
Each node has an existing SSH `alias`, exact `hostname`/`boot_id`, `bind` address,
`nic_lock`, and (when GPU memory is used) `gpu_lock`/`gpu_uuid`. Each case has
`source`, `requester` node keys, `source_kind`, `requester_kind`, `transport`,
`payload` (128 bytes..32 MiB), and `seed`. GPU cases expose only the selected UUID
through `CUDA_VISIBLE_DEVICES`, so their kind uses ordinal 0. Resource lock paths
must match the resource owner's existing convention.

First resolve the current platform attempt and run hardware/occupation preflight.
The controller verifies host/boot identity, deploys only the native binary and
supervisor to a unique `/var/tmp/orbitte-runs/` directory, verifies its hash and
holds nonblocking resource locks through each child lifetime. Per-peer native
work has a 55-second supervision deadline; the source also has a 45-second
completion deadline. Only owned children can be terminated. If an owned child
cannot be reaped after termination/kill, the supervisor reports failed cleanup
and keeps its resource locks until waitpid confirms exit. SSH disconnection
does not discard supervision. Healthy SSH starters explicitly wait/reap their
supervisors; the controller waits for those SSH commands too. A broken SSH
connection fails cleanup qualification even when native peers finish, because
a non-reaping container init can retain an orphaned supervisor zombie. Raw commands, failures, stdout/stderr and manifests
remain in the selected external output directory; keys/tokens never enter a
bundle. The runner does not reconfigure a platform job or its duration.

Before calling a hardware profile Qualified, additionally run actual delayed
completion/cancel/timeout, endpoint/native teardown faults, restart/reuse and
installed-artifact controls, alongside an independently built frozen TENT oracle
under the same declared budgets. IB and RoCE, GPU directions, GDR, NVLink, MNNVL,
SSD and GDS require separate evidence. No performance comparison is claimed.
