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
select that tested C toolchain. CI fixes Ubuntu 24.04 and Clang/libclang 18
for binding generation: the moving Ubuntu 26.04 image generated invalid opaque
layouts in `ibverbs-sys` with its default parser toolchain. Build with matching
`CLANG_PATH=/usr/bin/clang-18` and `LIBCLANG_PATH=/usr/lib/llvm-18/lib` when using
that CI profile; layout assertions remain enabled. Only Linux native builds
are tested; cross compilation remains unqualified.

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

P4.2b lazily opens one RNIC context and protection domain (PD) per engine.
Overlapping and successive slices retain that actual shared owner; separate
engines do not share PDs. Every slice still owns its CQ/QP/MR and stable host
staging, with independent permissions, rkey, QP and PSN. The shared PD is not a
source-grant authorization boundary. Port state, MTU, LID and the selected GID
are re-queried for every new endpoint instead of reusing cached addressing.

This follows the frozen TENT `e2d6e8e0` hierarchy in
`tent/src/transport/rdma/context.cpp` and `endpoint.cpp`: context/PD outlive their
endpoints and memory registrations. TENT's pooled CQs and endpoint/MR caches are
not part of this substage. READ/WRITE use real signaled verbs work requests and
CQ completions; no application/GPU pointer is registered with the RNIC. Endpoint
caching, multirail, zero-copy host RDMA, GPUDirect and pipelined chunks remain
open. Resource reuse alone is not a measured throughput or latency result.

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

Endpoint count and registered staging bytes remain bounded per engine. Each
slice tears down QP -> MR -> CQ, stopping on the first error. Failed teardown
retains the remaining handles, actual staging, its real shared device reference
and budgets; later RDMA admission is rejected. Even dropping the engine cannot
destroy a context/PD still retained by an endpoint.

`TransferEngine::close(timeout)` locks source then requester admission, fences
both before waiting, and gives their accepted operations one shared drain
deadline. Already accepted work can still create its slices during this drain;
closing admission does not cancel it. Once both owners are drained, the runtime
permanently closes device admission, then explicitly destroys PD -> context.
Concurrent/repeated callers serialize native destruction. A timeout leaves both
admissions closed and the device live; another call can continue waiting.
Native teardown failure retains the actual resources, remains charged, returns
`Unquiesced` and cannot be retried by another close caller or Drop.

`close_source` and `close_requester` preserve their independent contracts: either
one leaves the other direction and shared device available. Neither alone nor
the pair supplies an explicit PD/context teardown receipt. Use whole-engine
`close` before releasing an owner's native-resource budget. Drop can attempt
best-effort device teardown after the last endpoint disappears, but cannot
report failure; it preserves the same retention rule. The pinned OrbitKV
consumer still uses the earlier per-slice implementation; its pin and explicit
shutdown-receipt migration require a separate consumer gate.

`RdmaStats` adds active/retained shared device counts and successful device-open
count. An idle open engine can have one active device and zero endpoints;
whole-engine close returns active devices to zero on success. Existing retained
endpoint/byte and successful requester READ/WRITE CQ counts remain independent
of application commit/notification results.

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

The P4.2b CPU controls exercise overlapping/consecutive slice reuse, device
lifetime after runtime drop, partial device construction, every slice teardown
failure retaining its device, PD/context cleanup failure, concurrent exactly-once
close, directional close, whole-close admission fencing, drain timeout and a
previously accepted operation opening its slice after the fence. The physical
probe uses whole-engine close and requires exactly one device open per RDMA
peer, zero shared devices after shutdown and all existing byte/guard/CQ checks.
These new code paths need their own physical qualification; earlier consumer
profiles pinned to `02d64e55` do not establish it.

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
zero remaining registrations, workers, shared RDMA devices, endpoints, staging and retained CUDA
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
`nic_lock`, and (when GPU memory is used) `gpu_lock`/`gpu_uuid`. Optional node
`env` selects explicit native library paths, such as a CUDA forward-compatibility
library; role-specific environment and the selected GPU UUID take precedence.
The controller collects actual native `/proc/self/maps` from both peers and
requires them alongside successful supervision. Each case has
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

For OrbitKV's registered-host component, the same controller accepts
`runner: "orbitkv-peer-host"`. Deploy the consumer core test executable built
with `--no-default-features --features cuda-13,orbitte-rdma`; the runner invokes
its ignored exact `peer::orbitte::tests::rdma_host::registered_host_rdma_peer`
filter and passes role/endpoint/device/port/GID/payload/seed through environment.
Set `rdma_port`, keep both memory kinds `cpu` and transport `rdma`, and use
128 bytes..8 MiB payloads. Identity, supervision and RNIC leases are the same as
for the native runner; CUDA is never initialized and no GPU lock is requested.
Source/requester maps are collected. It checks consumer READ selection and
execution, not its PinnedAllocator, Manager grants/GPU restore or installed
serving. Current OrbitKV consumer builds require CUDA 13. Kernel R535 alone
is not an exclusion: the supplied H20 nodes also install `cuda-compat-13-1`
user-mode driver 590.48.01, a combination supported by NVIDIA's
[forward-compatibility matrix](https://docs.nvidia.com/deploy/cuda-compatibility/forward-compatibility.html).
Check mapped user-mode libraries and actual feature gates instead of inferring
support from the kernel driver version.

The consumer `660f893d` host component and `ff30ee1` runner/source-retention
controls have independent source review and reruns. The physical RoCE pilot
covers A-to-B 4 KiB and B-to-A 1 MiB payload regions with 64-byte guards at both
ends, fragmented READs with three requester CQ completions each, unchanged
guards/unrequested bytes and zero normal-close registration/endpoint/staging
charges. Maps contain real `libibverbs`/`libmlx5`
and no CUDA/TENT. All owned native and supervisor processes exit and RNIC leases
are released. The initial readiness coordination failure stays with successful
reruns in the external evidence. See
[OrbitTE #12](https://github.com/OrbitInfra/OrbitTE/pull/12) and
[OrbitKV #305](https://github.com/OrbitInfra/OrbitKV/pull/305) for frozen source,
binary hashes and independent review locations. This is component correctness,
not a throughput claim or pinned Manager/GPU acceptance.

A separate A-node CUDA 13.1 source profile at consumer `660f893d` passes pinned
source-grant controls, embedded Manager TCP GPU Save/Restore and embedded Manager
same-RNIC RDMA staging GPU Save/Restore. It uses installed `libcudart.so.13.1.80`
and compatibility `libcuda.so.590.48.01`; the RDMA run maps real verbs/mlx5 and
no TENT. Test-owned source/service workers drain and native/supervisor/coordinator
processes are reaped; the fixture kills its private etcd, so graceful-etcd
qualification is separate. External evidence remains outside the checkouts and is indexed in
[OrbitKV #306](https://github.com/OrbitInfra/OrbitKV/pull/306).
This is neither an installed Manager nor cross-process CUDA IPC/cross-host
Manager/GPU or GDR qualification. B-node compute remains untested while all
canonical GPU leases are busy; do not take another workload's GPU.

A later installed consumer fixture at `eb1cc90f` is independently accepted in
[OrbitKV #307](https://github.com/OrbitInfra/OrbitKV/pull/307). The complete
CPython 3.11 wheel contains matching native client and Manager artifacts. Two
separate Managers and an installed client exercise CUDA IPC registration and
Publish, inventory fencing, TCP and same-RNIC RDMA peer READ, then client-local
DRAM H2D Restore. Each provider compares 16,384 GPU bytes and untouched pages,
checks exact Save/remote/H2D counters, repays four original held IPC slots and
normally exits the client/Managers, supervisors and outer waiters. The private
etcd fixture is killed and reaped. Canonical leases are released and GPU memory
returns to zero; no producer-exit warning is accepted.

That installed profile uses Torch 2.13.0+cu130, one real CUDA 13.0.96 runtime,
compatibility driver 590.48.01 and kernel driver 535.161.08 on H20. The CUDA 13.1
compiler does not establish a loaded CUDA 13.1 runtime. Frozen `cudarc` tries
unversioned `libcudart.so` first; the profile explicitly freezes a private normal
alias to Torch's actual `.so.13`, its directory/target and preload. Full loader
order CPU controls preserve the mixed-runtime negative cases. Every actual
client/Manager mapping must match frozen files and the single selected CUDART;
`RTLD_NOLOAD` observes the version without loading another runtime. See the
[consumer recipe](https://github.com/OrbitInfra/OrbitKV/blob/main/docs/transport.md#installed-client-and-manager-profile).
Source/wheel manifests, raw runs, independent review and failed attempts remain
outside the checkouts at `/root/orbitte-artifacts/installed-rdma-20261022/`.

This accepts that healthy installed prerequisite, not a complete numbered
parity stage. Default/natural runtime configurations, uniform installed CUDA
13.1, Manager IPC Restore, inference serving, physical cross-host Manager/GPU,
GDR and native faults remain open. The earlier CUDA 13.1 source evidence stays
separate. Default OrbitKV wheels retain TENT; these selected payload providers
map no TENT libraries.

The CPU `remote_rdma_teardown_failure_retains_source_while_requester_is_locally_drained`
control drives actual TCP framing with verbs fault callbacks. After a source
MR teardown failure, requester QP/MR cleanup can succeed while the remote error
loses its Unquiesced detail on the wire. The test checks zero published READ
bytes, actual source backing retention, endpoint/staging/registration charges,
rejected native admission and a source-close timeout. It does not prove a
physical device failure or provide a remote termination receipt.

Before calling a hardware profile Qualified, additionally run actual delayed
completion/cancel/timeout, endpoint/native teardown faults, restart/reuse and
installed-artifact controls, alongside an independently built frozen TENT oracle
under the same declared budgets. IB and RoCE, GPU directions, GDR, NVLink, MNNVL,
SSD and GDS require separate evidence. No performance comparison is claimed.
