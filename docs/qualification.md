---
myst:
  enable_extensions: [attrs_inline]
---

# Qualification

This ledger records the exact source, artifact and execution profile behind
every result. [Capabilities](capabilities.md) summarizes current behavior and
the [completion plan](completion-plan.md) holds the status of each stage.

| State | Meaning |
| --- | --- |
| Implemented | A reachable source path owns and performs the behavior |
| Qualified | The declared artifact passes its stated source or hardware profile |
| Accepted | An independent reviewer inspects the change and verifies its applicable gates |

A build or CPU fixture never qualifies DMA, a fabric, GPU direct transfer or
performance, and an earlier hardware result does not cover later changes. Wire
labels v3–v23 below identify historical artifacts; current builds follow the
[pre-release wire policy](tent-parity.md#wire-and-consumer-boundaries).
The current [CPU oracle](cpu-oracle.md) uses Mooncake v0.3.14 (`e1ea5d31`). Earlier
reviews and installed profiles retain the reference revision used for each run.
Source CI builds the public consumers for the default, `rdma`, `cuda` and combined
features on Linux, and checks that native features are rejected on other
targets; these compile checks exercise no device.

The [S16 reference checks](https://github.com/OrbitInfra/OrbiTE/tree/main/tests/reference/inventory)
compare the frozen C headers, configuration precedence, provider metadata and
native request/reply bytes on Linux. TCP data fixtures also run against a real
registered TENT source with guard bytes. A frozen client also exercises native
OrbiTE prefix rejection, with no admission or payload movement.

The S17 Rust controls read the same 223 configuration rows, 67 environment
rows and 12 precedence vectors. They check feature selection, error paths,
subsystem settings and snapshot isolation in a child process. These CPU
checks cover configuration loading. Installed transfer profiles are tracked
separately below.

P2P-only configuration controls reject external metadata types and server
URIs through the Rust loaders, inline/file `MC_TENT_CONF`, legacy overrides
and the installed Python constructor, with migration guidance. No external
store or discovery service is
part of this profile. Rust per-peer retirement is implemented with real TCP,
HP-TCP and SHM controls on Linux, and CUDA/verbs table controls. The
`peer_retirement_*` tests check held ownership, submit/close/reclaim races, retained
stale-epoch rejection, selective cleanup and one-shot native failures. Guarded TCP
churn covers ten times the identity capacity; stale clones and failed native
cleanup remain charged under pressure. Reopening a reclaimed identity requires
an explicit authoritative open. RNIC,
NVLink and MNNVL retirement remain **Implemented; installed profile unqualified**.
Python same-epoch reopen preserves frozen segment-handle close; it is not
a drain receipt.

(tent-tcp-interoperability)=
## Historical TENT TCP interoperability

The S23 profile in [#171, `33f86e92`](https://github.com/OrbitInfra/OrbiTE/pull/171)
was accepted before the maintainer removed wire interoperability from scope.
The following records that artifact and its commands; it is not a current
profile or release gate. At that revision, the `Frozen TENT CPU comparison`
workflow built unmodified Mooncake v0.3.14 and ran `tests/reference/interop/run.py` on stable and Rust 1.85. The explicit
CPU/TCP profile checks Rust requester → C++ source and C++ requester → Rust
source, READ/WRITE with full bytes and guards, read-only denial, invalid ranges,
and bad request magic/version. Both directions repeat 50 times; `result.json`
records the frozen revision and each payload. Rust linkage is checked to exclude
the native oracle runtime. The `runtime::control_plane::tent` controls also check
response validation, bounded metadata, explicit selection, epoch retirement,
metadata-change fencing, partial responses and lost WRITE acknowledgements.

Historical command at `33f86e92`, after the CPU oracle created its frozen source/build:

```sh
python3 tests/reference/interop/run.py --source "$OUT/reference" \
  --build "$OUT/tent-build" --iterations 50 --output "$OUT/interop"
```

That evidence covered only the declared Linux loopback CPU profile;
it does not qualify GPU, RDMA bootstrap or physical-network performance.
TENT cannot attest to consumer epochs; unchanged stale addresses remain the
[cooperative profile's recorded limitation](https://github.com/OrbitInfra/OrbiTE/blob/33f86e92/docs/tent-interop.md#identity-and-retirement).
RDMA bootstrap has source codec/rejection controls only and remains unavailable
and unqualified; the native OrbiTE RDMA installed gates do not establish TENT
wire interoperability.

## Libfabric host DRAM

The `CI` workflow's `fabric` and full-feature fabric rows install libfabric and
set `FI_PROVIDER=tcp` and `ORBITE_REQUIRE_FABRIC=1` on stable and Rust 1.85.
`two_process_provider_read_write_with_guards` moves real READ/WRITE payloads
across child processes with unaligned and MR-straddling ranges. The native
controls check aliases, named selection, queued cancellation, source permissions,
peer retirement/restart and immediate reuse after successful completion.
Fallible-table controls hold real CQEs, reject partial posts, inject CQ errors
and make close/remove fail once; they assert retained backing and charges.
The fabric ownership suite repeats 50 times per toolchain with the suite's
thread count. The ignored child entry is invoked by its parent tests.

Only this software TCP-provider CPU profile is covered by a passing run.
EFA is **Implemented; installed profile unqualified**. The packaged gate is
compiled in CI but its hardware command below is not run. No result claims
EFA DMA, fabric performance or physical multi-rail qualification.

## Python wheels

The `Python wheels` workflow builds and installs CPU and combined-feature
wheels for CPython 3.10–3.14 on Linux x86_64 and aarch64, with stable and Rust
1.85. `tests/python/test_installed.py` checks all three S16 Python binding rows
and 128 detailed surface rows. Its real loopback and two-process transfers
check both directions, payload guards, notifications, policy/intent/deadline
routing, exceptions and context exits. Held TCP replies prove GIL release,
early-drop ownership, queued cancellation and timeout retention. Four timing
controls repeat 50 times. The source-distribution job rebuilds and installs its
wheel before repeating the same consumer controls.

`test_cuda_exporters.py` adds CPU-wheel unavailable checks and seven installed
CUDA-wheel controls through a CPU driver shim, including guarded TCP bytes,
CAI/DLPack aliases, producer/context handling, early drop and sticky cleanup.
All seven repeat 50 times on stable CPython 3.13 on both architectures. The
Rust boundary checks the DLPack LP64 layout and bounded duplicate export leases.

This evidence covers the CPU/TCP wheel profile only. Combined-feature builds
exercise that same CPU profile and do not qualify Python GPU, RDMA, GDS or
fabric paths. The native transports retain their installed verification queue.
The wheels and source archive are review artifacts; no PyPI publication is part
of CI. See [Python packaging](python.md#packaging-and-evidence).

## Device plugin controls

Historical evidence from S19/S20/S22, before removal of these interfaces. The
recorded labels below are preserved; these are not current release gates.

The `device-plugin` CI combinations compile CPU shared-library fixtures and run
`tests/device-plugin/run.py` against the packaged C installation on stable and
Rust 1.85. They check frozen LP64 layouts, missing/incompatible libraries,
registration and callback failures, guarded TCP READ/WRITE, cross-engine aliases
and a paused transfer through cancellation and drain. CUDA combinations also
run `tests/device-plugin/cuda.py` against a Driver API shim. Stable repeats these
controls 50 times. This was CPU evidence; the CUDA plugin never received
installed qualification and is now out of scope.

## Installed C ABI on Linux

Historical evidence from S19/S20/S22, before removal of these interfaces. The
recorded labels below are preserved; these are not current release gates.

The `Install both C libraries and run the independent C consumer` CI step builds both libraries from
the core and FFI package archives on stable and Rust 1.85. The C11 consumer
links once per library, compares the exported names and LP64 records to the
S16 inventory and checks guarded READ and WRITE across two loopback processes.
Refusal before dispatch, loss of the final notification ACK, deferred
unregister and same-port restart exercise the return and lifetime contracts.
The io_uring builds also move file bytes. These runs cover the Linux CPU
profile of the recorded S19 artifact.

## C++ source adapters

Historical evidence from S19/S20/S22, before removal of these interfaces. The
recorded labels below are preserved; these are not current release gates.

The installed C++20 client runs in the Linux feature matrix on stable and
Rust 1.85. Native and classic source/requester pairs exchange guarded TCP bytes
and notifications in both directions. A held HP-TCP response exercises timed
scatter waits, moving the operation and callback order. The native-reference
job also compiles the same declaration checks against frozen Mooncake headers.
The CUDA/verbs shims check direct GPU registration, staged-route rejection and
the pinned retry budget. These controls qualify the installed CPU loopback
profile of the recorded S20 artifact. The unrun native profiles are now out of scope.

## Verification queue

No GPU or RDMA machine has been available since 2026-11-03. The profiles below
wait for one hardware pass. Each run uses fresh nodes with recorded host,
provider, RNIC and GPU inventories and locks, a frozen packaged artifact and an
external output directory; failed runs are kept.

| Profile | Current state | Gate | Earlier evidence |
| --- | --- | --- | --- |
| [Python retained CUDA exporters]{#queue-python-cuda} | Implemented; installed profile unqualified | Build with a wheel-compatible Python and maturin: `python3 tests/hardware/build_installed.py --consumer python-cuda --toolchain stable --output "$ART"`. With CUDA PyTorch installed for that interpreter, run `python3 "$ART/python_cuda_run.py" --artifact "$ART" --python "$PYTHON" --gpu "$GPU_UUID" --gpu-lock "$GPU_LOCK" --output "$RUN"`. Repeat the build with 1.85.0 in a fresh directory. | CAI/DLPack, guarded GPU↔host TCP in both requester/source roles, exporter release; CI driver-shim controls only |
| [Caller-stream local CUDA copy]{#queue-local-cuda-copy} | Implemented; installed profile unqualified | [Packaged comparison on one GPU](#caller-stream-local-cuda-copy) | Linux CUDA shim, byte guards, planning and event retention |
| CPU-only requester to peer GPU over ordinary TCP | Implemented; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer peer-gpu-source --output "$ART/source"` and `python3 tests/hardware/build_installed.py --consumer peer-gpu-requester --output "$ART/requester"`. Run `python3 "$ART/source/peer_gpu_tcp_run.py" --source "$ART/source/installed/bin/orbite-peer-gpu-source" --requester "$ART/requester/installed/bin/orbite-peer-gpu-requester" --gpu "$GPU_UUID"`. | Separate CPU-only requester and CUDA-shim source processes in CI |
| Remote staging source controls and Pin capacity (F19/F07) | Implemented; installed profile unqualified | `python3 tests/hardware/build_installed.py --consumer staging-control --toolchain stable --output "$ART"`, then `python3 "$ART/staging_control_run.py" --binary "$ART/installed/bin/orbite-staging-control" --gpu "$GPU_UUID"`. For RNIC loopback run `python3 "$ART/staging_control_run.py" --binary "$ART/installed/bin/orbite-staging-control" --gpu "$GPU_UUID" --rdma "$RNIC" "$GID_INDEX"`. Repeat on 1.85.0 using a fresh output directory. | One GPU, loopback TCP and optional RNIC loopback. Hold a real Pin reply while Describe and a competing Pin complete, then verify guarded WRITE/READ and drain. The two-host RDMA pipeline gate remains queued below. |
| [Remote CUDA staging on the current build: requester ring, CROSS windows, shared shards, event loop, concurrent RDMA CROSS, Delegate queries, socket/connection/setup/finish progress, MR publication, QP retirement, host staging location]{#queue-remote-staging} | Implemented; source-accepted | [Pipeline validation](staging.md#installed-pipeline-validation) with `remote_staging: true` | [Pipeline](#bounded-remote-cuda-pipeline), [serial](#serial-remote-cuda-staging) |
| [Legacy NVLink single small copy and exact context restoration]{#queue-nvlink-sync} | Implemented; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer nvlink --toolchain stable --output "$ART"`. Run `python3 "$ART/nvlink_run.py" --binary "$ART/installed/bin/orbite-nvlink" --observer "$ART/libnvlink_observer.so" --binary-sha256 "$BIN_SHA" --observer-sha256 "$OBSERVER_SHA" --devices 0,1 --lock-root "$GPU_LOCK_ROOT" --output "$RUN" --mode sync-healthy --memory device --payload 65536`. Repeat with `--memory host` and a fresh output directory. | Guarded READ/WRITE, native copy, device drain and matching caller bindings. |
| [CUDA IPC grouped completion, stream pool, event progress, native batch copy and cache reclamation]{#queue-nvlink-grouped} | Implemented | `nvlink_run.py` batch and cache modes ([recipe](cuda-ipc.md#installed-qualification)) | [Cache](#bounded-cross-submission-cuda-ipc-cache), [subbatches](#bounded-cuda-ipc-subbatches) |
| [Mixed GPU Direct and staged batches, producer readiness]{#queue-mixed-gpu} | Implemented; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer mixed-gpu --toolchain stable --output "$ART"`. Run `python3 -B tests/hardware/run.py --profile "$MIXED_GPU_PROFILE" --installation "$ART/manifest.json" --binary "$ART/installed/bin/orbite-mixed-gpu" --output "$RUN"`. Repeat with Rust 1.85.0 in a fresh directory. | [Two-host profile and CPU controls](#mixed-gpu-batches), [producer events](#cuda-producer-readiness) |
| CUDA and custom topology, bandwidth-aware and weighted rails, effective link rate, priority, promotion and shared slots, transport selection, QoS and credits on RDMA | Implemented | Installed host and GPU consumers on multi-RNIC hosts, plus matched performance | [Bandwidth](#rdma-bandwidth-selection), [priority](#rdma-request-priority) |
| Host-shared priority slots, exact NIC slice accounting and automatic receiver credits | Implemented, installed gate unrun | Build `python3 tests/hardware/build_installed.py --consumer host --toolchain stable --output "$ART"`. Run `python3 tests/hardware/quota_run.py --artifact "$ART" --nic "$RNIC" --nic-lock "$RNIC_LOCK" --output "$RUN"` | Two packaged processes on a locked RNIC, guarded READ/WRITE, sender and NIC drain |
| Named NIC masks, endpoint QP pools, merged owners and deadline drops on two physical RNICs | Implemented, installed profile unqualified | Build with `python3 tests/hardware/build_installed.py --consumer host --toolchain stable --output "$ART"`. Run `python3 tests/hardware/selector_run.py --profile "$TWO_RNIC_HOST_PROFILE" --artifact "$ART" --output "$RUN"` | [Selector controls](#transport-policy-selection) |
| [Selective RDMA signaling and QP depths on two rails]{#queue-rdma-rails} | Implemented; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer host --toolchain stable --output "$ART"`. Run `python3 tests/hardware/selector_run.py --selective --profile "$TWO_RNIC_HOST_PROFILE" --artifact "$ART" --output "$RUN/selective"`. For a non-atomic HCA pair run `python3 tests/hardware/selector_run.py --non-atomic --profile "$NON_ATOMIC_HOST_PROFILE" --artifact "$ART" --output "$RUN/non-atomic"`. | Each pinned requester rail, a lower-ranked mapped peer among two source NICs, guarded READ/WRITE, eight WRs and three CQEs per round, and applied depths on both hosts. |
| RDMA rail cooldown and recovery after physical RoCE data-link loss | Implemented, installed gate unrun | Build `python3 tests/hardware/build_installed.py --consumer host --toolchain stable --output "$ART"`. Run `python3 tests/hardware/selector_run.py --profile "$TWO_RNIC_HOST_PROFILE" --artifact "$ART" --output "$RUN" --fault-netdev "$DEDICATED_ROCE_NETDEV"` | [Rail health](rdma-cuda.md#rail-health) |
| GPUDirect GPU source QP/MR reuse (B04) | Implemented; installed profile unqualified | Build `CC=gcc CXX=g++ python3 tests/hardware/build_installed.py --consumer gpu --toolchain stable --output "$ART"`, then run `python3 -B tests/hardware/run.py --require-gpu-source-reuse --profile "$GPU_SOURCE_REUSE_PROFILE" --binary "$ART/installed/bin/orbite-hardware-peer" --installation "$ART/manifest.json" --gdr-observer "$ART/liborbite_cq_fault.so" --output "$RUN"`. Repeat with Rust 1.85.0 and fresh output. GPU-source cases require `gpu_source_reuse: true` on two inventoried hosts. This gate must pass before enabling `RdmaConfig::gpu_source_reuse` by default. | CUDA/verbs controls cover key scope, guarded READ/WRITE, unregister during DMA and failed cleanup. |
| [GPUDirect host-wide context non-growth]{#queue-gpudirect} | Unqualified | [GPUDirect gate](rdma-cuda.md#gpudirect-rdma-ownership) | [GPUDirect](#gpudirect-rdma) |
| [RDMA terminal QP/MR teardown on the current build]{#queue-rdma-host} | Implemented; installed profile unqualified | Using the packaged host artifact above, run `python3 tests/hardware/run.py --profile "$TWO_RNIC_CLEANUP_PROFILE" --installation "$ART/manifest.json" --binary "$ART/installed/bin/orbite-rdma-host" --output "$RUN/teardown"` with `installed-cleanup-failure` and `installed-memory-cleanup` cases. | [Earlier installed cleanup profiles](installed-host.md#native-cleanup-failure) |
| P4 InfiniBand profile, physical link/device loss, provider teardown | Unqualified; needs disposable hardware | [Installed host](installed-host.md) | [P4 review](#p4-independent-review) |
| [CUDA↔file through DRAM staging, pinned slots and exact caller contexts]{#queue-cuda-file} | Implemented; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer storage-cuda --toolchain stable --output "$ART"`. Run `python3 "$ART/storage_cuda_run.py" --artifact "$ART" --gpu "$GPU_UUID" --directory "$NVME_EXT4_DIR" --gpu-lock "$GPU_LOCK" --nvme-lock "$NVME_LOCK" --output "$RUN"` | Native pinned slot, charged oversized fallback, absent/custom caller binding and full guarded bytes. [Shim and real-file controls](#local-file-segments) |
| [Native GDS / NVMe ext4 with nvidia-fs]{#queue-native-gds} | Implemented, installed profile unqualified | `python3 "$ART/gds_run.py" --artifact "$ART" --gpu "$GPU_UUID" --directory "$NVME_EXT4_DIR" --gpu-lock "$GPU_LOCK" --nvme-lock "$NVME_LOCK" --output "$RUN"` | cuFile and CUDA shim controls |
| Engine failover after RDMA bootstrap rejection | Implemented, installed gate unrun | Build `python3 tests/hardware/build_installed.py --consumer host --toolchain stable --output "$ART"`. Run `ORBITE_FAILOVER_NIC="$RNIC" "$ART/installed/bin/orbite-rdma-host" --failover` | Real RNIC baseline, one rejected grant, TCP bytes and guards |
| [SHM]{#queue-shm-cxl} | Qualified on same-host Linux CI for POSIX DRAM | [Physical CXL gate](#shared-memory-and-cxl-verification) | CXL unrun |
| [HP-TCP]{#queue-physical-rails} | Qualified for DRAM on IPv4 loopback | [Physical two-rail gate](#hp-tcp-rail-verification) | Physical profile unrun |
| MNNVL fabric and EGM | Implemented; installed profile unqualified | [Two-node fabric gate](#multi-node-nvlink-fabric) | VMM table and stream/event shims |
| Per-peer RDMA retirement | Implemented; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer peer-retirement --toolchain stable --output "$ART"`, install the identical artifact on both hosts, then run `python3 "$ART/peer_retirement_run.py" --artifact "$ART" --profile rdma --source-host "$SOURCE_HOST" --source-binary "$SOURCE_BINARY" --source-bind "$SOURCE_ADDR:19091" --other-bind "$SOURCE_ADDR:19092" --bind "$REQUESTER_ADDR:0" --device "$RNIC" --source-device "$SOURCE_RNIC" --output "$RUN/rdma"`. Repeat with Rust 1.85.0 and a fresh output. | Guarded READ/WRITE, matching QP retirement, unaffected second peer and same-endpoint restart; no device faults injected |
| Per-peer NVLink IPC retirement | Implemented; installed profile unqualified | With the same packaged consumer on a reserved NVLink host: `python3 "$ART/peer_retirement_run.py" --artifact "$ART" --profile nvlink --source-bind 127.0.0.1:19091 --other-bind 127.0.0.1:19092 --bind 127.0.0.1:0 --source-gpu "$SOURCE_GPU" --gpu "$REQUESTER_GPU" --output "$RUN/nvlink"` | Import/export release and guards; shim controls supply delayed-event and failed-close evidence |
| Per-peer MNNVL retirement | Implemented; installed profile unqualified | On a reserved fabric pair: `python3 "$ART/peer_retirement_run.py" --artifact "$ART" --profile mnnvl --source-host "$SOURCE_HOST" --source-binary "$SOURCE_BINARY" --source-bind "$SOURCE_ADDR:19091" --other-bind "$SOURCE_ADDR:19092" --bind "$REQUESTER_ADDR:0" --source-gpu "$SOURCE_GPU" --gpu "$REQUESTER_GPU" --domain "$FABRIC_DOMAIN" --output "$RUN/mnnvl"` | Native VMM import release, guarded bytes and fresh epoch; actual fabric domain comes from the platform owner |
| EFA host DRAM (S24, B17) | Implemented; installed profile unqualified | `python3 tests/hardware/build_installed.py --consumer fabric --toolchain stable --output "$ART"`, then copy that clean artifact to the same path on both hosts and run `python3 tests/hardware/fabric_run.py --artifact "$ART" --source-ssh "$SOURCE_SSH" --source-ip "$SOURCE_IP" --requester-ip "$REQUESTER_IP" --source-domain "$SOURCE_DOMAIN" --requester-domain "$REQUESTER_DOMAIN" --source-device "$SOURCE_EFA" --requester-device "$REQUESTER_EFA" --output "$OUT/efa.json"`. Repeat with 1.85.0 and a fresh artifact. | Two EFA hosts with delivery-complete support; exact provider/domain, native EFA byte-counter deltas, guarded READ/WRITE and checked close. Written, not run. |
| [P10 native GDS receipt faults]{#queue-p10-gds} | Written and compiled; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer gds-p10 --toolchain stable --output "$ART"`. For each `MODE` in `drain quarantine`, run `python3 "$ART/gds_p10_run.py" --artifact "$ART" --directory "$NVME_EXT4_DIR" --gpu "$GPU_UUID" --gpu-lock "$GPU_LOCK" --nvme-lock "$NVME_LOCK" --mode "$MODE" --output "$RUN/$MODE"`. Repeat with 1.85.0 and a fresh artifact. | Real cuFile submit with withheld status receipts; native-route counters, guarded READ, retained fd/GPU/charges. Not run. |
| P10 RDMA posted-work faults | Prepared; installed rerun required | Use the [installed host runner](installed-host.md#in-flight-dma-interruption) with `installed-inflight-dma`, `installed-lost-completion`, `installed-cleanup-failure` and restart profile cases: `python3 tests/hardware/run.py --profile "$P10_RDMA_PROFILE" --installation "$ART/manifest.json" --binary "$ART/installed/bin/orbite-rdma-host" --output "$RUN"`. Build with `python3 tests/hardware/build_installed.py --consumer host --output "$ART"`. | Existing exact WR/CQE/retirement and byte controls; no new hardware run. |
| P10 cooperative CUDA IPC faults | Prepared; installed profile unqualified | Build `python3 tests/hardware/build_installed.py --consumer nvlink --output "$ART"`. For each `MODE` in `batch-graceful batch-requester-exit`, run `python3 "$ART/nvlink_run.py" --binary "$ART/installed/bin/orbite-nvlink" --observer "$ART/libnvlink_observer.so" --binary-sha256 "$BIN_SHA" --observer-sha256 "$OBSERVER_SHA" --devices 0,1 --lock-root "$GPU_LOCK_ROOT" --output "$RUN/$MODE" --mode "$MODE" --memory device --payload 65536`. | Reuses native copy/event observer and grouped lifetime controls; no exporter-crash claim. Not run. |
| [P10 fault matrix, P11 matched performance against TENT and installed OrbitKV integration]{#queue-release} | CPU preparation implemented; hardware acceptance open | [Completion plan](completion-plan.md#p11--consumers-performance-and-release) | None |

Paths that need no special hardware, such as io_uring on Linux runners, are
qualified in CI and do not wait here.

## Caller-stream local CUDA copy

The Linux CUDA shim exercises coalesced and strided DMA, both native batch
signatures, a kernel upload with non-identical host and device aliases, and
partial submission. It checks exact restoration and retains the actual
payload, scratch and charges after an uncertain event. These are CPU controls.

On an exclusively allocated NVIDIA GPU, use an external artifact directory,
a fresh output directory and the GPU's pre-existing reservation lock. Provide
a read-only OrbitKV checkout containing `93541a21` and its native benchmark's
CUDA, NVRTC and Mooncake build prerequisites.

```bash
python3 tests/hardware/build_installed.py --consumer local-copy --toolchain stable --output "$ART"
python3 "$ART/local_copy_run.py" --artifact "$ART" --gpu "$GPU_UUID" --gpu-lock "$GPU_LOCK" --orbitkv "$ORBITKV_CHECKOUT" --output "$RUN"
```

The driver builds the frozen OrbitKV benchmark in a separate clone. Both
executables report the physical UUID and check every source byte, destination
byte and guard for H2D and D2H. Four matched reverse-order layouts cover
1, 16, 257 and 1,024 fragments. After five warmups the driver records 30
submit-to-drain wall times per direction and backend and their medians. The
unrun gate reserves qualification and timing claims for the named GPU.

## CPU benchmark evidence

S26's [method and reproduction commands](benchmarks.md) pin the workload,
compiler and host in external CI artifacts. [Run 42362843122](https://github.com/OrbitInfra/OrbiTE/actions/runs/42362843122)
completed 480 verified rows on each of stable and Rust 1.85.0 on 2026-11-07.
Each row has three warmups and 100 checked samples; each case has three process
trials. The raw `cpu-bench-*` artifacts retain all samples and commands.
GPU/RDMA/GDS/EFA rows say **not run (hardware)**.

The selected stable results below use OrbiTE branch commit `dfe3ffd2`
(Actions test merge `31b7112cc9e8650357ab06d2b93a44166a6b6524`) and frozen TENT
`e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c`. The recorded runner is
`runnervm7aw5e`: Ubuntu 24.04, Linux `7.0.0-1017-azure`, four allowed x86_64
CPUs (0–3), AMD EPYC 7763, glibc 2.39, Rust 1.99.1/LLVM 23.1.1 and GCC 13.3.0.
Each TENT/OrbiTE pair ran on the same job's host; toolchain jobs are not pooled.
Caller concurrency is one and exposed worker knobs are two; actual process
counts differ (TCP: OrbiTE 7, TENT 37; file: OrbiTE 7, TENT 27–29).
The original manifest's `retries: 0` means engine failover only; TENT's separate
TCP-local retry policy kept its frozen defaults (three, 100–2,000 ms).
The runner now names both settings explicitly without changing those values.

Cells are **OrbiTE / TENT**, the median of the three per-trial statistics.
CPU time is whole-process microseconds per batch, including clock overhead.
A dash means no matched baseline in this driver. Files are warmed ext4 with
OrbiTE O_DIRECT or buffered tmpfs, without fsync. These cloud-loopback results
are neither installed hardware qualification nor a general performance claim.

| Profile / operation / shape | p50 µs O / T | p99 µs O / T | GiB/s O / T | CPU µs O / T |
| --- | ---: | ---: | ---: | ---: |
| CPU memcpy control READ, 1 × 64 MiB | 3,631.6 / 3,579.8 | 3,864.6 / 3,752.2 | 17.125 / 17.412 | 3,652.1 / 3,592.5 |
| TCP READ, 1 × 4 KiB | 275.8 / 285.1 | 327.6 / 334.5 | 0.014 / 0.014 | 376.1 / 255.5 |
| TCP WRITE, 1 × 4 KiB | 278.9 / 287.9 | 355.3 / 318.7 | 0.013 / 0.014 | 393.0 / 256.8 |
| HP-TCP READ, 1 × 4 KiB | 180.1 / — | 288.0 / — | 0.019 / — | 241.2 / — |
| HP-TCP WRITE, 1 × 4 KiB | 168.5 / — | 288.9 / — | 0.021 / — | 225.1 / — |
| SHM READ, 1 × 4 KiB | 158.4 / — | 189.6 / — | 0.025 / — | 112.3 / — |
| SHM WRITE, 1 × 4 KiB | 158.6 / — | 195.3 / — | 0.024 / — | 112.4 / — |
| io_uring ext4 READ, 1 × 4 KiB | 292.4 / 275.6 | 465.1 / 486.7 | 0.012 / 0.013 | 261.6 / 165.9 |
| io_uring ext4 WRITE, 1 × 4 KiB | 328.2 / 272.4 | 664.0 / 529.5 | 0.011 / 0.013 | 294.6 / 167.8 |
| io_uring tmpfs READ, 1 × 4 KiB | 157.3 / 83.7 | 283.1 / 249.6 | 0.023 / 0.041 | 166.5 / 99.4 |
| io_uring tmpfs WRITE, 1 × 4 KiB | 198.1 / 84.5 | 321.2 / 258.2 | 0.018 / 0.041 | 249.4 / 100.1 |
| TCP READ, 1 × 64 MiB | 114,507.4 / 121,340.3 | 146,553.5 / 138,350.4 | 0.523 / 0.515 | 151,510.0 / 160,760.3 |
| HP-TCP READ, 1 × 64 MiB | 13,868.2 / — | 15,240.3 / — | 4.494 / — | 46,363.0 / — |
| SHM READ, 1 × 64 MiB | 3,918.6 / — | 4,109.8 / — | 16.064 / — | 4,039.0 / — |
| io_uring ext4 READ, 1 × 64 MiB | 25,963.8 / 26,999.7 | 54,305.0 / 34,079.6 | 2.138 / 2.293 | 5,266.2 / 4,922.9 |
| io_uring tmpfs READ, 1 × 64 MiB | 8,918.7 / 9,123.2 | 9,521.6 / 9,855.4 | 7.032 / 6.854 | 9,213.4 / 9,378.0 |
| TCP WRITE, 64 × 4 KiB contiguous | 436.6 / 443.5 | 539.4 / 496.6 | 0.553 / 0.552 | 684.1 / 560.4 |
| TCP WRITE, 64 × 4 KiB gapped | 9,543.9 / 6,233.4 | 9,992.8 / 6,483.5 | 0.026 / 0.039 | 17,181.7 / 13,064.4 |
| io_uring ext4 READ, 64 × 4 KiB gapped | 926.5 / 979.2 | 15,716.4 / 1,779.6 | 0.125 / 0.232 | 1,015.2 / 1,143.6 |

OrbiTE is materially slower for scattered small TCP batches: the 64-block
READ/WRITE p50s are 1.36×/1.53× TENT's. Four-KiB tmpfs READ/WRITE p50s are
1.88×/2.34×. Likely contributors are the per-batch worker and per-owner TCP
connections, versus TENT's task/RPC pools and caller-driven file submission;
these are source-based hypotheses, not profiler findings. Ext4's gapped
64-block READ/WRITE tails also inflate (p99 15.7/8.8 ms versus 1.8/1.7 ms),
reducing throughput despite similar medians. That tail cause is unresolved in
this warmed, page-cache/coherence-sensitive workload. No path was tuned here.

[CI run 42362843121](https://github.com/OrbitInfra/OrbiTE/actions/runs/42362843121)
records 24 passing CPU fault controls and two N/A dispositions per toolchain
in `p10-cpu-*`; the new TCP and HP-TCP process-kill controls each passed 50
repetitions. This is P10 preparation, not acceptance of its hardware matrix.

## Metrics

CPU controls scrape a real loopback endpoint with the Prometheus reference
parser, reconcile public and physical outcomes across retries and toggles, compare JSON histogram counts, inject exporter failures, exercise a busy port and check shutdown
with an incomplete request. Real TCP and io_uring, the RDMA and CUDA shims,
and the cuFile table check transport attribution at their executing owners.
The release control in `tests/metrics/overhead.rs` compares empty CPU submissions, a physical owner that expires before payload I/O,
and guarded 64-byte and 4 MiB TCP, HP-TCP and SHM same-host READs and WRITEs with metrics compiled out, configured
off, and enabled. The [S10 handoff](https://github.com/OrbitInfra/OrbiTE/pull/138) records the original TCP measurements. The [same-host comparison](https://github.com/OrbitInfra/OrbiTE/pull/145) includes all three transports and both sizes. The [HP-TCP comparison](hp-tcp.md#rails-and-ownership) reports same-host medians for rotating and least-loaded lanes. Physical
NIC and GPU performance remains unmeasured.

## Multi-node NVLink fabric

The CUDA shim covers the fabric and POSIX handle ABIs, registered offsets,
NUMA placement, producer handoff, concurrent cache reuse, eviction, safe
fallback and native retirement failures. The physical profiles are unrun.

Build the installed consumer on each node from the same checkout. Use two
different hosts in one NVLink clique, a dedicated GPU and an existing exclusive
GPU lock on each host. Use the host NUMA node attached to each selected GPU.
Both hosts must see the same fresh run directory. On each host set
`ART`, `GPU_UUID`, `GPU_LOCK`, `GPU_NUMA` and `BIND` (a concrete control IP
and port, for example `10.0.0.10:0`).

```bash
python3 tests/hardware/build_installed.py --consumer mnnvl --toolchain stable --output "$ART"
```

Run the following on the source and requester, setting `ROLE=source` on the
first node and `ROLE=requester` on the second. Start the source first. Each
profile uses its own directory under a shared `RUN`.

```bash
python3 "$ART/mnnvl_run.py" --role "$ROLE" --profile device --artifact "$ART" --gpu "$GPU_UUID" --gpu-lock "$GPU_LOCK" --numa "$GPU_NUMA" --bind "$BIND" --shared-run "$RUN/device"
python3 "$ART/mnnvl_run.py" --role "$ROLE" --profile egm --artifact "$ART" --gpu "$GPU_UUID" --gpu-lock "$GPU_LOCK" --numa "$GPU_NUMA" --bind "$BIND" --shared-run "$RUN/egm"
```

The driver compares the packaged archive and commit, distinct host boots and
GPUs, completed fabric-manager probes, cluster UUID and clique. It saves
per-link TX/RX payload counters before and after each profile and requires
enough traffic in both directions to cover the guarded READs and WRITEs.
Both peers require successful unregister and explicit VMM free receipts before
publishing success. The EGM variant exercises all four local/remote GPU and host-NUMA pairs.
See [NVIDIA fabric verification](https://docs.nvidia.com/multi-node-nvlink-systems/mnnvl-user-guide/verifying.html)
and the [NVLink counter definitions](https://docs.nvidia.com/deploy/nvidia-smi/index.html).

## Shared memory and CXL verification

The [Linux CI runs](https://github.com/OrbitInfra/OrbiTE/pull/145/checks) include `Install and exercise same-host SHM with POSIX names and a tmpfs directory`, which
builds a packaged consumer on stable and Rust 1.85. Two processes perform 1,048,583-byte
READs and WRITEs, check both guards and read-only denial, and verify the source name
was removed. The lifetime suite repeats 50 times, including process death with a live
copy, unregister, unlink, stale handles and failed native retirement. This qualifies
same-host DRAM SHM. The use of a tmpfs directory exercises the file path.

For the unrun physical gate, choose a mounted fsdax pmem filesystem and the matching
Linux CXL region. The driver checks the actual mapped inode's DAX flag and verifies
that its pmem resource range lies inside the selected CXL region.

```bash
python3 tests/hardware/build_installed.py --consumer shm --output /tmp/orbite-shm-installed
python3 /tmp/orbite-shm-installed/shm_run.py --binary /tmp/orbite-shm-installed/installed/bin/orbite-shm \
  --cxl "$CXL_MOUNT_PATH" --region "$CXL_REGION"
```

## HP-TCP rail verification

The Linux CI `Run the two-process HP-TCP loopback consumer` step transfers
2,097,155 bytes in each direction and checks the payload and guards on both
processes. The stable job also repeats the socket ownership and rail controls
50 times. This evidence covers loopback only.

For physical rails, build on both hosts at the same revision and install
the resulting binary at the same absolute path. Run the driver on the
requester with two IPv4 addresses per host, ordered by their static pairing.

```bash
python3 tests/hardware/build_installed.py --consumer hp-tcp --output /tmp/orbite-hp-installed
python3 tests/hardware/hp_tcp_run.py --binary /tmp/orbite-hp-installed/installed/bin/orbite-hp-tcp \
  --source-ssh "$SOURCE_SSH" --source-rails "$SOURCE_RAIL_A,$SOURCE_RAIL_B" \
  --requester-rails "$REQUESTER_RAIL_A,$REQUESTER_RAIL_B"
```

The physical profile is unrun. The driver records route and byte-counter
evidence for each interface alongside full byte checks on the installed crate.

## Batch progress and notification fallback

Implemented at source level. The Linux CPU controls exercise independent batch
locks, pending free during real TCP reads, active-queue-only fallback ticks,
and multi-peer delivery with a full inbox or missing callback. CUDA and cuFile
shims hold an abandoned stage at its event and keep a permanently failed
destroy charged while another batch retires. The CI stability job repeats
the progress, retirement and notification races 50 times. Hardware qualification
of the underlying native transports remains with their installed profiles.

## Local file segments

`cargo test --locked --lib segment::file::tests` uses real temporary files.
It checks shared and stale handles, cached size and refresh after resize,
EOF and integer overflow, positioned reads and read-only denial. A worker
holds an admitted write while the pathname is replaced and close times out.
The test checks the entire original and replacement files, then verifies fd
release and slot reuse after drain. The same tests run in the Linux feature
matrix. All `io-uring` feature sets require a live kernel
ring (`ORBITE_REQUIRE_IO_URING=1`), ext4 direct I/O and tmpfs buffered I/O.
**DRAM/file io_uring is qualified on Linux CI.** At `b9fb99a` (#128),
`real_files_move_full_bytes_on_ext4_and_tmpfs` checked READ and WRITE
payloads and guards on both filesystems. The suite also covered EOF/truncation,
short submission, syscall failure, queued cancellation, QoS, deadlines and
concurrent close. [Stable job](https://github.com/OrbitInfra/OrbiTE/actions/runs/41639331592/job/125928520140) · [Rust 1.85 job](https://github.com/OrbitInfra/OrbiTE/actions/runs/41639331592/job/125928520599)

On main after the storage closeout (`017654d`), the same real-file test passed on
[stable](https://github.com/OrbitInfra/OrbiTE/actions/runs/41671078266/job/126039289755) and [Rust 1.85](https://github.com/OrbitInfra/OrbiTE/actions/runs/41671078266/job/126039289814).

Vectored host controls on ext4 and tmpfs check guarded ranges, zero-filled
gaps and tails, and the exact prefix after truncation inside a gap. The loader audit runs disabled,
missing-library and shim-provider cases in separate Linux processes.

**CUDA/file staging is implemented. Its installed GPU/NVMe gate is unrun.**
The `cuda,io-uring` and `rdma,cuda,io-uring` jobs use the CUDA driver shim
with real files to check multi-chunk bytes, short READs, producer events,
copy failures, mixed host/device/padding vectors, retained sibling loans and queued cancellation. Build the installed
consumer with `python3 tests/hardware/build_installed.py --consumer storage-cuda --toolchain stable --output "$ART"`.
The queued driver checks the selected GPU UUID and local PCIe NVMe ext4 mount,
holds both existing resource locks and uses a fresh scratch file.

**Native GDS is implemented. Its installed gate is unrun.**
Build the cuFile consumer with `python3 tests/hardware/build_installed.py --consumer gds --toolchain stable --output "$ART"`.
On an otherwise idle GDS host the queued driver disables compatibility I/O,
checks GPU and file guards, and requires nvidia-fs successful read/write counts
and byte deltas matching the exercise. CI uses simulated cuFile receipts and
real temporary files for registration, partial submit, short I/O, cancel,
fork reopen, late quarantine cleanup, failed teardown and ordered vectored READ after a short cookie.

## Per-request remote staging

Source `27b73b5` separates the incoming Pin capability from each outgoing
request's remote-staging choice. One engine/batch can mix GPU Direct RDMA and
Pin/Delegate/Unpin over TCP/RDMA. Private host grants retain their existing host
registration policy, per-context lazy MR reuse and session retirement; source
epoch/range/permission/allocation checks remain authoritative. Wire v21 is
unchanged. That revision added the per-request `remote_staging` field. Current
callers opt in with `.with_remote_staging(true)`;
see [selection and caller migration](staging.md#serial-remote-staging).

One six-branch public control covers READ/WRITE with remote TCP, RDMA host Staged
and RDMA host Direct between two GPU Direct requests. A held middle CUDA event
preserves the completed prefix, waiting tail, admission and notification order.
The requester rejects incoming Pin while successfully issuing outgoing staging.
Nonuniform bytes, guards, a short tail, a second batch, fixed pinned allocations,
private MR retirement and persistent GPU MRs are checked. Existing whole-batch
validation covers unsupported staging choices before any early effects; existing
remote failure controls now select staging explicitly.

Evidence is outside Git at
`/root/orbitte-artifacts/per-request-staging-20261031/`.
Independent source review accepts `df46eab` (production `27b73b5`) in
`independent-review/source-review-df46eab.json`. The reviewer passes the mixed
control, whole-batch preflight and combined Clippy. Primary combined execution
passes 357 unit tests, eight integration tests and one doctest, with five hardware
units and one subprocess fixture ignored. The final test-only portability fix
passes combined/default preflight, formatting and Clippy; unchanged production
and mixed controls are bound by exact input hashes. These CPU verbs/CUDA ABI
controls establish source behavior, not native DMA or a throughput improvement.
Current installed v21 execution and full P5 remain unqualified.

## Mixed GPU batches

The CUDA and verbs shims exercise adjacent GPU Direct ranges on opposite sides
of a held staging operation. They check both directions, the completed prefix,
source and destination guards, alias cancellation, notification delivery and
the physical owners retained after an uncertain event. The producer-event
control covers explicit remote staging over TCP and RDMA.

The queued installed gate uses the existing SSH supervisor with two inventoried
hosts, distinct GPU UUIDs and the GPU/RNIC locks. Set the profile's `runner`
to `orbitte-mixed-gpu`. Each case names its `source` and `requester`, sets both
`source_kind` and `requester_kind` to `cuda:0` and `transport` to `rdma`.
Use `tcp-local`, `tcp-pin` and `rdma-pin` as `staged_leg` in separate cases. Each peer checks its entire GPU allocation. Each direction moves
14 bytes over GPU Direct and 13 through the staged leg. The requester checks
native completion bytes and the source checks private GPU grant creation and
close. Both check staging receipts and retained resources. This profile awaits
installed verification.

Source `5bdf4f2` checks RDMA readiness once per submission, using the transport's
own retained-resource state rather than a full statistics snapshot. Per-request
range/permission validation and native dispatch checks remain in place. See the
[batch contract](async-batches.md#mixed-gpu-direct-and-staged-submissions).

The existing mixed path passes six CPU controls before and after this change:
READ/WRITE with a held middle TCP GPU copy, healthy completion, drained copy
failure and uncertain event completion. They verify exact bytes/guards and
completed prefixes, no early notification or later RDMA dispatch, pinned-buffer
reuse and actual GPU-owner retention. These controls use real sockets and the
CPU verbs/CUDA ABI fixtures; they do not qualify native DMA.

Evidence is outside Git at
`/root/orbitte-artifacts/mixed-gpu-batches-20261031/`.
Independent source review accepts `5bdf4f2` in
`independent-review/source-review-5bdf4f2.json`. The reviewer independently passes
the six-branch mixed control and combined all-target Clippy. Primary combined
validation passes 356 unit tests, eight integration tests and one doctest, plus
formatting and Clippy; five hardware units and one subprocess fixture are ignored.

An isolated release-build CPU comparison measures only preparation and dropping
the prepared request owner, after warmup. Both versions use identical workloads,
the CPU verbs fixture and CPUs 0/1. Across 21 alternating baseline/candidate
pairs, median preparation costs fall from 10.47 to 5.45 microseconds for 64
requests and from 162.20 to 80.82 microseconds for 1,024 requests. The original
seven-pair measurement and all 56 raw logs are preserved under `perf/`, alongside
both source snapshots, workload/runner, build environment and binary hashes.
Both measurements show lower preparation cost for these batch sizes; the
single-request direction changes between measurements and has overlapping
ranges, so it does not establish a stable benefit. The independent performance
audit checks source, log and statistic provenance without rerunning timing.
These results do not measure DMA, end-to-end throughput or performance against
TENT. Installed v21 execution and full P5 remain unqualified.

## Nonreusable QP retirement progress

Requester nonreusable QP retirement now yields through the existing CQ worker
and retains the grant until native close or complete lane retention. See the
[contract](staging.md#nonreusable-qp-retirement-progress). Wire v21 is unchanged.
Independent source review accepts `b2a0aba` (production `4e4c4f2`) for this
source/CPU boundary.

The focused CPU controls hold actual provider destruction and verify busy
record/store/sibling locks, stale and unattached shared claims, original cleanup
deadlines and Retained-before-lane-detachment. Public READ/WRITE staging uses
two CPU verbs contexts on verified independent worker IDs: one CROSS holds an
accepted payload while another holds native QP destruction. Other-CROSS error,
worker stop and original deadline must enter aggregate cleanup before provider
release, while actual backing and admission stay owned. Healthy release checks
nonuniform bytes, sentinels, a short tail and a second transfer; native cleanup
failure retains the actual allocation and reports `Unquiesced`.

Evidence is selected outside Git at
`/root/orbitte-artifacts/qp-retirement-progress-20261031/`.
`primary-b2a0aba/gates.json` records all 11 primary gates on the accepted source.
Stable and Rust 1.85 each pass 236 RDMA units or 355 combined units, eight
integration tests and one doctest; five hardware units and one integration
subprocess fixture remain ignored. Source CI passes all eight profiles, the
frozen TENT CPU comparison and documentation.

`independent-review/source-review-b2a0aba.json` records source acceptance.
The reviewer independently passes all 11 gates on `4e4c4f2`, then verifies
295 of 296 tracked files are unchanged at `b2a0aba` and passes seven focused
retirement, combined Clippy and format gates on fresh targets. The final primary
matrix and CI are audited evidence, not additional independent executions.
The initial CI failure exposed a test assuming a poll was immediately ready
while the worker could hold its lock. `b2a0aba` replaces that assumption with
bounded polling and checks the live sibling on every poll; production is
unchanged. That failed CI log and development controls remain in the archive.

`independent-review/blocking-retire-b2a0aba/manifest.json` records a fresh-target
negative control that only restores blocking grant close in the Retire phase.
The unchanged public control fails its first READ/error-observation case at
the two-second wait (2.01 seconds measured, exit 101), while provider destruction
is held. The fixture releases the hold on unwind; later negative branches are
not claimed as executed. The healthy source control covers all ten READ/WRITE
completion, error, retention, stop and deadline cases.

The source gate does not qualify installed v21 DMA overlap, physical faults or
performance. Native provider calls and aggregate error cleanup remain synchronous;
no new hardware task was requested. Full P5 and ProxyManager parity remain open.

## Persistent MR publication progress

Requester grant construction now yields while an existing persistent MR creator
has not published its handle. The [contract](staging.md#persistent-mr-publication-progress)
keeps the cache creator, grant admission and connection ownership distinct.
The wire remains v21. Independent source review accepts `e69c47a` (production
`b99dfbe`) for this source/CPU boundary.

CPU controls gate an actual cache entry after native registration returns and
before publication, outside provider/context/cache locks. Public READ/WRITE
staging submissions keep a first accepted WR pending while another CROSS waits
for that entry. They verify another CROSS error is actually consumed and terminal
before creator release, including native close failure, stop/deadline retention,
healthy byte/sentinel results and connection reuse. These controls share the
existing cold-connection fixture in `tests/unit/cuda/grant_progress.rs`.
Private controls cover unchanged MR charges/borrowers, retirement/close,
canonical and shared connection claims, a lane closing before grant attachment,
MR cleanup failure and a provider returning after its original deadline.

Evidence is selected outside Git at
`/root/orbitte-artifacts/native-grant-progress-20261031/`.
`independent-review/source-review-e69c47a.json` records acceptance and the
11 independently verified gates. Stable and Rust 1.85 each pass 233 RDMA units
or 351 combined units, eight integration tests and one doctest; five native
units and one integration subprocess fixture remain ignored. CI passes all
eight profiles, frozen TENT CPU comparison and documentation.

`primary-e69c47a/gates.json` binds the 11 primary gates. Unchanged RDMA inputs
carry forward the passed `16847d8` gates; the final combined gates use `e69c47a`.
All 292 tracked files are bound by source manifests. The initial combined run
exposed a fixture observation race: the provider flag preceded production WR
accounting. `e69c47a` waits for actual accepted-WR counters without changing
production code or weakening the assertions; that failure log is preserved.

`independent-review/blocking-mr-control-e69c47a/manifest.json` binds a fresh-target
negative control that only restores blocking MR acquisition. The unchanged
public error-observation test fails its first READ case at the two-second report
wait (2.01 seconds measured), before the ten-second request deadline. It reaches
the actual pending entry; later negative branches are not claimed as executed.
Failed development controls remain alongside final gate logs. No hardware task
was requested. Current installed v21 execution, real DMA overlap and performance
remain unqualified. Native provider calls and context-lock contention are not
preempted; full ProxyManager parity remains open.

## Cold RDMA connection acquisition

Requester acquisition now retains one admitted grant lease while polling the
canonical fixed-width endpoint array. A pending cold builder no longer prevents
the staging loop from observing another accepted CROSS failure, its original
deadline or worker stop. Synchronous callers use the same state and selection
rules. The [contract](staging.md#cold-rdma-connection-acquisition) defines the
remaining synchronous native boundaries.

Evidence directory: `/root/orbitte-artifacts/cold-connection-progress-20261031/`.
The cold-connection review used TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`.
Independent source review accepts `06f685d` for this source/CPU boundary; see
`independent-review/source-review-06f685d.json`. The CPU controls use two
independent verbs ABI contexts, actual endpoint construction reservations and
public GPU staging submissions. One accepted WR is withheld on the first context
while the second CROSS repeatedly reaches the unpublished second array. READ/WRITE cases
cover completion and pair reuse, another CROSS error, failed native cleanup,
worker stop and deadline. Pending polls keep one charge; abort returns that lease
without cancelling the other builder or releasing an uncertain sibling.
The GPU copy loan can drain while RDMA uncertainty retains pinned host backing,
the actual CUDA owner and requester admission. This is not hardware DMA evidence.

`gates.json` records 11 primary gates on `173ae51`; independent fresh targets
pass the same 11 gates. Stable and Rust 1.85 each pass 229 RDMA units or
344 combined units, eight integration tests and one doctest; five hardware
units and one integration subprocess fixture are ignored in the parent runner.
The source manifests bind all 289 tracked files. `06f685d` changes only the
stop control: consume the released payload CQE before stopping, while the cold
reservation remains held. Primary and independent targeted reruns pass all
three public controls (10 READ/WRITE scenarios) on both toolchains; production
code remains byte-identical to `173ae51`. Source CI passes all eight profiles,
the frozen TENT CPU comparison and documentation.

`independent-review/blocking-acquisition-control-173ae51/manifest.json` binds
a fresh-target negative control: only the Acquire pending branch becomes a
blocking loop. The unchanged error-observation test fails its first READ case
at the two-second assertion, before the ten-second request deadline; later
branches do not execute after that failure. Failed development controls and
their corrections remain in `development-history.json`.

No hardware task was requested. Installed v21 execution, real DMA overlap and
performance remain unqualified. Native construction/connect/fill, eviction and
failure drain, nonreusable retirement and source handlers retain synchronous
limits; full ProxyManager parity remains open.

## Native RDMA setup progress

Requester setup now advances partial path tables, parent admission and native
HELLO/OFFER/ACCEPT/READY/GRANT frames while other accepted CROSS payloads remain
queryable. The final scoped CROSS receipt shares the same framing owner. All
parent grants must validate before any payload submission; ordinary synchronous
RDMA uses the same states and validators. Explicit cleanup aborts the partial
grant before prepared/submitted siblings, preserving uncertainty and real owners.
The wire remains v21.

CPU controls use the actual production owners with existing verbs/CUDA shims and
real loopback sockets. They withhold the second parent's path table, admission,
OFFER or final ready reply while the first accepted payload WR reports a CQ error.
Both directions and both successful/failed QP retirement check terminal delivery
and client EOF before relay release, unchanged GPU bytes, no Unpin, range loans,
charged chunks and retention of the actual allocation. Separate healthy controls
split each setup reply/payload, including the last slice's GRANT and descriptor,
assert zero posted payload before release, and check every byte/guard, short tails,
one completion SEND per slice and physical connection reuse. A changed GRANT
binding must fail before any payload. The existing one-slot stale-pair control
also covers a two-slice parent; existing stop/drain and panic/effect tests remain
required without weakened assertions.

Evidence is selected outside Git at
`/root/orbitte-artifacts/rdma-setup-progress-20261030/`.
Independent source review accepts `b2d9d3a` for this source/CPU boundary.
`gates.json` records 22 primary gates: stable and Rust 1.85 tests/Clippy for all
four feature profiles, both relay gates, the 25-observation Rust CPU trace, format
and strict documentation/local-link checks. Each combined run passes 334 unit
tests, eight integration tests and one doctest; five hardware units and one
integration subprocess fixture are marked ignored in the parent runner.
`source-manifest.json` binds all 284 tracked files and `build-environment.json`
records the toolchains. `ci-source-b2d9d3a.json` records 8/8 GitHub CI profiles,
the frozen TENT CPU comparison and documentation success.

`independent-review/source-review-b2d9d3a.json` records the independent diff,
owner/protocol and evidence audit, with fresh stable/MSRV targets and 21 rerun
gates. The final `blocking-setup-control-b2d9d3a/manifest.json` compares every
tracked file: only the setup progress entry is changed to drive the same states
to completion without yielding. The unchanged error-observation test then fails
its first branch's two-second assertion before the ten-second request deadline;
it does not execute all 16 branches after that failure. Compile and binary paths
identify the final archive. Earlier development cleanup-order and Clippy failures
are preserved in `development-history.json` and `primary-ca760b3-failed/`; the final
source passes the original stop/drain and panic/effect assertions.

At this setup revision, cold acquisition and native construction/open/connect/fill,
nonreusable retirement/copyback, failure drain and source handlers retained
synchronous boundaries. The subsequent [cold acquisition](#cold-rdma-connection-acquisition)
and [host location](#host-staging-location) stages cover their declared source
contracts. Installed v21, real DMA overlap and performance remain unqualified.
See [the setup contract](staging.md#native-rdma-setup-progress) for the frozen
TENT correspondence and resource boundaries.

## Native RDMA finish progress

Requester FinishCross now retains its RDMA transfer while polling completion
SEND, reusable-grant cleanup and partial DONE/Reply/ACK/READY frames. Other
submitted CROSS payloads stay queryable. The scoped outer receipt still gates
chunk reuse and GPU POST/Delegate. Shared synchronous/native callers consume the
same SEND submission and sealed grant-drain validation; the wire remains v21.

CPU controls combine actual production owners with the existing verbs/CUDA
shims and real loopback sockets. A later CROSS's partial native Reply remains withheld
while an earlier accepted payload WR reports failure; terminal delivery and
client EOF must precede relay release. A separate accepted SEND with its CQE
withheld must permit internal worker stop before the original deadline. Both
controls run READ/WRITE with successful QP retirement and injected retirement
failure, checking drain, unchanged GPU bytes, no Unpin and actual owner retention.
WRITE has no exported-GPU effect before its Delegate, even if CROSS touched
remote pinned staging. Successful cleanup preserves cancellation or its native
drain error; failed cleanup must remain `Unquiesced`.

Healthy controls split Reply, ACK and READY, resume the same connection, and
check all payload/guard bytes, one SEND per slice and QP reuse. A changed ACK
binding fails before a chunk completes. Evidence is selected outside Git at
`/root/orbitte-artifacts/rdma-finish-progress-20261030/`.
Independent source review accepts `e2f4037` for this source/CPU boundary.
`gates.json` records 22 primary gates: stable and Rust 1.85 tests/Clippy for all
four feature profiles, both relay gates, the 25-observation Rust CPU trace, format
and strict documentation/local-link checks. Each combined run passes 332 unit
tests, eight integration tests and one doctest; five hardware units and the integration
subprocess fixture are marked ignored in the parent runner. Source and tool hashes are recorded in
`source-manifest.json` and `build-environment.json`.

`independent-review/source-review-e2f4037.json` selects 21 independently rerun
gates, using a fresh stable target and the final-source MSRV artifacts. An older
cross-archive target reused a stale stable binary; its results are excluded and
preserved. The reviewer audits primary evidence and verifies all 281 files in
`blocking-finish-control-e2f4037/`: only the finish progress body changes to the
synchronous protocol. Both unchanged failure-observation and SEND-stop tests
then fail their two-second assertions before the ten-second request deadline.
The earlier CUDA-only warning and POST/next-PRE priority regression are preserved
with their failed runs; the final source passes the unchanged panic/effect test.
GitHub CI also passes 8/8 profiles, the frozen TENT CPU comparison and docs at
this implementation revision; see `ci-source-e2f4037.json`.

At this finish revision, Pin, native setup, nonreusable QP retirement/copyback,
cache fallback retirement, failure drain and source handlers retained synchronous
boundaries; setup socket progress is covered by the subsequent revision above. Provider calls
and locks are not preemptible. No new hardware task is requested; installed v21,
real DMA overlap and performance remain unqualified. Frozen TENT correspondence
and the exact owner boundaries are documented in
[the staging contract](staging.md#native-rdma-finish-progress).

## Connection and Pin lifetime

The current v21 source retains one nonblocking staging connection attempt through
success, stop or the original deadline. Typed task stop reaches the subsequent
Pin and native handshake socket waits. Pin validates the full header and success
tail before exposing a chunk. Source stop retains its distinct error identity.
Cancellation does not bypass native abort, drain or uncertain-owner retention.

CPU controls use real loopback sockets: a saturated accept queue holds a pending
connect, fragmented Pin replies remain withheld through requester terminal,
source errors provide no success tail, and invalid negotiation never starts
CROSS. Internal staging worker stop and the public request deadline are tested
separately. Native setup and finish controls combine partial socket replies with
accepted CROSS work, successful drain and injected QP cleanup failure. The
actual local GPU shim owner is retained when native cleanup is unproven.

Evidence is selected outside Git at
`/root/orbitte-artifacts/staging-connect-progress-20261030/`.
At `a2d1b9e`, stable and Rust 1.85 pass all four feature profiles' tests and
Clippy. Each combined run passes 329 unit tests, eight integration tests and one
doctest with two CPUs. Both relay gates, the 25-observation Rust CPU trace and
strict docs/local-link checks pass. `gates.json` records 22 primary gates;
`source-manifest.json` and `build-environment.json` bind the source and tools.

Independent source review accepts `a2d1b9e` for this source/CPU boundary.
`independent-review/source-review-a2d1b9e.json` records the inspected diff and 21
independently rerun feature/compiler/relay/format/docs gates. The reviewer also
audits the primary CPU trace and negative controls. The unchanged controller
suite is not repeated locally; GitHub CI executes its existing CPU gate.
`stop-ignorant-control/manifest.json` records a fresh archive with only the
channel's task stop removed. The unchanged partial-Pin and held-WR setup tests
both fail their two-second stop assertion while the original request deadline
is ten seconds. The independently checked archive and per-file hashes confirm
the unique mutation and unchanged tests.

The new Pin profiles cover READ and WRITE; the combined native socket stop and
cleanup-failure profiles cover READ setup and finish. Existing WRITE-effect
regressions pass separately. Source EOF cleanup remains withheld until after
requester terminal, so the tests distinguish requester retirement from remote
loan release. This does not qualify installed v21 execution, real DMA overlap
or performance; hardware tasks remain paused. At that revision Pin and native
handshakes still occupy their session worker and native/CQ waits remain synchronous;
requester finish is advanced by the [subsequent boundary](#native-rdma-finish-progress), and public active cancellation and engine close order
are unchanged. Full asynchronous control progress, location selection and full
ProxyManager/API/wire parity remain open. Frozen TENT likewise pins remote stage
buffers synchronously before using them in its chunk event loop.

## Socket progress

The current v21 source adds a concrete TcpTask shared by ordinary TCP and remote
staging. It owns partial request/reply/payload/ACK positions, a bounded snapshot
and the actual local registration. Scoped control operations preserve partial
receipts; each session has one framing owner and never interleaves protocol bytes.
TCP CROSS keeps outer header, inner transfer and final scoped receipt distinct.
Staging local failure/stop can cancel pending socket I/O before its original
deadline, then joins native drain and task destruction before owner retirement.

CPU controls cover byte-fragmented replies, READ guards and EOF, real WRITE
backpressure with snapshot/registration retention, and a peer withholding a
partial Delegate reply or TCP payload through local failure and worker exit.
The latter checks exact partial bytes, missing Unpin, healthy drain and retained
GPU owners when context restoration is uncertain. Existing shared-QP/native
cleanup failures and both feature families remain required regression gates.

Evidence is outside Git at
`/root/orbitte-artifacts/staging-control-progress-20261030/`.
At `736a16a`, stable and Rust 1.85 each pass default, RDMA-only, CUDA-only and
combined tests and Clippy. Each combined run passes 322 unit tests, eight
integration tests and one doctest with two CPUs. Both compiler relay gates,
all 51 CPU controller controls, the 25-observation Rust trace fixture and strict
docs/local-link checks pass. `gates.json` records all 23 primary gates;
`source-manifest.json` binds them to the frozen source.

Independent source review accepts `736a16a` for this source/CPU scope.
`independent-review/source-review-736a16a.json` records the diff and 21 independently
rerun feature/compiler/relay/format/docs gates. Controller and trace results
are audited primary evidence, not additional independent executions.
`blocking-socket-control/manifest.json` records the same public cancellation
control failing as expected when only `poll_read` is replaced with bounded
blocking `read_exact`. The reviewer verifies the unique change and matching
archive, test and log hashes; this negative control is an audited primary run.
The new stalled-socket profiles cover TCP framing plus local CUDA failure;
existing native acceptance and failed-QP cleanup controls pass separately. No
new combined partial-control/held-native-WR cleanup-failure profile is claimed.
This is source/CPU work only: current installed v21 execution, DMA overlap and
performance remain unqualified; hardware requests remain paused. At this source
boundary Connect/Pin and native handshake waits had no task stop token; see
[connection lifetime](#connection-and-pin-lifetime) for the subsequent change.
Pin, native RDMA setup/finish, source handlers and ordinary local GPU
snapshot/commit remain synchronous within their workers. Ordinary active batch cancellation, readiness notification,
location selection and full ProxyManager/API/wire parity remain outside this
substage. Frozen TENT's ordinary TcpTask uses synchronous workers. Persistent socket lanes are documented in [HP-TCP](hp-tcp.md).

## Remote Delegate progress

The source accepted at `3767dc5` submits CUDA Delegate work independently of completion at every
remote ring width. Scoped QueryDelegate results gate READ CROSS, WRITE slot
reuse and final Unpin, while other submitted native CROSS owners can progress.
This corresponds to frozen TENT's `submitRemoteStage`, `INFLIGHT_REMOTE` and
`tryTakeResult` ownership. Socket exchanges, Pin and TCP payload were synchronous
at that revision; the current bounded socket boundary is recorded
[above](#socket-progress).

CPU controls hold a source PRE/POST NOT_READY while another native CROSS
finishes, verify bytes and guards with unequal rings and a short final chunk,
and reject early, duplicate or mismatched queries before touching CUDA. Query
and context-restoration failures compose with held native work and failed QP
cleanup, retaining actual GPU/pin owners and admission. Lost terminal queries
and Unpin receipts cannot report success. Existing cancellation and source
sibling-drain controls use the new completion boundary.

Evidence is outside Git at
`/root/orbitte-artifacts/remote-operation-progress-20261030/`.
Stable and Rust 1.85 combined runs at `2d3a6a3` each pass 318 unit tests,
eight integration tests and one doctest. CUDA-only tests and Clippy, combined
Clippy, the v21 relay, all 51 controller controls and strict docs pass;
`gates.json` and `source-manifest.json` bind those 13 gates to the source.
The test-only change at `3767dc5` waits for an accepted WR before injecting
cancellation, removing a preparation-versus-posting race without weakening
byte, cleanup or owner assertions. `incremental-gates.json` and `incremental-source-manifest.json` record both
compiler checks and the unchanged production source.

Independent source review accepts `3767dc5` for this source/CPU ownership
scope. `independent-review/source-review-3767dc5.json` binds the frozen diff,
13 independently rerun feature/compiler/relay/docs gates and the final test
correction. The cancellation control passes 20/20 accepted two-CPU repeats:
ten stable and ten freshly compiled MSRV runs. Earlier MSRV repetitions whose
rebuild freshness was not established are retained but excluded. Failed development runs are
also retained with their corrected gates.

`synchronous-query-control/manifest.json` records the same public progress
control failing as expected when only source event query is replaced by a
blocking wait. The reviewer verifies that unique source change and the archive,
test and execution hashes; this is an independently audited primary execution,
not a second execution of the negative control.

Current v21 installed execution, DMA overlap and performance remain unqualified;
hardware requests remain paused. Earlier installed profiles stay bound to their
original revisions. At `3767dc5`, asynchronous TCP/control progress, location
selection and full ProxyManager parity remained open. See
[socket progress](#socket-progress) for the subsequent implementation and its
remaining blocking boundaries.

## Concurrent RDMA CROSS

The v20 source separates RDMA submission, payload query and bilateral finish.
One staging connection admits multiple CROSS chunks within a separately
negotiated native window. Each concrete `RdmaTransfer` owns its slices and
registration and uses the existing transport CQ workers. Admission remains
ordered; completion may reorder across remote slots. A scoped `FinishCross`
selects the original SEND/RQ/ACK/READY and cleanup exchange before terminal
publication. Source Bind completion cannot substitute for payload completion.
This corresponds to frozen TENT's `submitCrossStage` / `progressBatch` split.

Per-CROSS slice caps account for the whole concurrent window while existing
grant, receive and byte credits still charge actual work. The public-path CPU
control uses L/R=3/2, two slices per CROSS, one physical QP and a short final chunk.
It holds one exact payload CQE from the first private MR while the second CROSS completes, checks exact
bytes/guards and verifies the first still owns its native credits and GPU range.
A second control fails the later CROSS with an earlier payload pending; all
owners must drain, or failed QP cleanup must preserve `Unquiesced` and actual
pins, GPU owners and parent admission. A further control expires the first
CROSS after the second has already changed bytes: READ preserves the exact
noncontiguous POST range, WRITE keeps `RemoteEffect::Unknown`, and uncertain
native cleanup retains the actual owners. Source scope controls reject early,
duplicate and stale FinishCross and premature Unpin. Existing CUDA failure
controls check actual submitted ranges because completed READ chunks need not
form a prefix.

Evidence is outside Git at
`/root/orbitte-artifacts/staging-concurrent-cross-20261029/`.
Production gates at `9280d1f` pass stable/MSRV tests and Clippy, CUDA-only
checks, all 51 controller controls, the CPU trace fixture and strict docs.
The final source at `adc803f` adds test-only corrections and the partial-effects
regression. Stable (restricted to two CPUs) and Rust 1.85 combined runs each pass
314 unit tests, eight integration tests and one doctest; combined Clippy and
format checks pass. `gates.json`, `final-gates.json`,
`final-correction-gates.json` and the source manifests
bind the commands, revisions and unchanged production files.

`serialized-current-control/manifest.json` records a clean `665a32c` archive
with only the CROSS window forced to one; the same healthy progress control
fails as expected. The final test-only change normalizes an equivalent Boolean
predicate for MSRV Clippy; the original warning and corrected gate are retained.
Independent source review accepts `adc803f` for this source/CPU ownership
scope. `independent-review/source-review-adc803f.json` binds the frozen diff,
feature/compiler gates and final incremental checks. The corrected fault
controls pass 20/20 independent two-CPU repeats. Earlier repeated controls
include failures before the first payload was accepted; only the corrected
run is accepted. Failed development and CI runs are retained separately. The first fault shim used post-call ordinals, which could
hold all CQ credits or select a slice refill; it now binds an actual private MR
and a single accepted WR. The second MR's one error CQE is delivered only after
the first MR owns an accepted WR; other completions can still return credits.
This also removes the assumption that preparation order implies posting order.
Refill fault injection waits for all initial CUDA event records. Native-failure checks require a consumed error CQE while allowing the
caller to observe either the original error or the asynchronously retired lane.
These corrections retain the exact byte, drain and uncertain-owner assertions.
Hardware requests remain paused: installed v20 execution, DMA overlap and performance are unqualified.
TCP payload, control exchanges and source CUDA preparation retain their
synchronous boundaries; general remote-operation progress, asynchronous TCP
payload, location selection and full ProxyManager parity remain open.

## Requester chunk event loop

The accepted v19 requester at `1ac1d01` uses a bounded PRE/CROSS/POST event loop. Each local
slot queries its recorded CUDA completion and advances independently; ordered
CROSS commands retain the existing connection protocol. NOT_READY keeps the
same generation and physical owners. Query success requires context restoration
before slot reuse or completion accounting. Query errors quarantine the slot;
error cleanup drains all other accepted copies and preserves native uncertainty.
Source phase handling, shared shard session ownership and wire v19 are unchanged.

Public TCP/RDMA READ/WRITE controls hold one chunk's native completion and prove
that another slot refills beyond the former window barrier. They cover first
and reused event generations, multiple ring turns, short tails, exact bytes,
guards, range loans and shutdown. The same control run against the old requester
window loop fails at the expected barrier. Additional controls cover NOT_READY
followed by context restore failure, first/middle/final query failure, submit and
record recovery, final POST deadline and native cleanup retention. Existing
source/error-drain event-synchronize failure controls remain in the suite.

Evidence is outside Git at
`/root/orbitte-artifacts/staging-chunk-progress-20261029/`.
`old-window-final.json` binds the final source and expected negative result
from a fresh build with only the requester window loop restored to its old form.
The native observer forwards query entry/return codes;
its ledger preserves NOT_READY generations and rejects stale, repeated, foreign
or prematurely released completion owners. Slot submissions may reorder within
an exact payload phase; private setup/readback and actual generation ownership
remain strict.

Stable and Rust 1.85 combined runs each pass 311 unit tests, eight integration
tests and one doctest. CUDA-only tests/Clippy, combined Clippy, all 51 controller
controls, the CPU trace fixture and strict documentation checks pass;
`gates.json` binds these 11 gates to `1ac1d01`. Independent source review accepts
that commit for the source/CPU ownership scope. Fresh independent build trees
pass stable/MSRV tests and Clippy for both CUDA and RDMA+CUDA;
`independent-review/` records the frozen source, diff, gate logs and review.
Earlier failed assertions, the sampling-race control and a stale reviewer build
attempt are preserved and excluded from the accepted results.

No new hardware task is requested. Installed execution, physical overlap and
performance remain unqualified for that v19 source. The concurrent RDMA CROSS
substage above extends its connection progress; general remote-operation
scheduling, location selection and full ProxyManager parity remain open.

## Shared staging worker shards

The accepted v19 source at `3db26ee` replaces per-session thread creation with a lazily started
pool of at most eight shared shards. An idle shard is reused; queued tasks have
bounded admission and retain their original deadline. Pin starts after dequeue.
Queued cancellation destroys the actual task captures before its terminal receipt;
active work continues through the existing transport drain boundary. Independent
terminal delivery and caller unwind ordering prevent early private MR retirement.
Task panic retains uncertain owners while leaving the shard available.

CPU controls cover eight active sessions plus 512 queued tasks, queue exhaustion,
pre-start timeout/cancellation, shutdown timeout and retry, concurrent lazy
admission/close, reuse after panic, idle-thread Drop and caller unwind with a full
completion queue. A public-path control holds eight source CUDA completions and
expires a ninth queued request without remote Pin, then verifies exact bytes,
guards and thread reuse over TCP and RDMA. These are CPU ownership controls.

Evidence is outside Git at
`/root/orbitte-artifacts/staging-worker-shards-20261029/`.
Stable and Rust 1.85 combined runs each pass 309 unit tests, eight integration
tests and one doctest. CUDA-only tests/Clippy, combined Clippy, all 50 controller
controls, the CPU trace fixture and strict documentation checks pass. `gates.json`
records these commands and results. The original single-QP/eight-byte fixture
rejected the concurrency control with `ResourceExhausted`; that failed control
is preserved, and the accepted control uses the existing default RDMA budgets.
Independent source review accepts `3db26ee` for this source/CPU ownership scope.
`independent-review/source-review-3db26ee.json` binds the frozen source, diff
and 16 passing independent gates. [CI](https://github.com/OrbitInfra/OrbiTE/actions/runs/40835594882)
passes all eight feature/compiler combinations; the
[frozen TENT CPU comparison](https://github.com/OrbitInfra/OrbiTE/actions/runs/40835594607)
and [documentation checks](https://github.com/OrbitInfra/OrbiTE/actions/runs/40835594687)
also pass for that source. No new hardware task is requested.
Installed v19 execution, physical overlap and performance remain unqualified.
The requester event-loop substage above supersedes the local window barrier.
The concurrent RDMA CROSS substage above extends connection progress; shared
threads alone do not establish complete ProxyManager parity.

## Bounded session progress

The accepted v19 source at `4441017` adds bounded CROSS command/completion queues and one
joined connection worker per admitted staging request. Local CUDA progress can
continue while a CROSS or Delegate receipt is pending. CROSS remains ordered on
one connection. The shared-shard substage above supersedes its per-session worker;
the concurrent RDMA CROSS substage above extends its serialized native work. See [the ownership contract](staging.md#session-progress-and-completion).

CPU controls hold a local CUDA event while CROSS progresses, hold a source
Delegate receipt while local PRE/POST starts, and lose the final Delegate reply
after every CROSS completed. A controlled local submit failure cancels queued
CROSS commands while the active command drains; combined RDMA teardown failure
must override the ordinary local error and retain the actual pin/context owner.
A pure Rust worker panic during first READ or first/second WRITE grant cleanup
retains the actual GPU backing, all private pins and parent admission, with
conservative WRITE uncertainty and exact already-written bytes. Unequal rings,
tails, self-peer admission and final POST drain controls remain.

Evidence is stored outside Git at
`/root/orbitte-artifacts/async-cross-20261029/`.
Independent review accepts the production source contract at
[`4441017`](https://github.com/OrbitInfra/OrbiTE/commit/444101700732e33098329962d689aff82fac2b3c).
Stable and Rust 1.85 combined runs with the integrated panic regression each pass
303 unit tests, eight integration tests and one doctest; CUDA-only tests/Clippy,
combined Clippy, all 50 hardware controller controls, the CPU trace fixture and
strict documentation checks pass. `gates.json` and `integrated-gates.json`
preserve commands and outcomes, including the corrected CUDA-only Clippy
diagnostic. `independent-review/source-review-4441017.json` binds 16 independent
source gates; `worker-panic-control.json` in that directory binds the additional
reviewer control now included in the permanent tests. This accepts the source
substage; its remaining scheduling work is recorded in the shared-shard boundary above.
No new Forge task or hardware execution is requested. Current installed
qualification and performance remain unqualified; earlier H20 results retain
their original artifact boundaries.

## Requester CUDA pipeline

The accepted wire-v19 implementation at `c87c403` adds an independent bounded requester ring
to the accepted source pipeline. CUDA WRITE prequeues local PREs; READ can keep
POSTs pending until local slot reuse. Every final local POST drains before
Unpin. The request-local `StageBufferCache` retains private registrations, and
`PinnedStage` owns each accepted copy and one-time completion. Width and budget
formulas are in [the staging contract](staging.md#bounded-remote-pipeline).

Source controls cover unequal local/remote widths, multiple ring turns and tails,
pending copies during CROSS, opposite-direction sessions, shared-pool exhaustion,
first/middle/final native wait failures, deadline expiry during final POST and
lost Unpin. Refill submit/record failures drain accepted siblings while preserving
partial READ bytes and WRITE uncertainty. Context restoration and private MR
retirement failures retain actual owners and admissions after ordinary references
drop; MR controls cover both sides, directions and depths one and three.

Independent review accepts the source contract at
[`c87c403`](https://github.com/OrbitInfra/OrbiTE/commit/c87c403fd021872c3bd68fde55078bd6bdead9bb),
whose production and installed-consumer inputs are unchanged from `027efad`.
Stable and Rust 1.85 combined runs each pass 299 unit tests, eight integration
tests and one doctest; all-target Clippy, 50 hardware-controller controls and
strict documentation checks pass. The reviewer independently repeats the
applicable feature gates and the integrated failure controls. The existing IPC
timeout control now waits for actual server closure instead of a fixed sleep.

Evidence stays outside Git at
`/root/orbitte-artifacts/requester-pipeline-20261029/`.
`independent-review/source-review-c87c403.json` binds the frozen source, original
and incremental review gates, and additional failure controls. This accepts the
source substage; it does not close P5.5b.

Hardware execution is deferred at the user's direction; no new task is requested.
The controller and consumer now account for each ring's own width, private MRs,
persistent stream/events and physical pinned bytes. Neither these CPU controls
nor earlier v18 hardware runs qualify the current artifact or demonstrate
physical overlap or throughput. The session progress and shared-shard substages
above extend this source boundary. The chunk event-loop substage supersedes its
local window barrier; concurrent RDMA CROSS is covered above and location
selection remains open.

## Bounded requester CUDA staging

The prior v19 substage replaces the full-request requester payload with
one reusable private slot. Pin negotiates the minimum chunk size; actual pinned
budgets remain per-engine physical sizes. CUDA range loans permit disjoint
sessions and reject overlap; completed READ chunks remain visible on later
failure. Source controls cover asymmetric chunks/tails, simultaneous opposite
directions, shared-pool exhaustion, IPC/RDMA interlocks and sticky native failure.

Independent review accepts the source contract at
[`460d825`](https://github.com/OrbitInfra/OrbiTE/commit/460d825d6438fc70883e7acd61a83d099406b3e7),
whose production and installed-consumer inputs are unchanged from `fc183ab`.
Stable and Rust 1.85 combined runs each pass 294 unit tests, eight integration
tests and one doctest. Targeted Clippy, wire-v19 relay, all 50 hardware-controller
controls and strict documentation checks pass. The reviewer independently
repeats the applicable gates and verifies both clean installed packages.
This accepts source and package provenance, not installed execution.

Evidence remains outside Git at
`/root/orbitte-artifacts/proxy-requester-20261029/`.
`independent-review/source-review-460d825.json` binds the source gates, review and
installed manifests. `frozen-cases.json` defines 12 healthy cases per compiler:
TCP/RDMA, CPU/GPU requesters, both host directions, asymmetric physical chunk
sizes, short tails and source widths up to 16.

Installed v19 qualification remains pending. Earlier v18 pipeline evidence below
remains bound to its original artifact. The new healthy profile must verify exact
requester slot/MR reuse, matching copy/event generations, physical budgets, both
directions, byte guards and cleanup on fresh nodes. The requester ring above
supersedes this one-slot implementation; neither substage has installed v19
hardware qualification. Broader location scheduling and performance remain open.

## Bounded remote CUDA pipeline

Clean stable 1.99.1 and Rust 1.85.0 installed consumers from
[`c0f6a9e`](https://github.com/OrbitInfra/OrbiTE/commit/c0f6a9e3517f7b6999b1238e652fb7db77b96d43)
pass 24 cases on two fresh physical H20 hosts. Each compiler covers TCP/RDMA,
CPU/GPU requesters, both host directions, asymmetric caps, negotiated widths
1/2/3/4/16 and a cap shortened by the request's chunk count. Main pipeline
cases turn the ring more than three times and include a short tail. Each case
performs three READs and three WRITEs with full-buffer and guard comparisons,
one source handler and one parent admission slot. Reverse RDMA cases use CQ events.
Four fresh independent repeats cover both compilers, transports, host directions
and local memory kinds, including the 16-slot RDMA case. They add 24 sessions,
696 Delegate/CROSS operations each and 1,296 payload copies on 38 pool
allocations/frees. Independent review accepts source and this healthy pipeline
profile; it does not complete P5.5b.

The passive CUDA ledger matches each stream/event record generation to live
allocations and requires event wait plus context restoration before slot reuse
or cleanup entry. It checks setup/readback as well as payload copies, allows
out-of-order completion across slots, and verifies persistent per-slot handles
across sessions. The primary matrix records 144 sessions, 2,040 Delegate/CROSS
operations each and 3,432 payload chunk copies on 120 pool allocations/frees.
Afterward, all supervised processes are gone, `nvidia-smi` reports 0 MiB memory
used on all 16 GPUs and no compute processes, locks are reacquirable, and the
selected mlx5_3 context/PD/CQ/QP/MR identity sets match their original baselines.

The unchanged 16-slot RDMA case performs 300 CROSS operations. It exposed
native MW index reuse exhausting the low-byte key tags. The PD now retains an
exhausted, never-bound MW while allocating another index; key history remains
intact and bounded. Both compiler runs show 300 binds/invalidations, one index
rollover and zero final live/retained windows. This is OrbiTE's authorization
lifetime mechanism; it is not a claim that TENT uses the same key policy.
Accepted control sockets also disable Nagle. CROSS setup errors now use the
nested transport epoch; a socket close may still precede delivery of the error.

Evidence remains outside Git at
`/root/orbitte-artifacts/proxy-pipeline-qualification-20261028/`.
`primary-matrix.json` binds the two `installed-qualified-*` manifests, native
observers, `runs-qualified-*` cases, runtime provider hashes and cleanup audit.
`independent-review/source-review-c0f6a9e.json` records source review and additional
partial-allocation/cleanup and partial-WRITE controls.
`independent-review/live-c0f6a9e/hardware-review.json` binds all 24 primary and four
fresh review cases to their installed artifacts. Its sibling `runs-stable/`,
`runs-msrv/` and before/after inventories preserve independent execution and
cleanup evidence. Final postflight covers primary, review and earlier failed
runs; `safe-to-release.json` records that the reserved resources can be returned.
The earlier timeout,
key-exhaustion and diagnostic failures remain in `runs-stable/`,
`nodelay-control/` and `diagnostic-control/`; their cleanup is audited too.
The CI socket-close race and repeated corrected controls are preserved in
`ci-c0f6a9e-stable-combined.log` and `cross-error-test-race.log`.
Original pipeline source controls remain in
`/root/orbitte-artifacts/proxy-pipeline-20261028/`.

This qualifies healthy wire-v18 remote staging on one RoCEv2 rail per host.
RDMA retirement uses real-provider engine counters, without passive per-WR/CQE
or deregistration-order receipts. Native CUDA receipts establish lifetime and
completion, not physical execution overlap or throughput. Physical faults,
InfiniBand, requester-side bounded staging, broader asynchronous scheduling and
C++/Rust wire interoperability remain open. Earlier serial v17 evidence retains
its original artifact boundary. See [the contract and reproduction](staging.md#installed-pipeline-validation).

## Serial remote CUDA staging

The opt-in serial P5.5b session is implemented with CPU controls for public
TCP/RDMA execution, byte guards, one handler/batch/chunk, source permissions,
scoped phase ordering, CUDA/MR failure retention, self-peer/opposite-direction
operations and loss of the final Unpin reply. Independent review found that
nonce-only replies could accept a repeated prior receipt as final completion;
success replies now echo the full operation and range. The retained failing
control and current source-gate evidence live outside Git in
`/root/orbitte-artifacts/proxy-remote-20261028/`. Independent review accepts source
correctness at [`64c9899`](https://github.com/OrbitInfra/OrbiTE/commit/64c9899fa088898b01000fe87ccb924bed356c32),
including a fresh real TCP relay control that repeats the previous phase receipt
while withholding Unpin. Both directions reject early completion. The review
record is `independent-review/final-source-review.json` in that artifact root.
Production code is unchanged in the clean installed consumers from
[`9f2ad39`](https://github.com/OrbitInfra/OrbiTE/commit/9f2ad3967ee9d12f16c13a87050c41a71268e567).
The consumer now waits for its first submission to drain before reusing the
single batch admission slot. An earlier run correctly returned
`ResourceExhausted` when the fixture overlapped two submissions; that failed
attempt is retained. The CUDA validator now binds all asynchronous copies,
including setup/readback, to the live GPU and pinned host allocations, expected
slice offsets, event wait and context restoration before reuse or free.

Stable 1.99.1 and Rust 1.85.0 consumers pass 16 installed cases on two physical
H20 hosts. Each compiler covers remote GPU/local CPU and GPU/GPU, TCP and staged
RDMA, and READ/WRITE in both host orientations. The first orientation uses
257-byte chunks with 1,001-byte requests; the reverse uses 65,536-byte chunks
with 524,304-byte requests. Both have short tails. RDMA uses one RoCEv2 rail,
with polling in the first orientation and CQ events in the reverse. Each peer
has one handler and one parent batch slot. Each GPU peer uses one pinned chunk;
a CPU requester uses none.

The source counters record 96 completed Pin/Unpin sessions and 624 Delegate/CROSS
operations each. Native CUDA receipts bind 936 payload chunk copies to 24 actual
pool allocations and successful frees. Full buffers and guards match. The
runtime maps and provider hashes bind compatibility `libcuda.so.590.48.01` on
R535.161.08, `libibverbs` and `libmlx5`. After all runs, test/supervisor PIDs are
gone, `nvidia-smi` reports 0 MiB and no compute processes for all 16 GPUs, and the original
GPU/RNIC locks are reacquirable. The 48 Python controls include wrong GPU offsets
and cross-thread frees before setup/readback completion.

Four fresh independent repeats reverse the primary size/orientation pairing,
covering both compilers, transports and local memory kinds. They add 24 sessions,
156 Delegate/CROSS operations each and 234 chunk copies on six actual pool
allocations/frees. Independent postflight confirms process exit, zero GPU use,
reacquired locks and the original selected-device context/PD/CQ/QP/MR identity
sets. Background QP packet counters may naturally advance. Independent review
accepts source and this bounded healthy serial profile.

Evidence is outside Git at
`/root/orbitte-artifacts/proxy-remote-qualification-20261028/`.
`primary-matrix.json` binds the two clean packages, binaries and observers to the
profile, all case records, loaded libraries and post-run cleanup. `runs-stable/`
retains the failed admission fixture; `runs-final-stable/` and `runs-final-msrv/`
contain the successful final runs. Source review remains in the previous artifact
root above; validator review is under `independent-review/frozen-9f2ad39/`.
`independent-review/hardware-evidence-review.json` independently binds all 20
raw case records to the exact installed artifacts and runtime providers; fresh
runs are under `independent-review/live-repeat-stable-1/` and
`independent-review/live-repeat-msrv-1/`; profiles, commands and before/after
inventories are under `independent-review/live-repeat-plan/`.

This qualifies the declared healthy serial profile. RDMA completion and private
MR retirement are checked through the real provider's engine counters and source
ownership checks, without passive per-WR/CQE or deregistration-order receipts.
The new pipeline has [separate v18 evidence](#bounded-remote-cuda-pipeline).
Broader remote scheduling, physical GPU/RNIC faults, performance and mixed
C++/Rust wire interoperability remain open. See
[the contract and reproduction](staging.md#serial-remote-staging).

## Local CUDA staging chunks

Production source [`37e99ae`](https://github.com/OrbitInfra/OrbiTE/commit/37e99ae7bb5f2415a0a4fdd3f77bc25c9ffea477)
introduces one engine-owned `ProxyManager` pool shared by TCP and staged RDMA.
Clean installed consumers from
[`e44a6d1`](https://github.com/OrbitInfra/OrbiTE/commit/e44a6d117a18f2770984f0da41bb2419dd6782cf)
include corrected fixture accounting and native-ledger lifetime validation;
production code is unchanged between those commits.

Stable 1.99.1 and Rust 1.85.0 consumers pass 24 cases across two physical H20
hosts. Each compiler covers TCP and staged RDMA, GPU/host, host/GPU and GPU/GPU,
with both source/requester orientations. READ and WRITE each contain three
logical requests. The smaller profile uses 257-byte chunks and 1,001-byte
requests; the reverse profile uses 65,536-byte chunks and 524,304-byte requests.
Both end in a short chunk. RDMA uses one physical RoCEv2 RNIC, polling on the
first orientation and CQ events on the reverse orientation. The runtime maps
compatibility `libcuda.so.590.48.01` on R535.161.08.

The primary native ledgers bind 32 persistent chunk allocations to 160 reuses,
1,248 chunk copies and 32 successful frees. Every copy is bounded by its actual
pin/free lifetime and matched stream, event wait and caller-context restoration.
CPU peers consume no CUDA chunks. Both peers compare full buffers and guards;
all supervised children are reaped and resource locks released. These are
Driver API correctness results, without a performance claim.

Independent source gates pass the stable four-feature matrix and MSRV combined
Clippy/tests: combined features have 273 unit tests, eight integration tests
and one doctest, with six explicit hardware/subprocess skips. Source controls
cover held events, capacity, cancellation, partial deadline effects, native
context replacement and failed pin/copy/free cleanup. The public source and
requester tests also reproduce and reject early admission release when CUDA
`Unquiesced` coincides with an ordinary RDMA CQ cleanup error. The 47 Python
controller controls include wrong stream/event, copy-after-free and double-free
negative controls.

Four fresh independent repeats pass across both hosts and compilers, adding
six GPU peers, 264 chunk copies and six native chunk frees. The independent
ledger audit also binds every payload copy to the actual GPU allocation and
expected slice offsets. Post-run checks find all test and supervisor PIDs gone,
all 16 GPUs at zero allocation with no compute processes, and GPU/RNIC locks
reacquirable. Independent review accepts source and this bounded healthy profile.

Evidence remains outside Git at
`/root/orbitte-artifacts/proxy-staging-20261028/`. `primary-matrix.json` binds
the two clean package, binary and observer hashes to all 24 case records and
the exact profile. `installed-final-stable/`, `installed-final-msrv/` and
`independent-review/` retain packages, raw ledgers, reviews and controls.
`independent-review/frozen-e44a6d1/` contains the fresh repeats, independent
`raw-ledger-audit.json`, package bindings and both post-run inventories.
The first stable run passed its six forward cases, then rejected an invalid
reverse-TCP event-mode profile before launching the next case. Its failed
manifest is preserved; the six corrected reverse cases are separate results.
The earlier fixture-count failure and missing-toolchain-component build failure
also remain preserved, without being counted as passes.

This profile uses one chunk per engine and one GPU per host. Default-capacity
stress, physical CUDA failures and broader context scheduling remain
unqualified. Cross-transport reuse, capacity exhaustion and failed native
cleanup are covered by CPU controls. Remote Delegate/Pin/Unpin, nested
admission and PRE/CROSS/POST pipelines remain P5.5b; this local subset does not
complete P5 or full TENT ProxyManager parity. See [local staging](staging.md).

## Bounded cross-submission CUDA IPC cache

Source [`7da43b2`](https://github.com/OrbitInfra/OrbiTE/commit/7da43b21db8fbee69018830228deaab3f5f13206)
adds opt-in transport-owned imports across submissions. Concurrent first users
share one bounded entry; each reuse rechecks source authority. A separate export
owner holds the actual registration, whole allocation and physical charge until
native import close and its source receipt. Completed local requester owners are
released after each operation's event drain and context restoration.

Clean packaged consumers on stable 1.99.1 and Rust 1.85 exercise the same local
GPU, owned pinned host and mixed groups on two physical H20 hosts with NV18
connections. The 34-case matrix includes 24 cached healthy cases (17 and
2,097,171 bytes, both device directions), four source-close waits, four missing
import-close cases after requester exit, and two default subbatch regressions.
Healthy cache cases submit four concurrent three-slice READs, then a three-slice
WRITE. Fifteen payload copies must use exactly one native import/export and one
final close. Healthy/graceful cases compare logical bytes, gaps and guards.
The runtime uses compatibility `libcuda.so.590.48.01` on R535.161.08; this is
Driver API correctness evidence, not a toolkit/kernel or bandwidth result.

All 34 cases pass: 36 exports/imports, 396 payload copies (228 HtoD/DtoH),
32 native closes and 38 persistent pinned requester owners. The four missing
closes are the declared requester-exit cases. Every child is reaped, GPU UUID
locks are released and selected GPU allocation baselines are restored.
Independent review passes stable four-feature and MSRV combined source gates,
then the stable/MSRV cache and complete RDMA relay tests after test-only fixes.
The final combined gate has 265 unit tests, eight integration tests and one
doctest; six hardware/subprocess fixtures remain ignored in the CPU gate.
All eight toolchain/feature CI jobs, the frozen TENT CPU comparison, 45 controller
controls and strict documentation/link checks pass. The strengthened validator
rejects the independent readback counterexample and accepts its legitimate
control; final raw ledgers are revalidated without changing installed code.

Four fresh independent repeats cover host healthy, mixed source-close, mixed
requester exit and device healthy across both hosts, compilers and directions.
They record four exports/imports, 36 payload copies (19 host), three native
closes and four persistent pinned owners. Independent post-run checks find all
primary/review child PIDs gone, all 16 GPUs at zero allocation with no compute
processes, and all selected UUID locks reacquirable. Independent review accepts
source and this bounded fixed-working-set profile.

Evidence stays outside Git at
`/root/orbitte-artifacts/cuda-ipc-cache-20261028/`. `final-evidence/` binds
the two clean `7da43b2` installed packages to validator `b4a9400`, the frozen
matrix, `native-audit.json`, host/boot/GPU identities, driver hashes and per-case
raw logs. `independent-review/hardware-b4a9400/` preserves the fresh repeats,
cleanup audit and review. Earlier
`85df496` cases and original review counterexamples remain preserved separately.
The test-only validator fix rejects a missing payload context restoration that
previously borrowed a later owned-device readback's pop; it also bounds event
and wait matching before the next native copy. Source controls cover source-side
capacity, concurrent cold opens, deadlines, unregister/permission/admission,
missing receipts, late first offers, one-shot cleanup failure and actual source
allocation/registration retention after engine drop.

This profile supports a fixed working set until requester shutdown. Both peers
must enable `nvlink_cache_entries`; default zero keeps per-subbatch close. Source
allocations remain exclusively loaned throughout the cached lifetime, and one
allocation supports one live export token/context. That installed artifact has
no eviction or continuing flush; the later [reclamation source stage](#cuda-ipc-cache-reclamation)
does not extend its hardware qualification. Requester exit is injected after event drain and before native
close: it proves exporter retention, without final source payload readback or
in-flight GPU termination. Live trusted exporters are required. External/managed allocations,
broader stream scheduling, MNNVL/VMM and native GPU cleanup faults retain their
existing limits. This substage does not establish complete P5 or TENT parity.

## Local pinned host CUDA IPC

Source [`9adf5a5`](https://github.com/OrbitInfra/OrbiTE/commit/9adf5a5eabb8bb5b8dce312ba10d77275da75ab1)
qualifies local owned pinned host READ/WRITE against remote owned GPU exports,
including mixed host/GPU requester groups on one native context. It follows
frozen TENT's local DRAM/GPU and remote CUDA-only distinction. Ordinary/external
host backing and remote host exports are outside this ownership profile.

Clean packaged consumers built with stable 1.99.1 and Rust 1.85 pass 30 cases
on two physical H20 hosts with NV18 topology, using compatibility
`libcuda.so.590.48.01` on R535.161.08. Sixteen host READ/WRITE cases cover both
GPU-context directions and 17 / 2,097,171-byte payloads. Four mixed healthy,
four host source-close waits and four mixed requester-exit cases use 4,133 bytes;
two GPU/GPU cases retain the earlier behavior. Healthy/graceful cases compare
full logical bytes, gaps and guards. Requester exit occurs after native event
drain, before import close; it proves retained source ownership, without final
payload readback or termination during GPU DMA.

The native ledger records 52 imports, 156 payload copies (132 HtoD/DtoH),
48 closes and 48 persistent pinned requester owners. Exact pointer ABIs,
physical allocation initialization, each slice's event drain, context restoration
before release and absence of temporary allocation inside IPC cycles are checked.
Every child is reaped, locks are released and GPU allocation baselines restored.
This is Driver API correctness evidence; it does not measure NVLink bandwidth.

Independent source review passes stable four-feature and MSRV combined
Clippy/test gates, including 255 unit tests, eight integration tests and one
doctest for combined features. Six hardware/subprocess fixtures remain ignored
in that CPU gate. Validator-only commit `3b59f66` rejects out-of-cycle host copies
and pinned frees before close-context restoration. The original rejected
controls, 43 Python controller tests and 22 independent classification controls
are preserved; the production code, installed consumer and observer stay at
`9adf5a5`. Four fresh independent repeats pass host/mixed healthy,
source-close and requester-exit cases across both hosts and compilers: 18 payload
copies, including 15 host APIs, with six imports and five closes. Independent
post-run checks find all child PIDs gone, all 16 GPUs at zero allocation with no
compute processes, and selected UUID locks reacquirable. Source and this bounded
installed profile are independently accepted.

Evidence stays outside Git at `/root/orbitte-artifacts/cuda-ipc-host-20261028/`:
`installed-stable/`, `installed-msrv/`, `qualification-inputs.json`,
`matrix-result.json`, `native-audit.json`, `runs/*/evidence/` and
`controller-frozen.py` bind package,
controller, process/native records and host/boot/GPU identities.
`independent-review/` preserves reviews, controls and separate repeat artifacts.

Trusted live exporters remain required. Cross-submission caching has a separate
profile above. Remote host exports, broader stream scheduling, MNNVL/VMM,
exporter crashes and native GPU cleanup faults remain open. See [the contract](cuda-ipc.md) and
[the execution queue](completion-plan.md); this profile does not complete P5.

## Bounded CUDA IPC subbatches

Source [`818b53e`](https://github.com/OrbitInfra/OrbiTE/commit/818b53ebac70b1e53257c5e82161b927f4c4bb63)
qualifies reuse of one import by consecutive requests in a single submission.
Two distinct owned requester allocations sharing one native CUDA context issue
three slices in input order. Each slice drains its own event; native import
close and the source receipt/ACK precede confirmed success. Cross-submission
caching is outside this profile.

Clean packaged consumers on stable 1.99.1 and Rust 1.85 pass 28 installed cases
on two physical H20 hosts, using each host's GPU 0/1 with NV18 topology:
16 three-slice READ/WRITE cases cover both directions and 17 / 2,097,171-byte
payloads; four source-close waits, four requester exits before import close,
and four original single-request READ/WRITE regressions use 4,133 bytes.
The runtime maps compatibility `libcuda.so.590.48.01` on R535.161.08. This is
Driver API correctness evidence, not a CUDA toolkit/kernel or bandwidth result.

The 2,744 native records contain 48 exports/imports, 128 copies and 44 closes.
Healthy and graceful-close cases verify touched bytes, gaps and guards.
Requester-exit cases prove native copy drain and ownership retention; they do
not read back final payload bytes while the source loan remains retained.
The ledger requires distinct live
local allocations before use, the exact relocated offsets, a separate completed
event for each slice and close after all copies drain. Four lost-close cases
retain source export ownership until process exit. Every child is reaped, GPU
UUID locks are released and selected GPU allocations return to zero. Requester
termination happens after copies drain; it does not qualify termination during
GPU DMA or exporter-crash safety.

Independent source review passes stable/MSRV × four-feature Clippy/test gates;
the combined build passes 251 unit tests, eight integration tests and one
doctest, with six hardware/subprocess fixtures explicitly ignored. The review
also checks packaged-source and dependency provenance. Test-only commits
`72373d5` and `e8f2d4d` extend overlap, held-ACK, nonzero submission-index and
cancel/cleanup controls without changing production code, the installed
consumer or native observer. The final validator passes 41 Python controls,
including independent late-allocation and aliased-owner counterexamples.
Four fresh independent repeats also pass: each host runs stable and MSRV
three-slice healthy/source-close cases, totaling 408 native records, six imports,
18 copies and six closes. Independent post-run checks find all original and new
child PIDs gone, all GPUs at zero allocations with no compute processes, and
selected locks reacquirable. Independent review accepts the source and this
bounded installed profile with the limits below.

Evidence is outside Git at
`/root/orbitte-artifacts/cuda-ipc-batch-20261028/`: `installed-stable/` and
`installed-msrv/` contain packaged-consumer manifests; `matrix.json`,
`runs/*/evidence/` and `native-audit.json` bind all case logs, native calls,
host/boot/GPU identities and hashes to source `818b53e` and validator
`e8f2d4d`. `independent-review/` preserves source checks and original rejected
controls. The first review's temporary files were externally removed during a
build; that interruption is retained and affected checks were rerun in the
persistent artifact directory.

Trusted peers and a live exporter process/context remain required. In this
historical GPU/GPU profile, host directions were not included; current local
pinned host evidence is recorded above. External/managed allocations,
transport-wide idle import caches, device-wide
stream scheduling, MNNVL/VMM, native GPU cleanup faults and complete P5 remain
open. See [the ownership contract](cuda-ipc.md) and the
[completion plan](completion-plan.md).

## Cooperative CUDA IPC and NVLink

Source [`e19b3a5`](https://github.com/OrbitInfra/OrbiTE/commit/e19b3a5100f9eac37852db2d47437f6321b5862f)
qualifies the initial cooperative, owned GPU/GPU path on two physical H20 hosts.
Each host runs separate exporter/requester processes on its local GPU 0/1,
in both directions; their topology reports NV18. No cross-host GPU IPC or
measured NVLink bandwidth is claimed. Both installed consumers are built from
a clean packaged crate on stable 1.99.1 and Rust 1.85, with only the `cuda`
feature. They have no linked CUDA runtime, RDMA or Mooncake library dependency.
Runtime maps the recorded CUDA compatibility driver `libcuda.so.590.48.01`
on R535.161.08. This is Driver API qualification, not a toolkit/kernel test.

All 48 installed cases pass: 24 healthy READ/WRITE cases at 17, 4,133 and
2,097,171 bytes, eight source-close waits with an import still open, eight
requester exits before native import close, and eight stale-epoch rejections.
Each healthy case compares the entire logical allocation and guards. Native
evidence checks physical 2/4 MiB allocation initialization, exact export/import
handle pairing, relocated copy offsets, event completion, matched native close
and calling-context restoration before cleanup. Requester-exit cases kill the
importer after its copy drained but before close; they establish missing-close
retention, **not** in-flight GPU DMA termination or exporter-crash safety.

The 2,680 native records contain 64 exports/imports/copies and 56 closes. Eight
lost-close cases intentionally retain source allocation/admission until process
exit. All supervised children are reaped, all GPU UUID locks are released, and
the four selected GPUs return to their zero-allocation baseline. Loaded driver
paths and hashes, process identities, host/boot identities and every case file
are pinned in the external audit.

Independent review passes stable/MSRV × four-feature Clippy/test gates. The
combined build passes 249 unit tests, eight integration tests and one doctest;
six hardware/subprocess fixtures remain explicitly ignored in that CPU gate.
Thirty-nine controller controls and strict documentation checks pass. The review
found two evidence-validator gaps: early allocation frees and borrowing a later
cycle's context restoration. Commits `d2c8200` and `177a84f` fix those checks and
add counterexamples; all 48 original ledgers pass the strengthened validator.
These commits leave production code, installed consumer and native observer
unchanged. Four fresh independent hardware repeats also pass: stable and MSRV
healthy/graceful-close cases on both physical hosts. The eight review child PIDs
are gone, selected locks can be reacquired, and both hosts report zero GPU
allocations/no compute processes. Independent review accepts this bounded
cooperative profile; full P5.4 remains open.

Evidence stays outside Git at `/root/orbitte-artifacts/cuda-ipc-20261028/`:
`qualified-stable/` and `qualified-msrv/` hold installation manifests,
`matrix-profile.json` freezes the 48 cases, `matrix/` contains their raw process
and native logs, and `native-audit.json` binds the artifacts and validation
revision. Independent source/evidence reviews and counterexamples live at
`independent-review/` under the same artifact root (original workspace:
`/tmp/orbitte-ipc-review-20261028/`). Failed controls are preserved.

This profile requires trusted peers and a live exporter process/context until
every import closes. Legacy IPC handles authorize whole allocations, without
per-range hardware permissions or global revocation. Host/GPU IPC, external or
managed allocations, cached import ownership, broader stream scheduling,
MNNVL/VMM and forced-exporter-death safety remain unqualified or unimplemented.
Native import/copy/close failure retention has CPU controls only; physical GPU
cleanup faults and recovery remain unqualified.
See [the transport contract and reproduction method](cuda-ipc.md). P5.4 remains
open for those requirements; this substage does not establish full TENT parity.

## GPUDirect RDMA

Source [`cda0fdf`](https://github.com/OrbitInfra/OrbiTE/commit/cda0fdf16aec7b128e21c60fa860c340c164cf03)
qualifies explicit Direct RDMA for owned `CudaMemory` on two physical H20 hosts,
using one
selected GPU and two locked mlx5 RoCEv2 RNICs per host. Both stable 1.99.1 and
Rust 1.85 build independent installed consumers from the packaged crate. The
source passes 243 unit tests, eight integration tests, one doctest, Clippy and
all eight feature/toolchain CI combinations. Thirty-six controller controls
include corrupt or missing GPU MR, PD, rail, process and lifetime evidence.

All 72 installed cases pass: each toolchain runs both physical directions for
host/GPU, GPU/host and GPU/GPU READ plus WRITE. Small requests cover 1/17 bytes
with polling and 32/64 bytes with event progress. Two-rail cases use three
parent requests of 2 MiB + 17 bytes per
operation, a 1 MiB base slice and folded tails, with both polling and event
progress. Every case checks the complete allocation and guard bytes. Passive
native witnesses identify actual GPU pointer type, MR flags and range, local
SGE/lkey/QP PD, remote rkey/permission, and payload work on both selected
contexts. GPU sources issue no MW operations and use ordinary fenced SEND;
source private PD counters drain completely.

`native-audit.json` pins the two packages, binary/observer hashes, all 72 case
files, host/boot/GPU identities and loaded native libraries. Every supervised
child is reaped and every RNIC/GPU lock is released. Selected-RNIC QP/CQ/MR/PD
identity sets match their before-run inventories; GPU
memory returns to zero on both hosts. Two additional installed repeats capture
eight live `hardware_peer` PID/context identities and prove that all eight
disappear after exit. B gained six other context entries on selected RNICs and
three each on two unselected RNICs; their origin remains unassigned. The initial
failed global inventory check is preserved, and host-wide context non-growth
is **not qualified**. The final audit records `passed-with-limits`. Mapped CUDA
compatibility libraries are
`libcuda.so.590.48.01`, reporting Driver API 13.1, on R535.161.08 hosts.
This is a Driver API/GPUDirect result, not a CUDA toolkit/kernel compilation test.

Evidence stays outside Git at
`/root/orbitte-artifacts/gpudirect-rdma-20261027/`: `qualified-stable/` and
`qualified-msrv/` contain installation manifests; the four corresponding
`*-small/` and `*-multirail/` directories preserve all process and native
receipts. Development runs and the first controller rejection remain archived;
the latter rejected a GPU profile through a host-only validator before any
remote process started. The final packages include the corrected validator.

Independent review accepts the source and this bounded installed profile; it
reran the relevant source gates and independently checked the frozen packages
and native evidence. Its archived reports and checksums are in `reviewer/`.
Failure retention has integrated CPU ABI controls for partial construction,
payload/RQ MR and private-PD cleanup, and CUDA restoration/visibility failures.
Actual GPU teardown
faults, physical faults, InfiniBand, other GPUs/providers, performance, mixed
staged/direct workloads, external/managed/IPC allocations, NVLink and external
producer stream handoff remain
unqualified or later P5 work. The archived GPU-source cases used private MRs and QPs. The reuse profile has its own verification queue entry.
See the [ownership and reproduction contract](rdma-cuda.md#gpudirect-rdma-ownership).

## CUDA copy streams and completion events

Source [`ce86269`](https://github.com/OrbitInfra/OrbiTE/commit/ce86269d38ce766d4aadd79c7dc245c7ce013699)
adds owned, serialized copy streams/events. Both stable 1.99.1 and Rust 1.85
pass 233 unit tests, eight integration tests, one doctest and all-feature Clippy.
The CUDA controls delay actual shim copies until completion, reject premature
staging release and cover stale events, partial submit/record, context restore
and cleanup failures. An independent reviewer inspected the exact production
files and reran the eight CUDA ownership controls in a separate source archive;
no blocking finding was reported. CI's eight feature/toolchain combinations,
frozen TENT CPU comparison and strict documentation build also pass.

The packaged crate SHA256 is
`64b0ea10219675c77ca5f56058b72e9e599b0aa241f62a672d0b0aeb0b510f62`.
Two standalone consumers import that extracted package and build with stable
and Rust 1.85. Their hashes, lockfile, commands and logs are recorded outside Git
at `/root/orbitte-artifacts/cuda-streams-20261027/` (`package-manifest.json`
and `validation.json`). Earlier Clang/CMake environment failures remain archived;
the successful stable build uses the project's GCC configuration in a fresh
target directory.

The new installed consumers also pass on two physical H20 hosts, using two
GPUs per host: three unaligned payload lengths (17, 4,133 and 1,048,595 bytes),
eight concurrent callers and four rounds per caller, on both toolchains. All
24 GPU/size/toolchain combinations pass full-byte and guard comparisons with
zero retained resources. All four supervised processes are reaped, GPU locks
are released and selected-GPU memory returns to its initial zero usage. Driver
inventory and native loader logs identify `libcuda.so.590.48.01`, reporting
Driver API 13.1, on the R535 hosts. This is a real local-copy result using the
compatibility library, not a CUDA toolkit/kernel compilation test.

`gpu-audit.json` pins the logs, supervisor results, fresh host/boot/GPU identities
and before/after memory inventory. Native failure cases retain their separate
CPU-shim evidence. The explicit `cuda_copy` gate is described in
[RDMA/CUDA](rdma-cuda.md#copy-completion-and-reuse). These results qualify local
CUDA staging only. GDR, IPC/NVLink, a shared device-wide stream pool and external
producer integration remain P5 work.

## P4 independent review

An independent reviewer inspected exact main
[`121ab95`](https://github.com/OrbitInfra/OrbiTE/commit/121ab95a77f94a80286617c4a2032654e692f9b4)
against frozen TENT `e2d6e8e` on 2026-10-27. The accepted scope is the reviewed
source and bounded RoCE evidence, including fresh independent live repetition. No blocking production-code finding
was reported. One stale architecture statement about installed-consumer
qualification was corrected.

The reviewer independently reran source and packaged-consumer gates on stable
and Rust 1.85, all-feature Clippy, 34 controller controls, five native C controls
and the strict 19-page documentation build. Ten package manifests and 47 native
recovery files passed hash verification; the archived validators accepted 44
recorded two-host cases. The reviewer additionally ran four fresh two-host
cases from the frozen installed packages: stable A-to-B READ/WRITE and Rust 1.85
B-to-A READ/WRITE, with automatic discovery, two rails, 16 parents and
4 MiB + 1 byte requests. Full payload/guards, independent inventories and native
WR/CQE accounting all pass. Every child was reaped, RNIC locks were released,
provider hashes stayed unchanged and native resource counts returned to their
before-run values. These are selected RoCE cases; no physical-loss or
InfiniBand coverage is inferred.

Review source archive SHA256:
`e3230e5c58cd562390e10df910bd8ea2aa7bb6dbd46d4a88ec6f14c50ad39246`.
The report, commands, logs and evidence indexes are outside Git at
`/root/orbitte-artifacts/p4-final-acceptance-20261027/reviewer/` (`review.json`
and `review.md`). Full P4 is not accepted: physical link/device loss, provider
removal and InfiniBand remain unqualified. The user's 2026-10-27 decision allows
P5 work with available hardware while keeping these gaps explicit.

## Installed automatic discovery

Source [`36890c8`](https://github.com/OrbitInfra/OrbiTE/commit/36890c87b71c56922149824633d5b5811bc479e4)
passes four frozen two-host configurations: READ/polling and WRITE/events with
stable 1.99.1, then both in the reverse direction with Rust 1.85.0. Each process
leaves device, port and GID pins unset and selects within its two locked mlx5
RNICs. An identical blacklist confirms whitelist precedence. Before normal
registration, a separate engine attempts a 64-byte registration with absent
strict GID 255; registration returns `Unavailable`, and close leaves no owners.

A packaged, hash-pinned collector reads sysfs without importing OrbiTE's
selection code. Before/after inventories must match under the original resource
locks. Published PCI addresses, NUMA nodes, port state, RoCEv2 GIDs, physical
network interfaces/indices and memory priority tiers agree with that inventory.
The declared hosts have one unambiguous preferred GID per RNIC; ambiguous
inventories fail the verifier. The same consumer checks 16 concurrent parents,
4 MiB + 1 byte requests and three rounds of exact WR/CQE, full-payload, guard,
QP-limit and final-drain accounting. Two additional fixed-GID cases pass with
the new packages, one on each compiler.

Both packaged-consumer builds pass formatting, Clippy, tests and five native C
controls. The 34 controller tests include mismatched routes, PCI/NUMA ranks,
strict-pin fallback, changed inventories and ambiguous-GID negative controls.
Both hosts return to their pre-run native resource counts; all supervised
children exit and both original RNIC locks are released.

Artifacts are outside Git at
`/root/orbitte-artifacts/installed-discovery-20261027/`.
`native-audit.json` links the two final packages, four automatic cases and two
pinned controls. The initial failed run at `native-stable-v1/` remains archived:
its fixture incorrectly expected construction to perform lazy discovery. The
final fixture triggers discovery through public memory registration. Production
owners, dependencies, API and wire v14 are unchanged.

See the [reproduction contract](installed-host.md#automatic-discovery-against-an-independent-inventory).
Separate inventory collection is not independent reviewer acceptance. This
profile does not qualify physical loss, provider teardown, InfiniBand, hotplug,
page placement, GPU transfers or performance. Those limits retain their
existing stages and acceptance gates.

## Installed physical rail accounting

Source [`cc6ab0f`](https://github.com/OrbitInfra/OrbiTE/commit/cc6ab0fe0092c0d1d06792bb7190f48832f0eae9)
passes four frozen two-host configurations: READ/polling and WRITE/events with
stable 1.99.1, then both cases in the reverse physical direction with Rust
1.85.0. Both peers use two distinct mlx5 PCI RNICs on RoCEv2. The ordinary
installed consumer uses 16 concurrent parents, 4 MiB + 1 byte per request,
1 MiB blocks and one shared QP per rail, for three full-byte/guard-checked rounds.

The requester-only passive observer joins every accepted native payload WR to
its successful CQE by context, CQ, QPN and WR ID. Device names join native
contexts to the published PCI identities. Every round accounts for exactly
64 slices and 67,108,880 bytes, including folded tails. Both rails have multiple
parents outstanding on their QP; measured payload debt peaks range from four
to eight per QP and stay within the configured limit of eight. Native bytes
match public completion counters, QPs are reused, and all counted owners drain.

The source changes only qualification tools and documentation. Production
owners, API, Cargo dependencies and wire v14 are unchanged. The shared observer's
lost-CQE and completion-channel cleanup paths receive four affected two-host
regressions. Stable and MSRV independent-consumer builds pass formatting,
Clippy, consumer tests and all five C controls; 32 Python controller tests
include wrong-device, wrong-range/CQE, lost-byte and false-concurrency controls.

Artifacts, frozen profiles, manifests, child/provider logs and resource
inventories are outside Git at
`/root/orbitte-artifacts/installed-rail-accounting-20261027/`.
`native-audit.json` joins the two immutable packages and eight hardware cases.
The [reproduction guide](installed-host.md#physical-rail-accounting) defines the
receipt and concurrency checks. The observer forwards CQEs unchanged, but its
bookkeeping serializes native submission receipts. These are bounded admission
and accounting results, not throughput, fairness or fabric-saturation results.
Physical link/device loss, InfiniBand and independent acceptance remain open.

## Current source delivery

Source [`9e1fdea`](https://github.com/OrbitInfra/OrbiTE/commit/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578)
retains initially paused contexts and reselects automatic GIDs during recovery,
following the frozen TENT context lifecycle. Persistent MRs remain on the same
PD; explicit pins and physical drain still gate route publication. Public API
and wire v14 are unchanged. The preceding `4fbe01d` delivery reorganizes context
ownership and records the [source audit](architecture.md#p4-source-audit).

Stable 1.99.1 and Rust 1.85.0 each pass all-feature Clippy and **239
CPU/integration/doc tests**. The [eight-variant CI](https://github.com/OrbitInfra/OrbiTE/actions/runs/40448463937),
[frozen TENT CPU comparison](https://github.com/OrbitInfra/OrbiTE/actions/runs/40448463956)
and [documentation checks](https://github.com/OrbitInfra/OrbiTE/actions/runs/40448463913)
pass for that delivery. Local manifests and logs are in
`/root/orbitte-artifacts/rdma-context-recovery-20261027/delivery.json`.

### Focused native recovery

The [native context/GID gate](rdma-cuda.md#initially-paused-contexts-and-gid-reselection)
now passes on one physical mlx5 RoCE RNIC with stable 1.99.1 and Rust
1.85.0. Execution source is [`bde9284`](https://github.com/OrbitInfra/OrbiTE/commit/bde9284a950dd93bfa4069061ddb54943af0cc61);
its runtime, fixtures and Cargo inputs are identical to `9e1fdea`. Each
binary runs automatic and strict-pin cases: GID 0 changes to 3 only in
automatic mode, both cases complete two pause/drain/resume cycles with
unchanged PD and two persistent MRs, and final native owner counts reach zero.

Actual child maps contain the frozen libibverbs/mlx5 providers. Both
supervised children are reaped, locks are released and device resource
counts return to preflight values. The first native run also passed, but
its controller missed a libtest-prefixed receipt; that failed audit is
preserved alongside the corrected, fully observed repeat. The reconciled
manifest is `/root/orbitte-artifacts/p4-closeout-20261027/native-recovery-audit.json`.

This qualifies the declared process-local observed-port-state override
with real PD/MR/QP/CQ operations and GID queries. It sends no payload,
changes no physical link, and does not establish two-host or InfiniBand
qualification. Independent acceptance remains open.

### Installed cleanup fixture

The [packaged requester QP cleanup fixture](installed-host.md#native-cleanup-failure)
passes a single-host, two-process diagnostic with artifacts built from
`5897f28`: stable READ/polling and Rust 1.85 WRITE/events. Each uses one
64 KiB payload, one QP/active rail and two registered contexts. A real
`ibv_destroy_qp` returns EIO once; public batches remain unquiesced, and
post-drop native queries, actual backing and byte/SQ/batch charges prove
retention. The supervisor reaps the faulted requester; its replacement rejects
stale authority and completes healthy rounds against the live source.
Provider maps, exact child lifetimes and unchanged pre/post device resource
counts are recorded in
`/root/orbitte-artifacts/installed-cleanup-20261027/native-audit.json`.

The failed first diagnostic exposed fixture initialization order; its evidence
is retained. A separate failed audit incorrectly required ERR, although close
fences admission and calls destruction directly. The corrected verifier records
the actual retained QP state and preserves the original receipts.

These original runs are **single-host diagnostic evidence**: their two Forge
replicas shared a physical host. The subsequent
[two-host qualification](#installed-two-host-fault-qualification) uses a separately
inventoried allocation and package. Earlier libtest cleanup evidence remains
scoped to its own artifact; independent acceptance is still open.

### Installed in-flight DMA fixture

The [installed in-flight profile](installed-host.md#in-flight-dma-interruption)
passes with source [`56edccc`](https://github.com/OrbitInfra/OrbiTE/commit/56edcccd3c94d6504b5f65f8cb14f66051cbdc10):
stable 1.99.1 READ/polling with one 4 MiB WR, and Rust 1.85 WRITE/events with
one 16 MiB WR. Both use two registered mlx5 contexts and one active RoCE rail.
The actual failed WC and successful QP destruction identify the accepted
payload WR. Post-drain reads prove both copied and untouched destination bytes,
with guards intact. Both original processes retain their four MRs, release
all temporary charges and resume three healthy rounds before complete close.

The first 4 MiB WRITE run copied every byte despite its failed WC; the fixture
correctly rejected it. A later run exposed a fixture race that checked source
idle before the request arrived; an explicit requester-failure barrier fixes
that ordering. Both failed runs and their resource-reaping receipts are retained.
The final package inputs, native/public ledger checks, actual provider hashes,
original resource locks and unchanged device resource counts are recorded in
`/root/orbitte-artifacts/installed-inflight-20261027/native-audit.json`.

This closes the missing fixture for actual in-flight DMA interruption. It is
**single-host diagnostic evidence**, because those Forge replicas shared one
physical host. The subsequent two-host run is recorded below. Physical
link/device loss, broader cleanup failures, InfiniBand and independent acceptance
remain open. Production runtime, dependencies, public API and wire v14 are unchanged.

### Installed two-host fault qualification

Four frozen cases pass on **two distinct physical hosts**, each with eight H20
GPUs and two selected 400G mlx5 RoCE RNICs. The tests use host memory only.
Both installed artifacts were built from `56edccc`; their runtime and native
fixtures are identical to controller source
[`9a6c270`](https://github.com/OrbitInfra/OrbiTE/commit/9a6c27056ea4a68855b296cfa03ead1607dc7e86).
Stable 1.99.1 runs READ/polling on rail mask 1; Rust 1.85 runs WRITE/events in
the opposite direction on mask 2. Each artifact runs one 64 KiB requester QP
cleanup-failure case and one in-flight DMA case (4 MiB READ or 16 MiB WRITE).
Both contexts retain persistent MR copies throughout each surviving engine's
recovery.

The controller reconciles actual destructor EIO with retained backing, keys and
charges, exact requester reaping and replacement against the surviving source.
The DMA cases join an accepted WR, QP ERR, its actual failed WC and successful
QP destruction to copied and untouched post-drain bytes, intact guards and
three recovered rounds in the original processes. Every supervised child is
reaped, original RNIC locks are released, provider hashes stay fixed, and selected
device PD/CQ/QP/MR/context counts return to the preflight values.

The scenarios, package/binary hashes, host/boot inventories, native receipts and
resource audit are retained outside Git in
`/root/orbitte-artifacts/p4-closeout-20261027/two-host-native-audit.json`.
Rejected same-host replacement allocations remain in that directory's placement
record and contribute no two-host qualification. Earlier failed native controls
remain in their original diagnostic archives.

This qualifies only these four installed RoCE fault cases. It does not qualify
physical link/device loss, broader installed cleanup failures, physical rail
contention, InfiniBand, GPU/NVLink payloads or performance. Independent acceptance
and the remaining [P4 gates](completion-plan.md#p4-remaining-delivery) stay open.

### Installed memory cleanup and replacement

The [installed MR/MW gate](installed-host.md#mrmw-cleanup-and-engine-replacement)
passes **six frozen two-host cases**, three on each compiler artifact built
from [`1428138`](https://github.com/OrbitInfra/OrbiTE/commit/1428138b83a1ef16b55053d7f83a7c7eefcb80c3).
Controller source is
[`b83dbc3`](https://github.com/OrbitInfra/OrbiTE/commit/b83dbc32867cb434e130e8ec3b3b9b4d50fd1d33);
its runtime and native consumer sources are identical to those packages.
The two physical hosts and locked mlx5 RoCE devices are the same separately
inventoried allocation used above. Stable 1.99.1 runs A to B; Rust 1.85 runs
B to A. These are host-memory tests.

| Native failure | Frozen workload on each artifact | Retained owner after unregister, close and drop |
| --- | --- | --- |
| Requester MR | 64 KiB READ, polling, rail mask 1 | One MR and its 131,264-byte backing; no grant or SQ/RQ debt |
| Source MR | 64 KiB WRITE, events, rail mask 2 | One parent MR, backing and its PD key domain; no grant or SQ/RQ debt |
| Source MW | 64 KiB WRITE, events, rail mask 2 | One MW/grant, its parent MR, DMA loan, key domain and 65,536-byte direct quota |

Every selected real destructor returns EIO exactly once under an exact-FD,
thread-local syscall filter. The installed public API reports failed close;
actual backing survives fixture-owner drop. Native handle/key fields remain
intact and the real PD destructor returns EBUSY before and after replacement.
Both original processes recreate engines at the same service addresses,
reject old epochs before QP creation and finish three recovered byte/guard
rounds, with fresh allocations released and quarantined old backing retained.
The distinction between an allocated MW key and its BIND grant key is recorded
explicitly; the low-byte generation is not assumed identical.

The four existing QP cleanup/in-flight cases also pass with these new packages.
The shared native observer changed, so those affected regressions were rerun;
the unrelated healthy/concurrency matrices were not expanded. All native
children are reaped, original RNIC locks are free, providers remain unchanged,
and selected PD/CQ/QP/MR/context counts return to their preflight values.
The 27 controller tests and both packaged-consumer format, Clippy, test and
install checks pass. Native fault controls additionally check exact owner,
helper-thread/FD isolation and retry rejection.

Exact package/binary/provider hashes, scenario geometry, identities, receipts
and cleanup are in
`/root/orbitte-artifacts/installed-memory-20261027/native-audit.json`.
The first run was rejected by the supervisor's missing new-mode allowlist
before starting the requester; that failed run and its released-lock receipts
are preserved. The corrected supervisor allows only the declared MR/MW role.

This qualifies the declared installed memory-cleanup and engine-replacement
contracts. The subsequent [dependency cleanup delivery](#installed-dependency-cleanup)
qualifies its selected CQ/channel/PD cases. Physical link/device loss, physical
rail contention, InfiniBand and independent acceptance remain open.
It adds no GPU payload, performance or TENT wire/API compatibility claim.

### Installed dependency cleanup

The [CQ/channel/PD gate](installed-host.md#cq-completion-channel-and-pd-dependency-failures)
passes **six frozen two-host cases**, three on each compiler artifact built
from [`78127d7`](https://github.com/OrbitInfra/OrbiTE/commit/78127d7c12aa05789308d0974fa9cd22c84274b0).
Stable 1.99.1 runs A to B; Rust 1.85 runs B to A on the same independently
inventoried hosts and locked mlx5 RoCE RNICs. Each case has one requester,
one 64 KiB payload, one QP/active rail, two registered contexts and two process
generations. These are selected requester dependency contracts; the existing
native libtests retain their separately recorded source-side SQ/RQ scope.

| Failure | Workload on each artifact | Owner state after unregister, repeated close and fixture drop |
| --- | --- | --- |
| CQ | READ, polling, mask 1 | Two CQs, one MR/backing, grant, SQ and batch remain retained; the payload QP is closed |
| Completion channel | WRITE, events, mask 2 | One channel, one MR/backing, grant, SQ and batch remain retained; both engine CQs and the payload QP are closed |
| PD | READ, events, mask 1; idle teardown | One device/PD remains retained; business backing, payload MRs, grants, CQs, channel and batch/SQ charges are released |

A real dependent QP, CQ or MW makes the selected destructor return `EBUSY`
exactly once. Native probes before and after dropping fixture owners prove the
remaining dependency; the observer rejects destructor retries and parent-device
close. Both CQ/channel cases keep 65,536 direct/inflight bytes charged. The PD
case has zero payload debt and its actual allocation is released. The existing
supervisor reaps the faulted requester, keeps RNIC locks through replacement,
and verifies fresh authority at the same endpoint against the live source.
Old epochs open no QP or WR; three full-byte/guard rounds succeed before and
after replacement.

The ten affected MR/MW, QP-cleanup and in-flight DMA regressions also pass on
these packages. Both installed-consumer format, Clippy, test and install gates,
four native-observer CPU controls and 29 Python controller tests pass. Every
native child is reaped, original locks are released, provider hashes stay fixed,
and selected device resource counts return to their preflight values.
The first artifact pair also passed; final artifacts additionally fence the
observer's raw QP witness before native destruction begins.

Exact package/binary/provider hashes, scenario contracts, host/boot identities,
receipts and the reconciliation are outside Git at
`/root/orbitte-artifacts/installed-dependency-20261027/native-audit.json`.
The earlier passing packages and controls are preserved there too. Production
code, public APIs and wire v14 are unchanged.

This closes the declared installed dependency-failure profiles. It does not
qualify spontaneous hardware failure, physical link/device loss, provider
removal, physical rail contention, InfiniBand, GPU/NVLink payloads or performance.
Independent acceptance remains open. The current nodes expose Ethernet/RoCE;
GPU allocation and RNIC locks do not establish exclusive physical-port ownership.

## Recorded host RDMA evidence

The table links immutable delivery records. Paths below are external artifact
directories on the delivery host, under `/root/orbitte-artifacts/`; they are
not files in the source checkout. Each record identifies its source, binaries,
providers, workload, controls and limits.

| Profile and frozen source | Recorded scope | External directory and detailed record |
| --- | --- | --- |
| Installed lost completion — `02c15bd` | Across both compiler artifacts: 40 two-host/two-RNIC configurations, including 32 completion-loss cases. The tool observes actual successful payload CQEs, withholds them from the engine, then supervises peer death and survivor drain/reconnect. | `rdma-installed-completion-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#latest-installed-lost-completion-delivery) |
| Installed posted-RQ interruption — `c27029c` | Peer death after real completion receives and MWs are posted, before the requester receives payload authority. The surviving engine keeps its PD/MRs. | `rdma-installed-interruption-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-installed-posted-rq-interruption-delivery) |
| Installed peer restart — `0b6229c` | Idle persistent-resource restart with one surviving engine; old epochs are rejected before new native payload work. | `rdma-installed-failures-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-installed-peer-restart-delivery) |
| Native cleanup — `d5514d4` | Across both compiler artifacts: 44 configurations. Actual QP destructor EIO and completion-channel EBUSY, with CQ/PD and MR/MW regressions, verify retained owners and fresh-process recovery. | `rdma-native-cleanup-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-native-cleanup-delivery) |
| Installed healthy host — `ba09b1f` | 72 configurations across two compiler artifacts: READ/WRITE, both directions, polling/events, widths 1/3, rail masks 1/2/3 and selected 12/16/32-parent workloads. | `rdma-installed-host-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-installed-host-delivery) |
| Periodic monitor recovery — `08fb8ac` | Across both compiler artifacts: 276 public configurations, including 64 process-local query-failure recovery cases, plus 32 internal shared-QP cases. PD/MRs and healthy-rail QPs survive drained-lane replacement. | `rdma-monitor-recovery-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-periodic-monitor-recovery-delivery) |
| Fixed-width cold construction — `2d44743` | Concurrent first requests reserve bounded array slots before native QP creation; subsequent rounds reuse the published connections. | `rdma-cold-connections-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-fixed-width-cold-construction-delivery) |
| Public concurrent grants — `8511c53` | Public v14 grants share physical QPs with separate grant/SQ/RQ limits and exact completion dispatch. | `rdma-public-qp-20261026`; [record](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-public-concurrent-grant-delivery) |

These are scoped correctness results. Lost CQEs after successful DMA do not
prove interruption of in-flight DMA. Query-failure injection and QP ERR do not
prove physical cable/device loss. Native libtest cleanup faults do not qualify
an installed public consumer's cleanup-failure path. RoCE does not qualify
InfiniBand, GPU direct transfer or throughput/fairness.

The [earlier evidence index](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-qualified-native-source-concurrent-grant-records)
preserves the preceding QP ownership, worker pool, CQ lane, multirail,
MR/MW-failure and live-recovery records. The [historical RDMA guide](https://github.com/OrbitInfra/OrbiTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/rdma-cuda.md)
retains their exact recipes and external artifact locations. Old protocol
names, filenames and remaining-work lists describe those revisions only.

## Reproduce and review

Use the maintained recipe for the boundary under review:

| Boundary | Recipe and evidence |
| --- | --- |
| Source and CPU contracts | [Build checks](build.md#run-source-checks), exact source/toolchains and complete test output |
| Frozen native TENT comparison | [CPU oracle](cpu-oracle.md), pinned reference, observations and negative controls |
| Device selection, native bytes and supervision | [Native profile](rdma-cuda.md#run-a-native-profile), current attempt/host/boot, provider maps, original resource locks and child-reaping receipts |
| Packaged public host API | [Installed host](installed-host.md), archived crate, independent consumer, lockfile and binary hashes |
| Native teardown retention | [Native cleanup](native-cleanup.md), real failing handles, unresolved debt, retained backing and process lifetime |
| Initially paused context and GID recovery | [Focused native gate](rdma-cuda.md#initially-paused-contexts-and-gid-reselection), native context/PD/MR identity and complete close receipts |

Select an external artifact root before running a gate. Freeze source, package,
compiler and native library identities, retain failed attempts and controls,
and record which contract each result covers. Keep logs, JSON/CSV, traces,
plots and per-run reports outside Git. Repository code contains reusable
fixtures and reproduction instructions.

An independent reviewer checks the diff, reruns the affected gates, and accepts
the exact artifact and declared profile. A delivery handoff includes the
commit, changed contracts, commands/results, artifact locations and remaining
limits. Missing hardware stays unqualified; a new parent count or payload size
does not create another implementation milestone.

## P4 acceptance boundary

The host data path is implemented. The fixed remaining queue covers native
discovery/recovery, the declared installed fault profile, physical/fabric
qualification and independent review. Its detailed completion conditions are
maintained only in [P4 remaining delivery](completion-plan.md#p4-remaining-delivery).

P4 acceptance does not complete all TENT capabilities. GPU direct paths,
storage transports, remaining runtime policies and API/wire parity
retain their existing stages. OrbitKV integration follows accepted P4.

## Historical record preservation

The documentation cleanup preserves the complete pre-cleanup pages at
[source `9e1fdea`](https://github.com/OrbitInfra/OrbiTE/tree/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs).
An external byte-for-byte archive and SHA-256 manifest are also retained at
`/root/orbitte-artifacts/p4-closeout-20261027/documentation-before/manifest.json`.
Original experiment directories remain in place. This reorganization changes
navigation and removes duplicate status prose; it grants no new hardware
qualification.

## Host staging location

This source stage adds opt-in `Auto` / explicit CPU NUMA location selection to
engine-owned CUDA chunks. The default wildcard allocation profile is preserved.
The maintained [contract](staging.md#host-staging-location) separates logical
chunk sizes from page-rounded host backing and names the remaining limits.

External evidence directory:
`/root/orbitte-artifacts/staging-location-selection-20261030/`.
The host-staging design input was TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`;
`independent-review/design-review-60fe19b.json` records the initial independent
owner/placement review. Independent source review accepts `ab309f4`, whose
production code is identical to `2c1872d`. The only subsequent source change
releases test observation locks before assertions. The acceptance record is
`independent-review/source-review-ab309f4.json`
(SHA-256 `b428cbb3b99dee1caf52aa14e2d40b5d550ae1888c52f9004b6e4d984c453289`).

Both the implementation and independent reviewer pass 11 source gates at
`2c1872d`: stable/Rust 1.85 CUDA and combined tests, all-target Clippy, formatting,
strict documentation and local links. Each compiler passes 126 CUDA unit tests
and 340 combined unit tests, eight integrations and one doctest. Existing
hardware fixtures remain ignored. The test-only `ab309f4` change passes focused
regression checks; the reviewer verifies all 286 other tracked files unchanged.
Source CI at `2c1872d` passes all eight feature/compiler jobs, the frozen TENT CPU
comparison and documentation. Exact commands, logs and hashes are in
`gates.json`, `test-only-ab309f4-gates.json`, `ci-source-2c1872d.json` and the
independent acceptance record.

The independent negative control removes only the two NUMA-location reuse
guards. The unchanged test detects reuse of node 130 when node 2 was requested
and exits with one ordinary failed test. Its manifest is
`independent-review/context-only-negative-ab309f4/manifest.json`. The first
negative run also reached this assertion, but poisoned fixture locks caused a
secondary FFI abort; that superseded run is preserved and is not the accepted
control. No production fix was needed for the fixture unwind.

The CPU controls use actual anonymous mapped bytes, simulated NUMA binding and
CUDA registration, real TCP control/payload sockets and the existing verbs ABI
fixture. They cover both public transfer directions, different endpoint GPU
locations, repeated ring reuse, tails/sentinels, location changes under a single
budget, unknown/denied locations, sparse node masks, and registration/unregister/
context restoration/unmap failures. Failed cleanup retains actual backing and
CUDA owners; no fallback allocation or repeated release is allowed.

No hardware task was requested. Native NUMA binding, CUDA host registration,
page residency, current v21 installed execution, physical DMA overlap and
performance remain unqualified. Earlier installed evidence covers its recorded
wildcard profile and cannot qualify this new allocator. Broader TENT location
routing, remote topology publication and P6 selection remain open.


## CUDA IPC cache reclamation

Source `fd55f15` adds bounded idle LRU replacement and
`TransferEngine::flush_nvlink_cache(timeout)`. A full requester cache closes an
idle native import, acknowledges the source release, then reuses that slot for
a new token/key. Busy, constructing and uncertain owners stay reserved; a
duplicate target waits on the original replacement. Explicit flush captures
keys without closing outgoing admission. The [transport contract](cuda-ipc.md#cross-submission-relocation-cache)
defines the separate entry/physical budgets and concurrent-refill semantics.

The existing CUDA shim and real control sockets exercise rotating READ/WRITE
working sets, exact bytes/guards, LRU hits, bounded live entries, source loan
release and submission after flush. A held release ACK checks target
single-flight and continued hot-key progress outside the map lock. Active-copy
pressure covers new-key rejection and flush deadlines. Lost ACK and native
close failure distinguish safe receipt retries from retained native owners and
verify one-shot close. These are source/CPU controls, not native DMA or a
performance benchmark. The new tests remain within the existing IPC owner tree.

Evidence directory: `/root/orbitte-artifacts/nvlink-cache-reclaim-20261031/`.
`source-inputs.json`, `environment.json` and `gates.json` bind the primary source,
toolchain, focused IPC/NVLink gates, formatting and all-target Clippy. Independent
review and final CI records are recorded in that directory before PR readiness.
This review used TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`: its
transport relocation map caches imported pointers and closes them at uninstall.
The bounded LRU and source release protocol are deliberate OrbiTE extensions.

No new hardware task was requested. Current installed v21 dynamic-working-set
DMA, native cleanup faults and throughput remain unqualified. Historical fixed
working set qualification does not qualify the new reclaim path. Exporters must
remain alive and cooperative; native provider calls remain synchronous. This
does not complete all P5, TENT parity or OrbitKV integration.


## CUDA IPC group completion

Source `f388416` changes `NVLinkSubBatch` submission from per-slice waits to
ordered same-stream enqueue and one trailing CUDA event. The first actual local
allocation supplies the reused stream/event. Actual GPU and pinned host owners
are deduplicated and locked in a common order, retained before native submission,
and held through completion/recovery and context restoration. Memory guards are
released before source receipts. Recovered enqueue/cancel errors preserve only
the acknowledged prefix; shared record/wait/restore errors cover the attempted
group. The wire and public API are unchanged.

Primary source checks pass 111 CUDA/IPC/staging tests, including the
added mixed-owner alias/reversed-concurrency control, and the two NVLink protocol
controls. The existing subbatch table now checks one record/wait for three
ordered copies, shared completion failure spans, partial enqueue, cancellation,
close/ACK loss and actual backing retention. The CUDA shim queues real host-backed
copies until event/stream drain. These checks establish CPU ownership behavior,
not CUDA DMA or bandwidth. The installed consumer's native validator now requires
one trailing event for each group and rejects early record/pop, split streams,
early release and borrowing later-group or local-readback completion; its nine
CPU validator controls pass.

Evidence directory: `/root/orbitte-artifacts/nvlink-group-completion-20261031/`.
`source-inputs.json`, `environment.json` and `gates.json` bind the frozen source,
compiler, focused gates and log hashes. Independent review and final CI are
recorded in that directory before PR readiness.

This review used TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`.
Its `NVLinkSubBatch` / `startTransfer` also associates a group with one trailing
event. OrbiTE follows the ordered async-copy path while reusing allocation-owned
stream/event resources; native `cudaMemcpyBatchAsync`, device-wide pooling and
nonblocking completion queries are separate work. Producer readiness remains an
explicit handoff and final success still requires the cooperating source receipt.

No new hardware task was requested. The current grouped native path, installed
v21 DMA and matched performance remain unqualified. Historical per-slice H20
results do not qualify the new group path. Fewer event calls are not a measured
speedup or complete P5/TENT parity. OrbitKV integration remains deferred.


## CUDA IPC stream pool

Source `28c673f` moves NVLink stream/event ownership from the first local
allocation to a transport-owned `CUDAStreamPool` with exclusive
`CUDAStreamHandle` leases. Actual context identity controls reuse; a default
eight-slot global budget includes cold construction and quarantined resources.
Idle contexts can be replaced after native close; capacity waits use the request
deadline without holding allocation access locks. The pool mutex never spans
native work. Requester close drains and closes the pool after import cleanup.

Focused source checks pass three pool tests and 27 IPC-related tests (including
the two NVLink protocol tests), plus all-target Clippy with RDMA/CUDA. The existing changing-owner control
now proves eight consecutive pinned allocations reuse one stream/event, release
every completed host owner, and retain the pool until requester close. A later
GPU operation on the same actual context uses the same pool. New controls cover
concurrent cold capacity, deadline waits without memory locks, context replacement,
close fencing and one-shot uncertain construction/destruction. Existing copy,
record, wait and restoration controls check clean reuse versus quarantine.
These are CPU ownership/API-count observations, not native CUDA performance.

Evidence directory: `/root/orbitte-artifacts/nvlink-stream-pool-20261101/`.
`source-manifest.json` binds file hashes; `gates.json` records commands and log
hashes. Independent review and final CI are recorded there before PR readiness.
This review used TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`; its lazy
per-device stream pool and move-only handles supply the naming and ownership
pattern. OrbiTE adds actual-context matching, a cross-context slot bound,
reusable completion events and explicit quarantine/close ownership.

No hardware task was requested. Current installed v21 DMA, cross-context native
cleanup and matched throughput remain unqualified. At `28c673f`, group
completion still used a synchronous wait. The following event-progress stage
supersedes that wait; native CUDA batch-copy dispatch remains separate work. Earlier H20 evidence does not qualify this new pool, and
reduced native construction counts do not establish a speedup. Full P5/TENT
parity and OrbitKV integration remain open.


## CUDA IPC event progress

Source `0daa0a1` adds an inline ready-event fast path and one lazy transport
completion worker for pending events. A group's actual
whole-allocation GPU/pinned loans and exclusive stream lease survive NOT_READY;
ready query and owning-context restoration precede release. The caller retains
its import, source grant and receipts while the worker polls. Original deadlines
bound conflicting NVLink owner waits. A still-pending deadline or query/restore
failure retains and fences actual touched owners instead of synchronizing or
reporting a false drain. Requester shutdown drains callers and imports, stops
progress, then closes the stream pool.

Focused source gates pass three stream-pool tests, 29 IPC-related tests and ten
native-observer/validator controls, plus format and RDMA/CUDA all-target Clippy.
The existing changing-owner control observes zero worker creation for immediate
completion; the concurrency control requires both groups actually enter the
shared poller before releasing one. The new public-engine controls hold one GPU event pending while another pinned
host group completes on the same worker, verify exact bytes, running cancellation,
owner-wait deadline and close retry, and preserve actual owners on pending
deadline/NOT_READY context-restore failure. Existing mixed/reversed-owner,
source-receipt and partial-failure controls remain in the affected gate. The
native observer now records query results outside staging too; its validator
requires the matching ready event generation, enqueue context restoration and
every query context restoration before close/free. Cross-thread re-recording
of the same event cannot provide completion for an older generation. Historical wait-based traces
remain readable. These are CPU shim/validator checks, not installed DMA results.

Evidence: `/root/orbitte-artifacts/nvlink-event-progress-20261101/`.
`source-manifest.json` binds source inputs and `gates.json` records commands and
log hashes. Initial failed checks are preserved separately. Independent review
and final CI are recorded in that directory before PR readiness.

Frozen TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` retains task streams/events
in `NVLinkSubBatch` and calls `cudaEventQuery` from `getTransferStatus`. OrbiTE's
shared worker is the runtime adaptation of that completion lifetime. Setup,
control receipts and each Batch's FIFO execution are unchanged; this does not
claim a fully asynchronous scheduler, native batch-copy dispatch, automatic
producer-stream readiness or exporter-crash safety.

No hardware task was requested. Current installed v21 event-query DMA,
concurrency, native cleanup and matched TENT performance remain unqualified.
Historical H20 results do not qualify this changed completion path. Query API
counts and CPU shim concurrency are not native latency/bandwidth measurements.
Full P5/TENT parity and OrbitKV integration remain open.


## CUDA IPC native batch copy

Source `d6b564f` adds optional CUDA 13 `cuMemcpyBatchAsync_v2` dispatch for
independent NVLink groups. The driver probe checks the reported version and
exact versioned symbol against generated `cudarc` 13.0 types. Copy descriptors
retain request order; sorted address intervals reject overlapping writes and
source/destination dependencies while allowing shared reads. Ineligible groups
retain ordered enqueue. Both paths retain actual owners, the exclusive pooled
stream, trailing event, shared completion progress and source receipts.

Focused gates pass two driver tests (including a compiled native ABI/version
control), three stream-pool tests, 32 IPC-related tests and 11 native-observer
controls, plus formatting and RDMA/CUDA all-target Clippy. New CPU controls use
a shim that deliberately executes batch members in reverse order and can fail
after a non-prefix subset. They verify READ/WRITE bytes and guards for mixed
GPU/pinned owners, read aliases, ordered overlapping writes, unavailable
capability, cancellation prefixes, no replay, no fabricated completed prefix,
and retained owners after uncertain completion. The native observer forwards
the exact v2 entry unchanged and records one call plus its descriptors; the
validator binds those descriptors to one group and its actual event generation.
This is source/CPU evidence, not execution of a native GPU batch.

Evidence: `/root/orbitte-artifacts/nvlink-batch-copy-20261101/`.
`source-manifest.json` identifies the checked files; `gates.json` records
commands, exits and log hashes. The initial failed assertion is retained in
`initial-tests.log`; it used the wrong existing shim event label and was corrected.
Independent review and final CI are recorded in the same external directory.

Frozen TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` uses
`NVLinkSubBatch` / `startTransfer` arrays, stream-ordered source attributes,
`cudaMemcpyBatchAsync` and one trailing event. OrbiTE implements the CUDA 13
Driver API counterpart, selects fallback before submission, and preserves
ordered dependent-copy semantics. A failed native call never triggers replay.

No hardware task was requested. Current installed batch-copy DMA, native
partial-error behavior and matched TENT latency/bandwidth remain unqualified.
The functional call-count reduction is not a measured performance result.
Producer-stream readiness, broader P5 profiles and full TENT parity remain open.


## CUDA producer readiness

Source `33ef0b1` implements explicit GPU producer-stream handoff on the actual
allocation. `CallerSyncEvent` records on the caller thread, retains producer
inputs and shares one event generation across registration aliases. Copies,
staging, DMA loans and IPC source/requester access consume it once before
payload access. Re-record and final allocation drop drain the previous marker.
Failed record can recover only on the original producer stream; unproven
readiness or context restoration fences and retains actual owners.

Focused checks pass 124 CUDA tests, two NVLink transport tests, formatting and
RDMA/CUDA all-target Clippy. The four new readiness controls exercise queued
bytes/guards, cross-thread per-thread-stream capture, work after a marker, event
reuse, owner lifetime, public TCP READ/WRITE and copy/staging/IPC/DMA boundaries.
Failure controls cover record recovery, wait/context restoration, graph capture,
foreign-context rejection and final drop. Independent review found that the
initial test shim incorrectly resolved explicit streams through the current
context. Test-only source `6909d69` records creation contexts and adds explicit
stream calls with no current context or a different current context. Affected
CUDA controls and Clippy are rerun against that correction.

Evidence: `/root/orbitte-artifacts/cuda-producer-readiness-20261101/`.
`source-manifest.json` and `gates.json` retain initial source inputs/results;
`context-control/` records the corrected source and affected gates. The initial
byte-buffer type assertion failure and independent context-model finding remain
archived. Independent review, final CI and the accepted source/head identities
are recorded in the same external directory before PR readiness.

Frozen TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` uses
`CallerSyncEventPool` / `syncWithCallerStream` to record `cudaStreamPerThread`
and wait on the CPU. OrbiTE preserves that scoped wait, accepts the actual
explicit stream, and attaches the generation to allocation ownership across
transports. Consumption is CPU-blocking and cannot be interrupted by a request
deadline. This avoids device-wide producer synchronization but is not a measured
speedup or an asynchronous consumer-stream dependency.

No hardware task was requested. Native producer ordering, installed DMA and
matched TENT performance remain unqualified. At `6909d69`, pinned host handoff
remained open; the following [pinned substage](#pinned-host-producer-readiness)
implements it. Consumer-stream chaining is not exposed; broader P5 profiles and
full TENT parity remain open.


## Pinned host producer readiness

Source `eb609b4` extends explicit producer handoff to owned CUDA-pinned host
memory. `HostMemory::record_producer_stream` checks shared CPU/DMA/IPC exclusion;
the actual `PinnedAllocation` lazily owns the same `CallerSyncEvent` used by GPU
allocations. CPU reads/writes, staging access, host DMA and local IPC consume the
marker before bytes are touched. Final free/unregister drains pending readiness
first; unproven producer completion retains native backing and producer inputs. Ordinary/external
host wrappers are rejected. This profile uses the allocation's native context.

Focused gates pass 128 CUDA tests, 15 memory-owner tests, formatting and
RDMA/CUDA all-target Clippy. Four new controls cover actual DtoH bytes/guards,
per-thread/explicit streams, cross-thread registration aliases, later producer
work, one reusable event, all four public TCP host roles, staging, IPC READ/WRITE,
DMA admission, existing loan rejection and final-drop/failure retention. These
are source/CPU controls, not real GPU execution. Allocations without a producer
create no new event; consumed generations are not waited again by subsequent aliases.

Evidence: `/root/orbitte-artifacts/pinned-producer-readiness-20261101/`.
`source-manifest.json` and `gates.json` bind commands, source and log hashes.
The initial test compilation error is retained in `initial-tests.log`; its IPC
export fixture was corrected to consume the move-only owner. Final controls
also require the actual `event-sync` and `unpin` events when checking drop order.
Independent review and final-head CI are recorded in this directory before
PR readiness. No new hardware task was requested.

Frozen TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` records a reusable
caller-sync event and waits on the CPU before NVLink submission. OrbiTE reuses
that event contract at actual pinned-owner access across transports, keeping
the common state machine. Consumption remains CPU-blocking and deadlines cannot
interrupt it. No throughput/latency gain is claimed. Native producer ordering,
current installed DMA and matched TENT performance remain unqualified; full
P5/TENT parity and OrbitKV linkage remain open.


## CUDA pointer and location

Owner-backed range/type/device/context inspection and batched external CUDA
validation are implemented. The native context caches successful GPU PCI/NUMA
discovery for both staging entry paths while allowed nodes are refreshed. See
[the API](rust-api.md#memory-type-and-location) and [staging selection](staging.md#host-staging-location).

Source controls exercise interior-pointer adoption, clipped registrations,
retired owner lifetime, metadata inspection without payload access, unknown,
host/managed and driver-error rejection, and one attribute call per adoption.
The existing staging control now checks one PCI query across repeated copies
and a changed allowed-node set, with unchanged physical chunk accounting.
Ordinary/pinned host classification and bound mapping metadata are covered.

Evidence is outside Git at
`/root/orbitte-artifacts/cuda-memory-location-20261101/`.
The manifest and gate records bind source, commands and log hashes; initial
fixture compilation failure is retained in `cuda-initial.log`. Independent
review and final-head CI are recorded there before PR readiness.
No new hardware job was requested. Reduced native query counts establish the
code path, not an end-to-end latency, bandwidth or matched TENT result.
Current installed GPU execution, physical NUMA residency and performance remain
unqualified. Full F02/F03, P5 and TENT parity remain open.


## CUDA topology and RDMA selection

Native RDMA discovery with CUDA enabled now publishes visible `cuda:N` entries,
validated PCI identities, optional NUMA placement and GPU-to-RNIC priority tiers.
The existing selector consumes registered owner locations for ordinary and
sliced transfers. Source path publication enforces its own locality, while
compatible same-name pairing remains preferred. Strict NUMA rejection is
independent of PCI rank and explicit masks; unknown locality remains eligible.

The topology review used TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`,
`platform/cuda/cuda_probe.cpp`, `runtime/topology.*` and
`transport/rdma/quota.h`. OrbiTE follows its three-tier discovery and
`strict_local_numa` eligibility contracts. Invalid/unknown PCI distance cannot
win the nearest tier. Immutable rank arrays are compiled once, so per-slice
selection performs no native discovery, string construction or tier scans.
This establishes the implementation path, not a measured performance advantage.

Focused source controls cover visible ordinal-to-device/PCI mapping, same-bridge
and same-NUMA ranking, unknown PCI metadata, atomic publication on driver error,
strict cross-NUMA rejection despite preferred rank or a missing CPU inventory
row, device masks and fixed slot holes. Context owners keep CUDA-visible ordinals
separate from native device handles for external adoption and primary release. Public CUDA READ/WRITE controls move and check full bytes/guards through
multi-slice RDMA, including different GPU nodes on the two peers and explicit
NUMA overrides. Existing native-owner retention and byte-charge controls remain
in the affected RDMA gate. These are CPU/native-shim controls, not GPU execution.

Evidence: `/root/orbitte-artifacts/cuda-topology-selection-20261101/`.
Source manifests, commands, gate logs and independent review bind the exact
source. The initial fixture compilation diagnostic is retained separately;
CUDA enum result values corrected those callbacks before execution. Final-head
CI and documentation checks are recorded before PR readiness. No new hardware
task was requested. Current installed GPU/RNIC topology, real placement/routing
and matched TENT latency/bandwidth remain unqualified. Custom priority matrices,
bandwidth/QoS policy and full F02/P5 parity remain open; OrbitKV linkage is deferred.


## Custom topology input

F02 custom topology configuration is implemented for the RDMA inventory. The
reference was TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`,
`runtime/topology.cpp` (`parsePriorityMatrix`, `parseCustomTopology`,
`loadFromConfig`) and `transport/rdma/quota.cpp` candidate construction.
OrbiTE accepts the two JSON forms and inline-over-file precedence, resolving
them against physical discovery instead of replacing device identity. Invalid
explicit configuration is rejected instead of silently falling back. These
differences preserve stable context IDs and known physical NUMA restrictions.

Focused source controls cover rank/index remapping, explicit and synthesized
wildcards, duplicate/unknown entries, input size, file errors, atomic publication
and initialization failure before contexts or MRs. Existing public READ/WRITE
controls exercise configured host rails with a failed context hole and GPU
slices with a deliberately preferred cross-NUMA NIC. Strict locality, device
masks, byte integrity, guards and grant charges remain checked. The CUDA/native
callbacks are CPU controls and do not establish hardware execution.

Evidence is external at
`/root/orbitte-artifacts/topology-priority-matrix-20261101/`: exact-source
manifests, focused gate logs, independent review and CI status. Configuration
parsing and name/index resolution occur only at context initialization; hot-path
selection uses the existing compiled ranks and eligibility masks. This is a
source property, not a measured latency or bandwidth improvement. No hardware
job was requested. Installed custom routing, physical placement and matched
TENT performance remain unqualified. F01 environment/config compatibility,
bandwidth-aware selection, full P5 and OrbitKV integration remain open.


## RDMA bandwidth selection

P6/F16 per-slice bandwidth-aware selection and F20 local NIC snapshots are
implemented against frozen TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`,
`transport/rdma/quota.{h,cpp}`, `workers.cpp`, `context.cpp` and the common
`ib_link_speed.h`. One selector retains configured topology and physical
charges; per-NIC owners hold separate selection EWMA and posted-busy-time
transmit estimates. Native port-rate refresh seeds both without replacing QPs.
That delivery used legacy speed/width. The subsequent
[effective-rate delivery](#rdma-effective-link-rate) adds optional LAG queries
and header-supported extended XDR encoding. See [the contract](rdma-cuda.md#bandwidth-aware-rdma-selection).

Focused source controls cover heterogeneous-rate selection, baseline round
robin, mask/NUMA eligibility, retained charges, partial and uncertain posts,
failed completions, complete same-timestamp CQ pass accounting, idle-time
exclusion, old-generation sample rejection and
rate-only monitor refresh. Public READ/WRITE controls exercise the actual
engine/selector/SQ/CQ path through CPU verbs callbacks and inspect both peers'
snapshots. They establish source behavior, not RNIC throughput. Existing RDMA
and CUDA ownership checks cover the affected resources and combined-feature
build. No hardware task was requested.

Evidence is external at
`/root/orbitte-artifacts/rdma-bandwidth-selection-20261101/`: source manifests,
commands, focused logs, final CI and independent review. The initial monitor
control's two-second grant expired while waiting through three monitor periods;
that failed log is retained. The control now admits its grant with a sufficient
explicit deadline; production drain deadlines are unchanged.

Sampling reuses existing SQ/CQ owners, a per-NIC meter lock, an atomic selection
estimate and one clock read per CQ polling pass. This establishes the hot-path
design; measured sampling overhead, installed routing/DMA and matched TENT
latency/bandwidth remain unqualified. This delivery covered per-slice selection;
the subsequent whole-parent implementation is recorded below. Basic worker arbitration is recorded in [the later priority delivery](#rdma-request-priority); shared
QoS and full F16 parity remain open. OrbitKV integration
is deferred.


## RDMA weighted multipath

P6/F16 whole-parent weighted distribution follows frozen TENT
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` `quota.cpp`:
`buildCandidates`, `selectSinglePath`, `selectMultiPath` and its exact folded-tail
charge loop. `quota/allocation.rs` computes one inverse-score assignment,
commits all physical byte reservations together and transfers each reservation
to its actual grant. Fixed 64-entry arrays bound planning without a new heap
allocation. The every-100th-parent probe uses a shared selector counter and
rotating starts, so short parents can sample beyond the first eligible NICs.
Only available compatible candidates pass; physical fault recovery stays with
the existing monitor. See [the contract](rdma-cuda.md#bandwidth-aware-rdma-selection).

Focused CPU controls cover unequal rates, backlog, exact folded tails, partial
consumption, atomic overflow rejection, concurrent reservations, masks, failed
context holes, strict NUMA and probe rotation. Public READ/WRITE controls use
the existing engine and CPU verbs fixtures to verify 41-byte parent transfers,
guard bytes, selected peer paths and exact per-NIC payload totals, including
remote single-rail restrictions. Existing cleanup/retention and staging controls
remain the lifetime gate. Source checks do not establish native DMA throughput.

Evidence is external at
`/root/orbitte-artifacts/rdma-weighted-multipath-20261101/`, including source
revisions, commands, logs, CI and independent review. Installed routing,
throughput, fairness and a matched TENT performance comparison remain
unqualified. No Forge task was requested. Effective LAG/extended speed queries
and basic worker arbitration are implemented in the subsequent deliveries below;
shared QoS and complete F16/P6 parity remain open.


## RDMA effective link rate

F16 rate discovery follows frozen TENT
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` `context.cpp`
(`recordPortSpeed`, `queryEffectiveSpeed`, `refreshPortAttributes`) and
`ibv_loader.cpp` optional-symbol handling. The existing `ibverbs-sys` dependency
and native resource owners remain. A thin C shim calls the system header's
port-query inline and returns only a generated fixed-width result. No Rust
mirror of `verbs_context` is maintained. Extended-speed support follows the
installed header; older headers keep the legacy encoding. Effective speed is
loaded once from the runtime library and converted from 100 Mb/s units.

Source controls cover the C-header provider boundary, propagated query errors,
optional-symbol absence, output units/overflow, XDR decoding and unknown values.
The monitor control exercises degraded LAG rates, transient query errors,
recovery and zero/unavailable fallback through the existing context/selector;
health checks do not learn speed and rate changes preserve connection identity.
These controls use CPU fixtures and do not establish native provider behavior.

Evidence is external at
`/root/orbitte-artifacts/rdma-effective-link-rate-20261101/`, preserving the
initial compilation diagnostics, source checks, package build and review.
Native LAG/XDR behavior, installed DMA, throughput and matched TENT performance
remain unqualified. No hardware task was requested. Request priority and basic
worker arbitration and the installed verification scope are recorded below.


## RDMA rail health

Two fake RNICs drive the real path picker, native posting and CQ retirement.
The controls check masked cooldown rejection before a post, guarded payloads
on the healthy NIC and exactly one successful recovery probe. A deterministic
clock covers burst errors, failed trials, backoff and stale completions. The
packaged gate requires two inventory-locked RNICs, a separate control route,
and permission to bring the explicitly selected requester RoCE data link down
and back up. The supervisor verifies restoration before releasing its locks.
Physical link-loss and recovery remain unqualified.

## RDMA request priority

Source implementation adds `TransferRequest::priority` and opt-in
`RdmaConfig::deadline_bw_arbitration`. TENT's frozen priority constants,
worker post ordering and bounded greedy deadline/bandwidth formula are the
reference. [The dispatch contract](rdma-cuda.md#request-priority-and-bandwidth-arbitration)
records scope and deliberate differences.

The recorded controls covered ordinary/sliced public requests retaining Medium/Low,
the former TCP/NVLink preflight rejection, actual worker dispatch across two contexts under
one shared CQ credit in polling and event-driven modes, FIFO versus enabled
urgency ordering, posted backlog,
incremental candidate backlog and the bounded tail. These use CPU verbs controls
and real Rust owners; they are not native DMA or performance evidence.
Existing RDMA completion, event, failure and drain gates remain the regression
consumer. Artifacts and independent review are stored outside the checkout in
`/root/orbitte-artifacts/rdma-priority-arbitration-20261101/`.

The current TCP and NVLink paths accept Medium and Low as advisory. Loopback
tests check full bytes for explicit TCP requests and a QoS-resolved Low
prefetch with no RDMA candidate. The CUDA IPC shim checks mixed-priority
NVLink copies, completion, guard bytes and resource release.

Current installed native contention, fairness, throughput and scheduler overhead
remain unqualified. At this delivery, priority aging and shared-process slots
were open; the following delivery implements them. Full P6 parity remains open.
No new hardware allocation was requested for this stage.


## RDMA promotion and shared slots

Source implements frozen TENT's 10 ms priority aging, 1 ms checks, default
head-only tier promotion and opt-in per-entry promotion. A persistent destination
FIFO ordinal protects ordering across context rotation and queue reconstruction.
Each configured RDMA worker pool publishes one of the three priority slots
in shared memory. Participants join the same clock domain.

Focused worker controls exercise both promotion policies, original-age behavior,
stale namespaces, opposite context collection order and repeated reconstruction,
actual priority-gated native-post calls, and event-driven progress from a Low
submission with no initial CQE. The long High-slot control isolates promotion;
the short-slot run checks bounded completion, not a strict per-boundary timing
negative control. Real local subprocess controls cover same-object attachment,
conflicting intervals and a killed initializer. Two child processes charge
independent NIC leases on one tmpfs object. One is killed under the publish
lock; the survivor recovers the lock and all priority tiers, retaining its
own exact byte charge until release. Clock-domain, corrupt-slot and
incompatible-size controls fail without modifying the object. Native
verbs remain CPU controls; these processes do not establish RDMA hardware or
installed-package fairness. Default/RDMA/CUDA source gates, Clippy and strict
documentation checks remain the regression gates. Logs and independent review
belong outside the checkout at
`/root/orbitte-artifacts/rdma-promotion-slots-20261102/`.

This implements the reference's cooperative priority slots, not cross-process
byte quotas, tenant isolation, rate caps or strict per-WR timing. The name persists
after owner exit and requires all participants to stop before administrative
removal. Installed qualification, native contention/fairness and measured
scheduler overhead/throughput remain unqualified. No hardware job was requested.


## Transport retry

Real TCP fault injection counts attempted payloads and lost acknowledgements.
The RDMA sys shim distinguishes a clean bootstrap rejection, a drained CQE
failure, a partially posted WRITE and a retained QP. Destination bytes and
guards are checked on the original public mapping. CUDA and file shims
exercise staged and per-provider receipts. Native RNIC failover remains
unqualified pending the installed gate above.

## Runtime admission and dispatch window

Source implements opt-in `LocalTransferAdmissionQueue` and an independent
owner/byte dispatch window, corresponding to frozen TENT's admission and window
refill owners. Physical requests can carry multiple public task IDs. Real TCP
controls observe the exact number and length of payloads, per-owner credit and
QoS, absolute task mapping, cancellation, guards and a lost WRITE receipt.
The RDMA sys shim drives two NIC rates through the production selector and
queue. It checks both a zero-post drop and a successful payload on a different
rail. Cancelling a dispatched RDMA alias twice drains the shim's two accepted
work requests, suppresses the remaining slices and preserves destination guards.
A real proxy session exercises reserved capacity through its CUDA shim.
The packaged two-RNIC gate above checks a zero-post deadline drop followed by two shuffled aliases under a one-owner cap on each selected rail. It has not been run. Physical NIC deadline predictions and workload fairness remain unqualified.

CPU controls use real TCP writes and notification receipts to distinguish FIFO
from EDF, preserve a later same-Batch group's dependency despite its earlier
deadline, and drain accepted work after requester close. Focused queue controls
cover equal-deadline FIFO ties, independent byte/count limits and head-of-line
byte blocking. Public batch controls cover preflight oversize rejection,
queued cancel/expiry with zero I/O, empty submissions and a canceled later task
preserving its completed prefix. Existing RDMA cleanup-failure and mixed
GPU/RDMA/TCP controls additionally consume the queue and retain dispatch charges
with actual uncertain owners. Native calls in those controls are CPU fixtures.

Logs, source archive and independent review are external at
`/root/orbitte-artifacts/runtime-admission-20261102/`. Default and combined
source gates and exact-commit CI are recorded in that handoff. Installed
multi-transport contention, fairness and measured scheduler overhead/throughput
remain unqualified. No hardware job was requested for this delivery.

## Receiver credit gating

Source connects frozen TENT's cumulative `SenderCreditLedger` accounting model
to actual sync/async transport handoff and runtime queue eligibility. The frozen
ledger has no transport consumer and ignores TTL; OrbiTE's consumer and
freshness gate are explicit additions. Applications deliver grants through the
local Rust API. The automatic issuer handles negotiated receiver windows
separately. The manual ledger accepts partial grants of all four resources
and exposes each balance. Physical transfers charge bytes and request slots.
Remote Pin owns the actual staging and consumer resources at the receiver.

CPU controls cover atomic four-resource reservation and pre-handoff rollback,
concurrent consumption at the grant boundary, invalid/partial
and old/gapped cumulative updates, freshness without duplicate renewal, epoch
and source-session fencing, eligible large-head byte ordering and TTL-driven
bypass without updates/releases, real two-peer TCP
credit bypass and update wakeup, draining after close, cancel/expiry before I/O,
unstarted refund and no refund after transport handoff (including a canceled
multi-request suffix). A real TCP peer applies a WRITE and loses the final
acknowledgement. The sender retains the committed grant and rejects both
deactivation and epoch replacement after local drain. The existing RDMA
cleanup-failure control retains the actual uncertain native owners, dispatch
window and credit binding; deactivation and epoch replacement stay busy. Native
verbs in this control are a CPU ABI fixture, not hardware DMA evidence.

Logs, source archive and independent review belong at
`/root/orbitte-artifacts/receiver-credit-20261102/`. Source/CI gate results are
recorded in the handoff. Native contention/fairness, measured overhead and
throughput remain unqualified. No new hardware task is requested.

## QoS contract consumption

Source implements typed `QosContractResolver` overlays and immutable engine-bound
`EffectiveQosPolicy` handles. Frozen TENT supplies the resolution model only;
OrbiTE additionally feeds effective priority into existing transports and
atomically reserves permits for physical owners against their compiled
contract. The frozen resolver has no parent-charge tree or cluster allocator.

Four focused CPU controls cover overlay/zero/normalization rules, bounded static
slots, mixed-contract all-or-nothing admission with a later slot already full,
independent owners sharing one contract, repeated and unknown-name resolution sharing
budgets, implicit default/unspec enforcement, foreign policy rejection, clean
release after real TCP writes, credit-wait cancellation/expiry and the difference
between returning inflight QoS budgets and retaining committed cumulative credit.
Existing public ordinary/sliced RDMA priority controls now consume default and
explicit resolved policies before native post. The existing uncertain native
cleanup control retains actual backing, dispatch, credit and QoS ownership.
Native verbs in these controls are CPU ABI fixtures.

Logs, source archive and independent review are external at
`/root/orbitte-artifacts/qos-contract-20261102/`. Exact-commit source/CI results
belong to its handoff. JSON policy fields, automatic credit and admission
actions have CPU controls. Hardware fairness and throughput remain unqualified. No hardware job was requested and no performance advantage
is claimed.


## Automatic receiver credit windows

Source implements bounded receiver issuance and demand-driven cumulative control
synchronization on OrbitTE wire v22. `ReceiverCreditIssuer` reserves per-sender
windows from aggregate capacity, and `SenderCreditLedger` replenishes only from
successful actual-owner release. This is cooperative public-submission credit,
not physical RQ/buffer accounting or native pacing. Frozen TENT supplies the
sender-ledger model and has no receiver/network consumer.

Four focused CPU controls cover the new boundaries: one issuer state table
checks conservation, replay and terminal close; one public TCP WRITE control
checks direct and EDF queue waits, no return before actual Drop and reuse of
remaining grants; two real control-TCP relay cases drive the sender ledger to
check pending identity, alias/manual-mode rejection, lost Open/Close replies,
sync cancellation and completion arriving during an in-flight sync. The lost-Open
case also recovers through the public engine API after requester admission closes.
Two existing public RDMA READ CPU ABI controls now retain automatic credit for
requester Unquiesced and remote native-cleanup failure even when the requester is
locally drained. Native verbs in these controls are fixtures, not hardware DMA
evidence. No new hardware task is requested.

Logs, source archive and independent review are external at
`/root/orbitte-artifacts/receiver-credit-issuer-20261102/`; its handoff records
exact-commit gates. Fixed windows do not redistribute idle capacity. Failed or
crashed senders retain authority until a separate reclaim proof exists; terminal
tombstones remain bounded and do not reopen identities. Generic QoS-class
mapping and adaptive sizing have no frozen receiver-ledger
counterpart. Physical fairness and throughput remain unqualified.


## Staging and consumer owner credits

This source substage connects automatic remote Pin admission to the actual
ProxyManager staging owners. Wire v23 adds sender session/issuer epoch to Pin;
CreditSync/Close and Delegate/CROSS/Unpin bodies remain unchanged. Atomic source
admission charges negotiated depth and one session, then transfers stage tokens
before native construction. Only actual cleanup returns capacity; each Pin's
child-owner count prevents a consumer from returning after hidden Drop retention.
Manual updates accept the four named grants. The frozen engine supplies no mapping from arbitrary application objects to those grants.

Focused controls use real control sockets and CUDA/verbs ABI fixtures: negotiated
depth differs from the requester's proposal; TCP and RDMA READ/WRITE ordered
owners already committed finish after a requester fence; a later owner cannot start,
and missing bindings cannot bypass admission;
partial native allocation retains its one constructed stage and the consumer;
unassigned reservation rollback, aggregate windows and busy-Close fencing preserve
capacity. Sender payload reports cannot release source resource owners. Existing
staging and cleanup gates remain part of CI. These controls do not measure GPU
execution, NIC behavior, native contention or throughput.

Evidence, failed attempts, exact source and independent review are outside Git at
`/root/orbitte-artifacts/staging-credit-owners-20261102/`; the handoff records
checksums and CI links. Frozen TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`
supplies resource tags and ProxyManager ownership, not this automatic receiver
protocol. Physical pool reservation, failed-sender reclamation, matched throughput
and installed v23 hardware qualification remain unqualified. No hardware job was
requested for this substage.


## JSON QoS configuration

The supported TENT `qos` JSON section now loads from a string or bounded file
into `QosConfig`. Parsing validates known fields and objects, disabled semantics,
normalized unique identities, integer/byte-unit limits and the existing contract
count bound. The original `307b0ac` loader rejected all action metadata; the
[subsequent action consumer](#qos-admission-actions) accepts delay/reject.
Unknown/unconsumed keys remain errors. Root engine settings outside `qos` remain
the caller's responsibility.

Two focused CPU controls cover input shape, inheritance, numeric/byte units,
disabled behavior, duplicates, limits and bounded file loading. The existing
public mixed-contract admission/retention test now consumes JSON; the existing
ordinary/sliced RDMA worker-priority control does likewise. These preserve real
policy consumers without duplicating their state machines. CUDA/verbs coverage
uses ABI fixtures; no new hardware task or performance measurement is involved.

Frozen reference `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` supplies the
`QosContractResolver::loadFromConfig`, priority and binary byte parsing behavior.
Unlike its resolution-only metadata, OrbiTE accepts only fields consumed by its
runtime. Rejecting duplicate normalized keys and non-object empty nodes is a
deliberate strictness difference. Native schema/action/API parity remains open.

Exact source, CI, independent review and logs are maintained outside Git at
`/root/orbitte-artifacts/qos-json-20261103/`. JSON loading adds no transfer-time
lookup, lock or network work; this is a design property, not a measured throughput
advantage. Installed v23 hardware and performance remain unqualified.


## QoS admission actions

The runtime consumes TENT-named `allowed_degraded_actions` for delay/reject.
Allowing delay requires the existing runtime queue. Every bounded contract in
that engine acquires atomically at its Batch head together with receiver credit
and the dispatch window. Without delay, the prior immediate admission path
remains. Pending owners retain global inflight capacity; acquired quota remains
charged until actual cleanup, including Unquiesced retention.

Two public TCP controls cover same-Batch dependencies under EDF, independent
work bypassing a QoS-blocked byte head, exact payload order, actual-owner release,
intrinsically oversized owners, mixed-slot rejection precedence, rejection after
enqueue, joint receiver-credit accounting, cancellation and deadline expiry.
Existing parser controls cover action normalization, inherited replacement and
unsupported actions. The existing RDMA shared-CQ cleanup-failure control now
uses a delay-enabled contract and verifies that actual retained native owners
still retain QoS, credit and dispatch charges. It uses CPU/verbs ABI fixtures.
A focused owner-retirement control reproduces an external wake in the gap between
real permit Drop and queue-metadata removal, and verifies that a retired owner
cannot reacquire quota. Both discovered pre-fix failures remain in external evidence.

Frozen TENT `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` supplies names and
field overlay semantics, not execution policy: its resolver is
`resolution_only`. OrbiTE's [defined behavior](async-batches.md#qos-admission-actions)
is explicit. Unsupported QoS fallback, recompute and compression actions
return typed configuration errors. The frozen resolver combines fields into
one effective budget.

Exact source, CI and independent review are recorded outside Git at
`/root/orbitte-artifacts/qos-actions-20261103/`. There is no new wire version,
background thread or per-slice policy allocation. No hardware task, installed
v23 qualification or matched throughput/fairness result is claimed.


## Transport policy selection

The [initial selector](https://github.com/OrbitInfra/OrbiTE/pull/118) established
strict hints and source-owned Describe capabilities. S6 exercises named and
intent-selected requests through TCP sync/async, NVLink grants, remote staging
and real file transfers. A two-device verbs shim checks completed bytes on
each NIC, connected QP indices and the SL/TC passed to native connection setup.
Mixed TCP/RDMA hints and A/B/A RDMA policies preserve public task IDs. An
unavailable masked NIC rejects before requester admission.

The installed gate runs sliced READ and WRITE on distinct named rails and QP
pools with both physical NICs enabled. It checks the packaged peers' payloads,
guards, per-NIC completion bytes, connected pool indices and drain receipts.
The build and run commands are in the [verification queue](#verification-queue).

Native-ABI fixtures run on CPU. Installed hardware routing, physical DMA,
bandwidth and latency still require their own qualification. See the
[selector contract and remaining limits](transport-selection.md).
