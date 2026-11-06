# OrbiTE agent guide

This file gives a coding agent the context, rules and work order for OrbiTE
while no GPU or RDMA machine is available. The goal for this period is to
implement every remaining in-scope capability at source level, with CPU
controls driving the production owners, and to defer hardware verification to
one later pass.

[docs/completion-plan.md](docs/completion-plan.md) remains the authoritative
status record and [docs/tent-parity.md](docs/tent-parity.md) remains the
requirement list. This file only fixes the order of work for the
hardware-free period. When a step lands, update the plan's existing stage text;
do not turn this file into a second status diary.

## 1. What OrbiTE is

- OrbiTE is the transfer engine for
  [OrbitKV](https://github.com/OrbitInfra/OrbitKV)'s KV cache. It replaces
  OrbitKV's earlier Mooncake TENT dependency, because OrbitKV needed many
  changes TENT did not allow.
- Every end-to-end cache path must run through OrbiTE: CPU DRAM, NVIDIA GPU,
  local SSD, within and across hosts, including GPU↔DRAM and GPU↔SSD.
- The capability and architecture reference is Mooncake v0.3.14, frozen at
  `OrbitInfra/Mooncake@e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c`, directory
  `mooncake-transfer-engine/tent/`. README, docs site, doc style, module
  boundaries and naming follow Mooncake/TENT. OrbiTE is Rust and must not
  depend on the Mooncake C++ runtime; the C++ build is only a test oracle.
- Platforms: CPU and NVIDIA CUDA only.
- In-scope transports (10): `TCP`, `HP_TCP`, `SHM`, `RDMA`, `NVLINK`, `MNNVL`,
  `IOURING`, `GDS` are core; `MPCOMM`, `FABRIC` are optional network modules.
- Excluded: `UB`/URMA, Ascend, Sunrise, TPU/PJRT, XPU/SYCL, MLU, Hylink,
  MemFabric and ROCm/HIP. Their ABI constants stay recognized and return
  unavailable. Never implement them or add their SDKs.
- Cluster discovery, membership, KV indexes and external metadata stores
  (etcd, Redis, HTTP) belong to the consumer. OrbiTE is point-to-point and
  keeps only the self-description handshake: segment descriptors,
  capabilities, registration keys and source epochs. Reject non-P2P metadata
  configuration and direct consumers to publish endpoint and epoch themselves.

Read the frozen source directly when a step names it, for example:

```bash
gh api "repos/OrbitInfra/Mooncake/contents/mooncake-transfer-engine/tent/src/transport/io_uring/io_uring_transport.cpp?ref=e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c" -H "Accept: application/vnd.github.raw"
```

`python3 tests/reference/run.py --fetch-reference --output <dir>` also builds
the frozen reference for the CPU comparison.

## 2. Where to start

Read [Current focus](docs/completion-plan.md#current-focus) for the active
step, [capabilities](docs/capabilities.md) for the existing owners and
[qualification](docs/qualification.md) for the limits of the evidence.
Check `gh pr list` before branching from current main. The work order is in §7.

## 3. Working mode without hardware

- Implement hardware-dependent code completely. Every native call goes through
  a fallible function table so CPU shims can drive the real owners. Existing
  patterns: `src/transport/rdma/sys.rs` (verbs), `src/cuda/driver.rs`
  (`libloading` of `libcuda.so.1`), with shims such as
  `tests/unit/cuda/shim.rs`.
- For each hardware-dependent step, also write its installed gate now: a
  public-API consumer under `tests/hardware/`, built from the packaged crate by
  `tests/hardware/build_installed.py`, plus its driver script. Do not run it.
  Record the step as "Implemented; installed profile unqualified" and add the
  exact gate command to the verification queue in
  [docs/qualification.md](docs/qualification.md).
- Paths that need no special hardware (io_uring on Linux, HP-TCP, SHM,
  C ABI, Python, TENT TCP interop, libfabric `tcp` provider) must be
  tested for real in CI. Only those can be called qualified on CI evidence.
- Never write "qualified" or "accepted" for a GPU, RDMA, GDS, MNNVL or fabric
  profile without an installed hardware run. Missing hardware means
  unqualified, not passed.
- Do not request hardware jobs. Do not edit the OrbitKV repository or its
  dependency on OrbiTE; consumer integration stays deferred (P11).

## 4. Engineering rules

Ownership (the core contract of this codebase):

- An admitted transfer keeps its actual source/destination backing, native
  registrations, staging buffers and budget charges until the backend proves
  it can no longer access them.
- Logical outcome and physical quiescence are separate. A timeout, deadline,
  accepted cancel, dropped handle or failed submit is not a release receipt.
- Failed native cleanup keeps owners and charges and returns
  `Error::Unquiesced`. Never retry a destroy that may have half-succeeded,
  and never free memory a device might still touch.
- Never replay an uncertain WRITE. Keep source epochs and generation fences.
- Every queue, thread pool, ring, cache and session table is bounded and
  charged against an explicit budget.

Code:

- Rust edition 2024, MSRV 1.85. Give shared dependencies semver-compatible
  requirements with explicit minimum versions. Commit Cargo.lock and run CI
  with `--locked` on stable and 1.85. Reserve exact pins for bindings whose
  compatibility requires a release-specific audit, and document the reason
  beside each pin in Cargo.toml. Prefer runtime `libloading` for native SDKs.
- New native modules are opt-in Cargo features, Linux only. A build without a
  feature must not load or start that module.
- `#![deny(unsafe_code)]` stays. Use scoped `#[allow(unsafe_code)]`, with a
  `// SAFETY:` comment on every unsafe block.
- No `unwrap()`, `panic!`, `todo!` or `unreachable!` in `src/`. Return typed
  `Error` variants.
- Mirror TENT's module layout: new transports go in `src/transport/<name>/`
  after `tent/src/transport/<name>/`. Keep resources with their owner; do not
  add forwarding facades or a separate coordinator.
- Record every deliberate difference from TENT (especially safety fixes) in
  [docs/architecture.md](docs/architecture.md) or the transport's guide, with a
  negative control test.
- Keep `VERSION` in `src/runtime/control_plane.rs` fixed at 1 before the
  first release. Deploy the same build on both peers. The header cannot
  detect a build mismatch. Keep
  `relay_tracks_the_public_engines_current_wire`
  (`tests/hardware/rdma_host/gate.rs`) passing. Do not put wire numbers in
  code comments or current-state docs outside the
  [wire policy](docs/tent-parity.md#wire-and-consumer-boundaries).
- Leave `tests/reference/tent_driver.cpp`'s list of 18 upstream transports
  alone; it disables everything except TCP in the oracle.

Tests:

- Private unit tests live under `tests/unit/`, mirroring `src/`, and are
  included by their owner:

  ```rust
  #[cfg(test)]
  #[path = "../tests/unit/engine.rs"]
  pub(crate) mod tests;
  ```

- Test the production owners through a real boundary or a CPU shim. Do not
  count a fake as a network, DMA or storage pass.
- Never skip silently. If a CI gate needs a kernel facility, use an explicit
  environment variable such as `ORBITE_REQUIRE_IO_URING=1` so that an
  unavailable facility fails the test instead of skipping it.
- Raw logs, traces and run reports stay outside Git.

Docs (Sphinx with MyST, strict build):

- For each step, update the existing sections of
  [completion-plan.md](docs/completion-plan.md),
  [capabilities.md](docs/capabilities.md),
  [qualification.md](docs/qualification.md) and
  [architecture.md](docs/architecture.md). Add a user guide page when a
  user-visible capability appears and put it in a `toctree` in
  `docs/index.md`.
- Edit current text in place; do not append a delivery diary.
- MyST heading anchors drop punctuation; README is not part of the Sphinx
  build and uses GitHub anchors.

## 5. Gates for every PR

Native dependencies (Linux): `.github/scripts/install-native-deps.sh g++ cmake
pkg-config clang-18 libclang-18-dev libibverbs-dev`.

```bash
cargo fmt --check
cargo clippy --locked --features "$FEATURES" --all-targets -- -D warnings
cargo test --locked --features "$FEATURES"
cargo publish --dry-run --locked --features "$FEATURES"
python3 tests/reference/run.py --rust-only --output "$OUT/cpu-trace"
```

Run these for `FEATURES` in `''`, `rdma`, `cuda`, `rdma,cuda` and every new
feature set, on stable and `+1.85.0`. Add new feature sets to the CI matrix in
`.github/workflows/ci.yml`.

Docs:

```bash
python3 -m venv /tmp/orbite-docs-venv
/tmp/orbite-docs-venv/bin/pip install -r docs/requirements.txt
/tmp/orbite-docs-venv/bin/sphinx-build -n -W --keep-going docs /tmp/orbite-docs/html
python3 scripts/check-docs.py /tmp/orbite-docs/html
```

## 6. PR workflow

- One step (or one coherent sub-step) per PR, on a branch such as
  `feat/p7-io-uring`. Use conventional commits. The maintainer squash-merges.
- A PR is "Implemented" only. It becomes "Accepted" after an independent
  reviewer (another agent or a person) inspects the diff and reruns the
  gates. Do not accept your own work.
- End each PR description with a handoff:
  - commit;
  - F/B requirement IDs;
  - TENT source files followed, and deliberate differences;
  - changed contracts and wire version;
  - commands run and their results;
  - installed gates written but not run;
  - remaining limits.

## 7. Work order

Each step names its requirements, the frozen TENT source to follow, the
OrbiTE owner, what to implement and what "done" means. `[CPU]` steps can be
fully verified in CI. `[GPU]`/`[RDMA]` steps end at source level plus a
written installed gate.

### S0 — API and safety hygiene [CPU]

Do this first: later steps add request fields and transports.

- Add `TransferRequest::new(transport, local_address, remote_address, length)`
  and builder-style setters for `qos`, `priority` and `remote_staging`. Mark
  `TransferRequest`, `Error` and `Transport` `#[non_exhaustive]`, then update
  docs, examples and `tests/hardware` consumers.
- Enable `clippy::undocumented_unsafe_blocks` and add the missing `SAFETY`
  notes (about half of the 153 unsafe blocks lack them, for example
  `src/transport/rdma/sys/qp.rs:268`).
- Emit `compile_error!` when a native feature is enabled on a non-Linux
  target. Today `cuda` fails on macOS with missing `SYS_mbind` in
  `src/platform/numa.rs:63`.
- Done when CI is green on every combination with no behavior change.

### S0b — Freeze the pre-release development wire [CPU]

- Set `VERSION` in `src/runtime/control_plane.rs` to 1. Comment that it stays
  fixed before the first release and both peers must run the same build.
- Preserve the eight-byte `ORBITTE\0` magic, every frame size and every field
  offset. The existing prefix validator rejects other versions with
  `Error::Protocol`.
- Use one named version constant in `tests/hardware/rdma_host/gate.rs` for
  the relay and its fixtures in `gate_tests.rs`.
- Test rejection of the old v23 header and all unsupported versions on CPU.
  Show that two engines from the same build still connect.
- State the rule once under [wire and consumer boundaries](docs/tent-parity.md#wire-and-consumer-boundaries),
  including when an explicit compatibility policy begins. Remove wire
  numbers from code comments and other current-state text. Preserve historical
  artifact labels in [qualification.md](docs/qualification.md).
- Finish when all CI feature combinations, the CPU oracle and the
  installed-consumer builds pass.

### S1 — P7.1 file segments (F05) [CPU]

- **TENT:** `runtime/segment*.cpp`, `segment_manager.cpp`,
  `segment_tracker.cpp`, `SegmentType::File` and `FileSegmentDesc`, and
  `file://` handling in `runtime/transfer_engine_impl.cpp`.
- **Owner:** `src/segment.rs` (plus a child module), with engine entry points
  for opening, closing and inspecting file segments. File segments belong to
  the core build and need no storage SDK.
- **Implement:**
  - TENT's file addressing: the request offset maps to a file offset;
    confirm the exact mapping against TENT.
  - Permissions and size/info.
  - Descriptor caching and refresh.
  - An fd owner retained by in-flight I/O.
  - Close as admission close plus drain.
- **Done:**
  - Real temp-file tests for open/close/info and range overflow.
  - A write to a read-only segment is denied.
  - Closing with I/O pending keeps the fd.

### S2 — P7.2 io_uring DRAM↔file (B07) [CPU, Linux]

- **TENT:** `transport/io_uring/io_uring_transport.cpp`:
  - Probe a ring at install; report unavailable when the kernel blocks
    io_uring.
  - Open with `O_RDWR|O_DIRECT`, falling back to buffered I/O with a warning.
  - Run an all-or-nothing preflight (opcode, segment, bounce allocation, SQ
    space) before touching the ring.
  - Use a 4 KiB-aligned bounce buffer for unaligned or CUDA sources.
  - Issue `readv`/`writev`.
  - Treat a short CQE as FAILED with the exact transferred prefix, and copy
    back only that prefix for READ.
  - Advertise `dram_to_file`, plus `gpu_to_file` when CUDA is present.
- **Owner:** `src/transport/io_uring/`, feature `io-uring`. Prefer a pinned
  pure-Rust `io-uring` crate over linking liburing; record the choice.
- **Implement:**
  - Ring ownership. TENT uses one ring per sub-batch; a bounded worker-owned
    ring is acceptable as a recorded difference.
  - Partial submission: a short `io_uring_submit` means some SQEs are in
    flight and must drain.
  - Best-effort cancel with `IORING_OP_ASYNC_CANCEL`; an accepted cancel is
    not physical termination.
  - Drain before releasing the fd, memory or bounce buffer.
  - Ring teardown with outstanding SQEs returns `Unquiesced`.
- **Done:**
  - Real-file DRAM READ/WRITE with full byte checks.
  - Unaligned addresses; both O_DIRECT (ext4) and buffered fallback (tmpfs).
  - EOF/truncation short reads; injected syscall errors.
  - Concurrent unregister/close, cancellation and partial submit.
  - CI runs with `ORBITE_REQUIRE_IO_URING=1` on GitHub-hosted Linux, so this
    profile can be qualified on CI.

### S3 — P7.3 CUDA↔file through DRAM staging [GPU]

- **TENT:** the CUDA branch of the same io_uring transport (bounce buffer plus
  platform copy).
- **Owner:** reuse the CUDA staging owners (`src/cuda/staging.rs`,
  `src/runtime/proxy_manager.rs`) instead of per-request `posix_memalign`, and
  record the difference.
- **Implement:**
  - GPU range loans.
  - Producer readiness through `CallerSyncEvent` before reading GPU memory.
  - CUDA copy completion before a WRITE is submitted.
  - For READ, copy back only the completed prefix.
  - Failures retain GPU and staging owners.
- **Done:** CUDA shim tests for delayed completion, copy failure and cancel;
  installed gate `tests/hardware/storage_cuda.rs` written (GPU plus local
  NVMe) and listed in the verification queue.

### S4 — P7.4 GDS/cuFile (B08) [GPU]

- **TENT:** `transport/gds/gds_transport.cpp`:
  - `cuFileDriverOpen`, including after fork.
  - Per-file `O_DIRECT` with `cuFileHandleRegister(CU_FILE_HANDLE_TYPE_OPAQUE_FD)`.
  - `cuFileBufRegister`/`cuFileBufDeregister` for registered memory.
  - `cuFileBatchIOSetUp` at the configured depth and 16 MiB slicing.
  - `cuFileBatchIOSubmit`, `cuFileBatchIOGetStatus` polling with aggregation,
    and `cuFileBatchIOCancel`.
  - Destroy a batch only when every IO is terminal; otherwise quarantine it
    (`cleanupQuarantinedBatches`).
- **Owner:** `src/transport/gds/`, feature `gds = ["cuda"]`. Load
  `libcufile.so.0` through a fallible table, as `src/cuda/driver.rs` does.
- **Implement:**
  - Buffer registrations survive while any batch references them.
  - A quarantined batch is an `Unquiesced` owner that keeps its memory.
  - Detect and record compatibility versus native mode; a native-only profile
    disables compat mode.
  - Never fall back silently to io_uring.
- **Done:**
  - Shim tests: partial submit, status errors, cancel, quarantine and
    deregister failure retention.
  - Installed gate `tests/hardware/gds.rs` written, requiring native-route
    evidence (for example cuFile logs or nvidia-fs counters).

### S5 — P7.5 build isolation, BufIo audit and storage docs [CPU]

- CI proves the default, `rdma` and `cuda` builds neither contain nor load
  storage modules, and that file transports report unavailable when their
  feature is off.
- Audit `transport/bufio/bufio_transport.cpp`; the frozen loader does not
  register it. Document it in tent-parity.md and add no public `BUFIO`.
- Add `docs/storage.md`, the capabilities rows and the architecture section,
  and change the planned SSD path in `docs/_static/architecture.svg` to
  implemented.

### S6 — P6 selector contract (F12, F13) [CPU]

- **TENT:** `runtime/transport_selector.cpp`, `runtime/qos_contract.cpp`; tests
  `transport_selector_test`, `transport_hint_test`, `qp_pool_layout_test`.
- **Status:** PR #118 adds `Transport::Unspec`, TENT-style first-match rules
  over original CPU/CUDA memory type, inclusive size and effective priority,
  owner-derived capabilities in Describe replies and strict explicit hints.
  Build on it; implement only what it leaves open from the list below.
- **Implement:**
  - Policy order and named policies, all request intents and priority.
  - Segment-type, memory-type and size filters.
  - Explicit hints versus automatic selection, device masks and QP pools.
  - Unavailable-path errors.
- **Rules:**
  - Compile once at engine construction; no parsing on submit.
  - Consume the selection in real submission, including file transports and
    staging.
  - Merge only requests whose authority and policy fields agree.
  - Transport fallback, local recompute and compression stay rejected until
    S9 provides real owners.

### S7 — P6 explicit progress and notification fallback (F09, F11) [CPU]

- **TENT:** `runtime/progress_worker.cpp`; tests `progress_worker_test`,
  `poll_backoff_test`, `notification_fallback_test`, `local_notification_test`.
- **Implement:**
  - Explicit `progress_batch` and the observational polling option.
  - Bounded background wake/fallback timing and deferred batch retirement.
  - Notification fallback over control RPC with exactly-once logical
    delivery.

### S8 — P6 admission completion and merge (F08, F14) [CPU]

- **TENT:** `runtime/admission_queue.cpp`, `deadline_mlu.h`; tests
  `request_merge_test`, `admission_queue_test`, `runtime_queue_dispatch_test`,
  `coalesce_regions_test`.
- **Implement:**
  - Physical-request/alias granularity.
  - Merge eligibility and the public alias-to-owner mapping.
  - Staging reserves.
  - Deadline-infeasibility drop driven by a transport-aware bandwidth
    provider.
- Do not add a local-recompute callback; the frozen hook is empty.

### S9 — P6 failover, retry and rail health (F17, F18) [CPU + RDMA source]

- **TENT:** `commitPreparedSubmit` in `transfer_engine_impl.cpp`,
  `engine_failover_e2e_test`, `rdma/rail_monitor*`, endpoint teardown tests.
- **Implement:**
  - Submit-stage and completion-stage fallback.
  - Per-owner retry budgets and merged aliases.
  - No terminal resurrection.
  - Rail cooldown, probe and half-open recovery.
- **Proof:** before any replay, show that the earlier attempt can no longer
  modify the destination. Never replay an uncertain WRITE.
- **Tests:** drive these with the RDMA sys shim and TCP fault injection.

### S10 — P6 metrics (F20) [CPU]

- **TENT:** `metrics/tent_metrics.cpp`, `metrics/config_loader.cpp`,
  `common/qos_metrics*`.
- **Implement:**
  - TENT's metric names, types, labels and histograms.
  - HTTP/JSON/Prometheus output and config precedence.
  - A disabled mode whose overhead is measured.
- Keep it opt-in and keep secrets out of the output.

### S11 — P6 remaining quotas and credits (F15) [CPU]

- **Implement:**
  - Hierarchical and cluster quotas.
  - Adaptive receiver windows.
  - Reclamation for uncertain senders.
  - Manual resource mapping.
  - Explicit rejection of unsupported QoS actions and fields.

### S12 — P3 HP-TCP (B02) [CPU]

- **TENT:** `transport/hp_tcp/*` (7 files), `runtime/hp_tcp_transport_config.cpp`.
- **Implement:**
  - Worker-owned persistent lanes.
  - Bounded tasks, bytes and sessions.
  - Request IDs and generation-scoped registration IDs; remote permissions.
  - WRITE ACK and READ response accounting; timeouts, cancel and
    close/drain.
  - IPv4 rail validation, per-peer lane rotation, large-READ partitioning and
    static rail pairing; WRITEs stay on one lane.
- Keep TENT's exclusions: no GPU, TLS, per-WRITE striping, rail failover or
  ambiguous replay.
- **Done:** real-socket tests for fragmented frames, backpressure, partial
  READ/WRITE, stale registration after restart and cancel/drain. Physical
  multi-rail qualification is deferred.

### S13 — P3 SHM (B03) [CPU]

- **TENT:** `transport/shm/shm_transport.cpp`.
- **Implement:**
  - Shared mappings with path and offset; registration and permissions.
  - Overlap and alias handling.
  - Process lifetime and cleanup; the opt-in default.
  - CXL mount configuration (the CXL profile itself is deferred).
- **Done:** two-process same-host byte tests and unlink/unregister races.

### S14 — P5 MNNVL and EGM (B06) [GPU]

- **TENT:** `transport/mnnvl/mnnvl_transport.cpp`.
- **Implement:**
  - CUDA fabric-handle allocation, export and import. Expect
    `CU_MEM_HANDLE_TYPE_FABRIC` and `cuMemExportToShareableHandle` /
    `cuMemImportFromShareableHandle`, but confirm against TENT.
  - Opt-in selection, the EGM option and the supported directions.
- **Owner:** reuse the NVLink IPC owners in `src/transport/nvlink/` and
  `src/cuda/ipc/`.
- **Done:** shim tests plus a written NVL-fabric installed gate.

### R1 — Align with Mooncake v0.3.14

After S14, complete these PRs in order before starting S15. Base each on
current main after the preceding PR merges and stop for review after each.
The [v0.3.14 delta](docs/tent-parity.md#v0314-delta) assigns every TENT change.

- **R1a, reference.** Pin the release, verify the isolated native CPU trace
  and inventory the release delta. Keep historical evidence on its original
  revision. R1a changes no transfer behavior.
- **R1b, RDMA.** Selective signaling, adaptive congestion control core, rail
  refresh and wildcard mapping, QP access and read-depth limits, terminal teardown.
- **R1c, storage and CUDA.** io_uring pinned staging and drain, merged staging
  submission and shared lifetime tests, exact caller context and the legacy
  NVLink synchronous copy path.
- **R1d, HP-TCP, TCP and observability.** Least-loaded lanes, CPU requests to
  peer GPU TCP memory, metrics self-observation, lossless accounting, version
  diagnostics and the two RDMA boolean environment mappings.

For each port, map the upstream tests, preserve §4 with negative controls,
repeat timing-sensitive controls 50 times on Linux and use the §6 handoff.
Queue installed RDMA and GPU gates with exact commands. Those profiles remain
unqualified pending installed runs.

### S15 — P5 leftovers [GPU/RDMA]

- GPU source QP/MR reuse for GPU Direct RDMA.
- Reconcile the remaining ProxyManager resource/source control boundaries with
  TENT (`docs/staging.md`, "Bounded remote pipeline").
- The mixed direct/staged batch items still open under P5 in the plan.

### S15b — Local CUDA copy engine [GPU]

This is an OrbiTE extension for OrbitKV. Own local GPU↔pinned DRAM byte
movement. OrbitKV supplies block and layer layouts, cache tier policy and
codec compute. Keep the consumer contract in [local copies](docs/local-copy.md).

- Submit in order on the caller's CUDA stream. Complete through a CUDA event
  and retain backing and descriptor owners until the event proves completion.
  Leave the caller's stream unsynchronized.
- Support 1D, 2D strided and batched descriptors with `cuMemcpy2D` semantics
  on the caller stream and `cuMemcpyBatchAsync`.
- For highly fragmented batches, follow OrbitKV's
  `crates/orbitkv-core/src/transfer/{memcpy,kernel}.rs` and `kernel.cu` to
  coalesce contiguous ranges and equal-width strided rows for kernel copy.
- Verify byte ranges, event ordering and owner retention with the CUDA shim
  and write the installed GPU gate for the verification queue.

### S16 — P0 inventory and wire freeze [CPU, docs + fixtures]

- Map to acceptance cases:
  - the 33 C functions;
  - config keys, defaults and environment precedence;
  - capability bits, metrics and metadata;
  - wire messages;
  - the 88 reference C++ sources ending in `test.cpp`.
- Freeze, with small fixtures in source:
  - control RPC framing and serialization (`include/tent/rpc/rpc.h`,
    `src/rpc/rpc.cpp`);
  - the segment metadata format;
  - HP-TCP frames and provider bootstrap.
- Required before S17–S23.

### S17 — F01 configuration compatibility [CPU]

- **TENT:** `common/config*`, `config_lifecycle*`,
  `transfer_engine_config_override_test`.
- **Implement:**
  - `MC_TENT_CONF` as a file or inline JSON.
  - Legacy environment mappings and precedence; typed parsing.
  - Invalid root/path diagnostics.
  - Immutable per-engine snapshots; never mutate process environment to
    configure engines.

### S18 — F06 external metadata stores

Out of scope by design: the consumer owns discovery and etcd/Redis/HTTP; OrbiTE provides P2P self-description only.

### S19 — C ABI (F21) [CPU]

- **Implement:**
  - All 33 `tent_*` functions from the frozen
    `include/tent/transfer_engine.h`.
  - Exact constants, enum values, struct size and alignment.
  - The output allocation/free conventions.
  - Excluded transports recognized but unavailable.
- No panic crosses FFI. Add size/alignment fixtures.
- A separate crate is justified here (a real FFI/packaging boundary).

### S20 — C++ adapters and classic-TE shim (F21) [CPU]

- Provide source adapters for the native TENT C++ API and the TENT-backed
  classic TE (`src/transfer_engine.cpp` in the parent tree).
- Include the sync/wait helpers and explicit progress.
- Carry `policy_name`, `deadline_ns` and `intent_type` through a versioned FFI
  path.
- Never call the old TE backend.

### S21 — Python bindings (F21) [CPU]

- **TENT:** `src/python/pybind.cpp`.
- **Implement:**
  - Every class, property, overload and context manager.
  - GIL release during blocking calls.
  - Buffer/exporter lifetime through native work; guards cannot free
    submitted work early.
- Record the tooling choice (for example pyo3 with maturin).

### S22 — Device plugin ABI (F03) [CPU + GPU source]

- **TENT:** `include/tent/device_plugin.h`, `plugins/*`.
- **Implement:** registration, bad or missing plugin handling and CUDA plugin
  loading.

### S23 — TENT wire interoperability profile [CPU for TCP]

- Using the S16 freeze, implement a TENT-compatible control/metadata/TCP
  profile with explicit version rejection, beside OrbiTE's own protocol.
- **Done:**
  - Directional mixed-peer tests: a Rust requester against a C++ source and a
    C++ requester against a Rust source.
  - They use the frozen TENT build already produced by `tests/reference/`.
  - RDMA bootstrap interoperability is source only.

### S24 — P8.2 libfabric (B17) [CPU via the tcp provider]

- **TENT:** `transport/fabric/{fabric_attrs,fabric_context,fabric_transport}.cpp`.
- **Implement:**
  - Runtime-loaded libfabric and the host DRAM path.
  - Provider attributes and capabilities.
  - Endpoint, progress and completion ownership.
- CI moves real bytes with the `tcp` provider. EFA qualification is deferred.

### S25 — P8.1 MPComm (B15) [source]

- **TENT:** `transport/mpcomm/*` (4 files).
- **Implement:** adapter, peer registry, task mapping and registration. GDR
  only with the frozen `nvidia_peermem` requirement.
- **Done:** fake-provider tests.

### S26 — Benchmarks and fault-matrix preparation [CPU now]

- Add reusable byte-checked workloads in `benches/` and a matched-run driver
  against frozen TENT.
- Run CPU/TCP/io_uring comparisons now; leave the hardware profiles for the
  verification pass.
- Write the per-backend P10 fault fixtures.

## 8. Verification queue for when hardware returns

The canonical list is the
[verification queue](docs/qualification.md#verification-queue) in
qualification.md. Keep it current, with one exact gate per profile; the seed
list below shows its scope.

1. **Re-qualify source-accepted P5 work on the current artifact:**
   - v19–v23 staging: requester rings, CROSS windows, shared shards, the
     chunk event loop, concurrent CROSS, remote Delegate progress, socket,
     connection, finish and setup progress, host staging location, cold
     connection, MR publication and QP retirement.
   - NVLink P5.4e–h: group completion, stream pool, event progress and native
     batch copy; IPC cache LRU reclaim.
   - Mixed GPU batches and per-request remote staging.
   - GPU and pinned producer readiness.
   - CUDA and custom topology selection.
   - RDMA bandwidth selection, weighted multipath and effective link rate.
   - Request priority, promotion and shared slots; receiver credits and QoS
     on RDMA.
2. **Close P4:** the InfiniBand profile, physical link/device loss and provider
   teardown.
3. **New gates from this queue:**
   - S3 GPU↔file staging and S4 native GDS (with route evidence);
   - S9 RDMA failover;
   - S12 physical multi-rail;
   - S13 CXL;
   - S14 MNNVL/EGM;
   - R1b/R1c alignment, S15 and the S15b local CUDA copy extension;
   - S24 EFA;
   - S25 MPComm.
4. **Release work:** the P10 fault matrix, P11 matched performance against
   TENT, and installed OrbitKV integration. Follow the
   [release ladder](docs/completion-plan.md#release-ladder).

## 9. Do not

- Add Mooncake C++ runtime, Store, PG or EP dependencies, or any
  excluded-platform SDK.
- Weaken a gate, skip a missing facility silently, or describe a shim run as a
  hardware pass.
- Release owners on timeout or cancel, or replay uncertain writes.
- Commit raw logs, traces or generated HTML.
- Change OrbitKV, publish to crates.io, or push to `main` directly.
