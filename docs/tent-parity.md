# NVIDIA TENT parity contract

This document defines what OrbiTE must reproduce. Delivery is tracked only in
[the completion plan](completion-plan.md). A first owned-memory/CPU TCP substage
is implemented in the source tree; its scope and remaining parity gaps are
listed in the [CPU contract](cpu-tcp.md). No complete requirement row below is
qualified or accepted yet. The historical `orbit-te 0.1.0-alpha.1` publication
reserved the old package identity and provides no transfer API. The current package is `orbite`.

The selected purpose is a transfer engine for CPU DRAM, NVIDIA GPUs and local
file/SSD storage that carries every end-to-end OrbitKV cache path, including
GPU↔DRAM and GPU↔SSD. CPU and CUDA are the two
implementation platforms. GPU vendor selection does not restrict ordinary
CPUs, SSDs or compatible network devices to NVIDIA products. CPU-only operation
remains supported. Each NVIDIA GPU model/topology needs its own qualification.

The upstream inventory below makes this selection explicit: 10 transport types
apply to CPU/NVIDIA; seven require excluded accelerator platforms and UB requires
the excluded URMA/UnifiedBus platform. Optional
means opt-in build/runtime dependencies, not permission to leave an in-scope
requirement unfinished. This target does not claim every upstream platform.

## Frozen reference

| Reference | Revision | Purpose |
| --- | --- | --- |
| [Mooncake v0.3.14](https://github.com/kvcache-ai/Mooncake/releases/tag/v0.3.14), released 2026-11-03 | `e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c` | CPU/NVIDIA implementation and differential-test baseline, read through [OrbitInfra/Mooncake][reference] |
| Previous baseline | `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` | Historical reviews, oracle runs and installed profiles retain this revision where recorded |
| Initial OrbiTE source | [`b7e178fe2ac59e6a3ab19037f068236fb083292a`](https://github.com/OrbitInfra/OrbiTE/commit/b7e178fe2ac59e6a3ab19037f068236fb083292a) | Documents the starting point |

The reference exposes 33 C functions and 18 named selectable transport types.
There are 19 transport source directories because `bufio` also exists, but it
is not registered by the frozen runtime loader. The test tree contains 88 C++
files ending in `test.cpp`; that is a source inventory, not a count of executed
or passing tests. Compile guards, SDK availability and missing devices affect
which tests can run. See [the API][api], [types][types], [loader][loader] and
[test tree][tests].

Use implementation, public headers and reachable tests together as the oracle.
Resolve contradictory prose against those sources and record the decision.
Examples already found:

- Submit-stage failover is implemented in `commitPreparedSubmit` and covered by
  `engine_failover_e2e_test.cpp`; earlier paragraphs in the failover guide still
  describe it as missing.
- `exportLocalSegment` and `importRemoteSegment` return `NotImplemented` in the
  implementation. A declaration does not establish a working capability.
- Config lifecycle classes identify runtime-candidate fields and freeze a
  snapshot. Their presence does not establish public live configuration reload.
- The sample JSON enables some transports that the loader disables by default.
  Extract defaults from the consumed code, not the example configuration.

Baseline updates must be separate reviewed changes: record the new commit,
diff this inventory and the tests, retain old evidence and qualify affected
behavior again. Do not silently follow a moving `main` during implementation.

## v0.3.14 delta

The release is 160 commits ahead of the previous baseline. The path-limited
Git history contains 31 commits affecting 103 files under
`mooncake-transfer-engine/tent` (14,527 insertions and 1,028 deletions).
R1a changes the reference and oracle. R1b owns RDMA, R1c owns storage and
CUDA, and R1d owns HP-TCP, TCP and observability. A Port row can require an
upstream regression control even where an OrbiTE owner already implements
the behavior. Hardware qualification stays with the artifact that ran.

| Commit | Upstream change | Status | OrbiTE owner or evidence |
| --- | --- | --- | --- |
| [`33f5ea0f`](https://github.com/OrbitInfra/Mooncake/commit/33f5ea0fd9f09b921d2d8585fc398aa703888601) | [Build] Bump yalantinglibs to 45c6463 (#5132) | **N/A** | C++ build and ASIO/YLT API updates. R1a pins the new YLT archive for the native oracle. OrbiTE has no YLT runtime. |
| [`50896982`](https://github.com/OrbitInfra/Mooncake/commit/508969821645a7b02ebc46b6353c9e2679d772ec) | [TENT] Enable concurrent XPU host-device copies with a queue per copy (#5117) | **N/A** | Per-copy SYCL queues on Intel XPU. OrbiTE has no XPU platform. |
| [`e544719f`](https://github.com/OrbitInfra/Mooncake/commit/e544719fa61a9f877ffc5508ad91845c77aa6f0b) | [TENT] Add lossless request and transport attempt accounting (#5094) | **Port** | R1d implemented. Public aliases count at admission commit. Physical attempts carry their own terminal outcome across toggles and retries. Remote staging counts each CROSS. |
| [`7d4670b6`](https://github.com/OrbitInfra/Mooncake/commit/7d4670b67152cab643caa9a81e70bca39c782e8e) | [TransferEngine] NVLink: keep the sync memcpy fast path in TENT  (#5165) | **Port** | R1c implemented. Single legacy NVLink tasks below the KiB threshold use cuMemcpy. A DtoD receipt includes the legacy-stream drain. Grouped copies and MNNVL keep event completion. |
| [`4c0c5e5a`](https://github.com/OrbitInfra/Mooncake/commit/4c0c5e5ab1adea65cc13d09d88e10ecaafd6cb69) | [TENT] Parse MC_DISABLE_GPU_DIRECT_RDMA and MC_LOG_RDMA_SLICE_AFFINITY as booleans (#5125) | **Port** | R1d implemented. RDMA install parses the two legacy boolean switches. Numeric, mixed-case and invalid inputs are covered by `legacy_bool_env_accepts_numeric_values_at_rdma_install`. |
| [`fec029cc`](https://github.com/OrbitInfra/Mooncake/commit/fec029cc71caba91823892464e4fe8109e576205) | [TENT]Fix MemFabric SDMA memory allocation and NPU transport initialization for Mooncake TENT (#4834) | **N/A** | MemFabric SDMA allocation, native host storage and NPU initialization depend on the excluded Ascend/MemFabric runtime. |
| [`d0144780`](https://github.com/OrbitInfra/Mooncake/commit/d01447808d10af78011e9203d53796a15a032a03) | [TENT] Hash the batch address when picking a batch lock shard (#5095) | **Already covered** | OrbiTE gives each batch its own progress lock. `independent_batches_progress_while_a_sibling_progress_lock_is_held` exercises independent progress without address-based shard selection. |
| [`5e50b615`](https://github.com/OrbitInfra/Mooncake/commit/5e50b615b3c1d956f103a0191ec3ef5e7eef7da1) | [TENT] Wait for in-flight io_uring I/O before freeing a sub-batch (#5068) | **Port** | R1c implemented. Free defers to the consumed-SQE owner. A failed wait switches to the mapped CQ. The existing destruction cutoff quarantines a missing receipt. |
| [`5805e845`](https://github.com/OrbitInfra/Mooncake/commit/5805e8455200fa9aa93393de26b8a627601afa44) | [TE] hp_tcp: assign each operation to the least-loaded lane (#4935) | **Port** | R1d implemented. Least-loaded single-operation lanes retain rotating ties. `least_loaded_lane_bypasses_a_delayed_ack_and_releases_after_completion` holds one real socket while the other carries a READ and a WRITE. |
| [`956af65e`](https://github.com/OrbitInfra/Mooncake/commit/956af65e9b094e8adc8b4b9ec9b3105824b6e296) | [TE/TENT] Skip remote atomic QP access when the HCA reports no RDMA atomics. (#5047) | **Port** | R1b implemented. Each new QP clamps both READ depths to the local HCA limits. The grant permits only READ and WRITE, including on atomic-capable HCAs. |
| [`6692af10`](https://github.com/OrbitInfra/Mooncake/commit/6692af1046c3b2ffb12750ae8cdadd1c80d8ec59) | [TENT][FEAT] Reduce CQE Processing with Selective Signaling (#4632) | **Port** | R1b implemented. Default interval 1. Periodic and tail CQEs retire their own ordered group. Known partial posts, ambiguous posts, flush and probe ownership have separate controls. |
| [`d3ef7b1c`](https://github.com/OrbitInfra/Mooncake/commit/d3ef7b1c2821d8cdfd5986ee5949e8c17608a36c) | [TENT] Add metrics self-observability contract (#4938) | **Port** | R1d implemented. Both scrape formats report build, sampling, startup and serialization health. `exporter_health_survives_pause_and_failed_serialization` checks an HTTP failure and recovery. |
| [`5fa91fe1`](https://github.com/OrbitInfra/Mooncake/commit/5fa91fe135aba3255c638acd8fd95ba59a0c76c8) | [TENT] Add an opt-in pinned staging pool to the io_uring transport (#5012) | **Port** | R1c implemented. Optional page-aligned file slots are leased without waiting. Pageable fallback has a byte charge. Device fallback uses the bounded S3 proxy chunks. |
| [`c8fe63e5`](https://github.com/OrbitInfra/Mooncake/commit/c8fe63e52cbb9e1af6c3ed760d6ce1b7262ba0d4) | [TE] TransferEngine support hixl nic topo path (#4558) | **N/A** | The NIC topology path is consumed by Ascend HIXL resource configuration. |
| [`b730bbae`](https://github.com/OrbitInfra/Mooncake/commit/b730bbae5f8b89ce79f609df9c66b59bea1b3f8b) | [CI] Enable TENT runtime in T-One e2e as a parallel job (#3600) | **N/A** | T-One CI and build portability. The MNNVL change supplies missing pidfd syscall numbers in older C headers. OrbiTE already takes those numbers from Rust libc. |
| [`1dd37f57`](https://github.com/OrbitInfra/Mooncake/commit/1dd37f57ba944f5190dad30db1267c1c5e4a4a6b) | [Bugfix][TENT] Fail stop on terminal RDMA teardown errors (#4383) | **Port** | R1b implemented. Terminal QP/MR/PD or context teardown reports `Unquiesced` and keeps its owners. The grant and device retention controls cover repeat close and Drop. |
| [`ddda75d3`](https://github.com/OrbitInfra/Mooncake/commit/ddda75d351f25d8da7b3a53e18c119c7533836cd) | [TENT][Bugfix] Refresh Local Rail Topology Before Slice Selection (#4708) | **Port** | R1b implemented. Selection reads current local paths after the peer exchange. A two-context transfer exercises a local topology change during bootstrap. |
| [`8a4d4f68`](https://github.com/OrbitInfra/Mooncake/commit/8a4d4f68ca95de48967487dacbd714c35a663328) | [TENT] Enable GDS on Intel XPU through a cuFile-compatible library (#4904) | **N/A** | Intel xFile and `USE_XPU_GDS` supply the XPU-only device/file route. The NVIDIA GDS capability branch keeps DRAM/file and GPU/file. |
| [`6b90ea5e`](https://github.com/OrbitInfra/Mooncake/commit/6b90ea5e6c375e70d602d0ecd5e063406797e748) | [TENT] Stage XPU buffers through host memory in io_uring transport (#4902) | **N/A** | Stages XPU memory through a host buffer for file I/O. OrbiTE's accelerator path is CUDA. |
| [`8bfd2988`](https://github.com/OrbitInfra/Mooncake/commit/8bfd298880d447137378cf383017f49d8cd4a3aa) | [Bugfix][TENT] Submit merged staging requests only once (#4797) | **Port** | R1c implemented. One physical staged TCP submission carries both public aliases. The source counts one Pin, Delegate, CROSS and Unpin, with bytes and guards checked. |
| [`58e243fb`](https://github.com/OrbitInfra/Mooncake/commit/58e243fb299e4beb990524f2f0e5cabe33bd07e7) | [TENT][BUGFIX] Preserve mapped peer rail for wildcard targets (#4923) | **Port** | R1b implemented. Wildcard targets prefer the mapped healthy peer, including a lower-ranked direct peer. The request mask and current pair health bound alternatives. |
| [`6a835cbf`](https://github.com/OrbitInfra/Mooncake/commit/6a835cbf934aa50c287261f255c83e6faffecb71) | [TENT] Batch register MemFabric buffers to avoid aligned overlaps (#4870) | **N/A** | Overlapping aligned registrations and rollback are specific to the MemFabric batch API. |
| [`db4f146e`](https://github.com/OrbitInfra/Mooncake/commit/db4f146ecb5ae3bc432c0262c1cb6650a0a39ba4) | [TENT] Add intra-node PCIe P2P copies between Intel XPUs (#4379) | **N/A** | Intel XPU PCIe peer copies, Level Zero IPC imports and XPU-specific selector capabilities require SYCL. |
| [`a7496800`](https://github.com/OrbitInfra/Mooncake/commit/a7496800527b93105ac5c4177ce7cdabc5667dbf) | [TENT] Add asynchronous MLU host-device copies (#4720) | **Port** | R1c implemented. Shared controls cover submit publication before wake, scoped polling, staging-child drain and retained registration rollback. MLU and its runtime remain excluded. |
| [`dae22d47`](https://github.com/OrbitInfra/Mooncake/commit/dae22d472e31f6aac92ee1dfe938dc1441b58850) | [TENT] Do not advance the notify send slot on a failed post (#4434) | **N/A** | The corrected send slot belongs to the RDMA notification QP. OrbiTE notifications use local, TCP and control-RPC delivery. |
| [`b2c7d0a7`](https://github.com/OrbitInfra/Mooncake/commit/b2c7d0a7b5cec0b04563138f40d4c40937dd63fe) | [Bugfix][TENT] Restore caller CUDA context after plugin copies (#4839) | **Port** | R1c implemented. The CUDA owner restores the exact binding. A handled copy or cleanup error is terminal and cannot enter host memcpy. The external plugin ABI stays at S22. |
| [`042ab8a1`](https://github.com/OrbitInfra/Mooncake/commit/042ab8a18b0e6ccfe87782261a5556a3dd4446e4) | [TENT] Add Intel XPU GPUDirect dma-buf RDMA to the Transfer Engine (#4378) | **N/A** | The dma-buf exporter and accelerator-specific range checks serve Intel XPU. OrbiTE's NVIDIA RDMA path uses its CUDA registration owner. |
| [`3b08b771`](https://github.com/OrbitInfra/Mooncake/commit/3b08b77168870d265e32b6cfeab661f037426b9c) | [TE/TENT] Add adaptive RDMA congestion control core (#4197) | **Port** | R1b implemented. The opt-in `rdma-adaptive` shared core provides off/observe/enforce windows, generation-scoped evidence and completion classification. As in the TENT build, workers do not call it. |
| [`7540f582`](https://github.com/OrbitInfra/Mooncake/commit/7540f5826ed32cb755b1db027a2c609adb53f024) | [TENT] Add diagnostic core and version CLI (#4154) | **Port** | R1d implemented. The packaged offline `tent diagnostics version [--json]` binary, Rust diagnostics module and C/C++ header share the renderer and build identity. CI installs the binary and links both native clients. |
| [`7a058556`](https://github.com/OrbitInfra/Mooncake/commit/7a0585567458a4aba6a58115888c54b7fd1a44e1) | fix(tent): allow CPU clients to select peer GPU TCP memory (#4410) | **Port** | R1d implemented. `cpu_platform_can_select_peer_gpu` maps the capability matrix. A separately built CPU-only requester byte-checks READ and WRITE over real TCP against a CUDA-shim source. |
| [`e3e9d70e`](https://github.com/OrbitInfra/Mooncake/commit/e3e9d70eea3c8ea9eb236b4c9c92f361fbbe533f) | [Bugfix][TENT] Restore the caller's exact CUDA context after copies (#4643) | **Port** | R1c implemented. The Driver guard saves the exact caller binding, including null, and checks it after pop. A failed save precedes all copies. Unproven restoration fences the owner. |

## What parity means

| Surface | Required result |
| --- | --- |
| Functional behavior | Reproduce implemented operations, routing, status/error semantics, notifications, resource behavior and backend combinations in the selected CPU/NVIDIA scope |
| Rust API | Provide native Rust interfaces for the selected implemented TENT capability set, including features absent from its C API |
| TENT C ABI | All 33 functions and the LP64 transfer-engine layouts have installed [C API](c-api.md) controls. The fork's negative native return codes preserve dispatch uncertainty. Excluded transports return unavailable |
| TENT C++ API | The installed [native TENT and TENT-backed classic TE adapters](cpp-api.md) cover both S16 C++ binding rows. Rebuild callers against their headers and link the adapter archive to libtent |
| Python API | Cover the frozen TENT binding's classes, fields, overloads, errors and context managers through thin native bindings |
| Wire interoperability | Qualify OrbiTE-to-OrbiTE, TENT-to-OrbiTE and OrbiTE-to-TENT for each applicable backend/control protocol; API parity alone does not establish this |
| Configuration and operations | Match effective defaults, precedence, environment mappings, topology formats, metrics and explicit unavailable-backend behavior |
| Performance | Qualify matched workloads on each claimed hardware profile; source similarity or a Rust rewrite establishes no performance result |

C++ binary ABI interchangeability is not assumed: C++ consumers rebuild the
source adapter. The adapter must carry C++-only request fields through a
versioned native FFI path; the existing C request struct lacks `policy_name`,
`deadline_ns` and `intent_type` and must not be silently extended or drop them.

Keep compatibility entrypoints at the binding boundary and one Rust behavior
owner underneath. OrbiTE's runtime must not call the C++ TENT engine, legacy
TE engine, Mooncake Store, PG, EP or resharding components. External device and
OS interfaces such as verbs, CUDA, cuFile, MPComm and libfabric remain
explicit optional dependencies for their selected modules. HIP, Ascend, Sunrise,
PJRT/TPU, XPU/SYCL, MLU and URMA SDKs are outside this build and
implementation scope. Transport state machines and policy remain in Rust.

Safety defects are recorded as deliberate compatibility differences with
reproductions, rather than copied into Rust. Unknown physical completion may
retain ownership and reject more work within declared budgets. Do not claim
transparent recovery for that profile until safe reclamation is qualified.
Unsupported operations in the reference remain explicit unsupported operations;
implementing them would be a separately scoped extension.

## Runtime and service inventory

The IDs below are stable requirement identifiers. Each row needs executable
acceptance cases during P0, including relevant error, concurrency and teardown
cases. Source families identify where to inspect behavior; they do not imply
that every branch or provider has already been qualified upstream.

| ID | Required behavior | Reference source/test families | Delivery |
| --- | --- | --- | --- |
| F01 | Default/config-file/config-object construction; `MC_TENT_CONF` file or inline JSON; legacy environment mappings and precedence; typed parsing; invalid-root/path diagnostics; immutable per-engine snapshots | `common/config*`, `config_lifecycle*`, `transfer_engine_config_override_test` | P1 |
| F02 | CPU/NUMA, PCI and device discovery; RDMA allow/deny lists; custom topology and priority matrices; `cpu:`/`cuda:`; strict locality; unavailable accelerator locations are rejected | `runtime/topology*`, `platform/*`, `topology_priority_matrix_test`, `strict_local_numa_test` | P1/P4/P5/P8 |
| F03 | Platform allocation/free, location and pointer probing, synchronous copy and device/context/stream ownership; CUDA device-plugin C ABI | `runtime/platform*`, `memory_prober*`, `platform/*`, `device_plugin.h`, `plugins/*` | P1/P5/P8 |
| F04 | Single/batch memory allocation and registration; permissions/options; duplicate references, range geometry, warmup/prefault, unregister and partial-failure ownership | `transfer_engine_impl.cpp`, `segment_registry*`, `local_memory_lifecycle_test`, `coalesce_regions_test`, backend buffer tests | P1/P4/P5/P8 |
| F05 | Memory/file segments, `file://` addressing, open/close/info, descriptor caching/refresh, update subscription/invalidation and retry coalescing | `segment*`, `control_plane*`, `segment_manager_test`, `segment_desc_retry_test` | P2/P7 |
| F06 | P2P metadata for OrbitKV. Optional S18 adds etcd, Redis (username, password and DB) and HTTP stores for another consumer, including publication and lookup failure handling | `runtime/metastore.cpp`, `metastore/*`, `control_plane*`, `redis_metastore_test` | P2/P9 |
| F07 | RPC bootstrap, peer probe, data READ/WRITE, notifications, Delegate/Pin/Unpin, update subscriptions; reconnect and owned handler shutdown | `rpc/*`, `control_plane*`, `rpc_reconnect_test`, `rpc_handler_isolation_test`, `delegate_rpc_test` | P2/P5/P10 |
| F08 | Batch capacity/IDs; READ/WRITE; partial submission; merge eligibility and public alias-to-owner mapping; per-task/list/overall status and transferred bytes | `transfer_engine_impl.cpp`, `request_merge_test`, `request_submit_bench`, `engine_failover_e2e_test` | P1/P2/P6 |
| F09 | Observational polling option, explicit `progressBatch`, background progress worker, bounded wake/fallback timing, terminal-state stability and deferred batch retirement | `progress_worker*`, `progress_worker_test`, `runtime_queue_dispatch_test`, `poll_backoff_test` | P1/P6 |
| F10 | Best-effort queued/posted cancellation with backend-specific support, partial progress and retention; distinguish accepted cancel from physical termination | `cancelTransfer`, `Transport::supportsCancellation`, RDMA/HP-TCP cancel tests | Every backend/P10 |
| F11 | Attached completion notifications, explicit send/receive, local and transport/RPC fallback, exactly-once logical delivery accounting, empty-poll/free conventions | `local_notification_test`, `notification_fallback_test`, `transfer_engine_c.cpp` | P2/P6/P9 |
| F12 | Policy ordering; priority HIGH/MEDIUM/LOW; named policy; segment, location and size filters; device masks; QP pools; explicit transport hint and unavailable-path behavior | `transport_selector*`, `transport_hint_test`, `qos_contract*`, `qp_pool_layout_test` | P4/P6 |
| F13 | All request intents: unspecified, foreground get, prefetch, migration, checkpoint, weight loading and internal staging; policy binding and merge isolation | `common/types.h`, `qos_contract*`, `transport_selector_test` | P6 |
| F14 | Request/byte admission; FIFO/EDF and urgency promotion; dispatch windows; staging reserves; deadline-feasibility drop; transport-aware bandwidth provider | `admission_queue*`, `deadline_mlu.h`, `admission_queue_test`, `runtime_queue_dispatch_test` | P6 |
| F15 | Per-worker priority queues, host-shared priority slots, process-local per-NIC byte accounting, named QP pools and four-resource cumulative sender credit | `rdma/quota*`, `shared_quota*`, `receiver_credit*`, `quota_accounting_test`, `receiver_credit_test` | P4/P6 |
| F16 | Slicing/spraying; round-robin and bandwidth-aware choices; separate selection EWMA and transmit estimate; NUMA penalties, masks and strict-local rules | `rdma/workers*`, `bw_arbitration*`, `device_selector_test`, `bw_arbitration_test` | P4/P6 |
| F17 | Submit- and completion-stage fallback; per-owner retry budgets; merged aliases; no terminal resurrection; explicit-progress versus poll semantics | `transfer_engine_impl.cpp`, `engine_failover_e2e_test` | P6/P10 |
| F18 | RDMA endpoint caches, bootstrap concurrency, per-rail health, cooldown/probe/half-open recovery, device loss, retirement and failed-cleanup quarantine | `rdma/endpoint*`, `rail_monitor*`, endpoint/rail/async-event teardown tests | P4/P6/P10 |
| F19 | Local and remote staging; delegate/pin/unpin registry; chunk buffers; mixed direct/staged requests; nested work admission, completion and proxy shutdown | `proxy_manager*`, `remote_stage_operation*`, `stage_buffer_concurrency_test`, `proxy_shutdown_timeout_test` | P5/P6/P8/P10 |
| F20 | Transfer/attempt/queue/failure-stage and deadline observations; labels and histograms; HTTP/JSON/Prometheus output; configuration, disabled mode and available-NIC load snapshots | `metrics/*`, `common/qos_metrics*`, metrics tests, `getNicLoadStats` | P2/P6/P9 |
| F21 | Native C++, C and Python surfaces; classic-TE-to-TENT shim, including its synchronous/wait helpers and explicit unavailable operations | public headers, `src/python/pybind.cpp`, parent `src/transfer_engine.cpp`, `test_python_guards.py` | P9 |
| F22 | Transport/platform feature selection, missing-provider errors, installed artifacts, SDK/runtime discovery, examples and reproducible byte-checking benchmarks | TENT/parent CMake, provider adapters, CI, benchmark sources | Every stage/P11 |

For F15 the frozen RDMA shared header publishes one rotating priority slot.
The local selector owns NIC byte charges. QoS inherits policy fields without
charging a parent or cluster budget. The private receiver ledger accepts named
resources and monotone grants. Frozen TENT supplies neither a window-sizing
algorithm, a dead-sender reaper nor an application-resource mapping.

Primary sources: [runtime implementation][impl], [runtime directory][runtime],
[tests][tests], [configuration][config], [C++ compatibility shim][shim],
[Python binding][python], [metrics][metrics] and [device plugin ABI][plugin].

The deadline-feasibility code's `MLU` term and the Cambricon MLU device backend
are separate concepts. The former does not establish framework-level local
recompute: the frozen engine installs an empty local-decode suggestion hook.

## Upstream transport inventory and selected scope

All 18 upstream types are accounted for; only the 10 marked core or optional
are implementation targets. Capability bits and compiled providers constrain
supported transfer directions. Do not infer GPU support for a transport from
another transport's platform support. Every selected row requires normal bytes,
negative cases, teardown, installed-artifact and applicable mixed-peer gates.

| ID / type | Frozen capability and special obligations | Required hardware/provider | Scope / delivery |
| --- | --- | --- | --- |
| B01 `TCP` | RPC data path; host/CUDA copies, retries and explicit copy failures | Two processes/hosts; NVIDIA GPU for CUDA copies | Core, P2/P5 |
| B02 `HP_TCP` | CPU DRAM only; persistent worker-owned lanes, request/registration IDs, bounds and permissions, remote WRITE ACK, cancellation/drain, IPv4 static rails and large READ slicing | Two TCP hosts; multiple routed interfaces for rail gates | Core, P3 |
| B03 `SHM` | Host shared mappings, offset/path and permissions, concurrent registration and retirement; CXL mount configuration and locality/default behavior | Same-host processes and actual shared backing; CXL-backed profile separately | Core, P3 |
| B04 `RDMA` | Host and CUDA device memory; MR/PD/CQ/QP lifecycle, bootstrap, retries, multirail, notifications, quota, GDR capability and physical drain | Two RNIC hosts; IB/RoCE profiles, multiple rails; CUDA combinations separately | Core, P4/P5/P6 |
| B05 `NVLINK` | Device/host directions actually advertised, CUDA IPC handle sharing and stream completion; no stale mapping reuse | Two local GPU processes with supported CUDA peer access | Core, P5 |
| B06 `MNNVL` | CUDA fabric memory/export/import; opt-in selection, EGM option and supported directions | Supported CUDA fabric/NVLink topology, plus EGM profile where applicable | Core, P5 |
| B07 `IOURING` | File segments, read/write offsets and completion/error bytes; staging where selected; short/partial/error I/O | Linux io_uring and real local files/storage | Core storage, P7 |
| B08 `GDS` | Host/device-to-file directions, cuFile registration and completion/status ownership, explicit fallback behavior | NVIDIA GPU, cuFile/GDS-capable filesystem and storage | Core storage, P7 |
| B09 `UB` | Host URMA transport, endpoint/device capability negotiation, jetty/JFC lifecycle, slicing/quota, retries, notifications when enabled, cancellation and quarantine | Supported URMA devices/provider on two hosts; bonding profile separately | Excluded platform |
| B10 `AscendDirect` | Host/device registration and copy, HIXL/local-copy paths, fabric memory, timeout, resource configuration and agent mode | Ascend runtime and supported HIXL topology | Excluded accelerator |
| B11 `SUNRISE_LINK` | Advertised host/device directions, registration, provider endpoint and completion lifecycle | Sunrise runtime and link devices | Excluded accelerator |
| B12 `TPU` | Host-device staging through PJRT ABI and provider bridge; ownership of pending device work | Actual supported TPU/PJRT adapter | Excluded accelerator |
| B13 `XPU` | Host-device copy and staging with native SYCL allocations/context | Intel XPU, DPC++/SYCL runtime | Excluded accelerator |
| B14 `MLU` | Host-device copy/staging, allocation and process ownership | Cambricon MLU runtime/devices | Excluded accelerator |
| B15 `MPCOMM` | Host and supported NVIDIA GDR; peer registry, registration and completion mapping | Compatible provider/endpoints; frozen GPU support requires `nvidia_peermem` | Optional network, P8.1 |
| B16 `HYLINK` | HIP-backed host/device directions, endpoint and registered-region lifecycle | HIP devices and compatible Hylink provider | Excluded accelerator |
| B17 `FABRIC` | Frozen host-DRAM libfabric path, provider attributes/capabilities, endpoint/progress and completion ownership | Compatible libfabric provider on two hosts; qualify EFA on EFA hardware | Optional network, P8.2 |
| B18 `MEMFABRIC` | Ascend-backed host/device directions, dynamically loaded provider ABI, memory allocator and completion lifecycle | Ascend ACL and compatible MemFabric provider | Excluded accelerator |

Source authority: [transport types][types], [runtime loader][loader],
[transport implementations][transports] and [platform implementations][platforms].

The upstream's eight platform families are CPU, CUDA, ROCm/HIP, Ascend, Sunrise,
TPU, XPU and MLU. OrbiTE implements CPU, CUDA and the CUDA device-plugin ABI.
The other six platforms and ROCm plugin are excluded by project scope, rather
than classified as missing hardware qualification. In-scope provider stubs
qualify unavailable/error handling and state-machine controls only; they cannot
qualify real transfers.

Frozen TENT builds `BufIoTransport` as a static target. It caches buffered fds,
runs synchronous `pread`/`pwrite` at the request's file offset and stages CUDA
through host memory. `loadTransports()` installs only IOURING and GDS for
files and never constructs BufIo. None of its behavior is reachable through
the frozen loader. OrbiTE therefore has no public `BUFIO` transport.
[BufIo implementation][bufio] · [Frozen loader][loader]
The fault-proxy transport is a test mechanism, not a nineteenth production type.

HP-TCP's frozen scope excludes GPU memory, TLS, per-WRITE striping, dynamic rail
scheduling, rail failover and transparent replay of ambiguous WRITEs. Its large
READ slicing is not dynamic RDMA-style spraying. These omissions are explicit
reference boundaries, not missing parity work disguised as implemented support.

## OrbiTE local CUDA copy extension

S15b implements the consumer's local GPU↔pinned DRAM copy mechanics beyond
TENT's transport surface. The DMA geometry and mapped-memory kernel follow
OrbitKV's transfer module at `93541a21`. Stream-bound completions own each
registered range and descriptor upload. This extension adds no transport,
peer capability or wire record. [Local CUDA copies](local-copy.md)

## Local SSD paths and module boundary

The frozen TENT source implements both READ and WRITE for local file segments:

| User-visible movement | Actual implementation path | Meaning |
| --- | --- | --- |
| DRAM to/from SSD | `io_uring_prep_writev` / `io_uring_prep_readv` | Host file I/O; aligned host staging when needed |
| NVIDIA GPU to/from SSD through host memory | CUDA copy to/from host staging plus io_uring | A device-file route that uses DRAM |
| NVIDIA GPU to/from SSD through cuFile | `CUFILE_WRITE` / `CUFILE_READ`, `cuFileBatchIOSubmit` and completion polling | GDS API path; native direct movement requires separate evidence |

See [io_uring source][uring] and [GDS source][gds]. These are reference
implementations, not OrbiTE hardware qualification. Successful cuFile calls
alone do not establish the native storage-to-GPU path; filesystem, device,
topology, configuration and actual route observations matter. The NVIDIA
[API guide](https://docs.nvidia.com/gpudirect-storage/api-reference-guide/index.html)
describes compatibility mode and supported memory/configuration rules.

Local [file segments](storage.md) are part of the default build. The Linux
`io-uring` and `gds` features enable the file executors. The network/memory
core runs without loading storage libraries. The storage transports own byte
I/O and its resources. OrbitKV retains KV indexing, placement, eviction, extents, cache
policy and recovery semantics. OrbitKV's SSD tier is expected to move its
DRAM↔SSD and GPU↔SSD byte movement onto these modules; that switch is consumer
integration with its own P10/P11 qualification. A compatibility profile enables
the modules needed to compare with TENT's effective configuration.

## C and language binding inventory

The [acceptance inventory][inventory] is the shared input to S17–S23. Its F01 rows identify each supported setting, unsupported field or excluded provider. The Rust loader exercises them in the CPU matrix. Each
public C declaration has a call/ownership case. The companion C probe checks
the frozen public and plugin layouts on Linux LP64. The index also identifies
reference tests by source and GTest filter, and maps config, environment,
direction flags, metrics, metadata and logical RPCs to their owners and
acceptance triggers. The [public header][api] remains the upstream authority.

The richer Rust/C++ surface additionally covers `Request.policy_name`,
`deadline_ns`, `intent_type`, peer probing, explicit `progressBatch`, topology
introspection and the TENT-backed classic-TE sync/wait helpers. Export/import
segment declarations preserve the reference's `NotImplemented` result.

Python parity includes `OpCode`, `TransferStatusEnum`, `Permission`,
`TransportType`, `IntentType`, `SegmentInfoType`, `Notification`, `Request`,
`TransferStatus`, `SegmentInfoBuffer`, `SegmentInfo`, `MemoryOptions`,
`MemoryGuard`, `BatchGuard` and `TransferEngine`. Cover the current field
properties, constructors, overloads and method names in [the binding][python],
not just the native transfer methods. In particular, preserve buffer/exporter
ownership through native work, release the GIL during blocking native calls,
and prevent guard exit/destruction from freeing submitted work prematurely.

## Wire and consumer boundaries

OrbiTE uses wire version **1** throughout pre-release development. Both
peers must run the same build. The header carries no build identifier, so
it cannot detect a mismatched build. Wire versioning under an explicit
compatibility policy starts at the first release. RDMA peers using endpoint QP
pools must also share the same ordered pool layout, width and link-layer settings.
The control-plane magic keeps its historical spelling, `ORBITTE\0`.

The control RPC assigns IDs for descriptor lookup, RDMA bootstrap, SendData,
RecvData, Notify, Probe, Delegate, Pin, Unpin, update subscription, update
notification and UB bootstrap. The UB bootstrap ID stays recognized but
unavailable, like the excluded transports. See [the RPC contract][rpc]. The [wire fixtures][fixtures] freeze YLT request/reply headers, the
struct_pack function argument, opaque attachments, segment JSON, and HP-TCP
and provider bootstrap encodings. Linux CI compares them with unmodified
v0.3.14 code and replays TCP data frames against a registered TENT source.
OrbiTE continues to use its own control protocol until a peer explicitly
selects a future interoperability profile.

Interoperability is directional: test a Rust requester against a C++ source
and a C++ requester against a Rust source, with permission errors, restarts and
partial responses. A safety extension that changes wire or ABI requires an
explicit compatible profile or version rejection; do not silently downgrade
epoch/generation requirements for an old peer.

OrbitKV currently consumes a patched older TENT ABI with extra source-epoch and
source-close operations. Those are an additional consumer contract, not part of
the 33-function latest upstream C ABI. Before migration, compare each current
patch with the new source and implement only the still-needed semantics. Keep
the old consumer's physical ownership guarantees. See the maintained OrbitKV
[transport contract](https://github.com/OrbitInfra/orbitkv/blob/a9befeec596bb9064d25d34f93d07fe59077809d/docs/transport.md)
and [completion plan](https://github.com/OrbitInfra/orbitkv/blob/a9befeec596bb9064d25d34f93d07fe59077809d/docs/completion-plan.md#s3--transfer-lifetime-and-generation-safe-ownership).

OrbiTE owns byte movement and registration/operation lifetimes. OrbitKV owns
cache identity, replica authority, cache leases and recovery semantics. An
engine source epoch or a region generation does not prove an inference engine's
GPU page generation. DeepInfer is a prospective consumer; planning its use does
not establish a released cache integration. Native engine P/D remains owned by
the engine until separately integrated and qualified.

[reference]: https://github.com/OrbitInfra/Mooncake/tree/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent
[api]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/include/tent/transfer_engine.h
[types]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/include/tent/common/types.h
[loader]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/runtime/transport_loader.cpp
[impl]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp
[runtime]: https://github.com/OrbitInfra/Mooncake/tree/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/runtime
[tests]: https://github.com/OrbitInfra/Mooncake/tree/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/tests
[config]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/common/config.cpp
[shim]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/src/transfer_engine.cpp
[python]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/python/pybind.cpp
[metrics]: https://github.com/OrbitInfra/Mooncake/tree/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/metrics
[plugin]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/include/tent/device_plugin.h
[transports]: https://github.com/OrbitInfra/Mooncake/tree/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport
[platforms]: https://github.com/OrbitInfra/Mooncake/tree/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/platform
[rpc]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/include/tent/rpc/rpc.h
[uring]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport/io_uring/io_uring_transport.cpp
[bufio]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport/bufio/bufio_transport.cpp
[gds]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport/gds/gds_transport.cpp

[inventory]: https://github.com/OrbitInfra/OrbiTE/tree/main/tests/reference/inventory
[fixtures]: https://github.com/OrbitInfra/OrbiTE/tree/main/tests/reference/wire
