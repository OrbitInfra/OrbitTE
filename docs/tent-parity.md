# TENT parity contract

This document defines what OrbitTE must reproduce. Delivery is tracked only in
[the completion plan](completion-plan.md). Every OrbitTE runtime capability below
is currently unimplemented. The published `orbit-te 0.1.0-alpha.1` establishes
package identity and provides no transfer API.

## Frozen reference

Audited on 2026-10-21, Asia/Shanghai:

| Reference | Revision | Purpose |
| --- | --- | --- |
| `OrbitInfra/Mooncake` main | `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab` | Implementation and differential-test baseline |
| `kvcache-ai/Mooncake` main | Same commit at inspection | Confirms the fork contains the latest upstream TENT inspected for this plan |
| Latest upstream release | [`v0.3.13.post1`](https://github.com/kvcache-ai/Mooncake/releases/tag/v0.3.13.post1) | Historical comparison, not the full feature target |
| Initial OrbitTE source | [`b7e178fe2ac59e6a3ab19037f068236fb083292a`](https://github.com/OrbitInfra/OrbitTE/commit/b7e178fe2ac59e6a3ab19037f068236fb083292a) | Documents the starting point |

The reference exposes 33 C functions and 18 named selectable transport types.
There are 19 transport source directories because `bufio` also exists, but it
is not registered by the frozen runtime loader. The test tree contains 84 C++
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

## What parity means

| Surface | Required result |
| --- | --- |
| Functional behavior | Reproduce implemented operations, routing, status/error semantics, notifications, resource behavior and supported backend combinations at the frozen revision |
| Rust API | Provide native Rust interfaces for the full implemented TENT capability set, including features absent from its C API |
| TENT C ABI | Cover all 33 functions, constants, enum values, struct sizes/alignment, buffer ownership and output/free conventions; verify from C callers |
| TENT C++ API | Supply source-level adapters for native TENT and the TENT-backed classic-TE surface; rebuild callers against those adapters |
| Python API | Cover the frozen TENT binding's classes, fields, overloads, errors and context managers through thin native bindings |
| Wire interoperability | Qualify OrbitTE-to-OrbitTE, TENT-to-OrbitTE and OrbitTE-to-TENT for each applicable backend/control protocol; API parity alone does not establish this |
| Configuration and operations | Match effective defaults, precedence, environment mappings, topology formats, metrics and explicit unavailable-backend behavior |
| Performance | Qualify matched workloads on each claimed hardware profile; source similarity or a Rust rewrite establishes no performance result |

C++ binary ABI interchangeability is not assumed: C++ consumers rebuild the
source adapter. The adapter must carry C++-only request fields through a
versioned native FFI path; the existing C request struct lacks `policy_name`,
`deadline_ns` and `intent_type` and must not be silently extended or drop them.

Keep compatibility entrypoints at the binding boundary and one Rust behavior
owner underneath. OrbitTE's runtime must not call the C++ TENT engine, legacy
TE engine, Mooncake Store, PG, EP or resharding components. External device and
OS interfaces such as verbs, CUDA, HIP, cuFile, URMA, PJRT and provider SDKs
remain explicit optional dependencies. A small vendor ABI bridge is allowed
where a provider exposes only C++ interfaces, such as the frozen XPU SYCL path;
transport state machines and policy remain in Rust.

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
| F02 | CPU/NUMA, PCI and device discovery; RDMA allow/deny lists; custom topology and priority matrices; `cpu:`, `cuda:`, `hip:` and accepted aliases; strict locality | `runtime/topology*`, `platform/*`, `topology_priority_matrix_test`, `strict_local_numa_test` | P1/P4/P5/P8 |
| F03 | Platform allocation/free, location and pointer probing, synchronous copy and device/context/stream ownership; device-plugin C ABI | `runtime/platform*`, `memory_prober*`, `platform/*`, `device_plugin.h`, `plugins/*` | P1/P5/P8 |
| F04 | Single/batch memory allocation and registration; permissions/options; duplicate references, range geometry, warmup/prefault, unregister and partial-failure ownership | `transfer_engine_impl.cpp`, `segment_registry*`, `local_memory_lifecycle_test`, `coalesce_regions_test`, backend buffer tests | P1/P4/P5/P8 |
| F05 | Memory/file segments, `file://` addressing, open/close/info, descriptor caching/refresh, update subscription/invalidation and retry coalescing | `segment*`, `control_plane*`, `segment_manager_test`, `segment_desc_retry_test` | P2/P7 |
| F06 | P2P metadata plus etcd, Redis and HTTP stores; Redis username/password/DB behavior; publication and lookup failure handling | `runtime/metastore.cpp`, `metastore/*`, `control_plane*`, `redis_metastore_test` | P2/P9 |
| F07 | RPC bootstrap, peer probe, data READ/WRITE, notifications, Delegate/Pin/Unpin, update subscriptions, UB bootstrap; reconnect and owned handler shutdown | `rpc/*`, `control_plane*`, `rpc_reconnect_test`, `rpc_handler_isolation_test`, `delegate_rpc_test` | P2/P5/P8/P10 |
| F08 | Batch capacity/IDs; READ/WRITE; partial submission; merge eligibility and public alias-to-owner mapping; per-task/list/overall status and transferred bytes | `transfer_engine_impl.cpp`, `request_merge_test`, `request_submit_bench`, `engine_failover_e2e_test` | P1/P2/P6 |
| F09 | Observational polling option, explicit `progressBatch`, background progress worker, bounded wake/fallback timing, terminal-state stability and deferred batch retirement | `progress_worker*`, `progress_worker_test`, `runtime_queue_dispatch_test`, `poll_backoff_test` | P1/P6 |
| F10 | Best-effort queued/posted cancellation with backend-specific support, partial progress and retention; distinguish accepted cancel from physical termination | `cancelTransfer`, `Transport::supportsCancellation`, RDMA/HP-TCP/UB cancel tests | Every backend/P10 |
| F11 | Attached completion notifications, explicit send/receive, local and transport/RPC fallback, exactly-once logical delivery accounting, empty-poll/free conventions | `local_notification_test`, `notification_fallback_test`, `transfer_engine_c.cpp` | P2/P6/P9 |
| F12 | Policy ordering; priority HIGH/MEDIUM/LOW; named policy; segment, location and size filters; device masks; QP pools; explicit transport hint and unavailable-path behavior | `transport_selector*`, `transport_hint_test`, `qos_contract*`, `qp_pool_layout_test` | P4/P6 |
| F13 | All request intents: unspecified, foreground get, prefetch, migration, checkpoint, weight loading and internal staging; policy binding and merge isolation | `common/types.h`, `qos_contract*`, `transport_selector_test` | P6 |
| F14 | Request/byte admission; FIFO/EDF and urgency promotion; dispatch windows; staging reserves; deadline-feasibility drop; transport-aware bandwidth provider | `admission_queue*`, `deadline_mlu.h`, `admission_queue_test`, `runtime_queue_dispatch_test` | P6 |
| F15 | Per-worker priority queues, shared cross-process quotas, accounting and recovery, priority filtering, named QP pools and receiver credit/pacing | `rdma/quota*`, `shared_quota*`, `receiver_credit*`, `quota_accounting_test`, `receiver_credit_test` | P4/P6 |
| F16 | Slicing/spraying; round-robin and bandwidth-aware choices; separate selection EWMA and transmit estimate; NUMA penalties, masks and strict-local rules | `rdma/workers*`, `bw_arbitration*`, `device_selector_test`, `bw_arbitration_test` | P4/P6 |
| F17 | Submit- and completion-stage fallback; per-owner retry budgets; merged aliases; no terminal resurrection; explicit-progress versus poll semantics | `transfer_engine_impl.cpp`, `engine_failover_e2e_test` | P6/P10 |
| F18 | RDMA and UB endpoint caches, bootstrap concurrency, per-rail health, cooldown/probe/half-open recovery, device loss, retirement and failed-cleanup quarantine | `rdma/endpoint*`, `rail_monitor*`, `ub/*`, endpoint/rail/async-event/UB teardown tests | P4/P6/P8/P10 |
| F19 | Local and remote staging; delegate/pin/unpin registry; chunk buffers; mixed direct/staged requests; nested work admission, completion and proxy shutdown | `proxy_manager*`, `remote_stage_operation*`, `stage_buffer_concurrency_test`, `proxy_shutdown_timeout_test` | P5/P6/P8/P10 |
| F20 | Transfer/attempt/queue/failure-stage and deadline observations; labels and histograms; HTTP/JSON/Prometheus output; configuration, disabled mode and available-NIC load snapshots | `metrics/*`, `common/qos_metrics*`, metrics tests, `getNicLoadStats` | P2/P6/P9 |
| F21 | Native C++, C and Python surfaces; classic-TE-to-TENT shim, including its synchronous/wait helpers and explicit unavailable operations | public headers, `src/python/pybind.cpp`, parent `src/transfer_engine.cpp`, `test_python_guards.py` | P9 |
| F22 | Transport/platform feature selection, missing-provider errors, installed artifacts, SDK/runtime discovery, examples and reproducible byte-checking benchmarks | TENT/parent CMake, provider adapters, CI, benchmark sources | Every stage/P11 |

Primary sources: [runtime implementation][impl], [runtime directory][runtime],
[tests][tests], [configuration][config], [C++ compatibility shim][shim],
[Python binding][python], [metrics][metrics] and [device plugin ABI][plugin].

The deadline-feasibility code's `MLU` term and the Cambricon MLU device backend
are separate concepts. The former does not establish framework-level local
recompute: the frozen engine installs an empty local-decode suggestion hook.

## Complete transport and platform matrix

All 18 types stay in the completion target, including types not currently used
by OrbitKV. Capability bits and compiled providers constrain supported transfer
directions. Do not infer GPU support for a transport from another transport's
platform support. Each row requires normal bytes, negative cases, teardown,
installed-artifact and applicable mixed-peer interoperability gates.

| ID / type | Frozen capability and special obligations | Required hardware/provider | Delivery |
| --- | --- | --- | --- |
| B01 `TCP` | RPC data path; host/device copies through the selected platform; retries and explicit copy failures | Two processes/hosts; additional platform hardware for device copies | P2/P5/P8 |
| B02 `HP_TCP` | CPU DRAM only; persistent worker-owned lanes, request/registration IDs, bounds and permissions, remote WRITE ACK, cancellation/drain, IPv4 static rails and large READ slicing | Two TCP hosts; multiple routed interfaces for rail gates | P3 |
| B03 `SHM` | Host shared mappings, offset/path and permissions, concurrent registration and retirement; CXL mount configuration and locality/default behavior | Same-host processes and actual shared backing; CXL-backed profile separately | P3 |
| B04 `RDMA` | Host and supported device memory; MR/PD/CQ/QP lifecycle, bootstrap, retries, multirail, notifications, quota, GDR capability and physical drain | Two RNIC hosts; IB/RoCE profiles, multiple rails; CUDA/HIP combinations separately | P4/P5/P6/P8 |
| B05 `NVLINK` | Device/host directions actually advertised, CUDA IPC handle sharing and stream completion; no stale mapping reuse | Two local GPU processes with supported CUDA peer access | P5 |
| B06 `MNNVL` | CUDA fabric memory/export/import; opt-in selection, EGM option and supported directions | Supported CUDA fabric/NVLink topology, plus EGM profile where applicable | P5 |
| B07 `IOURING` | File segments, read/write offsets and completion/error bytes; staging where selected; short/partial/error I/O | Linux io_uring and real local files/storage | P7 |
| B08 `GDS` | Host/device-to-file directions, cuFile registration and completion/status ownership, explicit fallback behavior | NVIDIA GPU, cuFile/GDS-capable filesystem and storage | P7 |
| B09 `UB` | Host URMA transport, endpoint/device capability negotiation, jetty/JFC lifecycle, slicing/quota, retries, notifications when enabled, cancellation and quarantine | Supported URMA devices/provider on two hosts; bonding profile separately | P8.2 |
| B10 `AscendDirect` | Host/device registration and copy, HIXL/local-copy paths, fabric memory, timeout, resource configuration and agent mode | Ascend runtime and supported HIXL topology | P8.3 |
| B11 `SUNRISE_LINK` | Advertised host/device directions, registration, provider endpoint and completion lifecycle | Sunrise runtime and link devices | P8.5 |
| B12 `TPU` | Host-device staging through PJRT ABI and provider bridge; ownership of pending device work | Actual supported TPU/PJRT adapter | P8.6 |
| B13 `XPU` | Host-device copy and staging with native SYCL allocations/context | Intel XPU, DPC++/SYCL runtime | P8.7 |
| B14 `MLU` | Host-device copy/staging, allocation and process ownership | Cambricon MLU runtime/devices | P8.8 |
| B15 `MPCOMM` | Peer registry, endpoint parsing, memory registration, request/completion mapping and explicit unavailable-provider behavior | Compatible MPComm provider and endpoints | P8.9 |
| B16 `HYLINK` | HIP-backed host/device directions, endpoint and registered-region lifecycle | HIP devices and compatible Hylink provider | P8.1 |
| B17 `FABRIC` | Frozen host-DRAM libfabric path, provider attributes/capabilities, endpoint/progress and completion ownership | Compatible libfabric provider on two hosts; qualify EFA on EFA hardware | P8.10 |
| B18 `MEMFABRIC` | Ascend-backed host/device directions, dynamically loaded provider ABI, memory allocator and completion lifecycle | Ascend ACL and compatible MemFabric provider | P8.4 |

Source authority: [transport types][types], [runtime loader][loader],
[transport implementations][transports] and [platform implementations][platforms].

The eight platform families are CPU, CUDA, ROCm/HIP, Ascend, Sunrise, TPU, XPU
and MLU. CUDA/ROCm device plugins have their own exported plugin ABI and are
also in scope. A provider stub can qualify unavailable/error handling and
state-machine controls only; it cannot qualify real transfers.

`BufIoTransport` has source and a build target but no frozen public transport
enum/loader registration. Audit and retain its actual reachable behavior or
document it as unselected source; do not invent a selectable `BUFIO` capability.
The fault-proxy transport is a test mechanism, not a nineteenth production type.

HP-TCP's frozen scope excludes GPU memory, TLS, per-WRITE striping, dynamic rail
scheduling, rail failover and transparent replay of ambiguous WRITEs. Its large
READ slicing is not dynamic RDMA-style spraying. These omissions are explicit
reference boundaries, not missing parity work disguised as implemented support.

## C and language binding inventory

All functions below come from the frozen [public header][api]. Freeze sizes,
alignments, integer widths, constants and ownership with ABI fixtures during P0.

| Family | Required C entrypoints |
| --- | --- |
| Configuration | `tent_load_config_from_file`, `tent_set_config` |
| Engine | `tent_create_engine`, `tent_destroy_engine`, `tent_available`, `tent_segment_name`, `tent_rpc_server_addr_port` |
| Segment | `tent_open_segment`, `tent_close_segment`, `tent_get_segment_info`, `tent_free_segment_info` |
| Basic memory | `tent_allocate_memory`, `tent_free_memory`, `tent_register_memory`, `tent_unregister_memory` |
| Extended memory | `tent_register_memory_with_perm`, `tent_register_memory_batch`, `tent_unregister_memory_batch`, `tent_allocate_memory_ex`, `tent_register_memory_ex`, `tent_register_memory_batch_ex` |
| Batch and submit | `tent_allocate_batch`, `tent_free_batch`, `tent_submit`, `tent_submit_notif` |
| Notification | `tent_send_notifs`, `tent_recv_notifs`, `tent_free_notifs` |
| Status/cancel | `tent_task_status`, `tent_cancel_task`, `tent_overall_status`, `tent_task_status_list` |
| Rail observations | `tent_get_nic_load_stats` |

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

The control RPC assigns IDs for descriptor lookup, RDMA bootstrap, SendData,
RecvData, Notify, Probe, Delegate, Pin, Unpin, update subscription, update
notification and UB bootstrap. See [the RPC contract][rpc]. P0 must freeze the
actual framing/serialization and any external YLT/common dependencies before
choosing the Rust codec. RPC function IDs alone are not a wire specification.
Do the same for segment metadata, HP-TCP frames and provider handshakes.

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

OrbitTE owns byte movement and registration/operation lifetimes. OrbitKV owns
cache identity, replica authority, cache leases and recovery semantics. An
engine source epoch or a region generation does not prove an inference engine's
GPU page generation. DeepInfer is a prospective consumer; planning its use does
not establish a released cache integration. Native engine P/D remains owned by
the engine until separately integrated and qualified.

[api]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/transfer_engine.h
[types]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/common/types.h
[loader]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/transport_loader.cpp
[impl]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp
[runtime]: https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime
[tests]: https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/tests
[config]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/common/config.cpp
[shim]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/src/transfer_engine.cpp
[python]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/python/pybind.cpp
[metrics]: https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/metrics
[plugin]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/device_plugin.h
[transports]: https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport
[platforms]: https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/platform
[rpc]: https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/rpc/rpc.h
