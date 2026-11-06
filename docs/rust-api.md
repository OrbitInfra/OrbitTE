# Rust API Guide

The package is `orbite` and the crate is `orbite`. Generate the full type
documentation with:

```sh
cargo doc --locked --no-deps --open
```

Add `--features rdma,cuda` in an environment with the
[build prerequisites](build.md).

The separate [C API](c-api.md) exports the TENT constructor and its thread-local
settings. Rust applications can construct a typed `EngineConfig` directly.

## Transfer requests

`TransferRequest::new(transport, local_address, remote_address, length)` starts
with `Priority::High`, the engine's default QoS contract and remote staging off.
Override them with `.with_qos(policy)`, `.with_priority(priority)` and
`.with_remote_staging(true)`; an explicit QoS policy supplies its own priority.
Use `Transport::Unspec` for [transport selection](transport-selection.md).
`TransferRequest`, `Transport` and `Error` are non-exhaustive, so matches outside
the crate need a wildcard arm.

Both ranges must lie inside one applicable registration, and `TransferOp`
selects READ or WRITE. Submission resolves and retains every backing allocation
before any transfer effect. Priority orders RDMA payloads on each worker; TCP and
NVLink accept every priority as advisory, and requests within a batch keep their
order.

## Engine and registration

`TransferEngine::new(EngineConfig)` starts a source control service (loopback by
default) and owns requester and source admission. Configuration is immutable per
engine. `engine.config()` borrows its effective settings.

### Loading TENT configuration

```rust
use orbite::{EngineConfig, TransferEngine};

fn open_engine() -> orbite::Result<TransferEngine> {
    TransferEngine::new(EngineConfig::from_tent_env()?)
}
```

Use `EngineConfig::from_tent_json(text)` or `from_tent_file(path)` to supply
constructor settings. A nonempty `MC_TENT_CONF` replaces that document with
the contents of an existing file or with inline JSON. Legacy environment
aliases then override individual leaves. Explicit constructor metadata type,
servers, segment name, RPC hostname and port, and RDMA whitelist win over
both sources. An explicit true `transports/force_tcp` also survives
replacement. The presence of `MC_FORCE_TCP` forces memory traffic onto TCP,
including when its value is empty or `0`. `NCCL_IB_GID_INDEX` wins over
`MC_GID_INDEX`. NIC filters accept trimmed comma-separated names. Boolean
aliases accept `1/0`, `true/false`, `yes/no` and `on/off` without regard to
case. An invalid legacy boolean retains the configured leaf and logs a warning.
For metrics, a JSON leaf wins over its environment alias.

Nested objects take precedence over full slash keys at the root. The loader
rejects malformed roots, duplicate keys, unsupported fields and out-of-range
values before starting an engine. Each input document is limited to 1 MiB.
You can reuse the returned configuration after deleting the file. Ordinary
typed construction retains the existing subsystem environment switches.
The [configuration inventory](https://github.com/OrbitInfra/OrbiTE/blob/main/tests/reference/inventory/config.tsv)
lists the owner or rejection for each field.

| API | Contract |
| --- | --- |
| `register_memory` | Move host bytes into a registered range |
| `register_host_memory` | Register a range of an existing `Arc<HostMemory>`, retaining its allocation owner |
| `register_host_memory_batch` | Preflight a set of host ranges, then publish them together |
| `register_cuda_memory_batch` | Register CUDA allocations (`cuda` feature) |
| `unregister_memory` | Drop a registration reference; admitted users keep their backing |
| `resolve_local_address` | Resolve a contained registered range without adopting a raw pointer |

`Permission` distinguishes local-only, remote READ, remote WRITE and remote
READ/WRITE; repeated registration of a range keeps its first permission. With
RDMA host Direct policy, registration also prepares MRs on every usable context
before the range becomes visible, so it can fail when native resources are
exhausted; see the
[publication contract](rdma-cuda.md#eager-publication-and-cross-rail-slice-completion).

## Admission and QoS configuration

| Setting | Purpose | Guide |
| --- | --- | --- |
| `EngineConfig::runtime_queue` | FIFO/EDF dispatch window for all data submissions | [Dispatch window](async-batches.md#runtime-admission-and-dispatch-window) |
| `EngineConfig::qos`, `QosConfig::from_json(_file)` | Global/tenant/intent contracts; `resolve_qos` returns a reusable `EffectiveQosPolicy` | [QoS](async-batches.md#qos-contracts), [actions](async-batches.md#qos-admission-actions) |
| `EngineConfig::max_receiver_credit_sessions` | Sender credit gating; `activate`/`apply`/`deactivate_receiver_credit` for manual grants | [Credits](async-batches.md#receiver-credit-gating) |
| `EngineConfig::receiver_credit` | Receiver-side issuer; `connect`/`disconnect_receiver_credit` for automatic windows | [Automatic windows](async-batches.md#automatic-receiver-windows) |
| `EngineConfig::selector`, `TransportSelectorConfig::from_json` | Ordered transport selection rules (TENT's `policy` array) | [Selection](transport-selection.md) |

`EffectiveQosPolicy::fields()`, `effective_priority()` and `usage()` explain a
resolved policy and sample its budget.

## Memory type and location

`RegisteredMemory::memory_type()` returns `MemoryType::Cpu` or `Cuda` from the
retained backing, without locks or driver calls. `RegisteredMemory::location()`
returns `RangeLocation { address, length, location }` clipped to that
registration, and `HostMemory::location(offset, length)` /
`CudaMemory::location(offset, length)` check the owner subrange first. These
queries neither wait for producer readiness nor grant payload access.

`MemoryLocation::Cuda { device, context }` displays as `cuda:N`; the context
number is diagnostic, not a usable handle. `MemoryLocation::Cpu(Some(N))`
displays `cpu:N` and `Cpu(None)` displays `*`. Ordinary, external and portable
pinned host memory have unknown NUMA placement, and pinned DRAM stays CPU memory.

External CUDA adoption makes one `cuPointerGetAttributes` call (type, managed
status, context, device), resolves the device with `cuDeviceGet` and checks the
whole range with `cuMemGetAddressRange` in that context. Unknown attributes,
managed or host pointers and native errors fail adoption and never select CPU
access. This covers the owner-backed part of TENT's `Platform` `getMemoryType`,
`getPointerDeviceId` and `getLocation`; there is no general pointer classifier,
CPU page-residency scan or device-plugin ABI. See
[qualification](qualification.md#cuda-pointer-and-location).

## RDMA bandwidth and load

With `rdma`, `RdmaConfig::scheduling` configures `SchedulingParams` for
bandwidth-aware selection (on by default; `smart_selection = false` uses
round-robin in the first tier); see
[selection](rdma-cuda.md#bandwidth-aware-rdma-selection).
`TransferEngine::nic_load_stats()` returns initialized NICs without discovery:
rate estimates in **bytes per second**, `link_speed_gbps` in Gbit/s,
`inflight_bytes` (grant admission), `posted_bytes` (native payload debt) and
cumulative completed bytes, busy time and sample counts. Priority settings
(`deadline_bw_arbitration`, `priority_promotion_timeout`,
`priority_promotion_per_entry`, `shared_quota_shm_path`,
`slot_rotation_interval`) are described in
[request priority](rdma-cuda.md#request-priority-and-bandwidth-arbitration).

## Peer authority and file segments

```rust
let peer = requester.open_segment(
    source.endpoint(),
    source.source_epoch(),
    Duration::from_secs(1),
)?;
```

The expected `SourceEpoch` must come from the authoritative source owner;
descriptors identify regions but never replace that authority, so a restarted
process at the same endpoint cannot be reached through an old handle.

For a local file, `open_file_segment` takes a `file://` name and a permission;
`file_segment_info` returns the cached descriptor, `refresh_file_segment` re-reads
the size from the same fd, `resolve_file_offset` checks a range, and
`close_file_segment` closes every alias and waits for drain. See
[file segments](storage.md).

## Reports

`submit_and_wait` returns a `BatchReport` with completed request and byte counts,
failure details, notification status and local drain status. The completed prefix
is not a transaction. `RemoteEffect::Unknown` means an attempted WRITE may have
changed peer bytes, and never authorizes replay. `TransferFailure.affected_requests`
counts the consecutive tasks that share a failure from `request_index`; only later
requests were never attempted. A multirail or remote-staged request counts only
after all its slices or chunks complete, but earlier slices may already have
changed bytes. For asynchronous ownership see [async batches](async-batches.md).

For explicit progress, call `engine.progress_batch(batch.id()?)?`. A status
query observes the published result. `EngineConfig::enable_progress_worker`
enables event-driven turns and deferred free. Use
`Batch::submit_with_notification_targets` to track deliveries independently
of the payload result. See [explicit progress](async-batches.md#explicit-progress)
for backoff, queue wakes, bounded retries and owner retirement.

## Native providers

The CUDA feature accepts compatible cudarc 0.19 releases. Rust 1.85 builds
use 0.19.0 in the checked-in lockfile. Consumers resolving a fresh lockfile
on 1.85 can select it with `cargo update -p cudarc --precise 0.19.0`.
Consumers using cudarc 0.19.8 share one registry package with OrbiTE on
Rust 1.88 or newer, as required by that release's libloading dependency.

OrbitKV deliberately passes its `Arc<cudarc::driver::CudaStream>` as the
lifetime owner for local copies. Code that names that type follows cudarc's
semver. The underlying `unsafe LocalCopyEngine::bind` accepts a
`*mut std::ffi::c_void` and an `Arc<dyn Send + Sync>`. A caller with its own
CUDA binding can supply its raw stream and owner directly. The same contract
applies to `ProducerEvent::from_raw`, so a future C adapter can retain its
own handles. [Local copy ownership](local-copy.md)

`transport::rdma::RdmaConfig` selects discovery and pins, host Direct or staged
memory, progress mode (`Polling` by default, or `EventDriven`) and native budgets.
The `cuda` module owns device and pinned-host allocations and synchronized copies;
device transfers use staging or explicit GPUDirect RDMA. Unsafe
`CudaMemory::record_producer_stream` and `HostMemory::record_producer_stream`
hand already-queued producer work to transfers; read the
[producer contract](rdma-cuda.md#producer-stream-readiness) before using them.

## Close and drain

`close_source` and `close_requester` fence one direction; whole-engine `close`
fences both, drains accepted work and closes shared native resources. A timeout
never completes pending native work, and `Error::Unquiesced` keeps the real
resources and charges when cleanup could not be proved. With `cuda`,
`flush_nvlink_cache(timeout)` closes the cached imports present when it starts
while admission stays open; see
[the cache contract](cuda-ipc.md#cross-submission-relocation-cache).

Runnable examples: [TCP](../examples/tcp_roundtrip.rs),
[external allocation](../examples/external_host.rs) and
[async batches](../examples/async_batch.rs).
