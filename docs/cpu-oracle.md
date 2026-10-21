# Frozen CPU comparison

P0.1 supplies one executable behavioral trace for the existing CPU/TCP slice.
It runs public OrbitTE Rust APIs and the frozen native TENT C ABI in separate
runner processes. Each runner owns a source engine, requester engine and actual
host backing. Remote operations use real loopback TCP between those engines.
This is a correctness gate, not a throughput comparison or mixed-peer test.

## Reference and build boundary

The machine-readable [contract](../tests/reference/contract.json) pins Mooncake
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`, its pybind11 gitlink and YLT archive
revision/SHA-256. The runner checks clean reference source, both Git revisions
and the archive hash before building. Compiler commands, effective CMake cache,
binary/library hashes and loaded dependencies remain in external artifacts.
Native code is compiled from the unmodified reference; no C++ implementation
is copied into OrbitTE.

The reference build disables CUDA/HIP, runtime metrics and YLT IBV. Upstream
CMake can still compile/link discovered RDMA or HTTP support. The native driver
explicitly enables TCP and disables every other transport, selects P2P metadata,
uses four RPC threads, and disables merging, the runtime queue and background
progress. Compiled provider availability does not qualify those paths. No
external metadata service or accelerator is used by this trace.

Reference sources, libraries and build output stay outside the OrbitTE checkout
and OrbitKV's staged runtime. The runner removes inherited Mooncake configuration
and native loader overrides. It verifies that native Mooncake/ASIO dependencies
resolve inside its private build and that the Rust executable has none of those
dependencies. Normal Cargo builds do not build or load TENT.

## Trace and observations

[`cpu-v1.trace`](../tests/fixtures/cpu-v1.trace) is a small deterministic source
fixture. [`cpu-v1.expected`](../tests/fixtures/cpu-v1.expected) defines exact byte,
descriptor, completion and notification observations from its specified inputs.
Both drivers must match that expectation and each other; matching two wrong
outputs is insufficient.

| Contract | Trace coverage | Remaining boundary |
| --- | --- | --- |
| F04 host registration | Existing backing, exact duplicate registration, first permission retained during subsequent WRITE, partial/final unregister and descriptor refresh | Failed provider free, overlapping native registrations and retained DMA |
| F05/F07/F08 CPU movement | Noncontiguous batched READ, offset WRITE, exact bytes/sentinels and completed byte counts | Native error mapping, partial posts, cancellation, failover and mixed peers |
| F08 terminal completion | Native batch polled repeatedly before free; notifications must not be redelivered | Asynchronous multi-target hooks and merged task aliases |
| F11 notifications | Explicit local/remote, attached READ/WRITE, empty submission, deliberate repeats, empty names/messages and empty polls | Ambiguous native receipts, provider fallback and durable/deduplicated delivery |
| Selected F09 C output | Native notification result reuse, reset on free and repeated free | OrbitTE C ABI and the remaining 33-symbol/native contract |

The text format starts with `ORBITTE_CPU_TRACE 1`. `BUFFER` selects a named
source/local backing with length and `pattern` (index modulo 251) or one fill
byte. `REGISTER`, `UNREGISTER` and `DESCRIBE` change or inspect registration.
`FILL` and `BYTES` specify offsets/lengths. `READ`/`WRITE` take a label,
comma-separated `local_offset:remote_offset:length` slices, notification name
and message. `~` disables the attached notification; `-` means empty strings
or an empty slice list. `NOTIFY` selects local/remote delivery; `TAKE` drains
the source inbox. Fixtures use bounded, valid ranges and C-string-compatible
messages. Random addresses, IDs and timing are excluded from observations;
each driver checks that descriptors identify its actual backing.

Lines beginning `OBS` are compared exactly. Native errors, failed processes,
timeouts, missing observations, extra messages and missing cleanup fail the run.
Six comparator controls deliberately corrupt bytes/counts/status or remove/add
observations and require rejection. These are comparator checks, not network,
epoch or lifetime fault injection. The existing Rust tests retain their real
restart, partial-body, close/drain and lost-receipt controls; running this shared
trace does not qualify corresponding behavior in unmodified native TENT.

## Run the gates

Use Linux, Rust 1.85+, Python 3.11.8+, Git, CMake, a C++20 compiler and permission
to bind loopback sockets. Select a **new external directory for each run**;
the runner refuses to overwrite evidence or put it inside a source checkout.

The fast source gate needs no native libraries or downloads:

```sh
python3 tests/reference/run.py --rust-only --output /var/tmp/orbitte-fixture-001
```

It explicitly reports `passed-rust-fixture-only`; it cannot pass the native
comparison. Stable/MSRV CI runs this gate alongside ordinary Cargo checks.

For the real native comparison on Debian/Ubuntu, install the build dependencies
listed in [the native CI workflow](../.github/workflows/native-cpu.yml), then:

```sh
python3 tests/reference/run.py --fetch-reference --output /var/tmp/orbitte-native-001
```

This explicitly downloads the fixed public Git commit/submodule and archive.
For an already provisioned clean reference and verified archive:

```sh
python3 tests/reference/run.py \
  --tent-source /var/tmp/tent-reference \
  --ylt-archive /var/tmp/7801bc9ad9021781f15217552214e325a1cf7373.tar.gz \
  --output /var/tmp/orbitte-native-002
```

The full run reports `passed-cpu-trace` only after both real runners exit
successfully and all 24 observations match. Its manifest preserves exact inputs,
commands, command outcomes, binary hashes, native dependency paths/hashes,
observations, comparator controls and provenance. Failures keep their logs and
mark the manifest failed. Timeout kills are failures, never drain receipts.
Native CI runs for core/trace changes and uploads its evidence, including failed
runs. Source commit provenance and a clean reference do not turn an untested
hardware or API surface into an accepted profile.

## Qualification limits

This first comparison has valid-input CPU behavior coverage. It leaves the full
P0 inventory/oracle, TENT YLT/HP-TCP interoperability, metadata schemas, all native
errors/configuration, physical two-host tests, RNIC/GPU/storage and consumer
installed qualification open. OrbitKV-specific source epochs and source-close
extensions need their own audited adapter controls. Use the
[completion plan](completion-plan.md) for subsequent delivery and independent
acceptance; there is no second execution queue here.
