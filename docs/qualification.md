# Qualification

Qualification belongs to an exact source, artifact and execution profile.
The [capability matrix](capabilities.md) describes current behavior; the
[completion plan](completion-plan.md#p4-remaining-delivery) is the only queue
for remaining P4 work.

| State | Meaning |
| --- | --- |
| Implemented | A reachable source path owns and performs the behavior |
| Qualified | The declared artifact passes its stated source or hardware profile |
| Accepted | An independent reviewer inspects the change and verifies its applicable gates |

A build or CPU fixture does not qualify DMA, a new fabric, GPU direct transfer
or performance. Earlier hardware results retain their original revisions and
do not automatically qualify subsequent changes.

The latest installed delivery qualifies [automatic discovery](#installed-automatic-discovery)
against a separately captured host inventory, retaining the full
[physical rail accounting](#installed-physical-rail-accounting) contract.
The production host RDMA implementation is complete; physical/fabric
qualification and complete acceptance remain in the existing P4 queue.

## CUDA copy streams and completion events

Source [`ce86269`](https://github.com/OrbitInfra/OrbitTE/commit/ce86269d38ce766d4aadd79c7dc245c7ce013699)
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
[`121ab95`](https://github.com/OrbitInfra/OrbitTE/commit/121ab95a77f94a80286617c4a2032654e692f9b4)
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

Source [`36890c8`](https://github.com/OrbitInfra/OrbitTE/commit/36890c87b71c56922149824633d5b5811bc479e4)
passes four frozen two-host configurations: READ/polling and WRITE/events with
stable 1.99.1, then both in the reverse direction with Rust 1.85.0. Each process
leaves device, port and GID pins unset and selects within its two locked mlx5
RNICs. An identical blacklist confirms whitelist precedence. Before normal
registration, a separate engine attempts a 64-byte registration with absent
strict GID 255; registration returns `Unavailable`, and close leaves no owners.

A packaged, hash-pinned collector reads sysfs without importing OrbitTE's
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

Source [`cc6ab0f`](https://github.com/OrbitInfra/OrbitTE/commit/cc6ab0fe0092c0d1d06792bb7190f48832f0eae9)
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

Source [`9e1fdea`](https://github.com/OrbitInfra/OrbitTE/commit/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578)
retains initially paused contexts and reselects automatic GIDs during recovery,
following the frozen TENT context lifecycle. Persistent MRs remain on the same
PD; explicit pins and physical drain still gate route publication. Public API
and wire v14 are unchanged. The preceding `4fbe01d` delivery reorganizes context
ownership and records the [source audit](architecture.md#p4-source-audit).

Stable 1.99.1 and Rust 1.85.0 each pass all-feature Clippy and **239
CPU/integration/doc tests**. The [eight-variant CI](https://github.com/OrbitInfra/OrbitTE/actions/runs/40448463937),
[frozen TENT CPU comparison](https://github.com/OrbitInfra/OrbitTE/actions/runs/40448463956)
and [documentation checks](https://github.com/OrbitInfra/OrbitTE/actions/runs/40448463913)
pass for that delivery. Local manifests and logs are in
`/root/orbitte-artifacts/rdma-context-recovery-20261027/delivery.json`.

### Focused native recovery

The [native context/GID gate](rdma-cuda.md#initially-paused-contexts-and-gid-reselection)
now passes on one physical mlx5 RoCE RNIC with stable 1.99.1 and Rust
1.85.0. Execution source is [`bde9284`](https://github.com/OrbitInfra/OrbitTE/commit/bde9284a950dd93bfa4069061ddb54943af0cc61);
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
passes with source [`56edccc`](https://github.com/OrbitInfra/OrbitTE/commit/56edcccd3c94d6504b5f65f8cb14f66051cbdc10):
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
[`9a6c270`](https://github.com/OrbitInfra/OrbitTE/commit/9a6c27056ea4a68855b296cfa03ead1607dc7e86).
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
from [`1428138`](https://github.com/OrbitInfra/OrbitTE/commit/1428138b83a1ef16b55053d7f83a7c7eefcb80c3).
Controller source is
[`b83dbc3`](https://github.com/OrbitInfra/OrbitTE/commit/b83dbc32867cb434e130e8ec3b3b9b4d50fd1d33);
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
from [`78127d7`](https://github.com/OrbitInfra/OrbitTE/commit/78127d7c12aa05789308d0974fa9cd22c84274b0).
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
| Installed lost completion — `02c15bd` | Across both compiler artifacts: 40 two-host/two-RNIC configurations, including 32 completion-loss cases. The tool observes actual successful payload CQEs, withholds them from the engine, then supervises peer death and survivor drain/reconnect. | `rdma-installed-completion-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#latest-installed-lost-completion-delivery) |
| Installed posted-RQ interruption — `c27029c` | Peer death after real completion receives and MWs are posted, before the requester receives payload authority. The surviving engine keeps its PD/MRs. | `rdma-installed-interruption-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-installed-posted-rq-interruption-delivery) |
| Installed peer restart — `0b6229c` | Idle persistent-resource restart with one surviving engine; old epochs are rejected before new native payload work. | `rdma-installed-failures-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-installed-peer-restart-delivery) |
| Native cleanup — `d5514d4` | Across both compiler artifacts: 44 configurations. Actual QP destructor EIO and completion-channel EBUSY, with CQ/PD and MR/MW regressions, verify retained owners and fresh-process recovery. | `rdma-native-cleanup-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-native-cleanup-delivery) |
| Installed healthy host — `ba09b1f` | 72 configurations across two compiler artifacts: READ/WRITE, both directions, polling/events, widths 1/3, rail masks 1/2/3 and selected 12/16/32-parent workloads. | `rdma-installed-host-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-installed-host-delivery) |
| Periodic monitor recovery — `08fb8ac` | Across both compiler artifacts: 276 public configurations, including 64 process-local query-failure recovery cases, plus 32 internal shared-QP cases. PD/MRs and healthy-rail QPs survive drained-lane replacement. | `rdma-monitor-recovery-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-periodic-monitor-recovery-delivery) |
| Fixed-width cold construction — `2d44743` | Concurrent first requests reserve bounded array slots before native QP creation; subsequent rounds reuse the published connections. | `rdma-cold-connections-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-fixed-width-cold-construction-delivery) |
| Public concurrent grants — `8511c53` | Public v14 grants share physical QPs with separate grant/SQ/RQ limits and exact completion dispatch. | `rdma-public-qp-20261026`; [record](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-public-concurrent-grant-delivery) |

These are scoped correctness results. Lost CQEs after successful DMA do not
prove interruption of in-flight DMA. Query-failure injection and QP ERR do not
prove physical cable/device loss. Native libtest cleanup faults do not qualify
an installed public consumer's cleanup-failure path. RoCE does not qualify
InfiniBand, GPU direct transfer or throughput/fairness.

The [earlier evidence index](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/qualification.md#previous-qualified-native-source-concurrent-grant-records)
preserves the preceding QP ownership, worker pool, CQ lane, multirail,
MR/MW-failure and live-recovery records. The [historical RDMA guide](https://github.com/OrbitInfra/OrbitTE/blob/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs/rdma-cuda.md)
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
optional storage transports, remaining runtime policies and API/wire parity
retain their existing stages. OrbitKV integration follows accepted P4.

## Historical record preservation

The documentation cleanup preserves the complete pre-cleanup pages at
[source `9e1fdea`](https://github.com/OrbitInfra/OrbitTE/tree/9e1fdeaa9f7cd6c03e30aab2dff19a00f0a44578/docs).
An external byte-for-byte archive and SHA-256 manifest are also retained at
`/root/orbitte-artifacts/p4-closeout-20261027/documentation-before/manifest.json`.
Original experiment directories remain in place. This reorganization changes
navigation and removes duplicate status prose; it grants no new hardware
qualification.
