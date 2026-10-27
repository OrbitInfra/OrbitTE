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

The latest installed delivery adds [CQ/channel/PD dependency cleanup](#installed-dependency-cleanup)
to the qualified host RoCE profiles. The production host RDMA implementation
is complete; physical/fabric qualification and independent acceptance remain
in the existing P4 queue.

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
