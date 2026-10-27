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

This is **single-host diagnostic evidence**, not acceptance of the declared
two-host profile, interrupted in-flight DMA or physical loss. The two Forge
replicas currently share a physical host. Earlier libtest cleanup evidence
remains scoped to its own artifact; independent acceptance is still open.

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
