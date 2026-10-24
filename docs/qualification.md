# Qualification

OrbitTE distinguishes three states:

| State | Meaning |
| --- | --- |
| Implemented | A reachable source path owns and performs the behavior |
| Qualified | The declared code/artifact passes its stated source or hardware profile |
| Accepted | An independent reviewer inspects the change and verifies its applicable gates |

A successful build or CPU fixture does not qualify physical DMA, a new fabric,
GPU direct transfer or performance.

## Current source delivery

P4.12g extends the native fixture without changing production source,
dependencies or wire v12 from PR #36. Frozen operational source `aebb3e8`
passes 19 sequential stable/Rust 1.85 gates, 190 all-feature units per toolchain
and ten hardware-runner controls. Sixteen valid Rust plans match the isolated,
unchanged frozen TENT `planRdmaSlices` function; inclusive-quarter and
missing-fold mutations are rejected on both toolchains. This function probe is
separate from the full frozen native TENT CPU transport comparison and does not
execute native TENT RDMA transport.

Both frozen libtest binaries pass 288 CPU DRAM/IPv4 RoCEv2 cases over two actual
RNICs per host: 144 new geometry cases and 144 affected two-/four-parent
regressions. Each compiler covers both directions, READ/WRITE and polling/event
progress. Eight parents exercise 5 MiB, MiB-plus-one folded and exact-quarter
tails, 32/40-QP reuse, five shared requester SQ credits and atomic ninth-parent
rejection with one residual source RQ credit. Large/folded paused-close and
either-rail native faults retain actual MR, DMA, WR and receive-credit owners.

All 576 peers and supervisors are reaped with both original RNIC locks released.
Raw receipts account for 1,152 context opens/closes, 16,948 MR
registrations/deregistrations, 272 source RQ rejections and 12,032 per-owner RQ
checks before CQ destruction. In 64 injected two-WR windows, the exact failing
WR receives its native remote-access CQE while one sibling remains charged
through both CQ teardown receipts. Four native mutation runs reject early WR
debt or receive-credit return in polling/event modes without supervisor timeout.

See the [geometry contract and evidence](rdma-cuda.md#larger-requests-folded-tails-and-eight-parent-admission)
for hashes, profiles and reproduction. The unchanged original masked-routing,
publication-rollback and partial-post profiles retain their previous evidence;
this delivery reruns the affected concurrent fixture. These are same-agent
scoped libtest gates. Installed-consumer, independent acceptance and the complete
P4 exit remain open.

## Previous source RQ admission evidence

P4.12f adds atomic whole-parent source RQ admission on wire v12. Frozen source
`7606c1a` passes 17 stable/Rust 1.85 source/build gates, 190 all-feature units
per toolchain, four hardware tests ignored by default and ten runner controls.

Both frozen libtest binaries pass 296 CPU DRAM/IPv4 RoCEv2 cases over two
physical RNICs per host: 64 new four-parent/three-slice cases and all 232 previous
regressions. Twelve 8 KiB slices share three requester SQ credits and thirteen
source RQ credits. A fifth three-slice parent is rejected atomically despite
one residual credit. Both peers avoid new QPs for rejected plans. All twelve
QPs reuse across rounds; paused unregister/close and either-rail CQ faults
retain exact MR, DMA, WR and receive-credit owners until actual drain.

All 592 peers are reaped with both original NIC locks released. Raw evidence
accounts for 1,184 context opens/closes, 5,411 MR registration/
deregistration receipts, 80 atomic source RQ rejections and 1,536 source RQ
owner checks before CQ destruction, including 478
checks of still-pending owners. One CPU mutation rejects omitted parent
admission; two native mutation runs reject early credit return in polling/event
mode. These are same-agent scoped libtest-profile gates.

See the [RQ admission contract and evidence](rdma-cuda.md#whole-parent-source-rq-admission)
for frozen binary hashes, reproduction and retained exploratory failures.
Independent acceptance and the complete P4 exit remain open.

## Previous concurrent-parent evidence

P4.12e adds cross-context aggregate CQ quota wakeups and validates concurrent
parents over two physical RNICs per host. Frozen source `c76b9b5` passes 17
stable/Rust 1.85 source/build gates, with 188 all-feature units per toolchain,
four hardware tests ignored by default, and ten hardware-runner controls.

Both frozen libtest binaries pass 232 CPU DRAM/IPv4 RoCEv2 cases: 80 new
concurrent-parent cases and all 152 previous regressions. Two parents each use
two slices; the requester shares two SQ credits, the source four RQ credits.
Tests cover aggregate byte/connection rejection before new native QPs,
four-QP reuse, paused unregister/close and either-rail real CQ errors. Native
debt accounting includes payload and same-QP completion SENDs. Already drained
siblings may retire independently; the injected parent must fail and retain its
remaining authority until actual cleanup.

All 464 peers are reaped with both original NIC locks released. Raw evidence
accounts for 928 context opens/closes, 3,239 MR registrations/
deregistrations (including message MRs), 192 failed-CQ destruction receipts and
32 actual partial admissions in the retained regression. One CPU control rejects
missing quota wakeups; four native controls reject aggregate SQ overruns or
premature WR-debt repayment. These are same-agent libtest-profile gates.

See the [concurrency and TENT correspondence contract](rdma-cuda.md#cross-context-quota-wakeups-and-concurrent-parents)
for binary hashes, native evidence, failed exploratory runs and reproduction.
Full P4, broader workloads/physical faults and independent acceptance remain open.

## Previous two-RNIC evidence

P4.12d validates the existing cross-rail runtime with two actual RNICs per host
in a declared CPU DRAM/IPv4 RoCEv2 profile. Frozen source `8fc28cb` passes 17
stable/Rust 1.85 source/build gates, with 187 all-feature unit tests per
toolchain and ten hardware-runner controls. Production source and dependency
manifests are unchanged from the merge of PR #33.

Both frozen libtest binaries pass 152 native cases: READ/WRITE, masks 1/2/3,
connected QP reuse, eager MR publication/rollback, real either-rail CQ errors
and partial posts, and paused-post unregister/close. Both peer directions and
polling/event progress are covered. All 304 peers and supervisors are reaped
with both original NIC locks released. Native receipts account for 608 context
opens/closes and 1,776 MR registrations/deregistrations, including completion
messages. Four native negative controls reject one-payload-rail dispatch or
early repayment of the failed record's own WR debt; none times out.

These are same-agent gates over test-only native observers and public engine
operations. They qualify the exact libtest artifacts/profile. They do not
establish installed-consumer support, broader physical faults, performance,
independent acceptance or full P4. See the
[two-RNIC contract](rdma-cuda.md#two-rnic-native-qualification-and-failure-drain)
for binary hashes, raw evidence and reproduction.

## Previous publication and slicing evidence

P4.12c implements eager all-context host Direct MR publication and cross-rail
slice ownership/completion. Frozen source `14c0ace` passes 17 stable/Rust 1.85
checks, with 187 all-feature unit tests per toolchain and four effective CPU
mutation controls. These cover registration rollback, one source handler,
concurrent context workers and exact retained-loan behavior.

Both release binaries pass 48 two-host CPU DRAM/IPv6 RoCEv2 cases, including
actual registration rollback, CQ errors, partial post admission, connected
reuse and duplex transfers. All 96 peers exit with locks released. Sixteen
same-RNIC/two-PD helpers verify 32 real PD constructions and 112 MR registration
and deregistration receipts; 48 copies belong to rejected publication attempts.

That earlier profile used one selected RNIC per host; it did not qualify
physical two-RNIC striping/faults. The current P4.12d profile above adds that
bounded native scope. Independent acceptance remains open. See [the P4.12c gate contract](rdma-cuda.md#p412c-gates)
for frozen artifacts and reproduction boundaries.

## Previous native RDMA evidence

The earlier context-selection source, `4b3638c`, passes 13 sequential stable /
Rust 1.85 source and build gates, with 179 all-feature unit tests per toolchain.
Its two frozen binaries pass 48 two-host CPU DRAM / IPv6 RoCEv2 regression
cases. All 96 peers are reaped and the original NIC locks are released.

The regression includes shared-CQ and context-specific MR ownership, connected
reuse, duplex transfers, actual CQ errors and actual partial post admission.
Sixteen same-RNIC registration helpers account for 32 real PDs and 64 native
MR registration/deregistration receipts. Three CPU mutation controls reject
ignored device masks, compacted NIC IDs and removed cached-PD validation.

These are same-agent scoped gates using one selected RNIC per host. They do
not qualify scheduling over two physical RNICs. Independent acceptance and
the full native P4 stage remain open.

See the [detailed evidence and reproduction contract](rdma-cuda.md#scoped-p412b-validation)
for frozen binary hashes, native library identities, external artifact paths
and the negative controls. Raw logs, profiles, generated figures and per-run
reports remain outside the checkout and Git history.

## CPU and historical profiles

The [frozen CPU comparison](cpu-oracle.md) runs a shared behavioral trace
against unmodified native TENT in separate processes. It compares bytes,
registration, batch and notification observations; it does not establish
mixed-peer wire compatibility.

Earlier accepted TCP, host registration, source MW, key domain, shared-progress
and connected-reuse profiles remain scoped to their recorded source/artifacts.
Historical NVIDIA host-staging and consumer experiments do not qualify a new
GPUDirect path or a new installed consumer. The [RDMA contract](rdma-cuda.md)
preserves those records and their limits.

## Remaining stage boundary

The [completion plan](completion-plan.md) owns the delivery sequence. Complete
P4 before renewing OrbitKV integration. Outstanding work includes broader
multirail workload/topology coverage, physical link/device loss, lost completions,
native cleanup failure, restart/shutdown profiles and independent acceptance.
The declared two-RNIC routing/slicing, eight-parent geometry/RQ admission/reuse,
provider-fault and paused-shutdown gates pass; they do not close those broader
profiles. The source paths are implemented; a missing hardware gate is not missing code.
GDR and optional storage transports have their own stages.
