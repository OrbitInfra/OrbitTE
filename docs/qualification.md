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

Periodic context/endpoint reclaim and selected-port recovery are implemented.
The monitor queries native addresses every second, filters port events, preserves
newer fault generations and resumes selection only after old native owners drain.
PDs and persistent MRs survive; failed cleanup and event-consumer loss remain
quarantined. Recovery never replays an ambiguous payload.

Source [`08fb8ac`](https://github.com/OrbitInfra/OrbitTE/commit/08fb8ac2c8c1aef5b6c94e04c7583f6870eb6888)
passes 23 local gates on stable 1.99.1 and Rust 1.85.0, including 225
all-feature unit tests per compiler, twelve runner controls and frozen TENT
geometry comparisons. The native TENT CPU oracle passes 25 observations and
six negative controls; its merge tree, inputs and native/Rust binary hashes
match this source. The delivery also fixes a CPU test's observation race:
CACHE_MISS can arrive before the handler drops temporary admission, so the
test now awaits handler completion before checking charges.

Two frozen release binaries pass **276 public two-host/two-RNIC
configurations**, including **64 periodic-recovery cases**, plus **32 internal
shared-QP configurations**. Recovery covers READ/WRITE, polling/events, both
source/requester roles, either rail and widths 1/3. The fixture calls real
verbs, injects a process-private port-query failure, waits without new
submissions for old QPs/CQs to drain, then restores native queries without
emitting PORT_ACTIVE. Two complete byte/sentinel rounds verify reconnection
and warm reuse, unchanged PD/MRs and preserved healthy-rail QPs.

Existing cold construction, concurrent grant/RQ pressure, folded-tail geometry,
QP ERR, lost successful CQEs, MR/MW syscall errors, CQ/PD EBUSY, live reconnect
and process restart controls pass. All **664 supervised children** are
reaped, including 24 intentional kills. Postflight verifies node/provider
identity, released RNIC locks and no GPU compute. A failed development run
exposed the bounded-array bootstrap retry bug and remains with the evidence.

Raw manifests, binaries, failed runs and source/CI controls remain outside Git
at `/root/orbitte-artifacts/rdma-monitor-recovery-20261026`.
`native-raw-audit.json` reconciles recovery, native ownership and process/lock
records; `shared-native-audit.json` checks exact grant/receipt drain.
`final-delivery.json` pins source, delivery, CI and evidence hashes. See
[the recovery contract](rdma-cuda.md#periodic-monitor-recovery) and
[resource ownership](architecture.md#periodic-monitor-reclaim-and-port-recovery).

These are scoped, same-agent source/native gates. Query-failure injection is
not a physical link flap. Physical port/device loss, InfiniBand, installed host
qualification and independent P4 acceptance remain open. GPUDirect RDMA,
NVLink, general retry/bandwidth policy and native TENT wire parity are later
stages. OrbitKV integration remains deferred until P4 acceptance.

## Previous fixed-width cold construction delivery

Wire v14 implements fixed-width cold construction. Both peers negotiate the
smaller `num_lanes` width before preparing a grant. A canonical endpoint owns
unbound, handshaking, connected and native-retained slots; concurrent grants
share published pairs. The source publishes before GRANT, and each construction
token protects its slot from cancellation by an older guard. Staged/device paths
continue using non-reusable per-grant connections.

Source [`2d44743`](https://github.com/OrbitInfra/OrbitTE/commit/2d44743dd80dd2c37d9d15680258d6eed7d2c6f3)
passes 23 local gates on stable 1.99.1 and Rust 1.85.0, including four feature
combinations, 218 all-feature unit tests per compiler, twelve runner controls
and the frozen TENT geometry comparison. CPU controls force cold contention,
check timeout/cancellation/retry and failed ACCEPT rollback, and preserve slot
charges on failed native cleanup. Unequal lane counts and simultaneous
bidirectional first contact pass with role-specific arrays.

The two frozen release binaries pass **212 public two-host/two-RNIC
configurations**, including **32 forced cold-construction cases**. In each cold case,
all eight parents reach either construction or slot waiting before the gate
opens; native QP creation is still zero at that barrier. READ/WRITE,
polling/events, both directions and widths 1/3 are covered. Forty grants fit
two physical QPs in the single-lane/two-RNIC profile. Every observed array
stays within its negotiated width, and warm rounds preserve physical QPs.
Existing grant/RQ pressure, folded tails, QP ERR, missing successful CQEs,
MR/MW syscall errors, CQ/PD EBUSY, live reconnect and process-restart profiles
pass with those width assertions. Another 32 internal shared-QP configurations
cover reversed receipts and lost-CQE drain on each RNIC and both compilers.

All **536 supervised children** are reaped: 512 completed libtests and
24 deliberate kills. Postflight verifies the original node/provider identities,
both RNIC locks available and no GPU compute. The frozen native TENT CPU oracle
passes 25 observations and six negative controls, with its exact CI source
tree, input hashes and native/Rust binaries checked.

Removing the fixed slot limit in an external source copy makes the atomic
concurrency test fail (32 admissions into a width-one array). A deterministic
Unix-stream control also reproduces the pre-fix monitor-close EPIPE: after
source detach, the worker may close its wake reader before the stop write.
Close now waits for exit publication and joins the worker; live event sources
still prevent closing. This is a shutdown fix, not periodic port recovery.

Raw manifests, binaries, controls and failed development runs remain outside
Git at `/root/orbitte-artifacts/rdma-cold-connections-20261026`.
`native-raw-audit.json` checks physical width and builder/waiter counts;
`shared-native-audit.json` checks independent receipt/drain controls.
`final-delivery.json` pins the implementation, documentation-only delivery,
CI checks and evidence hashes. See [the bootstrap contract](rdma-cuda.md#fixed-width-cold-bootstrap)
and [resource ownership](architecture.md#fixed-width-cold-construction).

These are scoped, same-agent source/native gates. Independent acceptance,
periodic monitor recovery and final installed host qualification remain open.
Physical port/device loss, InfiniBand, GPUDirect RDMA, NVLink and native TENT
wire interoperability are not qualified by this delivery. OrbitKV integration
remains deferred until P4 is accepted.

## Previous public concurrent-grant delivery

Public v13 handshakes share live physical QPs across independent exact-range
grants. Whole-parent `max_grants` reservations are separate from physical
`max_endpoints`; the source also reserves RQ capacity before exposing authority.
RQ depth uses `max_cqes`, and message-buffer credits survive until MR cleanup.
Idle eviction requires every sibling grant to finish. Both connection identities
remain exact; the observed previous grant is a monotonic lower bound, while
every offer and completion retains its exact binding and WR namespace.

Source [`8511c53`](https://github.com/OrbitInfra/OrbitTE/commit/8511c53058b3676c9f8dce93fcb52b176b2e63f8)
passes 23 local gates on stable 1.99.1 and Rust 1.85.0: four feature combinations,
210 all-feature unit tests per compiler, twelve hardware-runner controls and
the isolated frozen TENT slice-geometry comparison. Its two frozen release
binaries pass 180 public two-RNIC regressions and 32 internal shared-QP controls
on two hosts, with CPU DRAM and IPv4 RoCEv2. The public profiles cover READ/WRITE,
1/2/3/6 CQ lanes, polling/events, whole-parent grant/RQ rejection, unequal and
folded tails, actual QP ERR, lost successful SQ/RQ CQEs, MR/MW syscall errors,
CQ/PD EBUSY, live reconnect and two-/three-generation process recovery.

The public protocol admits eight parents/forty grants over two physical QPs
in the observed single-lane, two-RNIC runs. A second healthy round opens zero
new QPs and preserves the previous physical connection set. Native logs count
physical QPs separately from active grants, reconcile SQ debt per grant
namespace and physical QP, and keep RQ credits charged through message MR
cleanup. These are correctness and ownership observations, not throughput
measurements or a guarantee of simultaneous DMA execution.

All 472 supervised children are reaped: 448 completed libtests and 24 deliberate
kills. Postflight confirms both RNIC locks are available, node/provider
identities are unchanged and no GPU compute was used. The actual frozen TENT
CPU comparison passes 25 observations and six negative controls; its binary
hashes, input hashes and identical CI merge tree are checked. The source CI
matrix and strict documentation checks pass.

Raw logs, failed development controls, frozen binaries and manifests are outside
the checkout at `/root/orbitte-artifacts/rdma-public-qp-20261026`.
`native-raw-audit.json` reconciles public cases; `shared-native-audit.json`
reconciles the internal four-grant controls. `final-delivery.json` pins the code,
documentation-only delivery commit and hashed evidence index. See the
[public protocol contract](rdma-cuda.md#public-concurrent-qp-grants) and
[shared-QP reproduction contract](rdma-cuda.md#shared-qp-grant-record-profile).

These are same-agent scoped source/native gates; independent acceptance and
installed-release qualification remain open. Concurrent cold construction can
still exceed a peer's configured lane width under the aggregate QP limit.
Fixed-width bootstrap reservations, periodic monitor recovery, physical
port/device faults, GPUDirect/performance and complete P4 acceptance remain
open. OrbitKV integration is deferred until that P4 boundary is accepted.

## Previous qualified native source: concurrent grant records

The concurrent grant-record delivery gives several runtime records one shared
`RdmaQp`. SQ completions select the record's namespace; RQ completions identify
the physical receive slot and then the exact binding/key grant. Native retirement
fences all affiliated records, while successful grants release their own loan/MW.
Physical QP/queue retention is counted once, separately from every retained grant.

The new explicit `shared_qp` hardware profile uses four grants on one real RC QP,
checks concurrent payload WR namespaces, reverse SEND_WITH_INV receipts, immediate
grant release and stale-record isolation. Its lost-receive control drops an actual
successful CQE and checks all four loans and RQ credits at both native CQ teardown
boundaries. It uses the production owners and native provider, with a fixture
rendezvous; it does not claim concurrent public v12 handshakes.

Source `afa68ec` passes 23 local gates on stable and Rust 1.85, including
208 all-feature unit tests per compiler, twelve hardware-runner controls and
the frozen TENT geometry comparison. The two frozen binaries pass 212 two-host
CPU DRAM/IPv4 RoCEv2 configurations: 180 existing two-RNIC regressions and
32 new shared-QP cases across both RNICs, READ/WRITE, polling/event progress,
healthy completion and lost receive completion.

The new cases check four distinct payload WR namespaces on one actual QP,
reverse receipt order, completed-record replacement while a donated RQ slot
remains pending, and stale close isolation. Sixteen controls drop actual
successful receive CQEs; 128 loan checks at native CQ destruction boundaries
verify that all four grants remain protected until physical drain. These
checks establish ownership and completion routing, not performance or
simultaneous DMA execution. All 472 children are reaped: 448 completed libtests
and 24 deliberate kills. Both RNIC locks are available afterward, with
unchanged node/provider identities and no GPU compute used.

The actual frozen TENT CPU comparison passes 25 observations and six negative
controls. Its Rust/C++/TENT binary hashes and identical merge tree are checked.
The source CI matrix and strict documentation checks pass. Evidence, including
failed development runs, stays under
`/root/orbitte-artifacts/rdma-grant-records-20261026`.
`native-raw-audit.json` and `shared-native-audit.json` reconcile the raw native
results; `final-delivery.json` identifies the qualified code and documentation-only
delivery commit. See the [shared-QP reproduction contract](rdma-cuda.md#shared-qp-grant-record-profile).

These are same-agent scoped gates. Public v12 still admits one active grant per
QP. Public multiplexing, fixed-width arrays, periodic recovery, physical
port/device faults, GPU/performance and independent acceptance stay open;
this delivery does not complete P4.

## Previous qualified native source: QP receive owner

The QP receive-owner delivery moves receive buffers, message MRs and parent
admission credits into `ConnectionResources::receive_queue`. `ReceiveWork`
holds an independent full-binding/key receipt. Successful CQEs identify the
consumed slot, then dispatch the message to its exact receipt; releasing that
grant preserves other pending or prepared buffers. Native RQ depth is
provisioned from `max_send_wr`, with the existing separate RQ CQ quota.

CPU contracts cover reversed grant-message order, early grant release with a
donated slot still pending, prepared-slot preservation, duplicate/stale/foreign
messages, capacity, rejected/uncertain posts and drain. Existing source CQ,
MR cleanup and connected-reuse fixtures exercise the new owner on the actual
transport path. Native failure assertions inspect the QP's RQ debt and credits.
Source `6b34ce7` passes all 23 local gates on stable and Rust 1.85, including
207 all-feature unit tests per compiler, frozen TENT geometry and eleven
hardware-runner controls. Both binaries pass the existing 90-case two-host,
two-RNIC CPU DRAM RoCEv2 profile: 180 configurations across polling/event
progress and 1/2/3/6 lanes. All 408 children are reaped, including 24 deliberate
kills. Native RQ assertions reconcile queue-owned debt and credits before CQ
destruction; healthy reuse requires no retained message MR or RQ debt.
Both RNIC locks are available after the run, with unchanged node/provider
identities and no GPU compute used.

The actual frozen TENT CPU comparison passes 25 observations and six negative
controls. Its Rust/C++/TENT binary hashes and identical merge tree are checked.
This delivery's gate manifests, raw native audit and CPU evidence are recorded
under `/root/orbitte-artifacts/rdma-receive-dispatch-20261025`;
`final-delivery.json` identifies the qualified source and documentation-only
delivery commit. The source CI matrix and strict documentation checks pass.

For source `6b34ce7`, public v12 has one active grant per QP and multiple-receipt
dispatch is qualified only as a CPU contract. Later delivery evidence above
expands the internal native scope without changing that historical result. Full P4,
physical port/device faults, GPUDirect, performance and independent acceptance
remain open. The previous qualified source below remains historical evidence.

## Previous qualified native source: QP send owner

The QP send-owner delivery moves physical send depth and unresolved WR IDs to
`ConnectionResources::send_queue`. Each grant's `SendWork` borrows this owner
for payload, MW bind and completion SEND. Duplicate/foreign completions cannot
release another work's credit; post failures fence admission, ambiguous posts
retain possible debt, and cancellation needs QP/CQ destruction before release.

Source `e7adce9` passes 23 local gates on stable and Rust 1.85, including
206 all-feature unit tests per compiler, eleven hardware-runner controls and
the frozen TENT geometry comparison. New CPU contracts exercise simultaneous
grants sharing SQ depth/CQ capacity, sibling completion isolation, ambiguous
posts and cancellation without early credit return. The public connected-reuse
fixture verifies that successive grants retain the same physical SQ owner.

Both frozen binaries pass the existing 90-case two-host/two-RNIC profile:
180 configurations in total, with polling/event progress, 1/2/3/6 lanes,
READ/WRITE, multirail admission, QP/CQ errors, lost completions, MR/MW syscall
failures, CQ/PD cleanup retention, live reconnect and process generations.
The native fault fixtures compare QP-owned debt with actual accepted-minus-
delivered WRs before CQ destruction. All 408 supervised children are reaped
(384 completed libtests and 24 deliberate kills); both NIC locks are available
after the run, with unchanged node/provider identities. No GPU compute is used.

The artifact root is `/root/orbitte-artifacts/rdma-qp-send-20261025`.
`native-regression-manifest.json`, `native-raw-audit.json` and the gate manifests
pin the tested source, binary hashes and per-case evidence. GitHub's actual
frozen TENT CPU run also passes 25 observations and six negative controls;
`native-cpu-reference.json` verifies the merge-tree identity and Rust/C++/TENT
library hashes. Strict documentation and the source CI matrix pass.

These are same-agent scoped gates. Shared-QP admission is exercised by CPU
contracts; the native wire profile still has one active grant per QP. The
[send-owner contract](architecture.md#qp-send-ownership) and
[P4 exit queue](completion-plan.md#p4-remaining-delivery) retain concurrent
grant/RQ ownership, fixed-width peer construction, periodic recovery and full
physical-fault/installed/independent P4 acceptance as open work. This delivery
makes no new physical fabric, GPUDirect or performance claim.

PR #44's earlier layout cleanup is merged. Its source audit and CPU validation
remain at `/root/orbitte-artifacts/rdma-layout-20261025`.

## Previous qualified native source: peer arrays and monitor

P4.12m (`5ea3a35`) adds context-owned peer QP arrays, verified worker NUMA
CPU binding and one transport monitor for async/completion events. Physical QP,
CQ-lane progress and exclusive `RdmaGrant` resources have separate owners.
Peer arrays drive bilateral connected reuse while preserving each QP's lane.

Nineteen stable/Rust 1.85 gates pass, with 203 all-feature units per toolchain
and eleven hardware-runner controls. Both frozen binaries pass 180 two-host/
two-RNIC CPU DRAM/IPv4 RoCEv2 configurations with 1/2/3/6 lanes, polling/event
progress, eight-parent admission, quota/unequal-tail pressure, QP/CQ faults,
lost completions, MR/MW command failures, CQ/PD cleanup retention, live recovery
and process generations. All 408 native children are reaped: 384 completed
libtests and 24 deliberate kills. This is the same declared hardware scope as
P4.12l, executed again with the new owners.

The raw audit reconciles 1,004 actual data-thread affinity observations with
the nodes' allowed CPU/NUMA sets and 4,136 native CQ/thread observations. It
checks 704 peer-QP-array observations and 870 native monitor-source observations;
52 healthy/live event profiles establish a single actual monitor thread.
Native guards reject skipped worker binding and skipped peer-array adoption.
A separate CPU control rejects a lost native async ACK. The controls use the
exact qualified source, preserve their patches/binaries and reap every child.

Evidence root: `/root/orbitte-artifacts/rdma-endpoint-monitor-20261025`.
`native-raw-audit.json`, `qualified-controls.json` and `finishing-gates.json`
retain artifact hashes, case manifests, actual library mappings and supervision
receipts. An initial CQ-cleanup/restart run exposed repeated monitor-close
EPIPE; `5ea3a35` fixes joined-close idempotence and preserves join failure.
The failed run is archived and excluded from all passed totals. Both RNIC
leases are available after the final run, with unchanged hardware/provider
identities and no GPU compute processes used.

These are same-agent scoped gates. Arrays still grow on demand under the
aggregate physical-QP limit; TENT's fixed-width concurrent QP multiplexing,
periodic port recovery/bandwidth refresh, physical fabric/device loss, broader
GPU/fabric/installed profiles, independent acceptance and complete P4 remain
open. No performance claim is made. See
[the owner contract](rdma-cuda.md#peer-qp-arrays-worker-affinity-and-unified-monitor).

## Previous qualified source: CQ lanes

P4.12l (`2952f06`) adds per-context CQ lanes and load-based slice admission
using TENT's unified `num_lanes`. PD/MR/key owners and aggregate limits stay
shared; cached QPs keep their original lane. The requester can slice a large
single-RNIC request across lanes, while each peer keeps local lane indices.

Nineteen stable/Rust 1.85 gates pass, with 198 all-feature units per toolchain
and eleven hardware-runner controls. Both frozen binaries pass 180 two-host/
two-RNIC CPU DRAM/IPv4 RoCEv2 configurations with 1/2/3/6 lanes. All 408 native
children are reaped: 384 completed libtests and 24 deliberate source/requester
kills. The matrix covers actual CQ/thread routing, load held through grant
ownership, admission/quota and unequal tails, lost CQEs, QP faults, MR/MW
command failures, CQ/PD EBUSY retention, live recovery and repeated generations.
Two native wrong-worker controls and one CPU reversed-load control reject
their intended errors. The control production source and target guard files
match the qualified implementation.

Evidence root: `/root/orbitte-artifacts/rdma-cq-lanes-20261025`.
`native-raw-audit.json` reconciles raw cases, exact binaries, provider mappings,
locks, native process exits, CQ lane/thread identities and admitted/retained
worker loads. Earlier failed fixture probes remain alongside the final matrix;
they are excluded from the passed totals. The retained native handles, MR/DMA
loans and no-retry assertions remain explicit. See
[the lane contract](rdma-cuda.md#multiple-cq-lanes-and-load-based-slice-dispatch).

These are same-agent scoped gates. Physical fabric/device loss, fixed-width QP
multiplexing, full monitor parity, broader GPU/fabric/installed profiles,
independent acceptance and complete P4 remain open. No performance claim is made.

## Previous qualified source

P4.12k (`914761b`) adds a configurable transport-level `Workers` pool.
Multiple context CQ pairs share data threads; each CQ retains one polling
owner. Drained context recovery preserves the pool, PDs and persistent MRs.
Endpoint handles retain the pool after the transport handle is dropped.
Configuration, resource observations, pool scheduling, endpoint/grant protocol
and native progress passes now have separate modules.

Nineteen stable/Rust 1.85 gates pass, with 195 all-feature units per toolchain
and eleven hardware-runner controls. Both frozen binaries pass 168 two-host/
two-RNIC CPU DRAM/IPv4 RoCEv2 configurations. The declared matrix covers
1/2/3 workers, polling/event progress, masked and cross-rail READ/WRITE,
eight-parent/MiB/folded/quarter-tail admission, actual QP/CQ errors, dropped
successful SQ/RQ CQEs, MR/MW command faults, CQ/PD cleanup failures and two/
three process generations. All 384 native children are reaped: 360 completed
libtests and 24 deliberately killed children. Original NIC leases remain held
through each generation and are available after final cleanup.

Actual native CQ observations bind every recorded poll to its NIC, context,
worker name and thread ID. Live fault cycles preserve the same threads, PDs
and MRs. Current grant accounting is independent of historical idle QPs;
bilateral checkpoints prove a rejected parent creates no new connection.
Lost-CQE debt equals actual accepted-minus-delivered work through both CQ
barriers. Two native controls reject assigning a CQ to the wrong worker, and
one CPU control rejects stopping the pool while endpoint owners remain.

These are same-agent scoped gates. TENT's multiple CQ lanes per context,
least-loaded slice enqueue, worker affinity and monitor consolidation remain
open. Priority and bandwidth policies remain in P6. Physical link/device loss,
spontaneous cleanup failures, GPU direct/installed profiles, independent
acceptance and the full P4 exit remain open. See the
[pool contract and evidence](rdma-cuda.md#transport-workers-and-shared-context-progress).

## Previous MR and MW cleanup and live recovery evidence

P4.12j (`586df69`) adds drained shared-progress replacement without replaying
failed transfers. Later requests rebuild QPs in the original PD/MR owners;
native MR/MW cleanup failures keep the affected old engine quarantined.

Nineteen stable/Rust 1.85 gates pass, with 192 all-feature units per toolchain
and eleven runner controls. Both frozen binaries pass 248 two-host/two-RNIC
CPU DRAM/IPv4 RoCEv2 configurations: 128 new lifecycle cases and 120 targeted
regressions. The new cases prove 64 native QP ERR transitions, 64 MR and 32 MW
EIO errors from thread-local syscall injection, observer-free backing/loan
retention, live-engine replacement with new epochs/PDs and old open/data
rejection before QPs or effects. All 536 children are reaped: 516 completed
libtests and twenty deliberate kills in the previous CQ/PD/restart profiles.
Retained old engines have no zero-counter claim.

Eight native mutation controls reject early MR/MW release and missing engine
recovery. The real frozen TENT CPU driver/shared-library comparison passes
25 observations and six controls. See [the contract and external evidence](rdma-cuda.md#mr-and-mw-cleanup-faults-and-live-process-recovery).

That frozen source used one data worker per context. P4.12k above replaces
it with the transport-level pool. Spontaneous provider failures,
physical device recovery, in-place healing of retained owners, wider installed/
GPU profiles, independent acceptance and the complete P4 exit remain open.

## Previous native cleanup and repeated process evidence

P4.12i extends native failure acceptance without changing production source,
dependencies or wire v12 from PR #38. Frozen operational source `02ef99d`
passes nineteen stable/Rust 1.85 gates, 191 all-feature unit tests per toolchain
and eleven runner controls. Both frozen binaries pass 280 two-host/two-RNIC CPU
DRAM/IPv4 RoCEv2 cases: 176 new configurations and 104 targeted regressions.

Actual dependent verbs objects produce 64 CQ and 64 PD EBUSY errors on either
peer and rail. CQ failures retain WR/RQ debt, MR/DMA/grant owners and admission
after repeated close/Drop, with observer references removed. PD-only failures
retain PD/context and release already drained application backing. Native
destruction is not retried after failure.

Source/requester death, two/three fresh process generations, both RPC rebindings
and repeated numeric registration IDs produce 244 all-prior-source-epoch
rejections before QPs or remote effects. All 984 native children are reaped:
772 completed libtests and 212 intentionally killed children. Completed
processes balance 1,544 context opens/closes and
44,608 MR registrations/deregistrations. Killed
processes have kernel waitpid/lease receipts and no post-death zero-counter or
sentinel claim. Eight native controls reject early WR/RQ return and post-Drop
CQ/PD retries; one CPU control rejects early restart-lease release.

The [cleanup/restart contract and evidence](rdma-cuda.md#native-cleanup-failure-and-repeated-process-generations)
records exact scope, hashes and reproduction. These are same-agent scoped
gates. Physical port/device loss, native QP/MR/MW and completion-channel/device-close cleanup
errors, poisoned-live-engine recovery, installed consumers, GPU/GDR,
independent acceptance and the full P4 exit remain open.

## Previous endpoint-fault and source-restart evidence

P4.12h fixes endpoint cleanup inheriting an exhausted data deadline. One shared
thirty-second destruction window now bounds a parent's cleanup independently
of data timeout. The transfer remains failed, while actual native teardown
permits backing/admission return. Unproven cleanup remains quarantined.

Frozen operational source `ec852d4` passes 19 sequential stable/Rust 1.85
gates, 191 all-feature units per toolchain, four hardware tests ignored by default
and eleven runner controls. Both binaries pass 284 two-host/two-RNIC CPU DRAM/
IPv4 RoCEv2 cases: 208 new configurations and 76 targeted regressions. The full
older matrices were not rerun; their historical evidence remains separate.

Receipts prove 64 actual QP ERR transitions, 128 deliberately dropped native
successful SQ/RQ CQEs and sixteen source SIGKILL/restarts. All 600 native
processes are reaped: 584 completed libtests and sixteen intentionally killed
sources. The completed processes balance 1,168 context opens/closes and
18,077 MR registrations/deregistrations. The killed sources
have explicit waitpid/lease receipts rather than in-process zero-counter claims.

Fresh processes rebind the same public RPC address and numeric registration ID;
old source descriptors and RDMA operations fail with EpochMismatch before new
QPs or remote effects. New generations verify actual payloads and forty-QP reuse.
Four native mutation runs reject early payload-debt/RQ-credit return after QP
destruction but before CQ drain. One CPU mutation rejects releasing both leases
between the killed process and its replacement.

The [destruction/restart evidence](rdma-cuda.md#native-endpoint-faults-lost-completions-and-restart)
records hashes, failed exploratory runs, commands and exact CI checks. These are
same-agent scoped libtest gates. Physical port/device-down remains unqualified
because both selected containers lack CAP_NET_ADMIN. Poisoned-live-engine
recovery, broader process-death profiles, installed consumers, GPU/GDR,
independent acceptance and the complete P4 exit remain open.

## Previous large-geometry evidence

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
