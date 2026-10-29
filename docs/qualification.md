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

The latest installed delivery covers the [bounded remote pipeline](#bounded-remote-cuda-pipeline),
[serial remote CUDA staging](#serial-remote-cuda-staging),
[local staging chunks](#local-cuda-staging-chunks) and the
[bounded cross-submission cache](#bounded-cross-submission-cuda-ipc-cache),
building on [local pinned host IPC](#local-pinned-host-cuda-ipc), bounded [subbatches](#bounded-cuda-ipc-subbatches) and the cooperative
[GPU IPC](#cooperative-cuda-ipc-and-nvlink) profile
below. The bounded [GPUDirect RDMA](#gpudirect-rdma) profile retains its
explicit host-wide context inventory limit. Earlier host
[automatic discovery](#installed-automatic-discovery) and
[physical rail accounting](#installed-physical-rail-accounting) retain their own
artifacts and acceptance boundaries.
The production host RDMA implementation is complete; physical/fabric
qualification and complete acceptance remain in the existing P4 queue.

## Shared staging worker shards

The current v19 source replaces per-session thread creation with a lazily started
pool of at most eight shared shards. An idle shard is reused; queued tasks have
bounded admission and retain their original deadline. Pin starts after dequeue.
Queued cancellation destroys the actual task captures before its terminal receipt;
active work continues through the existing transport drain boundary. Independent
terminal delivery and caller unwind ordering prevent early private MR retirement.
Task panic retains uncertain owners while leaving the shard available.

CPU controls cover eight active sessions plus 512 queued tasks, queue exhaustion,
pre-start timeout/cancellation, shutdown timeout and retry, concurrent lazy
admission/close, reuse after panic, idle-thread Drop and caller unwind with a full
completion queue. A public-path control holds eight source CUDA completions and
expires a ninth queued request without remote Pin, then verifies exact bytes,
guards and thread reuse over TCP and RDMA. These are CPU ownership controls.

Evidence is outside Git at
`/root/orbitte-artifacts/staging-worker-shards-20261029/`.
Stable and Rust 1.85 combined runs each pass 309 unit tests, eight integration
tests and one doctest. CUDA-only tests/Clippy, combined Clippy, all 50 controller
controls, the CPU trace fixture and strict documentation checks pass. `gates.json`
records these commands and results. The original single-QP/eight-byte fixture
rejected the concurrency control with `ResourceExhausted`; that failed control
is preserved, and the accepted control uses the existing default RDMA budgets.
Independent source review remains pending. No new hardware task is requested.
Installed v19 execution, physical overlap and performance remain unqualified.
Nonblocking chunk progress, concurrent CROSS on the wire and location selection
remain open; shared threads do not establish complete ProxyManager parity.

## Bounded session progress

The accepted v19 source at `4441017` adds bounded CROSS command/completion queues and one
joined connection worker per admitted staging request. Local CUDA progress can
continue while a CROSS or Delegate receipt is pending. CROSS remains ordered on
one connection. The shared-shard substage above supersedes its per-session worker;
multiple CROSS operations on the wire remain open. See [the ownership contract](staging.md#session-progress-and-completion).

CPU controls hold a local CUDA event while CROSS progresses, hold a source
Delegate receipt while local PRE/POST starts, and lose the final Delegate reply
after every CROSS completed. A controlled local submit failure cancels queued
CROSS commands while the active command drains; combined RDMA teardown failure
must override the ordinary local error and retain the actual pin/context owner.
A pure Rust worker panic during first READ or first/second WRITE grant cleanup
retains the actual GPU backing, all private pins and parent admission, with
conservative WRITE uncertainty and exact already-written bytes. Unequal rings,
tails, self-peer admission and final POST drain controls remain.

Evidence is stored outside Git at
`/root/orbitte-artifacts/async-cross-20261029/`.
Independent review accepts the production source contract at
[`4441017`](https://github.com/OrbitInfra/OrbitTE/commit/444101700732e33098329962d689aff82fac2b3c).
Stable and Rust 1.85 combined runs with the integrated panic regression each pass
303 unit tests, eight integration tests and one doctest; CUDA-only tests/Clippy,
combined Clippy, all 50 hardware controller controls, the CPU trace fixture and
strict documentation checks pass. `gates.json` and `integrated-gates.json`
preserve commands and outcomes, including the corrected CUDA-only Clippy
diagnostic. `independent-review/source-review-4441017.json` binds 16 independent
source gates; `worker-panic-control.json` in that directory binds the additional
reviewer control now included in the permanent tests. This accepts the source
substage; its remaining scheduling work is recorded in the shared-shard boundary above.
No new Forge task or hardware execution is requested. Current installed
qualification and performance remain unqualified; earlier H20 results retain
their original artifact boundaries.

## Requester CUDA pipeline

The current wire-v19 implementation adds an independent bounded requester ring
to the accepted source pipeline. CUDA WRITE prequeues local PREs; READ can keep
POSTs pending until local slot reuse. Every final local POST drains before
Unpin. The request-local `StageBufferCache` retains private registrations, and
`PinnedStage` owns each accepted copy and one-time completion. Width and budget
formulas are in [the staging contract](staging.md#bounded-remote-pipeline).

Source controls cover unequal local/remote widths, multiple ring turns and tails,
pending copies during CROSS, opposite-direction sessions, shared-pool exhaustion,
first/middle/final native wait failures, deadline expiry during final POST and
lost Unpin. Refill submit/record failures drain accepted siblings while preserving
partial READ bytes and WRITE uncertainty. Context restoration and private MR
retirement failures retain actual owners and admissions after ordinary references
drop; MR controls cover both sides, directions and depths one and three.

Independent review accepts the source contract at
[`c87c403`](https://github.com/OrbitInfra/OrbitTE/commit/c87c403fd021872c3bd68fde55078bd6bdead9bb),
whose production and installed-consumer inputs are unchanged from `027efad`.
Stable and Rust 1.85 combined runs each pass 299 unit tests, eight integration
tests and one doctest; all-target Clippy, 50 hardware-controller controls and
strict documentation checks pass. The reviewer independently repeats the
applicable feature gates and the integrated failure controls. The existing IPC
timeout control now waits for actual server closure instead of a fixed sleep.

Evidence stays outside Git at
`/root/orbitte-artifacts/requester-pipeline-20261029/`.
`independent-review/source-review-c87c403.json` binds the frozen source, original
and incremental review gates, and additional failure controls. This accepts the
source substage; it does not close P5.5b.

Hardware execution is deferred at the user's direction; no new task is requested.
The controller and consumer now account for each ring's own width, private MRs,
persistent stream/events and physical pinned bytes. Neither these CPU controls
nor earlier v18 hardware runs qualify the current artifact or demonstrate
physical overlap or throughput. The session progress and shared-shard substages
above extend this source boundary. Nonblocking chunk progress, concurrent CROSS
on the wire and location selection remain open.

## Bounded requester CUDA staging

The prior v19 substage replaces the full-request requester payload with
one reusable private slot. Pin negotiates the minimum chunk size; actual pinned
budgets remain per-engine physical sizes. CUDA range loans permit disjoint
sessions and reject overlap; completed READ chunks remain visible on later
failure. Source controls cover asymmetric chunks/tails, simultaneous opposite
directions, shared-pool exhaustion, IPC/RDMA interlocks and sticky native failure.

Independent review accepts the source contract at
[`460d825`](https://github.com/OrbitInfra/OrbitTE/commit/460d825d6438fc70883e7acd61a83d099406b3e7),
whose production and installed-consumer inputs are unchanged from `fc183ab`.
Stable and Rust 1.85 combined runs each pass 294 unit tests, eight integration
tests and one doctest. Targeted Clippy, wire-v19 relay, all 50 hardware-controller
controls and strict documentation checks pass. The reviewer independently
repeats the applicable gates and verifies both clean installed packages.
This accepts source and package provenance, not installed execution.

Evidence remains outside Git at
`/root/orbitte-artifacts/proxy-requester-20261029/`.
`independent-review/source-review-460d825.json` binds the source gates, review and
installed manifests. `frozen-cases.json` defines 12 healthy cases per compiler:
TCP/RDMA, CPU/GPU requesters, both host directions, asymmetric physical chunk
sizes, short tails and source widths up to 16.

Installed v19 qualification remains pending. Earlier v18 pipeline evidence below
remains bound to its original artifact. The new healthy profile must verify exact
requester slot/MR reuse, matching copy/event generations, physical budgets, both
directions, byte guards and cleanup on fresh nodes. The requester ring above
supersedes this one-slot implementation; neither substage has installed v19
hardware qualification. Broader location scheduling and performance remain open.

## Bounded remote CUDA pipeline

Clean stable 1.99.1 and Rust 1.85.0 installed consumers from
[`c0f6a9e`](https://github.com/OrbitInfra/OrbitTE/commit/c0f6a9e3517f7b6999b1238e652fb7db77b96d43)
pass 24 cases on two fresh physical H20 hosts. Each compiler covers TCP/RDMA,
CPU/GPU requesters, both host directions, asymmetric caps, negotiated widths
1/2/3/4/16 and a cap shortened by the request's chunk count. Main pipeline
cases turn the ring more than three times and include a short tail. Each case
performs three READs and three WRITEs with full-buffer and guard comparisons,
one source handler and one parent admission slot. Reverse RDMA cases use CQ events.
Four fresh independent repeats cover both compilers, transports, host directions
and local memory kinds, including the 16-slot RDMA case. They add 24 sessions,
696 Delegate/CROSS operations each and 1,296 payload copies on 38 pool
allocations/frees. Independent review accepts source and this healthy pipeline
profile; it does not complete P5.5b.

The passive CUDA ledger matches each stream/event record generation to live
allocations and requires event wait plus context restoration before slot reuse
or cleanup entry. It checks setup/readback as well as payload copies, allows
out-of-order completion across slots, and verifies persistent per-slot handles
across sessions. The primary matrix records 144 sessions, 2,040 Delegate/CROSS
operations each and 3,432 payload chunk copies on 120 pool allocations/frees.
Afterward, all supervised processes are gone, `nvidia-smi` reports 0 MiB memory
used on all 16 GPUs and no compute processes, locks are reacquirable, and the
selected mlx5_3 context/PD/CQ/QP/MR identity sets match their original baselines.

The unchanged 16-slot RDMA case performs 300 CROSS operations. It exposed
native MW index reuse exhausting the low-byte key tags. The PD now retains an
exhausted, never-bound MW while allocating another index; key history remains
intact and bounded. Both compiler runs show 300 binds/invalidations, one index
rollover and zero final live/retained windows. This is OrbitTE's authorization
lifetime mechanism; it is not a claim that TENT uses the same key policy.
Accepted control sockets also disable Nagle. CROSS setup errors now use the
nested transport epoch; a socket close may still precede delivery of the error.

Evidence remains outside Git at
`/root/orbitte-artifacts/proxy-pipeline-qualification-20261028/`.
`primary-matrix.json` binds the two `installed-qualified-*` manifests, native
observers, `runs-qualified-*` cases, runtime provider hashes and cleanup audit.
`independent-review/source-review-c0f6a9e.json` records source review and additional
partial-allocation/cleanup and partial-WRITE controls.
`independent-review/live-c0f6a9e/hardware-review.json` binds all 24 primary and four
fresh review cases to their installed artifacts. Its sibling `runs-stable/`,
`runs-msrv/` and before/after inventories preserve independent execution and
cleanup evidence. Final postflight covers primary, review and earlier failed
runs; `safe-to-release.json` records that the reserved resources can be returned.
The earlier timeout,
key-exhaustion and diagnostic failures remain in `runs-stable/`,
`nodelay-control/` and `diagnostic-control/`; their cleanup is audited too.
The CI socket-close race and repeated corrected controls are preserved in
`ci-c0f6a9e-stable-combined.log` and `cross-error-test-race.log`.
Original pipeline source controls remain in
`/root/orbitte-artifacts/proxy-pipeline-20261028/`.

This qualifies healthy wire-v18 remote staging on one RoCEv2 rail per host.
RDMA retirement uses real-provider engine counters, without passive per-WR/CQE
or deregistration-order receipts. Native CUDA receipts establish lifetime and
completion, not physical execution overlap or throughput. Physical faults,
InfiniBand, requester-side bounded staging, broader asynchronous scheduling and
C++/Rust wire interoperability remain open. Earlier serial v17 evidence retains
its original artifact boundary. See [the contract and reproduction](staging.md#installed-pipeline-validation).

## Serial remote CUDA staging

The opt-in serial P5.5b session is implemented with CPU controls for public
TCP/RDMA execution, byte guards, one handler/batch/chunk, source permissions,
scoped phase ordering, CUDA/MR failure retention, self-peer/opposite-direction
operations and loss of the final Unpin reply. Independent review found that
nonce-only replies could accept a repeated prior receipt as final completion;
success replies now echo the full operation and range. The retained failing
control and current source-gate evidence live outside Git in
`/root/orbitte-artifacts/proxy-remote-20261028/`. Independent review accepts source
correctness at [`64c9899`](https://github.com/OrbitInfra/OrbitTE/commit/64c9899fa088898b01000fe87ccb924bed356c32),
including a fresh real TCP relay control that repeats the previous phase receipt
while withholding Unpin. Both directions reject early completion. The review
record is `independent-review/final-source-review.json` in that artifact root.
Production code is unchanged in the clean installed consumers from
[`9f2ad39`](https://github.com/OrbitInfra/OrbitTE/commit/9f2ad3967ee9d12f16c13a87050c41a71268e567).
The consumer now waits for its first submission to drain before reusing the
single batch admission slot. An earlier run correctly returned
`ResourceExhausted` when the fixture overlapped two submissions; that failed
attempt is retained. The CUDA validator now binds all asynchronous copies,
including setup/readback, to the live GPU and pinned host allocations, expected
slice offsets, event wait and context restoration before reuse or free.

Stable 1.99.1 and Rust 1.85.0 consumers pass 16 installed cases on two physical
H20 hosts. Each compiler covers remote GPU/local CPU and GPU/GPU, TCP and staged
RDMA, and READ/WRITE in both host orientations. The first orientation uses
257-byte chunks with 1,001-byte requests; the reverse uses 65,536-byte chunks
with 524,304-byte requests. Both have short tails. RDMA uses one RoCEv2 rail,
with polling in the first orientation and CQ events in the reverse. Each peer
has one handler and one parent batch slot. Each GPU peer uses one pinned chunk;
a CPU requester uses none.

The source counters record 96 completed Pin/Unpin sessions and 624 Delegate/CROSS
operations each. Native CUDA receipts bind 936 payload chunk copies to 24 actual
pool allocations and successful frees. Full buffers and guards match. The
runtime maps and provider hashes bind compatibility `libcuda.so.590.48.01` on
R535.161.08, `libibverbs` and `libmlx5`. After all runs, test/supervisor PIDs are
gone, `nvidia-smi` reports 0 MiB and no compute processes for all 16 GPUs, and the original
GPU/RNIC locks are reacquirable. The 48 Python controls include wrong GPU offsets
and cross-thread frees before setup/readback completion.

Four fresh independent repeats reverse the primary size/orientation pairing,
covering both compilers, transports and local memory kinds. They add 24 sessions,
156 Delegate/CROSS operations each and 234 chunk copies on six actual pool
allocations/frees. Independent postflight confirms process exit, zero GPU use,
reacquired locks and the original selected-device context/PD/CQ/QP/MR identity
sets. Background QP packet counters may naturally advance. Independent review
accepts source and this bounded healthy serial profile.

Evidence is outside Git at
`/root/orbitte-artifacts/proxy-remote-qualification-20261028/`.
`primary-matrix.json` binds the two clean packages, binaries and observers to the
profile, all case records, loaded libraries and post-run cleanup. `runs-stable/`
retains the failed admission fixture; `runs-final-stable/` and `runs-final-msrv/`
contain the successful final runs. Source review remains in the previous artifact
root above; validator review is under `independent-review/frozen-9f2ad39/`.
`independent-review/hardware-evidence-review.json` independently binds all 20
raw case records to the exact installed artifacts and runtime providers; fresh
runs are under `independent-review/live-repeat-stable-1/` and
`independent-review/live-repeat-msrv-1/`; profiles, commands and before/after
inventories are under `independent-review/live-repeat-plan/`.

This qualifies the declared healthy serial profile. RDMA completion and private
MR retirement are checked through the real provider's engine counters and source
ownership checks, without passive per-WR/CQE or deregistration-order receipts.
The new pipeline has [separate v18 evidence](#bounded-remote-cuda-pipeline).
Broader remote scheduling, physical GPU/RNIC faults, performance and mixed
C++/Rust wire interoperability remain open. See
[the contract and reproduction](staging.md#serial-remote-staging).

## Local CUDA staging chunks

Production source [`37e99ae`](https://github.com/OrbitInfra/OrbitTE/commit/37e99ae7bb5f2415a0a4fdd3f77bc25c9ffea477)
introduces one engine-owned `ProxyManager` pool shared by TCP and staged RDMA.
Clean installed consumers from
[`e44a6d1`](https://github.com/OrbitInfra/OrbitTE/commit/e44a6d117a18f2770984f0da41bb2419dd6782cf)
include corrected fixture accounting and native-ledger lifetime validation;
production code is unchanged between those commits.

Stable 1.99.1 and Rust 1.85.0 consumers pass 24 cases across two physical H20
hosts. Each compiler covers TCP and staged RDMA, GPU/host, host/GPU and GPU/GPU,
with both source/requester orientations. READ and WRITE each contain three
logical requests. The smaller profile uses 257-byte chunks and 1,001-byte
requests; the reverse profile uses 65,536-byte chunks and 524,304-byte requests.
Both end in a short chunk. RDMA uses one physical RoCEv2 RNIC, polling on the
first orientation and CQ events on the reverse orientation. The runtime maps
compatibility `libcuda.so.590.48.01` on R535.161.08.

The primary native ledgers bind 32 persistent chunk allocations to 160 reuses,
1,248 chunk copies and 32 successful frees. Every copy is bounded by its actual
pin/free lifetime and matched stream, event wait and caller-context restoration.
CPU peers consume no CUDA chunks. Both peers compare full buffers and guards;
all supervised children are reaped and resource locks released. These are
Driver API correctness results, without a performance claim.

Independent source gates pass the stable four-feature matrix and MSRV combined
Clippy/tests: combined features have 273 unit tests, eight integration tests
and one doctest, with six explicit hardware/subprocess skips. Source controls
cover held events, capacity, cancellation, partial deadline effects, native
context replacement and failed pin/copy/free cleanup. The public source and
requester tests also reproduce and reject early admission release when CUDA
`Unquiesced` coincides with an ordinary RDMA CQ cleanup error. The 47 Python
controller controls include wrong stream/event, copy-after-free and double-free
negative controls.

Four fresh independent repeats pass across both hosts and compilers, adding
six GPU peers, 264 chunk copies and six native chunk frees. The independent
ledger audit also binds every payload copy to the actual GPU allocation and
expected slice offsets. Post-run checks find all test and supervisor PIDs gone,
all 16 GPUs at zero allocation with no compute processes, and GPU/RNIC locks
reacquirable. Independent review accepts source and this bounded healthy profile.

Evidence remains outside Git at
`/root/orbitte-artifacts/proxy-staging-20261028/`. `primary-matrix.json` binds
the two clean package, binary and observer hashes to all 24 case records and
the exact profile. `installed-final-stable/`, `installed-final-msrv/` and
`independent-review/` retain packages, raw ledgers, reviews and controls.
`independent-review/frozen-e44a6d1/` contains the fresh repeats, independent
`raw-ledger-audit.json`, package bindings and both post-run inventories.
The first stable run passed its six forward cases, then rejected an invalid
reverse-TCP event-mode profile before launching the next case. Its failed
manifest is preserved; the six corrected reverse cases are separate results.
The earlier fixture-count failure and missing-toolchain-component build failure
also remain preserved, without being counted as passes.

This profile uses one chunk per engine and one GPU per host. Default-capacity
stress, physical CUDA failures and broader context scheduling remain
unqualified. Cross-transport reuse, capacity exhaustion and failed native
cleanup are covered by CPU controls. Remote Delegate/Pin/Unpin, nested
admission and PRE/CROSS/POST pipelines remain P5.5b; this local subset does not
complete P5 or full TENT ProxyManager parity. See [local staging](staging.md).

## Bounded cross-submission CUDA IPC cache

Source [`7da43b2`](https://github.com/OrbitInfra/OrbitTE/commit/7da43b21db8fbee69018830228deaab3f5f13206)
adds opt-in transport-owned imports across submissions. Concurrent first users
share one bounded entry; each reuse rechecks source authority. A separate export
owner holds the actual registration, whole allocation and physical charge until
native import close and its source receipt. Completed local requester owners are
released after each operation's event drain and context restoration.

Clean packaged consumers on stable 1.99.1 and Rust 1.85 exercise the same local
GPU, owned pinned host and mixed groups on two physical H20 hosts with NV18
connections. The 34-case matrix includes 24 cached healthy cases (17 and
2,097,171 bytes, both device directions), four source-close waits, four missing
import-close cases after requester exit, and two default subbatch regressions.
Healthy cache cases submit four concurrent three-slice READs, then a three-slice
WRITE. Fifteen payload copies must use exactly one native import/export and one
final close. Healthy/graceful cases compare logical bytes, gaps and guards.
The runtime uses compatibility `libcuda.so.590.48.01` on R535.161.08; this is
Driver API correctness evidence, not a toolkit/kernel or bandwidth result.

All 34 cases pass: 36 exports/imports, 396 payload copies (228 HtoD/DtoH),
32 native closes and 38 persistent pinned requester owners. The four missing
closes are the declared requester-exit cases. Every child is reaped, GPU UUID
locks are released and selected GPU allocation baselines are restored.
Independent review passes stable four-feature and MSRV combined source gates,
then the stable/MSRV cache and complete RDMA relay tests after test-only fixes.
The final combined gate has 265 unit tests, eight integration tests and one
doctest; six hardware/subprocess fixtures remain ignored in the CPU gate.
All eight toolchain/feature CI jobs, the frozen TENT CPU comparison, 45 controller
controls and strict documentation/link checks pass. The strengthened validator
rejects the independent readback counterexample and accepts its legitimate
control; final raw ledgers are revalidated without changing installed code.

Four fresh independent repeats cover host healthy, mixed source-close, mixed
requester exit and device healthy across both hosts, compilers and directions.
They record four exports/imports, 36 payload copies (19 host), three native
closes and four persistent pinned owners. Independent post-run checks find all
primary/review child PIDs gone, all 16 GPUs at zero allocation with no compute
processes, and all selected UUID locks reacquirable. Independent review accepts
source and this bounded fixed-working-set profile.

Evidence stays outside Git at
`/root/orbitte-artifacts/cuda-ipc-cache-20261028/`. `final-evidence/` binds
the two clean `7da43b2` installed packages to validator `b4a9400`, the frozen
matrix, `native-audit.json`, host/boot/GPU identities, driver hashes and per-case
raw logs. `independent-review/hardware-b4a9400/` preserves the fresh repeats,
cleanup audit and review. Earlier
`85df496` cases and original review counterexamples remain preserved separately.
The test-only validator fix rejects a missing payload context restoration that
previously borrowed a later owned-device readback's pop; it also bounds event
and wait matching before the next native copy. Source controls cover source-side
capacity, concurrent cold opens, deadlines, unregister/permission/admission,
missing receipts, late first offers, one-shot cleanup failure and actual source
allocation/registration retention after engine drop.

This profile supports a fixed working set until requester shutdown. Both peers
must enable `nvlink_cache_entries`; default zero keeps per-subbatch close. Source
allocations remain exclusively loaned throughout the cached lifetime, and one
allocation supports one live export token/context. There is no eviction or
continuing flush. Requester exit is injected after event drain and before native
close: it proves exporter retention, without final source payload readback or
in-flight GPU termination. Live trusted exporters are required. External/managed allocations,
broader stream scheduling, MNNVL/VMM and native GPU cleanup faults retain their
existing limits. This substage does not establish complete P5 or TENT parity.

## Local pinned host CUDA IPC

Source [`9adf5a5`](https://github.com/OrbitInfra/OrbitTE/commit/9adf5a5eabb8bb5b8dce312ba10d77275da75ab1)
qualifies local owned pinned host READ/WRITE against remote owned GPU exports,
including mixed host/GPU requester groups on one native context. It follows
frozen TENT's local DRAM/GPU and remote CUDA-only distinction. Ordinary/external
host backing and remote host exports are outside this ownership profile.

Clean packaged consumers built with stable 1.99.1 and Rust 1.85 pass 30 cases
on two physical H20 hosts with NV18 topology, using compatibility
`libcuda.so.590.48.01` on R535.161.08. Sixteen host READ/WRITE cases cover both
GPU-context directions and 17 / 2,097,171-byte payloads. Four mixed healthy,
four host source-close waits and four mixed requester-exit cases use 4,133 bytes;
two GPU/GPU cases retain the earlier behavior. Healthy/graceful cases compare
full logical bytes, gaps and guards. Requester exit occurs after native event
drain, before import close; it proves retained source ownership, without final
payload readback or termination during GPU DMA.

The native ledger records 52 imports, 156 payload copies (132 HtoD/DtoH),
48 closes and 48 persistent pinned requester owners. Exact pointer ABIs,
physical allocation initialization, each slice's event drain, context restoration
before release and absence of temporary allocation inside IPC cycles are checked.
Every child is reaped, locks are released and GPU allocation baselines restored.
This is Driver API correctness evidence; it does not measure NVLink bandwidth.

Independent source review passes stable four-feature and MSRV combined
Clippy/test gates, including 255 unit tests, eight integration tests and one
doctest for combined features. Six hardware/subprocess fixtures remain ignored
in that CPU gate. Validator-only commit `3b59f66` rejects out-of-cycle host copies
and pinned frees before close-context restoration. The original rejected
controls, 43 Python controller tests and 22 independent classification controls
are preserved; the production code, installed consumer and observer stay at
`9adf5a5`. Four fresh independent repeats pass host/mixed healthy,
source-close and requester-exit cases across both hosts and compilers: 18 payload
copies, including 15 host APIs, with six imports and five closes. Independent
post-run checks find all child PIDs gone, all 16 GPUs at zero allocation with no
compute processes, and selected UUID locks reacquirable. Source and this bounded
installed profile are independently accepted.

Evidence stays outside Git at `/root/orbitte-artifacts/cuda-ipc-host-20261028/`:
`installed-stable/`, `installed-msrv/`, `qualification-inputs.json`,
`matrix-result.json`, `native-audit.json`, `runs/*/evidence/` and
`controller-frozen.py` bind package,
controller, process/native records and host/boot/GPU identities.
`independent-review/` preserves reviews, controls and separate repeat artifacts.

Trusted live exporters remain required. Cross-submission caching has a separate
profile above. Remote host exports, broader stream scheduling, MNNVL/VMM,
exporter crashes and native GPU cleanup faults remain open. See [the contract](cuda-ipc.md) and
[the execution queue](completion-plan.md); this profile does not complete P5.

## Bounded CUDA IPC subbatches

Source [`818b53e`](https://github.com/OrbitInfra/OrbitTE/commit/818b53ebac70b1e53257c5e82161b927f4c4bb63)
qualifies reuse of one import by consecutive requests in a single submission.
Two distinct owned requester allocations sharing one native CUDA context issue
three slices in input order. Each slice drains its own event; native import
close and the source receipt/ACK precede confirmed success. Cross-submission
caching is outside this profile.

Clean packaged consumers on stable 1.99.1 and Rust 1.85 pass 28 installed cases
on two physical H20 hosts, using each host's GPU 0/1 with NV18 topology:
16 three-slice READ/WRITE cases cover both directions and 17 / 2,097,171-byte
payloads; four source-close waits, four requester exits before import close,
and four original single-request READ/WRITE regressions use 4,133 bytes.
The runtime maps compatibility `libcuda.so.590.48.01` on R535.161.08. This is
Driver API correctness evidence, not a CUDA toolkit/kernel or bandwidth result.

The 2,744 native records contain 48 exports/imports, 128 copies and 44 closes.
Healthy and graceful-close cases verify touched bytes, gaps and guards.
Requester-exit cases prove native copy drain and ownership retention; they do
not read back final payload bytes while the source loan remains retained.
The ledger requires distinct live
local allocations before use, the exact relocated offsets, a separate completed
event for each slice and close after all copies drain. Four lost-close cases
retain source export ownership until process exit. Every child is reaped, GPU
UUID locks are released and selected GPU allocations return to zero. Requester
termination happens after copies drain; it does not qualify termination during
GPU DMA or exporter-crash safety.

Independent source review passes stable/MSRV × four-feature Clippy/test gates;
the combined build passes 251 unit tests, eight integration tests and one
doctest, with six hardware/subprocess fixtures explicitly ignored. The review
also checks packaged-source and dependency provenance. Test-only commits
`72373d5` and `e8f2d4d` extend overlap, held-ACK, nonzero submission-index and
cancel/cleanup controls without changing production code, the installed
consumer or native observer. The final validator passes 41 Python controls,
including independent late-allocation and aliased-owner counterexamples.
Four fresh independent repeats also pass: each host runs stable and MSRV
three-slice healthy/source-close cases, totaling 408 native records, six imports,
18 copies and six closes. Independent post-run checks find all original and new
child PIDs gone, all GPUs at zero allocations with no compute processes, and
selected locks reacquirable. Independent review accepts the source and this
bounded installed profile with the limits below.

Evidence is outside Git at
`/root/orbitte-artifacts/cuda-ipc-batch-20261028/`: `installed-stable/` and
`installed-msrv/` contain packaged-consumer manifests; `matrix.json`,
`runs/*/evidence/` and `native-audit.json` bind all case logs, native calls,
host/boot/GPU identities and hashes to source `818b53e` and validator
`e8f2d4d`. `independent-review/` preserves source checks and original rejected
controls. The first review's temporary files were externally removed during a
build; that interruption is retained and affected checks were rerun in the
persistent artifact directory.

Trusted peers and a live exporter process/context remain required. In this
historical GPU/GPU profile, host directions were not included; current local
pinned host evidence is recorded above. External/managed allocations,
transport-wide idle import caches, device-wide
stream scheduling, MNNVL/VMM, native GPU cleanup faults and complete P5 remain
open. See [the ownership contract](cuda-ipc.md) and the
[completion plan](completion-plan.md).

## Cooperative CUDA IPC and NVLink

Source [`e19b3a5`](https://github.com/OrbitInfra/OrbitTE/commit/e19b3a5100f9eac37852db2d47437f6321b5862f)
qualifies the initial cooperative, owned GPU/GPU path on two physical H20 hosts.
Each host runs separate exporter/requester processes on its local GPU 0/1,
in both directions; their topology reports NV18. No cross-host GPU IPC or
measured NVLink bandwidth is claimed. Both installed consumers are built from
a clean packaged crate on stable 1.99.1 and Rust 1.85, with only the `cuda`
feature. They have no linked CUDA runtime, RDMA or Mooncake library dependency.
Runtime maps the recorded CUDA compatibility driver `libcuda.so.590.48.01`
on R535.161.08. This is Driver API qualification, not a toolkit/kernel test.

All 48 installed cases pass: 24 healthy READ/WRITE cases at 17, 4,133 and
2,097,171 bytes, eight source-close waits with an import still open, eight
requester exits before native import close, and eight stale-epoch rejections.
Each healthy case compares the entire logical allocation and guards. Native
evidence checks physical 2/4 MiB allocation initialization, exact export/import
handle pairing, relocated copy offsets, event completion, matched native close
and calling-context restoration before cleanup. Requester-exit cases kill the
importer after its copy drained but before close; they establish missing-close
retention, **not** in-flight GPU DMA termination or exporter-crash safety.

The 2,680 native records contain 64 exports/imports/copies and 56 closes. Eight
lost-close cases intentionally retain source allocation/admission until process
exit. All supervised children are reaped, all GPU UUID locks are released, and
the four selected GPUs return to their zero-allocation baseline. Loaded driver
paths and hashes, process identities, host/boot identities and every case file
are pinned in the external audit.

Independent review passes stable/MSRV × four-feature Clippy/test gates. The
combined build passes 249 unit tests, eight integration tests and one doctest;
six hardware/subprocess fixtures remain explicitly ignored in that CPU gate.
Thirty-nine controller controls and strict documentation checks pass. The review
found two evidence-validator gaps: early allocation frees and borrowing a later
cycle's context restoration. Commits `d2c8200` and `177a84f` fix those checks and
add counterexamples; all 48 original ledgers pass the strengthened validator.
These commits leave production code, installed consumer and native observer
unchanged. Four fresh independent hardware repeats also pass: stable and MSRV
healthy/graceful-close cases on both physical hosts. The eight review child PIDs
are gone, selected locks can be reacquired, and both hosts report zero GPU
allocations/no compute processes. Independent review accepts this bounded
cooperative profile; full P5.4 remains open.

Evidence stays outside Git at `/root/orbitte-artifacts/cuda-ipc-20261028/`:
`qualified-stable/` and `qualified-msrv/` hold installation manifests,
`matrix-profile.json` freezes the 48 cases, `matrix/` contains their raw process
and native logs, and `native-audit.json` binds the artifacts and validation
revision. Independent source/evidence reviews and counterexamples live at
`independent-review/` under the same artifact root (original workspace:
`/tmp/orbitte-ipc-review-20261028/`). Failed controls are preserved.

This profile requires trusted peers and a live exporter process/context until
every import closes. Legacy IPC handles authorize whole allocations, without
per-range hardware permissions or global revocation. Host/GPU IPC, external or
managed allocations, cached import ownership, broader stream scheduling,
MNNVL/VMM and forced-exporter-death safety remain unqualified or unimplemented.
Native import/copy/close failure retention has CPU controls only; physical GPU
cleanup faults and recovery remain unqualified.
See [the transport contract and reproduction method](cuda-ipc.md). P5.4 remains
open for those requirements; this substage does not establish full TENT parity.

## GPUDirect RDMA

Source [`cda0fdf`](https://github.com/OrbitInfra/OrbitTE/commit/cda0fdf16aec7b128e21c60fa860c340c164cf03)
qualifies explicit Direct RDMA for owned `CudaMemory` on two physical H20 hosts,
using one
selected GPU and two locked mlx5 RoCEv2 RNICs per host. Both stable 1.99.1 and
Rust 1.85 build independent installed consumers from the packaged crate. The
source passes 243 unit tests, eight integration tests, one doctest, Clippy and
all eight feature/toolchain CI combinations. Thirty-six controller controls
include corrupt or missing GPU MR, PD, rail, process and lifetime evidence.

All 72 installed cases pass: each toolchain runs both physical directions for
host/GPU, GPU/host and GPU/GPU READ plus WRITE. Small requests cover 1/17 bytes
with polling and 32/64 bytes with event progress. Two-rail cases use three
parent requests of 2 MiB + 17 bytes per
operation, a 1 MiB base slice and folded tails, with both polling and event
progress. Every case checks the complete allocation and guard bytes. Passive
native witnesses identify actual GPU pointer type, MR flags and range, local
SGE/lkey/QP PD, remote rkey/permission, and payload work on both selected
contexts. GPU sources issue no MW operations and use ordinary fenced SEND;
source private PD counters drain completely.

`native-audit.json` pins the two packages, binary/observer hashes, all 72 case
files, host/boot/GPU identities and loaded native libraries. Every supervised
child is reaped and every RNIC/GPU lock is released. Selected-RNIC QP/CQ/MR/PD
identity sets match their before-run inventories; GPU
memory returns to zero on both hosts. Two additional installed repeats capture
eight live `hardware_peer` PID/context identities and prove that all eight
disappear after exit. B gained six other context entries on selected RNICs and
three each on two unselected RNICs; their origin remains unassigned. The initial
failed global inventory check is preserved, and host-wide context non-growth
is **not qualified**. The final audit records `passed-with-limits`. Mapped CUDA
compatibility libraries are
`libcuda.so.590.48.01`, reporting Driver API 13.1, on R535.161.08 hosts.
This is a Driver API/GPUDirect result, not a CUDA toolkit/kernel compilation test.

Evidence stays outside Git at
`/root/orbitte-artifacts/gpudirect-rdma-20261027/`: `qualified-stable/` and
`qualified-msrv/` contain installation manifests; the four corresponding
`*-small/` and `*-multirail/` directories preserve all process and native
receipts. Development runs and the first controller rejection remain archived;
the latter rejected a GPU profile through a host-only validator before any
remote process started. The final packages include the corrected validator.

Independent review accepts the source and this bounded installed profile; it
reran the relevant source gates and independently checked the frozen packages
and native evidence. Its archived reports and checksums are in `reviewer/`.
Failure retention has integrated CPU ABI controls for partial construction,
payload/RQ MR and private-PD cleanup, and CUDA restoration/visibility failures.
Actual GPU teardown
faults, physical faults, InfiniBand, other GPUs/providers, performance, mixed
staged/direct workloads, external/managed/IPC allocations, NVLink and external
producer stream handoff remain
unqualified or later P5 work. GPU source MR/QP reuse is deliberately absent.
See the [ownership and reproduction contract](rdma-cuda.md#gpudirect-rdma-ownership).

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
