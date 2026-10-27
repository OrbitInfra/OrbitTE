# Installed host RDMA qualification

This gate consumes a packaged `orbit-te` crate as a normal Rust dependency.
It runs the public `TransferEngine` API with the `rdma` feature only. Private
libtests and CUDA are absent from the installed binary. The explicit native fault profiles add a separate process-local qualification
library described below.
The published alpha on crates.io remains a name reservation; this workflow
packages the selected source without publishing it.

## Build the artifact

Use the prerequisites in [the build guide](build.md), including the native
compiler/libclang environment. From a clean committed checkout, select an
artifact directory outside the repository:

```sh
python3 tests/hardware/build_installed.py \
  --toolchain stable --output /var/tmp/orbitte-installed/stable
python3 tests/hardware/build_installed.py \
  --toolchain 1.85.0 --output /var/tmp/orbitte-installed/msrv
```

The script creates a `.crate`, extracts it into the artifact directory and
copies its packaged `rdma_host` fixture and resource/restart modules into an
independent consumer package.
Cargo fetches the packaged lockfile dependencies first, then resolves OrbitTE
from that extracted package offline, preserves locked dependency versions, checks formatting/Clippy and installs a release executable
under `installed/bin/orbitte-rdma-host`. The manifest pins the source commit,
archive, every fixture module, consumer lockfile, binary, features and local
native libraries. `orbitte-installed-host-v4` identifies this test protocol;
the production public API and wire v14 are unchanged.
The consumer is an acceptance fixture, not a new production CLI/API.

CI performs this build on stable and Rust 1.85.0 and retains the artifacts.
Running `--protocol` proves the binary starts; it is not hardware qualification.
`--allow-dirty` is for local build diagnosis only. The hardware controller
rejects a dirty or mismatched installation manifest.

## Run the two-host profile

Use the existing identity-pinned SSH profile and supervisor described in
[the native RDMA guide](rdma-cuda.md). Select `runner: "orbitte-installed-host"`,
two explicit `rails`, a `gid_index`, distinct source/requester nodes, and one
absolute resource lock per RNIC on each node. No GPU is requested. Each case
must name hosts with different inventoried hostnames and kernel boot IDs;
two executor aliases on one machine cannot qualify the two-host profile.
The controller rejects that placement before remote access, then rechecks each
host/boot identity during deployment and inside the resource-lock supervisor.

Each case has these explicit fields:

```json
{
  "source": "a", "requester": "b",
  "source_kind": "cpu", "requester_kind": "cpu", "transport": "rdma",
  "mode": "installed-host", "operation": "read", "rdma_events": false,
  "parents": 16, "payload": 6291457, "block": 1048576,
  "num_lanes": 3, "mask": 3
}
```

```sh
python3 tests/hardware/run.py --profile /var/tmp/host-profile.json \
  --installation /var/tmp/orbitte-installed/stable/manifest.json \
  --binary /var/tmp/orbitte-installed/stable/installed/bin/orbitte-rdma-host \
  --output /var/tmp/orbitte-installed/native-stable
```

The fixture bounds each peer to 256 MiB of application backing, 32 parents,
32 MiB per parent and 1–3 QPs per peer/context/operation. Each of three rounds
starts concurrent public parent requests, varies every parent's input, checks
the complete allocation and guards, and waits for source-handler/grant drain.
Both contexts publish persistent MRs even with a one-rail dispatch mask.
Native completion-byte counters, bounded QP creation and increasing reuse
must agree with the workload; persistent MRs must not be recreated. Both peers
close, unregister and release all counted owners before reporting success.

TCP carries only rendezvous and test barriers. Payload requests explicitly
select `Transport::Rdma`; native byte counters and mapped providers must prove
that path. The controller independently checks the complete round/result
ledger, provider mappings, native-child exit and released resource locks.
Missing, duplicate, mismatched or partially drained results fail the run.

## Peer restart with a surviving engine

Select `mode: "installed-restart"` with `fault_role: "source"` or
`"requester"` and `generations: 2` or `3`. The other profile fields and resource
bounds above still apply. The supervisor restarts only the selected direct
child; the other process keeps the same `TransferEngine` and registration
through every generation. RNIC locks stay held until the complete sequence is
reaped. The controller forwards each new test-control address independently of
the surviving process's generation.

Each generation executes three full public READ/WRITE rounds. Between
generations, the target reaches zero SQ/RQ debt and active grants, while its
cached QPs and four persistent MRs remain live. It publishes its exact PID and
resource snapshot, then the owning supervisor sends SIGKILL and reaps that
child before starting the replacement. The target never calls orderly close
at this point. This is abrupt death with idle cached native resources; it is
not death during outstanding DMA.

The replacement reuses its original engine service address and registration
ID, with a fresh epoch. Its peer verifies the unchanged surviving engine's
endpoint, epoch, region ID, address and length. The survivor keeps every old
`RemoteSegment` returned by the public API. Once each replacement is ready,
all earlier epochs must fail both `open_segment` and a data submission with
`EpochMismatch`, zero QP/WR creation, zero transferred bytes,
`RemoteEffect::None` and local drain. Both processes compare their complete
allocations before starting new-authority transfers.

The survivor's PID, engine identity, registered address and four opened MRs
must remain stable. Connection counters continue across generations; each
new generation creates at most the selected rail count times `num_lanes` QPs,
and the configured physical-QP budget bounds retained old plus new pairs.
Native byte counters must match each generation's workload. At the end, both
remaining processes close and unregister, with zero counted owners and every
opened QP closed. SIGKILL receipts prove child termination, not a Rust close
result for the killed process.

`installed_restart.py` reconciles phase/round receipts with the supervisor's
actual child PIDs and exit statuses. It also checks provider mappings for
every process, distinct replacement epochs, all stale-generation refusals,
persistent-MR counts, final byte/drain results and released locks. Its negative
controls reject incomplete or contradictory evidence. The same supervisor
continues to support the separate native cleanup fixtures that replace both
processes; their evidence must not be interpreted as a surviving-engine run.

## Interrupt a posted completion RQ

`installed-interruption` uses the same restart roles and two/three-generation
supervisor policy. Set `payload <= block` (one slice per parent), with 1–32
concurrent parents. The test protocol is `orbitte-installed-host-v4`; the
production protocol remains v14.

Before each nonfinal restart, complete three healthy byte/guard rounds, then
arm a source-side control relay. It forwards Describe and the v14 single-slice
RDMA handshake unchanged, but holds READY/GRANT after the source has bound its
MW and posted its real completion receive. It rejects TCP payload and sliced
opcodes. All native payload traffic still travels directly over RDMA.

Both peers must have all grants pending, unchanged payload-byte counters and
no newly opened QPs. Source receipts require one posted RQ, bound MW and
receive credit per parent. The requester must expose Running, undrained public
batches: short waits time out and `free()` returns `BatchBusy`. Conflicting CPU
writes fail on both peers. Only after both receipts agree may the supervisor
SIGKILL the exact source or requester child.

When the requester dies, the surviving source relay closes its held control
sockets; native source teardown must drain the outstanding RQ. When the source
dies, the surviving requester must report failed, locally drained batches
with zero bytes and `RemoteEffect::None`. The survivor verifies all backing
bytes, regains CPU write access, releases temporary grants/MWs/batches, and
keeps its process, engine, context/PD and four persistent MRs across recovery.
Failed CQ lanes must be fully reclaimed (`progress_failures == progress_recoveries`);
replacements are created on demand by the next connection. Fresh-epoch rounds
require all selected lanes, unchanged failure/recovery counters and correct bytes.
All old-epoch refusals then repeat as above.

This is a deterministic pre-payload authorization interruption with native RQ
debt. No failed request has permission to post payload yet. It does not prove
mid-payload DMA interruption, lost SQ CQEs, physical link/device loss or native
cleanup-failure retention. The relay is a test boundary, not a transport or
production proxy. CPU relay controls execute in every packaged-consumer build;
`installed_interruption.py` independently reconciles pending/drain receipts
with the restart ledger. Actual execution status is in [qualification](qualification.md).

## TENT correspondence and limits

The reference is the frozen TENT commit
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`; see
[the source mapping](architecture.md#correspondence-and-remaining-differences).

| TENT responsibility | OrbitTE path exercised by the installed consumer |
| --- | --- |
| Context/PD and registered buffers outlive individual endpoints | Public registration publishes context-owned persistent MRs; they survive peer process replacement in the live engine |
| `RdmaEndPoint` checks peer identity/QP arrays, retires a stale bootstrap and reconnects | Endpoint arrays and connection generations fence cached QPs before replacement; data uses the fresh peer authority |
| Transport `Workers` progress and monitor reclaim | The surviving worker pool keeps servicing the same contexts while new endpoints are admitted |
| `Workers::asyncPollCq`, `handleCompletion` and timeout sweep | Exact WR completion drives grants; missing observation fences shared QPs and releases unresolved ownership only after native drain |

Frozen TENT's `endpoint.cpp` distinguishes duplicate peer QP arrays from a new
connection on the same NIC path, retiring the latter for reconnection. OrbitTE
additionally makes the caller-supplied source epoch explicit in its public
segment contract. The old segment must fail rather than silently adopt the
new process's authority. This gate checks that Rust contract and owner
separation; it is not a TENT wire/API interoperability or native restart oracle.

TENT polls each context's worker-assigned CQ, dispatches completions to slices
and sweeps software timeouts. OrbitTE follows those ownership boundaries while
retaining its explicit Rust DMA loans and WRITE `Unknown` outcome. The native
loss tool tests those contracts without changing the production worker design.

The main fixture owns public requests, rendezvous and byte checks.
`rdma_host/resources.rs` owns context selection, registration and drain;
`rdma_host/restart.rs` owns the peer replacement sequence;
`rdma_host/interruption.rs` owns public pending-batch and drain assertions;
`rdma_host/gate.rs` owns the bounded test control relay.
`rdma_host/completion.rs` controls the separate native loss tool;
`installed_completion.py` joins native WR/CQE/destruction identities.
No test-only route bypasses the production handshake or native resource lifecycle. TCP carries
test coordination; payload requests remain explicit RDMA.

The installed gate covers healthy host traffic, idle-resource process restart,
posted-RQ interruption before payload authorization, lost payload CQEs,
requester QP cleanup failure and in-flight QP interruption. The last two
fixtures have [selected two-host qualification](qualification.md#installed-two-host-fault-qualification); broader
native teardown failures retain separate installed-artifact gates. Physical port/device faults, per-rail
bandwidth/fairness and InfiniBand/GPU transfers are outside this profile. Native fault libtests retain
their separate internal ownership evidence. See [qualification](qualification.md)
for executed artifact/profile results and
[P4's final acceptance queue](completion-plan.md#p4-remaining-delivery) for the
remaining hardware and independent-review gates.

## Lost payload completions

Select `mode: "installed-lost-completion"` with the same restart role and
2–3 generations. Freeze `payload <= block` (one native payload WR per parent),
8 parents with one lane or 16 with three lanes, a 1 MiB payload, both rails,
both directions, READ/WRITE, and polling/event-driven progress before running.
Regressions include healthy single-rail and posted-RQ interruption cases.

`build_installed.py` also compiles the packaged `tests/hardware/cq_fault/`
qualification tool against the system verbs headers. Its CPU control checks
real-callback pass-through, selective loss and error preservation. The manifest
pins the tool sources, compiler and shared-library hash. The controller copies
that immutable library beside the installed consumer; the supervisor checks
its hash before each requester generation and sets `LD_PRELOAD` only for that
fault profile. The production crate and dependency features remain unchanged.

After healthy warmup, both peers replace their buffers with a new input pattern
and destination sentinels. The tool arms only on the requester. It calls the
original provider `post_send` and `poll_cq`; it records accepted payload WRs and
removes their actual successful READ/WRITE CQEs from the returned completion
array. Nonmatching and error completions pass through. Separate bounded JSONL
receipts identify the native process, context, CQ, QP, WR and addresses. No
payload or successful completion is fabricated. The controller joins exact WR
identities and parent ranges, independently of public fixture counters.

At the kill point, all payload CQEs have been observed by the tool, while the
engine still owns SQ debt, source RQ/MW authorization, DMA loans and busy public
batches. The supervisor kills its exact configured child. The surviving
requester must fail and drain its batches, retain zero reported payload bytes,
and classify WRITE effects as `Unknown` (READ has no remote write effect).
Actual successful `ibv_destroy_qp` receipts must cover every affected native QP
before the public drain receipt. On either surviving role, full new payload
bytes and guards must match, temporary owners must drain, the original four
persistent MRs remain registered, and CPU access becomes available. Subsequent
generations reject stale authority and resume healthy native transfers.

This profile establishes **lost engine observation after real payload
completion**. It does not establish a physical link failure or termination of
DMA interrupted mid-flight. Native cleanup failures and physical faults keep
separate qualification gates; this is correctness evidence, not performance.


## Native cleanup failure

`mode: "installed-cleanup-failure"` exercises the packaged public consumer's
quarantine contract. Freeze `fault_role: "requester"`, two generations, one
parent, `payload <= block`, one lane and rail mask 1 or 2. It uses the same
identity-pinned two-peer controller and immutable fault library as completion
loss. The fixture passes the focused
[single-host diagnostic](qualification.md#installed-cleanup-fixture) and the
[declared two-host cases](qualification.md#installed-two-host-fault-qualification).
Broader native cleanup failures retain their separate gates.

After the healthy rounds, the observer records one accepted payload WR and
withholds its actual successful CQE. It arms cleanup failure on that exact
QP. When the engine retires the timed-out connection, a joined helper thread
calls the real `ibv_destroy_qp` under a thread-local seccomp filter. The filter
rejects write/ioctl only on that context's uverbs command descriptor. The
provider must return `EIO`; the calling engine worker remains unfiltered.
This is syscall fault injection, not a spontaneous hardware failure.

The public batch must fail with `Unquiesced` and `locally_drained = false`;
`free` stays busy, CPU writes remain excluded, and SQ, direct-byte, batch and
inflight-byte charges remain held. Unregister removes lookup ownership, while
two close attempts cannot report drain or retry native destruction. After all
fixture-owned strong handles drop, an external allocation's weak owner and a
real QP query must still prove retained backing and the same native QP.
Closing fences software admission before calling the destructor; it does not
require a preceding transition to ERR. Record the actual queried state. Separate
native receipts join the submitted WR, one destructor failure and both QP
queries to the public ownership proof. The supervisor then reaps that exact
process; its replacement runs the existing stale-authority and healthy-transfer
checks against the surviving source.

CPU controls verify FD/thread isolation and reject wrong QP identities,
retries, missing backing and false drain evidence. They do not qualify native
retention. This profile also does not prove interrupted in-flight DMA, MR/MW
or channel cleanup failure through the installed API, physical link loss or
provider teardown; those boundaries retain their separate gates.

## In-flight DMA interruption

`mode: "installed-inflight-dma"` uses one parent, one lane, rail mask 1 or 2,
and `payload == block` (one native WR). The focused profile selects 4 MiB
READ/polling and 16 MiB WRITE/events, with two persistent registered contexts.
Only this mode permits blocks up to 16 MiB; other modes retain their 4 MiB
limit. The first 4 MiB WRITE diagnostic completed every byte before ERR and
was correctly rejected as proof of interruption.
It has no restart controls: both original engines must survive the fault.
Use the same packaged consumer, pinned observer library, provider inventory
and resource leases described above. Implementation and executed scope are
reported separately in [qualification](qualification.md).

After three healthy rounds, the requester observer calls the real provider
`post_send`, then transitions that exact test-owned QP to `IBV_QPS_ERR` through
`ibv_modify_qp`. It changes no port or device configuration. All CQEs pass
through unchanged. The observer joins the accepted WR to its actual error WC
by CQ, QPN and WR ID; it never reads opcode or byte length from an error WC.
A real successful QP destruction must follow that failed completion before
the observer can report native drain.

The public batch must fail with zero successful payload bytes and proven
local drain. WRITE effects remain unknown. Both peers wait for SQ/RQ, grants,
windows and byte/batch charges to drain and for progress recovery. Only then
does the fixture read the entire registered allocation through the public
API. The destination must contain both copied source bytes and untouched
sentinel bytes; positions where source equals the sentinel are ambiguous and
count toward neither proof. Zero-progress and fully completed copies fail
qualification. Every other byte, including guards and inputs, must be unchanged.
No concurrent CPU read races with active DMA.

Both original processes retain their four persistent MRs and complete three
more healthy rounds using bounded replacement QPs, then close all resources.
`rdma_host/inflight.rs` owns the public assertions; `cq_fault/inflight.c` owns
the process-local native trigger/observer; `installed_inflight.py` joins the
independent native and public ledgers. CPU controls reject completed/no-op
copies, wrong WR/address/process identities, missing destruction and unpaid
debt. The trigger corresponds to the frozen TENT endpoint's
[`beginDestroyNoLock`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/endpoint.cpp)
ERR transition. OrbitTE keeps PD/MRs in `RdmaContext`, QPs in `RdmaEndpoint`,
WR completion ownership in CQ lanes, and shared progress in transport Workers.
The fixture verifies those existing owners through the installed API.

Both compiler profiles pass the focused
[single-host diagnostic](qualification.md#installed-in-flight-dma-fixture) and
[declared two-host cases](qualification.md#installed-two-host-fault-qualification).
These process-local QP faults do not qualify physical link/device loss,
InfiniBand or throughput.

## MR/MW cleanup and engine replacement

`installed-memory-cleanup` exercises the packaged public API with one parent,
one QP and one payload slice. Select `cleanup_kind: "mr"` and an explicit
`fault_role` of `requester` or `source`; `cleanup_kind: "mw"` requires `source`.
Use one rail (`mask: 1` or `2`), while registering both locked contexts.
The frozen cases reuse 64 KiB READ/polling and WRITE/events workloads; larger
payloads and higher concurrency add no new owner contract to this gate.

After three healthy byte/guard rounds, the target fails the real
`ibv_dereg_mr` or provider `dealloc_mw` call with EIO. A joined helper thread
filters command writes/ioctls only on that context's uverbs FD; engine workers
remain unfiltered. The observer selects the actual payload lkey or source
parent MR, and joins MW cleanup to the accepted BIND's MR/MW/key identity.
It rejects any destructor retry on the retained object.

Idle MR failure must retain the registration charge and actual backing, with
zero SQ/RQ debt. MW failure must retain its parent MR borrow, grant, exact DMA
loan, byte quota and key domain. Repeated public close must not report drain.
After unregistering and dropping fixture owners, a weak reference to the actual
allocation must survive. The observer checks unchanged native handle/key fields
and calls the real PD destructor, which must still return EBUSY. This is a
native dependency check, not an MR query API or a physical-device-loss test.

Both original processes then create fresh engines at the same service addresses.
Old source epochs and segments fail before native QP creation. Three more
complete byte/guard rounds reuse the fresh engines' persistent MRs and QPs;
the fresh allocations are released, while the old quarantined allocation and
native dependency remain retained. Process exit and supervisor reaping finally
release the kernel resources. A successful replacement does not mark the old
engine drained or recover its quarantined owners.

`rdma_host/memory.rs` owns these public lifecycle assertions;
`cq_fault/memory.c` owns native observation and scoped syscall failure;
`installed_memory.py` independently joins both ledgers to supervisor PIDs,
provider mappings and released locks. CPU controls reject wrong identities,
missing native errors, retries, early backing/quota release, false drain,
stale epochs and unreaped children. Hardware results belong in
[qualification](qualification.md), separately from fixture implementation.
