# Installed host RDMA qualification

This gate consumes a packaged `orbit-te` crate as a normal Rust dependency.
It runs the public `TransferEngine` API with the `rdma` feature only. Private
libtests and CUDA are absent from the installed binary. The explicit lost-completion
profile adds a separate process-local native qualification library described below.
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
has these explicit fields:

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

Frozen TENT's `endpoint.cpp` distinguishes duplicate peer QP arrays from a new
connection on the same NIC path, retiring the latter for reconnection. OrbitTE
additionally makes the caller-supplied source epoch explicit in its public
segment contract. The old segment must fail rather than silently adopt the
new process's authority. This gate checks that Rust contract and owner
separation; it is not a TENT wire/API interoperability or native restart oracle.

The main fixture owns public requests, rendezvous and byte checks.
`rdma_host/resources.rs` owns context selection, registration and drain;
`rdma_host/restart.rs` owns the peer replacement sequence;
`rdma_host/interruption.rs` owns public pending-batch and drain assertions;
`rdma_host/gate.rs` owns the bounded test control relay. No test-only route
bypasses the production handshake or native resource lifecycle. TCP carries
test coordination; payload requests remain explicit RDMA.

The installed gate covers healthy host traffic, idle-resource process restart
and posted-RQ interruption before payload authorization. Mid-payload death,
lost SQ completions, native teardown failures and poisoned-engine recovery
still need installed-artifact
gates. Physical port/device faults, per-rail bandwidth/fairness and
InfiniBand/GPU transfers are outside this profile. Native fault libtests retain
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
