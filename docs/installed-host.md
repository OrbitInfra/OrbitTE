# Installed host RDMA qualification

This gate builds a packaged `orbite` crate as an ordinary dependency and runs
the public `TransferEngine` API with only the `rdma` feature. Fault and
rail-accounting modes add a separate process-local observer library. The crate
is packaged locally, never published.

## Build the artifact

Install the [build prerequisites](build.md), then build from a clean committed
checkout into a directory outside the repository:

```sh
python3 tests/hardware/build_installed.py \
  --toolchain stable --output /var/tmp/orbite-installed/stable
python3 tests/hardware/build_installed.py \
  --toolchain 1.85.0 --output /var/tmp/orbite-installed/msrv
```

The script creates a `.crate`, extracts it, copies the packaged `rdma_host`
fixture into an independent consumer, resolves OrbiTE offline from that package
with locked versions, runs fmt and Clippy, and installs
`installed/bin/orbite-rdma-host`. The manifest pins the commit, archive, fixture
modules, lockfile, binary, features and native libraries; the test protocol is
`orbite-installed-host-v4`. CI builds both toolchains, but `--protocol` only
proves the binary starts. The controller rejects a dirty or mismatched manifest
(`--allow-dirty` is for local diagnosis only).

## Run the two-host profile

Use the identity-pinned SSH profile and supervisor from the
[RDMA guide](rdma-cuda.md#run-a-native-profile) with
`runner: "orbite-installed-host"`, two `rails`, a `gid_index`, distinct source
and requester nodes, and one resource lock per RNIC on each node. Nodes must
have different hostnames and boot IDs; the controller rechecks both at
deployment and inside the lock supervisor.

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
  --installation /var/tmp/orbite-installed/stable/manifest.json \
  --binary /var/tmp/orbite-installed/stable/installed/bin/orbite-rdma-host \
  --output /var/tmp/orbite-installed/native-stable
```

Bounds: 256 MiB of backing per peer, 32 parents, 32 MiB per parent and 1–3 QPs
per peer/context/operation. Each of three rounds runs concurrent parent requests
with fresh input, checks every byte and guard and waits for source drain. Both
contexts publish persistent MRs even under a one-rail mask, and native byte
counters, bounded QP creation and reuse must match the workload. TCP carries only
rendezvous; the controller checks the round ledger, provider mappings, child exit
and released locks.

## Physical rail accounting

Select `mode: "installed-rail-accounting"` with 16 parents,
`payload: 4194305`, `block: 1048576`, `num_lanes: 1` and `mask: 3`; run
READ/polling and WRITE/events, reversing hosts for the second toolchain. Each
round has exactly 64 payload WRs and 67,108,880 payload bytes.

The requester-only observer (`accounting.c`) forwards native calls and CQEs
unchanged and joins each accepted payload WR to its successful CQE by context,
CQ, QPN and WR ID. The verifier checks both rails' payload, at least two parents
outstanding on each rail's shared QP, at most eight payload WRs per QP, and
native bytes equal to the public counters. This measures accepted work, not
fabric saturation, fairness or throughput.

## Automatic discovery against an independent inventory

Add `"discovery": "auto"` to the rail-accounting cases: no device, port or GID
pins, a whitelist of exactly the two locked RNICs, and an identical blacklist to
check whitelist precedence. A separate engine first requests strict GID 255 and
must get `Unavailable` with no retained owners.

Under the RNIC locks, the supervisor runs `discovery_inventory.py` before and
after each child. This read-only tool shares no OrbiTE code; it records sysfs
PCI, NUMA, port, GID, interface and node data. The verifier requires an unchanged
inventory with one active Ethernet port and one RoCEv2 candidate per RNIC, then
compares public NIC IDs, memory priority tiers and strict-pin rejection against
it. This mirrors TENT's `platform/cpu_probe.cpp`, `runtime/topology.cpp` and
context address selection. It does not cover port transitions, link loss,
InfiniBand or hotplug.

## Peer restart with a surviving engine

Select `mode: "installed-restart"` with `fault_role: "source"` or `"requester"`
and `generations: 2` or `3`. Each generation runs three full READ/WRITE rounds,
drains to zero debt with cached QPs and four persistent MRs still live, then the
supervisor SIGKILLs and reaps only the selected child. This is abrupt death with
idle cached resources, not death during DMA.

The replacement reuses its service address and registration ID with a fresh
epoch. The survivor keeps its PID, engine, registration and MRs. Every earlier
epoch must fail both `open_segment` and data submission with `EpochMismatch`,
zero QP or WR creation, zero bytes, `RemoteEffect::None` and local drain. Each
generation creates at most `rails × num_lanes` new QPs. `installed_restart.py`
reconciles the receipts with the supervisor's actual PIDs and exit statuses.

## Interrupt a posted completion RQ

`installed-interruption` uses the restart roles with `payload <= block` (one
slice per parent) and 1–32 parents. Before each nonfinal restart, a source-side
control relay forwards Describe and the RDMA handshake but holds READY/GRANT
after the source has bound its MW and posted its completion receive; payload
still travels over RDMA.

Both peers must show pending grants, unchanged byte counters and no new QPs;
the requester's batches stay Running, waits time out and `free()` returns
`BatchBusy`. Then the exact child is killed. A surviving source must drain the
outstanding RQ; a surviving requester must report failed, locally drained
batches with zero bytes and `RemoteEffect::None`. The survivor keeps its engine,
PD and MRs, reclaims failed CQ lanes and resumes with fresh-epoch rounds. This
interrupts authorization before payload; it does not prove mid-payload DMA
interruption.

## Lost payload completions

Select `mode: "installed-lost-completion"` with the restart roles, 2–3
generations, `payload <= block`, 8 parents with one lane or 16 with three, a
1 MiB payload, both rails, both directions and both progress modes.

`build_installed.py` also builds the packaged `tests/hardware/cq_fault/` tool,
pinned by hash and loaded with `LD_PRELOAD` only for this profile. Armed on the
requester, it records accepted payload WRs and removes their successful CQEs
from `poll_cq`; it never fabricates data or completions. At the kill point the
engine still owns SQ debt, source authorization, DMA loans and busy batches. The
surviving requester must fail and drain with zero reported bytes and WRITE
effects `Unknown`, and successful `ibv_destroy_qp` receipts must cover every
affected QP before the drain receipt. This proves recovery after lost engine
observation, not physical link failure.

## Native cleanup failure

`mode: "installed-cleanup-failure"` checks quarantine through the public API:
`fault_role: "requester"`, two generations, one parent, `payload <= block`, one
lane and mask 1 or 2. The default `qp` selector withholds one successful CQE,
then makes the real `ibv_destroy_qp` return `EIO` by filtering writes and ioctls
on that context's uverbs descriptor in a helper thread. This is syscall fault
injection, not a spontaneous hardware fault.

The batch must fail with `Unquiesced` and `locally_drained = false`; `free` stays
busy, CPU writes stay excluded and SQ, byte and batch charges stay held. Repeated
close cannot report drain or retry destruction. After the fixture drops its
handles, a weak owner and a real QP query must still show the retained backing
and QP. The replacement process then runs the stale-authority and healthy
checks. See the [single-host diagnostic](qualification.md#installed-cleanup-fixture)
and [two-host cases](qualification.md#installed-two-host-fault-qualification).

## In-flight DMA interruption

`mode: "installed-inflight-dma"` uses one parent, one lane, mask 1 or 2 and
`payload == block` (one WR); the focused profile uses 4 MiB READ/polling and
16 MiB WRITE/events. Only this mode allows blocks up to 16 MiB. Both engines must
survive the fault.

After three healthy rounds, the observer posts the real WR and moves that QP to
`IBV_QPS_ERR` with `ibv_modify_qp`, changing no port or device. It joins the WR
to its error completion by CQ, QPN and WR ID, and requires a successful QP
destruction before native drain. The batch must fail with zero bytes, proven
local drain and WRITE effects unknown. After drain, the destination must contain
both copied and untouched sentinel bytes (zero-progress or fully completed copies
fail), with every other byte unchanged. Both processes then run three more
healthy rounds. The trigger corresponds to TENT's
[`beginDestroyNoLock`](https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport/rdma/endpoint.cpp)
ERR transition.

## MR/MW cleanup and engine replacement

`installed-memory-cleanup` uses one parent, one QP and one slice, with both
contexts registered and one rail active. Select `cleanup_kind: "mr"` with
`fault_role` `requester` or `source`, or `cleanup_kind: "mw"` with `source`. The
frozen cases use 64 KiB READ/polling and WRITE/events.

After three healthy rounds, the real `ibv_dereg_mr` or `dealloc_mw` returns
`EIO` through the same scoped filter, and any retry is rejected. MR failure must
keep the registration charge and backing; MW failure must keep its parent MR
borrow, grant, DMA loan, quota and key domain. A real PD destroy must still
return `EBUSY`. Both processes then create fresh engines at the same addresses:
old epochs fail before QP creation, three rounds pass, and the old quarantined
allocation stays retained until process exit. See the
[six two-host cases](qualification.md#installed-memory-cleanup-and-replacement).

## CQ, completion-channel and PD dependency failures

Use `installed-cleanup-failure` with `cleanup_kind` set to `cq`, `channel` or
`pd`; omitting it keeps the `qp` case. Channel failure requires event progress.

| Owner | Native failure | Must stay retained after the fixture drops its owners |
| --- | --- | --- |
| CQ | A dependent RESET QP blocks CQ destruction | CQ, PD and the unresolved grant, SQ, batch and allocation |
| Completion channel | An extra CQ blocks channel destruction | Channel, context and unresolved grant and allocation; QPs and engine CQs are released |
| PD | A Type-2 MW blocks idle PD destruction | PD and device only; MRs, grants, batches and the allocation are released |

Each destructor returns `EBUSY` once, and retries or premature device close fail
the run. The CQ and channel cases reuse a withheld CQE and require unquiesced
batches; the PD case is idle. All three then run the live-peer restart checks.
See [qualification](qualification.md#installed-dependency-cleanup).

## TENT correspondence and limits

| TENT responsibility | Exercised OrbiTE behavior |
| --- | --- |
| Context/PD and registered buffers outlive endpoints | Persistent context MRs survive peer process replacement |
| `RdmaEndPoint` retires a stale bootstrap and reconnects | Connection generations fence cached QPs; data uses the fresh authority |
| `Workers` progress and monitor reclaim | The surviving worker pool keeps serving the same contexts |
| `Workers::asyncPollCq`, `handleCompletion`, timeout sweep | Exact WR completion drives grants; missing observation fences QPs until native drain |

OrbiTE also makes the source epoch explicit, so an old segment fails instead of
adopting a new process's authority; this gate is not a TENT wire or restart
oracle. Fixture owners: `rdma_host/resources.rs` (registration and drain),
`restart.rs`, `interruption.rs`, `gate.rs` (test relay), `completion.rs`,
`inflight.rs` and `memory.rs`, with `installed_*.py` verifiers. Physical port or
device faults, per-rail fairness, InfiniBand and GPU transfers are outside this
profile; see [qualification](qualification.md) and the
[P4 queue](completion-plan.md#p4-remaining-delivery).
