# Installed host RDMA qualification

This gate consumes a packaged `orbit-te` crate as a normal Rust dependency.
It runs the public `TransferEngine` API with the `rdma` feature only. Private
libtests, injected verbs tables and CUDA are absent from the installed binary.
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
native libraries. `orbitte-installed-host-v2` identifies this test protocol;
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
`rdma_host/restart.rs` owns the peer replacement sequence. No test-only route
bypasses the production handshake or native resource lifecycle. TCP carries
test coordination; payload requests remain explicit RDMA.

The installed gate covers healthy host traffic and the declared idle-resource
process-restart profile. Outstanding-DMA death, lost completions, native
teardown failures and poisoned-engine recovery still need installed-artifact
gates. Physical port/device faults, per-rail bandwidth/fairness and
InfiniBand/GPU transfers are outside this profile. Native fault libtests retain
their separate internal ownership evidence. See [qualification](qualification.md)
for executed artifact/profile results and
[P4's final acceptance queue](completion-plan.md#p4-remaining-delivery) for the
remaining hardware and independent-review gates.
