# OrbitTE

A Rust-native transfer engine for AI infrastructure.

OrbitTE is an independent OrbitInfra project using Mooncake TENT as its
capability baseline. It is intended to provide byte movement for OrbitKV and
other systems while leaving cache identity, recovery planning and replica
ownership with those consumers.

## Status

**Early development.** The source tree implements owned/external CPU memory registration,
epoch-fenced P2P descriptors, bounded synchronous/asynchronous TCP READ/WRITE batches and
separate requester/source close and drain, plus bounded local/remote and
batch-completion notifications. Async batches retain real owners through
cancel, timeout and detached-handle cleanup.
Real loopback and independent-process tests cover bytes,
restart fencing and failure ownership. The published `0.1.0-alpha.1` is still
the original identity-only package; this implementation has not been released.

External host backing retains its actual allocation owner. Duplicate ranges
share registration references; overlapping views share one allocation lock.
Host registration batches preflight every range before publishing changes.
Transfers accept absolute addresses and retain the resolved backing during I/O;
local and remote lookup follows TENT's containing-buffer order.

Opt-in `rdma` and `cuda` features add single-rail RC verbs with actual application
host MRs or explicit private staging, retained NVIDIA device/pinned allocations
and synchronized CUDA copies. Direct host DMA leases retain actual registration
owners and exclude conflicting access across aliases; disjoint ranges remain
usable. Failed native cleanup retains those owners and charges. Device
allocations still use the declared staging path; GPUDirect is not implemented.
They use the existing `ibverbs-sys` and `cudarc` Rust bindings, share batch
owners and conservatively retain unquiesced failures.
Each engine lazily reuses one RNIC context/PD. Requester local-only and source MW-bind host MRs now persist across requests
in one bounded `LocalBufferManager` (P4.7e). P4.7f implements connected QP reuse
for negotiated host Direct peers, with one grant per fixed-role/operation pair.
Staged peers retain both physical QP-close barriers. Independent source/CPU review and two
separately built 31-case native runs accept the declared source-cache profile. P4.7a implements one-use
Type-2B MW source grants: parent MRs have LOCAL_WRITE/MW_BIND permissions only;
bound MW keys become visible after a successful BIND CQE and are invalidated
before final ACK. Independent review accepts its scoped CPU and native gates.
A context-owned async event consumer stays alive through native QP/CQ teardown;
lost consumption retains actual owners and loans.
P4.7b added a PD-lifetime full-rkey burn ledger: MW destruction cannot
reset key history, and exhausted tags or historical-index capacity refuse new
authority. Its wire v7 bound both process/PD/connection identities, an
independent grant generation and the exact request through bootstrap, grant,
DONE and ACK. Independent source/CPU review and two independently built
24-case native runs accept this scoped prerequisite stage. Its per-request QP barriers are the historical prerequisite; P4.7f now supplies
the bounded host Direct operation-drain protocol described below.
One post/poll worker per context now polls separate bounded SQ and RQ CQs
(P4.7d), with nonrepeating WR namespaces and separate queue credits. Whole-engine `close(timeout)`
fences both directions, drains accepted work and explicitly destroys shared device resources; failed
teardown stays retained. Independent review accepts its CPU ownership gates and
a scoped two-host DRAM/A-H20 GPU-staging native profile. Consumer dependency and
shutdown migration remain separate from those native gates. A separate direct
host profile now passes independent CPU review and twelve two-host ordinary
DRAM/CUDA-pinned host direct, mixed/staged and A-H20 device-staging/TCP cases.
The complete native P4 stage remains open; consumer linkage resumes after its
larger stage boundary.
Native requests now follow TENT's adaptive/folded slice geometry and bounded
signaled-WR windows. Per-slice CQ identities and accepted post prefixes own
physical WR charges independently of whole-request success; failed native
teardown retains the real backing and outstanding debt. Independent source/CPU
review and fourteen healthy two-host cases from both separately built native
binaries pass exact fragment CQ/byte/post counts for DRAM, pinned-host and
device-staged transfers. This accepts the declared P4.4 profile; full P4 remains
open.
P4.5 separates persistent host registration from exact per-request DMA exclusion.
Idle cached MRs allow CPU access; final unregister retires the registration,
and failed deregistration retains the actual backing, device and budget.
Independent CPU review and two separately built fourteen-case native runs
accept the declared MR reuse and normal-close profile. Shared CQ/progress now
passes independent source/CPU review and six four-QP plus fourteen public
regression cases from each separately built native binary. Single-endpoint
RDMA regression quotas exercise immediate resource reuse. P4.7f connected reuse now has source/CPU implementation; its declared native
profile and independent acceptance are recorded in the RDMA contract. The remaining
P4 exit gates stay open. P4.8 adds automatic device/port/GID selection and a
TENT-style host/RNIC topology snapshot, with strict pins, pre-open RNIC filters
and a frozen context route. Independent source/CPU gates and the implementation's
39-case IPv6 RoCEv2 native profile pass. Its independently built native repeat
awaits new authorized nodes after the original stage terminated; P4.8 acceptance
and the complete P4 exit remain open.
P4.9 adds an explicit `RdmaProgressMode::EventDriven` mode over the same shared
CQs and data worker. One native completion-channel consumer acknowledges events
without progress/budget locks between event receipts and stays alive through
actual CQ destruction. The
default remains TENT-style polling. Source controls cover wakeups, public
READ/WRITE and connected reuse, construction failures and retained cleanup;
independent review and physical event-mode qualification remain open.
The native fragment-error gate also checks a successful prefix followed by a
real RNIC access error, with outstanding WR/MR/loan ownership through native
QP/CQ drain. Full fault qualification and P4 acceptance remain open.
See the [RDMA/CUDA contract and physical runner](docs/rdma-cuda.md).
An opt-in [OrbitKV allocation bridge](https://github.com/OrbitInfra/OrbitKV/pull/303)
retains real CUDA-pinned allocator slots. The TCP and RDMA consumer candidates
connect source grants, epochs and generation-scoped completion to actual
Manager GPU Save/Restore. Independent CUDA 13.1 source gates and an installed
CPython 3.11 client/Manager fixture now pass in their explicit H20 profiles.
The installed fixture uses one selected Torch CUDA 13.0.96 runtime and checks
cross-process IPC Publish plus TCP/same-RNIC RDMA bytes and normal ownership
retirement. See the [runtime profiles](docs/rdma-cuda.md) and
[OrbitKV #307](https://github.com/OrbitInfra/OrbitKV/pull/307).
The default OrbitKV provider remains TENT.

The source now separates `runtime/control_plane::ControlService` from parallel
`transport/tcp::TcpTransport` and `transport/rdma::RdmaTransport`; RDMA is no
longer nested in or dispatched by TCP. Native owners use TENT's `RdmaContext`,
`LocalBufferManager`, `Workers`, CQ, slice and `EndpointStore` boundaries.
P4.7c separates physical connection quota from per-request memory/byte quota,
retains terminal endpoints through detached native retirement, and keeps both
physical QP barriers. Its source/CPU and declared native regression profiles
are independently accepted; P4.7f extends them with connected reuse. See the
[architecture correspondence and drain contract](docs/rdma-cuda.md#transport-boundaries-and-connectiongrant-retirement).

P4.7d adds a private receive buffer/MR and a wire v8 source RQ completion protocol.
After payload CQ drain, the requester sends a same-QP fenced SEND_WITH_INV;
the source validates its actual receive CQE, invalidated key and full grant
binding before confirming completion. SQ and RQ credits are independent, even
at capacity one. Both physical QP-close barriers remain. Independent review
accepts the scoped source/CPU and two independently built 29-case native runs.
P4.7f adds a separate negotiated operation-drain path on top of this prerequisite.

P4.7e accepts scoped persistent source registration with exact one-use MW authority.
Local and MW-bind capabilities for the same registered view are distinct physical
MRs sharing the same cache limits; source READ and WRITE reuse the MW-bind parent.
No parent key is published. See [the source registration contract](docs/rdma-cuda.md#persistent-source-registration).

P4.7f keeps the actual `RdmaEndpoint` QP and immutable connection identity in
`EndpointStore`, while `Workers` cleans each separate grant after its sealed
completion proof. Mandatory v9 rendezvous checks both cached identities and the
last grant before new authority. Fresh MW/key/generation/WR identities, exact
RQ invalidation, bound DONE/ACK/READY and actual grant cleanup precede reuse.
Idle QPs remain charged; eviction and shutdown destroy them outside the Store
lock. The declared source/CPU and 35-case native profiles have independent
acceptance. See [connected reuse](docs/rdma-cuda.md#connected-qp-reuse).

The protocol remains OrbitTE-only. Full TENT API/wire compatibility, RDMA/GPU
coverage, GPUDirect, NVLink, SSD, installed inference serving and physical fault
qualification remain open. Accepted evidence is scoped in the completion plan;
no complete numbered parity stage is qualified.
See the [CPU TCP contract and recipe](docs/cpu-tcp.md) for behavior and limits.

## Try the source

On Linux with Rust 1.85+ and permission to bind loopback sockets:

```sh
cargo run --locked --example tcp_roundtrip
cargo run --locked --example external_host
cargo run --locked --example notifications
cargo run --locked --example async_batch
cargo test --locked
```

The example prints `18 bytes: hello from OrbitTE`. The test suite includes a
source in a separate OS process, verifies READ/WRITE bytes, restarts it on the
same address and rejects requests carrying its old source epoch.
The external-host example batch-registers ranges of a pre-existing allocation
without copying it and submits an absolute-address transfer. It does not pin pages.
The notification example verifies READ bytes and a single completion message.
The [async example and contract](docs/async-batches.md) cover allocated capacity,
append, status, seal/wait/free and local owner retention.
Data completion and notification delivery have separate outcomes; a lost
notification receipt does not authorize replaying completed data or the message.
The [frozen CPU comparison](docs/cpu-oracle.md) runs one shared byte/registration/
notification trace against unmodified native TENT in a separate test build.
It adds selected behavioral evidence while full API/wire compatibility remains open.

## Implementation plan

Current work completes OrbitTE's native RDMA stage before returning to OrbitKV
linkage. Architecture follows frozen TENT's context, registered-buffer, endpoint
and worker ownership; consumer updates wait for the larger stage's acceptance.

The target is functional parity with the frozen latest Mooncake TENT baseline
for CPU DRAM and NVIDIA GPUs. Network and memory movement form the core;
local file/SSD movement through io_uring and cuFile/GDS is optional. Other
accelerator platforms are outside the implementation scope.

- [Completion plan](docs/completion-plan.md): delivery order, dependencies and
  acceptance gates; the project's only execution queue.
- [TENT parity contract](docs/tent-parity.md): reference revision, API inventory,
  runtime behavior, transport matrix and compatibility boundaries.

## Package identity

- Project: **OrbitTE**
- Repository: <https://github.com/OrbitInfra/OrbitTE>
- Cargo package: `orbit-te`
- Rust crate: `orbit_te`

## Development

```sh
cargo fmt --check
cargo check --locked
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo publish --dry-run --locked
```

## License and provenance

Licensed under Apache-2.0. See [LICENSE](LICENSE).

[Mooncake TENT](https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent)
is the capability and architecture baseline. The Rust runtime has no Mooncake
C++ dependency. The implementation and its development protocol are original
Rust code; source correspondence is recorded in the [CPU contract](docs/cpu-tcp.md).
