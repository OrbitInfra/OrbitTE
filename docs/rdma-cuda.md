# RDMA registration and CUDA staging

P4.1-P4.7f/P5.1 add opt-in source implementations. Acceptance is scoped to the
exact native profiles below; the complete hardware and parity stages stay open.
The [completion plan](completion-plan.md) is the only execution queue. Direct
application host registration and explicit staging share a bounded, single-rail
owner. Neither establishes full B04/B01 or native TENT compatibility.

## Build and select a path

The default crate still needs no CUDA/RDMA SDK or native dependency.
`--features rdma` uses the published
[`ibverbs-sys 0.3.2+55.0`](https://docs.rs/ibverbs-sys/0.3.2+55.0/ibverbs_sys/)
release for the standard libibverbs ABI. It links directly to the system
`libibverbs.so.1`. Building needs GCC with the GNU linker, CMake, libclang and
libibverbs development/linker files; install the runtime library and the actual
RNIC provider on each RDMA host. The dependency configures and builds vendored
rdma-core while generating bindings; runtime linking still uses the system
library. No Mooncake source is used.

```sh
# Debian / Ubuntu prerequisites
sudo apt-get install g++ cmake pkg-config libclang-dev libibverbs-dev
CC=gcc CXX=g++ cargo test --locked --all-features
```

The published dependency's symbol-version configure test failed with this
host's Clang 20/LLD combination; GCC/GNU ld passes. The command and CI explicitly
select that tested C toolchain. CI fixes Ubuntu 24.04 and Clang/libclang 18
for binding generation: the moving Ubuntu 26.04 image generated invalid opaque
layouts in `ibverbs-sys` with its default parser toolchain. Build with matching
`CLANG_PATH=/usr/bin/clang-18` and `LIBCLANG_PATH=/usr/lib/llvm-18/lib` when using
that CI profile; layout assertions remain enabled. Only Linux native builds
are tested; cross compilation remains unqualified. Both CI workflows use signed
Ubuntu 24.04 official HTTPS sources through
`.github/scripts/install-native-deps.sh`. The previous Azure HTTP mirror can
stream packages too slowly to finish within the ten-minute dependency step;
that failure occurs before Rust compilation. The source set, Clang 18 pins and
full correctness gates remain explicit.

The standard post/poll inline operations dispatch through the provider function
table exposed by the generated bindings. Missing callbacks return an unsupported
error. Port queries use libibverbs' stable compatibility symbol; the engine
consumes fields from its common output prefix. CQ errors preserve their status
and vendor syndrome without reading success-only opcode/length fields.
OrbitTE carries no C bridge or build script. Engine, protocol, RC setup,
progress, admission and backing owners remain Rust.

`--features cuda` uses [`cudarc 0.19.0`](https://docs.rs/cudarc/0.19.0/cudarc/driver/sys/)
for generated Driver API types and ABI signatures, and `libloading 0.8` for
fallible loading of the 16 required versioned entry points from `libcuda.so.1`.
The signatures are inferred from cudarc's generated fields. Its eager loader
is deliberately not called: a missing unrelated CUDA symbol must not panic an
engine that only needs allocation and copy. The library is retained through
all native owners and failed cleanup. The dependency is pinned to preserve
Rust 1.85; newer cudarc releases using libloading 0.9 need Rust 1.88. The
`cuda-12000` binding selection is a compile-time API baseline, not a requirement
to install a CUDA toolkit. No libcudart, nvcc, cuBLAS or NVRTC is needed.
Unsupported platforms/drivers remain unavailable rather than selecting CPU access.

OrbitTE owns fallible QP/MR/context destruction and DMA quiescence explicitly.
The higher-level wrappers' automatic `Drop` behavior does not supply our
failure-retention contract. Reusing bindings does not establish GPUDirect,
NVLink, device MR support or a new completed parity row.

Set `EngineConfig.rdma = Some(RdmaConfig::new("mlx5_0"))` and explicitly choose
the port/GID index, traffic class, hop limit and endpoint/direct/staging budgets.
`RdmaConfig.host_memory` defaults to `RdmaHostMemory::Direct`: actual DRAM and
CUDA-pinned host ranges use actual application backing. Requesters reuse a local-only MR for the full registered view; P4.7e source
grants reuse a separate MW-bind parent for the full view and bind only the exact
requested range. The source cache has scoped independent source/CPU/native acceptance. Set it to `Staged`
for private host copies. `max_direct_bytes` and `max_staging_bytes` separately
bound each engine's charged native bytes (64 MiB each by default); both share
`max_endpoints`. A failed host MR registration is an error, never a staging
fallback. This policy selects host backing only: `CudaMemory` is explicitly
staged in either mode, because direct GPU MR/GDR is not implemented. An
Ethernet GID must be nonzero; select a routable GID/type from the real device.
There is no automatic device, rail or topology discovery/selection yet.
Each `TransferRequest` now includes `transport: Transport::Tcp` or `Rdma`.
An uncompiled/unconfigured RDMA request fails preflight, with no fallback.
The peer also rejects unavailable RDMA before exposing a descriptor.

`CudaMemory::allocate` owns device memory and its primary-context reference.
Unsafe `CudaMemory::from_raw_parts` retains the supplied actual allocation AND
context owner, probes device type/context/ordinal/allocation extent, and rejects
host/managed pointers. All aliases must share one `Arc<CudaMemory>`.
`register_cuda_memory_batch` uses the same transactional range, duplicate,
permission and retired-owner accounting as host registration.
`cuda::allocate_pinned_host` returns a `HostMemory` backed by a real CUDA-pinned
allocation and its deallocator/context; ordinary host registration does not pin.

Before handing off an external GPU range, the caller must finish producers and
stop conflicting external accesses. After completion, the caller must hand off
ready bytes to consumers through its own stream/lifecycle contract. These APIs
do not infer arbitrary framework readiness or generation leases.

## Actual data paths and completion

```text
TCP:  CPU/GPU -> owned host bytes -> socket -> owned host bytes -> CPU/GPU
RDMA host direct: actual application host range <-> RC RDMA READ/WRITE <-> peer MR
RDMA staging: CPU/GPU -> owned host MR <-> RC RDMA READ/WRITE <-> owned host MR -> CPU/GPU
CUDA copy: device <-> private CUDA-pinned staging <-> caller host slice
```

P4.2b lazily opens one RNIC context and protection domain (PD) per engine.
Overlapping and successive requests retain that actual shared owner; separate
engines do not share PDs. P4.6 introduced shared progress. P4.7d gives that context separate shared SQ
and RQ CQs, polled by one post/poll worker. Each grant owns an exact direct range
lease or stable staging and its one-use source MW. P4.7e parent MRs outlive
grants. P4.7f retains the real connected QP/PSN/incarnation when both peers
negotiate host Direct and finish the complete operation-drain protocol below.
The first profile fixes role and READ/WRITE capability, with one active grant
per QP. Staged or device-backed peers still use per-request QPs and both
physical close barriers. Parent MRs have LOCAL_WRITE|MW_BIND only; requester
MRs have LOCAL_WRITE only. Neither parent key is exported. The shared PD is
not a source-grant authorization boundary. Port state, MTU, LID and selected GID
are re-queried for each new physical endpoint; retaining a QP does not rediscover
a changed route or add port recovery.

This follows the frozen TENT `e2d6e8e0` hierarchy in
`tent/src/transport/rdma/context.cpp`, `buffers.cpp` and `endpoint.cpp`: context/PD
outlive their endpoints and memory registrations. `LocalBufferManager` passes
application buffer addresses and lengths to each context's `registerMemReg`;
P4.3 establishes that direct host path with stricter exact-request MR extents
and retained failure owners. It does not copy TENT's removal behavior that can
erase an owner after failed deregistration. P4.5 follows TENT's independent
registered-buffer owner and separates local MR lifetime from endpoint lifetime.
P4.6 follows context-owned `cq.cpp` and worker-owned `workers.cpp` progress,
including quota reservation and retaining a completion owner through the end
of its handler. P4.7f adds the [scoped connected reuse](#connected-qp-reuse)
described below. Resource reuse alone is no performance result.

The persistent cache uses actual `RegisteredMemory` identity within one PD,
registers the entire registered view with `IBV_ACCESS_LOCAL_WRITE` for a requester
or `LOCAL_WRITE|MW_BIND` for a source, and retains its real allocation and device.
Capability is part of the cache key: one view used in both roles needs two actual
MRs, with two full-view byte charges. A local-only MR is never silently upgraded. `max_cached_mrs` and
`max_cached_mr_bytes` bound physical count and full-view bytes (defaults 64 and
64 MiB), independently of exact active DMA bytes. Creating, closing and retained
MRs remain charged. Idle entries are evicted in least-recently-used order;
borrowed or creating entries cannot be evicted. A view larger than the byte
budget or a cache full of busy owners returns `ResourceExhausted`; registration
errors never trigger staging. Concurrent callers share one in-progress
registration. Waiting for another creator uses the existing request deadline;
a timed-out waiter returns only its own reservation and exact DMA lease.
Native register/deregister executes outside the cache lock.

An idle MR holds no DMA exclusion lease, so normal CPU access stays available.
Each request independently retains its exact allocation-relative range through
all slices and successful native teardown. Physical QP destruction or the negotiated complete operation-drain receipt
precedes return of its cached-MR borrow. CPU exclusion ends only after the
corresponding native grant cleanup succeeds. Posting validates the request-local base and length inside the
larger MR, and constructs local SGEs relative to that request base. Remote
addresses remain relative to the source grant.

Final explicit unregister removes lookup visibility and marks the identity
inactive before retiring its cached MR. Duplicate registration references keep
it active until the final unregister. Already resolved work may finish with its
retained identity; an in-progress or borrowed MR becomes retiring and cannot
return to idle residency. A new registration at the same address has a distinct
identity. Idle retirement performs native deregistration immediately, so
`unregister_memory` can return `Unquiesced` after visibility was already removed.
That error does not restore visibility or authorize a second unregister to retry
native cleanup. Failed deregistration retains the real MR/backing/device and
full physical charge, poisons further RDMA admission, and is never retried by
close or Drop.

Source parent MRs deliberately have no remote read/write permission. Their
one-use Type-2B MWs grant only the exact requested range and direction. The
provider must advertise Type-2B MW support; unsupported allocation is an error,
never a fallback to a remote-enabled parent MR. A requester cached MR remains
local-only. The actual PD now owns a bounded, non-evicting full-rkey burn domain,
and both endpoint incarnations are explicit in the public protocol. These are
prerequisites for connected reuse; QP/grant owner separation and a physical
quiescence contract are still required before enabling it.

Canonical `HostMemory` owns raw stable backing and a range-access authority.
Direct leases use allocation-relative ranges and retain the exact
`RegisteredMemory`, its allocation and registration charge. Overlapping CPU
copies or DMA leases return unavailable across aliases and engines; disjoint
ranges may proceed. CPU copies form no allocation-wide Rust slice across DMA.
Unregister hides lookup but cannot release admitted backing. Native teardown
must succeed before the range is unlocked; failed QP/MR/CQ teardown retains
that real lease. External callers still coordinate access outside this owner.

Source admission, authoritative epoch, registration bounds and permissions are
checked before bootstrap. With staging, READ snapshots source bytes and WRITE
preloads requester staging. With direct host MRs, the RNIC accesses the actual
leased application range without those copies. Either peer can independently
select host direct or staging. Device pointers always use declared staging.
Protocol v9 rendezvous precedes the QP-only descriptors with zero memory address/key, plus
identities owned by each process, actual PD and connection. Every phase binds
the exact request, both identities and an independent source grant generation.
Both RC QPs connect; the source posts BIND_MW and consumes its exact successful
SQ CQE. It then registers a distinct local-only 141-byte receive buffer and
waits for native RQ acceptance before publishing the MW grant. GRANT does not
wait for a receive CQE, which requires the requester to have received the grant.
The requester validates the full binding and QP/PSN/GID, then submits bounded
signaled READ/WRITE slices. After all payload CQEs pass, it posts a signaled,
fenced SEND_WITH_INV on that same QP using a distinct local-only message MR.
The source's actual RQ CQE must match its own CQ, WR namespace, QPN, RECV opcode,
141-byte length, exactly WITH_INV, host-order invalidated rkey and full binding.
A requester SEND CQE or TCP DONE alone cannot prove source completion.

For negotiated host Direct, the requester consumes its SEND CQE, closes its
local grant/message MR and loan, and sends bound DONE. The source requires
both DONE and the actual validated RQ receipt, then deallocates its invalidated
MW, returns the parent borrow, closes the message MR and releases the exact
loan/grant before ACK. ACK is written before source READY publication; a final
bound READY frame lets the requester confirm publication before caching its
half. No old grant, preposted RQ or completion debt remains on an idle QP.
For either staged peer, both physical QP-close barriers remain. WRITE staging
commits and drops its output before ACK; READ staging copies back after ACK.
Idle active parent MRs remain registered independently of the connection, until
unregister, eviction or close. Historical LOCAL_INV and parent-key probes remain
separate regression controls. See [the complete reuse scope](#connected-qp-reuse).

P4.4 follows frozen TENT `slice.h::planRdmaSlices`,
`endpoint.cpp::submitSlices` and `workers.cpp` completion ownership. The base
`RdmaConfig.block_size` defaults to 64 KiB. Requests adapt to at most 64 slices
for host-local READ or 32 for WRITE/device-local READ; CUDA-pinned host memory
counts as host. A final tail shorter than one quarter of the chosen block folds
into the preceding slice. The entire requested range remains leased even after
a window's CQEs have arrived. Each request keeps an exclusive QP window within
the shared CQ.

`max_send_wr` defaults to 16 and accepts 1..64 per QP. `max_cqes` defaults to
1024 and accepts 1..4096 separately for each shared SQ and RQ CQ across all
attached request QPs; `max_endpoints` accepts
1..4096. The provider must return the requested CQ and QP capacities. The worker
reserves the minimum of remaining slices, QP window and currently free CQ
slots before dispatch. A full CQ backpressures other requests without losing
owners; completing one window allows the next. Round-robin dispatch avoids a
fixed first-QP preference. Every data submission, BIND, SEND and receive burns a fresh 128-ID namespace
within the worker/actual-CQ lifetime, including successive submissions on the
same physical QP. Zero is unassigned; checked allocation rejects exhaustion
before native posting or namespace publication. Slot identities are bounded
within that namespace. Completion routing validates the actual SQ/RQ CQ,
namespace, expected WR and native QPN. A prefix from an earlier operation cannot
pay current queue debt. These namespaces cannot be reset while live QPs remain;
worker/CQ replacement with live connections is not implemented.
RDMA READ credit stays at one; SQ/CQ capacity is not a throughput claim. Window
metadata remains bounded to at most 64 slices. Port/rail discovery, event progress and multirail remain open. Connected host
Direct reuse is scoped below.

Before a post dispatch, all window slots become charged. A known `bad_wr`
identity proves only an accepted prefix and an unposted suffix. A null/foreign
pointer on post failure leaves the whole window potentially posted; the engine
never dereferences or subtracts that pointer. Post failure stops the request
and sends no DONE. Unknown/duplicate CQ identity, wrong success opcode/READ
length, CQ errors and deadline expiry also stop progress. A logical failure
clears no unproven native debt. An ambiguous/partially accepted post, invalid
completion, CQ error or deadline with pending WRs poisons the entire shared
lane. New posts and registrations stop. Every attached record is fenced under
the same mutex used to create/connect a QP, all QPs are destroyed, then both CQs
are destroyed before either SQ or RQ debt is cleared. Only then are remaining
MRs/borrows and direct loans released. Failed teardown retains actual queue
metadata, allocation, registration and charges. TENT's pre-destroy cancellation of quota is not a release proof here.

`completed_reads`/`completed_writes` count successful slice CQEs;
`completed_read_bytes`/`completed_write_bytes` count their exact payload bytes.
`posted_work_requests` counts the proven accepted prefix, `post_calls` counts
verbs-post dispatch calls, and `partial_posts`/`uncertain_posts` distinguish
error receipts. `work_requests` counts reserved or possibly posted slots;
`retained_work_requests` survives failed teardown, and `peak_work_requests`
records the runtime high-water charge. A completed CQE can release its WR slot,
while the complete request's DMA lease remains until native close. Application
batch completion/bytes still require the whole request and final source receipt;
physical CQ progress alone cannot publish or roll back application data.
Running cancellation remains logical and does not interrupt accepted windows.

Wire v9 explicitly rejects v1-v8; it is OrbitTE-only. A WRITE attempt is marked
`RemoteEffect::Unknown` before posting regardless of requester memory policy:
the peer may expose direct host memory. DONE/ACK are completion receipts, not
an atomic commit boundary for direct transfers. Failed direct WRITE may have
changed remote bytes without DONE. Failed direct READ may have changed its local
target before a lost/failed ACK. Successful native destruction proves local
quiescence, not transactional rollback or remote source drain. A failed report
must not authorize replay. Staged source WRITE still requires DONE to copy into
application backing, but the requester does not assume that peer policy.

CUDA copies serialize per allocation, push its retained context, use private
pinned staging, synchronize, and pop/verify the context even on copy errors.
Caller host slices are never submitted to CUDA. A normal copy error followed by
successful synchronization returns an error without publishing partial READ
host-slice bytes. A failed H2D/registered-GPU commit can have changed GPU bytes
even when synchronization proves quiescence; a failure does not establish rollback
or authorize consumers to publish those bytes. Synchronization/restoration ambiguity retains device owner, pinned
staging and context. Failed native frees retain resources and their driver.

## Failure, drain and resource ownership

Endpoint count, registered direct host bytes and staging bytes stay bounded
per engine. A negotiated host Direct success cleans only its grant and retains a READY
QP. A staged success or any failed operation retires its QP and active grant
while the shared CQ remains open; unresolved debt uses the whole-lane barrier. Under the request mutex, registry membership is removed before Closed
is published. The close receipt then moves drained resources/output into the
caller endpoint, including on a logical transfer error. A worker snapshot keeps
only metadata; dropping the endpoint immediately returns its byte/count charge
even if that snapshot remains alive. Output stays stable until that drop.
A lane fault uses the all-QP/CQ barrier above. Cleanup stops on the first
native error: the entire remaining lane, its actual owners and unresolved
charges stay retained without retry. Later RDMA admission is rejected. A fatal
lane remains terminal even after successful cleanup; automatic recovery or CQ
replacement is not implemented. Even dropping the engine cannot destroy a context/PD still retained
by an endpoint.

`TransferEngine::close(timeout)` locks source then requester admission, fences
both before waiting, and gives their accepted operations one shared drain
deadline. Already accepted work can still create its endpoints during this drain;
closing admission does not cancel it. Once both owners are drained, the runtime
permanently closes device and claim admission, detaches and retires idle QPs,
allows accepted constructors/grants to finish or retire, waits for actual
connection/grant charges to reach zero, then drains cached host MRs, closes both shared CQs and joins its progress worker,
then explicitly destroys PD -> context. Waiting for another MR creator or
closer uses the same absolute deadline. A request's close wait also uses its
original absolute deadline; expiry before a physical receipt returns
`Unquiesced`, including when another native call holds the shared worker. The
lane continues to own and clean the actual record. A retained receipt waits
until all accepted constructors are fenced and actual remaining owners/charges
are recorded, using that same
deadline. Requester and source always consume the idempotent close receipt
before claiming drain. Timeout preserves the actual owner,
its charge and the PD; a later close can continue waiting.
Concurrent/repeated callers serialize native destruction. A timeout leaves both
admissions closed and the device live; another call can continue waiting.
Native teardown failure retains the actual resources, remains charged, returns
`Unquiesced` and cannot be retried by another close caller or Drop. Successful
close does not unregister caller-held memory or free allocated batch handles;
those owners retain their backing/capacity until explicit unregister/free/drop.

`close_source` and `close_requester` preserve their independent contracts: either
one leaves the other direction and shared device available. Neither alone nor
the pair supplies an explicit PD/context teardown receipt. Use whole-engine
`close` before releasing an owner's native-resource budget. Drop can attempt
best-effort device teardown after the last endpoint disappears, but cannot
report failure; it preserves the same retention rule. The pinned OrbitKV
consumer still uses the earlier per-slice implementation; its pin and explicit
shutdown-receipt migration require a separate consumer gate.

`RdmaStats` reports active/retained/opened CQ counts, live progress workers,
permanent lane failures and peak simultaneously pending QPs, alongside shared
device counts. An idle open engine can have one active device and zero endpoints;
whole-engine close returns active devices to zero on success. Existing retained
endpoint/byte and successful requester READ/WRITE CQ counts remain independent
of application commit/notification results. `direct_bytes` remains charged
through endpoint-owner release (or forever on unquiesced retention);
`opened_direct_endpoints` counts successfully constructed direct host endpoints.
Staging-only or device-staged peers report zero direct opens.
`persistent_mrs`/`persistent_mr_bytes` count both cached local-only and MW-bind
physical owners, including creation/closing/retention; `retained_persistent_mrs`
records failed deregistration. Successful creation, cache reuse and idle eviction
have separate cumulative counters. `persistent_source_mrs`, `opened_source_mrs`
and `reused_source_mrs` report the MW-bind subset. `retained_source_mrs` counts
parents retained by failed deregistration; a failed MW deallocation still keeps
its parent borrow charged in `persistent_source_mrs` without attempting deregistration.
Private staging and completion-message MRs are outside these cache counters. Successful
whole-engine close requires zero remaining persistent MRs before PD release.

`Error::Unquiesced` means a backend could not prove termination/teardown. A
synchronous report or async task/group can be logically failed with
`locally_drained == false`. Requester registrations/admission, and async batch
capacity/engine references, stay retained even after handle drop. Explicit free
returns `BatchBusy`; requester close times out. A source with unquiesced work
similarly retains its actual registered owner and source permit, so source close
cannot report a false drain. Ordinary remote backend errors do not assert local
physical ambiguity; the remote engine owns that retention.

Retention is conservative protection, not implemented recovery. There is no
reclaim proof/API for quarantined operations yet; process-scoped retained CUDA
resources are observable through `cuda::retained_resources`. A context with
failed restore/unpin is poisoned. GPU/RNIC loss, hardware abort/drain and physical
concurrent shutdown/reuse qualification remain open. Synchronous native calls/teardown
can exceed an API deadline; no hard real-time or provider interrupt is claimed.

## Verification and physical execution

P4.3 CPU controls verify owned/external canonical ranges across two engines,
exact MR addresses/lengths, independent direct/staging quotas, all mixed host
policies over real control sockets, post-failure direct WRITE ambiguity, failed
direct READ receipts and retained source leases. Running cancel, wait timeout,
unregister, detach and engine drop preserve the real direct loan until worker
exit. QP/MR/CQ and partial-construction failure controls keep the actual owner
and registration charge. These are CPU contract controls, not DMA receipts.

The P4.2b CPU controls exercise overlapping/consecutive slice reuse, device
lifetime after runtime drop, partial device construction, every slice teardown
failure retaining its device, PD/context cleanup failure, concurrent exactly-once
close, directional close, whole-close admission fencing, drain timeout and a
previously accepted operation opening its slice after the fence. The physical
probe uses whole-engine close and requires exactly one device open per RDMA
peer, zero shared devices after shutdown and all existing byte/guard/CQ checks.
The scoped native qualification below applies to these new code paths. Earlier
consumer profiles pinned to `02d64e55` remain separate and do not qualify a
consumer dependency or shutdown migration.

CPU-only ABI shims cover verbs descriptor layout, malformed fields, construction,
post/poll/CQ errors, deadline and every teardown boundary; they assert exact
retained budgets and admission rejection. CUDA shims cover pointer extent/type,
copy/sync/restore/unpin, private staging, whole-buffer guards, shared registration,
TCP directions and retained source/requester/batch ownership. They do not use
CUDA stub libraries or count as GPU/RNIC qualification.

```sh
cargo fmt --check
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked --all-features
cargo build --locked --release --all-features --example hardware_peer
```

### Shared device owner qualification

At `1782771` in [OrbitTE #14](https://github.com/OrbitInfra/OrbitTE/pull/14),
independent source/CPU review and six healthy native cases pass: two physical
RoCE v2 hosts exchange DRAM READ/WRITE in both roles; A's H20 exchanges CUDA
staging bytes with B's DRAM through RDMA and TCP controls. Complete buffers,
noncontiguous ranges, untouched bytes and guards are compared. Each RDMA peer
opens one shared device across its slices, then whole-engine close returns
active/retained device, endpoint and staging charges to zero.

The explicit GPU Driver API profile maps compatibility `libcuda590.48.01` on
kernel driver `535.161.08`, with no CUDART or TENT mapped. B uses CPU memory only.
Actual mapped libraries are frozen before the accepted GPU run and their
identities/bytes are unchanged after exit. Independent native peers, supervisors,
SSH starters and controller finish normally; original RNIC/A-GPU lock inodes
are reacquired/released and A GPU7 returns to zero memory with no compute apps.
This is native-engine healthy-path qualification, not an OrbitKV Manager,
installed wheel, serving, direct GPU MR/GDR, physical fault or performance result.

Evidence, source/binary manifests, failed profiles and checker controls remain
outside the checkout at `/root/orbitte-artifacts/rdma-context-20261022/`;
`independent-hardware-1782771/independent-review.json` records the accepted scope.
[The eight-job CI matrix](https://github.com/OrbitInfra/OrbitTE/actions/runs/39748938048)
and [frozen native TENT CPU comparison](https://github.com/OrbitInfra/OrbitTE/actions/runs/39748938003)
pass for that source. The latter compares the CPU contract, not native RDMA
wire interoperability.

### Direct host MR qualification

Source `09fa949` in [OrbitTE #15](https://github.com/OrbitInfra/OrbitTE/pull/15)
passes independent source/CPU review and twelve healthy native cases on the
same two RoCE v2 hosts. Ordinary DRAM uses exact direct MRs in both roles;
explicit staged and both mixed host policies pass separately. A's H20 also
exchanges CUDA-pinned host direct bytes and device-staged bytes with B's DRAM
in both roles, with CPU/GPU TCP controls. Every peer compares its complete
buffer, noncontiguous request ranges, untouched bytes and guards. Each direct
host RDMA peer constructs six direct endpoints; device-staged and explicit
staging peers construct zero. RDMA requesters observe three READ and three WRITE
CQ successes, and all direct/staging/device/endpoint charges return to zero.

The independent MSRV-built native binary differs from the implementation build
and reruns all twelve cases against independently frozen inputs. All 48 native
and supervisor PIDs are absent after each twelve-case run; 24 SSH starters and the controller
return zero through actual waits. The original two RNIC locks and A GPU7 lock
are reacquired/released, and A GPU7 returns to zero memory with no compute apps.
All mapped libraries match their preflight path/inode/SHA profile. A maps
compatibility `libcuda590.48.01` on kernel driver `535.161.08`; no CUDART or TENT
is mapped, and B does not initialize CUDA. `dc77a5b` only strengthens the CPU
assertion that successful close unlocks access before endpoint-owner drop;
production and native harness bytes are unchanged from the hardware source.

Evidence and frozen binaries stay outside the source checkout at
`/root/orbitte-artifacts/rdma-direct-host-20261022/`:
`implementation-hardware-review.json`,
`independent-source-09fa949/independent-review.json` and
`independent-hardware-09fa949/independent-review.json` record the scope.
The implementation archive is `implementation-evidence-09fa949.tar.gz`, SHA-256
`10822ee6c9d353563308056347a9d56235ebb6572e2afe53ce4f0e5775784e8c`.
The [eight-job CI matrix](https://github.com/OrbitInfra/OrbitTE/actions/runs/39757218570)
and [unmodified frozen TENT CPU comparison](https://github.com/OrbitInfra/OrbitTE/actions/runs/39757218335)
pass for the source plus test assertion. The native comparison covers selected
CPU API behavior, not RDMA wire interoperability. This profile does not qualify
persistent MR/endpoint caches, multirail, direct GPU MR/GDR, physical RNIC faults,
installed OrbitKV, performance or complete P4/TENT parity.

### Bounded slice and WR progress qualification

Source `e04e381` in [OrbitTE #16](https://github.com/OrbitInfra/OrbitTE/pull/16)
passes independent source/CPU review and fourteen healthy two-host cases from
each of the implementation and independently MSRV-built binaries. The native profile covers
single-rail RoCE v2 with window limits 1/2/3/4/8, explicit small and adaptive
blocks, folded short tails, ordinary/pinned host direct MRs, both mixed host
policies, explicit staging, A-H20 device staging in both roles and TCP controls.
Each requester's slice CQ counts, post calls and high-water WR charges are
checked against independently specified values by the external postflight gate,
alongside exact byte comparisons. A 12,305-byte
request folds its 17-byte tail into three slices; a large host-local READ uses
64 slices while WRITE/device-local READ caps at 32. Complete buffers,
noncontiguous ranges, untouched bytes, guards, notifications and final normal
resource release pass. Other window capacities remain physically unqualified.
These runs do not measure performance.

CPU controls cover accepted post prefixes and unknown/malformed `bad_wr`,
including null, foreign, one-past and interior pointers. Reordered, duplicate,
previous-window and malformed CQ entries cannot invent completion or repay
unidentified WR debt. Error CQ fields, timeout, failed QP/MR/CQ teardown and
multi-window cancel/detach keep actual owners until the required close boundary.
The WR/SGE chain uses stable raw bases without reborrowing the vector storage;
the independent source review identified this pointer contract before any
accepted hardware run. These controls do not inject real RNIC/GPU faults.

Each fourteen-case run waits for all 28 SSH starters and its controller to return
zero, then verifies all 56 native/supervisor PIDs are absent.
The original two RNIC locks and A GPU7 lock are reacquired/released; A GPU7
returns to zero memory with no compute apps. Actual mapped libraries match the
frozen path/inode/SHA profile. A maps compatibility `libcuda590.48.01` on kernel
driver `535.161.08`; no CUDART or TENT is mapped, and B uses CPU memory only.

Evidence and frozen source/binaries stay outside the checkout at
`/root/orbitte-artifacts/rdma-slices-20261022/`.
`implementation-hardware-review.json`,
`independent-source-e04e381/independent-review.json` and
`independent-hardware-e04e381/independent-review.json` record the accepted scope.
`implementation-evidence-e04e381.tar.gz` has SHA-256
`f77bbe0099d500322ddbf5d94e7848a2e89c169f8c29fe1cfc485bfe49cd4e8f`;
`independent-hardware-e04e381-evidence.tar.gz` has SHA-256
`b353de06a633b51b1a5b8e429a849ccc5ea20e694a4b5265072f5bbd5f0e2c59`.
The [PR checks](https://github.com/OrbitInfra/OrbitTE/pull/16/checks) run the eight-job
Rust/feature matrix and unmodified frozen TENT CPU comparison. That comparison
covers selected CPU behavior, not native RDMA wire interoperability. Persistent
MR/endpoint stores, shared CQ/worker progress, event progress, backend abort,
multirail, direct GPU MR/GDR, native hardware faults, installed OrbitKV and
complete P4/TENT parity remain open. OrbitKV migration stays deferred to P11.

### Persistent local host MR qualification

Source `2c4aa27` in [OrbitTE #17](https://github.com/OrbitInfra/OrbitTE/pull/17)
passes independent source/CPU review and fourteen healthy two-host cases from
each of two separately compiled binaries. Stable and Rust 1.85 all-feature
Clippy/tests pass: 80 unit controls, 3 notification tests, 5 TCP tests and one
doctest. Default/RDMA-only gates, supervision controls and the maintained CPU
trace/negative comparator controls pass separately.

The native profile keeps the preceding slice/window cases, RoCE v2 hosts and
library constraints. Eight direct host requester cases each construct exactly
one local-only MR, reuse it five times and retain one idle MR charged for the
entire registered view, including guards (up to 32 MiB + 128 bytes). The
requester verifies/changes CPU bytes between READ and WRITE while that MR is
idle. Source-grant, staged-host, device-staged and TCP peers report zero cached
MRs. Full noncontiguous buffers/guards, notifications and exact CQ/byte/post
counts pass. Whole-engine close returns persistent MR count/bytes and device
count to zero before backing is released. This qualifies healthy cache reuse
and normal whole-engine close, not physical cache eviction, unregister races
or failed deregistration; those ownership contracts currently have CPU controls.

Every fourteen-case run waits for its controller and 28 SSH starters to return
zero, verifies all 56 owned native/supervisor PIDs are absent, reacquires/releases
the same three original lock inodes, and checks A GPU7 has zero memory and no
compute apps. B uses CPU memory only and no B GPU is selected or queried.
A's 19/B's 15 frozen libraries stay unchanged; actual mapped-library identities
are verified separately from overlay file-stat identities. A maps compatibility
Driver API `590.48.01` on kernel driver `535.161.08`; no CUDART or TENT is mapped.

Evidence stays outside the checkout at
`/root/orbitte-artifacts/rdma-persistent-20261022/`:
`implementation-hardware-review.json`,
`independent-source-2c4aa27/independent-review.json` and
`independent-hardware-2c4aa27/verification.json` record the exact source,
binaries, frozen inputs, raw role records and acceptance checks. The independent
acceptance receipt is `independent-hardware-2c4aa27/independent-review.json`.
The implementation evidence archive is `implementation-evidence-2c4aa27.tar.gz`,
SHA-256 `735bd02bf909550c247cd836da5d80b18ffb4cd780613c9b0b8fa2c911ebefd7`.
The independent MSRV binary SHA-256 is
`5afb95fd70740c91b63bdf38a90f82442d7e1034452004f2ff3dc7f84626faa2`.
The [PR checks](https://github.com/OrbitInfra/OrbitTE/pull/17/checks) cover the
stable/MSRV feature matrix and unmodified frozen TENT CPU comparison; that
comparison covers selected CPU behavior, not native wire interoperability.
At that revision, source remote MR reuse/revocation, endpoint/QP/CQ pooling,
shared worker/event progress, backend abort, multirail, GPU MR/GDR, real hardware
faults, performance, installed OrbitKV and complete P4/TENT parity remained
open. P4.6 below adds shared CQ and post/poll progress.

### Shared CQ and progress qualification

P4.6 implements the single-context CQ and single post/poll owner. CPU controls
cover simultaneous QPs at aggregate capacity 1/3, independent SQ limits,
out-of-order routing, never-reused request identities, wrong-QP/unknown CQEs,
whole-lane retained ownership and debt before the native close barrier. They
also pause an accepted record before QP creation during abort/retention, block
one native dispatch while another request's close deadline expires, and check
that a retained requester cannot report `locally_drained` after a logical
transfer error. Closed resource handoff and retained accounting receipts have
deterministic controls; a retained metadata snapshot must not defer caller
quota release. A separate ACK gate requires source endpoint/byte quota to be
available for immediate reuse before the final ACK is written. Single-CPU
concurrent reruns exercise those boundaries. These are ownership controls, not
physical fault receipts.

The explicit ignored test
`transport::rdma::workers::tests::hardware::shared_cq_peer` constructs four actual RC QPs per
phase and performs READ then WRITE between two physical hosts. A test-only gate
pauses the first native dispatch until the other requests have queued. It
never synthesizes WRs, CQEs or bytes; the provider executes all posts/polls.
CQ capacities 1/3 and SQ windows 1/2 prove aggregate backpressure and overlapping
native QPs. Full buffers/guards, exact read/write bytes, one CQ/worker, one
requester MR open/seven reuses and complete normal shutdown are required.
Run with the `orbitte-shared-cq` hardware-controller profile and the release
unit-test executable. It is a native-owner component gate; its test bootstrap
does not claim public wire compatibility. The existing fourteen-case public
runner remains the DRAM/pinned/GPU-staging/TCP regression gate.

Source `8e38447` in [OrbitTE #18](https://github.com/OrbitInfra/OrbitTE/pull/18)
passes independent stable/Rust 1.85 source/CPU review: strict Clippy, 90 unit
controls, 3 notification tests, 5 TCP tests and one doctest. Each separately
built release unit binary passes 30 complete single-CPU runs with 16 test
threads. The independent reviewer also rejects 35 runs of six mutations that
restore the old constructor fence, premature retention receipt, skipped
requester close,
delayed resource handoff, incomplete retention accounting or ACK-before-drop.

Each separately compiled implementation/stable and independent/MSRV native
binary passes six healthy two-host four-QP cases in both
directions, with CQ capacities 1/3 and SQ windows 1/2. Peak WR and outstanding-QP
counts establish bounded completion debt, not simultaneous physical DMA or
throughput. Fourteen public DRAM/pinned-host/device-staged/TCP regressions pass
with exact slice CQ/byte/post and MR reuse counts. Every RDMA regression uses
`rdma_max_endpoints=1` to exercise immediate native quota reuse. Independent
review accepts this healthy single-rail profile at the exact runtime source.

In each native run, both controllers and all 40 SSH starters return zero. Postflight verifies all
80 owned native/supervisor PIDs are absent, reacquires/releases the three
original lock inodes, and checks A GPU7 has zero memory and no compute apps.
A's 19/B's 16 frozen libraries remain unchanged; actual mapped-library identities
are checked separately from overlay file-stat identities. A maps compatibility
Driver API `590.48.01` on kernel driver `535.161.08`; no CUDART or TENT is mapped.
B uses CPU memory only; no B GPU is selected or queried.

Evidence stays outside the checkout under
`/root/orbitte-artifacts/rdma-endpoints-20261022/`.
`independent-source-8e38447/independent-review.json` records source/CPU acceptance;
`final-8e38447/implementation-hardware-review.json` records the implementation
native receipt. `independent-hardware-8e38447/verification.json` records the
independent raw/postflight checks; its `independent-review.json` is the final
acceptance receipt. `final-8e38447/implementation-evidence-8e38447.tar.gz` has SHA-256
`76b3cb8ba97b64fe4e9d020cb18cfe0f302d50e48cff0a3b0ec23b89ed0ec793`.
The independent source/CPU/native archive `independent-evidence-8e38447.tar.gz`
has SHA-256
`6b8ff04806ceafc567fabbfbf82201c6a5a79623c1b2d728fdbb6545cbddca48`.
Earlier CI/quota/ACK failures and the independent native run rejected for an
unfrozen B library remain preserved; they do not qualify this source.
The [PR checks](https://github.com/OrbitInfra/OrbitTE/pull/18/checks) cover the
stable/MSRV feature matrix and unmodified frozen TENT CPU comparison.

Connected endpoint reuse, source remote-rkey reuse/revocation,
multirail/discovery, event progress, actual native failures and full P4 remain
open. Endpoint reuse must establish remote authorization revocation and both
endpoint incarnations before replacing the per-request QP/MR authority
boundary. OrbitKV linkage remains deferred to the accepted complete P4 exit.

### One-use source memory window qualification

P4.7a is independently accepted for the source/CPU and declared native profiles
at `fc9717f63066137a01f0a13d08934d2685fb43ff`. This closes the one-use source
MW prerequisite of connected endpoint reuse. QPs and parent MRs still close for
each request. One MW binds once, so this profile does not
rotate a key on a persistent QP or establish a reusable key namespace.

The layering follows frozen TENT's context/PD, registered buffers, context CQ
and worker-owned completions. The MW boundary is a deliberate stricter source
authority contract, rather than a claim that frozen TENT uses this handshake.
The selected `ibverbs-sys` vendored rdma-core v55 manpages require a successful
BIND CQE before publishing a key over a different channel; deallocating a MW
unbinds it, and a bound MW prevents parent MR deregistration. Upstream's
[parent MR and Type-2 tests](https://github.com/linux-rdma/rdma-core/blob/a758c7ba0d5e49fef3a6b62b153b3ffd29a7f7f3/tests/test_mr.py#L256)
exercise LOCAL_WRITE|MW_BIND without parent remote flags and invalidated-key
remote-access rejection.

BIND/INV reserve the same physical SQ/CQ budget as data, use distinct never-
recycled WR slots, validate QP identity/status/opcode and count no payload bytes.
A grant can be exported only while its successful bind is live. INV completion
revokes a key; it does not independently prove completion of arbitrary incoming
DMA. The normal path still requires requester data completions, requester QP
close and DONE, followed by INV completion and source QP/MW/MR cleanup. EOF,
timeout or unknown BIND/INV post/completion uses the real QP/CQ barrier before
releasing a loan. Failed cleanup retains actual owners/charges without retry.

The context owns a separate nonblocking asynchronous event consumer through
all affiliated QP/CQ destruction. It publishes a terminal atomic fence before
ACK without acquiring a request/device lock; after ACK it never dereferences
the event's native object. QP/CQ errors, port/topology/device changes and unknown
events fence the shared lane, including PORT_ACTIVE. Only COMM_EST and
SQ_DRAINED are informational; they never clear an existing failure. Lost event consumption
retains actual children and loans rather than attempting a destroy that may
wait forever for an unacknowledged event. Only after all QPs/CQs are closed does
the context stop/join this consumer and close the PD/context. This is scoped
fatal-event handling, not topology recovery or event-driven CQ progress.

`active_windows` includes every allocated native MW until successful deallocation;
`retained_windows` counts remaining actual MWs after failed cleanup. Successful
bind/invalidation counters are separate from read/write CQ/byte counters. The
six-request public RDMA source profile requires six allocations/binds/INVs and
zero remaining MWs. TCP/requester peers have zero MWs. Four-QP source profiles
require eight binds/INVs over READ and WRITE phases.

The ignored `transport::rdma::workers::tests::hardware::source_window_peer` gate, selected by
`runner: orbitte-window`, accepts `window_probe: revoked` or `parent` on CPU
RDMA peers. It intentionally submits one WRITE with an invalidated key or the
unadvertised non-remote parent key after successful source BIND. The source
caller explicitly closes after the peer observes the error and closes, while
async-driven QP retirement may occur earlier. Acceptance requires a real
REM_ACCESS_ERR, unchanged complete backing/guards, physical
close and no remaining owners. It does not qualify arbitrary device/CQ teardown
faults, persistent key reuse, connected reuse, GDR or the complete P4 exit.

The independent source review passes 22 gates with stable/Rust 1.85, 100 unit
tests, notification/TCP/doc tests, strict Clippy and supervision/comparator
controls. The implementation stable and independent MSRV release builds each
pass 30 single-CPU runs with 16 test threads. Sixty deliberate old-logic controls
reject skipped source INV, missing ACK or fault publication after ACK,
lost-consumer teardown,
PORT_ACTIVE health restoration, the old pre-lock abort fence and other source
window/CQ ownership regressions. These CPU fault controls do not establish
physical port/device/CQ teardown-failure qualification.

Each independently built native set passes 24 cases: four real invalid-key
probes in both directions, six four-QP shared-CQ profiles, and fourteen public
DRAM/CUDA-pinned host/direct/mixed/staged/A-H20 GPU-staging/TCP regressions.
The probes observe requester REM_ACCESS_ERR, source async-event acknowledgment,
unchanged complete buffers/guards and normal cleanup; the numeric source event
kind is not recorded. Every set checks 48 actual SSH wait results, absence of
all 96 owned native/supervisor PIDs, unchanged frozen libraries and mapped file
identities, reacquisition/release of the three original resource locks, and
zero memory on A GPU 7. B GPUs remain unselected and unqueried. No direct GPU
MR/GDR, connected endpoint reuse, multirail or performance result is inferred.

Evidence remains outside the checkout under
`/root/orbitte-artifacts/rdma-windows-20261022/`: source review and controls are
in `independent-source-fc9717f/independent-review.json`; implementation artifacts,
input hashes and receipts are in `native-fc9717f/implementation-hardware-review.json`;
the independent native receipt is `independent-hardware-fc9717f/independent-review.json`.
Earlier 986c8b5/9372eb0 reports and native attempts are preserved as intermediate
results, with their unresolved findings. Native provider calls already entered
remain non-preemptible. Missing physical fault gates stay unqualified. OrbitKV
linkage stays deferred until independent acceptance of the complete P4 exit.

### Persistent key domain and bilateral incarnation qualification

P4.7b is independently accepted for the source/CPU and declared native profiles
at `4d94410e56aa192a268fe75cc6d7cbe46c639f3a`. Each request still physically
closes its QP, MW and parent MR; this stage does not enable a connected endpoint
cache or MW rebind.

The context's actual PD owner retains a bitmap for every native MW index it
has used. A source constructor takes the provider's actual initial rkey,
selects an unused low-eight-bit tag, and burns the resulting full rkey under
the PD mutex before any BIND can be submitted. Close, failed/unknown BIND,
unused grant destruction and native MW deallocation never return that bit.
Reallocation of the same index with another initial tag still consults the
same bitmap. The selected key always differs from this allocation's native
initial key. Each index has at most 256 burned full-key values; an unchanged
initial tag permits at most 255 allocations before refusal. Exhaustion refuses
authority before BIND and uses the existing physical owner cleanup.
`max_window_key_indices` defaults to 65,536 and bounds the number of distinct
historical 24-bit indices, not the numerical value of an index. A new index
beyond this capacity fails; existing indices retain their remaining tags.
There is no history eviction, automatic PD replacement or parent-key fallback.
The bitmaps disappear only after actual PD/context destruction; failed native
cleanup retains the real PD and its history.

`window_key_indices` counts live historical index records, including retained
PDs. `burned_window_keys` and `window_key_exhaustions` are cumulative runtime
counters. The historical-index count reaches zero only after successful shared
device close. It is independent of `active_windows`, which can be zero between
requests while their key history remains charged.

The actual PD gets a fresh 128-bit random nonce on construction. Separate
checked, monotonic `u64` counters allocate connection generations and source
grant generations; failure never rolls back either counter. Generations refuse
to wrap before native endpoint creation. A connection identity contains the
process `SourceEpoch`, PD nonce and connection generation. QP numbers/PSNs
remain routing fields and do not substitute for that identity. These values
fence protocol instances, not authenticate peers on this trusted-network,
plaintext protocol.

The native `Descriptor` ABI remains 40 bytes. Integers are big endian:

| RDMA body | Bytes and validation |
| --- | --- |
| Source offer, after success reply with length 88 | Source identity (40), grant generation (8), QP-only descriptor (40); requester checks source epoch, nonzero PD nonce/generations and zero memory authority |
| Binding | Exact original Request header (52), source identity (40), requester identity (40), grant generation (8): 140 bytes |
| ACCEPT | Phase `1`, binding, requester QP-only descriptor: 181 bytes; source checks its identity, request and grant before connect/BIND |
| GRANT, after ready reply | Phase `2`, binding, completed MW descriptor: 181 bytes; requester checks both identities, request, grant and unchanged QP routing before data |
| DONE | Phase `3`, binding: 141 bytes; source checks all fields before INV/staged WRITE commit |
| ACK, after final success reply | Phase `4`, binding: 141 bytes; requester checks all fields before staged READ copyback |

Both identities and the independent grant generation are echoed exactly. Even
identical requests on identical connections cannot consume an older grant's
DONE/ACK. Partial frames, changed identities/ranges/grants and reflected phases
fail through the original deadline/physical close contract. An error reply is
never accepted as a completion receipt. Direct memory effects retain the
existing nontransactional semantics.

This follows frozen TENT's separation of context, endpoint store and buffer
owners. Its [endpoint store](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/endpoint_store.cpp#L38)
reuses nonterminal endpoints, detaches failed entries into a strong retirement
list and performs native retirement outside the map lock. Its
[RDMA bootstrap](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/runtime/control_plane.h#L46)
uses NIC paths and QP lists; its
[accept path](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/endpoint.cpp#L603)
retires a changed peer QP set. Explicit bilateral process/PD/connection and
grant generations, and the permanent rkey burn ledger, are OrbitTE's authority
contract; they are not fields or a MW scheme present in frozen TENT RDMA.

The selected rdma-core v55 `ibv_inc_rkey` changes only eight low bits. Its MW
manual describes key unbinding/rebinding, without a guarantee that LOCAL_INV's
CQE alone drains all previously accepted incoming DMA. This delivery keeps
requester CQEs and actual QP close before DONE, then source INV CQE and actual
QP/MW/MR close before releasing memory or ACK. Persistent connections still
need a separately proven drain protocol, split connection/grant owners,
nonrepeating per-grant WR identities and terminal store retirement. Full P4
and OrbitKV linkage remain open.

Independent source review passes stable/Rust 1.85 strict Clippy, the relevant
feature/comparator/supervision gates and 110 CPU units plus notification/TCP/doc
tests. Implementation gates also pass package verification. The implementation
stable and independent MSRV release builds each pass 20 single-CPU runs with
16 test threads. Thirty-eight old-logic executions across 19 fault families
must fail assertions rather than timeout or fail compilation. Real control
sockets keep malformed peers alive and check rejection before ACCEPT/connect,
BIND/data, DONE or staged output commit. Source permission and both physical
QP barriers remain unchanged.

Each separately built native set passes four real invalidated/parent-key
probes, six four-QP shared-CQ cases and fourteen public DRAM/pinned-host/mixed/
staged/A-H20 GPU-staging/TCP regressions. The public RDMA source records six burned
keys across one historical native index; the four-QP source records eight
burns across four indices. History survives request resource close and reaches
zero only at actual shared device close. Every set verifies 48 actual SSH waits
and three controller waits, absence of all 96 owned native/supervisor PIDs,
unchanged A19/B16 frozen libraries and mapped-file identities, reacquisition/
release of the three original resource locks, and zero memory/no compute apps
on A GPU7. B GPUs are never selected or queried.

Evidence is external under `/root/orbitte-artifacts/rdma-key-domains-20261023/`:
`independent-source-4d94410/independent-review.json` accepts source/CPU;
`native-4d94410/implementation-hardware-review.json` records the implementation
run; `independent-hardware-4d94410/independent-review.json` accepts the independent
native profile. Initial MSRV/fixture failures, surviving old controls and the
1884e74 interim native run remain preserved and do not qualify the final head.
The [PR checks](https://github.com/OrbitInfra/OrbitTE/pull/20/checks) include the
stable/MSRV feature matrix and the unmodified frozen TENT CPU comparison.
Physical tag/index exhaustion, stale-key replay across native MW reallocation,
delayed/replayed native completions, arbitrary cleanup/device/port faults and
full P4 remain unqualified. No connected reuse,
GDR, multirail, performance advantage or OrbitKV qualification is inferred.

### Transport boundaries and connection/grant retirement

P4.7c implements the following owner and module boundaries. Its independent
source/CPU and declared native regression acceptance is recorded below; the
historical P4.7b evidence remains scoped to its own source tree.

| Frozen TENT owner | Current Rust owner | Responsibility |
| --- | --- | --- |
| `runtime/control_plane.h::ControlService` | `src/runtime/control_plane.rs::ControlService` | Socket service, descriptor/notification RPC, source admission, range/permission checks and dispatch |
| `transport/tcp/tcp_transport.h::TcpTransport` | `src/transport/tcp.rs::TcpTransport` | TCP payload READ/WRITE and their acknowledgements |
| `transport/rdma/rdma_transport.h::RdmaTransport` | `src/transport/rdma/rdma_transport.rs`, `mod.rs` | RDMA bootstrap/grant protocol, native transport resources and data transfer |
| `transport/rdma/context.h::RdmaContext` | `src/transport/rdma/context.rs::RdmaContext` | Actual RNIC context/PD, async events, key history and connection/grant generations |
| `transport/rdma/buffers.h::LocalBufferManager` | `src/transport/rdma/buffers.rs::LocalBufferManager` | Requester local-only and source MW-bind registration cache and physical retirement |
| `transport/rdma/endpoint.h::RdmaEndPoint` | `src/transport/rdma/endpoint.rs::RdmaEndpoint` | Current connection/grant lifecycle and physical close handoff |
| `transport/rdma/endpoint_store.h::EndpointStore` | `src/transport/rdma/endpoint_store.rs::EndpointStore` | Bounded live/detached ownership and exact-object retirement |
| `transport/rdma/workers.h::Workers`, `cq.h::RdmaCQ`, `slice.h` | `src/transport/rdma/workers.rs::Workers`, `cq.rs::RdmaCq`, `slice.rs::SendQueue` | One shared-CQ worker, physical CQ ownership and bounded signaled slice debt |

Frozen TENT's `LocalBufferManager` keeps registrations separate from endpoint
connections. Its `EndpointStore::getOrInsert` reuses nonterminal endpoints and
retains terminal endpoints in `waiting_list_` until retirement. This stage
implements the separate owners and detached retirement; caching ready connected
QPs remains the next contract.

The engine directly selects `TcpTransport` or `RdmaTransport`. RDMA owns its
bootstrap, binding, verbs submission, DONE/ACK and error cleanup; TCP payload
code contains no RDMA branch or forwarding call. Both can use the shared
`ControlChannel`. This follows TENT's behavior/resource boundaries rather than
copying C++ classes mechanically: frozen TENT's `ControlService` itself also
contains the TCP SendData/RecvData RPC handlers invoked by `TcpTransport`.
Here the service retains common validation and dispatch, while `TcpTransport`
owns those byte operations. No empty transport trait or compatibility facade
was added. The pre-1.0 configuration path is now
`orbit_te::transport::rdma::{RdmaConfig, RdmaHostMemory, RdmaStats}`; the old
root `orbit_te::rdma` module is removed. OrbitKV remains on its existing provider.
P4.7c retained OrbitTE wire v7. P4.7d below uses wire v8; neither establishes
TENT wire interoperability.

`ConnectionResources` owns `ConnectionHandles` (QP and shared CQ reference),
its actual `RdmaContext`, immutable QP-only routing, connection identity and
physical endpoint quota. `GrantResources` independently owns `MemoryHandles`
(MR/MW/key), context, source grant generation, its creating connection identity,
exact DMA loan or staging, MR borrow and byte quota. Only a completed BIND can
publish memory authority. A connection descriptor always has zero address/key.
The workers' per-grant record retains slice/completion debt alongside these
owners; it does not reset an old WR namespace for a new connection.

A grant accepts a physical drain receipt only from its creating connection
in the same actual context. Successful QP destruction releases its connection
quota even if later MW/MR cleanup fails. Grant cleanup then deallocates the MW
and deregisters/releases the MR; the DMA loan unlocks only after both native
memory handles are clear and all signaled-WR debt is resolved. Failed QP cleanup
cannot start grant teardown. Failed MW/MR cleanup retains the real memory owner,
loan, bytes and context and fences further admission, even when the QP quota
is already zero. The failure path never retries retained native resources.

`active_endpoints` and `retained_endpoints` count physical connection quota;
`active_grants` and `retained_grants` separately count request memory owners.
A completed staging output can retain grant/byte quota after its QP has closed.
Dropping a stale progress snapshot cannot keep a closed connection charged.
Runtime close waits for both connection and grant owners before closing the
MR cache, shared CQ and actual PD/context.

`EndpointStore` removes a terminal record from the live set into a strong
retirement list before native cleanup. Detachment does not repay quotas or
WR debt; completion routing still finds the detached owner. Native destruction
runs outside the store lock, and final removal compares the exact `Arc`.
Whole-lane abort first closes every QP, then the shared CQ, before quiescing
unresolved work and releasing grant memory. Lost async-event consumption between
QP and MW cleanup retains the grants instead of trusting the earlier check.
The reusable native runner also verifies its exact test selector matches one
test before deployment; renamed or empty `--exact` filters cannot pass a gate.
Native peers check and report both connection and grant counts at normal drain.

This store supplies terminal lifetime management for the current per-request
QP model. It does not yet cache idle connected QPs, rebind a MW or remove either
peer's physical QP close barrier. The local MR cache remains requester-only;
P4.7c used one polling worker/shared CQ; P4.7d below separates SQ and RQ CQs
under that one worker. Physical cleanup/replay
faults, source persistent MRs, connected reuse and full P4 remain open.

P4.7c proposed a source receive receipt as a prerequisite for connected reuse.
[P4.7d below](#source-receive-completion-and-independent-wr-identities) implements
that prerequisite while retaining both physical QP-close barriers. Its own
qualification must not be inferred from the historical P4.7c evidence.

### Connection/grant retirement qualification

Source [`1b88240`](https://github.com/OrbitInfra/OrbitTE/commit/1b88240192822f1a80e61873d6f88a46b4764c38)
is independently accepted for the boundaries and physical-QP-close behavior
above. The reviewer independently reran stable and Rust 1.85 across default,
RDMA, CUDA and combined features: check, strict all-target Clippy and tests.
The combined profile passes 115 unit, 3 notification, 5 TCP and 1 documentation
test; hardware-only tests remain explicit gates. Both CPU trace runs, the five
supervision/selector controls, packaging, and 20 single-CPU/16-thread release
runs per independent build pass. Thirty CPU old-logic/owner regressions each
fail twice as intended (60 negative controls), with an unchanged passing control.
Four rejected children terminate at fake-provider assertions; these remain CPU
logic controls, not physical fault evidence.
Initial feature-fixture/MSRV failures and the early surviving terminal-store
control remain preserved; they do not qualify their superseded source trees.

The implementation stable build and an independently built MSRV artifact each
pass the declared 24-case native profile: four invalidated/parent-key rejection
cases, six four-QP shared-CQ cases and fourteen public DRAM, pinned-memory,
staged GPU and TCP regressions. Each role reports separate connection/grant
counts at zero after drain. Each run records all 48 successful SSH waits and
three controller waits, 96 absent owned PIDs, unchanged file/VMA identities and
hashes, and reacquisition of both original NIC locks and A's original GPU7 lock.
Only A's selected H20 GPU7 is used; it returns to zero memory with no compute
applications. B's GPUs are not selected. CUDA uses the frozen 13.1 compatibility
driver, with no mapped TENT/Mooncake/libcudart libraries.

Evidence lives outside the checkout in
`/root/orbitte-artifacts/rdma-connection-owners-20261023`.
`implementation-source-1b88240/manifest.json` and
`review/independent-source-1b88240/gates.json` pin source gates;
`review/independent-source-1b88240/mutations/manifest.json` records the negative
controls. `native-1b88240/implementation-hardware-review.json` and
`independent-hardware-1b88240/independent-review.json` pin the two
native runs. `review/independent-scoped-acceptance-1b88240.json` records the independent scoped
verdict. The [PR checks](https://github.com/OrbitInfra/OrbitTE/pull/21/checks)
cover the feature matrix and unmodified frozen TENT CPU comparison.

This accepts the declared healthy and key-rejection profile with both actual
QP-close barriers. It does not qualify failed physical teardown, arbitrary
device/CQ faults, key exhaustion/replay, incoming DMA drain without QP close,
connected QP reuse, GDR, multirail, performance, OrbitKV or full P4.

### Source receive completion and independent WR identities

P4.7d is independently accepted for the declared source/CPU/native profiles
below. This substage keeps per-request physical QPs. It does not publish idle
connected endpoints or claim P4 completion.

The frozen TENT context/buffer/endpoint/store/worker/slice ownership remains the
reference. TENT `RdmaCQ`/`Workers` own CQ progress; `RdmaEndPoint::submitSlices`
and native `Slice*` WR identities associate completions with their slices.
OrbitTE `Workers` now polls two actual `RdmaCq` owners (SQ and RQ) fairly in one
thread, including when only RQ debt remains. `ReceiveQueue` owns a single
posted receive's exact identity, expected binding, key and deadline. Checked
per-submission WR namespaces replace resettable/pointer identities. These are
Rust lifetime/authorization adaptations; frozen TENT does not implement this
OrbitTE grant receipt or wire format.

`GrantResources` owns `MessageBuffer`: separate stable bytes and a local-only
MR. No Rust slice accesses posted receive memory before its validated CQE.
Requester SEND bytes cannot alias a mutable READ destination. Checked control
storage has its own `max_endpoints * 140` byte limit, separate from payload
staging/direct quotas; allocation charges precede native registration. Partial
registration and uncertain WR posts retain that actual owner. After physical
grant cleanup the message MR and bytes are freed immediately, before payload
output handoff, so retained completed output cannot expand this control budget.

`max_cqes` bounds SQ and RQ reservations independently. A capacity-one receive
cannot block the SQ needed to produce the peer's receipt. Every receive uses
its own WR namespace; CQ source, exact fields and binding checks precede
logical success. Wrong identities never clear another operation's debt. An
error CQE uses only WR ID, QPN, status and vendor error; unused success fields
are ignored. Only native accepted/post-uncertain ownership or exact matching
CQEs affect queue charges. A pending/unknown receive or SEND uses whole-lane
abort: all QPs, then both CQs, then debt quiescence, then grant memory. Failed
second-CQ destruction keeps its remaining CQ plus both original unresolved
debts and memory owners; successful first-CQ destruction does not invent drain.

`receive_work_requests`, `retained_receive_work_requests` and their peak track
RQ debt separately. `receive_post_calls`, `completed_receives` and
`completed_sends` count native dispatch/completion receipts. `active_message_mrs`,
`message_bytes` and their retained counters expose control ownership. SQ
`posted_work_requests`/`post_calls` include SEND; payload read/write byte
counters exclude completion messages. Remote source invalidation is counted
only after the exact successful receive receipt and binding validation.

The [rdma-core v55 SEND manual](https://github.com/linux-rdma/rdma-core/blob/v55.0/libibverbs/man/ibv_post_send.3)
defines SEND_WITH_INV, signaled and fence flags. The
[CQ manual](https://github.com/linux-rdma/rdma-core/blob/v55.0/libibverbs/man/ibv_poll_cq.3)
defines receive fields and error-valid metadata. The
[RDMA maintainer's ordering discussion](https://lists.infradead.org/pipermail/linux-nvme/2017-November/013829.html)
explains why READ/SEND execution requires care. This stage waits for all payload
CQEs before fenced SEND. ABI flag controls and healthy native bytes cannot
independently prove adversarial provider reorderings or incoming DMA drain
without QP close. That complete operation-level proof, persistent source MRs,
connected endpoint reuse, arbitrary native faults, GDR and full P4 remain open.

### Source receive and WR identity qualification

Source [`62d18ed`](https://github.com/OrbitInfra/OrbitTE/commit/62d18ed5cf72fbc3fe6df62e3e6c41b6d4555e79)
is independently accepted for this prerequisite with both physical QP-close
barriers. The reviewer independently reran stable and Rust 1.85 across default,
RDMA, CUDA and combined features: check, strict all-target Clippy and tests.
The combined profile passes 125 library tests, 3 notification, 5 TCP and one
documentation test. CPU traces, five runner/selector controls, packaging and
20 single-CPU/16-thread release runs per compiler pass. Sixty-two broken CPU
variants are rejected twice (124 controls), with an unchanged passing baseline.
Fourteen rejected processes abort at seven fake-provider/ABI assertion types;
these are CPU controls, not physical fault injection. Earlier candidate Clippy
and byte-oracle corrections remain in external evidence and do not qualify
those older candidates.

The implementation's stable build and the reviewer's independent Rust 1.85
build each pass 29 native cases. Six source-key probes include the four prior
LOCAL_INV/parent-key controls and two actual remote-invalidation probes. In
both directions, the source observes its real successful receive CQE and keeps
its QP/MW alive while the requester submits the invalidated old key on the same
connected QP. The RNIC returns actual REM_ACCESS_ERR (10); full bytes and guards
match the legitimate READ followed by the rejected access. The hostile test
bypasses only the public one-grant submission guard, never native provider/CQ
results. This demonstrates that scoped key rejection, not release of arbitrary
incoming DMA without physical QP close.

Six four-QP shared-progress regressions remain. Three new duplex component
profiles use one `RdmaTransport` runtime per process, simultaneously source and
requester, with four source/requester pairs (eight actual QPs per phase) and
fragmented READ then WRITE. SQ/RQ capacity-one and asymmetric one/two profiles
pass exact payload/guard comparisons, eight sends and receives per process,
and independent bounded queue debt. These component cases complement fourteen
public wire-v8 host/pinned/NVIDIA staging/TCP regressions; they do not replace
public control-protocol verification or establish TENT interoperability.
Actual message MR/byte and SQ/RQ debt counters drain to zero before handback.

Each native run records four successful controller waits, 58 successful SSH
waits and 116 absent owned native/supervisor PIDs at postflight. Both retain the
original lock inodes, unchanged 19/16 DSO files and verified actual VMAs; A uses
only its selected H20 GPU 7 and returns to zero memory/no applications. B never
selects a GPU. Arbitrary physical failed teardown, partial posts, replayed CQEs
and device faults remain unqualified. Persistent source MRs, connected reuse,
GDR and multirail still require implementation; full P4 remains open. The
healthy cases are correctness evidence, not a performance comparison or an
operation-level proof for removing QP barriers.

Evidence remains outside Git under
`/root/orbitte-artifacts/rdma-receive-completion-20261023/`:
`implementation-source-62d18ed/manifest.json`,
`native-62d18ed/implementation-hardware-review.json`,
`independent-hardware-62d18ed/independent-raw-audit.json` and
`review/independent-scoped-acceptance-62d18ed.json`. The scoped acceptance receipt SHA-256 is
`0166daf44c229e874701849a953899a08bc25e7a8800105b5be5f2c767fec413`. [PR #22 checks](https://github.com/OrbitInfra/OrbitTE/pull/22/checks)
cover the feature matrix and frozen TENT CPU comparison separately; hardware
acceptance stays pinned to the source above.

### Run the native gate

The explicit native [`hardware_peer`](../examples/hardware_peer.rs) runner
performs three noncontiguous READs across two appended submissions, then three
WRITEs, generation-specific notifications and complete source/destination/guard
comparisons. Each run varies a deterministic seed. It checks exact completion
bytes and successful requester WR counts, closes the whole engine, then
unregisters/releases memory and checks zero remaining registrations, workers,
shared RDMA devices, endpoints, direct/staging bytes, WR debt and retained CUDA
resources.
A direct host requester must create one persistent full-view MR and reuse it
five times, with one idle MR before shutdown and none after shutdown. Source,
device-staged, explicit-staging and TCP peers must report zero cached MRs.
Direct host peers must construct exactly six direct endpoints; device-staged or
explicit-staging peers must construct zero. These counters accompany real CQ
counts and full byte comparisons rather than replacing them. It supports `cpu`, `pinned:N` and `cuda:N` memory on either peer.
This is a correctness pilot, not a benchmark or a full fault qualification.

Use [`tests/hardware/run.py`](../tests/hardware/run.py) with an externally stored
JSON profile and a fresh external output directory:

```sh
python3 tests/hardware/run.py --profile /external/profile.json \
  --binary /external/build/release/examples/hardware_peer \
  --output /external/runs/run-001
```

The profile supplies `ssh_config`, `rnic`, `gid_index`, `nodes` and `cases`.
Each node has an existing SSH `alias`, exact `hostname`/`boot_id`, `bind` address,
`nic_lock`, and (when GPU memory is used) `gpu_lock`/`gpu_uuid`. Optional node
`env` selects explicit native library paths, such as a CUDA forward-compatibility
library; role-specific environment and the selected GPU UUID take precedence.
The controller collects actual native `/proc/self/maps` from both peers and
requires them alongside successful supervision. Each case has
`source`, `requester` node keys, `source_kind`, `requester_kind`, `transport`,
`payload` (128 bytes..32 MiB), and `seed`. Optional `host_memory` selects
`direct` (default) or `staged`; `source_host_memory`/`requester_host_memory`
override that per peer. The example receives the explicit policy through
`ORBITTE_RDMA_HOST_MEMORY`. Optional `rdma_block_size` and `rdma_max_send_wr`
select positive bounded values through the example's `ORBITTE_RDMA_BLOCK_SIZE`
and `ORBITTE_RDMA_MAX_SEND_WR`; the library itself uses per-engine Rust config.
Current probes record exact slice CQ/byte/debt/post counters. The older scoped
qualification above used one WR per request and its 3/3 CQ counts apply only to
those frozen sources. GPU cases expose only the selected UUID
through `CUDA_VISIBLE_DEVICES`, so their kind uses ordinal 0. Resource lock paths
must match the resource owner's existing convention.

For the native library-test executable, `runner: "orbitte-window"` selects
exactly `transport::rdma::workers::tests::hardware::source_window_peer`.
`window_probe` accepts `revoked` (LOCAL_INV), `parent` (unadvertised local-only
parent key), or `remote` (source receive of SEND_WITH_INV, then same-QP old-key
access before source close). `runner: "orbitte-shared-cq"` selects the exact
`shared_cq_peer` test. Both component runners require host RDMA and explicit
`rdma_max_cqes` (1..4), `rdma_max_send_wr` (1..2). Setting `duplex: true` runs
simultaneous source/requester ownership on each process; optional
`source_rdma_max_cqes`/`requester_rdma_max_cqes` select asymmetric capacities
(1..4). Duplex uses SQ window one and incrementally advertises grants after
RQ acceptance. All cases still require the same original resource locks and
supervision; a zero-match libtest filter is rejected before deployment.

First resolve the current platform attempt and run hardware/occupation preflight.
The controller verifies host/boot identity, deploys only the native binary and
supervisor to a unique `/var/tmp/orbitte-runs/` directory, verifies its hash and
holds nonblocking resource locks through each child lifetime. Per-peer native
work has a 55-second supervision deadline; the source also has a 45-second
completion deadline. Only owned children can be terminated. If an owned child
cannot be reaped after termination/kill, the supervisor reports failed cleanup
and keeps its resource locks until waitpid confirms exit. SSH disconnection
does not discard supervision. Healthy SSH starters explicitly wait/reap their
supervisors; the controller waits for those SSH commands too. A broken SSH
connection fails cleanup qualification even when native peers finish, because
a non-reaping container init can retain an orphaned supervisor zombie. Raw commands, failures, stdout/stderr and manifests
remain in the selected external output directory; keys/tokens never enter a
bundle. The runner does not reconfigure a platform job or its duration.

For OrbitKV's registered-host component, the same controller accepts
`runner: "orbitkv-peer-host"`. Deploy the consumer core test executable built
with `--no-default-features --features cuda-13,orbitte-rdma`; the runner invokes
its ignored exact `peer::orbitte::tests::rdma_host::registered_host_rdma_peer`
filter and passes role/endpoint/device/port/GID/payload/seed through environment.
Set `rdma_port`, keep both memory kinds `cpu` and transport `rdma`, and use
128 bytes..8 MiB payloads. Identity, supervision and RNIC leases are the same as
for the native runner; CUDA is never initialized and no GPU lock is requested.
Source/requester maps are collected. It checks consumer READ selection and
execution, not its PinnedAllocator, Manager grants/GPU restore or installed
serving. Current OrbitKV consumer builds require CUDA 13. Kernel R535 alone
is not an exclusion: the supplied H20 nodes also install `cuda-compat-13-1`
user-mode driver 590.48.01, a combination supported by NVIDIA's
[forward-compatibility matrix](https://docs.nvidia.com/deploy/cuda-compatibility/forward-compatibility.html).
Check mapped user-mode libraries and actual feature gates instead of inferring
support from the kernel driver version.

The consumer `660f893d` host component and `ff30ee1` runner/source-retention
controls have independent source review and reruns. The physical RoCE pilot
covers A-to-B 4 KiB and B-to-A 1 MiB payload regions with 64-byte guards at both
ends, fragmented READs with three requester CQ completions each, unchanged
guards/unrequested bytes and zero normal-close registration/endpoint/staging
charges. Maps contain real `libibverbs`/`libmlx5`
and no CUDA/TENT. All owned native and supervisor processes exit and RNIC leases
are released. The initial readiness coordination failure stays with successful
reruns in the external evidence. See
[OrbitTE #12](https://github.com/OrbitInfra/OrbitTE/pull/12) and
[OrbitKV #305](https://github.com/OrbitInfra/OrbitKV/pull/305) for frozen source,
binary hashes and independent review locations. This is component correctness,
not a throughput claim or pinned Manager/GPU acceptance.

A separate A-node CUDA 13.1 source profile at consumer `660f893d` passes pinned
source-grant controls, embedded Manager TCP GPU Save/Restore and embedded Manager
same-RNIC RDMA staging GPU Save/Restore. It uses installed `libcudart.so.13.1.80`
and compatibility `libcuda.so.590.48.01`; the RDMA run maps real verbs/mlx5 and
no TENT. Test-owned source/service workers drain and native/supervisor/coordinator
processes are reaped; the fixture kills its private etcd, so graceful-etcd
qualification is separate. External evidence remains outside the checkouts and is indexed in
[OrbitKV #306](https://github.com/OrbitInfra/OrbitKV/pull/306).
This is neither an installed Manager nor cross-process CUDA IPC/cross-host
Manager/GPU or GDR qualification. B-node compute remains untested while all
canonical GPU leases are busy; do not take another workload's GPU.

A later installed consumer fixture at `eb1cc90f` is independently accepted in
[OrbitKV #307](https://github.com/OrbitInfra/OrbitKV/pull/307). The complete
CPython 3.11 wheel contains matching native client and Manager artifacts. Two
separate Managers and an installed client exercise CUDA IPC registration and
Publish, inventory fencing, TCP and same-RNIC RDMA peer READ, then client-local
DRAM H2D Restore. Each provider compares 16,384 GPU bytes and untouched pages,
checks exact Save/remote/H2D counters, repays four original held IPC slots and
normally exits the client/Managers, supervisors and outer waiters. The private
etcd fixture is killed and reaped. Canonical leases are released and GPU memory
returns to zero; no producer-exit warning is accepted.

That installed profile uses Torch 2.13.0+cu130, one real CUDA 13.0.96 runtime,
compatibility driver 590.48.01 and kernel driver 535.161.08 on H20. The CUDA 13.1
compiler does not establish a loaded CUDA 13.1 runtime. Frozen `cudarc` tries
unversioned `libcudart.so` first; the profile explicitly freezes a private normal
alias to Torch's actual `.so.13`, its directory/target and preload. Full loader
order CPU controls preserve the mixed-runtime negative cases. Every actual
client/Manager mapping must match frozen files and the single selected CUDART;
`RTLD_NOLOAD` observes the version without loading another runtime. See the
[consumer recipe](https://github.com/OrbitInfra/OrbitKV/blob/main/docs/transport.md#installed-client-and-manager-profile).
Source/wheel manifests, raw runs, independent review and failed attempts remain
outside the checkouts at `/root/orbitte-artifacts/installed-rdma-20261022/`.

This accepts that healthy installed prerequisite, not a complete numbered
parity stage. Default/natural runtime configurations, uniform installed CUDA
13.1, Manager IPC Restore, inference serving, physical cross-host Manager/GPU,
GDR and native faults remain open. The earlier CUDA 13.1 source evidence stays
separate. Default OrbitKV wheels retain TENT; these selected payload providers
map no TENT libraries.

The CPU `remote_rdma_teardown_failure_retains_source_while_requester_is_locally_drained`
control drives actual TCP framing with verbs fault callbacks. After a source
MR teardown failure, requester QP/MR cleanup can succeed while the remote error
loses its Unquiesced detail on the wire. The test checks zero published READ
bytes, actual source backing retention, endpoint/staging/registration charges,
rejected native admission and a source-close timeout. It does not prove a
physical device failure or provide a remote termination receipt.

Before calling a hardware profile Qualified, additionally run actual delayed
completion/cancel/timeout, endpoint/native teardown faults, restart/reuse and
installed-artifact controls, alongside an independently built frozen TENT oracle
under the same declared budgets. IB and RoCE, GPU directions, GDR, NVLink, MNNVL,
SSD and GDS require separate evidence. No performance comparison is claimed.

## Persistent source registration

P4.7e is independently accepted for the declared source/CPU/native profiles.
It advances B04's host path, F04 registration and F15/F18 physical ownership
foundations; complete parity rows remain open.
It keeps the frozen TENT
[`LocalBufferManager`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/buffers.cpp)
boundary: a buffer registration and its access capability belong to the buffer
manager, independently of an endpoint. OrbitTE keys entries by actual registration
identity and local/MW-bind capability within the owning PD. Direct ordinary DRAM
and CUDA-pinned host views use this cache; private host/device staging remains
per request. The common count/full-view byte limits charge both capabilities,
including construction, native close and failed cleanup. Concurrent requests
share one constructor per capability and retain separate exact DMA leases.

Frozen TENT can publish remote-enabled registered-buffer keys. OrbitTE's cached
source parent has only `LOCAL_WRITE|MW_BIND`, with neither `REMOTE_READ`,
`REMOTE_WRITE` nor relaxed ordering. Its key never appears in a public descriptor.
Each request allocates a separate Type-2B MW, burns a fresh full rkey in the
persistent PD ledger and binds the exact requested range with only that request's
READ or WRITE permission. READ and WRITE may share the same parent MR. Completion
messages still register separate `LOCAL_WRITE` MRs without MW-bind capability.

The source requires the existing exact receive/binding protocol and actual QP
close before deallocating its MW. Only successful native MW deallocation returns
the parent's borrower; remote invalidation alone cannot do that. Final unregister
marks **all** capabilities retiring before native work outside the cache lock,
including entries still borrowed or being created. Last release deregisters a
retiring parent. An admitted resolved owner may finish after unregister but
cannot leave a new idle entry. Failed MW cleanup retains the actual MW, parent
borrow, exact DMA lease, backing, PD and charges. Failed parent deregistration
retains its actual native MR and backing without a Drop retry.

Source controls cover capability separation under shared physical limits,
READ/WRITE parent reuse with distinct grants, concurrent construction, final
unregister, idle eviction and cleanup failures. The two-host gate also checks
actual source-cache counters and denies a write one byte before the MW while
still inside its registered parent. Existing parent-key and invalidated-key
probes remain required. These gates do not establish connected QP reuse, arbitrary
physical faults, direct GPU registration, multirail or a performance gain.

Source `dda4db90ad06c04ccfe060cd5e79fa1b9ab2daea` passes independent
stable 1.97.1/MSRV 1.85 four-feature gates, 128 unit tests, 148 rejected CPU
negative controls and 40 single-CPU release runs. Twenty-six negative-control
children intentionally abort inside CPU fake/provider-ABI assertions; those are
not hardware fault injections. The first-native-deregistration-failure test
specifically rejects marking only the first capability retiring. Both the
implementation stable build and independent MSRV build pass 31 native cases:
eight key/range rejection probes, six four-QP shared-progress, three same-runtime
duplex and fourteen public DRAM/pinned/GPU-staging/TCP regressions. A direct source
opens one parent and reuses it five times; a shared source reuses it seven times.
Duplex charges two full-view MRs and reuses one source parent seven times.

The physical key probes reject an unadvertised parent-key WRITE, a revoked-key
WRITE, a WRITE outside the exact MW and an old-key READ after actual remote
invalidation while the source QP is still live, in both directions. They require
actual REM_ACCESS_ERR, complete guards/bytes and physical close. There is no
separate parent-key READ hardware probe; absence of both parent remote flags is
also verified in source. Each native run confirms 62 actual SSH waits, all 124
owned PIDs absent, both queue debts and all memory/native owners at zero after
close, the original three resource-lock inodes reusable, unchanged frozen DSOs
and A-H20 GPU7 back at zero memory. B GPUs are not selected. Native eviction,
unregister, arbitrary partial posts, device and cleanup faults remain unqualified.

External evidence is under
`/root/orbitte-artifacts/rdma-source-registration-20261023/`:
`implementation-source-dda4db9/manifest.json`,
`native-dda4db9/implementation-hardware-review.json`,
`independent-hardware-dda4db9/independent-review.json` and
`independent-hardware-dda4db9/independent-raw-audit.json`. The independent scoped
acceptance is `review/independent-scoped-acceptance-dda4db9.json`, SHA-256
`adcd3c59f1da0f7e511679029922931107010cc526d1c8554d28e4af94245652`.
The two source archives and distinct native binary hashes are recorded there.
This accepts the stated P4.7e profile while both QP barriers remain. Connected
reuse, release of incoming-DMA owners on live QPs, full P4, GDR, multirail,
performance and OrbitKV integration remain open.

## Connected QP reuse

P4.7f is implemented for negotiated, coherent host Direct RC peers, fixed source
or requester role and fixed READ/WRITE capability. Each physical QP admits one
grant at a time. The declared source/CPU and native profiles have independent
acceptance. This is F02/F03/F07/F08/F15/F18 groundwork and a scoped
B04 profile, not the complete P4 exit or native TENT wire interoperability.

### Correspondence with frozen TENT

The reference remains `e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`.

| Frozen native TENT | OrbitTE owner and behavior |
| --- | --- |
| `context.cpp`: PD/CQs outlive endpoints and MRs | `RdmaContext` retains the actual PD and key/generation history; shared SQ/RQ CQs outlive every QP |
| `buffers.cpp`: `LocalBufferManager` registers buffers independently of endpoints | Local-only and MW-bind full-view MRs persist independently; exact grant loans remain separate |
| `endpoint_store.cpp`: FIFO/SIEVE `getOrInsert` reuses a nonterminal endpoint | `EndpointStore` atomically claims only an idle exact pair; active/idle physical QPs share the bounded capacity |
| `endpoint.cpp`: connected `RdmaEndPoint` retains its QPs | `ConnectionResources` retains the same native QP, QPN, PSN, peer routing and ConnectionId; reuse calls `prepare_grant`, never QP create/RTR/RTS |
| `workers.cpp`/`slice.h`: workers own post/poll and slice completion | `Workers` owns the sealed completion proof, native grant cleanup and nonrepeating SQ/RQ WR namespaces |
| `beginDestroy`/`finishDestroy` and `waiting_list_` | Terminal detachment is irreversible; retired owners retain routing/quota until actual close, outside the Store lock |

This uses the native [nonterminal lookup and retirement design](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/endpoint_store.cpp).
OrbitTE deliberately keeps per-grant one-use MW authority instead of exporting a
persistent remote-enabled parent key. It currently uses oldest-idle eviction,
not the full configurable native FIFO/SIEVE policy. Busy entries are never
victims. Native cleanup failure retains real objects, backing, debt and budgets;
no Drop retry or live PD/key-history reset supplies recovery.

### Successful operation proof

The selected coherent-host operation contract follows the data/SEND completion
pattern in [RFC 8166 section 2.3.2](https://www.rfc-editor.org/rfc/rfc8166.html#section-2.3.2)
and the receive-reported invalidation in
[RFC 8797 section 3.2](https://www.rfc-editor.org/rfc/rfc8797.html#section-3.2).
These transport patterns are combined with OrbitTE's mandatory binding, exact
native CQ validation and owner cleanup; a TCP receipt or invalidation flag alone
is insufficient. No arbitrary unrelated-QP, relaxed-ordering or device-memory
drain proof is inferred.

1. Requester payload CQEs resolve every accepted slice. Worker-owned sealing
   excludes any later payload post. The completion message is sent on that same
   actual SQ with FENCE|SIGNALED SEND_WITH_INV, invalidating only this grant's key.
2. The requester consumes the exact successful SEND CQE and cleans its local
   grant/MR borrow, message MR, exact loan and grant charge before bound DONE.
3. The source consumes the accepted actual RQ CQE with exact CQ direction,
   namespace, QPN, RECV opcode, 141-byte length, WITH_INV/rkey and full binding.
   It also requires the bytewise bound DONE from the control channel.
4. A private `GrantDrain` receipt binds the actual creating ConnectionId, local
   grant generation, capability and successful sealed work. The sole post/poll
   worker deallocates the invalidated MW before parent-borrow release, closes
   the message MR, releases the exact loan and drops the grant. Failure retains
   the remaining actual lane rather than retrying cleanup.
5. The source writes bound ACK, publishes READY, then writes bound READY. The
   requester validates both before publishing its own READY. A failed last
   write may retire only the exact still-idle Arc/claim token. An old handle,
   Drop or reply has no authority over a newer claim.

`ConnectionResources` remains the unique physical owner inside `Resources`;
its optional grant is `None` in READY. Each claim burns a checked local token
and uses a fresh deadline; each source grant burns a new generation/MW/key and
every operation burns new WR namespaces. READY carries no old work/receive,
message-sent seal, DMA exclusion or RQ/SQ debt. Physical identities never change
to label a rebuilt QP as reuse. Idle connections do not keep old registrations
alive; unregister and MR eviction can deregister their former parents.

### Wire v9 and cache behavior

The existing 52-byte Request is followed by a mandatory 105-byte HELLO: one
flags byte (host Direct candidate/reused pair, other bits zero), requester
Identity40, optional source Identity40, expected remote PD domain16 and previous
grant8. A fresh pair uses zero source identity and previous grant. A retry after
an explicit cache miss preserves the old expected domain. HELLO's requester
identity is immutable in ACCEPT.

The 89-byte OFFER has source Identity40, new grant8, QP-only Descriptor40 and a
release-profile byte (`0` physical close, `1` negotiated host Direct). Reused
OFFERs must keep the old source identity/routing and increase the grant number.
Every ACCEPT/GRANT/DONE/ACK/READY and native completion message binds Request52,
source Identity40, requester Identity40, grant8 and profile1: 141 bytes, plus
one phase byte on control frames. ACCEPT/GRANT additionally carry Descriptor40.
The source must match both cached identities, role, operation and previous
grant before creating authority. QP addresses/keys in bootstrap remain zero.

An unavailable/busy/stale source pair returns explicit status `12` before any
BIND, GRANT or data. Only this response permits one fresh-pair retry after
physical retirement of the stale requester half, under the original deadline,
process epoch and PD domain. EOF, malformed or missing ACK/READY, native faults,
timeout, ambiguous posts and changed identities never authorize WRITE replay.
If source ACK/READY is lost, an empty idle source half may outlive the requester;
it has no live MW/loan and must pass a later exact-pair rendezvous or be evicted.

Idle QPs retain physical `max_endpoints` quota. When full, detach only idle
entries, destroy outside the Store lock, then repay the actual connection lease;
failed native close retains the owner and poisons admission. Store membership
and idle-count changes share one control critical section, including against
lane failure. No Record lock, native call or condition wait occurs inside the
Store critical section. Whole close fences new claims and retires idle QPs
before waiting for physical usage zero; accepted builders/grants finish or
retire under their existing deadlines. Directional close stays independent.

`opened_connections` and `closed_connections` count actual native QPs,
`idle_endpoints` remains part of `active_endpoints`, `reused_connections` counts
successful idle claims and `completed_reusable_grants` counts sealed native
grant cleanup. Claim/creation failures do not return physical quota early.

### Verification and remaining scope

CPU gates cover unchanged QP identities across four grants and a different
registration, two-way claim exclusion, stale handles, exact drain bindings,
invalid HELLO/OFFER/ACCEPT/GRANT/DONE/ACK/READY, missing either source proof,
explicit miss/eviction under capacity one, generation/WR exhaustion, pre-open
async faults and deterministic idle-debit/lane-failure interleaving. The existing
native message ABI gate requires FENCE|SIGNALED and SEND_WITH_INV.

The opt-in `orbitte-connected` hardware runner selects exactly
`transport::rdma::workers::tests::hardware::connected::connected_endpoint_peer`.
It runs public v9 simultaneous host READ/WRITE in both directions on one runtime
per host, CQ capacity one or an asymmetric pair, four persistent physical QPs,
four rounds, actual old-parent deregistration/backing release with QPs live,
new registrations/ranges and full source/destination/guard checks. Each round
records native QPN/QP handle, PSN, both routing directions and ConnectionId;
new grants retain them exactly. Existing key/range rejection, shared progress,
duplex and DRAM/pinned/NVIDIA staging/TCP profiles remain required. Public
runner profiles set `ORBITTE_EXPECT_QP_REUSE` from both peers' actual selected
memory/policy, rather than deriving expected behavior from observed counters.

Source `371d804763251337ca4e1ca93dac3df7c87dc5ca` passes independent stable
1.99.1/MSRV 1.85 four-feature gates, 140 unit tests and 40 single-CPU release
runs. Twenty reverse controls for ten new contracts all reject the removed
guard; the unchanged baseline passes. Earlier controls that survived before
test strengthening, interrupted runs and actual failures remain preserved.

The implementation stable build at `71861df` and independently built MSRV
source `371d804` each pass 35 native cases: four connected public duplex cases,
eight actual key/range rejection probes, six shared-progress, three earlier
duplex and fourteen public DRAM/pinned/GPU-staging/TCP regressions. The later
source changes only CPU tests; the independent carry receipt verifies every
production and selected native-fixture blob, and the public peer binary is
byte-identical. This is a verified carry, not a claim that the implementation
reran native work at the later source commit.

Each connected peer retains four actual QPs through four rounds, claims them
twelve more times and cleans sixteen grants. Final unregister deregisters its
old parents and frees the old backing while those QPs remain live; new views
reuse the same QPN/PSN/routing/incarnations. Each peer opens four full-view MRs and
reuses them twelve times, including two source parents/six source reuses, with
eight completed SENDs, receives, MW binds and invalidations. Every full buffer
and guard matches. Each native set verifies all 70 SSH starter waits and the
absence of all 140 native/supervisor PIDs, reacquires the original resource
locks, verifies frozen/mapped DSOs and returns A GPU7 memory to zero. B GPUs
are not selected.

Raw evidence remains outside Git at
`/root/orbitte-artifacts/rdma-connection-reuse-20261023/`.
`native-71861df/implementation-hardware-review.json` records the implementation
run. `review/independent-scoped-acceptance-371d804.json` records independent scoped acceptance
(SHA-256 `b099fe7677c594976c34dbbd4694127e0c35d5d3a4e1125d10657ec7ceea579d`), including the final source/native build, carry and
resource-close evidence. The maintained commands and original lock/host
constraints above remain required to reproduce the profile.

GDR, arbitrary physical device/cleanup faults, multirail/topology discovery,
SIEVE policy, concurrent grants or role reversal on one QP, relaxed ordering,
performance, complete P4 and OrbitKV linkage remain unqualified.


## Device, port, GID and host topology discovery

P4.8 implements automatic **single-rail Linux host discovery**. Source/CPU is
independently accepted and the implementation's healthy IPv6 RoCEv2 run is
qualified; the independently built native repeat remains unqualified. This does not close
full P4, add multirail failover or infer an allocation's NUMA placement.

The frozen reference remains Mooncake/TENT
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`. Responsibilities follow its existing
owners rather than adding topology policy to TCP:

| Frozen TENT | OrbitTE | Responsibility |
| --- | --- | --- |
| `runtime/topology.h`, `topology.cpp` | `runtime/topology.rs` | `Topology`, `NicEntry`, `MemEntry`, three NIC priority ranks; transport attributes remain namespaced metadata |
| `platform/cpu_probe.cpp` | `platform/cpu_probe.rs` | CPU node inventory, RNIC PCI BDF and known/unknown NUMA node; local rank 0 and fallback rank 2 |
| `CpuPlatform::probe`, `listInfiniBandDevices` | `rdma::sys::devices`, `rdma::discovery` | Intersect provider-visible RNICs with readable/writable uverbs character nodes; apply device filters before native open |
| RDMA `getBestGidIndex`, `Context::openDevice` | `rdma::discovery`, `RdmaContext` | Enumerate/rank active ports and nonzero GIDs; validate the chosen route against the actual native context |
| RDMA context address ownership | `RdmaContext::Resources` | Freeze selected device/port/GID/type/netdev/ifindex/LID/MTU/link layer with its actual PD and key domain |
| Existing RDMA event and endpoint owners | `events`, `Workers`, `EndpointStore` | A changed/unqueryable route fences admission; actual QP/MW/MR owners supply retirement and retention |

`RdmaConfig::default()` discovers device, port and GID. Each `Some` selector is a
strict pin; `gid_index: Some(0)` selects zero explicitly and `port: Some(0)` is
invalid. `RdmaConfig::new("mlx5_0")` preserves its explicit device/port 1/GID 0
behavior. Setting one field to `None` only frees that selection dimension.
A nonempty `rdma_whitelist` takes precedence over `rdma_blacklist`, matching
TENT; it always intersects an explicit device pin. A pin excluded by the filter
is rejected, not silently replaced.

```rust,ignore
use orbit_te::transport::rdma::RdmaConfig;

let rdma = RdmaConfig {
    rdma_whitelist: vec!["mlx5_0".into(), "mlx5_1".into()],
    preferred_numa_node: Some(1), // Explicit caller hint, not allocation detection.
    ..RdmaConfig::default()
};
```

Discovery lists kernel `nodeN` host locations plus the wildcard `*`. An unknown
NIC NUMA node remains `None`, never node zero. Without an explicit NUMA hint the
wildcard ordering is deterministic. The snapshot contains candidate inventory;
only the NIC with `rdma.selected=true` has a native-validated selected route.
Other entries do not assert successful native context construction. GPU ordinal
mapping, PCIe link distances and per-allocation page location belong to later
platform/GPU work.

Within locality rank, selection prefers IB or RoCEv2, an associated physical
netdev, and a globally usable GID before link-local. A valid nonzero RoCEv1 GID
can remain a fallback. IPv4-mapped and true IPv6 RoCEv2 are both preferred:
TENT's frozen implementation prioritizes IPv4-mapped RoCEv2 and reaches true
IPv6 through its nonzero fallback. OrbitTE deliberately makes true IPv6
first-class. It does not infer GID family from the TCP control bind address,
and does not classify private IPv4 address ranges as overlay networks. The
kernel's virtual-netdev inventory supplies the overlay preference.

The Linux [stable sysfs ABI](https://www.kernel.org/doc/html/v6.8/admin-guide/abi-stable.html)
provides GID type and associated netdev metadata. Native validation uses
`ibv_query_port`, `ibv_query_gid`, and rdma-core v55's `_ibv_query_gid_ex` binding
with flags zero and the actual generated entry size. Bytes, port, index, GID
type, netdev ifindex and resolved interface name must agree. Missing associated
netdev metadata is allowed with native ifindex zero, including IB; malformed
or inconsistent metadata is rejected. A provider without the required extended
GID query returns an explicit error. It does not receive a fabricated type.
The public selected-path attributes include `rdma.port`, `rdma.gid_index`,
`rdma.gid`, `rdma.gid_type`, `rdma.net_device`, `rdma.net_device_index`,
`rdma.lid`, `rdma.mtu_bytes` and `rdma.link_layer`.

Candidate fallback is allowed only during the first lazy context construction.
Every failed actual context/PD must close completely before the next candidate
opens. If native cleanup fails, the real owner and statistics remain retained
and the cache stores `Unquiesced`; later calls cannot retry cleanup or switch
candidates. Once a context is published, no discovery refresh replaces its PD,
key ledger, MR cache or connection incarnations.

Both public requester and source paths re-query the frozen route **before**
claiming a READY connection. `open_endpoint` and fresh/reused `connect` recheck
it as well. The second native port/GID check before constructing a QP must
match the frozen descriptor. Any observed change or query failure permanently
fences that lane; restoring the same tuple does not clear the fence or reset
its burned rkeys. A lost async-event consumer still takes precedence as
`Unquiesced`. Synthetic route faults do not invent native event ACKs. Worker
notification occurs outside context/record locks. Pure owner lookup stays free
of native route queries so it does not block registration retirement behind
another MR construction.

`TransferEngine::topology()` returns the immutable snapshot after first RDMA
use; it returns `None` before initialization, without an RDMA transport, or
after successful engine close. Reading it neither opens hardware nor rescans or
heals an existing route. It describes the selected route at construction,
not a live health monitor.

For the physical runner, set a case's `rdma_auto` to `true`. Both peers then
use automatic device/port/GID selection with the profile RNIC as the sole
whitelist entry. The existing supervisor acquires its original NIC lock before
starting the binary; discovery cannot open another RNIC. Connected/public
peers emit `ORBITTE_TOPOLOGY` alongside byte/guard, QP-continuity and close
proofs. Retain explicit-pin regressions separately. Physical IB, route/device
fault injection, multirail selection and hot route rebuild remain unqualified.


### Discovery qualification and current gate

Source `97cc94f830dd71ea51e6d5529fe9575eedd4a4a5` passes the implementation's
47-command stable/MSRV, feature, package, CPU-trace and runner suite, including
eight single-CPU release runs. Independent review passes 76 gate commands,
including 40 single-CPU release pressure runs; it covers 153 all-feature unit
tests, three notification and five process/TCP tests and the doc example.
Another 42 effective negative controls cover 21 contracts. The initial candidate's four test-coverage gaps are fixed and
both old and final control results remain archived. Three selected-RNIC tests
are ignored in ordinary CPU runs and executed by the physical profile. The
ignored subprocess fixture is invoked explicitly by the process restart test;
its ignored listing is not a skipped restart gate.

The implementation's separately frozen stable native binaries pass 39 cases:
four automatic-selection cases (both public DRAM directions and connected
duplex QP continuity) plus the existing 35 explicit-profile regressions. Both
automatic peers select `mlx5_0 / port 1 / GID 3`, true IPv6 RoCEv2 with native
ifindex 2. Byte/guard, fixed-QP reuse, source grant/MR ownership and complete
normal close checks pass. All seven controllers and 78 SSH waits return zero;
156 peer/supervisor PIDs are absent afterward, original three locks are free,
19/16 frozen DSOs and 12/7 mapped-DSO controls match, and A GPU 7 has zero memory
and no compute applications. B GPUs are not selected.

The independent MSRV binaries are built and frozen but execute **zero** native
cases. Fresh Forge/History discovery finds both original attempts terminated:
job `20551990`, stage `95601664`, A `RELEASED` and B `KILLED`, with no RUNNING
replacement. The implementation's postflight completed at
`2026-10-23T10:46:23Z`; both release times are after `10:50:35Z`. Independent
raw-evidence review verifies the successful implementation run predates those
releases. It does not substitute for the missing independently built native
repeat. Source/CPU is Accepted, implementation native is Qualified, independent
native is Unqualified, and P4.8's acceptance checkbox remains open. Resume with
a newly authorized two-host resource profile; do not operate on the terminated
attempts or silently reuse their node/lock identities.

Evidence root: `/root/orbitte-artifacts/rdma-topology-discovery-20261023`.

- `gates-97cc94f/manifest.json`, SHA256
  `50d51ce4e58132f61a1d987be997e4c86c4021e6e18aaeace378f3953d522be0`.
- `native-97cc94f/implementation-hardware-review.json`, SHA256
  `9879192c9fd6ba6948031e333f693895ddb37faf4be6e2fe5ea1491443d79a32`.
- `review/independent-source-97cc94f/gates.json` and `mutations/manifest.json`,
  SHA256 `ec535f217d7680a101329ce4fe5a3d045bdf7fea6faacb479217e788e4d38ecc`
  and `3aa0ed2af145b04b0a756b37e707a3ee5c02ed299cdce6cbd75258856f307434`.
- `review/independent-implementation-native-raw-audit-97cc94f.json`, SHA256
  `15ea80535078acefe3717c82f3ae14695979b290d0e512cc5e4680299930a2cd`;
  `independent-hardware-97cc94f/` retains the unavailable repeat's selected-stage
  metadata, all-state History and frozen MSRV build provenance.

Physical IB, comparisons across multiple RNIC/NUMA ranks, real route/device
faults, GDR, multirail and performance remain unqualified. OrbitKV integration
continues to wait for the full P4 native-stage boundary.
