# Architecture

OrbiTE follows frozen Mooncake TENT's engine, transport, context, registered
buffer, endpoint and worker boundaries, implemented as Rust resource owners.
The [product positioning](https://github.com/OrbitInfra/OrbiTE#overview) defines
the consumer and non-goal boundary. The [release ladder](completion-plan.md#release-ladder)
places test consolidation after the beta API freeze and a NIXL-based P/D
design after installed GDR/NVLink qualification. Neither adds a current
transport, connector or performance claim.
Architectural alignment does not by itself establish behavior, ABI, wire or
performance parity. See [capabilities](capabilities.md) for support and the
[completion plan](completion-plan.md#p4-remaining-delivery) for open work.

:::{image} _static/architecture.svg
:alt: OrbiTE network transports and the implemented DRAM and GPU paths to local SSD
:class: dark-light
:::

## Python boundary

The separate `orbite-python` crate builds only `orbite._native`. This is a real
wheel-packaging boundary: it keeps PyO3 out of the primary Rust crate and owns
the existing bounded memory, segment and batch tables directly. The removed
C/C++ exports, raw argument/output adapters and external plugin loader have no
Python callers. No Python dependency or interpreter is loaded by an ordinary
Rust build. See the [consumer boundary](tent-parity.md#wire-and-consumer-boundaries). Shared PyO3 has a
compatible minimum of 0.27.2 and the lockfile fixes its MSRV-tested graph. Maturin
builds one wheel per CPython version because the 3.10 stable ABI lacks the buffer
API needed to retain exporters. Mooncake's `mooncake.tent` and standalone `tent`
imports remain untouched; consumers explicitly import `orbite`.

Python argument conversion captures a writable, contiguous memoryview or CUDA
Array Interface/DLPack exporter. The CUDA path verifies the full export against
driver allocation bounds and device attributes, consumes producer readiness in
the exact allocation context, and retains each Python/capsule lease through
aliases, worker loans, unregister and native quarantine. The capsule follows
the published DLPack C layout without a new dependency; CPU wheels never request
GPU capsules. External integer addresses without an exporter are
rejected before dereference. The installed negative controls reject bare pointers,
resize while registered and out-of-exporter ranges; a held TCP reply keeps the
actual exporter alive after timeout, cancel and early guard drop. A memory guard
whose allocation is registered defers free until unregister drains it, or engine
close drains the table. A batch guard moves its receipt to the existing deferred
retirement owner. Neither guard destruction waits for an unbounded transfer.

Every native call detaches from the GIL and catches unwinding. Wait uses the
existing bounded backoff, dropping the per-batch mutex between observations;
physical drain is required independently of task state. A Python thread releases
a held real TCP reply while another is blocked in wait. No Python objects enter
the payload worker other than the retained exporter owner. Decrementing that
owner uses PyO3's deferred reference pool; it never acquires the GIL beneath a
native lock. A cleanup-result fault keeps a real Python exporter and its closed
engine slot. Installed-wheel CUDA controls drive native range checks, exact context
restoration, a blocked copy and failed pinned cleanup through a fallible driver
shim. Export leases are bounded by the registration budget, including duplicates.
`trim_idle_staging_buffers` reclaims only idle ProxyManager slots, with a bounded
single pass and one-shot cleanup; active slots are untouched. The Python adapter
uses it together with `CudaMemory::trim_idle_copy_resources` before releasing
foreign registrations, avoiding an idle context cache pinning a Python tensor
indefinitely. Checked event/stream destruction reports `Unquiesced` before Drop;
failed cleanup keeps the registration, slot charge and exporter (`test_failed_staging_cleanup_keeps_exporter_and_never_retries`).
This retained interchange contract is an extension beyond TENT's raw integers.
If interpreter finalization prevents a safe detach, the bounded
engine slot remains retained. The [guide](python.md) records these deliberate
lifetime differences from frozen `src/python/pybind.cpp` and its guard test.

## Peer retirement

`runtime/segment_manager.rs` owns a bounded table of endpoint/epoch identities.
Each prepared memory submission retains a peer permit in its existing batch
admission owner, through queued work, retries, aliases and quarantine. Notification
hooks retain every target until their final attempt. Closing a peer fences the
entry before waiting; native cleanup runs outside table and peer locks. A fresh
epoch gets a different entry. Each requester-owned `RemoteSegment` and clone
holds an identity token; permits and cleanup hold the same entry. Under the table
lock, a sweep reclaims only entries with no outside references, either still open
or successfully cleaned up. Admission requires that token and never inserts by
key. An explicit `open_segment` can recreate a reclaimed identity only after
checking the authoritative epoch through Describe. Another requester must open
its own descriptor. This closes the borrowed-snapshot resurrection hole without
charging historical peer incarnations. There is no engine descriptor cache to
erase: snapshots lose admission authority immediately.

Timed-out drain entries still owe cleanup, and failed native cleanup entries
remain charged even after every handle disappears. Live/retired/unquiesced and
cumulative reclaimed-entry counters expose this distinction. The churn control
moves guarded TCP bytes through ten times the table capacity; pressure, last-clone,
foreign-requester, cleanup-held and concurrent reclaim/open/admit/close controls
check that only an explicit authoritative open can recreate admission.

After drain, each transport removes only matching requester resources. TCP
sockets live in the drained call. HP-TCP caches now key lanes by control endpoint
and epoch. SHM imports include that pair in their mapping key. RDMA fences its
matching endpoint arrays and uses the existing CQ retirement receipt; registration
MRs, contexts and workers remain shared. NVLink and MNNVL close matching relocation
entries through their existing one-shot native cleanup. Failed cleanup is sticky
and retains its actual native owners and charges. Global requester close waits
for selective cleanup and preserves its error. The existing fail-stop RDMA fault
domain still applies if a provider teardown error quarantines a shared CQ;
retirement does not weaken that safety boundary.

Frozen `segment_manager.cpp::closeRemote`, `segment_tracker.cpp` and
`transfer_engine_impl.cpp::closeSegment` invalidate handles/descriptors without
this reference-retained membership fence or cross-transport drain. Rust retirement is a
stronger extension. Python preserves reversible TENT handle close. `peer_retirement_*` controls distinguish
retirement from timeout, exercise submit/close races, keep a held TCP reply and
CUDA event charged, reject stale snapshots after a same-endpoint restart, and
prove native destroy is attempted once. The installed-wheel reopen control distinguishes handle close from peer retirement.

## Configuration ownership

Discovery, membership, KV indexes and etcd/Redis/HTTP stores live in the
consumer. The P2P control service describes an already identified peer's
segments, capabilities, registration keys and epochs; it does not find peers
or replace the consumer's authoritative epoch. This deliberately excludes
TENT's external metastore plugins. The configuration and installed Python
negative controls reject those types and server URIs, including through
`MC_TENT_CONF` and legacy overrides, before binding an engine or resolving
a hostname. Diagnostics name the endpoint/epoch migration without echoing
server credentials.

`EngineConfig::from_tent_env`, `from_tent_json` and `from_tent_file` resolve
a bounded document into the typed owners before any engine starts. Metrics
keeps its file-before-environment precedence. The selector, QoS, QP layout
and HP-TCP use their existing schemas. Both constructors capture custom RDMA
topology file contents. The TENT loader also resolves the RPC hostname once.
Device discovery still
checks those settings against the host when the engine starts.

TENT installs process-wide glog settings. OrbiTE leaves the log filter with
the application and rejects `log_level`. Legacy CUDA IPC still requires an
explicit opt-in for cooperative peers. RPC workers are capped at 128 and
RDMA send queues at 64 work requests. The endpoint budget counts OrbiTE's
retained native owners and has a ceiling of 4096. Unsupported verbs
parameters and TCP retry knobs return configuration errors. The CPU controls
exercise these differences and the captured environment without changing a
process variable.

TENT's `closeSegment` removes a name/ID mapping in `SegmentManager` and bumps
its cache version; shared descriptors already in use survive. OrbiTE's Python
close preserves that reversible handle contract. Rust `close_segment` fences
an endpoint/epoch, drains admitted work and retires matching transport caches;
a timeout or failed native cleanup retains owners and charges. See the
[discovery boundary](rust-api.md#discovery-boundary).

## Engine and transport boundary

S16 keeps the upstream serializer and headers inside the frozen CPU oracle.
The [acceptance inventory](https://github.com/OrbitInfra/OrbiTE/tree/main/tests/reference/inventory)
connects each public field or message to its source, owner, test trigger and
expected result. The oracle captures the two YLT envelope layers separately
from TENT's attachments. The Rust control plane uses its existing codec.
The [wire and consumer boundary](tent-parity.md#wire-and-consumer-boundaries)
excludes S23. The native acceptor has one listener and no profile dispatch.
It rejects a foreign magic prefix before waiting for the rest of the header;
`frozen_rpc_prefix_is_rejected_before_a_native_header_or_payload_is_read` checks
that boundary, unchanged guards and continued native service. The frozen oracle
also runs `frozen_tent_client_is_rejected_without_admission_or_payload` with
actual TENT descriptor, READ and WRITE clients. The native handler reports
`Error::Protocol` before source admission. YLT remains test-only.

Callers build a `TransferRequest` with a transport (or `Transport::Unspec`) and
two absolute addresses; the operation is chosen at submission, and an optional
engine-resolved QoS handle sets priority and caps. Public request, transport and
error types are non-exhaustive.

`TransferEngine` owns registration, batches, admission and directional shutdown.
`RegisteredMemory` retains the actual backing, and aliases share allocation-level
access coordination. `runtime::control_plane::ControlService` owns peer lookup,
request framing and source handlers, and dispatches payload work to the sibling
TCP, RDMA and NVLink transports. RDMA negotiates paths, connections and exact
memory grants over the control connection and moves bytes through verbs; see
[wire boundaries](tent-parity.md#wire-and-consumer-boundaries).

HP-TCP is built into the CPU transport set and is disabled by default. When
enabled, it replaces the ordinary TCP payload and retains the control service.
Each configured worker owns a single-thread Tokio reactor, the Rust counterpart to TENT’s per-worker Asio context. Persistent lanes
keep their worker affinity and use readiness-driven sockets with TCP_NODELAY.
Physical owners retain registration and task/byte admission through the last
slice receipt. The typed configuration, frame layout, static rail limits and
generation fence are described in [HP-TCP](hp-tcp.md).

SHM follows the shared allocator and synchronous copy in frozen TENT. TENT
can also copy a caller-supplied path from `MemoryOptions`. OrbiTE publishes
only allocations whose mapping and fd it owns, so an ordinary host buffer
cannot grant access to an unrelated inode. OrbiTE also publishes the inode,
allocation offset and registration grant. The source holds Linux OFD locks for
the permitted directions, and every requester holds a separate activity lock
through its copy. Those receipts fence a cached mapping across unregister or
process death. Local/native loans and peer copies coordinate on inode byte
ranges. Mapping failure precedes payload I/O, and failed unmap quarantines the
mapping and its charge. POSIX and directory-backed paths use the same owners.
See [shared memory](shm.md).

## Local file segment ownership

TENT's `SegmentManager` maps a name to one handle, `makeFileRemote` describes one
regular file with base zero, and io_uring passes the request offset straight to
positioned I/O. `segment/file.rs` follows that split: a bounded per-engine table
shares one entry per name, the size is cached for TENT's one hour, and refresh and
range admission read the size from the open fd, so replacing the pathname cannot
change the file an admitted operation uses. A lease keeps the fd until its I/O
finishes; close fences all aliases before waiting, and a timeout keeps the entry
charged until close is retried. See [file segments](storage.md).

An idle engine retains no storage thread or ring after the setup probe.
For DRAM/file payloads the caller or existing batch worker owns the io_uring,
iovecs, host range loans and bounce storage through CQE drain. File and peer
targets share admission and batch progress. CUDA/file submissions use the same
worker and ring for successive rounds. The optional file pool lends one
aligned slot per eligible request. A miss uses the charged pageable or proxy
fallback. Unlike TENT’s eager allocation, the slab is created on first use.
`r1c_aligned_slots_and_pageable_fallback_are_bounded_before_ring_creation`
checks capacity and preflight rollback. A mapped-CQ poll can recover a
failed wait, while the destruction deadline still quarantines missing receipts.
The whole GPU range stays loaned through file receipts and CUDA copy events.
A file vector has one file lease, admission owner and logical receipt. Overlapping
spans share an exclusive loan within their registration. The issuer retains
the original fragment order for kernel iovecs, staged copies and cuFile cookies.
Explicit file padding extends TENT's contiguous buffer contract. Host vectors
lease private charged scratch, and CUDA vectors use their pinned staging chunks.
Each lease stays exclusive until its completion receipt, so a READ that dirties
scratch cannot race a WRITE that needs zeros. Padding makes cuFile ineligible.
GDS discovery and CUDA buffer registration start at first eligible file use.
GDS binds a cuFile handle to the retained file entry and pins the CUDA
registration across queued and running batches. Its caller owns status polling
and quarantine. See the [file guide](storage.md) for completion and retention.

## Benchmark and fault boundaries

The [S26 harness](benchmarks.md) uses public engine calls; it adds no runtime
fast path or benchmark API. The frozen TENT build remains an isolated oracle
and performance baseline. Measurements require full-byte and guard checks;
logical status alone is not evidence. Fault fixtures reuse the executing owners
and record outstanding charges before the release receipt. A killed socket
requester, an injected CQ error and physical device loss are distinct cases.
GPU/RDMA/GDS installed fixtures stay unqualified until run on their hardware.

## Transport selector ownership

`runtime/transport_selector.rs` owns TENT-style first-match selection and
`transport_selector/config.rs` compiles bounded typed or JSON policies.
`segment/metadata.rs` owns the placement and provider metadata published in
Describe, plus the process identity shared with CUDA IPC. `TransferEngine::prepare`
resolves the rule index, transport, NIC mask and QP range before admission.
Every network request, local file operation and staged CROSS child carries that
decision. RDMA filters paths before allocating slices. Its endpoint owns QP
indices, native SL/TC and cache claims independently of CQ worker placement.
The pool layout is immutable across admitted work. See
[transport selection](transport-selection.md).

Library diagnostics use the shared `log` facade. OrbiTE installs no logger.
Consumers can enable selector warnings and errors through their own logger.

## Libfabric ownership

`transport/fabric/{fabric_attrs,fabric_context,fabric_transport}.rs` follows the
frozen TENT host path. The Linux-only feature builds a C inline-table bridge
against libfabric headers; every SDK object is opaque across that bridge and
`libfabric.so.1` is loaded at runtime. There is no libfabric link dependency.
Each installed domain has one joined progress worker, bounded MR and slice
charges, a CQ and an address-vector cache. [Libfabric](fabric.md) documents
configuration, grant framing and limits.

Deliberate differences have negative controls:

- Installation is opt-in; `default_build_never_selects_or_loads_a_fabric_owner`
  and `runtime_loading_attributes_and_configuration_are_explicit` check default
  isolation, absent libraries/symbols, exact domain filtering and delivery mode.
- TENT advertises long-lived whole-buffer keys. OrbiTE registers exact,
  operation-permitted source grants and requires their completion-bound nonce.
  `malformed_finish_cannot_release_a_published_grant` and
  `peer_exit_after_post_is_not_a_release_receipt` keep grants charged without a
  receipt. A completed source loan is released before its ACK; the real byte
  controls immediately reuse both ranges to check that publication boundary.
- EFA also requires delivery-complete WRITEs. Local transmit completion alone
  cannot authorize release at the target. Unsupported modes fail discovery.
- Partial post, CQ errors and timeout never replay a posted WRITE or claim a
  partial physical request succeeded. The partial-post/CQ-error control checks
  unknown effect and retained loans; the held-drain control keeps cookies and
  admission after the destruction window. Failed `fi_close` and `fi_av_remove`
  are sticky, with single-call counters and retained backing controls.
- Explicit fabric hints never substitute another transport, even on a clean
  registration rejection. The registration-rejection control enables retries
  and proves no payload moved; `Unspec` policy fallback has its own byte check.
- MR/slice/address limits supplement TENT's PTE and TX depth limits. The queued
  cancellation/capacity control proves rejection before payload. Provider
  environment is not mutated; the configuration control preserves `FI_HMEM`.

The configured EFA path remains implemented but unqualified. Software-provider
results establish neither EFA DMA safety nor physical-network performance.
NUMA preference follows TENT using each registration's published location;
unknown heap placement stays unknown instead of adding a page-location probe.
`numa_preference_keeps_unknown_unmatched_and_registered_fallback_rails` checks
that unknown or unmatched placement keeps every registered candidate and that
preference never invents a rail outside the MR's registered subset.
PTE accounting conservatively uses base pages instead of TENT's heap-page-size
probe; huge pages cannot reduce this charge. The over-budget chunk-assignment
control rejects a range that exceeds that base-page budget.

## Batch progress and notification ownership

The batch executor runs each admitted transport to its receipt. Submitting
work and completing an execution signal the same per-batch state that
`progress_batch` advances. TENT uses progress-lock shards. OrbiTE uses a lock
per live batch and a bounded weak-ID registry for admission and deferred
retirement. The optional engine worker owns a bounded wake set and joins on
drop. The typed `progress_fallback_interval` corresponds to TENT's
`runtime_queue/progress_fallback_interval_us`, and `notification_rpc_fallback`
corresponds to `notification/rpc_fallback`. A sweep probes retired owners independently and skips permanently
quarantined IDs. A source staging session continues to own its chunk and CUDA
loan after its socket disappears. Only a confirmed event and restored context
return the chunk. Incoming staging uses its own bounded source workers so a
blocked Delegate leaves Describe and credit traffic serviceable. A retained
session also keeps its admission slot. The held-Delegate and distinct-location
Pin controls cover these boundaries.
[Source staging](staging.md#remote-delegate-progress), [batch progress](async-batches.md#explicit-progress).

Notification routing visits installed channels before the control RPC. TCP and
self share the bounded control inbox. An async hook retains a charged set of
outstanding destinations. OrbiTE omits RDMA from notification routing because
its data grant QPs have no notification receive owner. An RPC-only engine can
send and receive even without an installed TCP notification channel. A lost
receipt precludes automatic replay. See [notifications](cpu-tcp.md#notification-contracts).

## Runtime queue ownership

| Owner | Responsibility | TENT counterpart |
| --- | --- | --- |
| `runtime/admission_queue.rs::LocalTransferAdmissionQueue` | Ordered eligible owners, per-batch dependency chains, dispatch counters | `LocalTransferAdmissionQueue`, `refillDispatchWindow` |
| `runtime/qos_contract.rs::QosContractResolver`, `qos_contract/admission.rs::QosQuota` | Compiled global/tenant/intent contracts and fixed per-contract counters | `QosContractResolver` (resolution only in TENT) |
| `runtime/receiver_credit.rs::SenderCreditLedger` | Cumulative grant/consumed accounting per peer binding | `SenderCreditLedger` (no runtime consumer in TENT) |
| `receiver_credit/issuer.rs::ReceiverCreditIssuer`, `sync.rs` | Fixed per-sender windows, tombstones and single-flight cumulative sync | No counterpart |

`runtime/request_merge.rs` maps original task IDs to physical owners after
registration, permission and route resolution. TENT sorts by hint and address.
OrbiTE also fences overlapping destinations and self-copy dependencies to
preserve public byte order. When either endpoint is CUDA, a change in the
selected transport, per-request remote staging or local/remote backing types
starts a new merge partition. This prevents a later direct alias from writing
through a pending stage or consuming its dispatch window. Disjoint host-only
hints can still merge across intervening requests. One source epoch, target and operation apply to the
whole submit. The peer advertises its transfer limit in Describe so a merge
cannot create an oversized grant on a smaller receiver.

Each physical owner holds its QoS permit, queue window and receiver credit.
Dispatch takes queue → QoS → credit locks. Credit sync and payload I/O run
outside them. EDF ranks independent batch heads. Reserving the only slot for
a successor would deadlock its FIFO worker. The worker advances its eligible
prefix while independent batches share the window. Files have no TENT memory-buffer key.
The direct file path keeps its native batches. When the runtime queue is
enabled, each file owner enters separately, as in TENT’s queued dispatch.

The source describes one exact location per registered view. TENT’s
`coalesceRegions` buckets multiple NUMA probe entries within one buffer.
OrbiTE has no such per-page location list. Adjacent registered views keep
their separate IDs, permissions and lifetimes. The queue reserves capacity
for a real proxy session, whose private CROSS tasks already have a bounded
lifetime inside that owner. This avoids reacquiring the parent’s window from
its worker. See [admission](async-batches.md#runtime-admission-and-dispatch-window).

### Retry ownership

The compiled selection fixes candidate order before merge. The existing batch
worker keeps a private, bounded decision slot for a drained completion. Poll,
explicit progress, cancel and free arbitrate that slot under the batch lock.
The same worker starts the next attempt. Public terminal states have no path
back to pending.

Frozen TENT permits duplicate same-content WRITEs after partial enqueue.
OrbiTE requires proof that a WRITE posted nothing. It has no general remote
revocation receipt for replay. A lost TCP ACK or a failed native WRITE can
therefore make the effect unknown. A drained READ can replay after QP retirement
or a confirmed CUDA copy and stage release. An unquiesced destination, an
overlap with the source, a short file result or an expired deadline prevents
that replay. The controls hold a real posting call, fail teardown, drop a WRITE
ACK and check the untouched guards. The file owners keep confirmed siblings.
A Describe RPC alone retries a connection error once, with the original
deadline and epoch. A replaced registration cannot inherit an admitted WRITE.

A disconnect can leave a source receive posted while the next grant has already
passed admission. Retiring that CQ lane returns `Unavailable` to both earlier
and later constructors, rather than misclassifying normal retirement as an
internal invariant failure that prevents policy fallback. The native QP/CQ
barrier still owns every pending receive, backing and charge. The
`receive_debt_retirement_rejects_new_grants_until_the_native_receipt` control
holds destruction and probes this interleaving; the real-control-socket
`source_receive_retirement_keeps_owners_and_allows_unposted_policy_fallback`
control checks merged aliases, one fallback, bytes and guards while the old
source owner remains charged. Any SQ or pending-work debt remains fail-stop even with a posted receive, as
`mixed_send_receive_debt_keeps_unposted_constructors_fail_stop` verifies.
Internal invariant failures remain non-retryable;
this is OrbiTE's drain-based admission behavior, not TENT's ambiguous replay.

## Metrics ownership

The optional per-engine registry preallocates the frozen transport, operation
and reason domains, including the failover pairs. Recording uses indexed
relaxed atomics. The physical admission owner carries timestamps and the first
failure stage across retries. The legacy transfer counters describe physical
owners. Lossless request counters include every accepted public alias. The
`metrics_attribute_each_retry_and_one_merged_outcome_to_the_real_transport`
control checks two aliases, a failed RDMA attempt and one successful TCP
attempt. Polling again leaves the totals unchanged. Counts have no authority to
release backing or admission. Registration gauges change at publication and final unregistration.
The exporter samples existing queue, QoS, receiver-credit and rail owners only
on a scrape. Benchmark-only QoS reports have no runtime series.

Frozen TENT uses process-wide ylt metrics. OrbiTE engines own separate
registries and use a bounded standard-library listener, so no C++ runtime or
HTTP dependency is needed. Each worker serves one connection with a 4 KiB
request-header limit and an absolute 500 ms I/O deadline. Dropping the engine
joins the HTTP workers and the optional periodic logger. A busy port degrades
to log-only recording. The loader rejects malformed values and caps the pool
at 64 threads. Tests exercise a busy port, incomplete and oversized headers,
per-engine isolation and toggle-safe gauges. A rejected CROSS and a skipped
suffix close recorded outcomes without releasing transfer leases. The CPU-only
TCP requester control checks both explicit peer capabilities and guarded
device bytes. A GPU Direct disable switch conflicts with an explicit Direct
request, preserving the registration policy. Invalid boolean spellings preserve
typed settings, and debug affinity logs omit memory addresses and keys.
See [metrics](metrics.md) and [RDMA configuration](rdma-cuda.md).

The offline diagnostics module and installed `orbite` binary share compile-time
build identity with the exporter. The CLI uses the standard argument iterator
and serde_json; it never starts an engine or accesses the network. Only version
text/JSON rendering and the exit-code table remain. Neither the CLI nor Python
uses arbitrary-document rendering or redaction, so those APIs, the C diagnostics
header and the `regex-lite` dependency are removed. The packaged CLI control
checks its identity and verifies the absence of network/thread syscalls.

## RDMA resource owners

A bounded rail table is shared by requester workers and the existing port
monitor. TENT keeps one table per worker. OrbiTE serializes only pair
accounting so two workers cannot arm concurrent trials. A receipt identifies
the admitted generation and trial. A late completion from before a port
fault cannot close the new circuit. `an_open_pair_admits_one_probe_and_only_its_receipt_can_reopen_it`
exercises that distinction. Discovery keeps peer epoch and NIC/PD identities,
rather than borrowing remote topology pointers. The grant path advertises the
remote memory owner's eligible NICs directly. A multi-slice submission uses
healthy pairs, or one trial grant when every permitted pair is paused.

The endpoint store fences admission before context shutdown. A record is
reclaimable after both native QP close and terminal grant retirement. An
external handle cannot construct into a fenced array. A finished logical grant
can still point to a live physical QP shared by other grants. Candidates check
the physical fence and claim the exact generation under the lane lock. The bounded store
retains a live or uncertain entry under pressure.


| Owner | Resource and lifetime | Frozen TENT correspondence |
| --- | --- | --- |
| `RdmaTransport` | Context slots, registrations, workers and parent dispatch | `RdmaTransport`, `context_set_`, `local_buffer_manager_`, `workers_` |
| `ContextSet`, `RdmaContext` | Stable NIC IDs, actual context/PD, route and persistent key domain | `RdmaContextSet`, `RdmaContext` |
| `LocalBufferManager` | Persistent host MRs keyed by actual context, allocation view and capability | Context-keyed `mem_reg_map` |
| `EndpointStore`, `RdmaEndpoint` | Peer identity, fixed-width construction slots and persistent QPs across CQ lanes | Context endpoint store, `RdmaEndPoint::qp_list_` |
| `RdmaQp`, `ConnectionResources`, `QpStore` | One QP shared by independently retained grant records | Endpoint QPs and affiliated slice ownership |
| `qp::send_queue::SendQueue` | Physical send depth, exact outstanding WR IDs and fenced admission | Per-QP `wr_depth_list_` and `slice_queue_` |
| `qp::receive_queue::ReceiveQueue` | Receive buffers/MRs, RQ debt and binding/key dispatch | QP-owned receive WRs; exact receipts are OrbiTE's |
| `RdmaGrant`, `GrantResources` | One exact-range DMA loan, source MW, SEND storage and grant charge | OrbiTE authority extension over registered regions |
| `RdmaSlice`, `SendWork`, `ReceiveWork` | Per-grant geometry, WR windows and completion identities | `RdmaSlice` work and completion state |
| `CqLane` | SQ/RQ CQ pair, grant records, submission, drain and recovery | Per-context CQs consumed by workers |
| `Workers`, `PostQueue` | Data threads, lane assignment, priority promotion and urgency ordering | `Workers::submit`, `asyncPostSend`, `promoteTimedOutRequests`, `orderByDeadline`, `asyncPollCq` |
| `SharedSlotManager` | Optional cross-process priority slots on existing workers | `SharedSlotManager::canSend` |
| `Monitor` | Event consumption and periodic context reclaim and recovery | `Workers::monitorThread`, `reclaimEndpoints`, `resumePausedContexts` |
| `DeviceSelector`, `DeviceInfo`, `SchedulingParams` | Compiled NIC eligibility, grant byte charges, bandwidth-weighted selection and estimates | TENT's classes of the same names |
| `CqQuota`, `GrantCredits`, `ReceiveCredits` | Aggregate SQ/RQ wakes and whole-parent grant/receive admission | Bounded slice admission; receive credits are OrbiTE's |

Each context slot owns its PD, CQ lanes and peer endpoint registry. Several
grant records can share one QP, which keeps its send and receive queues across
grants; peer arrays hold weak lane references to avoid ownership cycles. Native
cleanup is fallible: a failed QP, MR, MW, CQ or channel teardown keeps the
remaining owner, backing and charges, and a timeout never substitutes for
physical drain. See [native cleanup](native-cleanup.md) and the
[rail accounting gate](installed-host.md#physical-rail-accounting).

## Fixed-width cold construction

`EndpointStore::get_or_insert` gives every caller the same `RdmaEndpoint`, with
`min(local.num_lanes, remote.num_lanes)` slots keyed by local context, remote
process/PD/NIC, role and capability; staged and device paths use per-grant
connections instead. An uncloneable `Construction` reserves a slot, attaches the
QP owner before native creation and survives until the pair is published.
Waiters release all locks and use their own deadlines, and a unique token stops
an old guard from releasing a replacement slot. The source publishes its pair
before GRANT, so later handshakes share it; a fresh bootstrap may retire an old
**idle** pair but never an active sibling. TENT's
`getOrInsert` / `construct` / `EP_HANDSHAKING` follow the same lock discipline,
with an eager bidirectional QP list instead of OrbiTE's lazy per-capability
arrays.

## QP send ownership

`SendQueue` admits at most `max_send_wr` unresolved WRs across all work on a QP,
also charging the aggregate CQ budget. Payload, MW bind and completion SEND share
it. A failed post returns only a proven unaccepted suffix; an unknown post keeps
the whole possible debt and fences admission. An ordered RC CQE repays its
tail and the consecutive unsignaled WRs in the same posted group. Each grant
has its own ID range and each actual post ends in a signaled WR. A non-flush
error preserves successful earlier WRs. A flush retires the prefix with zero
successful bytes and cancels only its own rail probe. An uncertain WRITE keeps
its original effect. The partial-post and guarded WRITE controls exercise these
boundaries. Dropping a work handle never returns SQ credit. CQ lookup resolves the
exact record before checking QPN, WR identity and operation, and native access
follows grant → QP → context lock order. GPU sources that opt into reuse borrow the published local-only MR on rails that prove exact,
revocable indirect keys. A fresh key is bound to one QP and direction. The
private PD and exact MR remain the path for other providers. Frozen TENT
publishes a registration-lifetime key. OrbiTE's stale-key, guard-byte and
failed-destruction controls exercise its per-grant authority. See
[GPU ownership](rdma-cuda.md#gpudirect-rdma-ownership).

The `rdma-adaptive` Cargo feature exposes the shared v0.3.14 congestion
core. A device and each route have separate byte windows. A domain-local lock
serializes reset and tick against concurrent feedback. Admission in enforce mode
rolls back the device reservation if the route defers. Only an explicit receipt
returns a permit’s charge. The moved/dropped permit control covers this
difference from C++ move assignment. The TENT worker has no call site for this
core and OrbiTE keeps the same integration boundary.

## QP receive ownership

`ReceiveQueue` owns registered receive slots and their admission credits. A
successful RQ CQE names the consumed buffer by WR ID, while its 141-byte binding
and invalidated rkey identify the completed grant, which may differ from the
buffer's donor. Duplicate, stale or mismatched messages cannot complete another
receipt. RQ depth uses `max_cqes`, so a parent can prepare all receives even when
its grants exceed the SQ width, and receive credits stay charged until the
consumed buffer's MR is reaped. TENT also owns WRs at the QP and dispatches CQEs
to work identities; the bound source receipt is OrbiTE's authority extension.

## Follow one transfer

1. **Register.** Host Direct prepares local-only MRs and MW-bind parents in every
   usable context, including paused ones, before the range becomes visible.
2. **Plan.** TENT's slice geometry splits the parent; `DeviceSelector` picks a
   rail per slice from the owner's location, masks, strict NUMA, bandwidth
   estimates and byte charges.
3. **Authorize.** Both peers reserve the parent's grants, and the source reserves
   all receive credits before exposing the first grant. A fresh one-use MW
   authorizes only the requested range, and every slice is prepared before
   payload submission.
4. **Progress.** Worker `i` polls lane `i` across contexts; new grants pick the
   least-loaded lane, and reused QPs keep theirs.
5. **Complete.** Payload completion, the fenced completion SEND, the exact source
   receive and bound DONE/ACK/READY establish grant drain; the parent succeeds
   only when every slice has. Ambiguous WRITEs are never replayed.

All contexts share aggregate byte, QP, grant and SQ/RQ limits. Fully drained
faulted lanes can be replaced while the PD, MRs and worker pool stay alive.
Workers bind to NUMA CPU sets; this does not move application pages.

The frozen RDMA quota has two scopes. The selector charges exact slice bytes
in one process. A configured POSIX shm name shares priority eligibility among
processes on the same host. OrbiTE uses a same-UID header, bounded nonblocking
file locks and an aligned atomic slot. A process killed under the lock cannot
strand its peers. A stale Low slot cannot extend eligibility past its clock
boundary. The Linux controls kill a publisher with a live device lease, check
the surviving NIC charge and reject a foreign header without modifying it.
Pool shutdown joins the publisher and consults the single unmap receipt. An
unconfirmed unmap retains the mapping and fd and returns `Unquiesced`.

TENT's receiver ledger names four resources and assumes its caller has fenced
the old epoch. OrbiTE checks its physical permits and their completion
receipts before retiring even a manual epoch. The lost TCP WRITE acknowledgement
control checks that an applied payload with no receipt cannot free credit.
Manual grant updates never release automatic Pin leases. QoS uses one resolved
budget per contract. The parser controls reject unexecutable actions by name
at every policy layer.

## Periodic monitor reclaim and port recovery

The monitor scans contexts once per second, even without traffic, using
nonblocking inspection so a destructor waiting for an event ACK cannot stall it.
Faulted lanes and endpoint slots are reclaimed only after native drain.
Discovery keeps an initially down port as a paused context that accepts MR
registration but rejects routes. `RdmaContext::refresh_address` queries the
selected port and GID; paused automatic configuration reranks GIDs on the same
port, while pins stay strict. A monotonic route generation stops an old query
from clearing a newer fault, and recovery needs drained lanes, a successful query
and an unchanged generation. This follows TENT's one-second reclaim/resume pass
and `activateContext`, while keeping physical destruction on the data workers.
See [the recovery contract](rdma-cuda.md#initially-paused-contexts-and-gid-reselection).

## Source reading order

Start at an operation, then follow the resource it borrows. Child modules extend
their resource owner; submission and cleanup stay beside the resources they protect.

| Read | Location | Responsibility |
| --- | --- | --- |
| Public request | `engine.rs`, `batch.rs`, `memory.rs`, `segment/` | Engine, tasks, backing and file segments |
| Runtime | `runtime/transport_selector.rs`, `admission_queue.rs`, `qos_contract/`, `receiver_credit/` | Selection, admission, QoS and credits |
| Transport entry | `transport/rdma/rdma_transport.rs` | Registration, parent dispatch and source serving |
| Context orchestration | `rdma_transport/contexts.rs` | Initialization, selection, CQ attachment, recovery and shutdown |
| Context owners | `context_set.rs`, `context.rs`, `context/key_domain.rs`, `discovery.rs`, `quota.rs` | Stable slots, PD/key history, discovery and NIC admission |
| Peer rendezvous | `rdma_transport/connections.rs`, `rdma_transport/handshake.rs` | Fixed-width connection claims and bilateral publication |
| Grant construction | `grant/construction.rs`, `rdma_transport/requester.rs` | Admitted buffer, credit and connection ownership across MR waits |
| Wire data | `protocol.rs`, `path.rs` | Grant receipts and static NIC/PD path metadata |
| Persistent owners | `buffers.rs`, `endpoint.rs`, `endpoint_store.rs`, `qp.rs`, `qp_store.rs`, `grant.rs` | MR, peer, QP and grant lifetime |
| QP work ownership | `qp/send_queue.rs`, `qp/receive_queue.rs`, `slice.rs`, `receive.rs` | Physical SQ/RQ ownership and receipt dispatch |
| CQ execution | `cq_lane.rs`, `cq_lane/` | Lane lifecycle, admission, reuse, retirement, dispatch and polling |
| Scheduling and events | `workers.rs`, `workers/post_queue.rs`, `promotion_policy.rs`, `shared_quota.rs`, `bw_arbitration.rs`, `monitor.rs`, `events.rs`, `cq.rs` | Data threads, priority and urgency ordering, events and CQ/channel owners |
| Verbs boundary | `sys.rs`, `sys/` | Fallible function table and native calls by resource |
| Staging | `runtime/proxy_manager.rs`, `runtime/proxy_manager/`, `cuda/staging.rs`, `platform/numa.rs` | Pinned chunks, remote operations, rings and CUDA copy/drain |
| Host and GPU | `platform/cpu_probe.rs`, `cuda/`, `transport/nvlink/` | Host discovery, NVIDIA copy, GPU DMA and IPC owners |

Paths after the transport entry are relative to `src/transport/rdma/`, except
the public, runtime, staging and host/GPU rows, which are relative to `src/`.
The installed fixture `tests/hardware/rdma_host.rs` crosses these owners through
the public API; see [installed host](installed-host.md). Private tests live under
`tests/unit/`, mirroring `src/`.

## P4 source audit

The audit compared OrbiTE `f2c7e7f` (PR #55) with frozen TENT
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`. At that point both RDMA
implementations were about 11k physical lines (OrbiTE 38 Rust files, TENT 28
`.cpp`/`.h` files, tests excluded). Per-request MW authority, exact source
receipts, bilateral epochs and conservative retention are deliberate OrbiTE
contracts beyond TENT's registered-region path. The audit found two gaps
against the [context implementation at the audited revision](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/transport/rdma/context.cpp), both since implemented:

| Behavior | Frozen TENT | OrbiTE at `f2c7e7f` | Current source |
| --- | --- | --- | --- |
| Initially down port with valid device/GID | `RdmaContext::enable` keeps it as `DEVICE_PAUSED` | Discovery filtered non-active ports | Paused context/PD kept; MR publication includes it, route admission excludes it |
| Automatic GID reselection on recovery | `refreshAddress` reruns `getBestGidIndex` | Refresh queried the existing index | Paused refresh reranks GIDs on the same port; pins stay strict |

Both pass CPU transitions and the
[focused native gate](qualification.md#focused-native-recovery), with
[independent review](qualification.md#p4-independent-review). Physical and
fabric qualification stays in the [P4 queue](completion-plan.md#p4-remaining-delivery).

## Correspondence and remaining differences

The reference is frozen at `e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c`; see
TENT's [transport header][transport], [endpoint][endpoint] and [workers][workers].

| Area | Current OrbiTE | Difference from TENT |
| --- | --- | --- |
| QP arrays | Fixed-width, lazily populated, concurrent grants, drain-aware rollback | TENT builds its QP list eagerly and shares it across directions |
| Monitor | Unified get/ACK, periodic reclaim, paused contexts, GID reselection, rate refresh | OrbiTE keeps retained-resource quarantine |
| Path metadata | `path.rs` validates NIC/PD descriptions and compatible pairs; `rail_monitor.rs` gates selection and tracks native receipts | TENT `RailMonitor` |
| Grant authority | Exact-range host MW or private GPU PD/MR, with a bound source receipt | TENT publishes native MR keys; not wire parity |
| CQ layout | Separate SQ and RQ CQ per lane | TENT uses one lane CQ for data QPs |
| Worker load | Unfinished grant charge, including retained native work | Failover and remaining deadline policy are P6 |

[transport]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/include/tent/transport/rdma/rdma_transport.h
[endpoint]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport/rdma/endpoint.cpp
[context]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport/rdma/context.cpp
[workers]: https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/transport/rdma/workers.cpp

## CUDA copy stream ownership

The caller-stream local copy engine is an OrbiTE extension beyond TENT. It
takes the DMA compiler and grid-strided copy from OrbitKV. The caller supplies
layout and allocator leases. OrbiTE resolves physical registration identities,
bounds each flight, and records an event after its final enqueue, including
partial failures. An event receipt retires loans only after exact context
restoration. Separate pinned and device descriptor slots let outstanding kernel
launches retain their own upload. `pending_events_own_distinct_kernel_slots_payloads_and_abandoned_work`
and `query_destroy_record_or_restore_failure_quarantines_once_and_keeps_owners`
exercise those boundaries. [Caller-stream API](local-copy.md)

| Owner | Responsibility |
| --- | --- |
| `cuda::CudaMemory` | Allocation lock and primary-context reference |
| `cuda::stream::CopyStream` | Lazy nonblocking stream, reusable completion event and pending-work state |
| `cuda::stream_pool::CUDAStreamPool` / `CUDAStreamHandle` | Bounded context-matched streams shared by NVLink groups |
| `cuda::driver` | Fallibly loaded Driver API functions |
| `cuda/readiness.rs::CallerSyncEvent` | Producer event generation, its context and retained producer inputs |
| `cuda/pinned.rs::PinnedAllocation` | Pinned host allocation, context and lazily created readiness event |
| `runtime/proxy_manager.rs`, `proxy_manager/` | Shared staging chunks, remote sessions, rings and worker shards |

The Driver API guard saves and verifies the caller’s exact context, including
a null binding. Copy and final pinned-buffer cleanup errors reach the caller.
An uncertain restore poisons the owner even if the visible binding can be
repaired. The same-device custom-context and no-context controls inject failures
at save, switch, copy and restore. Host and CUDA allocations dispatch by owner,
so a handled CUDA failure cannot fall through to host memcpy. External device-plugin callbacks are out of scope; built-in copies retain
these context and completion controls.

Each asynchronous copy holds its staging until the recorded event completes. A failed enqueue
or record requires a proven stream drain; otherwise the allocation, staging,
stream/event and context are retained. Destroying a stream or event never proves
DMA completion. This follows TENT's
[CUDAStreamPool](https://github.com/OrbitInfra/Mooncake/blob/e1ea5d31bb5e46ff2df9f90e7c2fbaf52fcdee8c/mooncake-transfer-engine/tent/src/platform/cuda/cuda_stream_pool.cpp)
handles and NVLink's event-after-copy completion. The staging owners and their
progress states are described in [CUDA staging](staging.md#tent-correspondence),
and the readiness handoff in
[producer readiness](rdma-cuda.md#producer-stream-readiness).

## Platform pointer and location ownership

`memory/location.rs` defines TENT-style memory types and bounded `RangeLocation`
metadata supplied by the actual owners; CUDA-pinned host memory stays CPU memory.
`cuda/probe.rs` validates external CUDA pointers with one batched attribute query
and a mapped-range check, and caches each context's PCI/NUMA discovery. This
follows TENT's `Platform` / `MemoryProber` responsibilities without a global
raw-pointer registry; see [memory location](rust-api.md#memory-type-and-location).

`cuda/probe/topology.rs` supplies TENT's `CudaProbe` inventory to RDMA discovery,
`platform/pci.rs` resolves PCI/NUMA and bridge distance, and
`runtime/topology.rs::MemEntry` stores host and CUDA locations with three NIC
tiers. `runtime/topology/config.rs` implements TENT's `loadFromConfig`,
`parsePriorityMatrix` and `parseCustomTopology`. `DeviceSelector` compiles all of
this into rank arrays and masks once, so slice allocation does no driver, sysfs
or string work, preserving TENT's `CudaProbe → Topology → DeviceSelector` chain.

| Owner | TENT responsibility |
| --- | --- |
| `quota/scheduling.rs::SchedulingParams`, `quota/bandwidth.rs::DeviceInfo` | Bandwidth policy, selection EWMA and busy-time meter (`setDeviceBandwidth`, `noteCompleted`, `maybeSampleTransmit`, `getNicLoadStats`) |
| `quota/allocation.rs` | `buildCandidates`, `selectSinglePath`, `selectMultiPath` |
| `sys/port.c`, `sys/port.rs`, `RdmaContext` | `recordPortSpeed`, `queryEffectiveSpeed`, `refreshPortAttributes` |
| `workers/post_queue.rs`, `bw_arbitration.rs`, `promotion_policy.rs` | Priority tiers, `OrderByUrgency`, request promotion |
| `shared_quota.rs::SharedSlotManager` | Mapped current priority slot and joined monotonic publisher; per-NIC leases remain in DeviceSelector |

Physical grant release stays separate from payload sampling, and route identity
excludes link speed, so a rate change keeps existing QPs. See
[bandwidth-aware selection](rdma-cuda.md#bandwidth-aware-rdma-selection).

## CUDA IPC transport ownership

| Owner | Responsibility |
| --- | --- |
| `transport/mnnvl/` and `cuda/fabric` | Fabric-domain capability and grant binding, CUDA VMM references, VA reservations, access descriptors and EGM socket placement |
| `transport/nvlink.rs::NVLinkTransport` | Legacy or fabric identity checks, grant and close bindings, mapping quotas, stream pool and completion progress |
| `cuda/ipc.rs::IpcExport`, `IpcMapping` | Allocation-wide source loan; one imported reference with its context and touched local allocations |
| `transport/nvlink/mapping.rs::OpenedMapping` | Mapping charge and explicit native close |
| `transport/nvlink/subbatch.rs::NVLinkSubBatch` | Consecutive same-region, same-context copies |
| `cuda/ipc/copy.rs`, `cuda/ipc/batch.rs::CopyBatch` | Deduplicated owner locking, ordered enqueue and native batch arrays |
| `transport/nvlink/progress.rs` | Inline ready check, then one lazy worker querying pending events |
| `transport/nvlink/cache.rs::RelocationCache`, `export.rs::ExportCache` | Optional import cache and persistent source export lifetime |

The stream handle is acquired before memory locks and returned before close and
control I/O; loans survive event polls without holding allocation mutexes. TENT
keeps a transport-wide relocation cache and queries the task event in
`getTransferStatus`; OrbiTE closes imports per subbatch by default and adds
explicit close receipts. See [CUDA IPC](cuda-ipc.md).

MNNVL shares this completion owner and its bounded import/export cache. The
creator drops its physical handle reference after mapping. Local free retains
the handle again, unmaps, releases it and frees the VA. An imported mapping
keeps its physical reference until the same teardown sequence. Frozen TENT
clears its relocation table on uninstall. OrbiTE uses explicit native
receipts, bounds whole mapped extents and preserves the actual owners on
uncertain cleanup. New device and EGM mappings are zeroed through the rounded
extent before their first grant. Handle publication requires an admitted
range in the original epoch. See [MNNVL and EGM](cuda-ipc.md#multi-node-nvlink-and-egm).
