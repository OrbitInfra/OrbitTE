# CPU TCP foundation

This covers the implemented P1.1/P1.2/P1.3/P1.4/P2.1/P2.2 substages in the
[completion plan](completion-plan.md), pending independent acceptance. It
provides original Rust code with no dependencies and no C++ TENT runtime.
The source changes have not been published; crates.io `0.1.0-alpha.1` still
contains only the original package bootstrap.

## Architecture correspondence

The reference is Mooncake TENT
[`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`](https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent).
Use its ownership boundaries while implementing the OrbitKV consumer path:

| TENT responsibility | Current Rust owner | Delivered subset |
| --- | --- | --- |
| `runtime/transfer_engine_impl` | `src/engine.rs`, `src/batch.rs` | Immutable config, synchronous/async batch ownership, ordered execution and requester/source drain |
| `runtime/segment`, `segment_tracker`, registration lifecycle | `src/memory.rs`, `src/segment.rs` | Shared allocation owners, reference-counted ranges, base/length descriptors, bounds/permissions and epoch fencing |
| `runtime/control_plane`, RPC dispatch | `src/runtime/control_plane.rs` (`ControlService`) | Direct P2P descriptor open/explicit refresh and source-owned handlers |
| `transport/tcp` | `src/transport/tcp.rs` (`TcpTransport`) | Host byte movement, deadlines, complete READ response and final WRITE acknowledgement |
| Local/control notification queues and submit hooks | `src/notification.rs`, `src/engine.rs`, `src/runtime/control_plane.rs` | Bounded named messages, atomic polling and synchronous batch-completion delivery |
| Config/resource observations | `src/config.rs`, `TransferEngine::stats` | Per-engine budgets and retained/admitted resource counters |

The engine selects parallel TCP and RDMA transports directly. `ControlService`
owns the shared socket service, descriptor/notification protocol, source
admission and validated dispatch. `TcpTransport` owns TCP payload READ/WRITE;
`RdmaTransport` owns RDMA bootstrap, native work and authorization receipts.
RDMA uses a `ControlChannel` for setup without passing through `TcpTransport`.
Frozen TENT also shares its control service with transports; its TCP payload
RPC handlers live in `ControlService`, while this Rust layout places the byte
behavior in `TcpTransport` and dispatches from the service. No generic forwarding
transport facade or Mooncake fallback is present. Cache keys, replica/source
grants, GPU allocation and recovery policy remain with OrbitKV.

## Rust contracts

- `register_memory(Box<[u8]>, Permission)` consumes actual backing. Handles and
  admitted handlers retain it through `Arc`. Local byte access and remote copies
  serialize through the allocation's mutex. Remote permissions are checked
  by the source regardless of requester metadata.
- `HostMemory::from_raw_parts(address, length, owner)` is an explicit unsafe
  boundary for pre-existing host-accessible allocations, including externally
  pinned backing. `owner` must keep the real, stable, writable allocation alive;
  external CPU/device accesses must respect the documented handoff contract.
  This does not make a CUDA device pointer host accessible or pin any pages.
  `HostMemory::new(Box<[u8]>)` provides the fully owned alternative.
- `register_host_memory(Arc<HostMemory>, offset, length, permission)` registers a
  checked subrange. Overlaps use the same allocation lock, including across
  engines. Overlapping wrappers with different locks are rejected while an old
  registration remains active or retained. The retired-range index uses weak
  references and does not call user-owned destructors under its registry lock.
- `register_host_memory_batch(&[(Arc<HostMemory>, offset, length)], permission)`
  returns handles in input order. It preflights the whole batch under the registry
  lock, then publishes new ranges and duplicate reference increments together.
  Failure leaves existing references, IDs, exports and retained charges intact.
  Empty batches succeed while source admission is open. The per-call descriptor
  limit is `max_batch_requests`; retained limits count distinct ranges. New
  duplicates within one Rust batch are coalesced too; see the native distinction
  below. This CPU metadata transaction does not qualify provider rollback/free.
- Exact duplicate registrations share one ID and charge one retained range.
  As in TENT's `SegmentTracker`, the first descriptor/permission wins; another
  registration does not upgrade it. Each registration call adds one explicit
  reference. `RegisteredMemory::clone` only shares ownership; it does not add
  another explicit registration reference.
- Registration IDs increase for one engine lifetime and are never reused.
  `unregister_memory` removes one explicit reference, with the final reference
  removing lookup visibility. Existing handlers may finish;
  their backing and budget charges remain until all owners release them. Local
  handles continue to own usable memory after unregister or engine destruction.
- `resolve_local_address` and `RemoteSegment::resolve_address` require one range
  to cover the complete request. They use TENT's address-ascending, length-
  descending containing-buffer order, independently of wire enumeration order.
  They return the registration and relative offset, never dereference an
  unregistered raw address, join adjacent regions or acquire cache ownership.
- `open_segment(endpoint, expected_epoch, timeout)` takes the random 128-bit
  epoch from the authoritative source owner. It cannot learn replacement
  authority from a new process at the same address. Every operation checks
  that epoch again at the source. Refresh by calling open with the same epoch.
- `TransferRequest { local_address, remote_address, length }` accepts the absolute
  addresses used by TENT and OrbitKV transfer slices. These integers carry no
  ownership or authority. Submission resolves active local ranges and the
  supplied peer snapshot, retaining every local registration before I/O.
  Concurrent unregister after resolution cannot free admitted backing; a later
  submission must resolve an active registration again. Caller handoff still
  governs address reuse. Remote requests carry the resolved registration ID and
  the supplied peer's authoritative epoch. The previous public handle/offset
  request fields were removed before release; no compatibility alias is kept.
- `submit_and_wait` validates every slice before network effects. It then runs
  requests in order and stops at the first execution failure. Reports contain
  the completed prefix, exact completed bytes and the failed index; later
  requests were not attempted. A batch is not a transaction.
- READ receives a complete bounded payload before changing its local slice.
  WRITE copies its local input, waits for source readiness, sends a complete
  payload, then waits for an acknowledgement after the source commits it.
  Concurrent local writers can change data between slices; this is not a
  whole-registration or whole-batch snapshot.
- A WRITE failure after attempting its body returns `RemoteEffect::Unknown`.
  The source may already have applied it. There is no automatic retry. In this
  CPU profile, TCP uses owned staging bytes, so an uncertain remote effect does
  not require retaining the requester's original allocation for DMA.
- One absolute requester deadline covers the batch's connects, frames and
  payloads. A separate source deadline starts on accept and includes queue
  time. Socket polling does not extend either deadline. Deadlines are I/O
  bounds and admission checks, not hard real-time bounds on CPU copies or locks.
- `close_source(timeout)` permanently closes source admission, then waits for
  admitted source operations to exit. Timeout is not a drain receipt; admission
  stays closed and a later call can wait again. Success establishes source
  handler quiescence, not delivery of an acknowledgement to another process.
  A zero timeout closes admission and checks immediately for quiescence.
  Requester operations remain available. Engine drop stops accepts, interrupts
  socket waits and joins its workers before their final references are released.

Async allocation, task polling, cancellation, detach and explicit free are
specified in [the asynchronous batch contract](async-batches.md). They use the
same validation and data executor as synchronous calls. `locally_drained`
refers to requester owners; a lost remote WRITE receipt still has an unknown
remote effect and requires the source's own drain/ownership contract.

## Notification contracts

`Notification { name, message }` carries application-defined UTF-8 strings;
empty strings and deliberate repeated messages are valid. Each successful send
enqueues one message. `send_local_notification` delivers in process, and
`send_notification(peer, notification, timeout)` uses the same path for a peer
whose endpoint and epoch identify this engine. Other peers use epoch-fenced TCP.
Receipts confirm enqueue, not application consumption or persisted delivery.

`receive_notifications()` atomically drains the shared queue in enqueue order
into an owned vector; empty polling returns an empty vector. Concurrent pollers
receive disjoint messages. Sends are ordered by actual enqueue, including local
and remote sends; separate connections have no stronger global ordering.
Name matching, generations, counters and waits belong to the consumer. OrbitKV
already has a generation mailbox and one serialized native polling owner.

`submit_and_notify` sends once to the batch peer only after every data request
completes. Empty batches and failed prefixes leave `NotificationStatus::Skipped`.
Oversized notifications fail validation before any data request. A single
absolute deadline covers data and notification I/O. `BatchReport::is_complete`
reports data completion; inspect `report.notification` separately. A full inbox
can produce successful data bytes plus `NotificationStatus::Failed`. Such a
result never authorizes replaying committed data.

The receiver reserves count/bytes before sending ready. Partial, invalid UTF-8
or malformed bodies never enter the inbox; reservations release only when the
handler exits or commits. Draining unrelated messages cannot release an active
reservation. Source close rejects new local/remote sends, drains admitted sends
and leaves already queued messages readable until engine destruction.

`NotificationFailure.effect` is `None` before a body can be committed and
`Unknown` after body submission without a final receipt. Unknown outcomes are
never automatically replayed. Manual retries can duplicate a delivered message;
this profile has no message-ID deduplication or durable exactly-once guarantee.
Queue limits and UTF-8 validation are explicit Rust-profile restrictions.

At the frozen revision, [`TransferEngineImpl`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp)
owns the local queue and notification hooks, clears each receive poll, and
merges transport/local deliveries. A hook records unique request targets,
waits for all its tasks, and removes each successfully notified target so later
polls do not redeliver it. Empty submissions have no target to notify.
[`local_notification_test.cpp`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/tests/local_notification_test.cpp)
checks self-delivery, completed-transfer hooks and repeated polling;
[`notification_fallback_test.cpp`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/tests/notification_fallback_test.cpp)
restricts fallback to unavailable channels, not ambiguous posts. This is source
correspondence, not an executed native differential comparison. Multi-target
asynchronous hooks, transport merging/fallback and the native C free/output
conventions remain open. The Rust CPU path makes one attempt and reports an
uncertain receipt rather than retaining a hook that retries it on later polls.

## Budgets and profile

| Default | Value | Accounting |
| --- | --- | --- |
| Bind | `127.0.0.1:0` | OS selects a loopback port; an explicit routable bind is required for other hosts |
| Retained registrations | 1,024 / 1 GiB | Counts retained registration ranges, including unregistered handles/handlers; exact duplicates charge once, distinct overlapping ranges each charge; protocol limit 4,096 regions |
| Batch descriptors | 1,024 | Transfer or host-registration batch rejected before effects if exceeded |
| Allocated async batches / descriptor slots | 32 / 32,768 | Includes open, completed-but-unfreed and detached-active batches; at most one worker per allocated batch |
| In-flight requester submissions | 32 | Queued/running async groups plus synchronous, descriptor-open and explicit notification calls |
| In-flight requester bytes | 64 MiB | Admitted slice lengths plus notification name/message bytes until the call exits |
| One payload | 8 MiB | Configurable up to 64 MiB; each source independently enforces its limit |
| Source workers / queue | 4 / 4 sockets | Fixed worker pool and bounded socket queue; queue overflow closes the connection |
| Source timeout | 5 s | Monotonic accept-to-handler deadline, configurable in `(0, 300s]` |
| Pending notifications | 1,024 / 1 MiB | Queued messages plus reserved incoming bodies; empty messages still consume a count |
| One notification | 64 KiB | Combined UTF-8 bytes of name/message, configurable downwards and enforced at both ends |

Only Linux host-memory loopback is tested in this delivery. Entropy comes from
`/dev/urandom`; failure to read it fails construction. This protocol uses plain
TCP in a trusted environment. Epochs fence source restarts; they are not peer
authentication. There is one connection per request, sequential batch execution,
bounded payload staging and no topology selection or performance claim.

`stats()` samples retained regions/bytes, active source operations and requester
batch/byte admission, queued notifications and retained notification count/bytes
(including reservations). Counters are sampled under separate locks, not one global
atomic snapshot. Full TENT metrics and NIC-load observations remain open.
These counters bound registered/retained ranges, not the footprint of an
arbitrary external allocator owner or caller-held `HostMemory`. OrbitKV retains
responsibility for its pool's allocation/recycling budget and source grants.

## Native batch inputs and the OrbitKV boundary

Frozen TENT's `registerLocalMemory(vector, vector, options)` constructs buffer
records and calls [`SegmentTracker::addInBatch`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/segment_tracker.cpp).
Duplicates already in the published snapshot increment references and keep the
first descriptor; callback failure rolls those increments back. The shared
[CPU trace](cpu-oracle.md) executes its C `tent_register_memory_batch` on such
existing ranges and verifies partial/final unregister and first-permission WRITE.
Rust instead preflights its CPU-only metadata changes before committing them.
Frozen `addInBatch` can publish duplicate new descriptors from one input list;
Rust coalesces those into one ID with multiple references. That case has Rust
contract tests, not a native differential parity claim. Provider cleanup failure,
NUMA probing and location/transport options remain open.

At OrbitKV `3f4d65709e86a060f323da7e0d1b59975f6939bc`,
`MooncakeTransport` retains its actual `Arc<PinnedAllocator>` before registering
`memory_regions()`, and submits `TransferSlice { local, remote_address, length }`.
The Rust input boundary now supports that shape: wrap each disjoint host region
with `HostMemory::from_raw_parts` retaining that actual allocator, batch-register
its ranges, and pass addresses at submission. Overlaps must share one wrapper.
The external-host example demonstrates this boundary with a stable host box;
it does not instantiate OrbitKV's private CUDA-pinned allocator.

OrbitKV's native pin `12cbf549b6351c800de0f94a480276d216b9a43b` appends
`expected_source_epoch[33]` to the C request and adds source-epoch/close functions.
The frozen vanilla C request lacks that field. This Rust struct is neither C
layout, and linking either native ABI requires an explicit audited profile.
OrbitTE's synchronous calls and source-only close also do not yet replace
OrbitKV's asynchronous batch/cancellation ownership or its combined local
requester/source admission close. These remain adapter gates before migration.

## Native TENT correspondence for P1.2

At the frozen revision, [`BufferDesc`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/include/tent/runtime/segment.h)
carries the base address and length. [`SegmentTracker`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/segment_tracker.cpp)
increments exact-duplicate reference counts, retains their first descriptor,
sorts by address/descending length and removes the descriptor on the final
unregister. [`SegmentDesc::findBuffer`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/segment.cpp)
requires full containment and rejects overflowing ranges. The Rust unit cases
exercise those selected contracts. Native `local_memory_lifecycle_test.cpp`
also establishes descriptor refresh/removal and ownership retention on failed
provider free; the latter still needs a real Rust provider before parity can be
claimed. These are source comparisons, not a completed C++ differential oracle.

The Rust allocation owner and shared lock make native buffer lifetime and alias
responsibilities explicit. Source epochs and non-reused registration IDs remain
OrbitKV-required safety contracts; address discovery cannot replace authority.

## Development wire contract

This is **OrbitTE protocol version 9**, not TENT's YLT RPC framing or HP-TCP.
It makes this first Rust CPU slice executable while TENT wire compatibility
remains an explicit P0/P9 gate. Future wire changes require a version change
and corresponding decoder rejection tests.
Version 2 added base addresses; version 3 adds reserved notification admission,
body submission and final receipts. Version 4 added staged RDMA bootstrap/commit operations. Version 5 allows
explicit direct application host MRs: a WRITE attempt can change application
bytes before DONE/ACK, and a failed direct READ can change its local target.
Version 6 splits RDMA connection metadata from completed one-use MW grants;
BIND/INV completion precedes grant publication/final ACK. Version 7 binds
both process/PD/connection incarnations, the exact request and an independent
source grant generation to every RDMA phase and receipt. Version 8 requires
an actual source RQ receipt after same-QP fenced SEND_WITH_INV, while retaining
both physical QP-close barriers. Version 9 adds an explicit bilateral connection
rendezvous, a bound host-Direct release profile and ACK/READY completion for
connected RDMA reuse. Versions 1–8 are explicitly rejected.
There is no legacy-protocol fallback.

Integers are unsigned, big endian. Each connection carries one operation.
Both headers start with eight bytes `ORBITTE\0`, a two-byte version (`9`), a
one-byte operation/status and one zero reserved byte.

| Frame | Remaining fields |
| --- | --- |
| Request, 52 bytes | Expected epoch (16), registration ID (8), offset (8), length (8) |
| Reply, 36 bytes | Actual source epoch (16), payload length (8) |
| Descriptor payload | Region count (4), then entries sorted by unique ID: ID (8), base address (8), length (8), permission bits (1) and zero reserved bytes (7) |

Operations: `0` describe, `1` READ, `2` WRITE, `3` notify, `4` RDMA READ, `5` RDMA WRITE. See the [RDMA/CUDA contract](rdma-cuda.md) for bootstrap/commit sequencing. Describe requires zero
ID/range. Notify requires ID zero; offset is name length and length is message
length, both in UTF-8 bytes. Its body is name bytes, message bytes and a zero
commit marker. Even two empty strings require that marker before enqueue.
Permissions: `0` local-only (omitted from exported descriptors), `1` remote
read, `2` remote write, `3` remote read/write. Errors carry no payload.
Statuses: `0` success, `1` epoch mismatch, `2` unknown region, `3` permission
denied, `4` invalid range, `5` source closed, `6` exhausted, `7` protocol error,
`8` internal error, `9` WRITE/notification ready, `10` backend unavailable,
`11` remote native backend error. Unknown versions, flags, IDs, enum values,
overflowing/out-of-bounds ranges and oversized payloads are rejected.

READ: request → success header and exactly the requested bytes. WRITE:
request → ready → exactly the requested bytes → final success. The source
commits only a complete WRITE body; the requester commits only a complete READ
body. A dropped or invalid reply never authorizes replay. Describe replies are
bounded independently of the transfer payload limit, at 4 + 4,096 × 32 bytes.
Notify: request → reserve inbox and reply ready → complete body and marker →
validate/enqueue → final success with zero length. Rejection before ready
proves no enqueue; lost final receipt leaves delivery uncertain. Incomplete
bodies release their reservations when the owning handler exits.

## Verification

From the checkout, with permission to bind loopback sockets:

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo run --locked --example tcp_roundtrip
cargo run --locked --example external_host
cargo run --locked --example notifications
cargo run --locked --example async_batch
cargo publish --dry-run --locked
```

`tests/unit/` protects range/permission rules, epoch parsing, retained budget
ownership, admission, close/drain and malformed/fragmented wire behavior. Real
TCP controls inject short READs, incomplete WRITEs, lost WRITE ACKs and a
trickling response. They verify destination integrity, no automatic replay,
absolute deadlines and actual handler exit. `tests/tcp.rs` covers batch
prevalidation, prefix completion, concurrent byte correctness and descriptor
retirement. Its restart test launches the same test executable as a separate
source process, verifies actual source bytes and replaces it at the same port.
Source and destination now use externally owned backing and absolute-address
resolution. Additional controls prove actual owner release only after handler
exit, duplicate references, alias serialization and invalid address metadata.
P1.3 adds atomic batch-failure controls (range, bytes, count, ID/reference
exhaustion and alias locks), real multi-allocation batch READs and a blocked
requester READ whose owner must survive unregister/source close until return.
The ignored `peer_process_fixture` is invoked by that parent gate; it is not a
skipped runtime qualification case.
The same process gate now verifies attached READ/WRITE messages, explicit
Unicode messages, empty polling and stale-epoch notification rejection after
replacement. `tests/notifications.rs` checks shared local/remote queue bounds,
concurrent senders/pollers and data completion despite notification failure.
Wire controls cover reserved-body ownership during drain/close, malformed UTF-8,
empty/incomplete bodies and unknown final receipts without replay.

The [frozen CPU comparison](cpu-oracle.md) adds one shared valid-input trace
against unmodified native TENT. It verifies selected registration, byte movement,
completion and notification behavior; it does not establish wire interoperability.

CI runs the same CPU checks on stable Rust and the declared 1.85 MSRV. This is
source-level CPU evidence. Physical two-host, installed release, frozen C++
full oracle, mixed-peer, RDMA and NVIDIA hardware gates remain unqualified.

## Remaining OrbitKV integration boundary

The API now accepts existing host allocations, resolves source addresses and
delivers bounded notifications. OrbitKV still needs the concrete allocator/native
adapter and must connect its existing generation mailbox to this polling API
before migration. The async CPU lifecycle is implemented; its actual allocator,
ABI and installed profile still need qualification. Its allocator must
honor the external-access and retention contract. This substage does not change OrbitKV's
Mooncake dependency. RDMA registration/completion and CUDA memory need their
own backing, context and physical-drain proof; this CPU result cannot qualify
those paths. Full API/config/status/metadata compatibility remains tracked in
the completion plan and parity contract.
