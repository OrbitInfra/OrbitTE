# CPU TCP foundation

This page covers the CPU memory, registration, TCP and notification contracts
(P1.1–P1.4, P2.1–P2.2 in the [completion plan](completion-plan.md)). The TCP
payload path uses Rust sockets; the crates.io alpha does not contain it.

## Architecture correspondence

The reference is Mooncake TENT
[`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`](https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent).

| TENT responsibility | Rust owner | Delivered subset |
| --- | --- | --- |
| `runtime/transfer_engine_impl` | `src/engine.rs`, `src/batch.rs` | Immutable config, sync/async batches, ordered execution and requester/source drain |
| `runtime/segment`, `segment_tracker` | `src/memory.rs`, `src/segment.rs` | Shared allocation owners, reference-counted ranges, descriptors, permissions and epoch fencing |
| `runtime/control_plane`, RPC dispatch | `src/runtime/control_plane.rs` (`ControlService`) | P2P descriptor open/refresh and source-owned handlers |
| `transport/tcp` | `src/transport/tcp.rs` (`TcpTransport`) | Host byte movement, deadlines, complete READ response and final WRITE ACK |
| Notification queues and submit hooks | `src/notification.rs`, `src/engine.rs` | Bounded named messages, atomic polling and batch-completion delivery |
| Config and resource observations | `src/config.rs`, `TransferEngine::stats` | Per-engine budgets and resource counters |

`ControlService` owns the shared socket service, descriptor and notification
protocol, source admission and dispatch. TENT keeps its TCP payload handlers in
`ControlService`; OrbitTE moves the byte behavior into `TcpTransport`. RDMA uses
a `ControlChannel` for setup without going through `TcpTransport`. Cache keys,
replica grants and recovery policy stay with OrbitKV.

Each ordinary TCP request opens a connection. The control acceptor waits for
socket readiness and hands each accepted stream to the bounded handler queue.
A full queue closes the new connection. Shutdown signals the same acceptor and
joins it before the handlers. Both ends enable TCP_NODELAY. The previous
2 ms sleep after an empty accept delayed short loopback READs by an idle
polling interval. The S10 benchmark measures this path with a 64-byte payload
and checks both guards.

## Rust contracts

- **Owned memory.** `register_memory(Box<[u8]>, Permission)` consumes the backing,
  which handles and admitted handlers keep alive through `Arc`. The source checks
  remote permissions regardless of requester metadata.
- **External memory.** Unsafe `HostMemory::from_raw_parts(address, length, owner)`
  adopts a pre-existing host allocation; `owner` must keep it alive, stable and
  writable. It does not pin pages or make a device pointer host-accessible.
- **Subranges and batches.** `register_host_memory(memory, offset, length,
  permission)` registers a checked subrange; overlaps must share one
  `HostMemory` lock. `register_host_memory_batch` preflights the whole batch and
  publishes it atomically, so a failure leaves existing state intact.
- **Duplicates.** As in TENT's `SegmentTracker`, an exact duplicate shares one ID
  and charge, the first permission wins, and each call adds one reference.
  Duplicates inside one Rust batch are coalesced (TENT can publish them
  separately).
- **IDs and unregister.** Registration IDs are never reused. The final
  `unregister_memory` reference removes visibility; running handlers keep their
  backing and charges until they finish.
- **Address lookup.** `resolve_local_address` and `RemoteSegment::resolve_address`
  need one range covering the whole request, using TENT's address-ascending,
  length-descending order, and never dereference an unregistered address.
- **Authority.** `open_segment(endpoint, expected_epoch, timeout)` takes the
  128-bit epoch from the authoritative source owner, and every operation
  rechecks it at the source, so a new process at the same address cannot inherit
  authority.
- **Requests.** `TransferRequest::new(transport, local_address, remote_address,
  length)` takes absolute addresses that carry no authority. Submission resolves
  and retains every local registration before I/O, so a concurrent unregister
  cannot free admitted backing.
- **Execution.** `submit_and_wait` validates every request first, runs them in
  order and stops at the first failure; the report has the completed prefix,
  exact bytes and failed index. A batch is not a transaction.
- **READ and WRITE.** READ receives the full payload before touching its local
  slice. WRITE copies its input, waits for source readiness, sends the payload
  and waits for an ACK after the source commits. A WRITE failure after sending
  returns `RemoteEffect::Unknown` and is never retried.
- **Deadlines.** One absolute requester deadline covers connects, frames and
  payloads; a separate source deadline starts at accept. Neither is a hard
  real-time bound.
- **Source close.** `close_source(timeout)` permanently closes source admission and
  waits for handlers; a timeout keeps admission closed and can be retried.

Async batches use the same validation and executor; see
[async batches](async-batches.md).

## Notification contracts

`Notification { name, message }` carries UTF-8 strings (empty strings and
repeats are valid). `send_local_notification` delivers in process;
`send_notification(peer, notification, timeout)` uses epoch-fenced TCP for other
peers. A receipt confirms enqueue, not consumption. `receive_notifications()`
atomically drains the queue in enqueue order, so concurrent pollers get disjoint
messages. Matching, generations and waits belong to the consumer.

`submit_and_notify` sends once to the batch peer after every request succeeds;
empty or failed batches report `NotificationStatus::Skipped`. Data completion
and `report.notification` are separate, and a failed notification never
authorizes replaying data. The receiver reserves count and bytes before
accepting a body, and malformed or partial bodies never enter the inbox. After
body submission without a final receipt the effect is `Unknown`; manual retries
can duplicate a message.

The sender tries each installed notification-capable channel in order. Only
an unavailable channel with no delivery effect permits fallback. OrbitTE's
installed channel is TCP. The independent control RPC can receive without
that channel, and uses the same bounded inbox as self delivery. Set
`tcp_notifications` to disable the TCP channel or `notification_rpc_fallback`
to stop after its first attempt and disable RPC fallback. Both default to true.
When the RPC also fails, the sender preserves the last channel error and any
uncertain RPC effect. Receive drains all installed queues even if one fails.
Successful deliveries take precedence over an empty-channel error.
[Async completion hooks](async-batches.md#completion-notifications) retain
only destinations still eligible for retry.

## Budgets and profile

| Default | Value | Accounting |
| --- | --- | --- |
| Bind | `127.0.0.1:0` | Loopback with an OS-chosen port; other hosts need a routable bind |
| Retained registrations | 1,024 / 1 GiB | Retained ranges, including unregistered handles; protocol limit 4,096 regions |
| Batch descriptors | 1,024 | Rejected before effects if exceeded |
| Allocated async batches / slots | 32 / 32,768 | Open, finished-but-unfreed and detached batches |
| In-flight requester submissions | 32 | Async groups plus synchronous, open and notification calls |
| In-flight requester bytes | 64 MiB | Request bytes plus notification bytes |
| One payload | 8 MiB | Configurable up to 64 MiB; each source enforces its own |
| Source workers / queue | 4 / 4 sockets | Queue overflow closes the connection |
| Source timeout | 5 s | Accept-to-handler deadline, configurable in `(0, 300s]` |
| Pending notifications | 1,024 / 1 MiB | Queued messages plus reserved incoming bodies |
| One notification | 64 KiB | Name plus message bytes, enforced at both ends |

The protocol is plain TCP for trusted environments: epochs fence restarts but
do not authenticate peers. There is one connection per request and sequential
batch execution. `stats()` samples registration, admission and notification
counters under separate locks. These budgets cover registered ranges, not an
external allocator's own footprint.

## Development wire contract

OrbitTE uses its own pre-release control protocol; see the
[wire policy](tent-parity.md#wire-and-consumer-boundaries). Integers are
unsigned big-endian and each connection carries one operation. Both headers
begin with the eight-byte `ORBITTE\0` magic, a two-byte version, a one-byte
operation or status and one zero reserved byte.

| Frame | Remaining fields |
| --- | --- |
| Request, 52 bytes | Expected epoch (16), registration ID (8), offset (8), length (8) |
| Reply, 36 bytes | Actual source epoch (16), payload length (8) |
| Descriptor payload | Region count (4), segment capabilities (44, including an eight-byte big-endian maximum transfer length), then entries sorted by unique ID: ID (8), base address (8), length (8), permission bits (1), memory type (1), IPC export consent (1), zero reserved bytes (5) |

The [selector guide](transport-selection.md#protocol-and-qualification) describes
the capability fields; each transfer still needs a grant.

Operations: `0` describe, `1` TCP READ, `2` TCP WRITE, `3` notify, `4` RDMA READ,
`5` RDMA WRITE, `6` sliced RDMA READ and `7` sliced RDMA WRITE (see
[RDMA/CUDA](rdma-cuda.md)). Control RPC notify uses operation `22`. Describe requires a zero ID and range. Both notification operations use ID
zero, with offset as the name length and length as the message length; the body contains the UTF-8 name, message and a zero commit marker. The receiver sends final success only after its registered callback accepts the complete message. Permissions: `0` local-only (never
exported), `1` remote read, `2` remote write, `3` read/write. Statuses: `0`
success, `1` epoch mismatch, `2` unknown region, `3` permission denied, `4`
invalid range, `5` source closed, `6` exhausted, `7` protocol error, `8` internal
error, `9` ready, `10` backend unavailable, `11` remote native backend error and
`12` explicit RDMA cache miss. Unknown versions, flags, IDs, enum values,
out-of-bounds ranges and oversized payloads are rejected.

READ: request → success header and the requested bytes. WRITE: request → ready →
bytes → final success. Notify: request → ready → body and marker → final success.
Each side commits only complete bodies, a dropped reply never authorizes replay,
and describe replies are bounded at 48 + 4,096 × 32 bytes.

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

Unit tests cover ranges and permissions, epoch parsing, budgets, admission,
close/drain and malformed or fragmented frames. Real TCP controls inject short
READs, incomplete WRITEs, lost ACKs and trickling responses. `tests/tcp.rs`
covers prevalidation, prefix completion, concurrency and a restarted source
process at the same port (the ignored `peer_process_fixture` is that gate's
child, not a skipped case). `tests/notifications.rs` covers queue bounds,
concurrent senders and pollers, and data completion despite notification
failure. The [CPU oracle](cpu-oracle.md) compares one valid-input trace against
native TENT. CI runs all of this on stable and Rust 1.85; physical two-host,
mixed-peer and hardware gates are separate.
