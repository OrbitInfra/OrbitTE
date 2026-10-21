# CPU TCP foundation

This is the implemented P1.1/P2.1 substage in the
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
| `runtime/transfer_engine_impl` | `src/engine.rs` | Immutable config, batch validation/admission, ordered execution reports and source drain |
| `runtime/segment`, `segment_manager`, registration lifecycle | `src/memory.rs`, `src/segment.rs` | Allocation owners, registration IDs, bounds/permissions, epoch-scoped descriptor snapshots |
| `runtime/control_plane`, RPC dispatch | `src/tcp.rs` | Direct P2P descriptor open/explicit refresh and source-owned handlers |
| `transport/tcp` | `src/tcp.rs` | Host byte movement, deadlines, complete READ response and final WRITE acknowledgement |
| Config/resource observations | `src/config.rs`, `TransferEngine::stats` | Per-engine budgets and retained/admitted resource counters |

The TCP-only slice keeps RPC and TCP byte ownership together. Separate control
and backend owners when the next real transport needs them; no empty provider
traits or fallback to a Mooncake engine are present. Cache keys, replica/source
grants, GPU allocation and recovery policy remain with OrbitKV.

## Rust contracts

- `register_memory(Box<[u8]>, Permission)` consumes actual backing. Handles and
  admitted handlers retain it through `Arc`. Local byte access and remote copies
  serialize through the registration's mutex. Remote permissions are checked
  by the source regardless of requester metadata.
- Registration IDs increase for one engine lifetime and are never reused.
  `unregister_memory` removes lookup visibility. Existing handlers may finish;
  their backing and budget charges remain until all owners release them. Local
  handles continue to own usable memory after unregister or engine destruction.
- `open_segment(endpoint, expected_epoch, timeout)` takes the random 128-bit
  epoch from the authoritative source owner. It cannot learn replacement
  authority from a new process at the same address. Every operation checks
  that epoch again at the source. Refresh by calling open with the same epoch.
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

## Budgets and profile

| Default | Value | Accounting |
| --- | --- | --- |
| Bind | `127.0.0.1:0` | OS selects a loopback port; an explicit routable bind is required for other hosts |
| Retained registrations | 1,024 / 1 GiB | Includes unregistered backing retained by handles or handlers; hard protocol limit 4,096 regions |
| Batch descriptors | 1,024 | Whole batch rejected before I/O if exceeded |
| In-flight requester batches | 32 | Includes descriptor-open calls |
| In-flight requester bytes | 64 MiB | Sum of all admitted slice lengths until their batch exits |
| One payload | 8 MiB | Configurable up to 64 MiB; each source independently enforces its limit |
| Source workers / queue | 4 / 4 sockets | Fixed worker pool and bounded socket queue; queue overflow closes the connection |
| Source timeout | 5 s | Monotonic accept-to-handler deadline, configurable in `(0, 300s]` |

Only Linux host-memory loopback is tested in this delivery. Entropy comes from
`/dev/urandom`; failure to read it fails construction. This protocol uses plain
TCP in a trusted environment. Epochs fence source restarts; they are not peer
authentication. There is one connection per request, sequential batch execution,
bounded payload staging and no topology selection or performance claim.

`stats()` samples retained regions/bytes, active source operations and requester
batch/byte admission. Counters are sampled under separate locks, not one global
atomic snapshot. Full TENT metrics and NIC-load observations remain open.

## Development wire contract

This is **OrbitTE protocol version 1**, not TENT's YLT RPC framing or HP-TCP.
It makes this first Rust CPU slice executable while TENT wire compatibility
remains an explicit P0/P9 gate. Future wire changes require a version change
and corresponding decoder rejection tests.

Integers are unsigned, big endian. Each connection carries one operation.
Both headers start with eight bytes `ORBITTE\0`, a two-byte version (`1`), a
one-byte operation/status and one zero reserved byte.

| Frame | Remaining fields |
| --- | --- |
| Request, 52 bytes | Expected epoch (16), registration ID (8), offset (8), length (8) |
| Reply, 36 bytes | Actual source epoch (16), payload length (8) |
| Descriptor payload | Region count (4), then sorted unique entries of ID (8), length (8), permission bits (1) and zero reserved bytes (7) |

Operations: `0` describe, `1` READ, `2` WRITE. Describe requires zero ID/range.
Permissions: `0` local-only (omitted from exported descriptors), `1` remote
read, `2` remote write, `3` remote read/write. Errors carry no payload.
Statuses: `0` success, `1` epoch mismatch, `2` unknown region, `3` permission
denied, `4` invalid range, `5` source closed, `6` exhausted, `7` protocol error,
`8` internal error, `9` WRITE ready. Unknown versions, flags, IDs, enum values,
overflowing/out-of-bounds ranges and oversized payloads are rejected.

READ: request → success header and exactly the requested bytes. WRITE:
request → ready → exactly the requested bytes → final success. The source
commits only a complete WRITE body; the requester commits only a complete READ
body. A dropped or invalid reply never authorizes replay. Describe replies are
bounded independently of the transfer payload limit, at 4 + 4,096 × 24 bytes.

## Verification

From the checkout, with permission to bind loopback sockets:

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo run --locked --example tcp_roundtrip
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
The ignored `peer_process_fixture` is invoked by that parent gate; it is not a
skipped runtime qualification case.

CI runs the same CPU checks on stable Rust and the declared 1.85 MSRV. This is
source-level CPU evidence. Physical two-host, installed release, frozen C++
oracle, mixed-peer, RDMA and NVIDIA hardware gates remain unqualified.

## Remaining OrbitKV integration boundary

The current API consumes CPU allocations. OrbitKV exports existing pinned
allocator regions and issues remote READs by source address. It therefore
still needs an explicit external-allocation owner/registration contract,
address-to-registration adapter, generation-scoped notification mailbox and
native bindings before migration. This substage does not change OrbitKV's
Mooncake dependency. RDMA registration/completion and CUDA memory need their
own backing, context and physical-drain proof; this CPU result cannot qualify
those paths. Full API/config/status/metadata compatibility remains tracked in
the completion plan and parity contract.
