# Libfabric

The opt-in Linux `fabric` feature moves registered host DRAM with libfabric RMA.
The software `tcp;ofi_rxm` profile is exercised with real providers and separate
processes in CI. EFA is **Implemented; installed profile unqualified**. CUDA,
file targets and TENT wire interoperability are outside this profile.

## Configuration

Build with `cargo build --locked --features fabric`. The build needs a C compiler,
libclang and libfabric development headers; it compiles only a small C bridge
for the SDK's inline function tables. It does not link libfabric. An engine
loads `libfabric.so.1` only when `EngineConfig::fabric` is `Some`.

```rust
use orbite::{EngineConfig, FabricConfig, FabricProvider, TransferEngine};

let engine = TransferEngine::new(EngineConfig {
    fabric: Some(FabricConfig {
        provider: FabricProvider::Tcp,
        devices: vec!["lo".into()], // Explicit loopback domain for this example.
        ..Default::default()
    }),
    ..Default::default()
})?;
# Ok::<(), orbite::Error>(())
```

Use `Transport::Fabric` or a selector rule with `"transports": ["fabric"]`.
`Unspec` considers it only when both engines advertise installed host support.
The configured domain whitelist supplies fabric NIC selection; RDMA policy
masks and remote staging do not apply. There is no hidden switch to another
provider or transport. The existing explicit policy walk may recover a
provably unposted rejection on a later candidate for `Unspec`. An explicit
`Transport::Fabric` keeps only that candidate, even with a retry budget.

The TENT loader accepts the following keys under `transports/fabric`. Typed
fields have the same names. Defaults match the frozen transport except that
installation is explicitly off (`fabric: None`, `enable: false`).

| Key | Default | Meaning |
| --- | --- | --- |
| `provider` | `auto` | `auto` tries EFA only; `efa` selects EFA; `tcp` selects `tcp;ofi_rxm` |
| `fabric_name` | empty | Exact fabric filter; EFA uses `efa` when omitted |
| `devices` | empty list | Domain whitelist, accepting an EFA device with or without its `-rdm` suffix |
| `slice_size` | 512 KiB | Maximum RMA slice, also cut at both local and remote MR boundaries |
| `idle_sleep_us` | 50 | Worker idle wait after hot polling |
| `post_timeout_ms` | 10000 | Bound for queued/EAGAIN posting |
| `quiesce_timeout_ms` | 10000 | Extra drain window after the submission deadline |
| `op_timeout_ms` | 10000 | Mark a peer overdue while a posted operation remains incomplete |
| `max_posted_ops` | 0 | Provider TX depth, bounded by the pending budget |
| `max_pte_entries` | 22 × 1024 × 1024 | EFA per-buffer/NIC page-entry layout budget |
| `max_register_threads` | 8 | Maximum joined registration threads per buffer |
| `max_mr_size` | 0 | Provider/PTE-limited MR chunks |

Additional typed bounds are `max_pending_ops` (4096 slices and MRs per domain),
`max_peer_addresses` (4096 address-vector entries per domain) and `max_domains`
(16). Engine byte, source-operation and batch admission budgets also apply.
Invalid bounds fail before installation. Discovery or construction failure is
an engine construction error; it does not silently install TCP instead.
OrbiTE never sets `FI_HMEM` or changes another process environment variable.
Provider environment configuration remains the application's responsibility.

## Progress and ownership

One joined worker per domain owns posting and CQ progress, including progress
on an idle target. Discovery requests RDM endpoints, RMA READ/WRITE and remote
access capabilities, context cookies and thread-safe domains. It prefers IPv4
for duplicate domains and omits loopback when another domain exists unless
explicitly whitelisted. Unsupported MR modes, including raw keys, are rejected.
The selected virtual-address or MR-offset mode is checked between peers.

Each admitted request registers its exact range. A native control grant binds
remote keys to the source epoch, region, range, operation and a unique nonce.
This differs from TENT's long-lived whole-buffer keys: source range ownership
lasts until the requester reports every slice complete and source MR destruction
succeeds. Source loans are released before the final ACK. Successful MR close
releases its loan even if a worker still holds a terminal task reference.

WRITEs require `FI_DELIVERY_COMPLETE`, including EFA. TENT requests that mode
only for the software profile; requiring target visibility on EFA is an OrbiTE
safety restriction. A provider without that guarantee is unavailable.

An expired post that submitted nothing can release its grant. Any failed
posted attempt, lost final ACK, malformed receipt or expired drain is
`Unquiesced`. It retains backing, MRs, cookies, provider library and admission;
a WRITE has unknown effect and is never replayed. CQ error entries do not
establish remote quiescence. This deliberately reports no successful request
prefix for a partially failed physical owner, even if later slices completed.
Public aliases share that result. Queued batch cancellation is supported;
running fabric work drains and does not claim cancellation support.

`close_segment(endpoint, epoch, timeout)` fences and drains the peer before
removing its address-vector entries. Other peers continue. Native close/remove
failure is sticky and retains the affected domain; no destroy is retried.
Engine drain timeouts retain their existing `Timeout` contract; they do not
reach native destruction. A successful engine close joins every domain worker.

`EngineStats::fabric` reports native domains, workers, MRs/bytes, peer addresses,
retained and posted slices, CQ errors, overdue rejection and completed bytes.
The optional metrics recorder includes fabric attempts, logical outcomes and
latency, plus retained-resource gauges. These counters are not release receipts.

## Native control layout

This extends OrbiTE's existing [Describe layout](cpu-tcp.md) under the
[pre-release wire policy](tent-parity.md#wire-and-consumer-boundaries), without
changing its header or offsets. Segment metadata byte 1 bit 0 advertises
libfabric; the other bits and bytes 2–3 remain reserved. Requests use the
ordinary region/offset/length header and operations `FabricRead` (23) and
`FabricWrite` (24). Those are operation codes, not protocol versions.

A successful ordinary reply gives the byte length of a bounded JSON grant
(maximum 1 MiB): `peer` contains TENT's provider/address-mode/NIC attributes,
`buffer` contains contiguous MR chunks with NIC indices and keys, and `address`
and `nonce` bind this grant. There are at most 16 NICs and 4096 chunks.
The 24-byte finish is nonce (8-byte big-endian), outcome (one byte: complete or
provably unposted), seven zero bytes, and completed length (8-byte big-endian;
zero for unposted). Source MR close precedes the final ordinary zero-length
ACK. These grants are for cooperative native OrbiTE peers, not the frozen TENT
fabric bootstrap. The byte fixture is
`transport::fabric::tests::native_capability_and_request_byte_fixture`.

## Verification

```sh
FI_PROVIDER=tcp ORBITE_REQUIRE_FABRIC=1 \
  cargo test --locked --features fabric transport::fabric::
```

Missing library/provider support fails this CI profile. The fallible table
interposes on the real software provider for partial posts, CQ errors, held
completion and failed cleanup. Two child processes check READ/WRITE and peer
loss. These tests do not qualify EFA, physical multi-rail or performance.
The packaged EFA command is in the [verification queue](qualification.md#verification-queue).
