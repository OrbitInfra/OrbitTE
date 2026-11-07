# TENT TCP interoperability

Use this explicitly selected profile when a cooperative peer runs frozen
Mooncake v0.3.14. It implements the frozen P2P descriptor lookup and ordinary
TCP payload RPCs. OrbiTE peers always use native framing and `open_segment`.
There is no protocol autodetection, automatic downgrade or external metastore.

## Enable the profile

A source enables a **separate listener** with `EngineConfig::tent_bind_addr`.
`tent_endpoint()` returns its bound address; `endpoint()` continues to return
the native listener. Both share the existing bounded control queue, acceptor
and handler workers. Disabling the option creates no compatibility listener.

```rust
use orbite::{EngineConfig, TransferEngine};

let mut config = EngineConfig::default();
config.tent_bind_addr = Some("127.0.0.1:0".parse()?);
let engine = TransferEngine::new(config)?;
let tent_endpoint = engine.tent_endpoint();
// Register backing through the usual retained-memory API, then publish
// tent_endpoint to the cooperative TENT consumer.
```

A Rust requester opts in per segment. The consumer supplies the endpoint and
an authoritative epoch, then uses the ordinary sync or async batch API:

```rust
let peer = engine.open_tent_segment(endpoint, expected_epoch, timeout)?;
let request = orbite::TransferRequest::new(
    orbite::Transport::Tcp, local_address, remote_address, length,
);
let report = engine.submit_and_wait(
    orbite::TransferOp::Read, &peer, &[request], timeout,
)?;
```

The TENT-format config loader and C/C++/Python constructors continue to use
native framing. A TENT-format configuration does not opt into this profile.
An OrbiTE descriptor on the compatibility endpoint is rejected by
`open_tent_segment`: use its native endpoint instead.

## Identity and retirement

**Frozen TENT cannot validate an epoch on the wire.** The supplied epoch is
OrbiTE's local admission and retirement fence, not a remote attestation. The
consumer must retire an old identity with
`close_segment(endpoint, old_epoch, timeout)` when membership changes, drain
it, then explicitly open the new authoritative epoch. Old handles remain
fenced while any reference exists. A retirement timeout keeps owners and
charges; it is not a release receipt.

Before a WRITE, the requester fetches the complete descriptor again on the
connection that will carry the WRITE. Any changed metadata fences the old
local identity and returns `EpochMismatch` before sending payload. Descriptor
refresh never silently adopts changed addresses under an admitted epoch.
Frozen TENT publishes no restart generation; an unchanged descriptor cannot
prove freshness. A process can restart and reuse the same address and metadata,
so a **stale-address WRITE risk is inherent to this cooperative profile**.
Deploy it only where the consumer's membership and drain protocol covers that
risk. The native OrbiTE protocol retains its stronger epoch checks.

## Supported operations and bounds

| Operation | Behavior |
| --- | --- |
| Describe | Bounded memory-segment JSON, TCP-capable public ranges, exact endpoint match; redirects, invalid extents and excluded memory platforms fail closed |
| READ | Raw address and length; commit local bytes only after a complete, validated response. A truncated reply leaves the destination unchanged |
| WRITE | Snapshot local bytes, check metadata, then send once. A lost or rejected acknowledgement has unknown remote effect and never authorizes replay |
| Notify / Probe | Bounded notification delivery and liveness reply; malformed or rejected notifications have no success acknowledgement |
| RDMA bootstrap | Frozen envelope codec and explicit unavailable reply; no native MR key is exposed or imported through this profile |
| Pin / Unpin / Delegate / update subscriptions / UB bootstrap | Explicit RPC rejection; no fabricated staging lease, subscription or provider success |

The source checks actual registration bounds and permissions on every access.
Frozen TENT omits permissions from its segment JSON, so the requester does not
infer read-only access from that document. Backing and admission live through
the copy; uncertain CUDA cleanup retains both. GPU and RDMA interoperability
are unqualified. Receiver credits and remote staging require native RPCs and
are rejected for TENT targets. Address-only notification fanout uses native
framing; the per-segment notification API follows the explicitly opened profile.

Request and response lengths are bounded before allocation. Metadata is limited
to 4 MiB and the engine's existing region limit. Payload and handler budgets,
queue admission, QoS, aliases and per-peer drain use the existing owners. The
compatibility socket is owned by its call; no persistent requester pool or
extra coordinator is introduced. Cancellation retains the queued-only contract;
running calls finish or drain to their deadline. Protocol magic, version,
serialization and response sequence are validated explicitly in each direction.
The frozen YLT client does not check response magic/version; OrbiTE deliberately
does, with malformed-response negative controls.

## Evidence

[The installed native comparison](qualification.md#tent-tcp-interoperability)
uses an isolated, unmodified frozen TENT build and a Rust public-API consumer.
It checks both peer directions and payload guards on loopback. Unit socket
controls add held drain, membership retirement, metadata changes, truncated
responses and unknown-effect WRITE handling. This is CPU correctness evidence,
not a hardware or performance claim. The exact wire fixtures and current native
wire rule are in [wire boundaries](tent-parity.md#wire-and-consumer-boundaries).
