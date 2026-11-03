# Transport selection

`Transport::Unspec` selects an eligible payload transport before admission.
`EngineConfig::selector` optionally applies ordered TENT-style policies to every
request, including explicit TCP/RDMA/NVLink hints. Sync and async submissions use
the same selection in `TransferEngine::prepare`; the selected transport stays
fixed through queueing, execution and drain.

## Configuration

```rust
use orbit_te::{EngineConfig, TransportSelectorConfig};

let selector = TransportSelectorConfig::from_json(r#"{
  "policy": [
    {"name": "small_host", "segment_type": "memory",
     "local_memory": "cpu", "remote_memory": "cpu",
     "max_size": 4096, "priority": "high", "transports": ["tcp"]},
    {"name": "bulk", "segment_type": "memory",
     "transports": ["nvlink", "rdma", "tcp"]}
  ]
}"#)?;
let config = EngineConfig { selector: Some(selector), ..Default::default() };
# Ok::<(), orbit_te::Error>(())
```

Pass `Transport::Unspec` to the request constructor to use candidate order.

```rust
use orbit_te::{TransferRequest, Transport};

let request = TransferRequest::new(
    Transport::Unspec,
    local.base_address(),
    remote.base_address(),
    length,
);
```

An explicit transport must appear in the matching policy's candidate list and
be eligible. A rejected or unavailable hint fails the request. With
`selector: None`, explicit routing keeps its existing behavior and `Unspec`
uses the default eligible order of NVLink, RDMA, then TCP.

The first matching rule wins. An unavailable candidate is skipped within that
rule for `Unspec`; an exhausted rule fails instead of trying a later rule.
With no policies, or an empty `transports` list, use the default eligible order.
No matching rule is an error. Every request in a submission passes preflight
before payload I/O, task publication or admission charges.

| Field | Consumed behavior |
| --- | --- |
| `name` | Unique configuration label, 1–256 bytes; omitted defaults to `memory_default`. Named request selection remains open |
| `segment_type` | `memory`; `file` is rejected until file execution exists |
| `local_memory`, `remote_memory` | `cpu`, `cuda`, or `*`; omission/`*` matches either. Pinned host allocations remain CPU |
| `min_size`, `max_size` | Inclusive byte bounds; minimum must not exceed maximum |
| `priority` | Exact match on effective QoS priority, otherwise request priority. Accepts high/medium/low or integers 0/1/2 |
| `transports` | Ordered, unique `tcp`, `rdma`, `nvlink` candidates; omission/empty uses defaults |

The typed equivalents are `TransportSelectorConfig` and `SelectionPolicy`.
JSON input is bounded to 1 MiB and 256 policies; values compile at engine
creation. The loader reads only the root `policy` section. Policy nodes must be
objects; null/invalid values, unknown fields and unsupported providers fail.
There is no request-time parsing, environment lookup or policy-name allocation.

## Owner-based eligibility

`open_segment` receives source-owned memory type and IPC export consent, plus
configured segment capabilities. These are immutable descriptor snapshots,
not grants or health guarantees. Refresh explicitly after registration changes.
Every execution still checks epoch, permission, retained owner and provider
readiness. Closing or failing a provider does not authorize replay elsewhere.

| Transport | Required eligibility |
| --- | --- |
| TCP | Both ends' normal TCP handlers; CUDA endpoints use their existing local host staging. A CPU-only requester can access a CUDA source over ordinary TCP |
| RDMA | Both engines configure RDMA. Each side's existing host/CUDA Direct or Staged policy remains authoritative; this capability does not promise GPUDirect or current RNIC health |
| NVLink | Both engines opt in; equal host boot identity and different process nonces; remote `allocate_ipc` export consent; local owned CUDA or owned CUDA-pinned host backing |

Priority selects matching rules but does not make a transport ineligible.
When the matching rule permits it, a Low prefetch can use TCP when RDMA is
unavailable, or NVLink when both endpoints qualify for IPC. TCP and NVLink
treat priority as advisory. RDMA workers use it to dispatch ready payloads.

`remote_staging: true` requires a remote CUDA range and
source Pin capability, and restricts selection to TCP/RDMA. It does not silently
turn an ordinary transfer into Pin/Delegate/Unpin. Policy memory filters always
inspect the original registered ranges, including when staging is selected.

CUDA IPC capability is captured from allocation construction. Describe and
selection do not take payload locks, wait for producers, create exports or
query CUDA. Same-host placement never follows from an IP address or equal
CUDA ordinal. Process identity reuses the existing IPC handshake owner.

## Correspondence with TENT

The reference is frozen TENT commit
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`, specifically
`runtime/transport_selector.h` and `runtime/transport_selector.cpp`.

| TENT owner/behavior | OrbitTE implementation |
| --- | --- |
| `SelectionPolicy`, first-match rules | `runtime/transport_selector/config.rs`, compiled typed rule array |
| `SelectionContext`, capability checks | `runtime/transport_selector.rs`, original owner memory types and both segment snapshots |
| `select` with an explicit transport hint | One concrete `PreparedTransfer::transport` before admission; excluded/unavailable hints fail |
| Buffer metadata and platform classification | `RemoteRegion::memory_type()`, registration-owned capability bits and `segment/metadata.rs` |

TENT defaults to the buffer's registered transport order. OrbitTE currently
uses eligible NVLink → RDMA → TCP; configured lists override that order. TENT's
named policy override, intent matching, same-machine rule filters, device-name
masks and QP pools are still open. Unsupported fields are rejected instead of
accepted without execution. File/provider policies, retry/failover and automatic
staging selection also remain open.

The selector scans at most 256 compiled rules and three candidates, once per
public request. The selection loop allocates nothing, takes no lock and calls
no driver; no selector work is added per slice, WR or CQ completion. Existing
registration resolution, admission and transport owners keep their own costs.
This is an implementation property, not a measured throughput comparison.

## Protocol and qualification

Describe replies carry a four-byte region count, 36 bytes of segment
capabilities, then 32 bytes per region. Capability byte zero holds the RDMA,
remote-staging and NVLink bits (0–2). Bytes 1–3 are reserved. Bytes 4–19 and
20–35 hold the NVLink boot and process identities, respectively, and are zero
when NVLink is disabled. Each region uses bytes 25 and 26 for memory type
(CPU or CUDA) and CUDA IPC export consent. The decoder checks reserved fields,
capability bits, region types and the exact payload length.

This changes the Describe payload under the existing pre-release wire policy.
Deploy matching builds on both peers. A matching header version does not
establish descriptor compatibility. The control headers and native drain
sequence are unchanged. See the [wire layout](cpu-tcp.md#development-wire-contract)
and [wire policy](tent-parity.md#wire-and-consumer-boundaries).

Source controls cover first-match/hint behavior, actual TCP sync/async submission,
whole-group preflight, source-owned CUDA metadata, and selected RDMA/NVLink
handoff through existing CPU native-ABI fixtures. These fixtures do not establish
physical DMA, installed routing, bandwidth or latency.
See [qualification](qualification.md#transport-policy-selection).
