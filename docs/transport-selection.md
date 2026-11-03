# Transport selection

`Transport::Unspec` selects an eligible payload transport before admission.
`EngineConfig::selector` optionally applies ordered TENT-style policies to every
request, including explicit memory and file transport hints. Sync and async submissions use
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
use orbit_te::{IntentType, TransferRequest, Transport};

let request = TransferRequest::new(
    Transport::Unspec,
    local.base_address(),
    remote.base_address(),
    length,
).with_intent(IntentType::ForegroundGet);
// Bind to one known policy even when another rule would match first.
let named = request.with_policy_name("small_host")?;
```

An explicit transport must appear in the matching policy's candidate list and
be eligible. A rejected or unavailable hint fails the request. With
`selector: None`, explicit routing keeps its existing behavior and `Unspec`
uses NVLink, RDMA, then TCP for memory targets. A file target tries eligible GDS, then io_uring.

An unnamed request uses the first matching rule. Within that rule, `Unspec`
skips ineligible candidates. An exhausted rule fails before admission. An
empty candidate list uses the segment default. A **named request** matches the
exact name and segment type, bypassing intent, size, priority, placement and
memory filters. Its transport still has to be authorized and eligible. Unknown
names, wrong segment types and unavailable hints fail before payload I/O or task
publication. `Error::TransportPolicy` carries the bound rule and reason.
When no custom rules exist the default names are `memory_default` and `file_storage`.

| Policy field | Consumed behavior |
| --- | --- |
| `name` | Unique label of 1–256 UTF-8 bytes. The builder retains a bounded shared name. Preparation resolves the compiled rule index once |
| `segment_type` | `memory` or local `file`. A file target is on the same machine with CPU remote memory and requires a Linux file provider |
| `local_memory`, `remote_memory` | `cpu`, `cuda`, or `*`. Pinned host allocations are CPU |
| `same_machine` | Boolean. A memory peer is local only when both descriptors identify the same host boot. An absent identity gives no locality |
| `min_size`, `max_size` | Inclusive byte bounds |
| `priority` | Exact effective QoS or request priority. Accepts high/medium/low or 0/1/2 |
| `intent_type` | Case-insensitive `unspec` (also `intent_unspec`), `foreground_get`, `background_prefetch`, `migration`, `checkpoint`, `weight_loading`, `staging_internal`, or the corresponding integer 0–6. Omission matches any intent |
| `transports` | Ordered unique `tcp`, `rdma`, `nvlink` for memory or `gds`, `io_uring` for files |
| `devices` | RDMA NIC names compiled to a 64-bit mask using stable discovered IDs and the custom topology's verified mapping |
| `qp_pool` | Selects a contiguous endpoint QP range. An omitted, empty or unknown name permits every index |
| `service_level`, `traffic_class` | Policy values are diagnostic. Only the endpoint pool changes native SL/TC |

`transports.force_tcp` restricts memory requests to TCP. File selection keeps
its own rules. `use_legacy_transport_selection` enables the default owner order
for unnamed requests. Explicit name binding stays strict, and memory force-TCP
takes precedence. JSON input is bounded to 1 MiB and 256 policies. Unknown
policy fields and invalid intent reject the configuration. Frozen TENT skips a
rule with an invalid intent.

## RDMA device and QP selection

For example, these settings allow checkpoint writes on one NIC and the second
segment of the endpoint's QP array.

```json
{
  "transports": {"rdma": {"endpoint": {"qp_pools": [
    {"name": "interactive", "num_qp": 2},
    {"name": "bulk", "num_qp": 1, "service_level": 3, "traffic_class": 40}
  ]}}},
  "policy": [{"name": "save", "segment_type": "memory", "intent_type": "checkpoint",
              "devices": ["mlx5_1"], "qp_pool": "bulk", "transports": ["rdma"]}]
}
```

An unresolved or out-of-width NIC increments `EngineStats::unresolved_policy_devices`
when a topology is available. Partial resolution warns and keeps the resolved
subset. If none resolve, an error log identifies the names and the filter allows
all NICs. Without a topology the mask also allows all NICs. Both single grants
and multirail slice plans intersect this mask with the engine's device mask,
location restrictions, healthy contexts and compatible peer paths. A mask never
overrides any of those checks.

Named pools partition the data QPs in configuration order. Counts are positive
and their sum fits the endpoint limit of 64. With empty pools the flat width is
`qp_mul_factor`, defaulting to `RdmaConfig::num_lanes`. With pools the sum sets
the endpoint width independently of the worker/CQ lane count. Each selected
index inherits its pool's SL (0–15) and TC (0–255), or the RDMA defaults.
The requester folds its least-loaded worker's index into the selected range.
The QP uses the CQ at that index modulo the lane count.
Unbound traffic can use a named pool's QP and inherit its settings too. Both
ends use the selected index for fresh connections and cache reuse.

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
| Ordered and named policies, request intent | `runtime/transport_selector/config.rs` and `TransferRequest`. An invalid intent rejects the config. A named request keeps its binding in legacy mode |
| `SelectionContext`, owner capability checks | Original memory types and both segment descriptors, including boot identity and IPC consent |
| `select`, hint and host staging | One immutable selection before admission, carried into TCP, RDMA, NVLink, file I/O and the TCP/RDMA CROSS leg |
| `resolveDeviceMasks`, RDMA sub-batch grouping | One resolution per engine topology. Each prepared request retains its own mask and QP range through every child grant |
| Endpoint `qp_pools`, `selectQpInPool`, `setupOneQP` | Contiguous index segments and pool SL/TC on the physical RC QP. An individual grant retains its connection, range and native owner |
| `canMergeRequests` policy boundary | File and NVLink grouping preserve requested name and intent. Adjacent requests with different bindings stay separate. Physical aliases and general merging belong to S8 |

TENT's automatic memory order follows the buffer's registered transport list.
OrbitTE registrations advertise capabilities without an insertion order, so
automatic memory selection keeps NVLink → RDMA → TCP. A policy can reverse
that order. TENT documents GDS → io_uring → RDMA for files. Its frozen
`getDefaultPolicies` lists GDS and io_uring. OrbitTE supports these two local
owners. RDMA-to-file needs a remote file segment. SHM and HP-TCP have no
executor yet. The selector rejects fallback indices, local recompute and
compression. No failure triggers replay.

The selector scans at most 256 compiled rules and three candidates, once per
public request. The compiled rule scan allocates no strings, takes no selector lock and calls
no driver; no selector work is added per slice, WR or CQ completion. Existing
registration resolution, admission and transport owners keep their own costs.
This is an implementation property, not a measured throughput comparison.

## Protocol and qualification

Describe replies carry a four-byte region count, 36 bytes of segment
capabilities, then 32 bytes per region. Bits 0–2 of capability byte zero
identify RDMA, remote staging and NVLink. Bit 3 denotes a boot-only identity
when NVLink is disabled. Bytes 4–19 hold the boot identity when known and
20–35 hold the process nonce only for NVLink. Bytes 1–3 are reserved. Region
bytes 25 and 26 carry memory type and IPC export consent.

The RDMA path reply carries the advertised endpoint width. The rendezvous
hello carries the agreed reusable width and an optional pool index on both
cached and transient QPs. Deploy the same build and endpoint layout on both
peers. The header cannot detect a mismatch in pool definitions.
See the [wire policy](tent-parity.md#wire-and-consumer-boundaries).

CPU controls check TCP sync/async payloads, the RDMA verbs table's per-NIC
bytes and connection SL/TC, NVLink group boundaries, staging CROSS and local
files. Physical NIC selection and link-layer QoS await the installed gate.
See [qualification](qualification.md#transport-policy-selection).
