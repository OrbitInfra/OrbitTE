# Asynchronous batches

Batches are the engine's asynchronous submission API (TENT F08/F10). They work
for every transport. This page also covers the opt-in admission controls that
sit in front of all data submissions: the runtime queue, QoS contracts and
receiver credits.

## Ownership and API

`Arc<TransferEngine>::allocate_batch(capacity)` reserves descriptor slots and an
engine-scoped, increasing `BatchId`. Capacity is positive and at most
`max_batch_requests`. `max_allocated_batches` and `max_allocated_requests` bound
all live reservations, including finished batches not yet freed and dropped
handles whose workers are still running.

`Batch::submit(operation, peer, requests, notification, timeout)` appends a group
and returns its stable task range. Addresses, permissions, bytes and source
authority are validated before enqueue; a rejected group adds no tasks, and
accepted groups are never replayed or rolled back. Groups in one batch run FIFO,
one worker per batch; different batches progress concurrently.
On a file target, SQEs within the group complete independently. Each task
reports its own bytes, including a short prefix. See [file submission](storage.md).

`seal()` stops appends. `status()`, `task_status()`, `task_statuses()` and
`submissions()` expose published state. A status poll can also decide a pending transport retry as described below. Before seal the overall state is
`Open`; after seal it becomes `Completed`, `Failed` or `Cancelled` once every
group exits, and terminal states never change. `wait(timeout)` requires seal;
its timeout neither cancels nor releases work. Each submission's deadline starts
at submit and covers validation, queueing, transfer and the attached
notification.

By default, an explicit `free()` on a busy batch returns `BatchBusy` and keeps the
handle valid. Setting `EngineConfig::enable_progress_worker` also opts in to
deferred free. A busy free then fences the ID and returns immediately. Dropping
a handle in either mode detaches accepted work. Actual registrations, staging
chunks, file loans and admission stay with their owners until physical drain.
`EngineStats::deferred_batches` counts retired IDs waiting for that proof.
`quarantined_batches` counts retired IDs without a safe reclaim path. Both
consume allocation capacity. `submit_async` allocates, submits one group and
seals it. See the [async example](../examples/async_batch.rs).

## Physical owners and public tasks

Every public request keeps its task ID and length. `task_status(i).physical_owner`
is the absolute public ID of the first task carried by that physical transfer.
All aliases see the same running, cancellation and terminal state. Each reports
only bytes within its own range. Cancelling any alias addresses the owner.
Cancelling one memory owner leaves independent successors eligible. A queued
cancellation removes the owner before payload I/O. A running RDMA
owner cancels unposted slices and drains every accepted post before retiring
its backing.

Memory requests merge by default. Both addresses must be adjacent inside the
same local and remote registrations, with matching hint, resolved route, QoS,
priority, staging mode, intent, named policy and deadline. Both peers’ transfer
size limits apply. Shuffled slices can share an owner. The payload starts at the
lowest address and the owner executes at its first public position. Overlapping
destinations and self-transfer read/write dependencies fence reordering. Set
`EngineConfig::merge_requests` to false for one physical transfer per request.
File segments have no remote registered-buffer boundary in TENT and retain
independent physical requests.

A submission report counts the successful **public prefix**. Its failure span
includes later aliases whose owner ran before that failure. Read individual
task results to distinguish confirmed bytes, failures and skipped neighbours.
An attached notification covers the original public interval and fires only
after all of its owners succeed.

## Transport attempts

A physical owner keeps its original epoch, registration, deadline and permits
across attempts. Its public aliases report the same `transport` and
`failover_count`. `EngineConfig::max_failover_attempts` defaults to three
additional attempts. Zero pins the first route. A clean submit rejection can
advance directly to the next eligible candidate in the original policy. A
failed proxy session first tries the same route without remote staging.

Completion-stage retry requires a backend release receipt. A READ can retry
after its last possible write to the destination has drained. A WRITE that
might have posted reports `RemoteEffect::Unknown`. An expired deadline,
accepted cancel or uncertain teardown ends retry. A short file completion
keeps its own byte prefix. Within a native file batch, only eligible failed
owners can retry after the group drains. If unregister closes a cuFile
registration during preparation, an eligible io_uring route keeps its file lease.

`enable_auto_failover_on_poll` defaults to true. A task, vector or overall
status poll can advance one drained failure. With this setting disabled, a poll
that observes the failure makes it terminal. Explicit `progress_batch`,
`wait` and synchronous transfer can advance an unobserved failure. The batch
worker executes the authorized attempt with its original budget. Cancellation
through any alias fences that owner before the next handoff.

## Explicit progress

`engine.progress_batch(id)?` dispatches at most one FIFO group or notification
retry, or publishes one completion. It returns the batch status. The caller
can also probe a retired native submission for a late release receipt. The
existing per-batch executor owns the blocking socket, native calls and copies.
Independent batch IDs use separate locks. A freed or foreign ID returns
`BatchClosed`. Ordinary observation does no backend I/O. A pending failover is decided under the batch lock.

The optional progress worker coalesces wakes by batch ID and shares a single
runtime-queue wake. The wake set fits `max_allocated_batches`. Enabling the
runtime queue starts this worker too, while keeping busy-on-free unless
`enable_progress_worker` was requested. The default fallback interval is
50 ms. It applies only while the runtime queue owns work. Set
`progress_fallback_interval` to zero for event-only progress. Without either
option, the batch executor continues to make progress autonomously.

For caller-driven polling, `wait_before_next_poll(count)` yields for polls 0
through 127. Later delays start at 1 µs and double up to 200 µs.
`next_poll_delay(count)` returns the duration without sleeping. Increment the
count with saturating arithmetic.

### Completion notifications

`submit_with_notification_targets` attaches one message to a memory submission
and accepts a list of destination segments. The batch worker tries self first
and sends to each distinct destination after payload success. A later explicit
progress turn can retry a definitely unposted destination until the original
deadline. Delivered destinations are removed. An invalid request or uncertain
receipt ends attempts for that destination. Inspect `submissions().notification`
for the outcome independently of the data status. Pending hooks share the
engine's outbound target and message-byte limits, sized by
`max_pending_notifications` and `max_pending_notification_bytes`. A retry holds
requester admission through its final attempt, deadline or free. Free closes
retry admission. [TCP and RPC notifications](cpu-tcp.md#notification-contracts) describes
fallback and acknowledgements.

## Runtime admission and dispatch window

`EngineConfig::runtime_queue` defaults to `None`. Enable it to limit dispatch
independently of the retained-owner budgets:

```rust
use orbit_te::{EngineConfig, RuntimeQueueConfig, TransferEngine};

let engine = TransferEngine::new(EngineConfig {
    max_inflight_batches: 128,
    max_inflight_bytes: 256 << 20,
    runtime_queue: Some(RuntimeQueueConfig {
        max_dispatch_owners: 8,
        max_dispatch_bytes: 32 << 20,
        deadline_aware: true,
        ..Default::default()
    }),
    ..EngineConfig::default()
})?;
# Ok::<(), orbit_te::Error>(())
```

The queue admits all physical owners of a submit together. Defaults allow
1,024 outstanding owners and 1 GiB, with 64 owners and 64 MiB in the dispatch
window. Public task capacity is checked separately. A single owner larger
than the dispatch window returns `ResourceExhausted` before any task is
published. Notification bytes count once on the first owner. Requester-wide
`max_inflight_batches` still bounds submissions.

`staging_owner_reserve` and `staging_byte_reserve` default to zero. Ordinary
admission can consume only the total minus each reserve. Internal staging can
use available capacity up to both totals. OrbitTE reserves once for the
validated ProxyManager session that runs PRE/CROSS/POST. Its child chunks
share that charge. A public staging intent alone cannot spend the reserve.

FIFO orders eligible owners by admission. `deadline_aware` selects EDF with
FIFO ties. A submission’s eligible prefix can fill the window. A blocked owner
keeps its successors from taking the resources it needs, and a later append
waits for the earlier submit to drain. Independent batches can progress.
`with_deadline(instant)` may shorten an owner’s absolute submission deadline.
`promotion_slack_ns` promotes future deadlines strictly inside that slack
and preserves EDF within the promoted set. The default, zero, disables promotion.

With EDF and a positive `mlu_local_threshold`, dispatch can cancel an owner
whose predicted transfer time divided by its remaining time reaches the
threshold. The estimate includes eligible payload bytes already dispatched
on that transport plus the owner’s payload. A known RDMA rate and an expired
deadline yield an infeasible prediction. TCP, NVLink, files and proxy staging
currently have no independent bandwidth estimate. An unknown rate leaves the
owner eligible for ordinary dispatch. Ordinary execution timeouts still apply.

RDMA rail cooldown gates new slices within the compiled device mask. A masked
owner with no eligible pair receives a typed unavailable/policy error. A
paused or half-open rail retains in-flight native work until its real receipt.
A timeout or an accepted cancel cannot make such an owner reusable. See
[rail health](rdma-cuda.md#rail-health).

A drop removes queue accounting before I/O, publishes `Error::DeadlineInfeasible`
to its aliases and increments `RuntimeQueueStats::deadline_infeasible_drops`
once. Other eligible owners can run, including later tasks in the same submit.
A later WRITE in a failed report’s span can therefore have an unknown effect,
including when an earlier owner was cancelled.
The engine exposes this decision through status and stats. It installs no
local-decoding or compression callback. See
[RDMA deadline prediction](rdma-cuda.md#request-priority-and-bandwidth-arbitration)
and the [verification controls](qualification.md#runtime-admission-and-dispatch-window).

## QoS contracts

`EngineConfig::qos` defaults to `None`. `Some(QosConfig)` enables version-1
contracts with TENT's overlay order: global defaults, tenant defaults, then the
tenant's intent override. Each field overrides independently; `None` inherits,
an effective `None` means no cap, and `Some(0)` is a hard zero. Blank identities
resolve to `default`/`unspec`. Names are trimmed and lowercased, at most 256
bytes and unique; at most 4096 contracts compile.

```rust
use orbit_te::{EngineConfig, Priority, QosConfig, QosPolicyFields,
    QosRequestContext, QosTenantContract, TransferEngine};

let requester = TransferEngine::new(EngineConfig {
    qos: Some(QosConfig {
        defaults: QosPolicyFields {
            max_inflight_bytes: Some(64 << 20),
            max_inflight_requests: Some(128),
            ..Default::default()
        },
        tenants: vec![QosTenantContract {
            name: "tenant-a".into(),
            intents: [("foreground_get".into(), QosPolicyFields {
                priority: Some(Priority::High),
                max_inflight_bytes: Some(128 << 20),
                ..Default::default()
            })].into(),
            ..Default::default()
        }],
        ..Default::default()
    }),
    ..EngineConfig::default()
})?;
let policy = requester.resolve_qos(&QosRequestContext {
    tenant_id: "tenant-a".into(),
    intent: "foreground_get".into(),
    requested_priority: Priority::Low,
})?;
assert_eq!(policy.effective_priority(), Priority::High);
// Use request.with_qos(policy.clone()) for repeated submissions.
# Ok::<(), orbit_te::Error>(())
```

`EffectiveQosPolicy` is an immutable, engine-bound handle; attach it with
`request.with_qos(policy)`. Its priority replaces the request's own. A request
without a handle uses the precompiled `default`/`unspec` contract, so it cannot
bypass a configured default budget. A handle from another engine returns
`ForeignQosPolicy`. The effective priority feeds
[transport selection](transport-selection.md) and RDMA dispatch; TCP and NVLink
treat it as advisory. Caps apply to every data transport.

Each **resolved contract** has one budget: an unknown tenant shares the global
default, an unknown intent shares its tenant's default, and an explicit intent
has its own bucket. Unknown names cannot create new counters, and an intent
budget is not also charged to its tenant default. `max_inflight_bytes` counts
physical payload bytes and `max_inflight_requests` counts physical owners. In the default
reject mode, a group over its cap returns `ResourceExhausted` before any charge.
Accepted work keeps its charge until its prepared owner is actually released;
`Unquiesced` keeps it. Identities are application-supplied, not authentication.

`policy.fields()`, `matched_contract()` and `effective_priority()` explain
resolution, and `policy.usage()` reports current owners, bytes and requests.
Submission uses numeric slots with no per-slice lookup. TENT's
`QosContractResolver` only resolves policy (`diagnostic_scope=resolution_only`);
OrbitTE also enforces priority and caps. Transport fallback, local recompute,
compression and hierarchical or cluster quotas are open. See
[qualification](qualification.md#qos-contract-consumption).

### QoS admission actions

`allowed_degraded_actions` overlays like other fields (an explicit list, even
empty, replaces the inherited one). Typed Rust uses `QosDegradedActions` with
`delay` and `reject` flags; JSON uses an array of names.

| Effective actions | On contract quota pressure |
| --- | --- |
| Absent, `[]` or `["reject"]` | Return `ResourceExhausted` |
| `["delay"]` or `["delay", "reject"]` | Wait until capacity frees or the original deadline passes |
| Any, with a physical owner larger than its cap | Reject during preparation |

Delay requires `EngineConfig::runtime_queue`; otherwise engine creation returns
`InvalidConfig`:

```rust
use orbit_te::{EngineConfig, QosConfig, RuntimeQueueConfig, TransferEngine};

let engine = TransferEngine::new(EngineConfig {
    runtime_queue: Some(RuntimeQueueConfig::default()),
    qos: QosConfig::from_json(r#"{"qos":{"defaults":{
        "max_inflight_bytes":"64MiB", "max_inflight_requests":128,
        "allowed_degraded_actions":["delay","reject"]
    }}}"#)?,
    ..Default::default()
})?;
# Ok::<(), orbit_te::Error>(())
```

When delay is enabled anywhere, every capped contract acquires capacity at its
batch head, together with receiver credit and the dispatch window, with no
partial charge while waiting; independent batches can pass a blocked head. If a
reject-only slot fills after enqueue, that owner fails at dispatch with no
remote effect and its later tasks are skipped. Waiting never extends the
deadline. TENT parses these action names but does not execute them; this
behavior is OrbitTE's extension. See
[qualification](qualification.md#qos-admission-actions).

### JSON QoS configuration

`QosConfig::from_json(&str)` and `QosConfig::from_json_file(path)` read the `qos`
section of a TENT-format engine document and return `Result<Option<QosConfig>>`
for `EngineConfig::qos`. Other sections are ignored.

```json
{
  "qos": {
    "version": 1,
    "defaults": {
      "priority": "medium",
      "max_inflight_bytes": "64MiB",
      "max_inflight_requests": 128
    },
    "tenants": [
      {
        "name": "tenant-a",
        "defaults": {"max_inflight_bytes": "128MiB"},
        "intents": {
          "foreground_get": {"priority": "high"},
          "background_prefetch": {"priority": "low", "max_inflight_requests": 16}
        }
      }
    ]
  }
}
```

```rust
use orbit_te::{EngineConfig, QosConfig, TransferEngine};

let engine = TransferEngine::new(EngineConfig {
    qos: QosConfig::from_json_file("transfer-engine.json")?,
    ..EngineConfig::default()
})?;
# Ok::<(), orbit_te::Error>(())
```

| Field | Accepted values |
| --- | --- |
| `priority` | 0/1/2, or `high`/`medium`/`low` (case-insensitive) |
| `max_inflight_bytes` | Integer, or integer string with B, KiB, MiB, GiB or TiB (KB/MB/GB/TB also multiply by 1024, as in TENT) |
| `max_inflight_requests` | Nonnegative integer |
| `allowed_degraded_actions` | Array of `delay` and `reject` |

Only version 1 is accepted. Missing or null `qos`, `{}` and a version-only object
return `None`, matching TENT's disabled resolver. Duplicate or unknown fields,
duplicate normalized names and unsupported actions (`fallback_transport`,
`local_recompute`, `compress`) are errors reported as `Error::InvalidQosConfig`.
Input is limited to 1 MiB and parsed once before engine creation. See
[qualification](qualification.md#json-qos-configuration).

## Receiver credit gating

`EngineConfig::max_receiver_credit_sessions` defaults to zero (disabled). A
positive value (at most 4096) requires every nonempty outgoing data submission
to hold a credit binding for its peer endpoint and source epoch; a missing
binding fails during preparation. READ and WRITE both consume grants. Credits
authorize requests; they do not prove remote buffer capacity or limit rate.

In manual mode, call `activate_receiver_credit(peer, epoch, qos_class)` and
deliver `ReceiverCreditUpdateV1` through `apply_receiver_credit` (a local typed
API, not a TENT wire message). Totals are **cumulative**, not increments:

| Charge | Meaning |
| --- | --- |
| `DataBytes` | Sum of payload lengths, excluding notification bytes |
| `RequestSlots` | One per physical owner |
| `StagingSlots`, `ConsumerSlots` | Automatic remote Pin owners only (below) |

Decreasing totals or repeated resources reject the update; old or duplicate
sequences are ignored. A credit-starved head waits without reserving the dispatch
window, so other peers proceed. Credit is reserved with dispatch eligibility,
refunded if the owner is cancelled before its transport handoff, and fully
committed at that handoff, even if the transfer later fails. Completion does not
return credit; the issuer grants new cumulative totals.

`freshness_ttl_ms` gates new reservations only. `deactivate_receiver_credit`
fences new and uncommitted work and returns `BatchBusy` until every owner drains.
`receiver_credit_stats(peer)` reports owners and consumption. TENT's
`SenderCreditLedger` provides this accounting model but is not wired into its
admission path; OrbitTE consumes it per physical owner. See
[qualification](qualification.md#receiver-credit-gating).

## Automatic receiver windows

Configure `EngineConfig::receiver_credit = Some(ReceiverCreditConfig)` on the
receiving peer and `max_receiver_credit_sessions` on the requester, then call
`connect_receiver_credit(peer, qos_class, timeout)` once. All transports then
share that credit owner with no manual update loop:

```rust
use orbit_te::{EngineConfig, ReceiverCreditConfig, TransferEngine};
use std::time::Duration;

let source = TransferEngine::new(EngineConfig {
    receiver_credit: Some(ReceiverCreditConfig {
        max_sessions: 128,
        max_bytes: 256 << 20,
        max_requests: 1024,
        bytes_per_sender: 64 << 20,
        requests_per_sender: 256,
    }),
    ..EngineConfig::default()
})?;
let requester = TransferEngine::new(EngineConfig {
    max_receiver_credit_sessions: 32,
    ..EngineConfig::default()
})?;
let peer = requester.open_segment(source.endpoint(), source.source_epoch(), Duration::from_secs(3))?;
requester.connect_receiver_credit(&peer, 0, Duration::from_secs(3))?;
// Register memory and submit through the ordinary APIs.
// After every accepted data owner has completed and dropped:
requester.disconnect_receiver_credit(&peer, Duration::from_secs(3))?;
# Ok::<(), orbit_te::Error>(())
```

The receiver reserves a fixed byte/request window per sender engine;
`max_bytes` and `max_requests` bound the sum, and each physical owner must fit its
window. Defaults are 128 sessions, 64 MiB / 1024 aggregate and 1 MiB / 16
requests per sender. A receiver epoch can have only one endpoint binding per
requester, and a lost connect reply is recovered by retrying the same peer.

Grants are `window + completed_total`. The sender reports only successfully
completed owners, published when the physical permit drops. Failed, cancelled
or uncertain owners earn nothing. When the next owner lacks credit, its
existing worker performs one cumulative sync, shared by concurrent waiters, with
no background thread or per-slice RPC. `disconnect_receiver_credit` returns
`BatchBusy` until every committed owner has completed, then returns the window
and leaves a tombstone. Sender crashes are not reclaimed automatically.
`receiver_credit_issuer_stats()` reports sender and window counts.

TENT has no receiver issuer or network consumer; the `CreditSync` and
`CreditClose` frames and this issuer are OrbitTE extensions. See
[qualification](qualification.md#automatic-receiver-credit-windows).

## Staging and consumer owner credits

With `--features cuda`, a source can also set `max_staging_slots`,
`max_consumer_slots`, `staging_slots_per_sender` and `consumer_slots_per_sender`
(all default to zero, disabled). Connecting a sender then reserves these windows
too.

| Resource | Source owner | Returned when |
| --- | --- | --- |
| `StagingSlots` | One negotiated remote Pin ring slot | Its lease returns after CUDA and private transport cleanup |
| `ConsumerSlots` | One active remote Pin session | Every child slot has returned and the session's owners have finished |

The source acquires the negotiated depth plus one consumer atomically before
construction, and rejects a Pin with a missing, stale or closing binding before
allocation. Uncertain cleanup keeps both charges. These credits authorize remote
Pin owners; the shared staging pool can still return `ResourceExhausted`
independently. TENT defines these resource tags but does not connect them to
admission. See [qualification](qualification.md#staging-and-consumer-owner-credits).

## Results, cancellation and quiescence

Group reports count the fully successful prefix in request order and sum only
its bytes. `request_index` names the first unsuccessful request.
`affected_requests` covers the consecutive span that may have executed,
including later file SQEs submitted concurrently. Each file task retains its
own result. A short first READ can report three bytes and the next task can
complete, even when the group reports zero completed requests and zero bytes.
A failed remote staging request can show completed READ chunks while reporting
zero completed bytes. Each group's `NotificationStatus` is separate.

`cancel(task)` and `cancel_all()` stop work that has not started. Running TCP, NVLink and remote staging return `Error::Unavailable` for a cancel.
RDMA wakes its posting workers. Slices already posted can still move bytes and
complete normally. File operations keep their backend cancellation and drain
rules. `cancel_all()` still attempts each queued owner if a running one cannot
cancel.
File requests wait for missing CQEs until 30 seconds after the submission
deadline. Expiry returns `Error::Unquiesced` and keeps unresolved owners and
admission charges retained.

`locally_drained` means the requester's access to its backing has ended. It does
**not** prove remote quiescence: a lost WRITE acknowledgement leaves
`RemoteEffect::Unknown`, forbids replay and still requires source-side drain
before remote memory is reused.

`close_requester(timeout)` permanently rejects new outgoing work and waits for
accepted work; call it again after a timeout. `close_source` gates source
registration and handlers separately. Whole-engine `close(timeout)` fences both
directions, waits against one deadline and then closes shared RDMA resources; see
the [device owner contract](rdma-cuda.md#failure-drain-and-resource-ownership).

## Mixed GPU direct and staged submissions

One submission can alternate GPU Direct RDMA requests and GPU-over-TCP staging
requests, executed in input order. Each RDMA operation releases its range loan
before the next CUDA copy, and each TCP copy drains its event before the next RDMA
request. RDMA readiness is checked once per submission as a preflight; actual
dispatch rechecks context, connection and quota. A failed copy keeps the earlier
completed prefix and skips later tasks; uncertain completion keeps the GPU
backing, pinned chunk and admission with `locally_drained = false`.

Per-request [remote staging](staging.md#serial-remote-staging) can join the same
batch. TENT's `commitPreparedSubmit` gives each physical owner a transport and
staging plan; OrbitTE keeps that split with ordered batch execution. See
[qualification](qualification.md#mixed-gpu-batches).

## TENT correspondence and limits

TENT's
[`TransferEngineImpl`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp)
allocates batch capacity, appends tasks and defers freeing pending batches.
OrbitTE uses one lock per batch and keeps the caller's status reads observational.
TENT can drive hooks and queue progress while reading status. Here the existing
batch executor publishes transport receipts and explicit progress can dispatch a
pending hook. Seal fixes the terminal task set. When free or an abandoned stage
cannot confirm completion, its physical owners stay charged. The free-list sweep
continues past a stuck ID. Per-batch reclaim failures on a recoverable cuFile
operation are capped at 4,096 consecutive errors. A pending reply resets the
count. Uncertain destruction is parked immediately. The [shared CPU trace](cpu-oracle.md)
compares async allocate, append, status and free. Transport failover remains open.
