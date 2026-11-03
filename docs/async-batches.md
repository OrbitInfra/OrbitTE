# Asynchronous batches

P1.4 implements a bounded CPU/TCP subset of TENT F08/F10. The
[completion plan](completion-plan.md) remains the only execution queue. This
source profile has CPU controls; RDMA/CUDA source controls and installed
consumer qualification are recorded separately in the [qualification ledger](qualification.md).

## Ownership and API

Use `Arc<TransferEngine>::allocate_batch(capacity)` to reserve descriptor slots
and an engine-scoped monotonically increasing `BatchId`. Capacity is positive
and at most `max_batch_requests`. `max_allocated_batches` and
`max_allocated_requests` bound all live reservations, including completed batches
that have not been freed and dropped handles whose workers remain active.

`Batch::submit(operation, peer, requests, notification, timeout)` appends a group
and returns its stable public task range. All addresses, permissions, bytes and
source authority are validated before enqueue. The group owns resolved local
registrations and a requester-admission charge. A rejected group adds no public
tasks; accepted earlier groups are never replayed or rolled back. Groups in one
batch execute FIFO; different batches can progress concurrently. Each batch has
at most one worker, bounded by its allocation reservation. Submission records,
including empty submissions, are bounded by capacity too.

`seal()` stops append. `status()`, `task_status()`, `task_statuses()` and
`submissions()` only read worker-published state; they perform no transfer,
retry, notification or progress step. Before seal the overall state is `Open`,
even if every task submitted so far has completed. After seal, `Pending` becomes
`Completed`, `Failed` or `Cancelled` only after all accepted groups exit.
Per-task terminal states never change, and the sealed overall terminal is stable.

`wait(timeout)` requires seal and uses a condition variable. Its own timeout
neither cancels nor releases work. A submission's independent absolute deadline
begins at submit and includes validation/queue time and attached notification.

`free()` returns `BatchBusy` while a group still owns local work, preserving the
handle for retry. Once idle it seals, joins its worker, releases descriptor
capacity and its engine reference, and invalidates further batch operations.
Dropping an active handle instead disconnects the queue without discarding
accepted groups: the detached worker retains the engine, registrations and both
allocation/admission budgets until it exits. This prevents repeated handle drops
from bypassing the thread or retained-owner limits.

`submit_async(...)` allocates, submits one group and seals it. See the runnable
[`async_batch` example](../examples/async_batch.rs) for multiple appends.

## Runtime admission and dispatch window

`EngineConfig::runtime_queue` defaults to `None`. Enable it to limit actual
dispatch independently from accepted-owner retention:

```rust
use orbit_te::{EngineConfig, RuntimeQueueConfig, TransferEngine};

let engine = TransferEngine::new(EngineConfig {
    max_inflight_batches: 128,
    max_inflight_bytes: 256 << 20,
    runtime_queue: Some(RuntimeQueueConfig {
        max_dispatch_owners: 8,
        max_dispatch_bytes: 32 << 20,
        deadline_aware: true,
    }),
    ..EngineConfig::default()
})?;
# Ok::<(), orbit_te::Error>(())
```

Each nonempty submission is one owner. `max_inflight_batches/bytes` retain their
existing meaning: all accepted outgoing calls, queued work and uncertain native
owners. The queue adds two positive limits on concurrently reserved submissions
and their whole byte charges, including attached notification bytes. It is not
a live WR count, bandwidth limiter or a separate pool of retained memory.
A group exceeding `max_dispatch_bytes` returns `ResourceExhausted` before task
publication or transfer. Default `RuntimeQueueConfig` uses 64 owners, 64 MiB and
FIFO; the existing global budgets can impose tighter limits.

FIFO uses original admission sequence. With `deadline_aware=true`, the eligible
heads use earliest absolute submission deadline, with FIFO ties. All current
submissions have a timeout/deadline; it includes preparation, queue residence,
transport and attached notification. EDF only reorders heads that have not
reserved a window; it does not preempt a reservation or posted DMA. RDMA
`Priority` still independently orders payloads inside transport workers.
Only the oldest retained submission in each Batch is eligible. Later submissions
keep their original sequence and deadline when activated, so a short deadline
cannot overtake its own dependency or cause self-deadlock. A head that does not
fit remaining byte capacity blocks later eligible heads until capacity is released.
When receiver credit gating is enabled, credit-starved or fenced heads are
ineligible and may be bypassed; their same-Batch successors remain blocked.

Both synchronous and asynchronous data submissions use this queue across TCP,
RDMA, NVLink and outer staged transfers. Existing caller/Batch workers wait on a
condition variable; no additional scheduler thread is created. Cancellation of
the first task and queue expiry return before transport I/O and attached
notification. Canceling a later task preserves the uncanceled prefix. A later
group in one Batch is still observed only when its worker reaches that group.
Standalone notifications, segment open, batch allocation and empty submissions
keep their existing admission paths and do not acquire dispatch reservations.
Internal staging CROSS calls do not reacquire an outer window.

`EngineStats::runtime_queue` is `None` when disabled; otherwise it reports
`queued_owners/bytes` and `dispatching_owners/bytes`. Queued counts include
dependency-blocked groups. Dispatching counts mean reserved capacity, including
owners whose worker has not started I/O yet. Reservations end after backing
release; Unquiesced retains them, including the same-Batch dependency, until a
physical reclaim proof exists. `close_requester` fences new submissions while
accepted work continues to drain through the same queue.

This follows frozen TENT's separation of `tryAdmit`, `pickForDispatch` and
`refillDispatchWindow`. TENT schedules physical requests, including merged-task
aliases; OrbitTE currently schedules whole ordered submissions. Physical-request
granularity, staging reserves, deadline-infeasible cancellation/MLU and
local-recompute hooks remain open. The [source controls](qualification.md#runtime-admission-and-dispatch-window)
do not establish native fairness or a performance advantage.

## QoS contracts

`EngineConfig::qos` defaults to `None`. `Some(QosConfig)` enables typed version-1
contracts with the frozen TENT overlay order: global defaults, tenant defaults,
then that tenant's intent override. Each field overrides independently. A field
set to `None` inherits the earlier layer; an effective `None` has no contract cap.
`Some(0)` is a hard zero budget. Empty/all-whitespace request identity resolves to
`default`/`unspec`. Names use ASCII whitespace trimming and ASCII lowercase;
configuration keys must be nonempty, at most 256 bytes and unique after
normalization. At most 4096 compiled contracts, including defaults, are allowed.

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
// Set TransferRequest::qos to Some(policy.clone()) for repeated submissions.
# Ok::<(), orbit_te::Error>(())
```

`EffectiveQosPolicy` is an immutable engine-bound handle. Explicit handles use
the `requested_priority` captured at resolution when no layer overrides it;
they replace the request's legacy `priority`. `TransferRequest::qos=None` still
uses the engine's precompiled `default`/`unspec` contract, falling back to that
request's `priority` only when no layer specifies one. It cannot bypass a
configured default budget. With QoS disabled, ordinary `qos=None` requests keep
their prior behavior; an explicitly resolved disabled handle retains its
context priority. A handle from another engine returns `ForeignQosPolicy` during
preparation. This new request field requires `qos: None` in existing literals.

Effective priority follows the existing transport contract: RDMA supports
High/Medium/Low, including ordinary and sliced payloads and staging CROSS work;
TCP/NVLink currently require High. Unsupported effective priorities fail during
preparation. Policies neither reorder requests within a Batch nor change
FIFO/EDF selection. Byte/request caps apply to all data transports through the
shared preparation owner.

Each **resolved contract** has its own aggregate retained-byte/request budget:
an unknown tenant shares `global_default`; an unknown intent of a known tenant
shares that tenant's default bucket; an explicit intent uses its compiled bucket.
Repeated resolution, different fallback priorities and arbitrary unknown names
cannot create new counters. Field inheritance does not imply simultaneous
parent/child charging: an intent budget is independent of its tenant-default
bucket. Existing engine-wide inflight budgets remain the aggregate outer bound.
These cooperative identities are application-supplied, not authentication.

Preparation groups mixed-contract requests by static slot and atomically checks
the entire submission before charging any slot or publishing public tasks.
`max_inflight_bytes` counts payload lengths; `max_inflight_requests` counts public
requests, not submissions or native slices. Attached notifications are covered
by existing global budgets. Over-cap groups return `ResourceExhausted` immediately
without queueing for QoS space. Existing accepted work, including credit waits,
dispatch waits and uncertain DMA, retains the charge. Clean cancellation, expiry
or failure releases it only when actual prepared ownership ends; Unquiesced
retains it. Internal staging legs are not charged again. Empty groups, standalone
notifications and registration/control calls have no QoS data charge.

`policy.fields()`, `matched_contract()`, `effective_priority()` and the normalized
identity accessors explain resolution. `policy.usage()` returns that contract's
current owners, payload bytes and public request count, or `None` for an uncapped
policy. A priority-only configuration creates no quota ledger. Resolution is
reusable; submission uses numeric slots, one atomic admission lock per bounded
group and no additional per-slice policy lookup or network message.

Frozen TENT's `QosContractResolver` resolves policy and reports
`diagnostic_scope=resolution_only`; it has no runtime consumer in the frozen
tree. OrbitTE now consumes priority and retained-resource limits. Typed Rust
configuration, rejecting normalized duplicate intent keys and treating all-blank
request identity as the default are deliberate differences. TENT JSON loading,
degraded-action execution, parent/child and cluster-wide quotas, native fairness
and full QoS remain open. Receiver-credit `qos_class` is still a separate peer
binding and is not implicitly mapped from these contracts. See the
[source qualification](qualification.md#qos-contract-consumption).

## Receiver credit gating

`EngineConfig::max_receiver_credit_sessions` defaults to zero (disabled). Set a
positive bound, at most 4096, to require a cooperative credit binding for every
nonempty outgoing data submission. A missing binding fails during preparation,
before public task publication or transport I/O. Each binding identifies a peer
endpoint and authoritative `RemoteSegment::source_epoch()`. READ and WRITE both
consume request grants against that peer; on READ the payload receiver is local.
These credits do not establish remote buffer/RQ capacity or a pacing rate.

In manual mode, an application calls `activate_receiver_credit(peer, epoch, qos_class)`, then
delivers `ReceiverCreditUpdateV1` through `apply_receiver_credit`. The update is
a typed local API, not a TENT-compatible wire message. Use schema 1, flags 0, the
peer's source epoch as `receiver_session_id`, matching QoS class, positive credit
epoch and positive sequence. `DataBytes` and `RequestSlots` are **cumulative**
grant totals, not increments. Both must cover the whole next submission:

| Charge | Meaning |
| --- | --- |
| `DataBytes` | Sum of payload request lengths, excluding notification bytes |
| `RequestSlots` | Number of public requests in the submission |
| `StagingSlots`, `ConsumerSlots` | Automatic source-owned remote Pin slots/sessions, described below; manual updates remain rejected |

Partial updates preserve omitted totals; a decreasing total or repeated resource
rejects the whole update. Old/duplicate sequences have no effect. Sequence gaps
are applied and reported as `SequenceGap`. Each engine is one sender ledger;
applications must not copy one finite receiver grant independently to multiple
senders. This API does not authenticate the issuer. One class is bound per peer;
mapping from per-request QoS contracts to credit classes remains open.

With the runtime queue, a head lacking fresh credit waits without reserving the
dispatch window, so another eligible peer can run. Without that queue, the
caller/Batch worker waits on the credit ledger. Updates wake waiting owners and
refill the queue. Original submission deadlines and cancellation still apply.
All accepted owners, including those waiting for their first grant, retain the
binding and existing inflight budgets. Empty groups, standalone notifications,
registration/control work and internal staging CROSS work do not consume these
credits. An outer staged group consumes exactly once.

Credit is reserved atomically with dispatch eligibility. Cancellation, expiry or
fencing before the first transport handoff refunds any reservation when the
actual owner is released. At the first handoff, the **entire group's** debit is
committed: success, a transport error, or a canceled later suffix does not
refund it. Completion is not an automatic request-slot return; the issuer must
grant additional cumulative totals. `receiver_credit_stats(peer)` exposes
active/reserved owners and cumulative consumption, including still-refundable
reservations. Unquiesced ownership retains its binding and reservation.

`freshness_ttl_ms=0` has no expiry. Positive TTL gates new reservations only;
already reserved work remains usable. A duplicate update cannot extend TTL;
a new-sequence empty-grants heartbeat can refresh freshness without changing
balances. If a credit-eligible large head blocks the byte window, existing
waiters also wake at its credit expiry to reconsider smaller peers. No polling
thread is added. The frozen TENT ledger carries this field but does not consume it.

Activation in the same epoch preserves totals, sequence and freshness. A newer
epoch requires every accepted owner to drain. `deactivate_receiver_credit`
requires an exact epoch and fences new work plus reserved-but-uncommitted
groups; already committed groups may finish. It returns `BatchBusy` while any
actual owner remains, including uncertain DMA. Retry after drain to remove the
binding. Same-epoch activation cannot undo a fence. After removal, the caller
must use a fresh administrative epoch and reject delayed old updates: retired
epoch history is not retained. Source restart also needs a fresh peer binding.
Updates remain accepted during requester close so previously accepted work can
drain.

Frozen TENT's `SenderCreditLedger` provides the cumulative accounting model but
is not wired into its transport/admission path. OrbitTE now consumes it at
whole-submission granularity. The optional receiver issuer and cumulative wire
synchronization and automatic staging/consumer owner charges are described below.
Manual staging credit mapping and full QoS/pacing remain open.
[Source controls](qualification.md#receiver-credit-gating) do not qualify native
fairness, receiver protection or performance.

## Automatic receiver windows

Configure `EngineConfig::receiver_credit=Some(ReceiverCreditConfig)` on the
authoritative peer and enable `max_receiver_credit_sessions` on the requester.
Call `connect_receiver_credit(peer, qos_class, timeout)` once before data
submission. Ordinary TCP, RDMA, NVLink and staged requests then share the same
credit owner; no manual update loop is required.

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

Each sender engine has one source-instance identity. The receiver reserves a
fixed byte/request window for that identity and returns its credit epoch.
`max_bytes` and `max_requests` bound the sum of these windows. The issuer's
`max_sessions` bounds active and retired sender records; the requester's separate
`max_receiver_credit_sessions` bounds its active, pending and retired receiver
bindings. Window capacity can fill before the session-count limit. Defaults are 128 sessions, 64 MiB/1024
aggregate, and 1 MiB/16 public requests per sender. Each whole submission must
fit its window, otherwise preparation returns `ResourceExhausted` before task
publication. Windows count payload bytes and public requests, as in manual mode.

Automatic bindings cannot be manually activated, updated or retired. Within a
requester engine, one receiver source epoch may have only one endpoint binding;
using another address/relay for that same receiver cannot create another balance.
An initial exchange records its identity before I/O. If its reply is lost, retry
the same peer and class to recover the existing epoch and window. Pending state
is retained, even when the caller has stopped requester admission. Existing
automatic connections can still be recovered after `close_requester`; opening a
new binding requires outgoing admission. A pending epoch-zero binding must be
recovered before disconnect; an unknown close never claims success.

The receiver issues cumulative grants as `window + completed_total`. The sender
reports only monotonic committed and successfully completed data-group totals.
`BatchReport::is_complete()` marks a successful data group; notification failure
does not turn successful data into failure. Completed totals are published only
when the actual `PreparedSubmission` drops its backing and queue owners before
its `CreditPermit`. A partial/cancelled suffix, a transport error, or uncertain
remote cleanup earns no new grant, even if the requester is locally drained.
Unquiesced owners retain their original charge. Unstarted reservations still
roll back locally. No failure path invents a fresh window.

When the next group lacks credit and there are newly completed owners, its
existing caller/Batch worker performs one cumulative sync. Concurrent requests
share one in-flight sync per peer. The queue and ledger mutexes are released
before network I/O; completion and sync exit wake both wait domains. If another
group completes during the exchange, waiters reconsider that newer total before
parking. There is no background credit thread, per-slice RPC or completion-time
network call. An unchanged snapshot with no new completion causes no repeated
polling. Sync/close retries resend cumulative metadata, never payload.

Automatic windows have no freshness expiry. Std TCP connection establishment
is capped at 250 ms or the original remaining deadline, whichever is shorter;
after connection, control I/O retains that original deadline and checks
cancellation at the existing 25 ms socket polling boundary. A failed exchange
fails its current waiting submission and leaves the cumulative snapshot available
for a subsequent attempt; it does not poison other waiters or reset consumption.

`disconnect_receiver_credit` permanently fences new work on the binding. It
returns `BatchBusy` while owners remain or any committed group lacks successful
completion. Retry after clean drain. Existing sync and close receipts remain
serviceable after source admission closes; new receiver sessions do not.
Successful disconnect returns the fixed window and records a terminal tombstone
before acknowledging. Lost close replies can be retried. Neither side reopens
that sender/receiver identity, and tombstones count toward the session limit
until engine exit. EOF, TTL, sender crash and local handle drop never reclaim a
window automatically. Recovery of failed/uncertain groups and crashed senders
requires a separate physical proof; it is not implemented by this API.

`receiver_credit_stats(peer)` adds automatic-mode and completed-total counters.
`receiver_credit_issuer_stats()` reports active/retired senders and reserved
window bytes/requests. These are cooperative scheduling grants, not measured
receiver buffer/RQ occupancy, authorization or a native rate guarantee. Fixed
windows do not redistribute idle capacity or map QoS intent into credit classes.

OrbitTE wire **v22** adds fixed-size `CreditSync`/`CreditClose` metadata frames;
the current wire is **v23**, and both peers must use it even when automatic
credits are disabled. Pin adds its session binding; other payload framing is unchanged. Frozen TENT supplies the sender ledger
and resource/epoch/sequence model, but has no receiver issuer or network consumer.
This bounded issuer is an OrbitTE extension. Automatic staging/consumer owner
charges are described below; manual resource mapping, adaptive windows,
failed-sender reclamation and native fairness/performance remain open. See [source qualification](qualification.md#automatic-receiver-credit-windows).

## Staging and consumer owner credits

With `--features cuda`, a source can enable four additional
`ReceiverCreditConfig` fields: `max_staging_slots`, `max_consumer_slots`,
`staging_slots_per_sender`, and `consumer_slots_per_sender`. All four default to
zero (disabled); enabling requires positive values and per-sender windows that
fit their respective totals. Connecting an automatic sender reserves these
windows along with the existing byte/request windows. Example: totals of 16
staging slots and 4 consumers, with 4 and 1 per sender, authorize at most four
concurrent sender windows. Configure the physical pool separately.

| Resource | Actual source owner | Return condition |
| --- | --- | --- |
| `StagingSlots` | One negotiated remote Pin ring slot, including native construction | Its `StageBufferLease` safely returns Idle/Vacant after CUDA and private transport cleanup |
| `ConsumerSlots` | One active remote Pin session | Every child slot has returned and the GPU range, CUDA copies and private MR/transport owners have finished |

The source atomically acquires the negotiated slot depth and one consumer before
native construction. This uses the source's actual chunk size and depth, not
the requester's proposed ring width. Unassigned slots roll back; each assigned
token follows the real lease, including a failed constructor with retained
backing. A per-Pin child count prevents an implicit Drop failure from returning
the consumer early. Uncertain cleanup retains both the affected stage charge
and the session charge. Returning a reusable Idle slot does not destroy its
pooled allocation or stream.

An automatic request carries its sender session and issuer epoch in the existing
Pin exchange. A source with owner credits enabled rejects missing, stale or
closing bindings before allocation. The identifiers are cooperative session
binding, not authentication. Successful Unpin returns resources before its ACK;
a lost ACK cannot undo proven local cleanup. EOF with clean drain also returns
physical charges, while the existing byte/request ledger still withholds fresh
payload grants for unsuccessful groups. Closing a source credit binding fences
new Pins and remains busy until its actual resource charges drain. A requester
fence permits an already committed group to finish later Pin requests.

`receiver_credit_issuer_stats()` exposes reserved windows, active resource charges
(including retained owners), and explicitly retained subsets. A leaked native
owner can remain active without incrementing the explicit-retention subset;
active counts, not the subset alone, govern retirement. Sender success reports
never release these physical charges. `ConsumerSlots` means the whole Pin session,
not an external application consumption ACK.

The gate adds no RPC, per-chunk credit work or progress thread. It acquires once
per Pin and returns on existing cleanup paths. It authorizes remote Pin owners;
the shared physical pool still serves ordinary/local staging and can independently
return `ResourceExhausted`. It neither reserves physical buffers nor delays Pin
until space becomes available. Manual resource updates, adaptive redistribution,
crashed-sender recovery and generic consumer credits remain open.

Frozen TENT defines the four credit resource tags and a private sender ledger,
but does not connect that ledger to network admission. Its `ProxyManager` owns
bitmap chunks and per-chunk locks. OrbitTE keeps that resource boundary and adds
this automatic owner mapping; its v23 framing and consumer semantics are OrbitTE
extensions. See [source controls](qualification.md#staging-and-consumer-owner-credits).

## Results, cancellation and quiescence

Tasks expose waiting/running/completed/failed/cancelled/skipped states, exact
bytes of fully completed requests, cancellation requests and a retained structured failure. A failed remote staging
request can have visible completed READ chunks while contributing zero completed
bytes; drain status is not rollback. Failure indices are public batch task indices. `request_index` identifies the first
failed task; `affected_requests` gives the consecutive failure span. A shared
NVLink close/ACK failure can cover several already attempted tasks. Every task
in that span shares the failure and drain status; only subsequent tasks are
skipped/cancelled without I/O. A clean copy/cancel failure followed by a source
ACK preserves the copied prefix. Other accepted
groups still run. Submission reports preserve each group's completed prefix and
independent `NotificationStatus`; successful data can coexist with notification
failure. Empty or failed groups do not send attached messages.

`cancel(task)` and `cancel_all()` request cancellation. Waiting work checks that
request before dispatch. A running CPU operation finishes or reaches its existing
deadline; cancellation does not interrupt an unproven physical operation or
relabel committed bytes as cancelled. A late cancellation can therefore coexist
with a completed task. This profile has no provider abort or cancel receipt.

`locally_drained` means the requester's physical access to its registered backing
has ended. Group/batch drain is published after dropping the actual prepared
owners and their admission charge. It does **not** establish remote source
quiescence. In particular, a lost WRITE acknowledgement retains
`RemoteEffect::Unknown`, forbids automatic replay and still requires source-side
ownership/drain before reusing remote backing. Source handlers retain their own
owners independently.

`close_requester(timeout)` permanently rejects new outgoing work, including
open, notification and async submissions. It waits for accepted queued/running
work to release local owners. Timeout leaves admission closed; call again to
wait. `close_source` separately gates source registration and handlers. Existing
source-only close behavior is preserved. Whole-engine `close(timeout)` fences
both directions before waiting against one deadline; accepted queued work can
still create backend slices while draining. It then explicitly closes shared
RDMA device resources. A native cleanup error is retained without retry; see
the [device owner contract](rdma-cuda.md#failure-drain-and-resource-ownership). Closing requester admission does not
invalidate existing status/free operations or claim that a remote ACK was read.

## Mixed GPU direct and staged submissions

One submission can alternate explicit `Transport::Rdma` GPU Direct requests and
`Transport::Tcp` GPU staging requests. Configure RDMA `cuda_memory=Direct` and
set `TransferRequest.remote_staging=false` for these ordinary transfers. Each
request keeps its own transport and checked
local/remote ranges; requests execute in input order. A completed RDMA operation
releases its range loan before the next CUDA copy. The TCP copy drains its event
before the following RDMA request begins. Callers finish producer work before
handoff and wait for the engine's completion before consumer reuse.

RDMA readiness is checked once at the first RDMA request in each submission,
without sampling the full statistics snapshot. Every request still validates
its addresses, permissions and byte budget. This is a preflight observation,
not a device-state snapshot or a DMA grant: actual dispatch checks the current
context, connection, quota and native owner again. A concurrent failure can
therefore be reported at dispatch after preparation succeeded.

A pending middle copy keeps the earlier completed prefix visible and later
tasks waiting; neither batch completion nor its notification can overtake it.
A drained copy error leaves the completed prefix intact and skips later tasks.
Uncertain native completion retains the actual GPU backing, pinned chunk and
submission admission, with `locally_drained=false`. Completed-request bytes do
not count a failed request's partial local changes. Local staging continues to
reuse its bounded pinned chunks and allocation-owned stream/event.

The [source controls](qualification.md#mixed-gpu-batches) cover READ and WRITE
with healthy completion, a drained copy error and uncertain event completion.
Per-request `remote_staging=true` also permits remote Pin/Delegate/Unpin over
TCP/RDMA in the same engine and batch; only the source needs its incoming Pin
capability enabled. The source validates the actual CUDA allocation. Private
host staging grants follow host RDMA policy independently of GPU Direct. See
[remote selection](staging.md#serial-remote-staging). Installed v23 DMA,
concurrent transport dispatch and end-to-end throughput remain separate work.

Frozen TENT's `commitPreparedSubmit` assigns each physical owner a transport and
staging plan, and `ProxyManager` owns its staging resources. OrbitTE preserves
that division. Its current ordered batch execution and explicit per-request
transport selection do not reproduce TENT's complete selector/subbatch scheduler.

## Frozen correspondence and limits

The frozen [`TransferEngineImpl`](https://github.com/OrbitInfra/Mooncake/blob/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent/src/runtime/transfer_engine_impl.cpp)
allocates batch capacity, appends public tasks, retains runtime references and
defers freeing batches that remain pending. Rust's explicit busy-on-free plus
detached-handle ownership is a distinct safe API; matching the native deferred
C free convention remains an adapter requirement. Rust adds seal to make an
overall terminal result stable against further append; native TENT can append
after prior tasks complete.

The [shared CPU trace](cpu-oracle.md) now uses real async Rust APIs and native C
allocate/append/status/free with the same 25 byte/notification/cleanup
observations. Native queue/merge/progress-worker modes remain disabled for that
profile. Rust blocked-socket tests exercise queue deadline, cancel, wait timeout,
concurrent unregister, requester close and detached engine/allocator release.
They do not qualify native fault behavior or device DMA.

[RDMA/CUDA](rdma-cuda.md) now consume these owners with explicit per-request
transport selection. Their `Unquiesced` failures retain native owners and
admission/capacity: the logical result is terminal while `locally_drained` is
false, `free` remains busy and close cannot report drain. No reclaim proof is
implemented for this quarantine yet.

Request merging/alias mappings, full transport selection policy, explicit
progress, complete deadline/QoS policy, provider abort/reclaim and language/wire
compatibility remain open. Runtime FIFO/EDF and RDMA payload priority are
implemented within their documented source scope. No hardware backend inherits this CPU profile's drain proof.
