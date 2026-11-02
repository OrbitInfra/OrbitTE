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

## Receiver credit gating

`EngineConfig::max_receiver_credit_sessions` defaults to zero (disabled). Set a
positive bound, at most 4096, to require a cooperative credit binding for every
nonempty outgoing data submission. A missing binding fails during preparation,
before public task publication or transport I/O. Each binding identifies a peer
endpoint and authoritative `RemoteSegment::source_epoch()`. READ and WRITE both
consume request grants against that peer; on READ the payload receiver is local.
These credits do not establish remote buffer/RQ capacity or a pacing rate.

An application calls `activate_receiver_credit(peer, epoch, qos_class)`, then
delivers `ReceiverCreditUpdateV1` through `apply_receiver_credit`. The update is
a typed local API, not a TENT-compatible wire message. Use schema 1, flags 0, the
peer's source epoch as `receiver_session_id`, matching QoS class, positive credit
epoch and positive sequence. `DataBytes` and `RequestSlots` are **cumulative**
grant totals, not increments. Both must cover the whole next submission:

| Charge | Meaning |
| --- | --- |
| `DataBytes` | Sum of payload request lengths, excluding notification bytes |
| `RequestSlots` | Number of public requests in the submission |
| `StagingSlots`, `ConsumerSlots` | Reserved TENT schema tags; updates are rejected until a runtime charge mapping exists |

Partial updates preserve omitted totals; a decreasing total or repeated resource
rejects the whole update. Old/duplicate sequences have no effect. Sequence gaps
are applied and reported as `SequenceGap`. Each engine is one sender ledger;
applications must not copy one finite receiver grant independently to multiple
senders. This API does not authenticate the issuer. One class is bound per peer;
per-request QoS classes and the full QoS resolver remain open.

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
whole-submission granularity. Automatic receiver issuance, network delivery and
resynchronization, staging/consumer credits and full QoS/pacing remain open.
[Source controls](qualification.md#receiver-credit-gating) do not qualify native
fairness, receiver protection or performance.

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
[remote selection](staging.md#serial-remote-staging). Installed v21 DMA,
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
