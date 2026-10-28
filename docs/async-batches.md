# Asynchronous CPU batches

P1.4 implements a bounded CPU/TCP subset of TENT F08/F10. The
[completion plan](completion-plan.md) remains the only execution queue. This
source profile is pending independent acceptance; RDMA/CUDA and installed
consumer qualification are separate.

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

## Results, cancellation and quiescence

Tasks expose waiting/running/completed/failed/cancelled/skipped states, exact
completed bytes, cancellation requests and a retained structured failure. Failure
indices are public batch task indices. `request_index` identifies the first
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
progress, priority/EDF, provider abort/reclaim and language/wire compatibility
remain open. No hardware backend inherits this CPU profile's drain proof.
