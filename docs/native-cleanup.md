# Native RDMA cleanup failures

This gate tests the existing resource owners when native teardown fails while
completion debt remains. It uses the public parent/slice transfer protocol and
test-only verbs observation; it is separate from the
[installed healthy host consumer](installed-host.md).

## Owners and receipts

`RdmaQp` owns the physical connection and its SQ/RQ. A CQ lane owns completion
queues and the associated event channel; `RdmaContext` owns the device/PD.
Grant records retain memory authority, DMA loans and completion waiters.

| Failure | Native mechanism | Required retained boundary |
| --- | --- | --- |
| `ibv_destroy_qp` | Thread-local seccomp rejects write/ioctl on the selected test-owned uverbs command FD; the real verbs call returns EIO | QP, its CQs/PD, outstanding grants, MR/backing and SQ/RQ charges |
| `ibv_destroy_comp_channel` | Create a real extra CQ attached to the channel before calling the real destructor; libibverbs returns EBUSY | Channel/context and grants whose cancellation receipt is incomplete; already destroyed QPs stay released |
| `ibv_destroy_cq` / `ibv_dealloc_pd` | Existing real dependent QP/MW controls | The failing resource and remaining dependencies |
| `ibv_close_device` | Existing CPU ABI failure control | Device owner retained; close/drop never retry an ambiguous failure |

The QP syscall filter is confined to one child thread and one descriptor. It
does not take a physical port down or simulate spontaneous device failure.
The channel blocker belongs to the test and survives until its process exits.
Neither fault alters the shared NIC's configuration.

The selected rdma-core implementation calls a void provider `free_context`
and returns zero from `ibv_close_device`. Blocking a close syscall therefore
does not produce a native nonzero verbs return. Device-close error handling
is explicitly an ABI defense, not a claimed native fault result. Physical
device loss and provider-specific teardown behavior remain separate gates.

## Execute the profile

Use `runner: "orbitte-multirail"` with the identity-pinned two-node profile,
two physical `rails`, port 1/GID selection and one resource lock per RNIC.
Select `large-qp-cleanup-error` or `large-channel-busy`, an explicit
`fault_role` (`source` or `requester`), `fault_rail`, READ/WRITE and lane count.
Both rails must be enabled (`mask: 3`); channel failures require
`rdma_events: true`.

```json
{
  "source": "a", "requester": "b",
  "source_kind": "cpu", "requester_kind": "cpu", "transport": "rdma",
  "mode": "large-qp-cleanup-error", "operation": "read",
  "rdma_events": false, "num_lanes": 1,
  "mask": 3, "fault_rail": 0, "fault_role": "source"
}
```

Run a frozen release libtest using `tests/hardware/run.py --profile ...
--binary ... --output ...`, with output outside the checkout. The fixture
admits eight 5 MiB parents (five slices each), withholds an exact successful
CQE and then triggers teardown. It checks native failure/handle identity,
outstanding work, denied writes through the DMA loan, repeated close failure
and recovery rejection. After removing observer references and dropping the
engine, a weak backing reference and a native QP query or channel/CQ identity
and open descriptor must still prove ownership. A reference count alone is insufficient.

The supervisor reaps the exact retained process before releasing RNIC locks.
Fresh processes reuse the RPC addresses, reject the previous source epoch
before opening QPs, transfer two complete byte/guard rounds and drain. The
controller independently rejects missing native receipts, retries, early
backing release, false drain, wrong fault roles and incomplete child reaping.

The [installed cleanup fixture](installed-host.md#native-cleanup-failure) now
exercises the requester QP boundary through the ordinary packaged crate. Its
native hardware result remains unqualified; the libtest results above cannot
substitute for that installed gate.

## Native TENT correspondence

The frozen reference is Mooncake TENT
`e2d6e8e0f2df113d84f9e39379f98c4b67b199ab`.
Its `RdmaEndPoint::deconstructUnlocked` retains QPs when destruction fails;
`RdmaContext::disable` preserves CQ/PD/device resources when endpoint cleanup
fails. This is the same endpoint/context ownership boundary used here.

There is a deliberate difference in failure policy. TENT's endpoint monitor
can retry destruction; its context cleanup logs some later destructor errors
and clears those pointers. OrbitTE reports `Unquiesced` and quarantines the
remaining native owners without automatically retrying an ambiguous failure.
It also preserves unresolved grant loans and quota until a native receipt
proves cancellation. This gate validates that Rust contract; it does not claim
identical error policy or native wire compatibility.

See [qualification](qualification.md) for executed profiles and
[P4's remaining gates](completion-plan.md#p4-remaining-delivery) for limits.
