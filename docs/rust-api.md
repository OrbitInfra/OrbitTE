# Rust API Guide

The library package is `orbit-te`; the Rust crate is `orbit_te`. This guide
describes the current source API. Generate its full type documentation with:

```sh
cargo doc --locked --no-deps --open
```

Add `--features rdma,cuda` when documenting native providers in an environment
with the [build prerequisites](build.md).

## Engine and registration

`TransferEngine::new(EngineConfig)` starts a source control service and owns
requester/source admission. Its default bind address is loopback. Configuration
and budgets are immutable for that engine instance.

| API | Contract |
| --- | --- |
| `register_memory` | Transfer ownership of host bytes into a registered range |
| `register_host_memory` | Register a range of an existing `Arc<HostMemory>` while retaining its real allocation owner |
| `register_host_memory_batch` | Preflight a set of host ranges before publishing new registration references |
| `unregister_memory` | Retire a registration reference; admitted users retain their actual backing |
| `resolve_local_address` | Resolve a contained registered address range without adopting an arbitrary pointer |

`Permission` distinguishes local-only, remote READ and remote READ/WRITE
authority. Repeated registration of the same range keeps its original
permission. CPU access coordination is shared across aliases.

## Peer authority

```rust
let peer = requester.open_segment(
    source.endpoint(),
    source.source_epoch(),
    Duration::from_secs(1),
)?;
```

The expected `SourceEpoch` must come from the authoritative source owner.
Descriptors identify registered regions but do not replace that authority.
Restarting a process at the same endpoint does not authorize an old peer handle
to use the new process's allocations.

## Requests and reports

`TransferRequest` names the transport, local address, remote address and length.
Both ranges must be contained in one applicable registration. `TransferOp`
selects READ or WRITE. Submission resolves and retains each backing allocation
before transfer effects.

`submit_and_wait` returns `BatchReport` with completed request/byte counts,
failure information, notification status and local drain status. The completed
prefix is not a transaction. `RemoteEffect::Unknown` means an attempted WRITE
may have changed peer bytes; a failed report does not authorize automatic replay.

For asynchronous ownership, see [`Batch`](async-batches.md): submit groups,
seal, inspect or wait for completion, and retain the handle until resource
ownership is understood. Dropping a handle cannot free live native work.

## Native providers

`transport::rdma::RdmaConfig` selects discovery/pins, host direct or staged
memory, progress mode and bounded native resource budgets.
`RdmaProgressMode::Polling` is the default; `EventDriven` consumes native CQ
events while preserving the same data worker and completion validation.

The `cuda` module owns NVIDIA device/pinned-host allocations and synchronized
copies. Device pointers use declared host staging for network movement.
See [RDMA and CUDA](rdma-cuda.md) for exact range, grant and drain requirements.

## Close and drain

`close_source` fences source admission; `close_requester` fences requester
admission. Whole-engine `close` fences both, drains accepted work and closes
shared native resources. A timeout does not complete pending native work.
`Error::Unquiesced` retains the real resources and their charges after physical
cleanup could not be proved.

The runnable [TCP example](../examples/tcp_roundtrip.rs),
[external allocation example](../examples/external_host.rs) and
[async example](../examples/async_batch.rs) use the public source API.
