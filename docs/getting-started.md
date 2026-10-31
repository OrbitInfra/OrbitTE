# Quick Start

Build the current source on Linux with Rust 1.85 or later. The default CPU/TCP
path needs no CUDA toolkit, RNIC or native provider library. The published
`orbit-te` alpha is the original package reservation; use this checkout for
the examples below.

## Get the source

```sh
git clone https://github.com/OrbitInfra/OrbitTE.git
cd OrbitTE
cargo run --locked --example tcp_roundtrip
```

The example starts two engines, exports a source allocation, discovers its
descriptor using the source owner's epoch, and reads its bytes over loopback.

```text
18 bytes: hello from OrbitTE
```

## Register, discover and transfer

The complete program is [tcp_roundtrip.rs](../examples/tcp_roundtrip.rs).
The central transfer uses registered allocation addresses:

```rust
let request = TransferRequest {
    remote_staging: false,
    transport: orbit_te::Transport::Tcp,
    local_address: destination.base_address(),
    remote_address: remote.base_address(),
    length: exported.len(),
};
let report = requester.submit_and_wait(
    TransferOp::Read,
    &peer,
    &[request],
    Duration::from_secs(1),
)?;
```

`register_memory` gives the engine ownership of host bytes. `open_segment`
requires the authoritative source epoch; a peer at the same address cannot
replace that authority after restart. Requests resolve registered ranges and
keep their actual backing alive through completion.

Check `report.failure` as well as the completed request and byte counts. A
timeout is not proof that native DMA stopped. See the
[Rust API guide](rust-api.md) and [completion contract](cpu-tcp.md).

## Try the other CPU examples

```sh
cargo run --locked --example external_host
cargo run --locked --example notifications
cargo run --locked --example async_batch
```

| Example | What it demonstrates |
| --- | --- |
| `external_host` | Register ranges of an existing allocation while retaining its owner |
| `notifications` | Transfer data and consume the attached completion notification |
| `async_batch` | Submit and seal an owned asynchronous batch, then wait for its result |

The [async guide](async-batches.md) covers cancellation, detached handles and
resource retention. CPU/TCP can cross machines after selecting a routable
`EngineConfig::bind_addr`; the default binds loopback only.

## Enable RDMA or NVIDIA support

Use the [build guide](build.md) for prerequisites and feature flags. Host RDMA
uses libibverbs and the installed RNIC provider. NVIDIA allocations currently
use synchronized host staging for network transfer; direct GPU RDMA remains
under development. The [capability matrix](capabilities.md) separates these paths.
