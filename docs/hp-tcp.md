# HP-TCP

HP-TCP moves registered host bytes over persistent TCP connections. A WRITE completes when the source acknowledges the full length. A READ completes when the requester has received the matching response and all its data. The ordinary control service publishes endpoints and handles notifications.

## Enable HP-TCP

Select the payload transport on both peers. HP-TCP is built into the crate and starts no listener or worker until enabled.

```rust
use orbit_te::{EngineConfig, HpTcpConfig, TransferEngine};

fn main() -> Result<(), Box<dyn std::error::Error>> {
let engine = TransferEngine::new(EngineConfig {
    tcp: false,
    hp_tcp: HpTcpConfig {
        enable: true,
        rail_addresses: vec!["127.0.0.1".parse()?, "127.0.0.2".parse()?],
        ..Default::default()
    },
    ..Default::default()
})?;
engine.close(std::time::Duration::from_secs(5))?;
Ok(())
}
```

Use `Transport::HpTcp`, an `"hp_tcp"` policy candidate, or `Transport::Unspec`. Both engines need HP-TCP enabled, matching rail counts and host registrations. An explicit hint fails before payload I/O if the route is unavailable. The engine rejects enabling ordinary TCP payloads and HP-TCP together.

| `hp_tcp` field | Default | Meaning |
| --- | --- | --- |
| `enable` | false | Install this payload provider |
| `bind_address`, `advertise_address` | empty | Listen on IPv4 wildcard and derive the published host from the configured control address |
| `rail_addresses`, `port` | empty list, 0 | Ordered numeric IPv4 rails and a shared listener port. Zero lets the OS select a port |
| `worker_count`, `connections_per_peer` | 16, 4 | Socket reactors and rotating lanes per peer |
| `max_outstanding_tasks`, `max_outstanding_bytes` | 4,096, 4 GiB | Transport admission per physical owner |
| `max_transfer_bytes` | 1 GiB | Maximum frame body. Engine, registration and peer limits also apply |
| `connect_timeout_ms`, `progress_timeout_ms` | 2,000, 30,000 | Connection setup and stalled I/O limits, also bounded by the submission deadline |
| `idle_connection_timeout_ms` | 60,000 | Retire a pooled client socket once its lane is idle |

## Rails and ownership

Each lane runs on one socket worker. Peers rotate independently. A READ splits evenly across rails when each rail can carry at least 1 MiB. The first rails carry any remainder. Each slice binds the corresponding local IPv4 address and connects to the peer's address at the same index. A WRITE always uses a single lane. Each rail needs at least one lane. With multiple rails, use a wildcard listener. A single rail can also use a matching bind address.

The source advertises globally readable host regions, with write access only for `RemoteReadWrite`. Registration IDs use the source incarnation. The source checks the requested range, ID and permission before touching memory. Removing a registration denies new lookups while an accepted handler holds the backing through its response or socket teardown. A rejected WRITE drains exactly its framed body under the progress timeout before the source replies and closes. Public free and cancel retain the physical owner's charges until every slice stops accessing memory.

A dirty stream is discarded. A missing WRITE ACK after any header bytes were sent reports an unknown remote effect. A failed READ can leave complete chunks at its destination. A sliced READ cancels its siblings and waits for every socket operation before the engine considers another eligible transport. OrbitTE keeps an admitted owner's registration fixed during failover. CPU-backed HP-TCP excludes device and CUDA-owned pinned allocations, TLS, WRITE striping and dynamic rail failover.

## Wire and limits

The payload uses TENT's big-endian `MCTP` framing. The 48-byte request carries the opcode, request ID, generation-scoped registration ID, absolute remote address and length. The 32-byte response carries status, the same request ID and committed bytes. Reserved fields must be zero. Successful READ responses precede their payload. WRITE responses follow it.

OrbitTE advertises the source incarnation through Describe. An enabled source appends a bounded endpoint trailer after the existing region records: a big-endian port, one-byte rail count, a reserved zero byte, a big-endian 64-bit frame limit and four octets per IPv4 address. Each region records HP-TCP eligibility alongside its other transport capabilities. This bootstrap follows the [wire policy](tent-parity.md#wire-and-consumer-boundaries). Payload fixtures cover the frozen TENT bytes. Cross-implementation qualification is scheduled separately.

OrbitTE uses one single-thread Tokio reactor per configured worker, limits workers to 256, lanes per peer to 4,096, and advertised rails to 16. Session and lane tables have explicit limits. Linux CI exercises real sockets, concurrent transfers and separate processes on loopback IPv4 aliases. The physical two-interface gate is listed in [qualification](qualification.md#hp-tcp-rail-verification).
