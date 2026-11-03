# Metrics

Build with `--features metrics` to expose engine metrics over HTTP. A requester
counts one logical result per physical owner, including when several public
tasks merge. A retry records a second transport attempt. A successful READ
after an RDMA failure can therefore add bytes on TCP and an attempt failure
on RDMA.

## Configure the endpoint

With the feature enabled, each engine defaults to `0.0.0.0:9100`. Set
`http_host` to `127.0.0.1` for a loopback-only endpoint. Give engines on the
same host distinct ports. If the port is occupied, the engine keeps recording
and logging. `metrics().http_port()` on the enabled handle returns zero in
that case. Set `enabled` to false at construction to omit the listener,
periodic reporter and recording cells.

| File key (under `metrics`) | Environment | Default |
| --- | --- | --- |
| `enabled` | `TENT_METRICS_ENABLED` | `true` |
| `http_host` | `TENT_METRICS_HTTP_HOST` | `0.0.0.0` |
| `http_port` | `TENT_METRICS_HTTP_PORT` | `9100` |
| `http_server_threads` | `TENT_METRICS_HTTP_SERVER_THREADS` | `2` |
| `report_interval_seconds` | `TENT_METRICS_REPORT_INTERVAL` | `30`. Zero disables periodic logging. |

Resolution is per field. A key in the JSON file wins over the environment,
which wins over the typed `EngineConfig.metrics` value. Omitted typed fields
take the defaults above. Set `EngineConfig.metrics_config_file` to the JSON
file path. The host accepts an IP literal or a DNS hostname. The loader accepts
nested keys and slash paths such as
`metrics/http_port`. Invalid settings return a typed construction error. Ports
range from 1 to 65535. The fixed HTTP pool accepts 1–64 threads per engine.

```json
{"metrics":{"http_host":"127.0.0.1","http_port":9101,"report_interval_seconds":0}}
```

```sh
curl -fsS http://127.0.0.1:9101/health
curl -fsS http://127.0.0.1:9101/metrics
curl -fsS http://127.0.0.1:9101/metrics/json
```

`/metrics` returns Prometheus text. `/metrics/json` aggregates label values.
Histogram JSON contains the count, sum and disjoint counts at the finite
boundaries. Prometheus uses cumulative buckets through `+Inf`. A sub-microsecond
sample can have a zero sum and still increments the count. `/metrics/summary`
returns the summary available via the `log` facade.

A successful 64-byte TCP READ contributes the following samples.

```text
tent_read_bytes_total{transport="tcp"} 64
tent_transport_attempts_total{transport="tcp",operation="read"} 1
```

## Transfer metrics

Each entry with `{read,write}` denotes two separate families. Supported
transport values are `tcp`, `rdma`, `nvlink`, `io_uring` and `gds`. Failure
reasons are `submit`, `poll`, `timeout` and `canceled`.

| Family | Type | Labels and meaning |
| --- | --- | --- |
| `tent_{read,write}_bytes_total` | Counter | `transport`. Successful logical bytes on the final route. |
| `tent_{read,write}_requests_total` | Counter | `transport`. Completed and failed logical requests. |
| `tent_{read,write}_failures_total` | Counter | `transport`. Failed requests, including short file I/O. |
| `tent_transport_failover_total` | Counter | `from,to`. Each recovery decision that advances an owner. |
| `tent_transport_attempts_total`, `tent_transport_attempt_failures_total` | Counter | `transport,operation`. Physical attempts and failed attempts. |
| `tent_task_failures_total` | Counter | `transport,reason`. Terminal task failures. TENT exposes this breakdown in Prometheus only. |
| `tent_deadline_infeasible_total` | Counter | `transport`. An expired request deadline at submission or an S8 infeasibility drop. |
| `tent_quarantined_batches_total` | Counter | Unlabelled. Abandoned batches entering quarantine. |
| `tent_inflight_attempts`, `tent_registered_buffer_bytes` | Gauge | `transport`. Live attempts and bytes registered with each transport. |
| `tent_{read,write}_latency_us`, `tent_{read,write}_size_bytes` | Histogram | `transport`. End-to-end latency and successful owner length. |
| `tent_deadline_mlu_permille` | Histogram | `transport`. Successful transfer latency divided by the request's deadline window, multiplied by 1,000. |
| `tent_stage_queue_wait_us`, `tent_stage_dispatch_us`, `tent_stage_transport_us` | Histogram | `transport`. Admission to dispatch, dispatch to first attempt, and execution through logical completion. |
| `tent_transport_attempt_latency_us` | Histogram | `transport,operation`. Elapsed time for one attempt. |

io_uring uses the published memory without registering it with the provider.
Host memory can also take a GDS or pinned NVLink path without creating a cuFile
buffer registration or CUDA IPC export. Those operations contribute transfer
metrics. The buffer gauge counts direct RDMA registrations, NVLink-exportable
device ranges, successful CUDA GDS registrations and TCP-published bytes.

### Histogram boundaries

Each finite upper bound is inclusive.

| Domain | Upper bounds |
| --- | --- |
| Logical and attempt latency (µs) | 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000 |
| Size (bytes) | 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824 |
| Deadline MLU (per mille) | 100, 250, 500, 750, 900, 1000, 1250, 1500, 2000, 5000 |
| Stage latency (µs) | 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000 |

Runtime resource totals use an `orbitte_` prefix. The engine reports live
batches, requester workers and bytes. Configured owners also report queued
and dispatching owners and bytes, deadline drops, QoS inflight reservations,
active and reserved receiver credits, and RDMA rail errors, probes, recoveries
and open rails. The scrape carries no peer identity, raw addresses or keys.

| OrbitTE owner series | Type and unit |
| --- | --- |
| `orbitte_rdma_inflight_bytes`, `orbitte_rdma_allocated_bytes_total` | Live smart-selection bytes (gauge) and lifetime assigned slice bytes (counter), including paused NICs |
| `orbitte_receiver_credit_available_{bytes,requests,staging_slots,consumer_slots}`, `orbitte_receiver_credit_consumed_{bytes,requests,staging_slots,consumer_slots}` | Sender balances across retained bindings (gauges) |
| `orbitte_receiver_active_senders`, `orbitte_receiver_retired_senders_total` | Receiver sessions (live gauge and retired counter) |
| `orbitte_receiver_reserved_{bytes,requests,staging_slots,consumer_slots}` | Capacity committed to receiver-issued windows (gauges) |
| `orbitte_receiver_{active,retained}_{staging_slots,consumer_slots}` | Physical Pin owners and the subset with unconfirmed cleanup (gauges) |

TENT's `qos_metrics.cpp` calculates goodput, Jain fairness and isolation
leakage from benchmark class samples. Those quantities need an experiment with
class timings, weights and isolated throughput and have no live exporter series.

For example, `orbitte_dispatching_owners` and `orbitte_dispatching_bytes`
show the live dispatch window. `orbitte_qos_inflight_requests` and
`orbitte_receiver_credit_reserved_owners` can distinguish a QoS charge from
a sender waiting on a receiver grant. `orbitte_queue_deadline_drops_total`
counts rejected queue dispatches. `orbitte_rdma_open_rails` is a gauge, while
`orbitte_rdma_rail_errors_total`, `orbitte_rdma_rail_probes_total` and
`orbitte_rdma_rail_recoveries_total` are cumulative.

On an initialized handle, `set_enabled(false)` pauses counter and histogram
sampling. Gauge pairs continue across the toggle, so unregistering a buffer or
finishing an attempt while paused cannot leave a stale value behind.

After sampling resumes, a completion can add known bytes. A timing sample
requires both timestamps. An attempt started while sampling was paused supplies
no latency observation.
