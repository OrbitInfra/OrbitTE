//! Fixed series and inclusive bucket bounds from the frozen TENT registry.
use crate::{TransferOp, Transport};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

pub(super) const TRANSPORTS: [&str; 7] = [
    "unspec", "tcp", "rdma", "nvlink", "io_uring", "gds", "hp_tcp",
];
pub(super) const OPS: [&str; 2] = ["read", "write"];
pub(super) const REASONS: [&str; 4] = ["submit", "poll", "timeout", "canceled"];

pub(crate) fn slot(t: Transport) -> usize {
    match t {
        Transport::Unspec => 0,
        Transport::Tcp => 1,
        Transport::Rdma => 2,
        Transport::Nvlink => 3,
        Transport::IoUring => 4,
        Transport::Gds => 5,
        Transport::HpTcp => 6,
    }
}
pub(super) fn attempt_slot(t: usize, op: TransferOp) -> usize {
    t * 2 + usize::from(op == TransferOp::Write)
}
pub(super) const LATENCY: &[u64] = &[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000];
pub(super) const SIZE: &[u64] = &[
    1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824,
];
pub(super) const MLU: &[u64] = &[100, 250, 500, 750, 900, 1000, 1250, 1500, 2000, 5000];
pub(super) const STAGE: &[u64] = &[10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000];

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Labels {
    None,
    Transport,
    Attempt,
    Failure,
    Pair,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Counter,
    Gauge,
    Histogram(&'static [u64]),
}
pub(super) struct Spec {
    pub name: &'static str,
    pub help: &'static str,
    pub labels: Labels,
    pub kind: Kind,
}
impl Spec {
    const fn new(name: &'static str, help: &'static str, labels: Labels, kind: Kind) -> Self {
        Self {
            name,
            help,
            labels,
            kind,
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Metric {
    ReadBytes,
    WriteBytes,
    ReadRequests,
    WriteRequests,
    ReadFailures,
    WriteFailures,
    Failover,
    Attempts,
    AttemptFailures,
    TaskFailures,
    Infeasible,
    Quarantined,
    Inflight,
    Registered,
    ReadLatency,
    WriteLatency,
    ReadSize,
    WriteSize,
    Mlu,
    QueueWait,
    Dispatch,
    TransportTime,
    AttemptLatency,
}
pub(super) const SPECS: [Spec; 23] = [
    Spec::new(
        "tent_read_bytes_total",
        "Total bytes read via TENT",
        Labels::Transport,
        Kind::Counter,
    ),
    Spec::new(
        "tent_write_bytes_total",
        "Total bytes written via TENT",
        Labels::Transport,
        Kind::Counter,
    ),
    Spec::new(
        "tent_read_requests_total",
        "Total read requests via TENT",
        Labels::Transport,
        Kind::Counter,
    ),
    Spec::new(
        "tent_write_requests_total",
        "Total write requests via TENT",
        Labels::Transport,
        Kind::Counter,
    ),
    Spec::new(
        "tent_read_failures_total",
        "Total read failures via TENT",
        Labels::Transport,
        Kind::Counter,
    ),
    Spec::new(
        "tent_write_failures_total",
        "Total write failures via TENT",
        Labels::Transport,
        Kind::Counter,
    ),
    Spec::new(
        "tent_transport_failover_total",
        "Total cross-transport failover events",
        Labels::Pair,
        Kind::Counter,
    ),
    Spec::new(
        "tent_transport_attempts_total",
        "Total physical transport attempts submitted for execution",
        Labels::Attempt,
        Kind::Counter,
    ),
    Spec::new(
        "tent_transport_attempt_failures_total",
        "Physical transport attempts that terminated with FAILED",
        Labels::Attempt,
        Kind::Counter,
    ),
    Spec::new(
        "tent_task_failures_total",
        "Task-level failures by terminal transport and failure reason",
        Labels::Failure,
        Kind::Counter,
    ),
    Spec::new(
        "tent_deadline_infeasible_total",
        "Transfers whose deadline was already in the past at submit",
        Labels::Transport,
        Kind::Counter,
    ),
    Spec::new(
        "tent_quarantined_batches_total",
        "Batches abandoned by lazyFreeBatch after repeated failed reclaim attempts; reclaimed only at engine teardown",
        Labels::None,
        Kind::Counter,
    ),
    Spec::new(
        "tent_inflight_attempts",
        "In-flight transport attempts (submitted, not yet finished)",
        Labels::Transport,
        Kind::Gauge,
    ),
    Spec::new(
        "tent_registered_buffer_bytes",
        "Registered local buffer bytes",
        Labels::Transport,
        Kind::Gauge,
    ),
    Spec::new(
        "tent_read_latency_us",
        "Read latency distribution in microseconds",
        Labels::Transport,
        Kind::Histogram(LATENCY),
    ),
    Spec::new(
        "tent_write_latency_us",
        "Write latency distribution in microseconds",
        Labels::Transport,
        Kind::Histogram(LATENCY),
    ),
    Spec::new(
        "tent_read_size_bytes",
        "Read request size distribution in bytes",
        Labels::Transport,
        Kind::Histogram(SIZE),
    ),
    Spec::new(
        "tent_write_size_bytes",
        "Write request size distribution in bytes",
        Labels::Transport,
        Kind::Histogram(SIZE),
    ),
    Spec::new(
        "tent_deadline_mlu_permille",
        "Deadline feasibility ratio (MLU x 1000) distribution",
        Labels::Transport,
        Kind::Histogram(MLU),
    ),
    Spec::new(
        "tent_stage_queue_wait_us",
        "Causal chain: queue wait latency in microseconds",
        Labels::Transport,
        Kind::Histogram(STAGE),
    ),
    Spec::new(
        "tent_stage_dispatch_us",
        "Causal chain: dispatch latency in microseconds",
        Labels::Transport,
        Kind::Histogram(STAGE),
    ),
    Spec::new(
        "tent_stage_transport_us",
        "Causal chain: transport execution latency in microseconds",
        Labels::Transport,
        Kind::Histogram(STAGE),
    ),
    Spec::new(
        "tent_transport_attempt_latency_us",
        "Observed physical transport attempt latency in microseconds",
        Labels::Attempt,
        Kind::Histogram(LATENCY),
    ),
];
pub(super) struct Series {
    pub value: AtomicU64,
    pub buckets: Box<[AtomicU64]>,
    pub seen: AtomicBool,
}
pub(crate) struct Recorder {
    pub(super) origin: std::time::Instant,
    pub(super) cells: [Box<[Series]>; SPECS.len()],
    pub enabled: AtomicBool,
}
impl Recorder {
    pub(super) fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            origin: std::time::Instant::now(),
            cells: std::array::from_fn(|i| {
                let spec = &SPECS[i];
                let n = match spec.labels {
                    Labels::None => 1,
                    Labels::Transport => TRANSPORTS.len(),
                    Labels::Attempt => TRANSPORTS.len() * OPS.len(),
                    Labels::Failure => TRANSPORTS.len() * REASONS.len(),
                    Labels::Pair => TRANSPORTS.len() * TRANSPORTS.len(),
                };
                let width = match spec.kind {
                    Kind::Histogram(b) => b.len() + 1,
                    _ => 0,
                };
                (0..n)
                    .map(|_| Series {
                        value: AtomicU64::new(0),
                        seen: AtomicBool::new(false),
                        buckets: (0..width).map(|_| AtomicU64::new(0)).collect(),
                    })
                    .collect()
            }),
        }
    }
    pub(super) fn add(&self, m: Metric, i: usize, n: u64) {
        let c = &self.cells[m as usize][i];
        c.value.fetch_add(n, Relaxed);
        c.seen.store(true, Relaxed);
    }
    pub(super) fn observe(&self, m: Metric, i: usize, n: u64) {
        let Kind::Histogram(bounds) = SPECS[m as usize].kind else {
            return;
        };
        let c = &self.cells[m as usize][i];
        c.buckets[bounds.partition_point(|b| *b < n)].fetch_add(1, Relaxed);
        c.value.fetch_add(n, Relaxed);
    }
}
