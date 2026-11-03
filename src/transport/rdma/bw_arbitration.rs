//! TENT's bounded greedy deadline/bandwidth ordering within one priority and NIC.

use std::time::Instant;

pub(super) struct ArbFlow {
    pub(super) index: usize,
    pub(super) deadline: Option<Instant>,
    pub(super) length: usize,
}

fn predicted_mlu(flow: &ArbFlow, now: Instant, bandwidth: f64, bytes_ahead: u64) -> f64 {
    crate::runtime::deadline_mlu::deadline_mlu(
        bytes_ahead,
        flow.length,
        flow.deadline,
        now,
        bandwidth,
    )
}

/// Index is the original FIFO position and breaks every tie, including the tail.
/// Already posted bytes exclude all candidates. Scratch belongs to the worker.
pub(super) fn order_by_urgency(
    flows: &mut [ArbFlow],
    now: Instant,
    bandwidth: f64,
    mut bytes_ahead: u64,
) {
    if flows.len() < 2 || !bandwidth.is_finite() || bandwidth <= 0.0 {
        return;
    }
    let greedy = flows.len().min(64);
    for slot in 0..greedy {
        let mut best = slot;
        let mut urgency = predicted_mlu(&flows[best], now, bandwidth, bytes_ahead);
        for candidate in slot + 1..flows.len() {
            let score = predicted_mlu(&flows[candidate], now, bandwidth, bytes_ahead);
            if score > urgency || (score == urgency && flows[candidate].index < flows[best].index) {
                best = candidate;
                urgency = score;
            }
        }
        flows.swap(slot, best);
        bytes_ahead = bytes_ahead.saturating_add(flows[slot].length as u64);
    }
    flows[greedy..].sort_unstable_by(|a, b| {
        predicted_mlu(b, now, bandwidth, bytes_ahead)
            .total_cmp(&predicted_mlu(a, now, bandwidth, bytes_ahead))
            .then_with(|| a.index.cmp(&b.index))
    });
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/bw_arbitration.rs"]
mod tests;
