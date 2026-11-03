//! Frozen TENT's shared prediction. Queue admission and RDMA posting see
//! different bytes ahead, but use the same wire rate and deadline formula.
use std::time::Instant;

pub(crate) fn deadline_mlu(
    ahead: u64,
    length: usize,
    deadline: Option<Instant>,
    now: Instant,
    bytes_per_second: f64,
) -> f64 {
    let Some(deadline) = deadline else { return 0.0 };
    if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
        return 0.0;
    }
    if deadline <= now {
        return f64::MAX;
    }
    (ahead as f64 + length as f64) / bytes_per_second / deadline.duration_since(now).as_secs_f64()
}
