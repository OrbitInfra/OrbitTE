//! Frozen TENT head-only and per-entry aging decisions, without temporary vectors.

use std::time::{Duration, Instant};

pub(super) fn timed_out(enqueued: Instant, now: Instant, timeout: Duration) -> bool {
    now.checked_duration_since(enqueued)
        .is_some_and(|age| age >= timeout)
}

pub(super) const CHECK_INTERVAL: Duration = Duration::from_millis(1);
