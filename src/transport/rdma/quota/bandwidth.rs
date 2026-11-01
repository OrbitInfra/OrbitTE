//! Per-NIC payload observations, separate from physical grant admission.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::SchedulingParams;

/// TENT NIC load snapshot. Unavailable NICs are omitted by the selector.
/// These estimates describe local initiated payloads, not remote or wire proofs.
#[derive(Clone, Debug)]
pub struct NicLoadStats {
    pub device_id: usize,
    pub device_name: String,
    pub link_speed_gbps: Option<f64>,
    pub inflight_bytes: usize,
    pub posted_bytes: u64,
    pub completed_bytes: u64,
    pub ewma_bandwidth_bps: f64,
    pub transmit_bandwidth_bps: f64,
    pub selection_samples: u64,
    pub transmit_samples: u64,
    pub busy_time: Duration,
}

struct MeterSample {
    at: Instant,
    busy: Duration,
    completed: u64,
}

struct Bandwidth {
    link_speed_mbps: Option<u64>,
    seed: f64,
    transmit_bps: f64,
    posted: u64,
    completed: u64,
    busy: Duration,
    since: Option<Instant>,
    last: Instant,
    meter: Option<MeterSample>,
    tainted: bool,
    generation: u64,
    selection_samples: u64,
    transmit_samples: u64,
}

#[repr(align(64))]
pub(in crate::transport::rdma) struct DeviceInfo {
    params: SchedulingParams,
    selection_bps: AtomicU64,
    state: Mutex<Bandwidth>,
}

impl DeviceInfo {
    pub(in crate::transport::rdma) fn new(params: SchedulingParams) -> Self {
        let seed = params.seed(None);
        Self {
            params,
            selection_bps: AtomicU64::new(seed.to_bits()),
            state: Mutex::new(Bandwidth {
                link_speed_mbps: None,
                seed,
                transmit_bps: seed,
                posted: 0,
                completed: 0,
                busy: Duration::ZERO,
                since: None,
                last: Instant::now(),
                meter: None,
                tainted: false,
                generation: 0,
                selection_samples: 0,
                transmit_samples: 0,
            }),
        }
    }

    pub(in crate::transport::rdma) fn bandwidth(&self) -> f64 {
        f64::from_bits(self.selection_bps.load(Ordering::Relaxed))
    }

    pub(in crate::transport::rdma) fn set_bandwidth(&self, mbps: Option<u64>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.link_speed_mbps == mbps {
            return;
        }
        state.link_speed_mbps = mbps;
        state.seed = self.params.seed(mbps);
        state.transmit_bps = state.seed;
        self.selection_bps
            .store(state.seed.to_bits(), Ordering::Relaxed);
        state.invalidate();
    }

    pub(in crate::transport::rdma) fn invalidate(&self) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .invalidate();
    }

    pub(in crate::transport::rdma) fn posted(&self, bytes: u64, failed: bool, now: Instant) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let now = state.advance(now);
        if bytes != 0 {
            if state.posted == 0 {
                state.since = Some(now);
            }
            state.posted += bytes;
            if !state.tainted
                && state.meter.as_ref().is_none_or(|m| {
                    now.duration_since(m.at) > self.params.transmit_meter_max_interval
                })
            {
                state.meter = Some(MeterSample {
                    at: now,
                    busy: state.busy_at(now),
                    completed: state.completed,
                });
            }
        }
        if failed {
            state.invalidate();
        }
        state.generation
    }

    pub(in crate::transport::rdma) fn ended(&self, bytes: u64, success: bool, now: Instant) {
        if bytes == 0 {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let now = state.advance(now);
        // SQ identity validation ensures each charged WR reaches this once.
        state.posted -= bytes;
        if success {
            state.completed = state.completed.saturating_add(bytes);
        } else {
            state.invalidate();
        }
        if state.posted == 0 {
            if let Some(since) = state.since.take() {
                state.busy = state.busy.saturating_add(now.duration_since(since));
            }
        }
        if state.tainted && state.posted == 0 {
            state.tainted = false;
            state.meter = None;
        }
    }

    pub(in crate::transport::rdma) fn sample(&self, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let now = state.advance(now);
        if !state.tainted {
            let busy = state.busy_at(now);
            if let Some(old) = &state.meter {
                let elapsed = now.duration_since(old.at);
                if elapsed >= self.params.transmit_meter_interval {
                    let active = busy.saturating_sub(old.busy);
                    let completed = state.completed.saturating_sub(old.completed);
                    if elapsed <= self.params.transmit_meter_max_interval
                        && !active.is_zero()
                        && completed != 0
                    {
                        state.transmit_bps = self.params.learn(
                            state.transmit_bps,
                            completed as f64 / active.as_secs_f64(),
                            self.params.transmit_bandwidth_learning_rate,
                            state.seed,
                        );
                        state.transmit_samples = state.transmit_samples.saturating_add(1);
                    }
                    state.meter = Some(MeterSample {
                        at: now,
                        busy,
                        completed: state.completed,
                    });
                }
            }
        }
    }

    pub(in crate::transport::rdma) fn observe(
        &self,
        bytes: usize,
        elapsed: Duration,
        generation: u64,
    ) {
        if bytes == 0 || elapsed.is_zero() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.tainted || generation != state.generation {
            return;
        }
        let learned = self.params.learn(
            self.bandwidth(),
            bytes as f64 / elapsed.as_secs_f64(),
            self.params.bandwidth_learning_rate,
            state.seed,
        );
        self.selection_bps
            .store(learned.to_bits(), Ordering::Relaxed);
        state.selection_samples = state.selection_samples.saturating_add(1);
    }

    pub(in crate::transport::rdma) fn snapshot(
        &self,
        id: usize,
        name: &str,
        inflight: usize,
    ) -> NicLoadStats {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        NicLoadStats {
            device_id: id,
            device_name: name.into(),
            link_speed_gbps: state.link_speed_mbps.map(|rate| rate as f64 / 1000.0),
            inflight_bytes: inflight,
            posted_bytes: state.posted,
            completed_bytes: state.completed,
            ewma_bandwidth_bps: self.bandwidth(),
            transmit_bandwidth_bps: state.transmit_bps,
            selection_samples: state.selection_samples,
            transmit_samples: state.transmit_samples,
            busy_time: state.busy_at(Instant::now().max(state.last)),
        }
    }
}

impl Bandwidth {
    fn advance(&mut self, now: Instant) -> Instant {
        self.last = now.max(self.last);
        self.last
    }
    fn busy_at(&self, now: Instant) -> Duration {
        self.busy.saturating_add(
            self.since
                .map_or(Duration::ZERO, |since| now.saturating_duration_since(since)),
        )
    }
    fn invalidate(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.tainted = self.posted != 0;
        self.meter = None;
    }
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/quota/bandwidth.rs"]
mod tests;
