//! Callable device and route byte-window core from Mooncake v0.3.14.
//! Domain owners supply generations, feedback and control ticks. TENT does
//! not attach this core to its workers. OrbiTE follows the same boundary.
mod classification;
mod config;
pub use classification::{
    Classification, FailureScope, Outcome, classify_async_event, classify_completion,
};
pub use config::{Config, Mode};

use crate::Result;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathState {
    Healthy,
    Congested,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub mode: Mode,
    pub state: PathState,
    pub window_bytes: u64,
    pub inflight_bytes: u64,
    pub deferred_bytes: u64,
    pub generation: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Signals {
    pub backlog_bytes: u64,
    pub delivery_rate_bytes_per_sec: u64,
    pub completed_bytes: u64,
    pub successful_completions: u64,
    pub receiver_pressure: u64,
    pub route_timeouts: u64,
    pub hard_errors: u64,
    pub fatal_failures: u64,
    pub poller_stalled: bool,
}

#[derive(Debug)]
struct Epoch {
    generation: u32,
    window: u64,
    state: PathState,
    success: AtomicBool,
    pressure: AtomicBool,
    error: AtomicBool,
}
impl Epoch {
    fn new(generation: u32, window: u64) -> Self {
        Self {
            generation: generation.max(1),
            window,
            state: PathState::Healthy,
            success: false.into(),
            pressure: false.into(),
            error: false.into(),
        }
    }
}

#[derive(Debug)]
pub struct DomainState {
    config: Config,
    epoch: RwLock<Epoch>,
    inflight: AtomicU64,
    deferred: AtomicU64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionDomain {
    Device,
    Route,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Defer,
    Avoid,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    Window,
    StaleGeneration,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rejection {
    pub decision: Decision,
    pub domain: AdmissionDomain,
    pub reason: Reason,
    pub snapshot: Snapshot,
}

// One CAS loop for admission, saturating observation and receipt accounting.
fn update_bytes(counter: &AtomicU64, update: impl Fn(u64) -> Option<u64>) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let Some(next) = update(current) else {
            return false;
        };
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

impl DomainState {
    pub fn new(config: Config, generation: u32) -> Result<Arc<Self>> {
        config.validate()?;
        Ok(Arc::new(Self {
            epoch: RwLock::new(Epoch::new(generation, config.min_window_bytes)),
            config,
            inflight: 0.into(),
            deferred: 0.into(),
        }))
    }
    fn inspect(&self, epoch: &Epoch) -> Snapshot {
        Snapshot {
            mode: self.config.mode,
            state: epoch.state,
            window_bytes: epoch.window,
            inflight_bytes: self.inflight.load(Ordering::Acquire),
            deferred_bytes: self.deferred.load(Ordering::Relaxed),
            generation: epoch.generation,
        }
    }
    pub fn snapshot(&self) -> Snapshot {
        self.inspect(&self.epoch.read().unwrap_or_else(|p| p.into_inner()))
    }
    pub fn generation(&self) -> u32 {
        self.epoch
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .generation
    }
    fn reserve(
        &self,
        domain: AdmissionDomain,
        generation: u32,
        bytes: u64,
    ) -> std::result::Result<(), Rejection> {
        if self.config.mode == Mode::Off {
            return Ok(());
        }
        let epoch = self.epoch.read().unwrap_or_else(|p| p.into_inner());
        let enforce = self.config.mode == Mode::Enforce;
        if enforce && generation != epoch.generation {
            return Err(Rejection {
                decision: Decision::Avoid,
                reason: Reason::StaleGeneration,
                domain,
                snapshot: self.inspect(&epoch),
            });
        }
        // An empty domain can take one indivisible oversized slice. Independent
        // callers contend on that domain's counter, never a process-wide lock.
        if !update_bytes(&self.inflight, |current| {
            if enforce && current != 0 && (bytes > epoch.window || current > epoch.window - bytes) {
                None
            } else {
                Some(current.saturating_add(bytes))
            }
        }) {
            update_bytes(&self.deferred, |n| Some(n.saturating_add(bytes)));
            return Err(Rejection {
                decision: Decision::Defer,
                reason: Reason::Window,
                domain,
                snapshot: self.inspect(&epoch),
            });
        }
        Ok(())
    }
    fn release(&self, bytes: u64) {
        if self.config.mode != Mode::Off {
            update_bytes(&self.inflight, |n| Some(n.saturating_sub(bytes)));
        }
    }
    pub fn record_signals(&self, generation: u32, signals: Signals) {
        if self.config.mode == Mode::Off {
            return;
        }
        let epoch = self.epoch.read().unwrap_or_else(|p| p.into_inner());
        if epoch.generation != generation {
            return;
        }
        if signals.successful_completions != 0 {
            epoch.success.store(true, Ordering::Relaxed);
        }
        if signals.receiver_pressure != 0 {
            epoch.pressure.store(true, Ordering::Relaxed);
        }
        if signals.route_timeouts != 0 || signals.hard_errors != 0 || signals.fatal_failures != 0 {
            epoch.error.store(true, Ordering::Relaxed);
        }
    }
    pub fn control_tick(&self) {
        if self.config.mode == Mode::Off {
            return;
        }
        let mut epoch = self.epoch.write().unwrap_or_else(|p| p.into_inner());
        let success = epoch.success.swap(false, Ordering::Relaxed);
        let error = epoch.error.swap(false, Ordering::Relaxed);
        if epoch.pressure.swap(false, Ordering::Relaxed) {
            epoch.window = (epoch.window / 2).max(self.config.min_window_bytes);
            epoch.state = PathState::Congested;
        } else if !error && success {
            epoch.window = epoch
                .window
                .saturating_add((self.config.min_window_bytes / 4).max(1))
                .min(self.config.max_window_bytes);
            epoch.state = PathState::Healthy;
        }
    }
    pub fn reset_generation(&self, generation: u32) {
        let mut epoch = self.epoch.write().unwrap_or_else(|p| p.into_inner());
        *epoch = Epoch::new(generation, self.config.min_window_bytes);
        self.deferred.store(0, Ordering::Relaxed);
        // Old permits still own their bytes across a reset.
    }
}

#[derive(Clone, Debug)]
pub struct PathHandle {
    device: Option<(Arc<DomainState>, u32)>,
    route: Option<(Arc<DomainState>, u32)>,
}
impl PathHandle {
    pub fn new(device: Option<Arc<DomainState>>, route: Option<Arc<DomainState>>) -> Self {
        let pin = |state: Arc<DomainState>| {
            let generation = state.generation();
            (state, generation)
        };
        Self {
            device: device.map(pin),
            route: route.map(pin),
        }
    }
    /// The returned permit must travel with its owner until an explicit receipt.
    pub fn try_acquire(&self, bytes: u64) -> std::result::Result<Permit, Rejection> {
        if let Some((device, generation)) = &self.device {
            device.reserve(AdmissionDomain::Device, *generation, bytes)?;
        }
        if let Some((route, generation)) = &self.route {
            if let Err(e) = route.reserve(AdmissionDomain::Route, *generation, bytes) {
                if let Some((device, _)) = &self.device {
                    device.release(bytes);
                }
                return Err(e);
            }
        }
        for (domain, entry) in [
            (AdmissionDomain::Device, &self.device),
            (AdmissionDomain::Route, &self.route),
        ] {
            if let Some((owner, generation)) = entry {
                if owner.config.mode == Mode::Enforce {
                    let snapshot = owner.snapshot();
                    if snapshot.generation != *generation {
                        for (held, _) in [&self.device, &self.route].into_iter().flatten() {
                            held.release(bytes);
                        }
                        return Err(Rejection {
                            decision: Decision::Avoid,
                            domain,
                            reason: Reason::StaleGeneration,
                            snapshot,
                        });
                    }
                }
            }
        }
        Ok(Permit {
            path: Some(self.clone()),
            bytes,
        })
    }
}

#[must_use = "keep the reservation until its owner supplies a completion receipt"]
#[derive(Debug)]
pub struct Permit {
    path: Option<PathHandle>,
    bytes: u64,
}
impl Permit {
    pub fn is_current(&self) -> bool {
        self.path.as_ref().is_some_and(|p| {
            (p.device.is_some() || p.route.is_some())
                && [&p.device, &p.route]
                    .into_iter()
                    .flatten()
                    .all(|(s, g)| s.generation() == *g)
        })
    }
    /// Completes at most once. Dropping an active permit cannot return its
    /// domain charges. Call this after a known-unposted rollback or native drain.
    pub fn complete(&mut self, outcome: Outcome, scope: FailureScope) -> bool {
        let Some(path) = self.path.take() else {
            return false;
        };
        for (is_route, entry) in [(false, path.device), (true, path.route)] {
            let Some((owner, generation)) = entry else {
                continue;
            };
            let shared = matches!(
                scope,
                FailureScope::Cq | FailureScope::Port | FailureScope::Device
            );
            let mut signals = Signals::default();
            match outcome {
                Outcome::Success => signals.successful_completions = 1,
                Outcome::Congestion => signals.receiver_pressure = 1,
                Outcome::ReceiverPressure if is_route => signals.receiver_pressure = 1,
                Outcome::Fatal
                | Outcome::RouteTimeout
                | Outcome::LocalConfiguration
                | Outcome::RemoteMetadata
                    if is_route || shared =>
                {
                    signals.hard_errors = 1
                }
                _ => {}
            }
            owner.record_signals(generation, signals);
            owner.release(self.bytes);
        }
        true
    }
}
#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/adaptive_congestion_control.rs"]
mod tests;
