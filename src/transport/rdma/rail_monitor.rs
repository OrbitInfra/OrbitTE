//! Health of one requester NIC / peer NIC pair. The shared context monitor
//! observes port generations. A slice owns its probe through actual completion.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::{
    RdmaConfig,
    path::{self, NicPath},
};
use crate::{Error, Result, SourceEpoch};
use serde::Deserialize;

const MAX_COOLDOWN: Duration = Duration::from_secs(300);

#[derive(Deserialize, Default)]
struct Mapping {
    #[serde(default)]
    all: Vec<Pair>,
    #[serde(default)]
    direct: Vec<Pair>,
}
#[derive(Deserialize)]
struct Pair {
    local: String,
    remote: String,
}
struct Settings {
    threshold: u32,
    window: Duration,
    cooldown: Duration,
    interval: Duration,
    limit: usize,
    mapping: Option<Mapping>,
}
impl Settings {
    fn new(c: &RdmaConfig) -> Self {
        let mapping = c
            .rail_topology
            .as_deref()
            .filter(|s| !s.is_empty())
            .and_then(|s| match serde_json::from_str::<Mapping>(s) {
                Ok(m) => Some(m),
                Err(e) => {
                    log::warn!("invalid RDMA rail topology, using discovered routes: {e}");
                    None
                }
            });
        Self {
            threshold: c.rail_error_threshold,
            window: c.rail_error_window,
            cooldown: c.rail_cooldown.min(MAX_COOLDOWN),
            interval: c.rail_probe_interval,
            limit: c.max_rail_pairs,
            mapping,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct Key {
    peer: SourceEpoch,
    local: usize,
    remote: usize,
    local_domain: [u8; 16],
    remote_domain: [u8; 16],
}
impl Key {
    fn new(peer: SourceEpoch, l: &NicPath, r: &NicPath) -> Self {
        Self {
            peer,
            local: l.nic_id,
            remote: r.nic_id,
            local_domain: l.domain,
            remote_domain: r.domain,
        }
    }
}
#[derive(Default)]
struct State {
    consecutive: u32,
    last_error: Option<Instant>,
    resume: Option<Instant>,
    last_probe: Option<Instant>,
    cooldown: Duration,
    probe: Option<u64>,
    half_open: bool,
    active: usize,
    generation: u64,
}
impl State {
    fn can_admit(&self, now: Instant, c: &Settings) -> bool {
        self.resume.is_none_or(|t| {
            self.probe.is_none()
                && (now >= t
                    || self
                        .last_probe
                        .is_none_or(|p| now.saturating_duration_since(p) >= c.interval))
        })
    }
    fn pause(&mut self, now: Instant, c: &Settings, backoff: bool) {
        self.cooldown = if backoff {
            (self.cooldown * 2).min(MAX_COOLDOWN)
        } else {
            c.cooldown
        };
        self.resume = now.checked_add(self.cooldown);
        self.last_probe = Some(now);
        self.half_open = false;
        self.probe = None;
        self.generation = self.generation.wrapping_add(1);
    }
}

struct Table {
    pairs: HashMap<Key, State>,
    ports: [Option<(u64, bool)>; 64],
    errors_by_nic: [u64; 64],
    probes_by_nic: [u64; 64],
    next: u64,
    probes: u64,
    recoveries: u64,
    errors: u64,
}
impl Default for Table {
    fn default() -> Self {
        Self {
            pairs: HashMap::new(),
            ports: [None; 64],
            errors_by_nic: [0; 64],
            probes_by_nic: [0; 64],
            next: 0,
            probes: 0,
            recoveries: 0,
            errors: 0,
        }
    }
}
#[derive(Default)]
pub(super) struct RailMonitor {
    config: OnceLock<Settings>,
    #[cfg(test)]
    clock: Mutex<Option<Instant>>,
    table: Mutex<Table>,
}

impl RailMonitor {
    pub(super) fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(now) = *self.clock.lock().unwrap_or_else(|p| p.into_inner()) {
            return now;
        }
        Instant::now()
    }
    #[cfg(test)]
    pub(in crate::transport::rdma) fn set_time(&self, now: Instant) {
        *self.clock.lock().unwrap() = Some(now);
    }
    pub(super) fn configure(&self, c: &RdmaConfig) {
        self.config.get_or_init(|| Settings::new(c));
    }
    fn settings(&self) -> Result<&Settings> {
        self.config
            .get()
            .ok_or(Error::Internal("RDMA rail monitor not configured"))
    }

    // The remote table has already been filtered by the remote memory owner.
    // Same-name links win discovery ties. An explicit topology replaces that map.
    fn route<'a>(
        &self,
        table: &Table,
        c: &Settings,
        peer: SourceEpoch,
        l: &NicPath,
        remote: &'a [NicPath],
        now: Instant,
    ) -> Option<&'a NicPath> {
        if c.mapping.is_none() {
            if let Ok(r) = path::remote_path(l, remote) {
                if table
                    .pairs
                    .get(&Key::new(peer, l, r))
                    .is_none_or(|s| s.resume.is_none())
                {
                    return Some(r);
                }
            }
        }
        remote
            .iter()
            .filter(|r| {
                path::compatible(l, r)
                    && c.mapping.as_ref().is_none_or(|m| {
                        m.all
                            .iter()
                            .any(|p| p.local == l.name && p.remote == r.name)
                    })
                    && table
                        .pairs
                        .get(&Key::new(peer, l, r))
                        .is_none_or(|s| s.can_admit(now, c))
            })
            .min_by_key(|r| {
                (
                    table
                        .pairs
                        .get(&Key::new(peer, l, r))
                        .is_some_and(|s| s.resume.is_some()),
                    !c.mapping.as_ref().is_some_and(|m| {
                        m.direct
                            .iter()
                            .any(|p| p.local == l.name && p.remote == r.name)
                    }),
                    l.name != r.name,
                )
            })
    }
    pub(super) fn mask(
        &self,
        peer: SourceEpoch,
        local: &[NicPath],
        remote: &[NicPath],
        now: Instant,
    ) -> Result<(u64, u64)> {
        let c = self.settings()?;
        let table = self.table.lock().unwrap_or_else(|p| p.into_inner());
        // Preserve the established physical same-name preference. Fall back to
        // other compatible pairs when the preferred set is unavailable.
        let mut healthy = 0;
        let mut mask = 0;
        for l in local {
            if let Some(r) = self.route(&table, c, peer, l, remote, now) {
                mask |= 1u64 << l.nic_id;
                if table
                    .pairs
                    .get(&Key::new(peer, l, r))
                    .is_none_or(|s| s.resume.is_none())
                {
                    healthy |= 1u64 << l.nic_id;
                }
            }
        }
        if c.mapping.is_none() {
            let preferred = path::local_device_mask(local, remote);
            if mask & preferred != 0 {
                mask &= preferred;
            }
        }
        Ok((mask, healthy & mask))
    }

    pub(super) fn admit<'a>(
        self: &Arc<Self>,
        peer: SourceEpoch,
        l: &NicPath,
        remote: &'a [NicPath],
        now: Instant,
    ) -> Result<(&'a NicPath, RailLease)> {
        let c = self.settings()?;
        let mut t = self.table.lock().unwrap_or_else(|p| p.into_inner());
        let r = self
            .route(&t, c, peer, l, remote, now)
            .ok_or(Error::Unavailable("no healthy permitted RDMA rail"))?;
        let key = Key::new(peer, l, r);
        if !t.pairs.contains_key(&key) && t.pairs.len() >= c.limit {
            let idle = t
                .pairs
                .iter()
                .find(|(_, s)| s.active == 0 && s.resume.is_none() && s.consecutive == 0)
                .map(|(k, _)| *k)
                .ok_or(Error::ResourceExhausted)?;
            t.pairs.remove(&idle);
        }
        t.next = t.next.checked_add(1).ok_or(Error::ResourceExhausted)?;
        let id = t.next;
        let s = t.pairs.entry(key).or_default();
        s.active += 1;
        let generation = s.generation;
        if let Some(end) = s.resume {
            s.probe = Some(id);
            s.half_open = now >= end;
            s.last_probe = Some(now);
            t.probes = t.probes.saturating_add(1);
            t.probes_by_nic[key.local] = t.probes_by_nic[key.local].saturating_add(1);
        }
        Ok((
            r,
            RailLease {
                owner: self.clone(),
                key,
                id,
                generation,
                done: AtomicBool::new(false),
            },
        ))
    }

    pub(super) fn port(&self, id: usize, generation: u64, available: bool, now: Instant) {
        let Some(c) = self.config.get() else {
            return;
        };
        let mut t = self.table.lock().unwrap_or_else(|p| p.into_inner());
        let previous = t.ports[id].replace((generation, available));
        if previous.is_some_and(|(g, up)| g != generation || (up && !available)) {
            let mut affected = 0;
            for (k, s) in &mut t.pairs {
                if k.local == id {
                    if s.resume.is_none() || s.half_open {
                        s.pause(now, c, s.half_open);
                    } else {
                        // A port-event burst invalidates old trials. Extending
                        // the same open interval would starve a healthy retry.
                        s.generation = s.generation.wrapping_add(1);
                        s.probe = None;
                    }
                    affected += 1;
                }
            }
            t.errors = t.errors.saturating_add(affected);
            t.errors_by_nic[id] = t.errors_by_nic[id].saturating_add(affected);
        }
    }

    pub(super) fn nic_snapshot(&self, stats: &mut [super::NicLoadStats]) {
        let t = self.table.lock().unwrap_or_else(|p| p.into_inner());
        for n in stats {
            n.rail_errors = t.errors_by_nic[n.device_id];
            n.rail_probes = t.probes_by_nic[n.device_id];
            n.open_rails = t
                .pairs
                .iter()
                .filter(|(k, s)| k.local == n.device_id && s.resume.is_some())
                .count();
        }
    }

    pub(super) fn snapshot(&self, stats: &mut super::RdmaStats) {
        let t = self.table.lock().unwrap_or_else(|p| p.into_inner());
        stats.rail_errors = t.errors;
        stats.rail_probes = t.probes;
        stats.rail_recoveries = t.recoveries;
        stats.open_rails = t.pairs.values().filter(|s| s.resume.is_some()).count();
        stats.half_open_rails = t.pairs.values().filter(|s| s.half_open).count();
    }

    fn finish(&self, lease: &RailLease, outcome: Option<bool>, now: Instant) {
        let Some(c) = self.config.get() else {
            return;
        };
        let mut t = self.table.lock().unwrap_or_else(|p| p.into_inner());
        if outcome == Some(false) {
            t.errors = t.errors.saturating_add(1);
            t.errors_by_nic[lease.key.local] = t.errors_by_nic[lease.key.local].saturating_add(1);
        }
        let Some(s) = t.pairs.get_mut(&lease.key) else {
            return;
        };
        let probe = s.probe == Some(lease.id);
        if lease.generation != s.generation || (s.resume.is_some() && !probe) {
            return;
        }
        match outcome {
            Some(true) => {
                let recovered = s.resume.is_some();
                s.resume = None;
                s.probe = None;
                s.half_open = false;
                s.cooldown = Duration::ZERO;
                s.consecutive = 0;
                s.last_error = None;
                if recovered {
                    t.recoveries = t.recoveries.saturating_add(1);
                    log::info!(
                        "RDMA rail recovered on NIC pair {} / {}",
                        lease.key.local,
                        lease.key.remote
                    );
                }
            }
            Some(false) => {
                s.consecutive = if s
                    .last_error
                    .is_some_and(|at| now.saturating_duration_since(at) <= c.window)
                {
                    s.consecutive.saturating_add(1)
                } else {
                    1
                };
                s.last_error = Some(now);
                if s.half_open || (s.resume.is_none() && s.consecutive >= c.threshold) {
                    s.pause(now, c, s.half_open);
                    log::warn!(
                        "RDMA rail paused on NIC pair {} / {}",
                        lease.key.local,
                        lease.key.remote
                    );
                } else if probe {
                    s.probe = None;
                }
            }
            None if probe => {
                s.probe = None;
                s.half_open = false;
            }
            None => {}
        }
    }
}

pub(super) struct RailLease {
    owner: Arc<RailMonitor>,
    key: Key,
    id: u64,
    generation: u64,
    done: AtomicBool,
}
impl RailLease {
    #[cfg(feature = "cuda")]
    pub(super) fn fault(&self, error: &Error) {
        let result = if matches!(error, Error::Backend { .. } | Error::Unquiesced { .. }) {
            Some(false)
        } else {
            None
        };
        if !self.done.swap(true, Ordering::AcqRel) {
            self.owner.finish(self, result, self.owner.now());
        }
    }
    pub(super) fn setup_failed(&self, error: &Error) {
        if self.done.swap(true, Ordering::AcqRel) {
            return;
        }
        let probe = self
            .owner
            .table
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pairs
            .get(&self.key)
            .is_some_and(|s| s.probe == Some(self.id));
        let result = if !probe && matches!(error, Error::Backend { .. } | Error::Unquiesced { .. })
        {
            Some(false)
        } else {
            None
        };
        self.owner.finish(self, result, self.owner.now());
    }
    pub(super) fn receipt(&self, outcome: &Result<()>) {
        let result = match outcome {
            Ok(()) => Some(true),
            Err(Error::Backend { .. } | Error::Unquiesced { .. }) => Some(false),
            _ => None,
        };
        if !self.done.swap(true, Ordering::AcqRel) {
            self.owner.finish(self, result, self.owner.now());
        }
    }
}
impl Drop for RailLease {
    fn drop(&mut self) {
        if !self.done.load(Ordering::Acquire) {
            self.owner.finish(self, None, self.owner.now());
        }
        let mut t = self.owner.table.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = t.pairs.get_mut(&self.key) {
            s.active -= 1;
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/rail_monitor.rs"]
mod tests;
