//! Both scrape formats walk the same bounded cells. JSON buckets are disjoint,
//! while Prometheus buckets are cumulative and end with +Inf.
use super::schema::*;
use serde_json::{Map, Value, json};
use std::{
    fmt::Write,
    sync::{Arc, atomic::Ordering::Relaxed},
};

pub(super) struct Scrape {
    pub recorder: Arc<Recorder>,
    pub owners: Box<dyn Fn() -> Owners + Send + Sync>,
}
#[derive(Default)]
pub(crate) struct Owners {
    pub gauges: Vec<(&'static str, u64)>,
    pub counters: Vec<(&'static str, u64)>,
}
fn labels(kind: Labels, i: usize) -> String {
    match kind {
        Labels::None => String::new(),
        Labels::Transport => format!("transport=\"{}\"", TRANSPORTS[i]),
        Labels::Attempt => format!(
            "transport=\"{}\",operation=\"{}\"",
            TRANSPORTS[i / 2],
            OPS[i % 2]
        ),
        Labels::Operation => format!("operation=\"{}\"", OPS[i]),
        Labels::LogicalOutcome => format!(
            "operation=\"{}\",outcome=\"{}\"",
            OPS[i / 4],
            OUTCOMES[i % 4]
        ),
        Labels::AttemptOutcome => format!(
            "{},outcome=\"{}\"",
            labels(Labels::Attempt, i / 4),
            OUTCOMES[i % 4]
        ),
        Labels::Failure => format!(
            "transport=\"{}\",reason=\"{}\"",
            TRANSPORTS[i / 4],
            REASONS[i % 4]
        ),
        Labels::Pair => format!(
            "from=\"{}\",to=\"{}\"",
            TRANSPORTS[i / TRANSPORTS.len()],
            TRANSPORTS[i % TRANSPORTS.len()]
        ),
    }
}
impl Scrape {
    pub fn prometheus(&self) -> String {
        self.try_prometheus().unwrap_or_default()
    }
    pub fn try_prometheus(&self) -> std::io::Result<String> {
        self.recorder
            .pipeline
            .serialize(super::pipeline::Format::Prometheus, |now| {
                Ok(self.prometheus_at(now))
            })
    }
    pub fn write_prometheus(&self, mut writer: impl std::io::Write) -> std::io::Result<()> {
        self.recorder
            .pipeline
            .serialize(super::pipeline::Format::Prometheus, |now| {
                writer.write_all(self.prometheus_at(now).as_bytes())
            })
    }
    fn prometheus_at(&self, now: u64) -> String {
        let owners = (self.owners)();
        let mut out = String::with_capacity(4096);
        for (i, spec) in SPECS.iter().enumerate() {
            let mut header = false;
            for (j, c) in self.recorder.cells[i].iter().enumerate() {
                let value = c.value.load(Relaxed);
                let buckets: Vec<_> = c.buckets.iter().map(|n| n.load(Relaxed)).collect();
                let count: u64 = buckets.iter().sum();
                if matches!(spec.kind, Kind::Histogram(_)) && count == 0 {
                    continue;
                }
                if !matches!(spec.kind, Kind::Histogram(_))
                    && spec.labels != Labels::None
                    && !c.seen.load(Relaxed)
                    && value == 0
                {
                    continue;
                }
                if !header {
                    let kind = match spec.kind {
                        Kind::Counter => "counter",
                        Kind::Gauge => "gauge",
                        Kind::Histogram(_) => "histogram",
                    };
                    let _ = writeln!(
                        out,
                        "# HELP {} {}\n# TYPE {} {kind}",
                        spec.name, spec.help, spec.name
                    );
                    header = true;
                }
                let label = labels(spec.labels, j);
                let name = spec.name;
                if let Kind::Histogram(bounds) = spec.kind {
                    let mut total = 0;
                    for (k, n) in buckets.iter().enumerate() {
                        total += n;
                        let le = bounds
                            .get(k)
                            .map_or_else(|| "+Inf".into(), |b| format!("{b}.000000"));
                        let _ = writeln!(out, "{name}_bucket{{{label},le=\"{le}\"}} {total}");
                    }
                    let _ = writeln!(
                        out,
                        "{name}_sum{{{label}}} {value}\n{name}_count{{{label}}} {count}"
                    );
                } else if label.is_empty() {
                    let _ = writeln!(out, "{name} {value}");
                } else {
                    let _ = writeln!(out, "{name}{{{label}}} {value}");
                }
            }
        }
        for (group, ty) in [(&owners.gauges, "gauge"), (&owners.counters, "counter")] {
            for &(name, value) in group {
                let _ = writeln!(out, "# TYPE {name} {ty}\n{name} {value}");
            }
        }
        self.recorder
            .pipeline
            .write_prometheus(&mut out, self.recorder.enabled.load(Relaxed), now);
        out
    }
    pub fn json(&self) -> Value {
        self.recorder
            .pipeline
            .serialize(super::pipeline::Format::Json, |now| Ok(self.json_at(now)))
            .unwrap_or_default()
    }
    pub fn write_json(&self, writer: impl std::io::Write) -> std::io::Result<()> {
        self.recorder
            .pipeline
            .serialize(super::pipeline::Format::Json, |now| {
                serde_json::to_writer(writer, &self.json_at(now)).map_err(std::io::Error::from)
            })
    }
    fn json_at(&self, now: u64) -> Value {
        let owners = (self.owners)();
        let mut root = self
            .recorder
            .pipeline
            .json(self.recorder.enabled.load(Relaxed), Some(now));
        for (i, spec) in SPECS.iter().enumerate() {
            let cells = &self.recorder.cells[i];
            let value = cells.iter().map(|c| c.value.load(Relaxed)).sum::<u64>();
            let v = if let Kind::Histogram(bounds) = spec.kind {
                let mut counts = Map::new();
                let mut total = 0;
                for j in 0..=bounds.len() {
                    let n: u64 = cells.iter().map(|c| c.buckets[j].load(Relaxed)).sum();
                    total += n;
                    if let Some(b) = bounds.get(j) {
                        counts.insert(b.to_string(), Value::from(n));
                    }
                }
                json!({"buckets":counts,"sum":value,"count":total})
            } else {
                Value::from(value)
            };
            root.insert(spec.name.into(), v);
        }
        for &(name, v) in owners.gauges.iter().chain(&owners.counters) {
            root.insert(name.into(), v.into());
        }
        root.into()
    }
    pub fn summary(&self) -> String {
        let total = |m: Metric| {
            self.recorder.cells[m as usize]
                .iter()
                .map(|c| c.value.load(Relaxed))
                .sum::<u64>()
        };
        let bytes = |n: u64| {
            for (scale, unit) in [
                (1_000_000_000_000, "TB"),
                (1_000_000_000, "GB"),
                (1_000_000, "MB"),
                (1_000, "KB"),
            ] {
                if n >= scale {
                    return format!("{:.2} {unit}", n as f64 / scale as f64);
                }
            }
            format!("{n}.00 B")
        };
        format!(
            "Metrics HTTP: {} (start failures: {}) | Read: {} ({} reqs, {} fails) | Write: {} ({} reqs, {} fails) | Failovers: {} | Quarantined batches: {}",
            if self.recorder.pipeline.http_up.load(Relaxed) {
                "up"
            } else {
                "down"
            },
            self.recorder.pipeline.start_failures.load(Relaxed),
            bytes(total(Metric::ReadBytes)),
            total(Metric::ReadRequests),
            total(Metric::ReadFailures),
            bytes(total(Metric::WriteBytes)),
            total(Metric::WriteRequests),
            total(Metric::WriteFailures),
            total(Metric::Failover),
            total(Metric::Quarantined)
        )
    }
}
