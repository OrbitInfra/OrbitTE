//! Per-engine TENT metrics and HTTP exposition.
mod config;
mod expose;
mod http;
mod pipeline;
mod record;
mod schema;
use crate::Result;
pub use config::MetricsConfig;
pub(crate) use expose::Owners;
use expose::Scrape;
pub(crate) use record::Observation;
pub(crate) use schema::Recorder;
use std::{
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Instant,
};

/// Engine-owned metrics. Dropping the engine stops and joins the service.
pub struct Metrics {
    _workers: http::Workers,
    scrape: Arc<Scrape>,
    port: u16,
}
impl Metrics {
    pub(crate) fn start(
        cfg: &MetricsConfig,
        owners: impl Fn() -> Owners + Send + Sync + 'static,
    ) -> Result<Self> {
        let scrape = Arc::new(Scrape {
            recorder: Arc::new(Recorder::new()),
            owners: Box::new(owners),
        });
        let (mut workers, port) = match http::Workers::http(cfg, &scrape) {
            Ok(w) => {
                scrape.recorder.pipeline.http_up.store(true, Relaxed);
                (w, cfg.http_port)
            }
            Err(e) => {
                scrape
                    .recorder
                    .pipeline
                    .start_failures
                    .fetch_add(1, Relaxed);
                log::warn!(
                    "metrics HTTP endpoint unavailable on {}:{} ({e}), continuing with log-only metrics",
                    cfg.http_host,
                    cfg.http_port
                );
                (Default::default(), 0)
            }
        };
        workers.report(cfg.report_interval_seconds, &scrape)?;
        Ok(Self {
            _workers: workers,
            scrape,
            port,
        })
    }
    pub(crate) fn recorder(&self) -> &Arc<Recorder> {
        &self.scrape.recorder
    }
    pub(crate) fn timestamp(&self) -> Option<Instant> {
        self.enabled().then(Instant::now)
    }
    /// Zero means the configured port could not be served.
    pub fn http_port(&self) -> u16 {
        self.port
    }
    pub fn enabled(&self) -> bool {
        self.recorder().enabled.load(Relaxed)
    }
    /// Pauses sampling. Gauges continue to describe the actual owners.
    pub fn set_enabled(&self, enabled: bool) {
        self.recorder().enabled.store(enabled, Relaxed);
    }
    /// Export to a fallible sink. A write failure is recorded against this format.
    pub fn write_prometheus(&self, writer: impl std::io::Write) -> std::io::Result<()> {
        self.scrape.write_prometheus(writer)
    }
    pub fn write_json(&self, writer: impl std::io::Write) -> std::io::Result<()> {
        self.scrape.write_json(writer)
    }
    pub fn prometheus(&self) -> String {
        self.scrape.prometheus()
    }
    pub fn json(&self) -> serde_json::Value {
        self.scrape.json()
    }
}
pub(crate) fn pipeline_status(metrics: Option<&Metrics>) -> serde_json::Value {
    match metrics {
        Some(m) => m.recorder().pipeline.json(m.enabled(), None),
        None => pipeline::Pipeline::default().json(false, None),
    }
    .into()
}
pub(crate) fn transport_bit(t: crate::Transport) -> u16 {
    1 << schema::slot(t)
}

impl Recorder {
    pub(crate) fn registration(&self, m: &crate::RegisteredMemory, add: bool) {
        let mask = m.0.metric_transports.load(Relaxed);
        for t in 0..schema::TRANSPORTS.len() {
            if mask & (1 << t) == 0 {
                continue;
            }
            if add {
                self.add(schema::Metric::Registered, t, m.len() as u64);
            } else {
                self.cells[schema::Metric::Registered as usize][t]
                    .value
                    .fetch_sub(m.len() as u64, Relaxed);
            }
        }
    }
    pub(crate) fn quarantine(&self) {
        if self.enabled.load(Relaxed) {
            self.add(schema::Metric::Quarantined, 0, 1);
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/metrics/mod.rs"]
mod tests;
