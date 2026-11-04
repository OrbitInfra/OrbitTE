//! Exporter health is independent of transfer sampling.
use crate::diagnostics::{BUILD_COMMIT, BUILD_VERSION};
use serde_json::{Value, json};
use std::{
    fmt::Write,
    io,
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Copy)]
pub(super) enum Format {
    Prometheus,
    Json,
}
const FORMATS: [&str; 2] = ["prometheus", "json"];

#[derive(Default)]
pub(crate) struct Pipeline {
    pub(super) http_up: AtomicBool,
    pub(super) start_failures: AtomicU64,
    attempts: [AtomicU64; 2],
    failures: [AtomicU64; 2],
    success: [AtomicU64; 2],
    #[cfg(test)]
    pub(super) fail_next: AtomicBool,
}
impl Pipeline {
    pub(super) fn serialize<T>(
        &self,
        format: Format,
        render: impl FnOnce(u64) -> io::Result<T>,
    ) -> io::Result<T> {
        let i = format as usize;
        self.attempts[i].fetch_add(1, Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        #[cfg(test)]
        let render = |now| {
            if self.fail_next.swap(false, Relaxed) {
                Err(io::Error::other("injected serialization failure"))
            } else {
                render(now)
            }
        };
        match render(now) {
            Ok(data) => {
                self.success[i].fetch_max(now, Relaxed);
                Ok(data)
            }
            Err(e) => {
                self.failures[i].fetch_add(1, Relaxed);
                log::error!("{} metrics serialization failed: {e}", FORMATS[i]);
                Err(e)
            }
        }
    }

    fn families(
        &self,
        format: Option<Format>,
        now: u64,
    ) -> [(&'static str, &'static str, [u64; 2]); 3] {
        let mut success = self.success.each_ref().map(|n| n.load(Relaxed));
        if let Some(f) = format {
            success[f as usize] = now.max(success[f as usize]);
        }
        [
            (
                "tent_metrics_serialization_attempts_total",
                "counter",
                self.attempts.each_ref().map(|n| n.load(Relaxed)),
            ),
            (
                "tent_metrics_serialization_failures_total",
                "counter",
                self.failures.each_ref().map(|n| n.load(Relaxed)),
            ),
            (
                "tent_metrics_last_success_timestamp_seconds",
                "gauge",
                success,
            ),
        ]
    }
    pub(super) fn write_prometheus(&self, out: &mut String, enabled: bool, now: u64) {
        let _ = writeln!(
            out,
            "# TYPE tent_metrics_build_info gauge\ntent_metrics_build_info{{version=\"{BUILD_VERSION}\",commit=\"{BUILD_COMMIT}\"}} 1"
        );
        for (name, kind, value) in [
            (
                "tent_metrics_http_server_up",
                "gauge",
                u64::from(self.http_up.load(Relaxed)),
            ),
            ("tent_metrics_runtime_enabled", "gauge", u64::from(enabled)),
            (
                "tent_metrics_http_start_failures_total",
                "counter",
                self.start_failures.load(Relaxed),
            ),
        ] {
            let _ = writeln!(out, "# TYPE {name} {kind}\n{name} {value}");
        }
        for (name, kind, values) in self.families(Some(Format::Prometheus), now) {
            let _ = writeln!(out, "# TYPE {name} {kind}");
            for (label, n) in FORMATS.iter().zip(values) {
                let _ = writeln!(out, "{name}{{format=\"{label}\"}} {n}");
            }
        }
    }
    pub(super) fn json(&self, enabled: bool, now: Option<u64>) -> serde_json::Map<String, Value> {
        let mut fields = serde_json::Map::from_iter([
            (
                "tent_metrics_build_info".into(),
                json!({"version":BUILD_VERSION,"commit":BUILD_COMMIT}),
            ),
            (
                "tent_metrics_http_server_up".into(),
                u64::from(self.http_up.load(Relaxed)).into(),
            ),
            (
                "tent_metrics_runtime_enabled".into(),
                u64::from(enabled).into(),
            ),
            (
                "tent_metrics_http_start_failures_total".into(),
                self.start_failures.load(Relaxed).into(),
            ),
        ]);
        for (name, _, values) in self.families(now.map(|_| Format::Json), now.unwrap_or(0)) {
            fields.insert(
                name.into(),
                json!({"prometheus":values[0],"json":values[1]}),
            );
        }
        fields
    }
}
