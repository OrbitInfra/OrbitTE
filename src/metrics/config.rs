//! TENT metrics configuration. The engine reads these inputs once, before binding.
use crate::{Error, Result};
use serde_json::Value;
use std::{fs::File, io::Read, path::Path};

/// TENT metrics settings. Compile the `metrics` feature to opt in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub http_host: String,
    pub http_port: u16,
    pub http_server_threads: u16,
    /// Zero disables periodic log summaries.
    pub report_interval_seconds: u32,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            http_host: "0.0.0.0".into(),
            http_port: 9100,
            http_server_threads: 2,
            report_interval_seconds: 30,
        }
    }
}

const FIELDS: [(&str, &str); 5] = [
    ("enabled", "TENT_METRICS_ENABLED"),
    ("http_host", "TENT_METRICS_HTTP_HOST"),
    ("http_port", "TENT_METRICS_HTTP_PORT"),
    ("http_server_threads", "TENT_METRICS_HTTP_SERVER_THREADS"),
    ("report_interval_seconds", "TENT_METRICS_REPORT_INTERVAL"),
];
const MAX_FILE: u64 = 1024 * 1024;

impl MetricsConfig {
    pub(crate) fn load(&self, path: Option<&Path>) -> Result<Self> {
        let file = path
            .map(|p| -> Result<Value> {
                let mut bytes = Vec::new();
                File::open(p)?.take(MAX_FILE + 1).read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_FILE {
                    return Err(Error::InvalidConfig("metrics configuration exceeds 1 MiB"));
                }
                serde_json::from_slice(&bytes)
                    .map_err(|_| Error::InvalidConfig("invalid metrics configuration JSON"))
            })
            .transpose()?;
        self.resolve(file.as_ref(), |key| {
            std::env::var_os(key)
                .map(|v| {
                    v.into_string()
                        .map_err(|_| Error::InvalidConfig("metrics environment is not UTF-8"))
                })
                .transpose()
        })
    }

    pub(super) fn resolve(
        &self,
        file: Option<&Value>,
        env: impl Fn(&str) -> Result<Option<String>>,
    ) -> Result<Self> {
        let mut cfg = self.clone();
        if file.is_some_and(|v| !v.is_object())
            || file
                .and_then(|v| v.get("metrics"))
                .is_some_and(|v| !v.is_object())
        {
            return Err(Error::InvalidConfig(
                "expected a metrics configuration object",
            ));
        }
        for (key, variable) in FIELDS {
            let value = file.and_then(|v| {
                v.get("metrics")
                    .and_then(|v| v.get(key))
                    .or_else(|| v.get(format!("metrics/{key}")))
            });
            let text = if let Some(v) = value {
                match (key, v) {
                    ("enabled", Value::Bool(b)) => Some(b.to_string()),
                    ("http_host", Value::String(s)) => Some(s.clone()),
                    (
                        "http_port" | "http_server_threads" | "report_interval_seconds",
                        Value::Number(n),
                    ) if n.is_u64() => Some(n.to_string()),
                    _ => return Err(Error::InvalidConfig("invalid metrics file field type")),
                }
            } else {
                env(variable)?
            };
            if let Some(value) = text {
                match key {
                    "enabled" => {
                        cfg.enabled = match value.to_ascii_lowercase().as_str() {
                            "1" | "true" | "yes" => true,
                            "0" | "false" | "no" => false,
                            _ => return Err(Error::InvalidConfig("invalid metrics/enabled")),
                        }
                    }
                    "http_host" => cfg.http_host = value,
                    "http_port" => {
                        cfg.http_port = value
                            .parse()
                            .map_err(|_| Error::InvalidConfig("invalid metrics/http_port"))?
                    }
                    "http_server_threads" => {
                        cfg.http_server_threads = value.parse().map_err(|_| {
                            Error::InvalidConfig("invalid metrics/http_server_threads")
                        })?
                    }
                    "report_interval_seconds" => {
                        cfg.report_interval_seconds = value.parse().map_err(|_| {
                            Error::InvalidConfig("invalid metrics/report_interval_seconds")
                        })?
                    }
                    _ => return Err(Error::Internal("unknown metrics key")),
                }
            }
        }
        if cfg.http_port == 0 {
            return Err(Error::InvalidConfig("invalid metrics/http_port"));
        }
        if !(1..=64).contains(&cfg.http_server_threads) {
            return Err(Error::InvalidConfig(
                "metrics/http_server_threads outside 1..=64",
            ));
        }
        if cfg.http_host.is_empty()
            || cfg.http_host.len() > 253
            || !cfg
                .http_host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".:-".contains(&b))
        {
            return Err(Error::InvalidConfig("invalid metrics/http_host"));
        }
        Ok(cfg)
    }
}
