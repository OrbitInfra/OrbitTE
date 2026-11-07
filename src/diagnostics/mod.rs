//! Offline TENT diagnostics. The version collector reads compile-time facts only.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Write};

pub const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const BUILD_COMMIT: &str = env!("ORBITE_BUILD_COMMIT");
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Pass,
    Warn,
    Fail,
    Skip,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Check {
    pub id: String,
    pub outcome: Outcome,
    pub summary: String,
    pub remediation: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct BuildInfo {
    pub version: String,
    pub commit: String,
    pub build_type: String,
    pub compiler: String,
    pub features: BTreeMap<String, bool>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub schema_version: u32,
    pub command: String,
    pub build: BuildInfo,
    pub checks: Vec<Check>,
}

pub fn version_snapshot() -> Snapshot {
    Snapshot {
        schema_version: SCHEMA_VERSION,
        command: "version".into(),
        build: BuildInfo {
            version: BUILD_VERSION.into(),
            commit: BUILD_COMMIT.into(),
            compiler: env!("ORBITE_BUILD_COMPILER").into(),
            build_type: env!("ORBITE_BUILD_PROFILE").into(),
            features: [
                ("tent", true),
                ("ascend", false),
                ("ascend_direct", false),
                ("hip", false),
                ("sunrise", false),
                ("tpu", false),
                ("cuda", cfg!(feature = "cuda")),
                ("rdma", cfg!(feature = "rdma")),
                ("fabric", cfg!(feature = "fabric")),
                ("uring", cfg!(feature = "io-uring")),
                ("gds", cfg!(feature = "gds")),
                ("metrics", cfg!(feature = "metrics")),
                ("rdma-adaptive", cfg!(feature = "rdma-adaptive")),
            ]
            .map(|(k, v)| (k.to_owned(), v))
            .into(),
        },
        checks: vec![Check {
            id: "build.info.available".into(),
            outcome: Outcome::Pass,
            summary: "compile-time build information is available".into(),
            remediation: Vec::new(),
        }],
    }
}
impl Snapshot {
    pub fn render_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
    pub fn render_text(&self) -> String {
        let mut out = format!(
            "OrbiTE diagnostics schema: {}\nCommand: {}\nVersion: {}\nCommit: {}\nBuild type: {}\nCompiler: {}\nFeatures:",
            self.schema_version,
            self.command,
            self.build.version,
            self.build.commit,
            self.build.build_type,
            self.build.compiler
        );
        for (name, enabled) in &self.build.features {
            let _ = write!(
                out,
                " {name}={}",
                if *enabled { "enabled" } else { "disabled" }
            );
        }
        out.push('\n');
        if !self.checks.is_empty() {
            out.push_str("\nChecks:\n");
        }
        for c in &self.checks {
            let outcome = match c.outcome {
                Outcome::Pass => "pass",
                Outcome::Warn => "warn",
                Outcome::Fail => "fail",
                Outcome::Skip => "skip",
            };
            let _ = writeln!(out, "  [{outcome}] {}: {}", c.id, c.summary);
            for r in &c.remediation {
                let _ = writeln!(out, "    remediation: {r}");
            }
        }
        out
    }
    pub fn exit_code(&self, warnings_are_errors: bool) -> u8 {
        self.checks
            .iter()
            .map(|c| match (c.outcome, c.id.as_str()) {
                (Outcome::Fail, "tool.internal") => 4,
                (Outcome::Fail, "engine.process" | "tool.probe_missing") => 3,
                (Outcome::Fail, _) => 1,
                (Outcome::Warn, _) if warnings_are_errors => 1,
                _ => 0,
            })
            .max()
            .unwrap_or(0)
    }
}
#[cfg(test)]
#[path = "../../tests/unit/diagnostics/mod.rs"]
mod tests;
