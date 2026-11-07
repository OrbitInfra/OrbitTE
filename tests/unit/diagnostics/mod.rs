use super::*;
use serde_json::Value;

fn check(id: &str, outcome: Outcome) -> Check {
    Check {
        id: id.into(),
        outcome,
        summary: "check detail".into(),
        remediation: vec!["inspect it".into()],
    }
}
#[test]
fn version_schema_renderers_and_exit_precedence() {
    let mut s = version_snapshot();
    assert_eq!(s.checks.len(), 1);
    assert_eq!(s.exit_code(true), 0);
    let build = &s.build;
    assert!(!build.compiler.is_empty() && !build.commit.is_empty());
    assert_eq!(build.features["cuda"], cfg!(feature = "cuda"));
    assert_eq!(build.features["rdma"], cfg!(feature = "rdma"));
    assert!(build.features["tent"]);
    let text = s.render_text();
    let doc: Value = serde_json::from_str(&s.render_json().unwrap()).unwrap();
    assert_eq!(doc["schema_version"], 1);
    assert_eq!(doc["command"], "version");
    assert_eq!(doc["checks"][0]["outcome"], "pass");
    assert!(text.contains("OrbiTE diagnostics schema:"));
    assert!(text.contains("[pass] build.info.available:"));
    assert_eq!(doc["build"]["features"].get("device-plugin"), None);
    for (ids, warnings, expected) in [
        (vec![("warn", Outcome::Warn)], false, 0),
        (vec![("warn", Outcome::Warn)], true, 1),
        (
            vec![("tool.internal", Outcome::Pass), ("bad", Outcome::Fail)],
            false,
            1,
        ),
        (
            vec![("engine.process", Outcome::Fail), ("bad", Outcome::Fail)],
            false,
            3,
        ),
        (
            vec![
                ("warn", Outcome::Warn),
                ("tool.probe_missing", Outcome::Fail),
            ],
            true,
            3,
        ),
        (
            vec![
                ("tool.internal", Outcome::Fail),
                ("engine.process", Outcome::Fail),
            ],
            true,
            4,
        ),
        (
            vec![
                ("engine.process", Outcome::Fail),
                ("tool.internal", Outcome::Fail),
            ],
            true,
            4,
        ),
    ] {
        s.checks = ids.into_iter().map(|(id, o)| check(id, o)).collect();
        assert_eq!(s.exit_code(warnings), expected);
    }
}
