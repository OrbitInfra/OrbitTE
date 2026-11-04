use super::*;
use serde_json::json;

fn check(id: &str, outcome: Outcome) -> Check {
    Check {
        id: id.into(),
        outcome,
        summary: "check detail".into(),
        evidence: vec![],
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
    s.sections.push(Section {
        id: "topology".into(),
        summary: "partial".into(),
        evidence: vec![Evidence {
            source: "os".into(),
            field: "nic.speed".into(),
            value: SanitizedValue::unavailable(),
        }],
    });
    let mut redact = Redactor::new(RedactionMode::Default);
    for (id, outcome) in [
        ("good", Outcome::Pass),
        ("maybe", Outcome::Warn),
        ("bad", Outcome::Fail),
        ("absent", Outcome::Skip),
    ] {
        let mut c = check(id, outcome);
        c.evidence.push(Evidence {
            source: "device".into(),
            field: "nic".into(),
            value: redact.sanitize("nic", json!(id), Sensitivity::Public),
        });
        s.checks.push(c);
    }
    let text = s.render_text();
    let doc: Value = serde_json::from_str(&s.render_json().unwrap()).unwrap();
    assert_eq!(doc["schema_version"], 1);
    assert_eq!(doc["command"], "version");
    let e = &doc["sections"][0]["evidence"][0];
    assert_eq!(e["available"], false);
    assert!(e.get("observed").is_none());
    assert!(text.contains("nic.speed = unavailable [os]"));
    for (i, (id, label)) in [
        ("good", "pass"),
        ("maybe", "warn"),
        ("bad", "fail"),
        ("absent", "skip"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(doc["checks"][i + 1]["outcome"], label);
        assert_eq!(doc["checks"][i + 1]["remediation"][0], "inspect it");
        assert!(text.contains(&format!("[{label}] {id}: check detail")));
        assert_eq!(
            text.contains(&format!("nic = {id} [device]")),
            label != "pass"
        );
    }
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

#[test]
fn secret_fields_and_text_are_redacted_in_every_mode() {
    for mode in [
        RedactionMode::Default,
        RedactionMode::Strict,
        RedactionMode::None,
    ] {
        let mut r = Redactor::new(mode);
        for (f, s) in [
            ("auth_token", Sensitivity::Public),
            ("buffer", Sensitivity::RawAddress),
            ("rdma_rkey", Sensitivity::Public),
        ] {
            assert_eq!(
                r.sanitize(f, json!("sensitive"), s).observed(),
                Some(&json!("[redacted]"))
            );
        }
        let text=r.sanitize_text(r#"MC_REDIS_PASSWORD=abc url redis://u:p@h:6379/0 Authorization: Bearer x rkey=12345 lkey 0x99 addr=0x7f0069fa6000 {"redis_password": "hunter2"} peer 10.1.2.3"#);
        for secret in [
            "=abc",
            "u:p@",
            "Bearer x",
            "12345",
            "0x99",
            "0x7f0069fa6000",
            "hunter2",
        ] {
            assert!(!text.contains(secret), "{text}");
        }
        assert!(text.contains("redis://[redacted]@h:6379/0"));
        assert!(text.contains(r#""redis_password": "[redacted]""#));
        assert_eq!(text.contains("10.1.2.3"), mode != RedactionMode::Strict);
        assert_eq!(
            r.metadata().fields_redacted,
            if mode == RedactionMode::Strict {
                11
            } else {
                10
            }
        );
    }
}
#[test]
fn strict_pseudonyms_match_fields_whole_words_and_address_shapes() {
    let mut r = Redactor::new(RedactionMode::Strict);
    for (i, s) in ["node-a", "eth1", "node-b", "192.0.2.10"]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            r.sanitize("identity", json!(s), Sensitivity::Deployment)
                .observed(),
            Some(&json!(format!("entity-{}", i + 1)))
        );
    }
    assert_eq!(
        r.sanitize_text("node-a eth1 eth10 node-a1 xnode-a (eth1)"),
        "entity-1 entity-2 eth10 node-a1 xnode-a (entity-2)"
    );
    assert_eq!(
        r.sanitize_text("peer 192.0.2.10 gid fe80::1 again 192.0.2.10 other 192.0.2.11"),
        "peer entity-4 gid entity-5 again entity-4 other entity-6"
    );
    let count = r.metadata().fields_redacted;
    let text=r.sanitize_text("addr:10.0.0.1 seg 198.51.100.7:15321 gid:fe80::1a2b:3cff:fe4d:5e6f raw 00:00:00:00:00:00:00:00:00:00:ff:ff:c6:33:64:07 mac 0a:1b:2c:3d:4e:5f guid 0a1b:2c3d:4e5f:6a7b mapped [::ffff:203.0.113.9] end 10.9.8.7.");
    assert_eq!(r.metadata().fields_redacted - count, 8);
    assert!(text.contains(":15321 ") && text.ends_with('.'));
    for addr in [
        "10.0.0.1",
        "198.51.100.7",
        "fe80::",
        "ff:ff",
        "0a:1b",
        "0a1b:",
        "203.0.113.9",
        "10.9.8.7",
    ] {
        assert!(!text.contains(addr), "{text}");
    }
    let safe = "std::string I1011 05:17:20.123456 file.cpp:123] mlx5_2:4096 version 1.2.3 cafe deadbeef :: a::b GID[3] port 1: mtu 4096";
    assert_eq!(r.sanitize_text(safe), safe);
    for mode in [RedactionMode::Default, RedactionMode::None] {
        let mut r = Redactor::new(mode);
        r.sanitize("host", json!("node-a"), Sensitivity::Deployment);
        assert_eq!(
            r.sanitize_text("node-a 198.51.100.7 fe80::1"),
            "node-a 198.51.100.7 fe80::1"
        );
        assert_eq!(r.metadata().fields_redacted, 0);
    }
}
