use super::*;
use crate::runtime::qos_contract::QosContractResolver;
use crate::{QosRequestContext, SourceEpoch};

#[test]
fn tent_json_preserves_enablement_units_and_compiled_overlays() {
    for input in [
        "{}",
        r#"{"other_engine_setting":123}"#,
        r#"{"qos":null}"#,
        r#"{"qos":{}}"#,
        r#"{"qos":{"version":1}}"#,
    ] {
        assert!(QosConfig::from_json(input).unwrap().is_none(), "{input}");
    }
    for input in [r#"{"qos":{"defaults":{}}}"#, r#"{"qos":{"tenants":[]}}"#] {
        assert!(QosConfig::from_json(input).unwrap().is_some());
    }
    for (input, expected) in [
        ("0", 0),
        (r#""0""#, 0),
        (r#""2 b""#, 2),
        (r#"" 1 KB ""#, 1024),
        (r#""2MiB""#, 2 << 20),
        (r#""1gb""#, 1 << 30),
    ] {
        let json = format!(r#"{{"qos":{{"defaults":{{"max_inflight_bytes":{input}}}}}}}"#);
        assert_eq!(
            QosConfig::from_json(&json)
                .unwrap()
                .unwrap()
                .defaults
                .max_inflight_bytes,
            Some(expected)
        );
    }
    for (input, priority) in [
        ("0", Priority::High),
        (r#"" high ""#, Priority::High),
        (r#""1""#, Priority::Medium),
        (r#""MeDIuM""#, Priority::Medium),
        ("2", Priority::Low),
        (r#""low""#, Priority::Low),
    ] {
        let json = format!(r#"{{"qos":{{"defaults":{{"priority":{input}}}}}}}"#);
        assert_eq!(
            QosConfig::from_json(&json)
                .unwrap()
                .unwrap()
                .defaults
                .priority,
            Some(priority)
        );
    }
    for (value, delay, reject) in [
        ("[]", false, false),
        (r#"[" DELAY "]"#, true, false),
        (r#"["Reject"]"#, false, true),
        (r#"["reject","delay","delay"]"#, true, true),
    ] {
        let input = format!(r#"{{"qos":{{"defaults":{{"allowed_degraded_actions":{value}}}}}}}"#);
        let fields = QosConfig::from_json(&input).unwrap().unwrap().defaults;
        assert_eq!(
            fields.allowed_degraded_actions,
            Some(QosDegradedActions { delay, reject })
        );
    }
    // Actions replace the inherited field, including an explicit empty list.
    let actions = QosConfig::from_json(
        r#"{"qos":{
        "defaults":{"allowed_degraded_actions":["delay","reject"]},
        "tenants":[{"name":"a","defaults":{"allowed_degraded_actions":["reject"]},
            "intents":{"empty":{"allowed_degraded_actions":[]}}}]
    }}"#,
    )
    .unwrap()
    .unwrap();
    let queue = crate::runtime::admission_queue::LocalTransferAdmissionQueue::new(
        crate::RuntimeQueueConfig::default(),
    );
    let resolver = QosContractResolver::new(&actions, Some(&queue)).unwrap();
    for (tenant, intent, expected) in [
        (
            "missing",
            "unspec",
            QosDegradedActions {
                delay: true,
                reject: true,
            },
        ),
        (
            "a",
            "unspec",
            QosDegradedActions {
                delay: false,
                reject: true,
            },
        ),
        ("a", "empty", QosDegradedActions::default()),
    ] {
        let policy = QosContractResolver::resolve(
            Some(&resolver),
            SourceEpoch([1; 16]),
            &QosRequestContext {
                tenant_id: tenant.into(),
                intent: intent.into(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(policy.fields().allowed_degraded_actions, Some(expected));
    }
    let input = r#"{
        "transports": ["rdma", "tcp"],
        "qos": {
            "version":1,
            "defaults":{"priority":"medium", "max_inflight_requests":128},
            "tenants":[{"name":" Tenant-A ", "defaults":{"max_inflight_bytes":"64MiB"},
                "intents":{"Foreground_Get":{"priority":"high", "max_inflight_bytes":"128MiB"},
                           "blocked":{"max_inflight_requests":0}}}]
        }
    }"#;
    let path = std::env::temp_dir().join(format!(
        "orbite-qos-{:?}.json",
        SourceEpoch::generate().unwrap()
    ));
    std::fs::write(&path, input).unwrap();
    let config = QosConfig::from_json_file(&path).unwrap().unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(
        QosConfig::from_json_file(&path),
        Err(Error::Io(_))
    ));
    let resolver = QosContractResolver::new(&config, None).unwrap();
    for (tenant, intent, priority, bytes, requests) in [
        (
            "tenant-a",
            "foreground_get",
            Priority::High,
            Some(128 << 20),
            Some(128),
        ),
        (
            "tenant-a",
            "checkpoint",
            Priority::Medium,
            Some(64 << 20),
            Some(128),
        ),
        (
            "missing",
            "foreground_get",
            Priority::Medium,
            None,
            Some(128),
        ),
        (
            "tenant-a",
            "blocked",
            Priority::Medium,
            Some(64 << 20),
            Some(0),
        ),
    ] {
        let policy = QosContractResolver::resolve(
            Some(&resolver),
            SourceEpoch([1; 16]),
            &QosRequestContext {
                tenant_id: tenant.into(),
                intent: intent.into(),
                requested_priority: Priority::Low,
            },
        )
        .unwrap();
        assert_eq!(
            (
                policy.effective_priority(),
                policy.fields().max_inflight_bytes,
                policy.fields().max_inflight_requests
            ),
            (priority, bytes, requests)
        );
    }
}

#[test]
fn json_rejects_ambiguous_or_unconsumed_qos_and_bounded_input() {
    for input in [
        "[]",
        "null",
        r#"{"qos":[]}"#,
        r#"{"qos":1}"#,
        r#"{"qos":{"version":"1"}}"#,
        r#"{"qos":{"version":2}}"#,
        r#"{"qos":{"defaults":[]}}"#,
        r#"{"qos":{"defaults":null}}"#,
        r#"{"qos":{"tenants":null}}"#,
        r#"{"qos":{"tenants":[[]]}}"#,
        r#"{"qos":{"tenants":[{}]}}"#,
        r#"{"qos":{"tenants":[{"name":" "}]}}"#,
        r#"{"qos":{"tenants":[{"name":"a","defaults":[]}]}}"#,
        r#"{"qos":{"tenants":[{"name":"a","intents":[]}]}}"#,
        r#"{"qos":{"tenants":[{"name":"a","intents":{"x":[]}}]}}"#,
        r#"{"qos":{"tenants":[{"name":"a"},{"name":" A "}]}}"#,
        r#"{"qos":{"tenants":[{"name":"a","intents":{"x":{}," X ":{}}}]}}"#,
        r#"{"qos":{"tenants":[{"name":"a","intents":{"x":{},"x":{}}}]}}"#,
        r#"{"qos":{"defaults":{"priority":0,"priority":2}}}"#,
        r#"{"qos":null,"qos":{"defaults":{}}}"#,
        r#"{"qos":{"strict_mode":true}}"#,
        r#"{"qos":{"defaults":{"allowed_degraded_actions":null}}}"#,
        r#"{"qos":{"defaults":{"allowed_degraded_actions":"delay"}}}"#,
        r#"{"qos":{"defaults":{"allowed_degraded_actions":[1]}}}"#,
        r#"{"qos":{"defaults":{"allowed_degraded_actions":["fallback_transport"]}}}"#,
        r#"{"qos":{"defaults":{"allowed_degraded_actions":["local_recompute"]}}}"#,
        r#"{"qos":{"defaults":{"allowed_degraded_actions":["compress"]}}}"#,
        r#"{"qos":{"defaults":{"min_bandwidth_gbps":20}}}"#,
        r#"{"qos":{"defaults":{"max_inflight_requests":"128"}}}"#,
        r#"{"qos":{"defaults":{"max_inflight_requests":-1}}}"#,
        r#"{"qos":{"defaults":{"max_inflight_requests":1.0}}}"#,
        r#"{"qos":{"defaults":{"max_inflight_bytes":"1.5MiB"}}}"#,
        r#"{"qos":{"defaults":{"max_inflight_bytes":"18446744073709551615TiB"}}}"#,
        r#"{"qos":{"defaults":{"max_inflight_bytes":18446744073709551616}}}"#,
        r#"{"qos":{"defaults":{"max_inflight_bytes":null}}}"#,
        r#"{"qos":{"defaults":{"priority":3}}}"#,
        r#"{"qos":{"defaults":{"priority":null}}}"#,
        r#"{"qos":{"defaults":{"priority":true}}}"#,
        r#"{"qos":{"defaults":{"priority":1.0}}}"#,
        r#"{"qos":{"defaults":{"priority":"urgent"}}}"#,
        r#"{} {}"#,
    ] {
        assert!(QosConfig::from_json(input).is_err(), "accepted: {input}");
    }
    let too_many = (0..4096)
        .map(|i| format!(r#"{{"name":"{i}"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    assert!(QosConfig::from_json(&format!(r#"{{"qos":{{"tenants":[{too_many}]}}}}"#)).is_err());
    let path = std::env::temp_dir().join(format!(
        "orbite-qos-large-{:?}.json",
        SourceEpoch::generate().unwrap()
    ));
    let mut oversized = " ".repeat(MAX_JSON_BYTES);
    oversized.push_str("{}");
    std::fs::write(&path, &oversized).unwrap();
    let file_result = QosConfig::from_json_file(&path);
    std::fs::remove_file(&path).unwrap();
    assert!(matches!(file_result, Err(Error::InvalidQosConfig(_))));
    assert!(matches!(
        QosConfig::from_json(&oversized),
        Err(Error::InvalidQosConfig(_))
    ));
    let error =
        QosConfig::from_json(r#"{"qos":{"defaults":{"allowed_degraded_actions":["unknown"]}}}"#)
            .unwrap_err();
    assert!(error.to_string().contains("allowed_degraded_actions"));
}

#[test]
fn unusable_fields_and_actions_name_the_offending_key_at_every_policy_layer() {
    fn rejected(json: &str, needle: &str) {
        assert!(
            matches!(QosConfig::from_json(json),Err(Error::InvalidQosConfig(ref reason)) if reason.contains(needle)),
            "expected {needle:?} in rejection of {json}: {:?}",
            QosConfig::from_json(json)
        );
    }
    for (field, value) in [
        ("min_bandwidth_gbps", "20"),
        ("max_bandwidth_gbps", "100"),
        ("burst_bytes", "1024"),
        ("weight", "8"),
        ("deadline_profile", "true"),
        ("max_inflight_requests", "-1"),
        ("priority", "9"),
        ("max_inflight_bytes", "false"),
        ("allowed_degraded_actions", "123"),
        ("name", "true"),
    ] {
        let bad = format!(r#"{{"{field}":{value}}}"#);
        for config in [
            format!(r#"{{"defaults":{bad}}}"#),
            format!(r#"{{"tenants":[{{"name":"t","defaults":{bad}}}]}}"#),
            format!(r#"{{"tenants":[{{"name":"t","intents":{{"foreground_get":{bad}}}}}]}}"#),
        ] {
            rejected(&format!(r#"{{"qos":{config}}}"#), field);
        }
    }
    for action in [
        "fallback_transport",
        "local_recompute",
        "compress",
        "unrecognized",
    ] {
        let bad = format!(r#"{{"allowed_degraded_actions":["delay","{action}"]}}"#);
        for config in [
            format!(r#"{{"defaults":{bad}}}"#),
            format!(r#"{{"tenants":[{{"name":"t","defaults":{bad}}}]}}"#),
            format!(r#"{{"tenants":[{{"name":"t","intents":{{"staging_internal":{bad}}}}}]}}"#),
        ] {
            let json = format!(r#"{{"qos":{config}}}"#);
            for needle in ["allowed_degraded_actions", action] {
                rejected(&json, needle);
            }
        }
    }
    rejected(
        r#"{"qos":{"strict_mode":true,"defaults":{}}}"#,
        "strict_mode",
    );
    rejected(r#"{"qos":{"intent_defaults":{}}}"#, "intent_defaults");
}
