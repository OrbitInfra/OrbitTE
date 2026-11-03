use super::*;
fn resolve(
    base: &MetricsConfig,
    file: Option<Value>,
    env: &[(&str, &str)],
) -> crate::Result<MetricsConfig> {
    base.resolve(file.as_ref(), |key| {
        Ok(env
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| (*v).into()))
    })
}
#[test]
fn defaults_file_environment_and_typed_overrides_are_fieldwise() {
    let d = MetricsConfig::default();
    assert_eq!(
        (
            d.enabled,
            d.http_host.as_str(),
            d.http_port,
            d.http_server_threads,
            d.report_interval_seconds
        ),
        (true, "0.0.0.0", 9100, 2, 30)
    );
    assert_eq!(resolve(&d, None, &[]).unwrap(), d);
    assert_eq!(resolve(&d, Some(json!({"metrics":{}})), &[]).unwrap(), d);
    let keys = [
        ("TENT_METRICS_ENABLED", "NO"),
        ("TENT_METRICS_HTTP_PORT", "6543"),
        ("TENT_METRICS_HTTP_HOST", "localhost"),
        ("TENT_METRICS_HTTP_SERVER_THREADS", "4"),
        ("TENT_METRICS_REPORT_INTERVAL", "0"),
    ];
    let env = resolve(&d, None, &keys).unwrap();
    assert_eq!(
        env,
        MetricsConfig {
            enabled: false,
            http_port: 6543,
            http_host: "localhost".into(),
            http_server_threads: 4,
            report_interval_seconds: 0
        }
    );
    let file = json!({"metrics":{"enabled":true,"http_port":6000,"http_host":"::1","http_server_threads":8,"report_interval_seconds":10}});
    let f = MetricsConfig {
        enabled: true,
        http_port: 6000,
        http_host: "::1".into(),
        http_server_threads: 8,
        report_interval_seconds: 10,
    };
    assert_eq!(resolve(&d, Some(file.clone()), &[]).unwrap(), f);
    assert_eq!(resolve(&d, Some(file), &keys).unwrap(), f);
    let typed = MetricsConfig {
        http_port: 7001,
        report_interval_seconds: 61,
        ..Default::default()
    };
    let cfg=resolve(&typed,Some(json!({"metrics/http_port":7002,"metrics":{"latency_buckets":[1,2],"size_buckets":"42"}})),&[("TENT_METRICS_HTTP_PORT","7003"),("TENT_METRICS_HTTP_SERVER_THREADS","3")]).unwrap();
    assert_eq!(
        (
            cfg.http_port,
            cfg.http_server_threads,
            cfg.report_interval_seconds
        ),
        (7002, 3, 61)
    );
    assert_eq!(
        resolve(&typed, None, &[("TENT_METRICS_HTTP_PORT", "7003")])
            .unwrap()
            .http_port,
        7003
    );
}
#[test]
fn invalid_settings_are_typed_and_stale_bucket_keys_do_not_change_the_schema() {
    let d = MetricsConfig::default();
    for v in ["true", "TRUE", "True", "1", "yes", "YES"] {
        assert!(
            resolve(&d, None, &[("TENT_METRICS_ENABLED", v)])
                .unwrap()
                .enabled
        );
    }
    for v in ["false", "FALSE", "False", "0", "no", "NO"] {
        assert!(
            !resolve(&d, None, &[("TENT_METRICS_ENABLED", v)])
                .unwrap()
                .enabled
        );
    }
    for (k, vs) in [
        (
            "TENT_METRICS_HTTP_PORT",
            &["0", "65536", "8080x", "", "-10"][..],
        ),
        (
            "TENT_METRICS_HTTP_SERVER_THREADS",
            &["0", "65", "-1", "threads"][..],
        ),
        (
            "TENT_METRICS_REPORT_INTERVAL",
            &["42x", "-10", "4294967296"][..],
        ),
        ("TENT_METRICS_ENABLED", &["maybe"][..]),
        (
            "TENT_METRICS_HTTP_HOST",
            &[
                "",
                "host/path",
                ":",
                "bad::host",
                "-invalid",
                "two..labels",
                "999.999.0.1",
            ][..],
        ),
    ] {
        for v in vs {
            assert!(
                matches!(resolve(&d, None, &[(k, v)]), Err(Error::InvalidConfig(_))),
                "{k} {v}"
            );
        }
    }
    for host in ["0.0.0.0", "::1", "localhost", "rank-1.example."] {
        assert_eq!(
            resolve(&d, None, &[("TENT_METRICS_HTTP_HOST", host)])
                .unwrap()
                .http_host,
            host
        );
    }
    assert!(resolve(&d, None, &[("TENT_METRICS_HTTP_HOST", &"a".repeat(64))]).is_err());
    for v in ["1", "8080", "65535"] {
        assert_eq!(
            resolve(&d, None, &[("TENT_METRICS_HTTP_PORT", v)])
                .unwrap()
                .http_port,
            v.parse::<u16>().unwrap()
        );
    }
    for file in [
        json!({"metrics":{"http_port":0}}),
        json!({"metrics":{"http_server_threads":-1}}),
        json!({"metrics":{"enabled":"true"}}),
        json!({"metrics":[]}),
        json!([]),
    ] {
        assert!(matches!(
            resolve(&d, Some(file), &[]),
            Err(Error::InvalidConfig(_))
        ));
    }
    assert_eq!(
        resolve(
            &d,
            Some(json!({"metrics":{"mlu_buckets":"invalid","stage_buckets":[]}})),
            &[]
        )
        .unwrap(),
        d
    );
}
#[test]
fn engine_loads_a_metrics_section_from_a_real_bounded_file() {
    let path = std::env::temp_dir().join(format!("orbitte-metrics-config-{}", std::process::id()));
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = Cleanup(path.clone());
    let cfg = EngineConfig {
        metrics_config_file: Some(path.clone()),
        ..Default::default()
    };
    std::fs::write(
        &path,
        br#"{"other":{"secret":"never exported"},"metrics":{"enabled":false}}"#,
    )
    .unwrap();
    assert!(
        TransferEngine::new(cfg.clone())
            .unwrap()
            .metrics()
            .is_none()
    );
    for bytes in [b"{".as_slice(), &vec![b' '; 1024 * 1024 + 1]] {
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            TransferEngine::new(cfg.clone()),
            Err(Error::InvalidConfig(_))
        ));
    }
}
