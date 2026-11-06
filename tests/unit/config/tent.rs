use super::*;
use std::{collections::BTreeMap, ffi::OsString, fs, path::PathBuf, process::Command};

const CONFIG: &str = include_str!("../../reference/inventory/config.tsv");
const ENV: &str = include_str!("../../reference/inventory/environment.tsv");
const PRECEDENCE: &str = include_str!("../../reference/inventory/precedence.json");

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static ID: AtomicUsize = AtomicUsize::new(0);
        let p = std::env::temp_dir().join(format!(
            "orbite-config-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn load(root: Value, lookup: impl Fn(&str) -> Option<OsString>) -> Result<EngineConfig> {
    super::load(document::parse(&root.to_string(), "constructor")?, lookup)
}

fn config(root: Value) -> Result<EngineConfig> {
    load(root, |_| None)
}

fn feature(owner: &str) -> bool {
    match owner {
        "rdma" => cfg!(feature = "rdma"),
        "cuda" => cfg!(feature = "cuda"),
        "gds" => cfg!(feature = "gds"),
        "io-uring" => cfg!(feature = "io-uring"),
        "metrics" => cfg!(feature = "metrics"),
        "shm" => cfg!(target_os = "linux"),
        _ => true,
    }
}

fn row_document(key: &str, value: Value) -> Value {
    let mut root = json!({});
    if key.starts_with("policy[]") {
        let mut entry = json!({"segment_type":"memory"});
        entry[key.trim_start_matches("policy[]/")] = value;
        root["policy"] = json!([entry]);
    } else if let Some(field) = key.strip_prefix("qos/tenants[]/") {
        let mut tenant = json!({"name":"team"});
        document::set(&mut tenant, &field.replace("{intent}", "restore"), value).unwrap();
        root["qos"] = json!({"tenants":[tenant]});
    } else if let Some(field) = key.strip_prefix("transports/rdma/endpoint/qp_pools[]/") {
        let mut pool = json!({"name":"pool","num_qp":2});
        pool[field] = value;
        document::set(
            &mut root,
            "transports/rdma/endpoint/qp_pools",
            json!([pool]),
        )
        .unwrap();
    } else {
        document::set(&mut root, key, value).unwrap();
    }
    if key.starts_with("runtime_queue/") {
        root["enable_runtime_queue"] = true.into();
    }
    if key == "transports/hp_tcp/enable" {
        document::set(&mut root, "transports/tcp/enable", false.into()).unwrap();
    }
    root
}

#[test]
fn every_config_row_reaches_its_schema_or_fails_closed() {
    let mut counts = [0; 3];
    for line in CONFIG.lines().skip(1) {
        let cols: Vec<_> = line.split('\t').collect();
        let (key, owner) = (cols[0], cols[6]);
        let sample: Value = serde_json::from_str(cols[7]).unwrap();
        let probe = |value| {
            load(row_document(key, value), |k| {
                (k == "MC_ENABLE_MNNVL" && owner == "cuda").then(|| "".into())
            })
        };
        let result = probe(sample);
        if owner.starts_with("unsupported:") || owner.starts_with("excluded:") {
            assert!(
                matches!(&result, Err(Error::TentConfig{path,..}) if path==key),
                "{key}: {result:?}"
            );
            counts[if owner.starts_with("unsupported:") {
                1
            } else {
                2
            }] += 1;
        } else {
            counts[0] += 1;
            if feature(owner) || (owner == "shm" && key.ends_with("cxl_mount_path")) {
                assert!(result.is_ok(), "{key}: {result:?}");
            } else {
                assert!(result.is_err(), "{key} accepted without {owner}");
            }
            // Neither a delegated subtree nor a directly assigned scalar may be silently ignored.
            let bad = probe(json!({"?invalid":true}));
            if key == "policy[]/qp_pool" {
                // TENT and the existing selector spray on a non-string pool name.
                assert!(bad.unwrap().selector.unwrap().policy[0].qp_pool.is_none());
            } else {
                assert!(bad.is_err(), "{key} accepted the wrong type");
            }
        }
    }
    assert_eq!(counts, [144, 46, 33]);
}

#[test]
fn every_environment_row_has_a_parser_or_an_inactive_or_rejected_owner() {
    let mut count = 0;
    for line in ENV.lines().skip(1) {
        let cols: Vec<_> = line.split('\t').collect();
        let (name, key, disposition) = (cols[0], cols[1], cols[5]);
        let mut base = json!({});
        let mut text = "2";
        let mut expected = json!(2);
        match disposition.split(':').next().unwrap() {
            "excluded" | "inactive" => {
                // In particular, P2P never even requests Redis credentials.
                let _ = environment::resolve(base, |k| {
                    assert_ne!(k, name);
                    None
                })
                .unwrap();
                count += 1;
                continue;
            }
            "unsupported" if name.contains("RPC_CLIENT_IO_THREADS") => {
                assert!(
                    matches!(environment::resolve(base,|k|(k==name).then(||text.into())),Err(Error::TentConfig{path,..}) if path==name)
                );
                count += 1;
                continue;
            }
            "branch" => {
                assert!(
                    environment::resolve(base, |k| (k == name).then(|| "0".into()))
                        .unwrap()
                        .1
                );
                count += 1;
                continue;
            }
            "presence" => {
                text = "";
                expected = true.into();
            }
            "root" => {
                let (r, _) = environment::resolve(base, |k| {
                    (k == name).then(|| r#"{"merge_requests":false}"#.into())
                })
                .unwrap();
                assert_eq!(r, json!({"merge_requests":false}));
                count += 1;
                continue;
            }
            "metrics" if key.ends_with("/enabled") => {
                text = "ON";
                expected = true.into();
            }
            "metrics" if key.ends_with("/http_host") => {
                text = "localhost";
                expected = text.into();
            }
            _ if matches!(
                name,
                "MC_IOURING_PINNED_STAGING"
                    | "MC_STRICT_LOCAL_NUMA"
                    | "MC_MNNVL_EGM"
                    | "MC_LOG_RDMA_SLICE_AFFINITY"
                    | "MC_DISABLE_GPU_DIRECT_RDMA"
            ) =>
            {
                text = "yEs";
                expected = true.into();
            }
            _ if name.starts_with("MC_TE_FILTERS") => {
                text = " a, ,b, ";
                expected = json!(["a", "b"]);
            }
            _ if matches!(
                name,
                "MC_IOURING_STAGING_SLOTS" | "MC_IOURING_STAGING_SLOT_KB"
            ) =>
            {
                document::set(&mut base, "transports/io_uring/pinned_staging", true.into())
                    .unwrap();
            }
            _ => {}
        }
        let (r, _) = environment::resolve(base, |k| (k == name).then(|| text.into())).unwrap();
        assert_eq!(document::get(&r, key), Some(&expected), "{name}");
        if disposition.starts_with("unsupported") {
            assert!(
                matches!(config(r),Err(Error::TentConfig{path,..}) if path==key),
                "{name}"
            );
        }
        count += 1;
    }
    assert_eq!(count, 67);
}

#[test]
fn frozen_precedence_vectors() {
    let vectors: Vec<Value> = serde_json::from_str(PRECEDENCE).unwrap();
    let temp = Temp::new();
    assert_eq!(vectors.len(), 12);
    for test in vectors {
        let id = test["id"].as_str().unwrap();
        let mut env: BTreeMap<String, OsString> = BTreeMap::new();
        if let Some(entries) = test["env"].as_object() {
            for (k, v) in entries {
                env.insert(k.clone(), v.as_str().unwrap().into());
            }
        }
        if let Some(value) = test.get("inline") {
            env.insert("MC_TENT_CONF".into(), value.to_string().into());
        }
        if let Some(value) = test.get("file") {
            let file = temp.0.join(id);
            fs::write(&file, value.to_string()).unwrap();
            env.insert("MC_TENT_CONF".into(), file.into());
        }
        if let Some(raw) = test["raw"].as_str() {
            env.insert("MC_TENT_CONF".into(), raw.into());
        }
        let resolved = environment::resolve(
            test.get("base").cloned().unwrap_or_else(|| json!({})),
            |k| env.get(k).cloned(),
        );
        if test["ok"] == false {
            assert!(resolved.is_err(), "{id}");
            continue;
        }
        let (root, _) = resolved.unwrap();
        let root = if test["mode"] == "metrics" {
            #[cfg(feature = "metrics")]
            {
                let cfg = config(root).unwrap().metrics;
                json!({"enabled":cfg.enabled,"http_port":cfg.http_port,"http_host":cfg.http_host,"http_server_threads":cfg.http_server_threads,"report_interval_seconds":cfg.report_interval_seconds})
            }
            #[cfg(not(feature = "metrics"))]
            {
                continue;
            }
        } else {
            root
        };
        for (key, want) in test["expect"].as_object().unwrap() {
            assert_eq!(
                document::get(&root, key).unwrap_or(&Value::Null),
                want,
                "{id} {key}"
            );
        }
    }
}

#[test]
fn invalid_roots_paths_files_types_and_credentials_are_bounded_diagnostics() {
    for text in [
        "[]",
        "4",
        "null",
        r#"{"":{}}"#,
        r#"{"/metrics/enabled":true}"#,
        r#"{"metrics//enabled":true}"#,
        r#"{"metrics/enabled/":true}"#,
        r#"{"metrics":{"enabled":true,"enabled":false}}"#,
        r#"{"password":"private"#,
    ] {
        let e = document::parse(text, "constructor").unwrap_err();
        assert!(matches!(e, Error::TentConfig { .. }));
        assert!(!e.to_string().contains("private"));
    }
    for text in [
        r#"{"rpc_server_port":"private"}"#,
        r#"{"rpc_server_threads":129}"#,
        r#"{"rpc_server_port":65536}"#,
        r#"{"rpc_server_port":"123 "}"#,
        r#"{"runtime_queue":true}"#,
        r#"{"metrics":{"enabled/":false}}"#,
        r#"{"metadata_type":"redis","metadata_servers":"private"}"#,
    ] {
        let err = config(document::parse(text, "constructor").unwrap()).unwrap_err();
        assert!(!err.to_string().contains("private"));
    }
    let temp = Temp::new();
    for p in [temp.0.clone(), temp.0.join("missing")] {
        assert!(EngineConfig::from_tent_file(p).is_err());
    }
    let large = temp.0.join("large");
    fs::write(&large, vec![b' '; document::MAX_BYTES + 1]).unwrap();
    assert!(EngineConfig::from_tent_file(large).is_err());
    let nested = document::parse(
        r#"{"transports/tcp/enable":false,"transports":{"tcp":{"enable":true}}}"#,
        "test",
    )
    .unwrap();
    assert_eq!(
        document::get(&nested, "transports/tcp/enable"),
        Some(&true.into())
    );
}

#[test]
fn constructor_identity_and_legacy_corner_cases_reach_typed_fields() {
    let base = json!({"rpc_server_port":"0","local_segment_name":"owner","metadata_type":"p2p","metadata_servers":"preserved","rpc_server_hostname":"127.0.0.1","transports":{"force_tcp":true}});
    let cfg=load(base, |k|match k {
        "MC_TENT_CONF"=>Some(r#"{"metadata_type":"redis","rpc_server_port":65530,"merge_requests":false,"runtime_queue":{"max_dispatch_bytes":1024},"enable_runtime_queue":true}"#.into()),
        "MOONCAKE_LOCAL_HOSTNAME"=>Some("ignored.invalid".into()),_=>None
    }).unwrap();
    assert!(!cfg.merge_requests && cfg.tcp && !cfg.hp_tcp.enable && cfg.enable_progress_worker);
    assert!(cfg.selector.unwrap().force_tcp);
    assert_eq!(cfg.runtime_queue.unwrap().max_dispatch_bytes, 1024);
    assert_eq!(cfg.bind_addr, "127.0.0.1:0".parse().unwrap());
    let id = cfg.tent.unwrap();
    assert_eq!(id.local_segment_name, "owner");
    assert_eq!(id.metadata_servers, "preserved");
    #[cfg(feature = "rdma")]
    assert!(cfg.rdma.is_none());
}

#[test]
fn public_constructors_capture_the_file_and_do_not_reread_process_environment() {
    const CHILD: &str = "ORBITE_S17_SNAPSHOT_TEST";
    if std::env::var_os(CHILD).is_none() {
        let temp = Temp::new();
        let path = temp.0.join("engine.json");
        fs::write(&path,r#"{"local_segment_name":"first","rpc_server_port":"0","metrics":{"enabled":false},"transports":{"force_tcp":true}}"#).unwrap();
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        for row in ENV.lines().skip(1) {
            cmd.env_remove(row.split('\t').next().unwrap());
        }
        let output=cmd.args(["--exact","config::tent::tests::public_constructors_capture_the_file_and_do_not_reread_process_environment","--nocapture"])
            .env(CHILD,"yes").env("MC_TENT_CONF",&path).env("MC_FORCE_TCP","0")
            .env("TENT_METRICS_ENABLED","intentionally-invalid-shadowed").output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    let path = PathBuf::from(std::env::var_os("MC_TENT_CONF").unwrap());
    let first = EngineConfig::from_tent_env().unwrap();
    let explicit = EngineConfig::from_tent_json(r#"{"local_segment_name":"explicit"}"#).unwrap();
    fs::write(
        &path,
        r#"{"local_segment_name":"second","merge_requests":false,"metrics":{"enabled":false}}"#,
    )
    .unwrap();
    let second = EngineConfig::from_tent_file(&path).unwrap();
    fs::remove_file(&path).unwrap();
    for (c, name, merge) in [
        (first, "first", true),
        (explicit, "explicit", true),
        (second, "second", false),
    ] {
        let engine = crate::TransferEngine::new(c).unwrap();
        assert_eq!(
            engine.config().tent.as_ref().unwrap().local_segment_name,
            name
        );
        assert_eq!(engine.config().merge_requests, merge);
        assert!(engine.endpoint().port() > 0);
    }
    assert_eq!(std::env::var_os("MC_FORCE_TCP"), Some("0".into()));
}
#[test]
fn owner_settings_compile_into_the_existing_configuration() {
    let cfg=config(json!({
        "transports/tcp/enable":false,
        "transports/hp_tcp":{"enable":true,"rail_addresses":["127.0.0.2"],"connections_per_peer":2},
        "transports/shm/cxl_mount_path":"/dev/shm",
        "max_failover_attempts":1,"enable_auto_failover_on_poll":false,"notification/rpc_fallback":false,
        "policy":[{"name":"bulk","segment_type":"memory","transports":["hp_tcp"],"max_size":8192}],
        "qos":{"defaults":{"max_inflight_bytes":8192,"allowed_degraded_actions":["reject"]}},
        "runtime_queue":{"max_dispatch_owners":3,"staging_byte_reserve":512},
        "enable_runtime_queue":true
    })).unwrap();
    assert!(
        cfg.hp_tcp.enable
            && !cfg.tcp
            && !cfg.enable_auto_failover_on_poll
            && !cfg.notification_rpc_fallback
    );
    assert_eq!(cfg.hp_tcp.connections_per_peer, 2);
    assert_eq!(cfg.hp_tcp.rail_addresses[0].octets(), [127, 0, 0, 2]);
    assert_eq!(cfg.shm.cxl_mount_path, Path::new("/dev/shm"));
    assert_eq!(
        cfg.selector.unwrap().policy[0].transports,
        [crate::Transport::HpTcp]
    );
    assert_eq!(cfg.qos.unwrap().defaults.max_inflight_bytes, Some(8192));
    assert_eq!(cfg.runtime_queue.unwrap().staging_byte_reserve, 512);
    assert_eq!(cfg.max_failover_attempts, 1);
    assert_eq!(cfg.handler_threads, 1);
    assert!(
        (4..=8).contains(
            &config(json!({"transports":{"tcp":{"enable":true}}}))
                .unwrap()
                .handler_threads
        )
    );
    assert!(config(json!({"transports/hp_tcp/enable":true})).is_err());
    assert!(config(json!({"qos":{"defaults":{"allowed_degraded_actions":["compress"]}}})).is_err());
}

#[cfg(feature = "rdma")]
#[test]
fn captured_topology_lane_aliases_and_verbs_settings_reach_rdma() {
    let tmp = Temp::new();
    let topo = tmp.0.join("topology.json");
    let rail = tmp.0.join("rail.json");
    fs::write(&topo, r#"{"nics":[],"mems":[]}"#).unwrap();
    fs::write(&rail, "[]").unwrap();
    let c=load(json!({"topology/custom_json_path":topo,"transports/rdma/workers/rail_topo_path":rail,
        "transports/rdma/num_lanes":3,"transports/rdma/endpoint/qp_pools":[{"name":"bulk","num_qp":2}],
        "topology/rdma_whitelist":["nic0"],"transports/rdma/rail_cooldown_secs":5,"transports/rdma/default_bandwidth_gbps":120}),
        |key|match key {
            "MC_GID_INDEX"=>Some("2".into()),"NCCL_IB_GID_INDEX"=>Some("3".into()),
            "MC_TE_FILTERS"=>Some("other".into()),"MC_TE_FILTERS_EXCLUDE"=>Some(" b, a ,, ".into()),
            "MC_NUM_CQ_PER_CTX"|"MC_WORKERS_PER_CTX"=>Some("3".into()),"MC_IB_SL"=>Some("4".into()),
            "MC_IB_TC"=>Some("14".into()),"MC_DISABLE_GPU_DIRECT_RDMA"=>Some("1".into()),_=>None
        }).unwrap();
    let rdma = c.rdma.as_ref().unwrap();
    assert_eq!(
        (
            rdma.gid_index,
            rdma.service_level,
            rdma.traffic_class,
            rdma.num_lanes
        ),
        (Some(3), 4, 14, 3)
    );
    assert!(rdma.disable_gpu_direct_rdma);
    assert_eq!(rdma.rdma_whitelist, ["nic0"]);
    assert_eq!(rdma.rdma_blacklist, ["b", "a"]);
    assert_eq!(rdma.rail_cooldown, Duration::from_secs(5));
    assert_eq!(rdma.scheduling.default_bandwidth_gbps, 120.0);
    assert_eq!(c.selector.unwrap().qp_pools[0].name, "bulk");
    fs::remove_file(&topo).unwrap();
    fs::remove_file(&rail).unwrap();
    assert_eq!(
        rdma.custom_topology_json.as_deref(),
        Some(r#"{"nics":[],"mems":[]}"#)
    );
    assert_eq!(rdma.rail_topology.as_deref(), Some("[]"));
    assert!(
        config(json!({"topology/custom_json_path":topo,"topology/priority_matrix":{"*":[[],[]]}}))
            .is_ok()
    );
    assert!(config(json!({"topology/custom_json_path":topo})).is_err());
    for bad in [
        json!({"transports/rdma/device/num_cq_list":2,"transports/rdma/workers/num_workers":3}),
        json!({"transports/rdma/endpoint/max_qp_wr":256}),
        json!({"transports/rdma/endpoint/qp_pools":[{"name":"empty","num_qp":0}]}),
    ] {
        assert!(config(bad).is_err());
    }
    assert_eq!(config(json!({})).unwrap().rdma.unwrap().num_lanes, 6);
}

#[cfg(feature = "cuda")]
#[test]
fn nvlink_and_fabric_use_the_selected_loader_branch() {
    let doc = json!({"transports/nvlink/enable":true,"transports/nvlink/async_memcpy_threshold":47,"transports/mnnvl/egm":true});
    let fabric = load(doc.clone(), |k| {
        (k == "MC_ENABLE_MNNVL").then(|| "0".into())
    })
    .unwrap();
    assert!(!fabric.nvlink);
    let fabric = fabric.mnnvl.unwrap();
    assert!(fabric.egm && fabric.host_register);
    assert_eq!(fabric.async_memcpy_threshold, 47);
    let legacy = config(doc).unwrap();
    assert!(legacy.nvlink && legacy.mnnvl.is_none());
    assert_eq!(legacy.nvlink_async_memcpy_threshold_kb, 47);
    assert!(!config(json!({})).unwrap().nvlink);
    assert!(config(json!({"transports/nvlink/host_register":true})).is_err());
}

#[cfg(feature = "io-uring")]
#[test]
fn pinned_pool_sizes_only_follow_the_active_pool() {
    let c = load(json!({}), |k| {
        (k == "MC_IOURING_STAGING_SLOTS").then(|| "invalid".into())
    })
    .unwrap();
    assert!(!c.io_uring.pinned_staging);
    let cfg = load(
        json!({"transports/io_uring/pinned_staging":true}),
        |k| match k {
            "MC_IOURING_STAGING_SLOTS" => Some("7".into()),
            "MC_IOURING_STAGING_SLOT_KB" => Some("8".into()),
            _ => None,
        },
    )
    .unwrap();
    assert_eq!(
        (cfg.io_uring.staging_slots, cfg.io_uring.staging_slot_kb),
        (7, 8)
    );
    assert!(
        !config(json!({"transports/io_uring/enable":false}))
            .unwrap()
            .io_uring_enabled
    );
    #[cfg(feature = "gds")]
    {
        assert!(!config(json!({})).unwrap().gds_enabled);
        let gds = config(json!({"transports/gds/enable":true,"transports/gds/io_batch_depth":7}))
            .unwrap();
        assert!(gds.gds_enabled);
        assert_eq!(gds.gds_batch_depth, 7);
    }
}

#[test]
fn full_leaf_wins_over_a_synthesized_flat_subtree() {
    let base = document::parse(
        r#"{"transports/rdma":{"enable":true}, "transports/rdma/enable":false}"#,
        "constructor",
    )
    .unwrap();
    assert_eq!(
        document::get(&base, "transports/rdma/enable"),
        Some(&Value::Bool(false))
    );
}

#[cfg(feature = "rdma")]
#[test]
fn typed_engine_captures_topology_before_lazy_device_discovery() {
    let temp = Temp::new();
    let path = temp.0.join("matrix");
    fs::write(&path, r#"{"*":[[],[]]}"#).unwrap();
    let rdma = crate::transport::rdma::RdmaConfig {
        custom_json_path: Some(path.clone()),
        ..Default::default()
    };
    let engine = crate::TransferEngine::new(EngineConfig {
        rdma: Some(rdma),
        ..Default::default()
    })
    .unwrap();
    fs::remove_file(path).unwrap();
    assert_eq!(
        engine
            .config()
            .rdma
            .as_ref()
            .unwrap()
            .custom_topology_json
            .as_deref(),
        Some(r#"{"*":[[],[]]}"#)
    );
}
