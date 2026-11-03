use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::segment::ProcessIdentity;
use crate::{EngineConfig, Permission, SourceEpoch, TransferEngine, TransferOp, TransferRequest};

fn make_selector(config: &TransportSelectorConfig) -> Result<TransportSelector> {
    TransportSelector::compile(config, None, &QpLayout::new(&config.qp_pools, 1)?)
}

fn select(
    selector: Option<&TransportSelector>,
    ctx: &SelectionContext<'_>,
    hint: Transport,
) -> Result<Transport> {
    ctx.select(
        selector,
        &TransferRequest::new(hint, 0, 0, ctx.transfer_size),
    )
    .map(|r| r.transport)
}

const TIMEOUT: Duration = Duration::from_secs(3);

fn region(kind: MemoryType, ipc_exportable: bool) -> RemoteRegion {
    RemoteRegion {
        epoch: SourceEpoch([1; 16]),
        id: 1,
        address: 1,
        length: 64,
        permission: Permission::RemoteReadWrite,
        memory_type: kind,
        ipc_exportable,
    }
}

fn context(remote: &RemoteRegion) -> SelectionContext<'_> {
    SelectionContext {
        local_memory: MemoryType::Cpu,
        local_ipc: false,
        remote,
        local_segment: SegmentMetadata {
            rdma: true,
            ..Default::default()
        },
        remote_segment: SegmentMetadata {
            rdma: true,
            ..Default::default()
        },
        transfer_size: 16,
        priority: Priority::High,
        remote_staging: false,
    }
}

#[test]
fn transport_selector_first_match_bounds_priority_and_hard_hint() {
    let remote = region(MemoryType::Cpu, false);
    let mut context = context(&remote);
    let selector = make_selector(
        &TransportSelectorConfig::from_json(
            r#"{"policy":[
        {"name":"cuda", "segment_type":"memory", "remote_memory":"cuda", "transports":["nvlink"]},
        {"name":"small", "segment_type":"memory", "local_memory":"cpu", "remote_memory":"cpu",
         "min_size":8, "max_size":16, "priority":"high", "transports":["tcp","rdma"]},
        {"name":"bulk", "segment_type":"memory", "transports":["rdma"]}
    ]}"#,
        )
        .unwrap(),
    )
    .unwrap();
    for size in [8, 16] {
        context.transfer_size = size;
        assert_eq!(
            select(Some(&selector), &context, Transport::Unspec).unwrap(),
            Transport::Tcp
        );
        assert_eq!(
            select(Some(&selector), &context, Transport::Rdma).unwrap(),
            Transport::Rdma
        );
    }
    for size in [7, 17] {
        context.transfer_size = size;
        assert_eq!(
            select(Some(&selector), &context, Transport::Unspec).unwrap(),
            Transport::Rdma
        );
        assert!(select(Some(&selector), &context, Transport::Tcp).is_err());
    }
    context.transfer_size = 16;
    context.priority = Priority::Medium;
    assert_eq!(
        select(Some(&selector), &context, Transport::Unspec).unwrap(),
        Transport::Rdma
    );
    context.priority = Priority::High;
    context.local_memory = MemoryType::Cuda;
    assert_eq!(
        select(Some(&selector), &context, Transport::Unspec).unwrap(),
        Transport::Rdma
    );
    context.local_segment.rdma = false;
    assert!(select(Some(&selector), &context, Transport::Unspec).is_err());
    context.local_memory = MemoryType::Cpu;
    assert!(select(Some(&selector), &context, Transport::Rdma).is_err());
    assert_eq!(
        select(Some(&selector), &context, Transport::Unspec).unwrap(),
        Transport::Tcp
    );
    let remote = region(MemoryType::Cuda, false);
    context.remote = &remote;
    // The first matching rule has no eligible NVLink candidate. Do not try small/bulk.
    assert!(select(Some(&selector), &context, Transport::Unspec).is_err());
    let no_match = make_selector(&TransportSelectorConfig {
        policy: vec![SelectionPolicy {
            min_size: Some(100),
            ..Default::default()
        }],
        ..Default::default()
    })
    .unwrap();
    assert!(select(Some(&no_match), &context, Transport::Tcp).is_err());
}

#[test]
fn transport_selector_uses_both_owners_and_preserves_staging_intent() {
    let remote = region(MemoryType::Cuda, true);
    let mut context = context(&remote);
    context.local_ipc = true;
    let identity = ProcessIdentity {
        machine: SourceEpoch([2; 16]),
        process: SourceEpoch([3; 16]),
    };
    context.local_segment.nvlink = Some(identity);
    context.remote_segment.nvlink = Some(ProcessIdentity {
        process: SourceEpoch([4; 16]),
        ..identity
    });
    let select = |context: &SelectionContext<'_>| select(None, context, Transport::Unspec).unwrap();
    assert_eq!(select(&context), Transport::Nvlink);
    context.local_ipc = false;
    assert_eq!(select(&context), Transport::Rdma);
    context.local_ipc = true;
    let ordinary = region(MemoryType::Cuda, false);
    context.remote = &ordinary;
    assert_eq!(select(&context), Transport::Rdma);
    context.remote = &remote;
    context.remote_segment.nvlink = Some(identity);
    assert_eq!(select(&context), Transport::Rdma);
    context.remote_segment.nvlink = Some(ProcessIdentity {
        machine: SourceEpoch([5; 16]),
        process: SourceEpoch([4; 16]),
    });
    assert_eq!(select(&context), Transport::Rdma);
    context.remote_segment.nvlink = None;
    context.remote_segment.rdma = false;
    assert_eq!(select(&context), Transport::Tcp);
    for priority in [Priority::Medium, Priority::Low] {
        context.priority = priority;
        assert_eq!(select(&context), Transport::Tcp);
    }
    context.priority = Priority::High;
    context.remote_staging = true;
    assert!(self::select(None, &context, Transport::Unspec).is_err());
    context.remote_segment.remote_staging = true;
    assert_eq!(
        self::select(None, &context, Transport::Unspec).is_ok(),
        cfg!(feature = "cuda")
    );
    let host = region(MemoryType::Cpu, false);
    context.remote = &host;
    assert!(self::select(None, &context, Transport::Unspec).is_err());
}

#[test]
fn qos_low_prefetch_selects_tcp_without_rdma() -> Result<()> {
    use crate::{QosConfig, QosRequestContext};

    let config = r#"{
        "policy":[{"name":"prefetch","segment_type":"memory","priority":"low","transports":["rdma","tcp"]}],
        "qos":{"tenants":[{"name":"reader","intents":{"background_prefetch":{"priority":"low"}}}]}
    }"#;
    let source = TransferEngine::new(EngineConfig::default())?;
    let remote =
        source.register_memory(vec![1, 2, 3, 4].into_boxed_slice(), Permission::RemoteRead)?;
    let requester = TransferEngine::new(EngineConfig {
        selector: Some(TransportSelectorConfig::from_json(config)?),
        qos: QosConfig::from_json(config)?,
        ..Default::default()
    })?;
    let local = requester.register_memory(vec![0; 4].into_boxed_slice(), Permission::LocalOnly)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let request = TransferRequest::new(
        Transport::Unspec,
        local.base_address(),
        remote.base_address(),
        4,
    );
    // The default High priority does not match the only policy.
    assert!(matches!(
        requester.submit_and_wait(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&request),
            TIMEOUT
        ),
        Err(Error::Unavailable("no matching transport selection policy"))
    ));
    let prefetch = requester.resolve_qos(&QosRequestContext {
        tenant_id: "reader".into(),
        intent: "background_prefetch".into(),
        requested_priority: Priority::High,
    })?;
    assert_eq!(prefetch.effective_priority(), Priority::Low);
    let report = requester.submit_and_wait(
        TransferOp::Read,
        &peer,
        &[request.with_qos(prefetch)],
        TIMEOUT,
    )?;
    assert!(report.is_complete());
    assert_eq!(report.transferred_bytes, 4);
    let mut actual = [0; 4];
    local.read(0, &mut actual)?;
    assert_eq!(actual, [1, 2, 3, 4]);
    Ok(())
}

#[test]
fn transport_selector_rejects_invalid_or_unconsumed_configuration() {
    for (case, policy) in [
        (
            "positional policy",
            r#"["x","memory","*","*",0,8,0,["tcp"]]"#,
        ),
        #[cfg(not(any(feature = "io-uring", feature = "gds")))]
        ("file", r#"{"segment_type":"file"}"#),
        (
            "file provider on memory",
            r#"{"segment_type":"memory","transports":["gds"]}"#,
        ),
        (
            "bad memory",
            r#"{"segment_type":"memory","local_memory":"npu"}"#,
        ),
        ("bad priority", r#"{"segment_type":"memory","priority":3}"#),
        (
            "inverted bounds",
            r#"{"segment_type":"memory","min_size":2,"max_size":1}"#,
        ),
        (
            "null memory",
            r#"{"segment_type":"memory","remote_memory":null}"#,
        ),
        ("null bound", r#"{"segment_type":"memory","max_size":null}"#),
        (
            "invalid intent",
            r#"{"segment_type":"memory","intent_type":"nebulous"}"#,
        ),
        (
            "unsupported field",
            r#"{"segment_type":"memory","transport_index":1}"#,
        ),
        (
            "provider",
            r#"{"segment_type":"memory","transports":["iouring"]}"#,
        ),
        (
            "duplicate",
            r#"{"segment_type":"memory","transports":["tcp","tcp"]}"#,
        ),
    ] {
        assert!(
            TransportSelectorConfig::from_json(&format!(r#"{{"policy":[{policy}]}}"#)).is_err(),
            "{case}"
        );
    }
    assert!(TransportSelectorConfig::from_json("[]").is_err());
    assert!(TransportSelectorConfig::from_json(&" ".repeat((1 << 20) + 1)).is_err());
    assert!(
        make_selector(&TransportSelectorConfig {
            policy: vec![SelectionPolicy {
                transports: vec![Transport::Unspec],
                ..Default::default()
            }],
            ..Default::default()
        })
        .is_err()
    );
}

#[test]
fn transport_selector_public_sync_async_and_atomic_preflight() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let remote = source
        .register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            selector: Some(
                TransportSelectorConfig::from_json(
                    r#"{"policy":[
            {"name":"host", "segment_type":"memory", "local_memory":"cpu", "remote_memory":"cpu",
             "max_size":4, "transports":["rdma","tcp"]},
            {"name":"large", "segment_type":"memory", "transports":["rdma"]}
        ]}"#,
                )
                .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap(),
    );
    let local = requester
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    assert_eq!(peer.regions()[0].memory_type(), MemoryType::Cpu);
    assert!(!peer.metadata.rdma);
    let make_request = |transport, length| {
        TransferRequest::new(
            transport,
            local.base_address(),
            remote.base_address(),
            length,
        )
    };
    let request = make_request(Transport::Unspec, 4);
    let oversized = make_request(Transport::Unspec, 8);
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[request.clone(), oversized],
                TIMEOUT
            )
            .is_err()
    );
    let mut bytes = [0; 8];
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0; 8]);
    assert_eq!(requester.stats().unwrap().inflight_batches, 0);
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[make_request(Transport::Rdma, 4)],
                TIMEOUT
            )
            .is_err()
    );
    let report = requester
        .submit_and_wait(
            TransferOp::Write,
            &peer,
            std::slice::from_ref(&request),
            TIMEOUT,
        )
        .unwrap();
    assert!(report.is_complete());
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7, 7, 7, 7, 0, 0, 0, 0]);
    local.write(0, &[0; 8]).unwrap();
    let mut batch = requester.allocate_batch(1).unwrap();
    batch
        .submit(TransferOp::Read, &peer, &[request], None, TIMEOUT)
        .unwrap();
    batch.seal().unwrap();
    batch.wait(TIMEOUT).unwrap();
    local.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7, 7, 7, 7, 0, 0, 0, 0]);
    batch.free().unwrap();
    // Unconfigured explicit callers keep their existing route, while Unspec also works.
    let direct = TransferEngine::new(EngineConfig::default()).unwrap();
    let target = direct
        .register_memory(vec![0; 4].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest::new(
        Transport::Unspec,
        target.base_address(),
        remote.base_address(),
        4,
    );
    assert!(
        direct
            .submit_and_wait(TransferOp::Read, &peer, &[request], TIMEOUT)
            .unwrap()
            .is_complete()
    );
}

#[test]
fn intent_names_binding_and_force_tcp() -> Result<()> {
    use IntentType::*;
    let remote = region(MemoryType::Cpu, false);
    let ctx = context(&remote);
    let intents = [
        ("INTENT_UNSPEC", Unspec),
        ("foreground_get", ForegroundGet),
        ("BACKGROUND_PREFETCH", BackgroundPrefetch),
        ("migration", Migration),
        ("checkpoint", Checkpoint),
        ("weight_loading", WeightLoading),
        ("staging_internal", StagingInternal),
    ];
    for (index, (name, intent)) in intents.into_iter().enumerate() {
        for json in [format!("\x22{name}\x22"), index.to_string()] {
            let config = TransportSelectorConfig::from_json(&format!(
                r#"{{"policy":[
                {{"name":"intent","segment_type":"memory","intent_type":{json},"transports":["rdma"]}},
                {{"name":"other","segment_type":"memory","transports":["tcp"]}}]}}"#
            ))?;
            let selector = make_selector(&config)?;
            let req = TransferRequest::new(Transport::Unspec, 0, 0, 16);
            let choose = |req: &TransferRequest| ctx.select(Some(&selector), req);
            assert_eq!(
                choose(&req.clone().with_intent(intent))?.transport,
                Transport::Rdma
            );
            let other = if intent == Checkpoint {
                Migration
            } else {
                Checkpoint
            };
            let mismatch = req.with_intent(other);
            assert_eq!(choose(&mismatch)?.transport, Transport::Tcp);
            assert_eq!(
                choose(&mismatch.clone().with_policy_name("intent")?)?.transport,
                Transport::Rdma
            );
            assert!(
                matches!(choose(&mismatch.with_policy_name("missing")?), Err(Error::TransportPolicy {policy, reason:"unknown policy"}) if policy.as_str()=="missing")
            );
        }
    }
    let req = TransferRequest::new(Transport::Unspec, 0, 0, 1);
    let selector = make_selector(&TransportSelectorConfig::from_json(
        r#"{"transports":{"force_tcp":true},"policy":[{"name":"bulk","segment_type":"memory","transports":["rdma"]}]}"#,
    )?)?;
    assert_eq!(ctx.select(Some(&selector), &req)?.transport, Transport::Tcp);
    let mut strict = req;
    strict.transport = Transport::Rdma;
    assert!(
        matches!(ctx.select(Some(&selector), &strict), Err(Error::TransportPolicy {policy,..}) if policy.as_str()=="bulk")
    );
    for invalid in ["null", "7", "-1", r#""unspecified""#] {
        let json = format!(
            r#"{{"policy":[{{"name":"bad","segment_type":"memory","intent_type":{invalid}}}]}}"#
        );
        let error = TransportSelectorConfig::from_json(&json)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("bad") && error.contains("intent_type"),
            "{error}"
        );
    }
    Ok(())
}

#[test]
fn device_masks_compile_once_against_stable_topology_ids() -> Result<()> {
    let config = TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"rails","segment_type":"memory","devices":["b","c","missing","outside"]}]}"#,
    )?;
    let layout = QpLayout::new(&[], 1)?;
    let remote = region(MemoryType::Cpu, false);
    let ctx = context(&remote);
    let req = TransferRequest::new(Transport::Unspec, 0, 0, 16);
    let mut names: Vec<_> = (0..65).map(|i| i.to_string()).collect();
    names[2] = "b".into();
    names[7] = "c".into();
    names[64] = "outside".into();
    let compile = |names: Option<&[String]>| TransportSelector::compile(&config, names, &layout);
    let compiled = compile(Some(&names))?;
    assert_eq!(compiled.unresolved_devices, 2);
    let mask =
        |s: &TransportSelector| -> Result<u64> { Ok(ctx.select(Some(s), &req)?.device_mask) };
    assert_eq!(mask(&compiled)?, 1 << 2 | 1 << 7);
    names.swap(7, 0);
    assert_eq!(mask(&compiled)?, 1 << 2 | 1 << 7);
    assert_eq!(mask(&compile(Some(&names))?)?, 1 << 2 | 1);
    let missing = compile(Some(&[]))?;
    assert_eq!(missing.unresolved_devices, 4);
    assert_eq!(mask(&missing)?, u64::MAX);
    assert_eq!(mask(&compile(None)?)?, u64::MAX);
    Ok(())
}

#[test]
fn qp_pool_layout_and_spray_partition_the_native_indices() -> Result<()> {
    let cfg = TransportSelectorConfig::from_json(
        r#"{"transports":{"rdma":{"endpoint":{"qp_mul_factor":2,
       "qp_pools":[{"name":"kv","num_qp":4,"service_level":5,"traffic_class":96},{"name":"ctrl","num_qp":2}]}}}}"#,
    )?;
    assert_eq!(cfg.qp_mul_factor, Some(2));
    let layout = QpLayout::new(&cfg.qp_pools, 2)?;
    assert_eq!(layout.width, 6);
    for i in 0..6 {
        let p = layout.pool_at(i).unwrap();
        assert_eq!(p.name, if i < 4 { "kv" } else { "ctrl" });
        assert_eq!(p.service_level, (i < 4).then_some(5));
        assert_eq!(p.traffic_class, (i < 4).then_some(96));
    }
    assert!(layout.has_pools() && layout.pool_at(6).is_none());
    let flat = QpLayout::new(&[], 6)?;
    assert!(!flat.has_pools() && flat.width == 6);
    assert!(QpLayout::new(&[], 0).is_err());
    for i in 0..128 {
        for name in [None, Some(""), Some("absent")] {
            assert_eq!(layout.range(name).index(i), i % 6);
        }
        assert_eq!(flat.range(Some("kv")).index(i), i % 6);
        assert_eq!(layout.range(Some("kv")).index(i), i % 4);
        assert_eq!(layout.range(Some("ctrl")).index(i), 4 + i % 2);
    }
    for count in ["0", "-1", "65"] {
        let json = format!(
            r#"{{"transports":{{"rdma":{{"endpoint":{{"qp_pools":[{{"name":"bad","num_qp":{count}}}]}}}}}}}}"#
        );
        assert!(
            TransportSelectorConfig::from_json(&json)
                .and_then(|c| QpLayout::new(&c.qp_pools, 1))
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn policy_qos_is_diagnostic_and_pool_ranges_are_compiled() -> Result<()> {
    let remote = region(MemoryType::Cpu, false);
    let ctx = context(&remote);
    for (sl, tc, pool, selected, diagnostic) in [
        (5, 96, r#""bulk""#, true, (Some(5), Some(96))),
        (16, -1, r#""""#, false, (None, None)),
        (-1, 256, "123", false, (None, None)),
    ] {
        let config = TransportSelectorConfig::from_json(&format!(
            r#"{{"policy":[{{"name":"s","segment_type":"memory","service_level":{sl},"traffic_class":{tc},"qp_pool":{pool}}}],
        "transports":{{"rdma":{{"endpoint":{{"qp_pools":[{{"name":"first","num_qp":2}},{{"name":"bulk","num_qp":1}}]}}}}}}}}"#
        ))?;
        let selector = make_selector(&config)?;
        let r = ctx.select(
            Some(&selector),
            &TransferRequest::new(Transport::Unspec, 0, 0, 16),
        )?;
        assert_eq!((r.service_level, r.traffic_class), diagnostic);
        assert_eq!(r.qp_pool.unwrap().index(0), if selected { 2 } else { 0 });
    }
    Ok(())
}

#[test]
fn boot_identity_filters_rules_without_nvlink_and_never_uses_peer_ip() -> Result<()> {
    let config = TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"near","segment_type":"memory","same_machine":true,"transports":["tcp"]},{"name":"far","segment_type":"memory","same_machine":false,"transports":["rdma"]}]}"#,
    )?;
    let selector = make_selector(&config)?;
    let remote = region(MemoryType::Cpu, false);
    let mut ctx = context(&remote);
    let req = TransferRequest::new(Transport::Unspec, 0, 0, 16);
    ctx.local_segment.boot = Some(SourceEpoch([3; 16]));
    for (peer, expected) in [
        (None, Transport::Rdma),
        (Some(SourceEpoch([5; 16])), Transport::Rdma),
        (ctx.local_segment.boot, Transport::Tcp),
    ] {
        let meta = SegmentMetadata {
            boot: peer,
            rdma: true,
            ..Default::default()
        };
        let mut bytes = [0; SegmentMetadata::WIRE_SIZE];
        meta.encode(&mut bytes);
        ctx.remote_segment = SegmentMetadata::decode(&bytes)?;
        assert_eq!(ctx.select(Some(&selector), &req)?.transport, expected);
    }
    Ok(())
}

#[test]
fn default_order_legacy_force_tcp_and_strict_hints() -> Result<()> {
    let r = region(MemoryType::Cuda, true);
    let mut ctx = context(&r);
    ctx.local_ipc = true;
    ctx.local_segment.nvlink = Some(ProcessIdentity {
        machine: SourceEpoch([1; 16]),
        process: SourceEpoch([2; 16]),
    });
    ctx.remote_segment.nvlink = Some(ProcessIdentity {
        machine: SourceEpoch([1; 16]),
        process: SourceEpoch([3; 16]),
    });
    let req = TransferRequest::new(Transport::Unspec, 0, 0, 16);
    assert_eq!(ctx.select(None, &req)?.transport, Transport::Nvlink);
    let mut config = TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"ordered","segment_type":"memory","transports":["tcp","rdma","nvlink"]}]}"#,
    )?;
    assert!(!config.use_legacy_transport_selection);
    let mut pinned = req.clone();
    pinned.transport = Transport::Rdma;
    for (legacy, force, unnamed, hint_ok) in [
        (false, false, Transport::Tcp, true),
        (true, false, Transport::Nvlink, true),
        (true, true, Transport::Tcp, false),
    ] {
        config.use_legacy_transport_selection = legacy;
        config.force_tcp = force;
        let selector = make_selector(&config)?;
        assert_eq!(ctx.select(Some(&selector), &req)?.transport, unnamed);
        assert_eq!(ctx.select(Some(&selector), &pinned).is_ok(), hint_ok);
        assert_eq!(
            ctx.select(Some(&selector), &req.clone().with_policy_name("ordered")?)?
                .transport,
            Transport::Tcp
        );
    }
    for src in [MemoryType::Cpu, MemoryType::Cuda] {
        for dst in [MemoryType::Cpu, MemoryType::Cuda] {
            let remote = region(dst, false);
            let mut ctx = context(&remote);
            ctx.local_memory = src;
            assert_eq!(ctx.select(None, &req)?.transport, Transport::Rdma);
        }
    }
    for (name, expected) in [
        ("tcp", Transport::Tcp),
        ("rdma", Transport::Rdma),
        ("nvlink", Transport::Nvlink),
    ] {
        let config = TransportSelectorConfig::from_json(&format!(
            r#"{{"policy":[{{"segment_type":"memory","transports":["{name}"]}}]}}"#
        ))?;
        assert_eq!(config.policy[0].transports, [expected]);
    }
    for provider in [
        "ub", "xpu", "mlu", "hylink", "hp_tcp", "shm", "fabric", "mnnvl", "mpcomm",
    ] {
        let json = format!(
            r#"{{"policy":[{{"name":"unsupported","segment_type":"memory","transports":["{provider}"]}}]}}"#
        );
        assert!(TransportSelectorConfig::from_json(&json).is_err());
    }
    for field in ["transport_index", "compression", "local_recompute"] {
        let error = TransportSelectorConfig::from_json(&format!(
            r#"{{"policy":[{{"name":"bulk","segment_type":"memory","{field}":1}}]}}"#
        ))
        .unwrap_err()
        .to_string();
        assert!(error.contains("bulk") && error.contains(field), "{error}");
    }
    Ok(())
}

#[test]
fn named_tcp_and_intent_reach_sync_and_async_owners_before_any_charge() -> Result<()> {
    let source = TransferEngine::new(EngineConfig::default())?;
    let remote = source.register_memory(
        Vec::from(*b"original").into_boxed_slice(),
        Permission::RemoteReadWrite,
    )?;
    let engine = Arc::new(TransferEngine::new(EngineConfig {
        selector: Some(TransportSelectorConfig::from_json(
            r#"{"policy":[{"name":"copy","segment_type":"memory","intent_type":"migration","transports":["tcp"]},{"name":"other","segment_type":"memory","transports":["rdma"]}]}"#,
        )?),
        ..Default::default()
    })?);
    let local = engine.register_memory(
        Vec::from(*b"selected").into_boxed_slice(),
        Permission::LocalOnly,
    )?;
    let peer = engine.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    let req = TransferRequest::new(
        Transport::Unspec,
        local.base_address(),
        remote.base_address(),
        8,
    );
    assert!(
        TransferRequest::new(Transport::Tcp, 0, 0, 1)
            .with_policy_name("")
            .is_err()
    );
    assert!(req.clone().with_policy_name("a".repeat(257)).is_err());
    let named = req
        .clone()
        .with_intent(IntentType::Checkpoint)
        .with_policy_name("copy")?;
    let mut batch = engine.allocate_batch(2)?;
    let rejected = batch.submit(
        TransferOp::Write,
        &peer,
        &[named.clone(), req.clone().with_policy_name("absent")?],
        None,
        TIMEOUT,
    );
    assert!(
        matches!(rejected,Err(Error::TransportPolicy {policy,reason:"unknown policy"}) if policy.as_str()=="absent")
    );
    assert_eq!(engine.stats()?.inflight_batches, 0);
    let mut bytes = [0; 8];
    remote.read(0, &mut bytes)?;
    assert_eq!(&bytes, b"original");
    assert!(
        matches!(engine.submit_and_wait(TransferOp::Write,&peer,std::slice::from_ref(&req),TIMEOUT), Err(Error::TransportPolicy { policy,.. }) if policy.as_str()=="other")
    );
    assert!(
        engine
            .submit_and_wait(TransferOp::Write, &peer, &[named], TIMEOUT)?
            .is_complete()
    );
    local.write(0, b"XXXXXXXX")?;
    assert_eq!(
        batch.submit(
            TransferOp::Read,
            &peer,
            &[req.with_intent(IntentType::Migration)],
            None,
            TIMEOUT
        )?,
        0..1
    );
    batch.seal()?;
    assert_eq!(batch.wait(TIMEOUT)?.completed_tasks, 1);
    local.read(0, &mut bytes)?;
    assert_eq!(&bytes, b"selected");
    batch.free()?;
    Ok(())
}
