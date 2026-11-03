use std::time::Duration;

use super::*;
use crate::{
    BatchState, EngineConfig, Permission, RuntimeQueueConfig, TransferEngine, TransferOp,
    TransferRequest, Transport,
};

const TIMEOUT: Duration = Duration::from_secs(3);

fn context(tenant: &str, intent: &str, priority: Priority) -> QosRequestContext {
    QosRequestContext {
        tenant_id: tenant.into(),
        intent: intent.into(),
        requested_priority: priority,
    }
}

#[test]
fn tent_layers_override_fields_and_compile_bounded_shared_slots() {
    let config = QosConfig {
        defaults: QosPolicyFields {
            priority: Some(Priority::Medium),
            max_inflight_requests: Some(4),
            ..Default::default()
        },
        tenants: vec![QosTenantContract {
            name: " Tenant-A ".into(),
            defaults: QosPolicyFields {
                max_inflight_bytes: Some(8),
                ..Default::default()
            },
            intents: BTreeMap::from([
                (
                    "Foreground_Get".into(),
                    QosPolicyFields {
                        priority: Some(Priority::High),
                        max_inflight_bytes: Some(16),
                        ..Default::default()
                    },
                ),
                (
                    "blocked".into(),
                    QosPolicyFields {
                        max_inflight_requests: Some(0),
                        ..Default::default()
                    },
                ),
            ]),
        }],
        ..Default::default()
    };
    let resolver = QosContractResolver::new(&config, None).unwrap();
    let owner = SourceEpoch([1; 16]);
    for (tenant, intent, name, priority, bytes, requests, matched) in [
        (
            "TENANT-A",
            " foreground_get ",
            "tenant-a.foreground_get",
            Priority::High,
            Some(16),
            Some(4),
            true,
        ),
        (
            "tenant-a",
            "unknown",
            "tenant-a.default",
            Priority::Medium,
            Some(8),
            Some(4),
            false,
        ),
        (
            "missing",
            "foreground_get",
            "global_default",
            Priority::Medium,
            None,
            Some(4),
            false,
        ),
        (
            "tenant-a",
            "blocked",
            "tenant-a.blocked",
            Priority::Medium,
            Some(8),
            Some(0),
            true,
        ),
    ] {
        let policy = QosContractResolver::resolve(
            Some(&resolver),
            owner,
            &context(tenant, intent, Priority::Low),
        )
        .unwrap();
        assert_eq!(
            (
                policy.matched_contract(),
                policy.effective_priority(),
                policy.matched()
            ),
            (name, priority, matched)
        );
        assert_eq!(
            (
                policy.fields().max_inflight_bytes,
                policy.fields().max_inflight_requests
            ),
            (bytes, requests)
        );
    }
    let disabled =
        QosContractResolver::resolve(None, owner, &context("  ", "", Priority::Low)).unwrap();
    assert!(!disabled.enabled());
    assert_eq!(
        (
            disabled.tenant_id(),
            disabled.intent(),
            disabled.matched_contract()
        ),
        ("default", "unspec", "compatibility_default")
    );
    assert_eq!(disabled.effective_priority(), Priority::Low);
    assert_eq!(disabled.usage(), None);
    assert!(matches!(
        disabled.contract(SourceEpoch([2; 16])),
        Err(Error::ForeignQosPolicy)
    ));
    for invalid in 0..5 {
        let mut config = config.clone();
        match invalid {
            0 => config.version = 2,
            1 => config.tenants.push(config.tenants[0].clone()),
            2 => {
                config.tenants[0]
                    .intents
                    .insert(" foreground_GET ".into(), QosPolicyFields::default());
            }
            3 => config.tenants[0].name = " ".into(),
            4 => config.tenants[0].name = "x".repeat(257),
            _ => unreachable!(),
        }
        assert!(
            QosContractResolver::new(&config, None).is_err(),
            "case {invalid}"
        );
    }
    let excessive = QosConfig {
        tenants: (0..4096)
            .map(|i| QosTenantContract {
                name: i.to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    assert!(QosContractResolver::new(&excessive, None).is_err());
}

fn request(local: u64, remote: u64, policy: Option<EffectiveQosPolicy>) -> TransferRequest {
    TransferRequest::new(Transport::Tcp, local, remote, 1)
        .with_qos(policy)
        .with_priority(Priority::Low)
}

#[test]
fn public_mixed_contract_admission_is_atomic_and_repeated_resolution_cannot_bypass_caps() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            qos: QosConfig::from_json(r#"{
                "qos": {
                    "defaults": {"priority":"high", "max_inflight_bytes":"2B", "max_inflight_requests":1},
                    "tenants": [
                        {"name":"a", "defaults":{"max_inflight_requests":2}},
                        {"name":"b"}
                    ]
                }
            }"#).unwrap(),
            runtime_queue: Some(RuntimeQueueConfig {
                max_dispatch_owners: 1,
                max_dispatch_bytes: 8,
                deadline_aware: false,
            }),
            ..Default::default()
        })
        .unwrap(),
    );
    let local = requester
        .register_memory(vec![7].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(vec![0; 3].into_boxed_slice(), Permission::RemoteWrite)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let a = requester
        .resolve_qos(&context("a", "first-unknown", Priority::Low))
        .unwrap();
    let a_again = requester
        .resolve_qos(&context(" A ", "other-unknown", Priority::Medium))
        .unwrap();
    let b = requester
        .resolve_qos(&context("b", "unspec", Priority::Low))
        .unwrap();
    let requests = [
        request(local.base_address(), remote.base_address(), Some(a.clone())),
        request(
            local.base_address(),
            remote.base_address() + 1,
            Some(b.clone()),
        ),
        request(
            local.base_address(),
            remote.base_address() + 2,
            Some(a_again.clone()),
        ),
    ];
    let held = requester
        .prepare(
            TransferOp::Write,
            &peer,
            &requests[1..2],
            None,
            TIMEOUT,
            None,
        )
        .unwrap();
    let mut batch = requester.allocate_batch(3).unwrap();
    assert!(matches!(
        batch.submit(TransferOp::Write, &peer, &requests, None, TIMEOUT),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(batch.status().unwrap().submitted_tasks, 0);
    assert_eq!(a.usage().unwrap(), QosUsage::default());
    assert_eq!(a.usage(), a_again.usage());
    assert_eq!(
        b.usage().unwrap(),
        QosUsage {
            owners: 1,
            inflight_bytes: 1,
            inflight_requests: 1,
        }
    );
    assert_eq!(requester.stats().unwrap().inflight_batches, 1);
    drop(held);
    let prepared = requester
        .prepare(TransferOp::Write, &peer, &requests, None, TIMEOUT, None)
        .unwrap();
    assert_eq!(
        a.usage().unwrap(),
        QosUsage {
            owners: 1,
            inflight_bytes: 2,
            inflight_requests: 2
        }
    );
    assert_eq!(b.usage().unwrap().inflight_requests, 1);
    let report = requester.execute(&prepared, None);
    assert_eq!(report.completed_requests, 3);
    assert_eq!(a.usage().unwrap().inflight_requests, 2); // Actual owner still retained.
    drop(prepared);
    let mut bytes = [0; 3];
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7; 3]);
    assert_eq!(a.usage().unwrap(), QosUsage::default());
    assert_eq!(b.usage().unwrap(), QosUsage::default());

    let g1 = requester
        .resolve_qos(&context("missing-a", "one", Priority::Low))
        .unwrap();
    let g2 = requester
        .resolve_qos(&context("missing-b", "two", Priority::High))
        .unwrap();
    let mut global = request(
        local.base_address(),
        remote.base_address(),
        Some(g1.clone()),
    );
    let held = requester
        .prepare(
            TransferOp::Write,
            &peer,
            &[global.clone()],
            None,
            TIMEOUT,
            None,
        )
        .unwrap();
    global.qos = Some(g2.clone());
    assert!(matches!(
        requester.submit_and_wait(TransferOp::Write, &peer, &[global.clone()], TIMEOUT),
        Err(Error::ResourceExhausted)
    ));
    global.qos = None; // Implicit default/unspec is the same global budget here.
    assert!(matches!(
        requester.submit_and_wait(TransferOp::Write, &peer, &[global.clone()], TIMEOUT),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(g1.usage(), g2.usage());
    drop(held);
    assert!(
        requester
            .submit_and_wait(TransferOp::Write, &peer, &[global.clone()], TIMEOUT)
            .unwrap()
            .is_complete()
    );
    global.qos = Some(source.resolve_qos(&QosRequestContext::default()).unwrap());
    assert!(matches!(
        requester.submit_and_wait(TransferOp::Write, &peer, &[global], TIMEOUT),
        Err(Error::ForeignQosPolicy)
    ));
    batch.seal().unwrap();
    batch.free().unwrap();
}

#[test]
fn implicit_default_intent_enforces_hard_zero_and_effective_transport_priority() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let remote = source
        .register_memory(vec![0].into_boxed_slice(), Permission::RemoteWrite)
        .unwrap();
    for (bytes, requests, priority) in [
        (Some(0), None, Priority::High),
        (None, Some(0), Priority::High),
        (None, None, Priority::Low),
    ] {
        let requester = TransferEngine::new(EngineConfig {
            qos: Some(QosConfig {
                tenants: vec![QosTenantContract {
                    name: "DEFAULT".into(),
                    intents: BTreeMap::from([(
                        " UNSPEC ".into(),
                        QosPolicyFields {
                            priority: Some(priority),
                            max_inflight_bytes: bytes,
                            max_inflight_requests: requests,
                            ..Default::default()
                        },
                    )]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        let local = requester
            .register_memory(vec![9].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        let mut request = request(local.base_address(), remote.base_address(), None);
        request.priority = Priority::High;
        let result = requester.submit_and_wait(TransferOp::Write, &peer, &[request], TIMEOUT);
        if priority == Priority::Low {
            assert!(matches!(
                result,
                Err(Error::Unavailable("request priority requires RDMA"))
            ));
        } else {
            assert!(matches!(result, Err(Error::ResourceExhausted)));
        }
        assert_eq!(requester.stats().unwrap().inflight_batches, 0);
        let policy = requester
            .resolve_qos(&context(" ", "", Priority::High))
            .unwrap();
        assert_eq!(policy.matched_contract(), "default.unspec");
        assert_eq!(
            policy.usage(),
            bytes.or(requests).map(|_| QosUsage::default())
        );
    }
    let mut bytes = [0];
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0]);
}

#[test]
fn queued_credit_waits_retain_qos_and_clean_failures_return_only_inflight_budget() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            qos: Some(QosConfig {
                defaults: QosPolicyFields {
                    max_inflight_bytes: Some(1),
                    max_inflight_requests: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            }),
            max_receiver_credit_sessions: 1,
            runtime_queue: Some(RuntimeQueueConfig {
                max_dispatch_owners: 1,
                max_dispatch_bytes: 8,
                deadline_aware: false,
            }),
            ..Default::default()
        })
        .unwrap(),
    );
    let remote = source
        .register_memory(vec![0].into_boxed_slice(), Permission::RemoteWrite)
        .unwrap();
    let local = requester
        .register_memory(vec![9].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let policy = requester
        .resolve_qos(&QosRequestContext::default())
        .unwrap();
    let requests = [request(
        local.base_address(),
        remote.base_address(),
        Some(policy.clone()),
    )];
    // Later preparation failure must also release a previously reserved QoS charge.
    assert!(matches!(
        requester.submit_async(TransferOp::Write, &peer, &requests, None, TIMEOUT),
        Err(Error::Unavailable(_))
    ));
    assert_eq!(policy.usage().unwrap(), QosUsage::default());
    requester.activate_receiver_credit(&peer, 1, 0).unwrap();
    let mut waiting = requester
        .submit_async(TransferOp::Write, &peer, &requests, None, TIMEOUT)
        .unwrap();
    assert_eq!(policy.usage().unwrap().inflight_requests, 1);
    assert!(matches!(
        requester.submit_and_wait(TransferOp::Write, &peer, &requests, TIMEOUT),
        Err(Error::ResourceExhausted)
    ));
    waiting.cancel(0).unwrap();
    assert_eq!(waiting.wait(TIMEOUT).unwrap().state, BatchState::Cancelled);
    waiting.free().unwrap();
    assert_eq!(policy.usage().unwrap(), QosUsage::default());
    let mut expired = requester
        .submit_async(
            TransferOp::Write,
            &peer,
            &requests,
            None,
            Duration::from_millis(20),
        )
        .unwrap();
    assert_eq!(expired.wait(TIMEOUT).unwrap().state, BatchState::Failed);
    expired.free().unwrap();
    assert_eq!(policy.usage().unwrap(), QosUsage::default());
    requester
        .apply_receiver_credit(
            &peer,
            &crate::ReceiverCreditUpdateV1 {
                schema_version: 1,
                flags: 0,
                receiver_session_id: peer.source_epoch(),
                qos_class: 0,
                epoch: 1,
                sequence: 1,
                freshness_ttl_ms: 0,
                grants: vec![
                    crate::CreditAmount {
                        resource: crate::CreditResource::DataBytes,
                        grant_total: 1,
                    },
                    crate::CreditAmount {
                        resource: crate::CreditResource::RequestSlots,
                        grant_total: 1,
                    },
                ],
            },
        )
        .unwrap();
    source.close_source(TIMEOUT).unwrap();
    let failed = requester
        .submit_and_wait(TransferOp::Write, &peer, &requests, TIMEOUT)
        .unwrap();
    assert!(failed.failure.is_some() && failed.locally_drained);
    assert_eq!(policy.usage().unwrap(), QosUsage::default());
    assert_eq!(
        requester
            .receiver_credit_stats(&peer)
            .unwrap()
            .consumed_bytes,
        1
    );
}

#[path = "qos_contract/actions.rs"]
mod actions;
