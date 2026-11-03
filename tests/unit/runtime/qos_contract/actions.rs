use super::*;
use crate::{CreditAmount, CreditResource, ReceiverCreditUpdateV1, TaskState};

fn config(credits: usize) -> EngineConfig {
    EngineConfig {
        qos: QosConfig::from_json(
            r#"{"qos":{
            "defaults":{"priority":"high", "max_inflight_bytes":1,
                "max_inflight_requests":1, "allowed_degraded_actions":["delay","reject"]},
            "tenants":[{"name":"a"}, {"name":"b"},
                {"name":"r", "defaults":{"allowed_degraded_actions":["reject"]}}]
        }}"#,
        )
        .unwrap(),
        runtime_queue: Some(RuntimeQueueConfig {
            max_dispatch_owners: 4,
            max_dispatch_bytes: 8,
            deadline_aware: true,
        }),
        max_receiver_credit_sessions: credits,
        ..Default::default()
    }
}

fn policy(engine: &TransferEngine, tenant: &str) -> EffectiveQosPolicy {
    engine
        .resolve_qos(&context(tenant, "unspec", Priority::High))
        .unwrap()
}

#[test]
fn public_qos_delay_preserves_batch_heads_and_actual_owner_release() {
    let mut invalid = config(0);
    invalid.runtime_queue = None;
    assert!(matches!(
        TransferEngine::new(invalid),
        Err(Error::InvalidConfig(_))
    ));
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let mut config = config(0);
    config.runtime_queue.as_mut().unwrap().max_dispatch_bytes = 2;
    config.qos.as_mut().unwrap().tenants[0]
        .defaults
        .allowed_degraded_actions = Some(QosDegradedActions {
        delay: false,
        reject: true,
    });
    let requester = Arc::new(TransferEngine::new(config).unwrap());
    let remote = source
        .register_memory(vec![0; 4].into_boxed_slice(), Permission::RemoteWrite)
        .unwrap();
    let local = requester
        .register_memory(vec![7, 9].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let a = policy(&requester, "a");
    let b = policy(&requester, "b");
    let make = |offset, target, qos: &EffectiveQosPolicy| {
        request(
            local.base_address() + offset,
            remote.base_address() + target,
            Some(qos.clone()),
        )
    };
    let held = requester
        .prepare(
            TransferOp::Write,
            &peer,
            &[make(0, 3, &b)],
            None,
            TIMEOUT,
            None,
        )
        .unwrap();
    let mut batch = requester.allocate_batch(3).unwrap();
    batch
        .submit(
            TransferOp::Write,
            &peer,
            &[make(0, 0, &a), make(0, 1, &b)],
            None,
            TIMEOUT,
        )
        .unwrap();
    // The later group has the earlier EDF deadline and available A quota, but
    // may not take A away from its waiting [A, B] predecessor.
    batch
        .submit(
            TransferOp::Write,
            &peer,
            &[make(1, 0, &a)],
            None,
            Duration::from_secs(2),
        )
        .unwrap();
    batch.seal().unwrap();
    assert_eq!(a.usage().unwrap(), QosUsage::default());
    assert_eq!(b.usage().unwrap().owners, 1);
    let stats = requester.stats().unwrap();
    assert_eq!(stats.runtime_queue.unwrap().queued_owners, 2);
    assert!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .all(|task| task.state == TaskState::Waiting)
    );
    // Independent work bypasses the QoS-blocked head without taking its slots.
    assert!(
        requester
            .submit_and_wait(TransferOp::Write, &peer, &[make(0, 2, &a)], TIMEOUT)
            .unwrap()
            .is_complete()
    );
    assert!(requester.execute(&held, None).is_complete());
    assert_eq!(
        b.usage().unwrap().owners,
        1,
        "logical completion must not release the actual owner"
    );
    assert_eq!(batch.status().unwrap().completed_tasks, 0);
    drop(held);
    assert_eq!(batch.wait(TIMEOUT).unwrap().state, BatchState::Completed);
    let mut bytes = [0; 4];
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [9, 7, 7, 7]);
    batch.free().unwrap();
    assert_eq!(a.usage().unwrap(), QosUsage::default());
    assert_eq!(b.usage().unwrap(), QosUsage::default());
    assert_eq!(requester.stats().unwrap().inflight_batches, 0);
}

#[test]
fn public_qos_delay_combines_credit_reject_cancel_and_deadline_admission() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let requester = Arc::new(TransferEngine::new(config(1)).unwrap());
    let remote = source
        .register_memory(vec![0; 3].into_boxed_slice(), Permission::RemoteWrite)
        .unwrap();
    let local = requester
        .register_memory(vec![5].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    requester.activate_receiver_credit(&peer, 1, 0).unwrap();
    let a = policy(&requester, "a");
    let b = policy(&requester, "b");
    let r = policy(&requester, "r");
    let make = |target, qos: &EffectiveQosPolicy| {
        request(
            local.base_address(),
            remote.base_address() + target,
            Some(qos.clone()),
        )
    };
    let grant = |sequence, total| {
        requester
            .apply_receiver_credit(
                &peer,
                &ReceiverCreditUpdateV1 {
                    schema_version: 1,
                    flags: 0,
                    receiver_session_id: peer.source_epoch(),
                    qos_class: 0,
                    epoch: 1,
                    sequence,
                    freshness_ttl_ms: 0,
                    grants: vec![
                        CreditAmount {
                            resource: CreditResource::DataBytes,
                            grant_total: total,
                        },
                        CreditAmount {
                            resource: CreditResource::RequestSlots,
                            grant_total: total,
                        },
                    ],
                },
            )
            .unwrap()
    };
    // Both reject-only groups initially fit QoS but have no receiver credit.
    // Neither can reserve QoS until the same dispatch decision acquires credit.
    let held_r = requester
        .prepare(
            TransferOp::Write,
            &peer,
            &[make(0, &r)],
            None,
            TIMEOUT,
            None,
        )
        .unwrap();
    let mut rejected = requester
        .submit_async(TransferOp::Write, &peer, &[make(0, &r)], None, TIMEOUT)
        .unwrap();
    assert_eq!(r.usage().unwrap(), QosUsage::default());
    assert_eq!(
        requester
            .stats()
            .unwrap()
            .runtime_queue
            .unwrap()
            .dispatching_owners,
        0
    );
    grant(1, 2);
    assert_eq!(rejected.wait(TIMEOUT).unwrap().state, BatchState::Failed);
    let failure = rejected.task_status(0).unwrap().failure.unwrap();
    assert!(matches!(failure.error, Error::ResourceExhausted));
    assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
    rejected.free().unwrap();
    assert_eq!(r.usage().unwrap().owners, 1);
    assert_eq!(
        requester
            .receiver_credit_stats(&peer)
            .unwrap()
            .consumed_bytes,
        1
    );

    let held_a = requester
        .prepare(
            TransferOp::Write,
            &peer,
            &[make(1, &a)],
            None,
            TIMEOUT,
            None,
        )
        .unwrap();
    let mut batch = requester.allocate_batch(2).unwrap();
    for requests in [
        vec![make(1, &a), make(1, &a)],
        vec![make(1, &a), make(0, &r)],
    ] {
        // Intrinsically oversized groups and a saturated reject slot cannot be
        // hidden by another slot allowing delay. No partial charge or task IDs.
        assert!(matches!(
            batch.submit(TransferOp::Write, &peer, &requests, None, TIMEOUT),
            Err(Error::ResourceExhausted)
        ));
        assert_eq!(batch.status().unwrap().submitted_tasks, 0);
    }
    batch.seal().unwrap();
    batch.free().unwrap();
    let mut waiting = requester
        .submit_async(TransferOp::Write, &peer, &[make(1, &a)], None, TIMEOUT)
        .unwrap();
    grant(2, 4);
    assert_eq!(
        requester
            .receiver_credit_stats(&peer)
            .unwrap()
            .consumed_bytes,
        2,
        "QoS wait cannot consume receiver credit"
    );
    assert!(
        requester
            .submit_and_wait(TransferOp::Write, &peer, &[make(2, &b)], TIMEOUT)
            .unwrap()
            .is_complete()
    );
    assert_eq!(
        requester
            .receiver_credit_stats(&peer)
            .unwrap()
            .consumed_bytes,
        3
    );
    waiting.cancel(0).unwrap();
    assert_eq!(waiting.wait(TIMEOUT).unwrap().state, BatchState::Cancelled);
    waiting.free().unwrap();
    let mut expired = requester
        .submit_async(
            TransferOp::Write,
            &peer,
            &[make(1, &a)],
            None,
            Duration::from_millis(20),
        )
        .unwrap();
    assert_eq!(expired.wait(TIMEOUT).unwrap().state, BatchState::Failed);
    assert!(matches!(
        expired.task_status(0).unwrap().failure.unwrap().error,
        Error::Timeout
    ));
    expired.free().unwrap();
    assert_eq!(a.usage().unwrap().owners, 1);
    assert_eq!(r.usage().unwrap().owners, 1);
    let mut bytes = [0; 3];
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0, 0, 5]);
    drop((held_a, held_r));
    assert_eq!(
        requester
            .receiver_credit_stats(&peer)
            .unwrap()
            .consumed_bytes,
        1
    );
    assert_eq!(a.usage().unwrap(), QosUsage::default());
    assert_eq!(r.usage().unwrap(), QosUsage::default());
    assert_eq!(requester.stats().unwrap().inflight_batches, 0);
}

#[test]
fn retired_qos_owner_cannot_be_recharged_before_queue_metadata_drops() {
    let queue = crate::runtime::admission_queue::LocalTransferAdmissionQueue::new(
        RuntimeQueueConfig::default(),
    );
    let config = config(0).qos.unwrap();
    let resolver = QosContractResolver::new(&config, Some(&queue)).unwrap();
    let policy = QosContractResolver::resolve(
        Some(&resolver),
        SourceEpoch([1; 16]),
        &QosRequestContext::default(),
    )
    .unwrap();
    let mut charges = Vec::new();
    QosCharge::add(&mut charges, resolver.default_contract(), 1).unwrap();
    let deadline = std::time::Instant::now() + TIMEOUT;
    let running = resolver.admit(charges.clone()).unwrap().unwrap();
    let running_queue = queue
        .try_admit(1, deadline, None, None, running.reservation())
        .unwrap();
    let pending = resolver.admit(charges).unwrap().unwrap();
    let pending_queue = queue
        .try_admit(1, deadline, None, None, pending.reservation())
        .unwrap();
    assert_eq!(policy.usage().unwrap().owners, 1);
    // Reproduce an external quota/credit wake between the real permit's Drop
    // and removal of its queue metadata after cancellation or expiry.
    drop(pending);
    drop(running);
    assert_eq!(policy.usage().unwrap(), QosUsage::default());
    assert_eq!(queue.stats().dispatching_owners, 1);
    drop((pending_queue, running_queue));
    assert_eq!(policy.usage().unwrap(), QosUsage::default());
    assert_eq!(queue.stats(), crate::RuntimeQueueStats::default());
}
