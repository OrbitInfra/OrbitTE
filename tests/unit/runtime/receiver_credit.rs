use super::*;
use crate::{
    BatchState, EngineConfig, Permission, RuntimeQueueConfig, RuntimeQueueStats, TransferEngine,
    TransferOp, TransferRequest, Transport,
};

const TIMEOUT: Duration = Duration::from_secs(3);

fn peer() -> RemoteSegment {
    RemoteSegment {
        endpoint: "127.0.0.1:1234".parse().unwrap(),
        epoch: SourceEpoch([1; 16]),
        regions: Vec::new(),
    }
}

fn update(
    peer: &RemoteSegment,
    sequence: u64,
    bytes: u64,
    requests: u64,
) -> ReceiverCreditUpdateV1 {
    ReceiverCreditUpdateV1 {
        schema_version: 1,
        flags: 0,
        receiver_session_id: peer.source_epoch(),
        qos_class: 7,
        epoch: 1,
        sequence,
        freshness_ttl_ms: 0,
        grants: vec![
            CreditAmount {
                resource: CreditResource::DataBytes,
                grant_total: bytes,
            },
            CreditAmount {
                resource: CreditResource::RequestSlots,
                grant_total: requests,
            },
        ],
    }
}

#[test]
fn cumulative_updates_reserve_atomically_and_refresh_only_on_a_new_sequence() {
    let peer = peer();
    let ledger = SenderCreditLedger::new(1, None);
    ledger.activate(&peer, 1, 7).unwrap();
    let permit = ledger.prepare(&peer, 5, 1).unwrap();
    ledger.apply_update(&peer, &update(&peer, 1, 5, 0)).unwrap();
    assert!(!permit.reservation.try_reserve().unwrap());
    assert_eq!(ledger.stats(&peer).unwrap().consumed_bytes, 0);
    let mut partial = update(&peer, 3, 0, 1);
    partial.grants.remove(0);
    assert_eq!(
        ledger.apply_update(&peer, &partial).unwrap(),
        CreditUpdateDisposition::SequenceGap
    );
    assert!(permit.reservation.try_reserve().unwrap());
    assert!(permit.reservation.try_reserve().unwrap());
    permit.reservation.commit().unwrap();
    drop(permit);
    let consumed = ledger.stats(&peer).unwrap();
    assert_eq!(
        (consumed.consumed_bytes, consumed.consumed_requests),
        (5, 1)
    );
    assert_eq!(consumed.active_owners, 0);
    for sequence in [1, 3] {
        assert_eq!(
            ledger
                .apply_update(&peer, &update(&peer, sequence, 99, 99))
                .unwrap(),
            CreditUpdateDisposition::DuplicateOrOld
        );
        assert_eq!(ledger.stats(&peer).unwrap(), consumed);
    }
    for invalid in 0..9 {
        let mut next = update(&peer, 4, 10, 2);
        match invalid {
            0 => next.schema_version = 2,
            1 => next.flags = 1,
            2 => next.receiver_session_id = SourceEpoch([2; 16]),
            3 => next.epoch = 2,
            4 => next.qos_class = 8,
            5 => next.grants[1].grant_total = 0,
            6 => next.grants.push(next.grants[0]),
            7 => next.grants.push(CreditAmount {
                resource: CreditResource::StagingSlots,
                grant_total: 1,
            }),
            8 => next.grants.push(CreditAmount {
                resource: CreditResource::ConsumerSlots,
                grant_total: 1,
            }),
            _ => unreachable!(),
        }
        assert!(
            ledger.apply_update(&peer, &next).is_err(),
            "invalid case {invalid}"
        );
        assert_eq!(
            ledger.stats(&peer).unwrap(),
            consumed,
            "partial mutation {invalid}"
        );
    }
    let mut fresh = update(&peer, 4, 10, 2);
    fresh.freshness_ttl_ms = 1000;
    ledger.apply_update(&peer, &fresh).unwrap();
    let reserved = ledger.prepare(&peer, 5, 1).unwrap();
    assert!(reserved.reservation.try_reserve().unwrap());
    ledger
        .lock()
        .entries
        .get_mut(&CreditKey::from(&peer))
        .unwrap()
        .expires = Some(Instant::now());
    assert_eq!(
        ledger.apply_update(&peer, &fresh).unwrap(),
        CreditUpdateDisposition::DuplicateOrOld
    );
    ledger.activate(&peer, 1, 7).unwrap();
    assert!(!ledger.stats(&peer).unwrap().fresh);
    reserved.reservation.commit().unwrap(); // Freshness does not revoke a reservation.
    drop(reserved);
    let waiting = ledger.prepare(&peer, 1, 1).unwrap();
    assert!(!waiting.reservation.try_reserve().unwrap());
    fresh.sequence = 5;
    fresh.freshness_ttl_ms = 0;
    fresh.grants.clear(); // A new heartbeat refreshes without resetting consumption.
    ledger.apply_update(&peer, &fresh).unwrap();
    assert!(ledger.stats(&peer).unwrap().fresh);
    assert!(!waiting.reservation.try_reserve().unwrap());
    assert_eq!(ledger.stats(&peer).unwrap().consumed_bytes, 10);
    assert!(matches!(
        ledger.activate(&peer, 2, 7),
        Err(Error::BatchBusy)
    ));
    assert!(matches!(ledger.deactivate(&peer, 1), Err(Error::BatchBusy)));
    assert!(matches!(
        waiting.reservation.try_reserve(),
        Err(Error::Cancelled)
    ));
    assert!(matches!(
        ledger.activate(&peer, 1, 7),
        Err(Error::Cancelled)
    ));
    drop(waiting);
    ledger.activate(&peer, 2, 8).unwrap();
    assert!(matches!(
        ledger.deactivate(&peer, 1),
        Err(Error::EpochMismatch)
    ));
    assert_eq!(ledger.stats(&peer).unwrap().consumed_bytes, 0);
    let mut restarted = peer.clone();
    restarted.epoch = SourceEpoch([2; 16]);
    assert!(matches!(
        ledger.prepare(&restarted, 1, 1),
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        ledger.activate(&restarted, 1, 7),
        Err(Error::ResourceExhausted)
    ));
    ledger.deactivate(&peer, 2).unwrap();
    ledger.activate(&restarted, 1, 7).unwrap();
}

#[test]
fn credit_eligible_large_head_keeps_byte_order_without_reserving_early() {
    let peer = peer();
    let queue = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
        max_dispatch_owners: 2,
        max_dispatch_bytes: 8,
        deadline_aware: false,
    });
    let ledger = SenderCreditLedger::new(2, Some(&queue));
    ledger.activate(&peer, 1, 7).unwrap();
    ledger
        .apply_update(&peer, &update(&peer, 1, 20, 10))
        .unwrap();
    let prepare = |peer: &RemoteSegment, bytes| {
        let credit = ledger.prepare(peer, bytes, 1).unwrap();
        let queued = queue
            .try_admit(
                bytes,
                Instant::now() + TIMEOUT,
                None,
                Some(Arc::clone(&credit.reservation)),
            )
            .unwrap();
        (queued, credit) // Same release order as PreparedSubmission.
    };
    let held = prepare(&peer, 5);
    let large = prepare(&peer, 4);
    let small = prepare(&peer, 3);
    assert_eq!(queue.stats().dispatching_owners, 1);
    assert_eq!(ledger.stats(&peer).unwrap().consumed_bytes, 5);
    drop(held);
    assert_eq!(queue.stats().dispatching_bytes, 7);
    assert_eq!(ledger.stats(&peer).unwrap().consumed_bytes, 7);
    large.0.wait(|| false).unwrap();
    small.0.wait(|| false).unwrap();
    drop((large, small));
    assert_eq!(queue.stats(), RuntimeQueueStats::default());
    assert_eq!(ledger.stats(&peer).unwrap().consumed_bytes, 0);

    let mut short = peer.clone();
    short.epoch = SourceEpoch([2; 16]);
    ledger.activate(&short, 1, 7).unwrap();
    ledger
        .apply_update(&short, &update(&short, 1, 4, 1))
        .unwrap();
    let held = prepare(&peer, 5);
    let large = prepare(&short, 4);
    let small = prepare(&peer, 3);
    assert_eq!(queue.stats().dispatching_bytes, 5);
    // Deterministic setup, then the actual Condvar must wake on TTL alone.
    ledger
        .lock()
        .entries
        .get_mut(&CreditKey::from(&short))
        .unwrap()
        .expires = Some(Instant::now() + Duration::from_millis(30));
    ledger.notify();
    small.0.wait(|| false).unwrap();
    assert_eq!(queue.stats().dispatching_bytes, 8);
    assert_eq!(ledger.stats(&short).unwrap().consumed_bytes, 0);
    drop((held, large, small));
    assert_eq!(queue.stats(), RuntimeQueueStats::default());
    assert_eq!(ledger.stats(&peer).unwrap().consumed_bytes, 0);
}

fn engine(queue: bool) -> Arc<TransferEngine> {
    Arc::new(
        TransferEngine::new(EngineConfig {
            max_receiver_credit_sessions: 2,
            runtime_queue: queue.then_some(RuntimeQueueConfig {
                max_dispatch_owners: 1,
                max_dispatch_bytes: 8,
                deadline_aware: true,
            }),
            ..EngineConfig::default()
        })
        .unwrap(),
    )
}

fn request(local: u64, remote: u64) -> TransferRequest {
    TransferRequest {
        priority: Default::default(),
        remote_staging: false,
        transport: Transport::Tcp,
        local_address: local,
        remote_address: remote,
        length: 1,
    }
}

#[test]
fn public_credit_wait_bypasses_ineligible_peers_and_updates_drain_after_close() {
    for queued in [false, true] {
        let requester = engine(queued);
        let a = TransferEngine::new(EngineConfig::default()).unwrap();
        let b = TransferEngine::new(EngineConfig::default()).unwrap();
        let ar = a
            .register_memory(vec![0].into_boxed_slice(), Permission::RemoteWrite)
            .unwrap();
        let br = b
            .register_memory(vec![0].into_boxed_slice(), Permission::RemoteWrite)
            .unwrap();
        let local = requester
            .register_memory(vec![42].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let ap = requester
            .open_segment(a.endpoint(), a.source_epoch(), TIMEOUT)
            .unwrap();
        let bp = requester
            .open_segment(b.endpoint(), b.source_epoch(), TIMEOUT)
            .unwrap();
        let aq = [request(local.base_address(), ar.base_address())];
        let bq = [request(local.base_address(), br.base_address())];
        assert!(matches!(
            requester.submit_async(TransferOp::Write, &ap, &aq, None, TIMEOUT),
            Err(Error::Unavailable(_))
        ));
        assert_eq!(requester.stats().unwrap().inflight_batches, 0);
        for peer in [&ap, &bp] {
            requester.activate_receiver_credit(peer, 1, 7).unwrap();
        }
        requester
            .apply_receiver_credit(&ap, &update(&ap, 1, 1, 0))
            .unwrap();
        requester
            .apply_receiver_credit(&bp, &update(&bp, 1, 1, 1))
            .unwrap();
        let mut waiting = requester
            .submit_async(TransferOp::Write, &ap, &aq, None, TIMEOUT)
            .unwrap();
        if queued {
            assert_eq!(
                requester
                    .stats()
                    .unwrap()
                    .runtime_queue
                    .unwrap()
                    .dispatching_owners,
                0
            );
        }
        let report = requester
            .submit_and_wait(TransferOp::Write, &bp, &bq, TIMEOUT)
            .unwrap();
        assert_eq!(report.completed_requests, 1);
        let mut bytes = [0];
        br.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [42]);
        ar.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0]);
        assert_eq!(
            requester
                .receiver_credit_stats(&bp)
                .unwrap()
                .available_requests,
            0
        );
        assert!(matches!(
            requester.close_requester(Duration::ZERO),
            Err(Error::Timeout)
        ));
        requester
            .apply_receiver_credit(&ap, &update(&ap, 2, 1, 1))
            .unwrap();
        assert_eq!(waiting.wait(TIMEOUT).unwrap().state, BatchState::Completed);
        waiting.free().unwrap();
        requester.close_requester(TIMEOUT).unwrap();
        ar.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [42]);
        for peer in [&ap, &bp] {
            let stats = requester.receiver_credit_stats(peer).unwrap();
            assert_eq!(
                (
                    stats.consumed_bytes,
                    stats.consumed_requests,
                    stats.active_owners
                ),
                (1, 1, 0)
            );
        }
    }
}

#[test]
fn cancel_expiry_and_fence_release_unstarted_credit_but_handoff_never_refunds() {
    for queued in [false, true] {
        let requester = engine(queued);
        let source = TransferEngine::new(EngineConfig::default()).unwrap();
        let remote = source
            .register_memory(vec![0; 2].into_boxed_slice(), Permission::RemoteWrite)
            .unwrap();
        let local = requester
            .register_memory(vec![42].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        let requests = [request(local.base_address(), remote.base_address())];
        requester.activate_receiver_credit(&peer, 1, 7).unwrap();
        let mut cancelled = requester
            .submit_async(TransferOp::Write, &peer, &requests, None, TIMEOUT)
            .unwrap();
        cancelled.cancel(0).unwrap();
        assert_eq!(
            cancelled.wait(TIMEOUT).unwrap().state,
            BatchState::Cancelled
        );
        cancelled.free().unwrap();
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
        assert!(matches!(
            expired.task_status(0).unwrap().failure.unwrap().error,
            Error::Timeout
        ));
        expired.free().unwrap();
        requester
            .apply_receiver_credit(&peer, &update(&peer, 1, 1, 1))
            .unwrap();
        let held = requester
            .prepare(TransferOp::Write, &peer, &requests, None, TIMEOUT, None)
            .unwrap();
        drop(held);
        assert_eq!(
            requester
                .receiver_credit_stats(&peer)
                .unwrap()
                .consumed_bytes,
            0
        );
        let held = requester
            .prepare(TransferOp::Write, &peer, &requests, None, TIMEOUT, None)
            .unwrap();
        assert!(matches!(
            requester.deactivate_receiver_credit(&peer, 1),
            Err(Error::BatchBusy)
        ));
        assert!(matches!(
            requester.activate_receiver_credit(&peer, 2, 7),
            Err(Error::BatchBusy)
        ));
        assert!(matches!(
            requester.execute(&held, None).failure.unwrap().error,
            Error::Cancelled
        ));
        drop(held);
        assert_eq!(
            requester
                .receiver_credit_stats(&peer)
                .unwrap()
                .consumed_bytes,
            0
        );
        requester.deactivate_receiver_credit(&peer, 1).unwrap();
        requester.activate_receiver_credit(&peer, 2, 7).unwrap();
        let mut grant = update(&peer, 1, 1, 1);
        grant.epoch = 2;
        requester.apply_receiver_credit(&peer, &grant).unwrap();
        let pair = [
            request(local.base_address(), remote.base_address()),
            request(local.base_address(), remote.base_address() + 1),
        ];
        let mut partial = requester
            .submit_async(TransferOp::Write, &peer, &pair, None, TIMEOUT)
            .unwrap();
        partial.cancel(1).unwrap(); // One grant cannot admit the two-request group yet.
        grant.sequence = 2;
        for amount in &mut grant.grants {
            amount.grant_total = 2;
        }
        requester.apply_receiver_credit(&peer, &grant).unwrap();
        assert_eq!(partial.wait(TIMEOUT).unwrap().state, BatchState::Cancelled);
        assert_eq!(
            partial.task_status(0).unwrap().state,
            crate::TaskState::Completed
        );
        partial.free().unwrap();
        let stats = requester.receiver_credit_stats(&peer).unwrap();
        assert_eq!(
            (
                stats.consumed_bytes,
                stats.consumed_requests,
                stats.active_owners
            ),
            (2, 2, 0)
        );
        let mut bytes = [0; 2];
        remote.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [42, 0]);
        grant.sequence = 3;
        for amount in &mut grant.grants {
            amount.grant_total = 3;
        }
        requester.apply_receiver_credit(&peer, &grant).unwrap();
        source.close_source(TIMEOUT).unwrap();
        let report = requester
            .submit_and_wait(TransferOp::Write, &peer, &requests, TIMEOUT)
            .unwrap();
        assert!(report.failure.is_some());
        assert!(report.locally_drained);
        assert_eq!(
            requester
                .receiver_credit_stats(&peer)
                .unwrap()
                .consumed_bytes,
            3
        );
        assert_eq!(
            requester
                .receiver_credit_stats(&peer)
                .unwrap()
                .active_owners,
            0
        );
        remote.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [42, 0]);
    }
}
