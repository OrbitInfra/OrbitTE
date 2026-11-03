use super::*;
use crate::runtime::deadline_mlu::deadline_mlu;
use std::sync::atomic::{AtomicU64, AtomicUsize};

fn input(
    bytes: usize,
    deadline: Option<Instant>,
    kind: QueueOwnerKind,
    rate: Option<Transport>,
) -> QueueInput {
    QueueInput {
        bytes,
        length: bytes,
        deadline: Instant::now() + crate::test_support::EVENTUAL_PROGRESS,
        scheduling_deadline: deadline,
        kind,
        rate: rate.map(|t| (t, u64::MAX)),
        credit: None,
        qos: None,
    }
}
fn user(bytes: usize) -> QueueInput {
    input(bytes, None, QueueOwnerKind::User, None)
}
fn stage(bytes: usize) -> QueueInput {
    input(bytes, None, QueueOwnerKind::StagingInternal, None)
}

#[test]
fn physical_limits_and_internal_reserves_are_atomic_on_count_and_bytes() {
    for config in [
        RuntimeQueueConfig {
            staging_owner_reserve: 2,
            max_outstanding_owners: 1,
            ..Default::default()
        },
        RuntimeQueueConfig {
            staging_byte_reserve: 2,
            max_outstanding_bytes: 1,
            ..Default::default()
        },
        RuntimeQueueConfig {
            mlu_local_threshold: f64::NAN,
            ..Default::default()
        },
    ] {
        assert!(matches!(
            TransferEngine::new(EngineConfig {
                runtime_queue: Some(config),
                ..Default::default()
            }),
            Err(Error::InvalidConfig(_))
        ));
    }
    let q = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
        max_outstanding_owners: 3,
        max_outstanding_bytes: 12,
        staging_owner_reserve: 1,
        staging_byte_reserve: 4,
        max_dispatch_owners: 1,
        ..Default::default()
    });
    assert!(q.admit(vec![], None).unwrap().is_empty());
    for invalid in [
        vec![user(1), user(0)],
        vec![user(5), user(4)],
        vec![user(1), user(1), user(1)],
        vec![stage(13)],
        vec![stage(1), stage(usize::MAX)],
    ] {
        assert!(matches!(
            q.admit(invalid, None),
            Err(Error::ResourceExhausted)
        ));
        assert_eq!(q.stats(), RuntimeQueueStats::default());
    }
    let users = q.admit(vec![user(4), user(4)], None).unwrap();
    assert!(matches!(
        q.admit(vec![user(1)], None),
        Err(Error::ResourceExhausted)
    ));
    let internal = q.admit(vec![stage(4)], None).unwrap();
    assert!(matches!(
        q.admit(vec![stage(1)], None),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        q.admit(vec![user(1)], None),
        Err(Error::ResourceExhausted)
    ));
    assert!(!q.cancel(&users[0].ticket)); // an active window has to drain
    assert!(q.cancel(&users[1].ticket));
    assert!(!q.cancel(&users[1].ticket));
    assert!(matches!(users[1].wait(|| false), Err(Error::Cancelled)));
    assert_eq!(q.stats().queued_bytes, 4);
    drop(users);
    internal[0].wait(|| false).unwrap();
    drop(internal);
    let all_internal = q.admit(vec![stage(12)], None).unwrap();
    assert!(matches!(
        q.admit(vec![user(1)], None),
        Err(Error::ResourceExhausted)
    ));
    drop(all_internal);
    assert_eq!(q.stats(), RuntimeQueueStats::default());
}

#[test]
fn physical_windows_charge_only_live_wire_bytes_and_use_each_transports_current_rate() {
    let q = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
        max_dispatch_owners: 4,
        max_dispatch_bytes: 128,
        deadline_aware: true,
        mlu_local_threshold: 1.0,
        ..Default::default()
    });
    let bandwidth = Arc::new(AtomicU64::new(8));
    let reads = Arc::new(AtomicUsize::new(0));
    let (rate, hits) = (bandwidth.clone(), reads.clone());
    q.set_bandwidth(Box::new(move |t, _| {
        hits.fetch_add(1, Ordering::Relaxed);
        if t == Transport::Rdma {
            rate.load(Ordering::Relaxed) as f64 / 1000.0
        } else {
            1e6
        }
    }))
    .unwrap();
    let now = Instant::now();
    let dl = Some(now + Duration::from_secs(1000));
    let item = |bytes, t| input(bytes, dl, QueueOwnerKind::User, Some(t));
    let mut first = item(4, Transport::Rdma);
    first.bytes += 32; // notification bytes are charged but do not cross the RDMA wire
    let p = q
        .admit(
            vec![first, item(1, Transport::Rdma), item(4, Transport::Rdma)],
            None,
        )
        .unwrap();
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    assert!(p[0].ticket.terminal().is_ok() && p[1].ticket.terminal().is_ok());
    assert!(matches!(
        p[2].wait(|| false),
        Err(Error::DeadlineInfeasible)
    ));
    assert_eq!(q.stats().dispatching_bytes, 37);
    assert_eq!(q.lock().eligible_bytes[Transport::Rdma as usize], 5);
    assert_eq!(q.stats().deadline_infeasible_drops, 1);
    let tcp = q.admit(vec![item(20, Transport::Tcp)], None).unwrap();
    tcp[0].wait(|| false).unwrap();
    bandwidth.store(100, Ordering::Relaxed);
    let healthy = q.admit(vec![item(4, Transport::Rdma)], None).unwrap();
    healthy[0].wait(|| false).unwrap();
    drop((healthy, tcp, p));
    assert_eq!(q.lock().eligible_bytes, [0; 6]);
    assert_eq!(q.stats().deadline_infeasible_drops, 1);
}

#[test]
fn deadline_prediction_needs_a_known_eligible_rate_and_positive_edf_threshold() {
    let now = Instant::now();
    let past = now - Duration::from_secs(1);
    assert_eq!(
        deadline_mlu(3, 2, Some(now + Duration::from_secs(1)), now, 5.0),
        1.0
    );
    assert_eq!(
        deadline_mlu(u64::MAX, usize::MAX, Some(past), now, 5.0),
        f64::MAX
    );
    assert_eq!(deadline_mlu(9, 9, None, now, 1.0), 0.0);
    for (edf, theta, rate, deadline, eligible, drop) in [
        (true, 1., Some(1.), Some(past), true, true),
        (false, 1., Some(1.), Some(past), true, false),
        (true, 0., Some(1.), Some(past), true, false),
        (true, 1., None, Some(past), true, false),
        (true, 1., Some(0.), Some(past), true, false),
        (true, 1., Some(f64::NAN), Some(past), true, false),
        (true, 1., Some(1.), None, true, false),
        (true, 1., Some(1.), Some(past), false, false),
    ] {
        let q = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
            deadline_aware: edf,
            mlu_local_threshold: theta,
            ..Default::default()
        });
        if let Some(rate) = rate {
            q.set_bandwidth(Box::new(move |_, _| rate)).unwrap();
        }
        let p = q
            .admit(
                vec![input(
                    20,
                    deadline,
                    QueueOwnerKind::User,
                    eligible.then_some(Transport::Rdma),
                )],
                None,
            )
            .unwrap();
        assert_eq!(
            matches!(p[0].wait(|| false), Err(Error::DeadlineInfeasible)),
            drop
        );
        assert_eq!(q.stats().deadline_infeasible_drops, u64::from(drop));
    }
}

#[test]
fn promotion_partitions_critical_future_slack_stably_across_admits() {
    let now = Instant::now();
    for (edf, slack, expected) in [
        (true, 0, vec![2, 3, 4, 5, 1]),
        (true, 2_000_000_000_000, vec![3, 4, 2, 5, 1]),
        (false, 2_000_000_000_000, vec![1, 2, 3, 4, 5]),
    ] {
        let q = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
            deadline_aware: edf,
            promotion_slack_ns: slack,
            max_dispatch_owners: 1,
            ..Default::default()
        });
        let held = q.admit(vec![user(1)], None).unwrap();
        let mut p = Vec::new();
        for dl in [
            None,
            Some(now - Duration::from_secs(1)),
            Some(now + Duration::from_secs(1000)),
            Some(now + Duration::from_secs(1000)),
            Some(now + Duration::from_secs(5000)),
        ] {
            p.extend(
                q.admit(vec![input(1, dl, QueueOwnerKind::User, None)], None)
                    .unwrap(),
            );
        }
        {
            let state = q.lock();
            assert_eq!(
                state
                    .next_ready(q.config, now + Duration::from_secs(4000))
                    .unwrap()
                    .1,
                p[if edf && slack != 0 {
                    4
                } else if edf {
                    1
                } else {
                    0
                }]
                .id
            );
        }
        drop(held);
        let mut items: Vec<_> = p.into_iter().map(Some).collect();
        for id in expected {
            let item = items[id - 1].take().unwrap();
            assert!(q.lock().owners[&item.id].dispatching);
            drop(item);
        }
    }
}

#[test]
fn same_submit_can_fill_the_window_and_an_exact_threshold_drop_frees_capacity() {
    let config = RuntimeQueueConfig {
        deadline_aware: true,
        mlu_local_threshold: 1.0,
        max_dispatch_owners: 2,
        max_outstanding_owners: 4,
        ..Default::default()
    };
    let q = LocalTransferAdmissionQueue::new(config);
    let held = q.admit(vec![user(1), user(1)], None).unwrap();
    let now = Instant::now();
    let p = q
        .admit(
            vec![
                input(
                    6,
                    Some(now + Duration::from_secs(2)),
                    QueueOwnerKind::User,
                    Some(Transport::Rdma),
                ),
                user(1),
            ],
            None,
        )
        .unwrap();
    let rate = |_, _| 3.0;
    {
        let mut s = q.lock();
        assert_eq!(s.dispatching_owners, 2);
        s.release(held[0].id);
        assert!(s.refill_dispatch_window(config, Some(&rate), now));
        assert!(!s.owners.contains_key(&p[0].id));
        assert!(s.owners[&p[1].id].dispatching);
        assert_eq!((s.outstanding_bytes, s.dispatching_bytes), (2, 2));
    }
    assert!(matches!(
        p[0].wait(|| false),
        Err(Error::DeadlineInfeasible)
    ));
    p[1].wait(|| false).unwrap();
    drop((held, p));
    assert_eq!(q.stats().deadline_infeasible_drops, 1);
}

#[test]
fn one_batch_worker_keeps_its_ordered_head_when_a_successor_has_an_earlier_deadline() {
    // EDF still chooses between independent workers. Granting the only slot
    // to this worker's successor would deadlock its earlier payload.
    let q = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
        max_dispatch_owners: 1,
        deadline_aware: true,
        ..Default::default()
    });
    let now = Instant::now();
    let item = |after| {
        input(
            1,
            Some(now + Duration::from_secs(after)),
            QueueOwnerKind::User,
            None,
        )
    };
    let hold = q.admit(vec![user(1)], None).unwrap();
    let mut ordered = q
        .admit(vec![item(300), item(100)], None)
        .unwrap()
        .into_iter();
    let independent = q.admit(vec![item(200)], None).unwrap();
    drop(hold);
    assert!(q.lock().owners[&independent[0].id].dispatching);
    drop(independent);
    let head = ordered.next().unwrap();
    assert!(q.lock().owners[&head.id].dispatching);
    let tail = ordered.next().unwrap();
    assert!(!q.lock().owners[&tail.id].dispatching);
    drop(head);
    assert!(q.lock().owners[&tail.id].dispatching);
}

#[test]
fn promotion_and_deadline_drop_share_a_refill_without_charging_a_dropped_owner() {
    let q = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
        max_dispatch_owners: 1,
        deadline_aware: true,
        mlu_local_threshold: 1.0,
        promotion_slack_ns: 2_000_000_000_000,
        ..Default::default()
    });
    let hold = q.admit(vec![user(1)], None).unwrap();
    q.set_bandwidth(Box::new(|_, _| 0.0001)).unwrap();
    let now = Instant::now();
    let expired = q
        .admit(vec![input(1, Some(now), QueueOwnerKind::User, None)], None)
        .unwrap();
    let urgent = q
        .admit(
            vec![input(
                1,
                Some(now + Duration::from_secs(100)),
                QueueOwnerKind::User,
                Some(Transport::Rdma),
            )],
            None,
        )
        .unwrap();
    drop(hold);
    assert!(matches!(
        urgent[0].ticket.terminal(),
        Err(Error::DeadlineInfeasible)
    ));
    assert!(q.lock().owners[&expired[0].id].dispatching);
    assert_eq!(
        (
            q.stats().deadline_infeasible_drops,
            q.stats().dispatching_bytes
        ),
        (1, 1)
    );
}

#[test]
fn switching_transports_moves_the_bandwidth_term_without_readmitting_the_owner() {
    let q = LocalTransferAdmissionQueue::new(Default::default());
    let p = q
        .admit(
            vec![input(8, None, QueueOwnerKind::User, Some(Transport::Rdma))],
            None,
        )
        .unwrap();
    let stats = q.stats();
    assert_eq!(q.lock().eligible_bytes[Transport::Rdma as usize], 8);
    p[0].reroute(None);
    assert_eq!(stats, q.stats());
    assert_eq!(q.lock().eligible_bytes[Transport::Rdma as usize], 0);
    p[0].reroute(Some((Transport::Rdma, 1)));
    assert_eq!(stats, q.stats());
    assert_eq!(q.lock().eligible_bytes[Transport::Rdma as usize], 8);
    drop(p);
    assert_eq!(q.stats(), RuntimeQueueStats::default());
}
