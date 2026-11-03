use crate::test_support::EVENTUAL_PROGRESS as WAIT;
use crate::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Pair {
    src: TransferEngine,
    dst: Arc<TransferEngine>,
    remote: RegisteredMemory,
    local: RegisteredMemory,
    peer: RemoteSegment,
}
impl Pair {
    fn new(source: EngineConfig, requester: EngineConfig) -> Self {
        let src = TransferEngine::new(source).unwrap();
        let remote = src
            .register_memory(
                (0..32u8).collect::<Vec<_>>().into(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let dst = Arc::new(TransferEngine::new(requester).unwrap());
        let local = dst
            .register_memory(vec![0xcc; 32].into(), Permission::LocalOnly)
            .unwrap();
        let peer = dst
            .open_segment(src.endpoint(), src.source_epoch(), WAIT)
            .unwrap();
        Self {
            src,
            dst,
            remote,
            local,
            peer,
        }
    }
    fn req(&self, offset: u64, len: usize) -> TransferRequest {
        TransferRequest::new(
            Transport::Tcp,
            self.local.base_address() + offset,
            self.remote.base_address() + offset,
            len,
        )
    }
    fn mapping(&self, requests: &[TransferRequest]) -> Vec<usize> {
        let mut batch = self
            .dst
            .submit_async(TransferOp::Read, &self.peer, requests, None, WAIT)
            .unwrap();
        assert_eq!(batch.wait(WAIT).unwrap().state, BatchState::Completed);
        let mapping = batch
            .task_statuses()
            .unwrap()
            .into_iter()
            .map(|t| t.physical_owner)
            .collect();
        batch.free().unwrap();
        mapping
    }
}

#[test]
fn shuffled_aliases_keep_absolute_public_ids_lengths_and_notification_intervals() {
    for queued in [false, true] {
        let f = Pair::new(
            Default::default(),
            EngineConfig {
                runtime_queue: queued.then_some(Default::default()),
                ..Default::default()
            },
        );
        let mut batch = f.dst.allocate_batch(6).unwrap();
        batch
            .submit(TransferOp::Read, &f.peer, &[f.req(1, 1)], None, WAIT)
            .unwrap();
        // Two folded owners. Their lowest-address members aren't necessarily
        // the representative public tasks. Include a different-hint neighbour.
        let req = [
            f.req(18, 2),
            f.req(5, 2),
            f.req(16, 2),
            f.req(7, 1),
            TransferRequest {
                transport: Transport::Unspec,
                ..f.req(8, 1)
            },
        ];
        let note = Notification {
            name: "aliases".into(),
            message: "complete".into(),
        };
        batch
            .submit(TransferOp::Read, &f.peer, &req, Some(&note), WAIT)
            .unwrap();
        batch.seal().unwrap();
        let status = batch.wait(WAIT).unwrap();
        assert_eq!(
            (
                status.state,
                status.completed_tasks,
                status.transferred_bytes
            ),
            (BatchState::Completed, 6, 9)
        );
        let tasks = batch.task_statuses().unwrap();
        assert_eq!(
            tasks.iter().map(|t| t.physical_owner).collect::<Vec<_>>(),
            [0, 1, 2, 1, 2, 5]
        );
        assert_eq!(
            tasks
                .iter()
                .map(|t| t.transferred_bytes)
                .collect::<Vec<_>>(),
            [1, 2, 2, 2, 1, 1]
        );
        assert!(
            tasks
                .iter()
                .all(|t| t.locally_drained && t.state == TaskState::Completed)
        );
        assert_eq!(batch.submissions().unwrap()[1].completed_requests, 5);
        let mut got = [0; 32];
        f.local.read(0, &mut got).unwrap();
        let mut expect = [0xcc; 32];
        for i in [1, 5, 6, 7, 8, 16, 17, 18, 19] {
            expect[i] = i as u8;
        }
        assert_eq!(got, expect);
        assert_eq!(f.src.receive_notifications().unwrap(), [note]);
        batch.free().unwrap();
    }
}

#[test]
fn gaps_hints_priorities_intents_policy_contracts_deadlines_caps_and_opt_out_split() {
    let selector = TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"one","segment_type":"memory","transports":["tcp"]},{"name":"two","segment_type":"memory","transports":["tcp"]}]}"#,
    )
    .unwrap();
    let f = Pair::new(
        EngineConfig {
            max_transfer_bytes: 6,
            ..Default::default()
        },
        EngineConfig {
            selector: Some(selector),
            qos: Some(QosConfig {
                tenants: vec![QosTenantContract {
                    name: "two".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    let base = f.req(4, 2);
    let next = f.req(6, 2);
    assert_eq!(f.mapping(&[base.clone(), next.clone()]), [0, 0]);
    assert_eq!(f.mapping(&[base.clone(), f.req(8, 2)]), [0, 1]);
    assert_eq!(f.mapping(&[f.req(4, 4), f.req(8, 4)]), [0, 1]); // peer's actual cap
    assert_eq!(
        f.mapping(&[f.req(1, 5), f.req(6, 1), f.req(7, 1)]),
        [0, 0, 2]
    );
    let deadline = Instant::now() + WAIT;
    assert_eq!(
        f.mapping(&[
            base.clone().with_deadline(deadline),
            next.clone().with_deadline(deadline)
        ]),
        [0, 0]
    );
    let q = f
        .dst
        .resolve_qos(&QosRequestContext {
            tenant_id: "two".into(),
            ..Default::default()
        })
        .unwrap();
    let mut other = next.clone();
    other.qos = Some(q);
    for (a, b) in [
        (
            base.clone(),
            TransferRequest {
                transport: Transport::Unspec,
                ..next.clone()
            },
        ),
        (base.clone(), next.clone().with_priority(Priority::Low)),
        (
            base.clone().with_intent(IntentType::Migration),
            next.clone(),
        ),
        (
            base.clone().with_policy_name("one").unwrap(),
            next.clone().with_policy_name("two").unwrap(),
        ),
        (
            base.clone().with_deadline(deadline),
            next.clone()
                .with_deadline(deadline - Duration::from_secs(1)),
        ),
        (base.clone(), other),
    ] {
        assert_eq!(f.mapping(&[a, b]), [0, 1]);
    }
    assert_eq!(
        f.mapping(&[
            base.with_policy_name("two")
                .unwrap()
                .with_intent(IntentType::Checkpoint),
            next.with_policy_name("two")
                .unwrap()
                .with_intent(IntentType::Checkpoint)
        ]),
        [0, 0]
    );
    let f = Pair::new(
        Default::default(),
        EngineConfig {
            merge_requests: false,
            ..Default::default()
        },
    );
    assert!(f.mapping(&[]).is_empty());
    assert_eq!(
        f.mapping(&[f.req(8, 2), f.req(4, 2), f.req(6, 2)]),
        [0, 1, 2]
    );
}

#[test]
fn separate_registration_boundaries_and_self_overlap_preserve_public_bytes() {
    let engine = Arc::new(TransferEngine::new(Default::default()).unwrap());
    let backing = Arc::new(HostMemory::new((0..16u8).collect::<Vec<_>>().into()).unwrap());
    let spans = engine
        .register_host_memory_batch(
            &[(backing.clone(), 0, 4), (backing.clone(), 4, 4)],
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let other = engine
        .register_memory(vec![0; 16].into(), Permission::RemoteReadWrite)
        .unwrap();
    let peer = engine
        .open_segment(engine.endpoint(), engine.source_epoch(), WAIT)
        .unwrap();
    for (a, b) in [
        (spans[0].base_address(), other.base_address()),
        (other.base_address(), spans[0].base_address()),
    ] {
        let requests = [0, 4].map(|i| TransferRequest::new(Transport::Tcp, a + i, b + i, 4));
        let mut batch = engine
            .submit_async(TransferOp::Read, &peer, &requests, None, WAIT)
            .unwrap();
        assert_eq!(batch.wait(WAIT).unwrap().state, BatchState::Completed);
        assert_eq!(batch.task_status(1).unwrap().physical_owner, 1);
        batch.free().unwrap();
    }
    other.write(0, &[7, 8, 9, 10]).unwrap();
    let a = other.base_address();
    let req = [0, 1, 2].map(|i| TransferRequest::new(Transport::Tcp, a + i, a + i + 1, 1));
    let mut b = engine
        .submit_async(TransferOp::Write, &peer, &req, None, WAIT)
        .unwrap();
    assert_eq!(b.wait(WAIT).unwrap().completed_tasks, 3);
    assert_eq!(
        b.task_statuses()
            .unwrap()
            .iter()
            .map(|t| t.physical_owner)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    let mut data = [0; 4];
    other.read(0, &mut data).unwrap();
    assert_eq!(data, [7; 4]);
    b.free().unwrap();
}

#[test]
fn cancelling_a_queued_alias_uncharges_one_owner_without_touching_either_range() {
    let f = Pair::new(
        Default::default(),
        EngineConfig {
            runtime_queue: Some(RuntimeQueueConfig {
                max_dispatch_owners: 1,
                max_outstanding_owners: 3,
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    let held = f
        .dst
        .prepare(TransferOp::Write, &f.peer, &[f.req(0, 1)], None, WAIT, None)
        .unwrap();
    let mut batch = f
        .dst
        .submit_async(
            TransferOp::Write,
            &f.peer,
            &[f.req(4, 2), f.req(6, 2), f.req(12, 1)],
            None,
            WAIT,
        )
        .unwrap();
    assert_eq!(
        f.dst.stats().unwrap().runtime_queue.unwrap().queued_owners,
        2
    );
    batch.cancel(1).unwrap();
    assert_eq!(
        f.dst.stats().unwrap().runtime_queue.unwrap().queued_owners,
        1
    );
    drop(held);
    let report = batch.wait(WAIT).unwrap();
    assert_eq!(
        (
            report.state,
            report.completed_tasks,
            report.transferred_bytes
        ),
        (BatchState::Cancelled, 1, 1)
    );
    let failed = batch.submissions().unwrap()[0].failure.clone().unwrap();
    assert!(matches!(failed.error, Error::Cancelled));
    assert_eq!(
        (
            failed.request_index,
            failed.affected_requests,
            failed.remote_effect
        ),
        (0, 3, RemoteEffect::Unknown)
    );
    assert!(
        batch.task_statuses().unwrap()[..2]
            .iter()
            .all(|t| t.physical_owner == 0
                && t.state == TaskState::Cancelled
                && t.locally_drained
                && t.cancel_requested)
    );
    assert_eq!(
        f.dst.stats().unwrap().runtime_queue.unwrap().queued_owners,
        0
    );
    let mut guard = [0; 32];
    f.remote.read(0, &mut guard).unwrap();
    let mut expected = std::array::from_fn(|i| i as u8);
    expected[12] = 0xcc;
    assert_eq!(guard, expected);
    let sibling = batch.task_status(2).unwrap();
    assert_eq!(
        (
            sibling.physical_owner,
            sibling.state,
            sibling.transferred_bytes
        ),
        (2, TaskState::Completed, 1)
    );
    batch.free().unwrap();
}

#[test]
fn interleaved_hints_fold_per_hint_and_source_overlap_fences_only_its_interval() {
    let f = Pair::new(Default::default(), Default::default());
    let r = [
        f.req(3, 1),
        TransferRequest {
            transport: Transport::Unspec,
            ..f.req(13, 1)
        },
        f.req(4, 1),
        TransferRequest {
            transport: Transport::Unspec,
            ..f.req(14, 1)
        },
    ];
    assert_eq!(f.mapping(&r), [0, 1, 0, 1]);
    // Overlapping READ destinations preserve public write order. A disjoint
    // pair after them can still fold.
    let r = [
        f.req(1, 2),
        TransferRequest {
            remote_address: f.remote.base_address() + 9,
            ..f.req(2, 1)
        },
        f.req(15, 1),
        f.req(16, 1),
    ];
    assert_eq!(f.mapping(&r), [0, 1, 2, 2]);
    let mut bytes = [0; 2];
    f.local.read(1, &mut bytes).unwrap();
    assert_eq!(bytes, [1, 9]);
}

#[test]
fn a_merged_span_keeps_the_original_selected_size_rule_and_qos_slot() {
    let selector = TransportSelectorConfig::from_json(
        r#"{"policy":[
        {"name":"two","segment_type":"memory","max_size":2,"transports":["tcp"]},
        {"name":"large","segment_type":"memory","transports":["rdma"]}]}"#,
    )
    .unwrap();
    let f = Pair::new(
        Default::default(),
        EngineConfig {
            selector: Some(selector),
            qos: Some(QosConfig {
                defaults: QosPolicyFields {
                    max_inflight_requests: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    let q = f.dst.resolve_qos(&QosRequestContext::default()).unwrap();
    let req = [f.req(2, 2), f.req(4, 2).with_qos(q)];
    assert_eq!(f.mapping(&req), [0, 0]);
}

#[test]
fn distinct_engine_epochs_can_still_share_overlapping_host_backing() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let requester = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());
    let memory = Arc::new(HostMemory::new(vec![7, 8, 9, 10, 11].into()).unwrap());
    let remote = source
        .register_host_memory(memory.clone(), 0, 5, Permission::RemoteWrite)
        .unwrap();
    let local = requester
        .register_host_memory(memory, 0, 5, Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), WAIT)
        .unwrap();
    let r = [0, 1, 2].map(|i| {
        TransferRequest::new(
            Transport::Tcp,
            local.base_address() + i,
            remote.base_address() + i + 1,
            1,
        )
    });
    let mut batch = requester
        .submit_async(TransferOp::Write, &peer, &r, None, WAIT)
        .unwrap();
    assert_eq!(batch.wait(WAIT).unwrap().completed_tasks, 3);
    assert_eq!(
        batch
            .task_statuses()
            .unwrap()
            .iter()
            .map(|t| t.physical_owner)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    let mut got = [0; 5];
    local.read(0, &mut got).unwrap();
    assert_eq!(got, [7, 7, 7, 7, 11]);
    batch.free().unwrap();
}
