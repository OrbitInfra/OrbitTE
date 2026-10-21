use super::*;

use crate::{EngineConfig, Permission};

const TIMEOUT: Duration = Duration::from_secs(3);

#[test]
fn capacity_ids_append_and_sealed_status_follow_actual_tcp_bytes() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let remote = source
        .register_memory(
            (0u8..32).collect::<Vec<_>>().into_boxed_slice(),
            Permission::RemoteRead,
        )
        .unwrap();
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            max_allocated_batches: 1,
            max_allocated_requests: 4,
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let local = engine
        .register_memory(vec![0xee; 32].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = engine
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let requests = [
        TransferRequest {
            transport: crate::Transport::Tcp,
            local_address: local.base_address(),
            remote_address: remote.base_address() + 8,
            length: 8,
        },
        TransferRequest {
            transport: crate::Transport::Tcp,
            local_address: local.base_address() + 16,
            remote_address: remote.base_address(),
            length: 8,
        },
    ];
    let mut batch = engine.allocate_batch(2).unwrap();
    let id = batch.id().unwrap();
    assert!(matches!(
        engine.allocate_batch(1),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(batch.wait(TIMEOUT), Err(Error::BatchOpen)));
    assert_eq!(
        batch
            .submit(TransferOp::Read, &peer, &requests[..1], None, TIMEOUT)
            .unwrap(),
        0..1
    );
    assert_eq!(
        batch
            .submit(TransferOp::Read, &peer, &requests[1..], None, TIMEOUT)
            .unwrap(),
        1..2
    );
    assert!(matches!(
        batch.submit(TransferOp::Read, &peer, &requests[..1], None, TIMEOUT),
        Err(Error::ResourceExhausted)
    ));
    batch.seal().unwrap();
    assert!(matches!(
        batch.submit(TransferOp::Read, &peer, &[], None, TIMEOUT),
        Err(Error::BatchClosed)
    ));
    let status = batch.wait(TIMEOUT).unwrap();
    assert_eq!(
        (
            status.state,
            status.completed_tasks,
            status.transferred_bytes,
            status.locally_drained
        ),
        (BatchState::Completed, 2, 16, true)
    );
    assert_eq!(batch.status().unwrap(), status);
    assert_eq!(batch.status().unwrap(), status);
    let tasks = batch.task_statuses().unwrap();
    assert!(tasks.iter().all(|task| task.state == TaskState::Completed
        && task.locally_drained
        && task.transferred_bytes == 8));
    assert!(!batch.cancel(0).unwrap());
    assert!(matches!(batch.task_status(2), Err(Error::InvalidTask)));
    let mut bytes = [0; 32];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[..8], &(8u8..16).collect::<Vec<_>>());
    assert_eq!(&bytes[8..16], &[0xee; 8]);
    assert_eq!(&bytes[16..24], &(0u8..8).collect::<Vec<_>>());
    assert_eq!(&bytes[24..], &[0xee; 8]);
    batch.free().unwrap();
    assert!(matches!(batch.status(), Err(Error::BatchClosed)));
    assert_eq!(
        (
            engine.stats().unwrap().allocated_batches,
            engine.stats().unwrap().requester_workers
        ),
        (0, 0)
    );
    let mut replacement = engine.allocate_batch(4).unwrap();
    assert!(replacement.id().unwrap().as_u64() > id.as_u64());
    replacement.free().unwrap();
}

#[test]
fn rejected_group_changes_no_tasks_and_later_groups_survive_a_failed_prefix() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let first = source
        .register_memory(vec![1; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let retired = source
        .register_memory(vec![2; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let engine = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());
    let local = engine
        .register_memory(vec![0xee; 32].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = engine
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    source.unregister_memory(&retired).unwrap();
    let request = |local_offset, remote: &crate::RegisteredMemory| TransferRequest {
        transport: crate::Transport::Tcp,
        local_address: local.base_address() + local_offset,
        remote_address: remote.base_address(),
        length: 8,
    };
    let valid = request(0, &first);
    let invalid = TransferRequest {
        transport: crate::Transport::Tcp,
        local_address: 0,
        ..valid.clone()
    };
    let mut batch = engine.allocate_batch(4).unwrap();
    assert!(matches!(
        batch.submit(
            TransferOp::Read,
            &peer,
            &[valid.clone(), invalid],
            None,
            TIMEOUT
        ),
        Err(Error::RegionNotFound)
    ));
    assert_eq!(batch.status().unwrap().submitted_tasks, 0);
    assert_eq!(engine.stats().unwrap().inflight_batches, 0);
    let notify = Notification {
        name: "scope".into(),
        message: "done".into(),
    };
    assert_eq!(
        batch
            .submit(
                TransferOp::Read,
                &peer,
                &[valid, request(8, &retired), request(16, &first)],
                Some(&notify),
                TIMEOUT
            )
            .unwrap(),
        0..3
    );
    assert_eq!(
        batch
            .submit(
                TransferOp::Read,
                &peer,
                &[request(24, &first)],
                Some(&notify),
                TIMEOUT
            )
            .unwrap(),
        3..4
    );
    batch.seal().unwrap();
    let status = batch.wait(TIMEOUT).unwrap();
    assert_eq!(
        (
            status.state,
            status.completed_tasks,
            status.transferred_bytes
        ),
        (BatchState::Failed, 2, 16)
    );
    let tasks = batch.task_statuses().unwrap();
    assert_eq!(
        tasks.iter().map(|task| task.state).collect::<Vec<_>>(),
        [
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Skipped,
            TaskState::Completed
        ]
    );
    assert!(matches!(
        tasks[1].failure.as_ref().unwrap().error,
        Error::RegionNotFound
    ));
    assert_eq!(tasks[1].failure.as_ref().unwrap().request_index, 1);
    let submissions = batch.submissions().unwrap();
    assert!(matches!(
        *submissions[0].notification,
        NotificationStatus::Skipped
    ));
    assert!(matches!(
        *submissions[1].notification,
        NotificationStatus::Delivered
    ));
    assert_eq!(source.receive_notifications().unwrap(), [notify]);
    let mut bytes = [0; 32];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(
        bytes,
        [
            1, 1, 1, 1, 1, 1, 1, 1, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee,
            0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 1, 1, 1, 1, 1, 1, 1, 1
        ]
    );
    batch.free().unwrap();
}

#[test]
fn empty_batches_and_notification_failure_keep_data_completion_separate() {
    let source = TransferEngine::new(EngineConfig {
        max_pending_notifications: 1,
        ..EngineConfig::default()
    })
    .unwrap();
    let remote = source
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let engine = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());
    let local = engine
        .register_memory(vec![0; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = engine
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let notification = Notification {
        name: "full".into(),
        message: "queue".into(),
    };
    source.send_local_notification(&notification).unwrap();
    let request = TransferRequest {
        transport: crate::Transport::Tcp,
        local_address: local.base_address(),
        remote_address: remote.base_address(),
        length: 8,
    };
    for requests in [&[][..], std::slice::from_ref(&request)] {
        let mut batch = engine
            .submit_async(
                TransferOp::Read,
                &peer,
                requests,
                Some(&notification),
                TIMEOUT,
            )
            .unwrap();
        let status = batch.wait(TIMEOUT).unwrap();
        assert_eq!(status.state, BatchState::Completed);
        let results = batch.submissions().unwrap();
        if requests.is_empty() {
            assert!(matches!(
                *results[0].notification,
                NotificationStatus::Skipped
            ));
        } else {
            assert_eq!(status.transferred_bytes, 8);
            assert!(matches!(
                *results[0].notification,
                NotificationStatus::Failed(_)
            ));
            let mut bytes = [0; 8];
            local.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [7; 8]);
        }
        batch.free().unwrap();
    }
    assert_eq!(source.receive_notifications().unwrap(), [notification]);
}

#[test]
fn allocation_budget_and_requester_close_reject_without_leaking_reservations() {
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            max_allocated_batches: 2,
            max_allocated_requests: 3,
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let mut first = engine.allocate_batch(2).unwrap();
    assert!(matches!(
        engine.allocate_batch(2),
        Err(Error::ResourceExhausted)
    ));
    let mut second = engine.allocate_batch(1).unwrap();
    assert!(matches!(
        engine.allocate_batch(1),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(engine.stats().unwrap().allocated_requests, 3);
    first.free().unwrap();
    second.free().unwrap();
    assert_eq!(engine.stats().unwrap().allocated_requests, 0);
    engine.close_requester(Duration::ZERO).unwrap();
    assert!(matches!(
        engine.allocate_batch(1),
        Err(Error::RequesterClosed)
    ));
    assert!(matches!(
        engine.open_segment(engine.endpoint(), engine.source_epoch(), TIMEOUT),
        Err(Error::RequesterClosed)
    ));
    // Outgoing and source admission are independent.
    let local = engine
        .register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    engine.unregister_memory(&local).unwrap();
}
