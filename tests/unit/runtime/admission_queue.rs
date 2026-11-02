use std::time::Duration;

use super::*;
use crate::{
    BatchState, EngineConfig, Notification, Permission, TaskState, TransferEngine, TransferOp,
    TransferRequest, Transport,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn dispatch_window_preserves_fifo_ties_and_never_bypasses_a_large_head() {
    for deadline_aware in [false, true] {
        let queue = LocalTransferAdmissionQueue::new(RuntimeQueueConfig {
            max_dispatch_owners: 2,
            max_dispatch_bytes: 8,
            deadline_aware,
        });
        let now = Instant::now();
        let first = queue.try_admit(5, now + TIMEOUT, None).unwrap();
        let large = queue.try_admit(4, now + TIMEOUT, None).unwrap();
        let small = queue.try_admit(3, now + TIMEOUT, None).unwrap();
        assert_eq!(
            queue.stats().dispatching_owners,
            1,
            "must not skip the 4-byte head"
        );
        assert!(!queue.lock().owners[&small.id].dispatching);
        assert!(matches!(
            queue.try_admit(9, now + TIMEOUT, None),
            Err(Error::ResourceExhausted)
        ));
        drop(first);
        assert_eq!(
            (
                queue.stats().dispatching_owners,
                queue.stats().dispatching_bytes
            ),
            (2, 7)
        );
        large.wait(|| false).unwrap();
        small.wait(|| false).unwrap();
        drop((large, small));
        assert_eq!(queue.stats(), RuntimeQueueStats::default());

        // The count limit must still apply when the byte window has room.
        let held = queue.try_admit(1, now + TIMEOUT, None).unwrap();
        let held2 = queue.try_admit(1, now + TIMEOUT, None).unwrap();
        let tie1 = queue.try_admit(1, now + TIMEOUT, None).unwrap();
        let tie2 = queue.try_admit(1, now + TIMEOUT, None).unwrap();
        drop(held);
        assert!(queue.lock().owners[&tie1.id].dispatching);
        assert!(!queue.lock().owners[&tie2.id].dispatching);
        drop((held2, tie1, tie2));
    }
}

fn engines(deadline_aware: bool) -> (TransferEngine, Arc<TransferEngine>) {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            runtime_queue: Some(RuntimeQueueConfig {
                max_dispatch_owners: 1,
                max_dispatch_bytes: 8,
                deadline_aware,
            }),
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    (source, requester)
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
fn public_fifo_edf_dispatch_preserves_batch_dependencies_and_drains_after_close() {
    for deadline_aware in [false, true] {
        let (source, requester) = engines(deadline_aware);
        let remote = source
            .register_memory(vec![0].into_boxed_slice(), Permission::RemoteWrite)
            .unwrap();
        let local = requester
            .register_memory(vec![1, 2, 3].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        let requests: Vec<_> = (0..3)
            .map(|i| request(local.base_address() + i, remote.base_address()))
            .collect();
        // A real prepared owner reserves the window before its transport starts.
        let held = requester
            .prepare(
                TransferOp::Write,
                &peer,
                &requests[..1],
                None,
                TIMEOUT,
                None,
            )
            .unwrap();
        let mut first = requester.allocate_batch(2).unwrap();
        for (i, seconds) in [(0, 5), (1, 3)] {
            first
                .submit(
                    TransferOp::Write,
                    &peer,
                    &requests[i..i + 1],
                    Some(&Notification {
                        name: format!("a{i}"),
                        message: String::new(),
                    }),
                    Duration::from_secs(seconds),
                )
                .unwrap();
        }
        first.seal().unwrap();
        let mut second = requester
            .submit_async(
                TransferOp::Write,
                &peer,
                &requests[2..],
                Some(&Notification {
                    name: "b".into(),
                    message: String::new(),
                }),
                Duration::from_secs(4),
            )
            .unwrap();
        assert_eq!(
            requester.stats().unwrap().runtime_queue.unwrap(),
            RuntimeQueueStats {
                queued_owners: 3,
                queued_bytes: 8,
                dispatching_owners: 1,
                dispatching_bytes: 1,
            }
        );
        assert!(
            first
                .task_statuses()
                .unwrap()
                .iter()
                .all(|task| task.state == TaskState::Waiting)
        );
        assert!(matches!(
            requester.close_requester(Duration::ZERO),
            Err(Error::Timeout)
        ));
        assert!(matches!(
            requester.allocate_batch(1),
            Err(Error::RequesterClosed)
        ));
        drop(held);
        assert_eq!(first.wait(TIMEOUT).unwrap().state, BatchState::Completed);
        assert_eq!(second.wait(TIMEOUT).unwrap().state, BatchState::Completed);
        let notifications = source.receive_notifications().unwrap();
        let order: Vec<_> = notifications
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        assert_eq!(
            order,
            if deadline_aware {
                vec!["b", "a0", "a1"]
            } else {
                vec!["a0", "a1", "b"]
            }
        );
        let mut bytes = [0];
        remote.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [if deadline_aware { 2 } else { 3 }]);
        first.free().unwrap();
        second.free().unwrap();
        requester.close_requester(TIMEOUT).unwrap();
        assert_eq!(
            requester.stats().unwrap().runtime_queue.unwrap(),
            RuntimeQueueStats::default()
        );
    }
}

#[test]
fn queued_cancellation_timeout_and_oversize_have_no_io_and_partial_cancel_keeps_prefix() {
    let (source, requester) = engines(true);
    let remote = source
        .register_memory(vec![0; 3].into_boxed_slice(), Permission::RemoteWrite)
        .unwrap();
    let local = requester
        .register_memory(vec![7; 9].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let requests: Vec<_> = (0..3)
        .map(|i| request(local.base_address(), remote.base_address() + i))
        .collect();
    let held = requester
        .prepare(
            TransferOp::Write,
            &peer,
            &requests[..1],
            None,
            TIMEOUT,
            None,
        )
        .unwrap();
    let mut batch = requester.allocate_batch(2).unwrap();
    let oversized = Notification {
        name: "12345678".into(),
        message: String::new(),
    };
    assert!(matches!(
        batch.submit(
            TransferOp::Write,
            &peer,
            &requests[..1],
            Some(&oversized),
            TIMEOUT
        ),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(batch.status().unwrap().submitted_tasks, 0);
    assert_eq!(requester.stats().unwrap().inflight_batches, 1);
    batch
        .submit(TransferOp::Write, &peer, &requests[..2], None, TIMEOUT)
        .unwrap();
    batch.seal().unwrap();
    batch.cancel(0).unwrap();
    assert_eq!(batch.wait(TIMEOUT).unwrap().state, BatchState::Cancelled);
    batch.free().unwrap();
    let mut expired = requester
        .submit_async(
            TransferOp::Write,
            &peer,
            &requests[..1],
            None,
            Duration::from_millis(30),
        )
        .unwrap();
    assert_eq!(expired.wait(TIMEOUT).unwrap().state, BatchState::Failed);
    assert!(matches!(
        expired.task_status(0).unwrap().failure.unwrap().error,
        Error::Timeout
    ));
    expired.free().unwrap();
    // Empty submissions retain their original success/no-notify behavior.
    let mut empty = requester
        .submit_async(TransferOp::Write, &peer, &[], Some(&oversized), TIMEOUT)
        .unwrap();
    assert_eq!(empty.wait(TIMEOUT).unwrap().state, BatchState::Completed);
    empty.free().unwrap();
    let mut bytes = [0; 3];
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0; 3]);
    assert!(source.receive_notifications().unwrap().is_empty());
    assert_eq!(
        requester
            .stats()
            .unwrap()
            .runtime_queue
            .unwrap()
            .queued_owners,
        0
    );
    let mut partial = requester
        .submit_async(TransferOp::Write, &peer, &requests, None, TIMEOUT)
        .unwrap();
    partial.cancel(1).unwrap();
    drop(held);
    assert_eq!(partial.wait(TIMEOUT).unwrap().state, BatchState::Cancelled);
    let tasks: Vec<_> = partial
        .task_statuses()
        .unwrap()
        .iter()
        .map(|task| task.state)
        .collect();
    assert_eq!(
        tasks,
        [
            TaskState::Completed,
            TaskState::Cancelled,
            TaskState::Skipped
        ]
    );
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7, 0, 0]);
    partial.free().unwrap();
    assert_eq!(requester.stats().unwrap().inflight_batches, 0);
    assert_eq!(
        requester.stats().unwrap().runtime_queue.unwrap(),
        RuntimeQueueStats::default()
    );
}
