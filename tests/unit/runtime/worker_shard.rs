use super::*;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

fn stats(workers: &Workers) -> StagingStats {
    let mut stats = StagingStats::default();
    workers.stats(&mut stats);
    stats
}

fn block_shards(workers: &Workers) -> (Vec<TaskHandle>, Vec<mpsc::Sender<()>>) {
    let mut tasks = Vec::new();
    let mut releases = Vec::new();
    for _ in 0..MAX_SHARDS {
        let (entered, ready) = mpsc::sync_channel(1);
        let (release, blocked) = mpsc::channel();
        tasks.push(
            workers
                .submit(
                    TransferOp::Read,
                    Instant::now() + Duration::from_secs(5),
                    Arc::new(AtomicBool::new(false)),
                    move || {
                        entered.send(()).unwrap();
                        let result = blocked
                            .recv_timeout(Duration::from_secs(3))
                            .map_err(|_| Error::Timeout);
                        (result, RemoteEffect::None)
                    },
                )
                .unwrap(),
        );
        ready.recv_timeout(Duration::from_secs(1)).unwrap();
        releases.push(release);
    }
    (tasks, releases)
}

fn release_tasks(tasks: Vec<TaskHandle>, releases: Vec<mpsc::Sender<()>>) {
    for release in releases {
        release.send(()).unwrap();
    }
    for task in tasks {
        task.finish(false).0.unwrap();
    }
}

#[test]
fn bounded_queues_cancel_and_release_captures_without_waiting_for_active_work() {
    let workers = Workers::default();
    let (active, releases) = block_shards(&workers);
    let ran = Arc::new(AtomicUsize::new(0));
    let mut queued = Vec::new();
    for _ in 0..MAX_SHARDS * QUEUE_CAPACITY {
        let owner = Arc::new(vec![0x35; 16]);
        let weak = Arc::downgrade(&owner);
        let ran = Arc::clone(&ran);
        let task = workers
            .submit(
                TransferOp::Write,
                Instant::now() + Duration::from_secs(5),
                Arc::new(AtomicBool::new(false)),
                move || {
                    assert_eq!(owner[0], 0x35);
                    ran.fetch_add(1, Ordering::AcqRel);
                    (Ok(()), RemoteEffect::Unknown)
                },
            )
            .unwrap();
        queued.push((task, weak));
    }
    let full = stats(&workers);
    assert_eq!(
        (full.worker_shards, full.active_tasks, full.queued_tasks),
        (MAX_SHARDS, MAX_SHARDS, MAX_SHARDS * QUEUE_CAPACITY)
    );
    let rejected = workers.submit(
        TransferOp::Read,
        Instant::now() + Duration::from_secs(5),
        Arc::new(AtomicBool::new(false)),
        || panic!("full queue executed a rejected task"),
    );
    assert!(matches!(rejected, Err(Error::ResourceExhausted)));
    for (task, weak) in queued {
        let (result, effect) = task.finish(true);
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(effect, RemoteEffect::None);
        assert!(
            weak.upgrade().is_none(),
            "terminal preceded capture destruction"
        );
    }
    assert_eq!(ran.load(Ordering::Acquire), 0);
    let cancelled = stats(&workers);
    assert_eq!(
        (cancelled.active_tasks, cancelled.queued_tasks),
        (MAX_SHARDS, 0)
    );
    release_tasks(active, releases);
    workers
        .close(Instant::now() + Duration::from_secs(1))
        .unwrap();
    let closed = stats(&workers);
    assert_eq!(
        (
            closed.worker_shards,
            closed.active_tasks,
            closed.queued_tasks
        ),
        (0, 0, 0)
    );
}

#[test]
fn queued_deadline_expires_before_start_and_close_can_retry_after_active_drain() {
    for close in [false, true] {
        let workers = Workers::default();
        let (active, releases) = block_shards(&workers);
        let owner = Arc::new(vec![0x53; 16]);
        let weak = Arc::downgrade(&owner);
        let deadline = Instant::now() + Duration::from_millis(30);
        let queued = workers
            .submit(
                TransferOp::Write,
                deadline,
                Arc::new(AtomicBool::new(false)),
                move || {
                    drop(owner);
                    panic!("cancelled/expired task started I/O")
                },
            )
            .unwrap();
        if close {
            assert!(matches!(
                workers.close(Instant::now() + Duration::from_millis(5)),
                Err(Error::Timeout)
            ));
            let (result, effect) = queued.finish(false);
            assert!(matches!(result, Err(Error::Cancelled)));
            assert_eq!(effect, RemoteEffect::None);
            assert!(weak.upgrade().is_none());
            assert_eq!(stats(&workers).active_tasks, MAX_SHARDS);
            release_tasks(active, releases);
        } else {
            std::thread::sleep(
                deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
            );
            release_tasks(active, releases);
            let (result, effect) = queued.finish(false);
            assert!(matches!(result, Err(Error::Timeout)), "{result:?}");
            assert_eq!(effect, RemoteEffect::None);
            assert!(weak.upgrade().is_none());
        }
        workers
            .close(Instant::now() + Duration::from_secs(1))
            .unwrap();
        workers
            .close(Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(stats(&workers).worker_shards, 0);
        assert!(matches!(
            workers.submit(
                TransferOp::Read,
                Instant::now() + Duration::from_secs(1),
                Arc::new(AtomicBool::new(false)),
                || (Ok(()), RemoteEffect::None),
            ),
            Err(Error::RequesterClosed)
        ));
    }
}

#[test]
fn task_panic_preserves_shared_thread_and_drop_joins_idle_shards() {
    let workers = Workers::default();
    let mut thread_id = None;
    for operation in [TransferOp::Read, TransferOp::Write] {
        for panic in [true, false] {
            let owner = Arc::new(vec![0x57; 16]);
            let weak = Arc::downgrade(&owner);
            let (seen, thread) = mpsc::sync_channel(1);
            let task = workers
                .submit(
                    operation,
                    Instant::now() + Duration::from_secs(1),
                    Arc::new(AtomicBool::new(false)),
                    move || {
                        seen.send(std::thread::current().id()).unwrap();
                        assert_eq!(owner[0], 0x57);
                        assert!(!panic, "injected shared task panic");
                        (Ok(()), RemoteEffect::None)
                    },
                )
                .unwrap();
            let id = thread.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(*thread_id.get_or_insert(id), id);
            let (result, effect) = task.finish(false);
            if panic {
                assert!(matches!(
                    result,
                    Err(Error::Unquiesced {
                        operation: "staging worker panic",
                        ..
                    })
                ));
            } else {
                result.unwrap();
            }
            assert_eq!(
                effect,
                if panic && operation == TransferOp::Write {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                }
            );
            assert!(weak.upgrade().is_none());
        }
    }
    let idle = stats(&workers);
    assert_eq!(
        (
            idle.worker_shards,
            idle.opened_worker_shards,
            idle.active_tasks
        ),
        (1, 1, 0)
    );
    let weak = Arc::downgrade(&workers.pool.lock().unwrap().shards[0].0);
    drop(workers);
    assert!(
        weak.upgrade().is_none(),
        "idle shard retained its queue after Drop"
    );
}

#[test]
fn close_and_lazy_admission_have_one_order_and_never_restart_a_closed_pool() {
    for _ in 0..16 {
        let workers = Arc::new(Workers::default());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let submitter = {
            let workers = Arc::clone(&workers);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                match workers.submit(
                    TransferOp::Read,
                    Instant::now() + Duration::from_secs(2),
                    Arc::new(AtomicBool::new(false)),
                    || (Ok(()), RemoteEffect::None),
                ) {
                    Ok(task) => {
                        let (result, effect) = task.finish(false);
                        assert!(result.is_ok() || matches!(result, Err(Error::Cancelled)));
                        assert_eq!(effect, RemoteEffect::None);
                    }
                    Err(error) => assert!(matches!(error, Error::RequesterClosed)),
                }
            })
        };
        barrier.wait();
        workers
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        submitter.join().unwrap();
        let closed = stats(&workers);
        assert_eq!(
            (
                closed.worker_shards,
                closed.active_tasks,
                closed.queued_tasks
            ),
            (0, 0, 0)
        );
        assert!(closed.opened_worker_shards <= 1);
    }
}
