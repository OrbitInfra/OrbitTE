use super::*;
use crate::rdma::tests::{events, pause, runtime};
use crate::rdma::{Endpoint, Lease, Runtime, Staging};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let end = deadline();
    while !ready() {
        assert!(Instant::now() < end, "progress control timed out");
        thread::yield_now();
    }
}

fn wait_blocked(pause: &Arc<(Mutex<(bool, bool)>, Condvar)>) {
    let (lock, changed) = &**pause;
    let (state, timed) = changed
        .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(3), |s| !s.0)
        .unwrap();
    assert!(!timed.timed_out() && state.0);
}

fn release(pause: &Arc<(Mutex<(bool, bool)>, Condvar)>) {
    pause.0.lock().unwrap().1 = true;
    pause.1.notify_all();
}

fn configured(mode: &str, capacity: usize) -> Runtime {
    let mut runtime = runtime(mode);
    runtime.config.max_endpoints = 8;
    runtime.config.max_staging_bytes = 64;
    runtime.config.max_send_wr = 1;
    runtime.config.max_cqes = capacity;
    runtime.config.block_size = 2;
    runtime
}

#[test]
fn shared_cq_bounds_aggregate_debt_routes_multiple_qps_and_reuses_one_worker() {
    for capacity in [1, 3] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let runtime = configured("pause_post_reverse", capacity);
            let mut endpoints = (0..4)
                .map(|_| {
                    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
                    endpoint.connect(&endpoint.descriptor()).unwrap();
                    endpoint
                })
                .collect::<Vec<_>>();
            let progress = Arc::clone(&endpoints[0].progress);
            let first = endpoints.remove(0);
            let pause = pause(&runtime);
            thread::scope(|scope| {
                let run = |mut endpoint: Endpoint| {
                    let remote = endpoint.descriptor();
                    endpoint.transfer(&remote, operation, deadline()).unwrap();
                    endpoint.close().unwrap();
                };
                let first = scope.spawn(move || run(first));
                wait_blocked(&pause);
                let records = endpoints
                    .iter()
                    .map(|e| Arc::clone(&e.record))
                    .collect::<Vec<_>>();
                let workers = endpoints
                    .into_iter()
                    .map(|endpoint| scope.spawn(move || run(endpoint)))
                    .collect::<Vec<_>>();
                wait_until(|| records.iter().all(|r| r.lock().unwrap().work.is_some()));
                release(&pause);
                first.join().unwrap();
                for worker in workers {
                    worker.join().unwrap();
                }
            });
            let stats = runtime.stats().unwrap();
            assert_eq!(
                (stats.opened_cqs, stats.active_cqs, stats.progress_workers),
                (1, 1, 1)
            );
            assert_eq!(
                (stats.peak_work_requests, stats.peak_pending_qps),
                (capacity, capacity)
            );
            assert_eq!(
                (
                    stats.active_endpoints,
                    stats.work_requests,
                    stats.progress_failures
                ),
                (0, 0, 0)
            );
            assert_eq!(stats.posted_work_requests, 16);
            assert_eq!(stats.completed_reads + stats.completed_writes, 16);
            assert_eq!(progress.shared.control.lock().unwrap().next_tag, 4);
            let mut next = runtime.staging_endpoint(8, 0).unwrap();
            assert_eq!(
                next.record.tag, 4,
                "closed QP identities are never recycled"
            );
            next.close().unwrap();
            drop(next);
            runtime.close(deadline()).unwrap();
            let stats = runtime.stats().unwrap();
            assert_eq!(
                (
                    stats.active_cqs,
                    stats.progress_workers,
                    stats.active_devices
                ),
                (0, 0, 0)
            );
        }
    }
}

// Pause at the real owner boundary: accepted by the lane, before QP creation.
fn unopened(runtime: &Runtime) -> Endpoint {
    let device = runtime.device().unwrap();
    let progress = runtime.progress(Arc::clone(&device)).unwrap();
    {
        let mut usage = runtime.usage.lock().unwrap();
        usage.active_endpoints += 1;
        usage.staging_bytes += 8;
    }
    let resources = Box::new(Resources {
        device,
        handles: sys::Handles {
            cq: progress.handle().unwrap(),
            ..Default::default()
        },
        cached_mr: None,
        buffer: Buffer::Staged(Staging::new(8).unwrap()),
        _lease: Lease {
            usage: Arc::clone(&runtime.usage),
            drained: Arc::clone(&runtime.drained),
            length: 8,
            is_direct: false,
        },
        max_read_slices: 64,
        permission: 0,
        window_used: false,
        window_bound: false,
    });
    Endpoint {
        record: progress.register(resources).unwrap(),
        progress,
        local: Descriptor::default(),
        config: runtime.config.clone(),
        verbs: runtime.verbs,
        deadline: deadline(),
        resources: None,
    }
}

#[test]
fn lane_abort_and_retention_fence_registered_but_unopened_qps() {
    for mode in ["cq_pause", "close0"] {
        let runtime = configured(mode, 3);
        let mut opened = runtime.staging_endpoint(8, 0).unwrap();
        let mut paused = unopened(&runtime);
        let progress = Arc::clone(&opened.progress);
        let events = events(&runtime);
        let pause = pause(&runtime);
        let paused_record = Arc::clone(&paused.record);
        if mode == "cq_pause" {
            progress
                .shared
                .fail(Fault::Protocol("injected fatal CQ fault"));
            // All QPs have passed destroy_qp, but CQ destruction and the last
            // abort loop have not run. A late constructor must already fail.
            wait_blocked(&pause);
            let before = events.lock().unwrap().clone();
            let denied = paused.open(0, 1).is_err();
            let unchanged = *events.lock().unwrap() == before;
            release(&pause);
            assert!(
                denied && unchanged,
                "native QP creation after the first close pass"
            );
        } else {
            let guard = paused_record.lock().unwrap();
            progress
                .shared
                .fail(Fault::Protocol("injected fatal CQ fault"));
            // Retain has fenced the first record and is blocked by the last
            // accepted constructor. It cannot publish finished before that fence.
            wait_until(|| opened.record.lock().unwrap().phase == Phase::Retained);
            let finished = progress.shared.control.lock().unwrap().finished;
            opened.deadline = Instant::now() + Duration::from_millis(20);
            assert!(
                matches!(
                    opened.close(),
                    Err(Error::Unquiesced {
                        code: libc::ETIMEDOUT,
                        ..
                    })
                ),
                "retention receipt must wait for all record fences and accounting"
            );
            assert_eq!(runtime.stats().unwrap().retained_endpoints, 0);
            drop(guard);
            assert!(
                !finished,
                "retention published before fencing every constructor"
            );
        }
        wait_until(|| progress.shared.control.lock().unwrap().finished);
        let before = events.lock().unwrap().clone();
        assert!(paused.open(0, 1).is_err());
        assert!(paused.connect(&opened.descriptor()).is_err());
        assert_eq!(*events.lock().unwrap(), before);
        assert_eq!(before.iter().filter(|e| **e == "open_endpoint").count(), 1);
        let retained = mode == "close0";
        assert_eq!(
            matches!(opened.close(), Err(Error::Unquiesced { .. })),
            retained
        );
        assert_eq!(
            matches!(paused.close(), Err(Error::Unquiesced { .. })),
            retained
        );
        drop(paused);
        drop(opened);
        drop(paused_record);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (stats.retained_endpoints, stats.retained_cqs),
            if retained { (2, 1) } else { (0, 0) }
        );
        assert_eq!(stats.progress_failures, 1);
        assert!(runtime.staging_endpoint(8, 0).is_err());
        if retained {
            assert!(matches!(
                runtime.close(deadline()),
                Err(Error::Unquiesced { .. })
            ));
        } else {
            runtime.close(deadline()).unwrap();
        }
    }
}

#[test]
fn close_deadline_returns_unquiesced_while_another_native_call_owns_the_worker() {
    let runtime = configured("pause_post", 3);
    let mut first = runtime.staging_endpoint(8, 0).unwrap();
    first.connect(&first.descriptor()).unwrap();
    let mut second = runtime.staging_endpoint(8, 0).unwrap();
    second.deadline = Instant::now() + Duration::from_millis(30);
    let record = Arc::clone(&second.record);
    let pause = pause(&runtime);
    let worker = thread::spawn(move || {
        first
            .transfer(&first.descriptor(), TransferOp::Read, deadline())
            .unwrap();
        first.close().unwrap();
    });
    wait_blocked(&pause);
    let before = Instant::now();
    assert!(matches!(
        second.close(),
        Err(Error::Unquiesced {
            code: libc::ETIMEDOUT,
            ..
        })
    ));
    assert!(before.elapsed() < Duration::from_millis(500));
    assert!(
        record
            .lock()
            .unwrap()
            .resources
            .as_ref()
            .is_some_and(|r| !r.handles.qp.is_null())
    );
    drop(second); // The lane still holds the actual QP, buffer and charge.
    assert_eq!(runtime.stats().unwrap().active_endpoints, 2);
    release(&pause);
    worker.join().unwrap();
    wait_until(|| record.lock().unwrap().phase == Phase::Closed);
    drop(record);
    runtime.close(deadline()).unwrap();
    assert_eq!(runtime.stats().unwrap().active_endpoints, 0);
}

#[test]
fn a_retained_record_always_returns_unquiesced_after_a_logical_transfer_failure() {
    let runtime = configured("wc_close0", 3);
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    endpoint.connect(&endpoint.descriptor()).unwrap();
    let result = endpoint.transfer(&endpoint.descriptor(), TransferOp::Read, deadline());
    assert!(result.is_err());
    wait_until(|| endpoint.record.lock().unwrap().phase == Phase::Retained);
    // Reproduce the requester window between returning transfer and closing.
    assert!(!endpoint.has_resources());
    assert!(matches!(
        endpoint.close(),
        Err(Error::Unquiesced {
            operation: "ibv_destroy_qp",
            ..
        })
    ));
    let stats = runtime.stats().unwrap();
    assert_eq!((stats.retained_endpoints, stats.retained_cqs), (1, 1));
    assert!(stats.retained_bytes > 0);
}

#[test]
fn completed_outputs_and_quota_move_to_endpoint_despite_live_progress_snapshots() {
    for mode in ["ok", "post"] {
        let runtime = runtime(mode);
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        if mode == "post" {
            endpoint.connect(&endpoint.descriptor()).unwrap();
            assert!(
                endpoint
                    .transfer(&endpoint.descriptor(), TransferOp::Read, deadline())
                    .is_err()
            );
        }
        let snapshot = Arc::clone(&endpoint.record);
        assert_eq!(endpoint.close().is_err(), mode == "post");
        assert!(snapshot.lock().unwrap().resources.is_none());
        assert_eq!(
            runtime.stats().unwrap().active_endpoints,
            1,
            "caller still owns output"
        );
        drop(endpoint);
        assert_eq!(
            runtime.stats().unwrap().active_endpoints,
            0,
            "worker snapshots own no closed resources"
        );
        let mut next = runtime.staging_endpoint(8, 0).unwrap();
        next.close().unwrap();
        drop(next);
        // Even an arbitrarily delayed snapshot cannot defer native-runtime close.
        runtime.close(deadline()).unwrap();
        drop(snapshot);
    }
}

#[test]
fn cq_cleanup_failure_retains_only_still_owned_resources_and_never_retries() {
    for mode in ["close2", "cq_create_close2"] {
        let runtime = runtime(mode);
        let result = runtime.staging_endpoint(8, 0);
        let events = events(&runtime);
        if mode == "close2" {
            let mut endpoint = result.unwrap();
            endpoint.close().unwrap();
            drop(endpoint);
        } else {
            assert!(matches!(result, Err(Error::Unquiesced { .. })));
        }
        for _ in 0..2 {
            assert!(matches!(
                runtime.close(deadline()),
                Err(Error::Unquiesced { .. })
            ));
        }
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.retained_endpoints,
                stats.staging_bytes
            ),
            (0, 0, 0)
        );
        assert_eq!((stats.active_cqs, stats.retained_cqs), (1, 1));
        drop(runtime);
        let events = events.lock().unwrap();
        assert_eq!(events.iter().filter(|e| **e == "destroy_cq").count(), 1);
        assert!(!events.contains(&"dealloc_pd"));
    }
}

#[test]
fn stale_or_wrong_qp_completions_poison_the_lane_without_inventing_success() {
    for mode in ["identity", "wrong_qpn"] {
        let runtime = runtime(mode);
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        endpoint.connect(&endpoint.descriptor()).unwrap();
        assert!(
            endpoint
                .transfer(&endpoint.descriptor(), TransferOp::Read, deadline())
                .is_err()
        );
        let closed = endpoint.close();
        assert!(!matches!(closed, Err(Error::Unquiesced { .. })));
        drop(endpoint);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.completed_reads,
                stats.work_requests,
                stats.progress_failures
            ),
            (0, 0, 1)
        );
        runtime.close(deadline()).unwrap();
    }
}

#[test]
fn unresolved_lane_debt_and_all_dma_loans_survive_until_the_shared_native_barrier() {
    for suffix in ["", "_close0", "_close2"] {
        let runtime = configured(&format!("direct:unknown_qp_pause{suffix}"), 3);
        let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([3; 16]), 1, 16);
        let memory = registry
            .register(
                vec![0x55; 16].into_boxed_slice(),
                crate::Permission::RemoteReadWrite,
            )
            .unwrap();
        let mut active = runtime.endpoint(&memory, 0, 4, 1, deadline()).unwrap();
        let mut passive = runtime.endpoint(&memory, 8, 4, 1, deadline()).unwrap();
        active.connect(&active.descriptor()).unwrap();
        let progress = Arc::clone(&active.progress);
        let pause = pause(&runtime);
        let worker = thread::spawn(move || {
            assert!(
                active
                    .transfer(&active.descriptor(), TransferOp::Read, deadline())
                    .is_err()
            );
            let result = active.close();
            (active, result)
        });
        wait_blocked(&pause);
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.work_requests, 1);
        assert_eq!(stats.active_cqs, 1);
        assert!(memory.read(0, &mut [0; 1]).is_err());
        assert!(memory.write(8, &[9]).is_err());
        memory.write(4, &[7; 4]).unwrap();
        release(&pause);
        let (active, result) = worker.join().unwrap();
        let retained = !suffix.is_empty();
        assert_eq!(matches!(result, Err(Error::Unquiesced { .. })), retained);
        wait_until(|| progress.shared.control.lock().unwrap().finished);
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.work_requests, usize::from(retained));
        assert_eq!(stats.retained_work_requests, usize::from(retained));
        assert_eq!(stats.retained_endpoints, if retained { 2 } else { 0 });
        if retained {
            assert!(memory.read(0, &mut [0; 1]).is_err());
            assert!(memory.write(8, &[9]).is_err());
            assert!(matches!(passive.close(), Err(Error::Unquiesced { .. })));
        } else {
            memory.write(0, &[9; 4]).unwrap();
            memory.write(8, &[9; 4]).unwrap();
            assert!(passive.close().is_err());
        }
        drop(active);
        drop(passive);
        if retained {
            assert!(runtime.close(deadline()).is_err());
        } else {
            runtime.close(deadline()).unwrap();
        }
    }
}

#[path = "hardware.rs"]
mod hardware;

#[test]
fn control_and_data_work_share_one_cq_slot_while_source_revocation_is_pending() {
    let mut runtime = runtime("direct:window_inv_pause");
    runtime.config.max_endpoints = 2;
    runtime.config.max_direct_bytes = 16;
    runtime.config.max_cqes = 1;
    let registry = crate::memory::MemoryRegistry::new(crate::SourceEpoch([1; 16]), 2, 16);
    let memory = registry
        .register(
            vec![0x42; 8].into_boxed_slice(),
            crate::Permission::RemoteReadWrite,
        )
        .unwrap();
    let output = registry
        .register(
            vec![0xcc; 8].into_boxed_slice(),
            crate::Permission::LocalOnly,
        )
        .unwrap();
    let mut source = runtime
        .endpoint(&memory, 0, 8, 2, Instant::now() + Duration::from_secs(3))
        .unwrap();
    source.connect(&source.descriptor()).unwrap();
    source.bind_window().unwrap();
    let mut requester = runtime
        .endpoint(&output, 0, 8, 0, Instant::now() + Duration::from_secs(3))
        .unwrap();
    requester.connect(&requester.descriptor()).unwrap();
    let record = Arc::clone(&requester.record);
    let pause = pause(&runtime);
    let source_worker = std::thread::spawn(move || {
        let result = source.invalidate_window();
        (source, result)
    });
    let (lock, changed) = &*pause;
    let (state, timed) = changed
        .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(2), |s| !s.0)
        .unwrap();
    assert!(!timed.timed_out());
    drop(state);
    let data_worker = std::thread::spawn(move || {
        let remote = requester.descriptor();
        requester
            .transfer(
                &remote,
                TransferOp::Read,
                Instant::now() + Duration::from_secs(3),
            )
            .unwrap();
        requester.close().unwrap();
    });
    let end = Instant::now() + Duration::from_secs(2);
    while record.lock().unwrap().work.is_none() {
        assert!(Instant::now() < end);
        std::thread::yield_now();
    }
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_windows,
            stats.work_requests,
            stats.completed_window_binds,
            stats.completed_window_invalidations
        ),
        (1, 1, 1, 0)
    );
    assert!(memory.write(0, &[0; 8]).is_err());
    lock.lock().unwrap().1 = true;
    changed.notify_all();
    let (mut source, result) = source_worker.join().unwrap();
    result.unwrap();
    source.close().unwrap();
    drop(source);
    data_worker.join().unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.work_requests,
            stats.peak_work_requests,
            stats.completed_reads,
            stats.completed_window_invalidations
        ),
        (0, 1, 1, 1)
    );
    runtime
        .close(Instant::now() + Duration::from_secs(1))
        .unwrap();
}

#[test]
fn lost_async_consumer_fences_next_native_destroy_in_the_same_close_pass() {
    let mut runtime = runtime("direct:async_qp_pause");
    runtime.config.max_endpoints = 2;
    runtime.config.max_direct_bytes = 16;
    let runtime = Arc::new(runtime);
    let mut first = runtime.staging_endpoint(8, 2).unwrap();
    let mut second = runtime.staging_endpoint(8, 2).unwrap();
    let record = Arc::clone(&second.record);
    let pause = crate::rdma::tests::pause(&runtime);
    let events = crate::rdma::tests::events(&runtime);
    let close_first = thread::spawn(move || first.close());
    let (lock, changed) = &*pause;
    let (state, timed) = changed
        .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(2), |s| !s.0)
        .unwrap();
    assert!(!timed.timed_out());
    drop(state);
    let close_second = thread::spawn(move || second.close());
    let deadline = Instant::now() + Duration::from_secs(2);
    while record.actions.load(Ordering::Acquire) & CLOSE == 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    crate::rdma::device::tests::emit_async(&runtime, 255);
    while runtime.stats().unwrap().async_event_workers != 0 {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    lock.lock().unwrap().1 = true;
    changed.notify_all();
    assert!(matches!(
        close_first.join().unwrap(),
        Err(Error::Unquiesced { .. })
    ));
    assert!(matches!(
        close_second.join().unwrap(),
        Err(Error::Unquiesced { .. })
    ));
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_endpoints,
            stats.retained_endpoints,
            stats.retained_windows,
            stats.direct_bytes
        ),
        (1, 1, 1, 8)
    );
    let events = events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|s| **s == "destroy_qp").count(),
        1,
        "no second destroy after consumer loss"
    );
    assert!(!events.contains(&"destroy_cq"));
}
