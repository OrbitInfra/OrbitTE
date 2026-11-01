use super::*;
use crate::transport::rdma::grant::GrantLease;
use crate::transport::rdma::qp::ConnectionLease;
use crate::transport::rdma::tests::{events, pause, runtime};
use crate::transport::rdma::{
    ConnectionResources, GrantResources, RdmaGrant, RdmaTransport, Staging,
};

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

fn configured(mode: &str, capacity: usize) -> RdmaTransport {
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
                let run = |mut endpoint: RdmaGrant| {
                    let remote = endpoint.descriptor();
                    endpoint
                        .transfer(&remote, operation, crate::Priority::High, deadline())
                        .unwrap();
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
                (2, 2, 1)
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
fn unopened(runtime: &RdmaTransport) -> RdmaGrant {
    let device = runtime.device().unwrap();
    let progress = runtime.progress(Arc::clone(&device)).unwrap();
    {
        let mut usage = runtime.usage.lock().unwrap();
        usage.active_endpoints += 1;
        usage.active_grants += 1;
        usage.staging_bytes += 8;
    }
    let resources = Box::new(Resources {
        connection: super::super::qp::RdmaQp::new(ConnectionResources {
            receive_queue: super::super::qp::receive_queue::ReceiveQueue::new(
                runtime.config.max_cqes,
                runtime.config.max_grants,
                Arc::clone(&runtime.usage),
            )
            .unwrap(),
            send_queue: super::super::qp::send_queue::SendQueue::new(
                runtime.config.max_send_wr,
                Arc::clone(&runtime.usage),
                None,
            )
            .unwrap(),
            device: Arc::clone(&device),
            handles: sys::ConnectionHandles {
                cq: progress.handle(false).unwrap(),
                receive_cq: progress.handle(true).unwrap(),
                ..Default::default()
            },
            incarnation: None,
            local: Descriptor::default(),
            remote: None,
            permission: 0,
            closed: false,
            lease: Some(ConnectionLease {
                usage: Arc::clone(&runtime.usage),
                drained: Arc::clone(&runtime.drained),
            }),
        }),
        grant: Some(GrantResources {
            _worker: runtime.workers.reserve(&progress).unwrap(),
            device,
            handles: sys::MemoryHandles::default(),
            cached_mr: None,
            buffer: Buffer::Staged(Staging::new(8).unwrap()),
            _lease: GrantLease {
                _credit: super::super::grant::admission::GrantCredits::acquire(
                    Arc::clone(&runtime.usage),
                    runtime.config.max_grants,
                    1,
                )
                .unwrap()
                .take()
                .unwrap(),
                _selection: runtime
                    .allocate(8, crate::MemoryLocation::Cpu(None), u64::MAX)
                    .unwrap(),
                usage: Arc::clone(&runtime.usage),
                drained: Arc::clone(&runtime.drained),
                length: 8,
                is_direct: false,
            },
            max_read_slices: 64,
            permission: 0,
            generation: 0,
            rkey: 0,
            connection: None,
            window_used: false,
            window_bound: false,
            message: None,
        }),
    });
    RdmaGrant {
        record: progress.register(resources).unwrap(),
        progress,
        local: Descriptor::default(),
        incarnation: None,
        config: runtime.config.clone(),
        deadline: deadline(),
        output: None,
        claim: 1,
        released: false,
        cached_peer: None,
        endpoint: None,
        construction: None,
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
            if retained { (2, 2) } else { (0, 0) }
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
            .transfer(
                &first.descriptor(),
                TransferOp::Read,
                crate::Priority::High,
                deadline(),
            )
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
        record.lock().unwrap().resources.as_ref().is_some_and(|r| !r
            .connection
            .lock()
            .unwrap()
            .handles
            .qp
            .is_null())
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
    let result = endpoint.transfer(
        &endpoint.descriptor(),
        TransferOp::Read,
        crate::Priority::High,
        deadline(),
    );
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
    assert_eq!((stats.retained_endpoints, stats.retained_cqs), (1, 2));
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
                    .transfer(
                        &endpoint.descriptor(),
                        TransferOp::Read,
                        crate::Priority::High,
                        deadline()
                    )
                    .is_err()
            );
        }
        let snapshot = Arc::clone(&endpoint.record);
        assert_eq!(endpoint.close().is_err(), mode == "post");
        assert!(snapshot.lock().unwrap().resources.is_none());
        assert_eq!(
            (
                runtime.stats().unwrap().active_endpoints,
                runtime.stats().unwrap().active_grants
            ),
            (0, 1),
            "physical QP capacity is released while the caller still owns output"
        );
        drop(endpoint);
        assert_eq!(
            (
                runtime.stats().unwrap().active_endpoints,
                runtime.stats().unwrap().active_grants
            ),
            (0, 0),
            "worker snapshots own no closed connection or grant resources"
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
        let remaining = if mode == "close2" { 2 } else { 1 };
        assert_eq!(
            (stats.active_cqs, stats.retained_cqs),
            (remaining, remaining)
        );
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
                .transfer(
                    &endpoint.descriptor(),
                    TransferOp::Read,
                    crate::Priority::High,
                    deadline()
                )
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
        let mut active = runtime
            .endpoint(&memory, 0, 4, 1, deadline(), None)
            .unwrap();
        let mut passive = runtime
            .endpoint(&memory, 8, 4, 1, deadline(), None)
            .unwrap();
        active.connect(&active.descriptor()).unwrap();
        let progress = Arc::clone(&active.progress);
        let pause = pause(&runtime);
        let worker = thread::spawn(move || {
            assert!(
                active
                    .transfer(
                        &active.descriptor(),
                        TransferOp::Read,
                        crate::Priority::High,
                        deadline()
                    )
                    .is_err()
            );
            let result = active.close();
            (active, result)
        });
        wait_blocked(&pause);
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.work_requests, 1);
        assert_eq!(stats.active_cqs, 2);
        assert!(memory.read(0, &mut [0; 1]).is_err());
        assert!(memory.write(8, &[9]).is_err());
        runtime.recover_progress().unwrap();
        assert_eq!(runtime.stats().unwrap().progress_recoveries, 0);
        assert!(Arc::ptr_eq(
            &progress,
            &runtime.progress(runtime.device().unwrap()).unwrap()
        ));
        memory.write(4, &[7; 4]).unwrap();
        release(&pause);
        let (active, result) = worker.join().unwrap();
        let retained = !suffix.is_empty();
        assert_eq!(matches!(result, Err(Error::Unquiesced { .. })), retained);
        wait_until(|| progress.shared.control.lock().unwrap().finished);
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.work_requests, usize::from(retained));
        assert_eq!(stats.retained_work_requests, usize::from(retained));
        assert_eq!(
            stats.retained_endpoints,
            if suffix == "_close0" { 2 } else { 0 }
        );
        assert_eq!(stats.retained_grants, if retained { 2 } else { 0 });
        if retained {
            runtime.recover_progress().unwrap();
            assert_eq!(runtime.stats().unwrap().progress_recoveries, 0);
            assert!(!progress.recoverable().unwrap());
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
        .endpoint(
            &memory,
            0,
            8,
            2,
            Instant::now() + Duration::from_secs(3),
            None,
        )
        .unwrap();
    source.connect(&source.descriptor()).unwrap();
    source.bind_window().unwrap();
    let mut requester = runtime
        .endpoint(
            &output,
            0,
            8,
            0,
            Instant::now() + Duration::from_secs(3),
            None,
        )
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
                crate::Priority::High,
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
    let pause = crate::transport::rdma::tests::pause(&runtime);
    let events = crate::transport::rdma::tests::events(&runtime);
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
    crate::transport::rdma::context::tests::emit_async(&runtime, 255);
    while runtime.stats().unwrap().async_event_sources != 0 {
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
            stats.direct_bytes,
            stats.retained_grants
        ),
        (1, 1, 2, 16, 2)
    );
    let events = events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|s| **s == "destroy_qp").count(),
        1,
        "no second destroy after consumer loss"
    );
    assert!(!events.contains(&"destroy_cq"));
    assert!(
        !events.contains(&"dealloc_window"),
        "consumer loss between QP and MW close retains both grants"
    );
}

#[test]
fn abort_rechecks_async_consumer_after_waiting_for_an_accepted_constructor() {
    let mut runtime = runtime("direct:async_abort_lock_pause");
    runtime.config.max_endpoints = 2;
    runtime.config.max_direct_bytes = 16;
    let mut first = runtime.staging_endpoint(8, 2).unwrap();
    let mut second = runtime.staging_endpoint(8, 2).unwrap();
    let events = crate::transport::rdma::tests::events(&runtime);
    let pause = crate::transport::rdma::tests::pause(&runtime);
    let record = Arc::clone(&second.record);
    let constructor = record.lock().unwrap();
    crate::transport::rdma::context::tests::emit_async(&runtime, 3);
    wait_blocked(&pause);
    crate::transport::rdma::context::tests::emit_async(&runtime, 255);
    wait_until(|| runtime.stats().unwrap().async_event_sources == 0);
    release(&pause);
    drop(constructor);
    wait_until(|| runtime.stats().unwrap().retained_grants == 2);
    assert!(matches!(first.close(), Err(Error::Unquiesced { .. })));
    assert!(matches!(second.close(), Err(Error::Unquiesced { .. })));
    drop(first);
    drop(second);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_endpoints,
            stats.retained_endpoints,
            stats.retained_windows,
            stats.direct_bytes,
            stats.retained_grants
        ),
        (1, 1, 2, 16, 2)
    );
    let events = events.lock().unwrap();
    assert_eq!(events.iter().filter(|s| **s == "destroy_qp").count(), 1);
    assert!(!events.contains(&"destroy_cq"));
    assert!(!events.contains(&"deregister"));
}

pub(super) fn before_abort_lock(shared: &ProgressState, record: &Record) {
    if shared.config.device.as_deref() != Some("async_abort_lock_pause") || record.tag != 1 {
        return;
    }
    let device = Arc::clone(&shared.control.lock().unwrap().cq.as_ref().unwrap()._device);
    let pause = crate::transport::rdma::context::tests::pause_device(&device);
    let (lock, changed) = &*pause;
    let mut state = lock.lock().unwrap();
    state.0 = true;
    changed.notify_all();
    while !state.1 {
        state = changed.wait(state).unwrap();
    }
}

#[test]
fn blocking_native_retirement_detaches_before_destroy_and_keeps_the_store_available() {
    let mut transport = runtime("qp_pause");
    transport.config.max_endpoints = 2;
    transport.config.max_staging_bytes = 24;
    let transport = Arc::new(transport);
    let mut first = transport.staging_endpoint(8, 0).unwrap();
    let progress = Arc::clone(&first.progress);
    let record = Arc::clone(&first.record);
    record.send_namespace.store(1, Ordering::Release);
    let pause = pause(&transport);
    let closer = thread::spawn(move || {
        let result = first.close();
        (first, result)
    });
    let mut second = None;
    let observation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        wait_blocked(&pause);
        {
            let control = progress
                .shared
                .control
                .try_lock()
                .expect("native close must not hold the endpoint-store lock");
            assert!(record.require_live().is_err());
            assert!(Arc::ptr_eq(
                &control.qps.work_owner(1, false).unwrap(),
                &record
            ));
            assert_eq!(control.qps.len(), 1);
        }
        assert_eq!(transport.stats().unwrap().active_endpoints, 1);
        second = Some(transport.staging_endpoint(8, 0).unwrap());
        assert_eq!(transport.stats().unwrap().active_endpoints, 2);
    }));
    release(&pause);
    let retired = closer.join();
    observation.unwrap();
    let (first, result) = retired.unwrap();
    let mut second = second.unwrap();
    result.unwrap();
    assert_eq!(
        (
            transport.stats().unwrap().active_endpoints,
            transport.stats().unwrap().active_grants
        ),
        (1, 2)
    );
    second.close().unwrap();
    drop((first, second));
    transport.close(deadline()).unwrap();
}

#[test]
fn source_receive_progresses_without_sq_debt_and_control_bytes_end_before_output_handoff() {
    let runtime = configured("ok", 1);
    let binding = [0x42; COMPLETION_MESSAGE_SIZE];
    let mut source = runtime.staging_endpoint(8, 2).unwrap();
    source.connect(&source.descriptor()).unwrap();
    source.bind_window().unwrap();
    let bind_namespace = source.record.send_namespace.load(Ordering::Acquire);
    post_receive(&mut source, &binding).unwrap();
    let receive_namespace = source.record.receive_namespace.load(Ordering::Acquire);
    assert!(receive_namespace > bind_namespace);
    let posted = runtime.stats().unwrap();
    assert_eq!(
        (
            posted.work_requests,
            posted.receive_work_requests,
            posted.message_bytes
        ),
        (0, 1, COMPLETION_MESSAGE_SIZE)
    );
    assert!(
        !source
            .record
            .lock()
            .unwrap()
            .receive
            .as_ref()
            .unwrap()
            .completed()
    );

    let mut requester = runtime.staging_endpoint(8, 0).unwrap();
    requester.connect(&requester.descriptor()).unwrap();
    let mut namespaces = vec![bind_namespace, receive_namespace];
    for operation in [TransferOp::Read, TransferOp::Write] {
        requester
            .transfer(
                &requester.descriptor(),
                operation,
                crate::Priority::High,
                deadline(),
            )
            .unwrap();
        namespaces.push(requester.record.send_namespace.load(Ordering::Acquire));
    }
    requester
        .send_completion(&binding, requester.descriptor().rkey)
        .unwrap();
    namespaces.push(requester.record.send_namespace.load(Ordering::Acquire));
    assert!(namespaces.windows(2).all(|pair| pair[0] < pair[1]));
    let calls = runtime.stats().unwrap().post_calls;
    assert!(
        requester
            .transfer(
                &requester.descriptor(),
                TransferOp::Read,
                crate::Priority::High,
                deadline()
            )
            .is_err()
    );
    assert!(
        requester
            .send_completion(&binding, requester.descriptor().rkey)
            .is_err()
    );
    assert_eq!(runtime.stats().unwrap().post_calls, calls);
    assert_eq!(
        runtime.stats().unwrap().receive_work_requests,
        1,
        "SQ CQEs cannot pay RQ debt"
    );
    requester.close().unwrap();
    assert_eq!(
        runtime.stats().unwrap().message_bytes,
        COMPLETION_MESSAGE_SIZE
    );

    crate::transport::rdma::tests::deliver_completion(&source, &binding, |_| {}, false);
    source.receive_completion().unwrap();
    let received = runtime.stats().unwrap();
    assert_eq!(
        (
            received.completed_sends,
            received.completed_receives,
            received.receive_work_requests
        ),
        (1, 1, 0)
    );
    assert_eq!((received.active_cqs, received.progress_workers), (2, 1));
    source.close().unwrap();
    let handed_off = runtime.stats().unwrap();
    assert_eq!(
        (
            handed_off.message_bytes,
            handed_off.active_message_mrs,
            handed_off.active_endpoints,
            handed_off.active_grants
        ),
        (0, 0, 0, 2)
    );
    drop(source);
    drop(requester);
    runtime.close(deadline()).unwrap();
}

#[test]
fn wr_namespace_exhaustion_never_wraps_or_reaches_native_post() {
    for exhausted in [0, u64::MAX / super::super::WR_ID_STRIDE] {
        let runtime = configured("ok", 1);
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        endpoint.connect(&endpoint.descriptor()).unwrap();
        endpoint
            .progress
            .shared
            .next_work_namespace
            .store(exhausted, Ordering::Release);
        assert!(matches!(
            endpoint.transfer(
                &endpoint.descriptor(),
                TransferOp::Read,
                crate::Priority::High,
                deadline()
            ),
            Err(Error::ResourceExhausted)
        ));
        assert_eq!(endpoint.record.send_namespace.load(Ordering::Acquire), 0);
        assert_eq!(
            endpoint
                .progress
                .shared
                .next_work_namespace
                .load(Ordering::Acquire),
            exhausted
        );
        assert_eq!(runtime.stats().unwrap().post_calls, 0);
        endpoint.close().unwrap();
        drop(endpoint);
        runtime.close(deadline()).unwrap();
    }
}

#[test]
fn received_binding_wrong_cq_qp_and_old_wr_cannot_confirm_a_source_grant() {
    for case in ["binding", "old_wr", "qpn", "wrong_cq", "opcode"] {
        let runtime = configured("ok", 1);
        let binding = [0x42; COMPLETION_MESSAGE_SIZE];
        let mut endpoint = runtime.staging_endpoint(8, 2).unwrap();
        endpoint.connect(&endpoint.descriptor()).unwrap();
        endpoint.bind_window().unwrap();
        post_receive(&mut endpoint, &binding).unwrap();
        let mut bytes = binding;
        if case == "binding" {
            bytes[COMPLETION_MESSAGE_SIZE - 1] ^= 1;
        }
        crate::transport::rdma::tests::deliver_completion(
            &endpoint,
            &bytes,
            |wc| match case {
                "old_wr" => wc.id -= super::super::WR_ID_STRIDE,
                "qpn" => wc.qpn += 1,
                "opcode" => wc.opcode = 0,
                _ => {}
            },
            case == "wrong_cq",
        );
        assert!(
            matches!(endpoint.receive_completion(), Err(Error::Protocol(_))),
            "{case}"
        );
        let _ = endpoint.close();
        drop(endpoint);
        let _ = runtime.close(deadline());
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.completed_receives,
                stats.receive_work_requests,
                stats.retained_receive_work_requests,
                stats.message_bytes,
                stats.active_message_mrs
            ),
            (0, 0, 0, 0, 0),
            "{case}"
        );
        assert_eq!(stats.progress_failures, 1, "{case}");
    }
}

#[test]
fn receive_and_message_failures_retain_exact_native_owners_without_drop_retries() {
    for mode in [
        "receive_post_fail",
        "receive_unknown_close0",
        "receive_unknown_receive_cq_close",
        "message_register_fail_close1",
        "message_close",
        "send_completion_unknown_close0",
    ] {
        let runtime = configured(mode, 1);
        let sending = mode.starts_with("send_completion");
        let mut endpoint = runtime
            .staging_endpoint(8, if sending { 0 } else { 2 })
            .unwrap();
        endpoint.connect(&endpoint.descriptor()).unwrap();
        let binding = [0x42; COMPLETION_MESSAGE_SIZE];
        let posted = if sending {
            endpoint
                .transfer(
                    &endpoint.descriptor(),
                    TransferOp::Read,
                    crate::Priority::High,
                    deadline(),
                )
                .unwrap();
            endpoint.send_completion(&binding, endpoint.descriptor().rkey)
        } else {
            endpoint.bind_window().unwrap();
            post_receive(&mut endpoint, &binding)
        };
        if mode == "message_close" {
            posted.unwrap();
            crate::transport::rdma::tests::deliver_completion(&endpoint, &binding, |_| {}, false);
            endpoint.receive_completion().unwrap();
        } else {
            assert!(
                matches!(
                    posted,
                    Err(Error::Backend { .. } | Error::Unquiesced { .. })
                ),
                "{mode}: {posted:?}"
            );
        }
        let events = events(&runtime);
        let close = endpoint.close();
        let unquiesced = mode != "receive_post_fail";
        assert_eq!(
            matches!(close, Err(Error::Unquiesced { .. })),
            unquiesced,
            "{mode}: {close:?}"
        );
        drop(endpoint);
        let _ = runtime.close(deadline());
        let stats = runtime.stats().unwrap();
        if unquiesced {
            assert_eq!(
                (
                    stats.message_bytes,
                    stats.retained_message_bytes,
                    stats.active_message_mrs,
                    stats.retained_message_mrs,
                    stats.retained_grants
                ),
                (COMPLETION_MESSAGE_SIZE, COMPLETION_MESSAGE_SIZE, 1, 1, 1),
                "{mode}"
            );
        } else {
            assert_eq!(
                (
                    stats.message_bytes,
                    stats.active_message_mrs,
                    stats.retained_grants
                ),
                (0, 0, 0)
            );
        }
        if mode.contains("receive_unknown") {
            assert_eq!(
                (
                    stats.receive_work_requests,
                    stats.retained_receive_work_requests
                ),
                (1, 1)
            );
            assert_eq!(
                (stats.active_cqs, stats.retained_cqs),
                if mode.contains("receive_cq_close") {
                    (1, 1)
                } else {
                    (2, 2)
                }
            );
            assert_eq!(
                stats.retained_endpoints,
                usize::from(mode.ends_with("close0"))
            );
        } else {
            assert_eq!(
                (
                    stats.receive_work_requests,
                    stats.retained_receive_work_requests
                ),
                (0, 0)
            );
        }
        if sending {
            assert_eq!(
                (
                    stats.work_requests,
                    stats.retained_work_requests,
                    stats.active_cqs,
                    stats.retained_cqs
                ),
                (1, 1, 2, 2)
            );
        }
        let before = events.lock().unwrap().clone();
        drop(runtime);
        assert_eq!(
            *events.lock().unwrap(),
            before,
            "{mode}: retained owners must not retry native destruction from Drop"
        );
    }
}

#[test]
fn second_cq_partial_construction_cleans_only_proven_native_owners() {
    for mode in ["receive_cq_create", "receive_cq_create_close2"] {
        let runtime = configured(mode, 1);
        let result = runtime.staging_endpoint(8, 0);
        let stats = runtime.stats().unwrap();
        if mode.ends_with("close2") {
            assert!(matches!(result, Err(Error::Unquiesced { .. })));
            assert_eq!(
                (
                    stats.active_cqs,
                    stats.retained_cqs,
                    stats.active_devices,
                    stats.retained_devices
                ),
                (2, 2, 1, 0)
            );
            let log = events(&runtime);
            assert!(matches!(
                runtime.close(deadline()),
                Err(Error::Unquiesced { .. })
            ));
            drop(runtime);
            assert!(!log.lock().unwrap().contains(&"dealloc_pd"));
        } else {
            assert!(matches!(result, Err(Error::Backend { .. })));
            assert_eq!(
                (
                    stats.active_cqs,
                    stats.retained_cqs,
                    stats.active_devices,
                    stats.retained_devices
                ),
                (0, 0, 1, 0)
            );
            runtime.close(deadline()).unwrap();
            assert_eq!(runtime.stats().unwrap().active_devices, 0);
        }
        assert_eq!(
            (
                stats.active_endpoints,
                stats.active_grants,
                stats.message_bytes
            ),
            (0, 0, 0)
        );
    }
}

static IDLE_DEBIT: std::sync::OnceLock<(Mutex<(bool, bool)>, Condvar)> = std::sync::OnceLock::new();

pub(super) fn before_idle_debit(shared: &ProgressState) {
    if shared.config.device.as_deref() != Some("copy_idle_debit_pause") {
        return;
    }
    let (lock, changed) = IDLE_DEBIT.get_or_init(Default::default);
    let mut state = lock.lock().unwrap();
    state.0 = true;
    changed.notify_all();
    while !state.1 {
        state = changed.wait(state).unwrap();
    }
}

#[test]
fn idle_detachment_and_charge_debit_are_atomic_against_lane_failure() {
    use crate::engine::tests::{rdma_fixture, source_runtime};
    use crate::{Permission, TransferRequest, Transport};
    for terminal_close in [false, true] {
        *IDLE_DEBIT.get_or_init(Default::default).0.lock().unwrap() = (false, false);
        let source = rdma_fixture("direct:copy");
        let requester = rdma_fixture("direct:copy_idle_debit_pause");
        let local = requester
            .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(vec![0x29; 8].into_boxed_slice(), Permission::RemoteRead)
            .unwrap();
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(3),
            )
            .unwrap();
        assert!(
            requester
                .submit_and_wait(
                    TransferOp::Read,
                    &peer,
                    &[TransferRequest {
                        priority: Default::default(),
                        remote_staging: false,
                        transport: Transport::Rdma,
                        local_address: local.base_address(),
                        remote_address: remote.base_address(),
                        length: 8,
                    }],
                    Duration::from_secs(3)
                )
                .unwrap()
                .is_complete()
        );
        let runtime = source_runtime(&requester);
        let progress = runtime.progress(runtime.device().unwrap()).unwrap();
        let mut records = Vec::new();
        progress
            .shared
            .control
            .lock()
            .unwrap()
            .qps
            .snapshot(&mut records);
        assert_eq!(records.len(), 1);
        let record = records.remove(0);
        let claim = record.lock().unwrap().claim;
        let started = AtomicBool::new(false);
        thread::scope(|scope| {
            let progress = &progress;
            let close = scope.spawn(|| {
                if terminal_close {
                    progress.close_record(&record, claim, deadline())
                } else {
                    progress.retire_idle(&record, claim, deadline())
                }
            });
            let (lock, changed) = IDLE_DEBIT.get().unwrap();
            let (state, timed) = changed
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(2), |s| !s.0)
                .unwrap();
            assert!(!timed.timed_out());
            let atomic = progress.shared.control.try_lock().is_err();
            let failed = scope.spawn(|| {
                started.store(true, Ordering::Release);
                progress
                    .shared
                    .fail(Fault::Protocol("concurrent idle retirement fault"));
            });
            while !started.load(Ordering::Acquire) {
                thread::yield_now();
            }
            drop(state);
            lock.lock().unwrap().1 = true;
            changed.notify_all();
            let _ = close.join().unwrap();
            failed.join().unwrap();
            assert!(
                atomic,
                "store detachment and counter debit need one critical section"
            );
        });
        requester.close(Duration::from_secs(3)).unwrap();
        let stats = requester.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.idle_endpoints,
                stats.retained_endpoints,
                stats.active_grants
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(
            (
                stats.opened_connections,
                stats.closed_connections,
                stats.progress_failures
            ),
            (1, 1, 1)
        );
        source.close(Duration::from_secs(3)).unwrap();
    }
}

#[test]
fn exhausted_local_claim_token_retires_the_actual_idle_qp_without_wrapping() {
    use crate::engine::tests::{rdma_fixture, source_runtime};
    use crate::{Permission, TransferRequest, Transport};
    let source = rdma_fixture("direct:copy");
    let requester = rdma_fixture("direct:copy");
    let local = requester
        .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(vec![0x29; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            Duration::from_secs(3),
        )
        .unwrap();
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[TransferRequest {
                    priority: Default::default(),
                    remote_staging: false,
                    transport: Transport::Rdma,
                    local_address: local.base_address(),
                    remote_address: remote.base_address(),
                    length: 8,
                }],
                Duration::from_secs(3)
            )
            .unwrap()
            .is_complete()
    );
    let runtime = source_runtime(&requester);
    let progress = runtime.progress(runtime.device().unwrap()).unwrap();
    let mut records = Vec::new();
    progress
        .shared
        .control
        .lock()
        .unwrap()
        .qps
        .snapshot(&mut records);
    let record = records.remove(0);
    record.lock().unwrap().claim = u64::MAX;
    assert!(matches!(
        progress.claim(|_| true, deadline()),
        Err(Error::ResourceExhausted)
    ));
    let state = record.lock().unwrap();
    assert_eq!(state.claim, u64::MAX);
    assert!(state.phase == Phase::Closed);
    assert!(record.terminal.load(Ordering::Acquire));
    drop(state);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.opened_connections,
            stats.closed_connections,
            stats.reused_connections
        ),
        (1, 1, 0)
    );
    assert_eq!(
        (
            stats.active_endpoints,
            stats.idle_endpoints,
            stats.retained_endpoints,
            stats.active_grants
        ),
        (0, 0, 0, 0)
    );
    requester.close(Duration::from_secs(3)).unwrap();
    source.close(Duration::from_secs(3)).unwrap();
}

#[test]
fn completion_event_ack_precedes_progress_lock_and_cq_destruction_wait() {
    let runtime = super::super::cq::tests::event_runtime("completion_destroy_event");
    let device = runtime.device().unwrap();
    let progress = runtime.progress(Arc::clone(&device)).unwrap();
    let mut extra = RdmaCq::new(
        device,
        Arc::clone(&runtime.usage),
        1,
        progress.shared.completion_events.clone(),
        runtime.verbs,
    )
    .unwrap();
    let locked = progress.shared.control.lock().unwrap();
    extra.close(runtime.verbs).unwrap();
    drop(extra);
    drop(locked);
    runtime.close(deadline()).unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_cqs,
            stats.active_completion_channels,
            stats.completion_event_sources,
            stats.active_devices
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(stats.completion_events, 3);
}

#[test]
fn completion_event_burst_is_acked_while_progress_or_budget_is_locked() {
    for (control, usage) in [(true, false), (false, true), (true, true)] {
        let runtime = super::super::cq::tests::event_runtime("completion_destroy_burst");
        let device = runtime.device().unwrap();
        let progress = runtime.progress(Arc::clone(&device)).unwrap();
        let mut extra = RdmaCq::new(
            device,
            Arc::clone(&runtime.usage),
            1,
            progress.shared.completion_events.clone(),
            runtime.verbs,
        )
        .unwrap();
        let locked = control.then(|| progress.shared.control.lock().unwrap());
        let budget = usage.then(|| runtime.usage.lock().unwrap());
        let result = extra.close(runtime.verbs);
        drop(budget);
        drop(locked);
        if result.is_err() {
            extra.retain();
        } else {
            drop(extra);
        }
        result.unwrap_or_else(|error| {
            panic!("CQ event burst blocked: control={control}, usage={usage}, error={error:?}")
        });
        runtime.close(deadline()).unwrap();
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.completion_events, 24);
        assert_eq!(
            (
                stats.active_cqs,
                stats.active_completion_channels,
                stats.completion_event_sources
            ),
            (0, 0, 0)
        );
    }
}

#[test]
fn asynchronous_event_burst_is_acked_while_progress_or_budget_is_locked() {
    for (control, usage) in [(true, false), (false, true), (true, true)] {
        let runtime = runtime("async_ok");
        let device = runtime.device().unwrap();
        let progress = runtime.progress(device).unwrap();
        let native = events(&runtime);
        let locked = control.then(|| progress.shared.control.lock().unwrap());
        let budget = usage.then(|| runtime.usage.lock().unwrap());
        for _ in 0..8 {
            super::super::context::tests::emit_async(
                &runtime,
                ibverbs_sys::IBV_EVENT_COMM_EST as u8,
            );
        }
        let until = deadline();
        let acknowledged = loop {
            let count = native
                .lock()
                .unwrap()
                .iter()
                .filter(|event| **event == "async_ack")
                .count();
            if count == 8 || Instant::now() >= until {
                break count;
            }
            thread::yield_now();
        };
        drop(budget);
        drop(locked);
        runtime.close(deadline()).unwrap();
        assert_eq!(acknowledged, 8, "control={control}, usage={usage}");
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.acknowledged_async_events, 8);
        assert_eq!(
            (
                stats.active_cqs,
                stats.active_devices,
                stats.async_event_sources
            ),
            (0, 0, 0)
        );
    }
}

fn post_receive(
    endpoint: &mut super::super::RdmaGrant,
    binding: &[u8; COMPLETION_MESSAGE_SIZE],
) -> crate::Result<()> {
    let credits = super::super::receive::ReceiveCredits::acquire(
        Arc::clone(&endpoint.progress.shared.usage),
        endpoint.progress.shared.config.max_cqes,
        1,
    )?;
    endpoint.post_receive(binding, credits)
}

#[test]
fn drained_progress_replacement_preserves_pd_and_rejects_old_owners() {
    for (mode, count) in [
        ("copy", 1),
        ("events:copy", 1),
        ("copy", 3),
        ("events:copy", 3),
    ] {
        let mut runtime = if mode.starts_with("events:") {
            crate::transport::rdma::tests::event_runtime("copy")
        } else {
            configured(mode, 3)
        };
        runtime.config.num_lanes = count;
        let device = runtime.device().unwrap();
        let mut identities = std::collections::HashSet::new();
        for generation in 0..3 {
            let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
            endpoint.connect(&endpoint.descriptor()).unwrap();
            let identity = endpoint.incarnation().unwrap();
            assert!(identities.insert(identity.encode()));
            assert_eq!(identity.domain, device.domain().unwrap());
            let progress = Arc::clone(&endpoint.progress);
            endpoint
                .transfer(
                    &endpoint.descriptor(),
                    TransferOp::Read,
                    crate::Priority::High,
                    deadline(),
                )
                .unwrap();
            progress
                .shared
                .fail(Fault::Backend("controlled progress failure", libc::EIO));
            wait_until(|| progress.recoverable().unwrap());
            assert!(progress.recoverable().unwrap());
            let _ = endpoint.close();
            drop(endpoint);
            thread::scope(|scope| {
                for _ in 0..8 {
                    scope.spawn(|| runtime.recover_progress().unwrap());
                }
            });
            let stats = runtime.stats().unwrap();
            assert_eq!(
                (stats.progress_failures, stats.progress_recoveries),
                (generation + 1, generation + 1)
            );
            assert_eq!(
                (stats.work_requests, stats.active_cqs, stats.progress_lanes),
                (0, 2 * (count - 1), count - 1)
            );
            assert!(Arc::ptr_eq(&device, &runtime.device().unwrap()));
            assert!(progress.handle(false).is_err());
            assert_eq!(stats.progress_workers, count);
            assert_eq!(stats.opened_progress_workers, count as u64);
        }
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        endpoint.connect(&endpoint.descriptor()).unwrap();
        endpoint
            .transfer(
                &endpoint.descriptor(),
                TransferOp::Write,
                crate::Priority::High,
                deadline(),
            )
            .unwrap();
        endpoint.close().unwrap();
        drop(endpoint);
        runtime.close(deadline()).unwrap();
    }
}

#[test]
fn context_async_fence_wakes_every_parked_cq_lane() {
    let mut runtime = crate::transport::rdma::tests::event_runtime("copy");
    runtime.config.num_lanes = 3;
    let device = runtime.device().unwrap();
    let lanes = runtime.cq_list(Arc::clone(&device)).unwrap();
    // Each lane has no records and can park indefinitely in event mode.
    device.asynchronous.fence_route();
    device.asynchronous.notify();
    wait_until(|| lanes.iter().all(|lane| lane.recoverable().unwrap()));
    assert_eq!(runtime.stats().unwrap().progress_failures, 3);
    for lane in lanes {
        lane.close(deadline()).unwrap();
    }
    let _ = runtime.close(deadline());
}

impl CqLane {
    pub(in crate::transport::rdma) fn records(&self) -> Vec<Arc<Record>> {
        let mut records = Vec::new();
        self.shared
            .control
            .lock()
            .unwrap()
            .qps
            .snapshot(&mut records);
        records
    }
}

#[path = "grant_records.rs"]
mod grant_records;

#[test]
fn periodic_reclaim_removes_drained_faulted_lanes_without_port_recovery() {
    let runtime = configured("ok", 3);
    let mut grant = runtime.staging_endpoint(8, 0).unwrap();
    let owner = runtime.device().unwrap();
    grant
        .progress
        .shared
        .fail(Fault::Backend("controlled CQ failure", libc::EIO));
    wait_until(|| runtime.stats().unwrap().progress_recoveries == 1);
    assert_eq!(runtime.stats().unwrap().route_recoveries, 0);
    assert!(owner.asynchronous.check().is_ok());
    assert!(
        runtime.context_set.lock().unwrap().slots[0]
            .cq_list
            .iter()
            .all(Option::is_none)
    );
    let _ = grant.close();
    drop(grant);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[cfg(feature = "cuda")]
pub(in crate::transport::rdma) fn before_staging_abort(
    runtime: &RdmaTransport,
    endpoint: &RdmaGrant,
) {
    if runtime.config.device.as_deref() == Some("copy_staging_close_fault") {
        assert_eq!(runtime.proxy.stats().unwrap().retained_chunks, 1);
        endpoint.record.lock().unwrap().failure =
            Some(Fault::Backend("injected drained CQ failure", 5));
    }
}

#[path = "cq_lane/retirement.rs"]
mod retirement;
