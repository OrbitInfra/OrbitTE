use super::*;
use crate::transport::rdma::context::RdmaContext;
use crate::transport::rdma::tests::VERBS;
use crate::transport::rdma::{RdmaConfig, events};

#[test]
fn pool_size_is_independent_of_contexts_and_empty_workers_join() {
    for count in [1, 2, 4] {
        let config = RdmaConfig {
            num_lanes: count,
            ..RdmaConfig::new("copy")
        };
        let usage: Arc<Mutex<RdmaStats>> = Arc::default();
        let quota = Arc::default();
        let counters: Arc<events::Counters> = Arc::default();
        let workers = Arc::new(Workers::default());
        let mut contexts = Vec::new();
        let mut progress = Vec::new();
        for id in [0, 2, 5] {
            let context =
                RdmaContext::open(&config, &VERBS, Arc::clone(&usage), Arc::clone(&counters))
                    .unwrap();
            let state = CqLane::new(
                &config,
                Arc::clone(&workers),
                Arc::clone(&context),
                Arc::clone(&usage),
                Arc::clone(&quota),
                &VERBS,
            )
            .unwrap();
            workers.attach(id % count, &state).unwrap();
            contexts.push(context);
            progress.push(state);
        }
        let stats = *usage.lock().unwrap();
        assert_eq!(
            (
                stats.progress_workers,
                stats.opened_progress_workers,
                stats.progress_lanes,
                stats.active_cqs
            ),
            (count, count as u64, 3, 6)
        );
        let pool_threads: Vec<_> = workers
            .pool
            .lock()
            .unwrap()
            .workers
            .iter()
            .map(|(_, handle)| handle.thread().id())
            .collect();
        for state in &progress {
            state
                .close(Instant::now() + Duration::from_secs(3))
                .unwrap();
        }
        let stats = *usage.lock().unwrap();
        assert_eq!(
            (
                stats.progress_lanes,
                stats.active_cqs,
                stats.progress_workers
            ),
            (0, 0, count)
        );
        let replacement = CqLane::new(
            &config,
            Arc::clone(&workers),
            Arc::clone(&contexts[1]),
            Arc::clone(&usage),
            quota,
            &VERBS,
        )
        .unwrap();
        workers.attach(2 % count, &replacement).unwrap();
        assert_eq!(
            workers
                .pool
                .lock()
                .unwrap()
                .workers
                .iter()
                .map(|(_, handle)| handle.thread().id())
                .collect::<Vec<_>>(),
            pool_threads
        );
        assert_eq!(usage.lock().unwrap().opened_progress_workers, count as u64);
        // Transport shutdown owns both the live CQ pair and otherwise idle threads.
        workers
            .close(Instant::now() + Duration::from_secs(3))
            .unwrap();
        assert!(replacement.handle(false).is_err());
        assert!(workers.attach(2 % count, &replacement).is_err());
        replacement
            .close(Instant::now() + Duration::from_secs(3))
            .unwrap();
        let stats = *usage.lock().unwrap();
        assert_eq!(
            (
                stats.progress_workers,
                stats.progress_lanes,
                stats.active_cqs
            ),
            (0, 0, 0)
        );
        for context in contexts {
            context.close().unwrap();
        }
    }
}

#[test]
fn invalid_worker_counts_fail_before_provider_discovery() {
    for count in [0, 65, usize::MAX] {
        let config = RdmaConfig {
            num_lanes: count,
            ..RdmaConfig::default()
        };
        assert!(matches!(
            crate::transport::rdma::RdmaTransport::new(
                config,
                #[cfg(feature = "cuda")]
                crate::transport::rdma::tests::proxy()
            ),
            Err(Error::InvalidConfig(_))
        ));
    }
}

static PANIC_GATE: OnceLock<(Mutex<(bool, bool)>, Condvar)> = OnceLock::new();

pub(super) fn before_tick(state: &ProgressState) {
    if state.config.device.as_deref() != Some("panic_worker") {
        return;
    }
    let (lock, changed) = PANIC_GATE.get_or_init(Default::default);
    let mut gate = lock.lock().unwrap();
    gate.0 = true;
    changed.notify_all();
    while !gate.1 {
        gate = changed.wait(gate).unwrap();
    }
    drop(gate);
    panic!("controlled data-worker panic before native progress");
}

#[test]
fn unexpected_worker_exit_retains_active_and_pending_contexts_and_fences_assignment() {
    let config = RdmaConfig {
        num_lanes: 2,
        ..RdmaConfig::new("panic_worker")
    };
    let usage: Arc<Mutex<RdmaStats>> = Arc::default();
    let quota = Arc::default();
    let counters = Arc::default();
    let workers = Arc::new(Workers::default());
    let mut states = Vec::new();
    for id in [0, 2] {
        let context =
            RdmaContext::open(&config, &VERBS, Arc::clone(&usage), Arc::clone(&counters)).unwrap();
        let state = CqLane::new(
            &config,
            Arc::clone(&workers),
            context,
            Arc::clone(&usage),
            Arc::clone(&quota),
            &VERBS,
        )
        .unwrap();
        workers.attach(id % config.num_lanes, &state).unwrap();
        states.push(state);
        if id == 0 {
            let (lock, changed) = PANIC_GATE.get_or_init(Default::default);
            let (gate, timed) = changed
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(3), |gate| !gate.0)
                .unwrap();
            assert!(!timed.timed_out() && gate.0);
        }
    }
    let (lock, changed) = PANIC_GATE.get().unwrap();
    lock.lock().unwrap().1 = true;
    changed.notify_all();
    for state in &states {
        assert!(matches!(
            state.close(Instant::now() + Duration::from_secs(3)),
            Err(Error::Unquiesced { .. })
        ));
        assert!(!state.recoverable().unwrap());
    }
    let healthy = RdmaConfig {
        num_lanes: 2,
        ..RdmaConfig::new("copy")
    };
    let context = RdmaContext::open(&healthy, &VERBS, Arc::clone(&usage), counters).unwrap();
    let state = CqLane::new(
        &healthy,
        Arc::clone(&workers),
        Arc::clone(&context),
        Arc::clone(&usage),
        quota,
        &VERBS,
    )
    .unwrap();
    assert!(matches!(
        workers.attach(0, &state),
        Err(Error::Unquiesced { .. })
    ));
    workers.attach(1, &state).unwrap();
    state
        .close(Instant::now() + Duration::from_secs(3))
        .unwrap();
    assert!(matches!(
        workers.close(Instant::now() + Duration::from_secs(3)),
        Err(Error::Unquiesced { .. })
    ));
    let stats = *usage.lock().unwrap();
    assert_eq!(
        (
            stats.progress_workers,
            stats.progress_lanes,
            stats.retained_cqs,
            stats.active_cqs,
            stats.progress_failures
        ),
        (0, 0, 4, 4, 2)
    );
    context.close().unwrap();
}

impl Workers {
    pub(crate) fn inflight_slices(&self) -> Vec<usize> {
        self.pool
            .lock()
            .unwrap()
            .workers
            .iter()
            .map(|(worker, _)| worker.inflight_slices.load(Ordering::Acquire))
            .collect()
    }
}

#[test]
fn concurrent_dispatch_balances_admitted_slices_and_counts_completed_grant_owners() {
    let mut runtime = crate::transport::rdma::tests::runtime("copy");
    runtime.config.num_lanes = 3;
    runtime.config.max_endpoints = 32;
    runtime.config.max_staging_bytes = 256;
    let mut endpoints = thread::scope(|scope| {
        let tasks: Vec<_> = (0..14)
            .map(|_| scope.spawn(|| runtime.staging_endpoint(8, 0).unwrap()))
            .collect();
        tasks
            .into_iter()
            .map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    let mut loads = runtime.workers.inflight_slices();
    loads.sort_unstable();
    assert_eq!(
        loads,
        [4, 5, 5],
        "concurrent admission must include already admitted slices"
    );
    let busiest = endpoints
        .iter()
        .map(|endpoint| &endpoint.progress)
        .max_by_key(|lane| runtime.workers.inflight_slices()[*lane.worker_id.get().unwrap()])
        .unwrap();
    let pinned: Vec<_> = (0..7)
        .map(|_| runtime.workers.reserve(busiest).unwrap())
        .collect();
    let before = runtime.workers.inflight_slices();
    let (selected, extra) = runtime
        .workers
        .select(&runtime.cq_list(runtime.device().unwrap()).unwrap())
        .unwrap();
    assert_eq!(
        before[*selected.worker_id.get().unwrap()],
        *before.iter().min().unwrap(),
        "dispatch ignored load held by grants pinned to a cached QP lane"
    );
    drop(extra);
    drop(pinned);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_devices,
            stats.progress_lanes,
            stats.active_cqs,
            stats.progress_workers
        ),
        (1, 3, 6, 3)
    );
    for endpoint in &mut endpoints {
        endpoint.connect(&endpoint.descriptor()).unwrap();
        endpoint
            .transfer(
                &endpoint.descriptor(),
                crate::TransferOp::Read,
                Instant::now() + Duration::from_secs(3),
            )
            .unwrap();
    }
    assert_eq!(
        runtime.workers.inflight_slices().iter().sum::<usize>(),
        14,
        "a data CQE does not release the enclosing grant owner"
    );
    for mut endpoint in endpoints {
        endpoint.close().unwrap();
    }
    assert_eq!(runtime.workers.inflight_slices(), [0, 0, 0]);
    runtime
        .close(Instant::now() + Duration::from_secs(3))
        .unwrap();
}

pub(super) fn observe_affinity(affinity: &crate::platform::cpu_probe::CpuAffinity) {
    let actual = crate::platform::cpu_probe::allowed_cpus().unwrap();
    assert_eq!(
        actual, affinity.cpus,
        "RDMA data worker ignored its NUMA CPU mask"
    );
    if std::env::var_os("ORBITTE_SHARED_RAILS").is_some() {
        let node = affinity.node.map_or("null".into(), |node| node.to_string());
        println!(
            "ORBITTE_WORKER_AFFINITY {{\"worker\":\"{}\",\"thread\":\"{:?}\",\"node\":{node},\"cpus\":{actual:?}}}",
            thread::current().name().unwrap(),
            thread::current().id()
        );
    }
}
