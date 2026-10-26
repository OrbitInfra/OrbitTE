//! Observe the actual native CQ caller, without manufacturing completions.
use std::collections::BTreeMap;
use std::ffi::CStr;

use super::*;

type Owners = BTreeMap<(usize, usize), (String, String)>;
static OWNERS: OnceLock<Mutex<Owners>> = OnceLock::new();

pub(super) fn count() -> usize {
    let count = env::var("ORBITTE_RDMA_LANES")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap();
    assert!((1..=64).contains(&count));
    count
}

pub(super) fn reset() {
    OWNERS.get_or_init(Default::default).lock().unwrap().clear();
}

pub(super) unsafe fn observe(handles: *const sys::ConnectionHandles) {
    let cq = unsafe { (*handles).cq.cast::<ibverbs_sys::ibv_cq>() };
    assert!(!cq.is_null());
    let context = unsafe { (*cq).context };
    let device = unsafe { CStr::from_ptr((*(*context).device).name.as_ptr()) }
        .to_str()
        .unwrap();
    let rails = env::var("ORBITTE_SHARED_RAILS").unwrap();
    let nic = rails.split(',').position(|name| name == device).unwrap();
    let lane_id = crate::transport::rdma::tests::cq_lane_owner(cq as usize);
    let current = thread::current();
    let thread = format!("{:?}", current.id());
    let name = current.name().unwrap();
    assert_eq!(
        name,
        format!("orbitte-rdma-{}", lane_id),
        "native CQ was polled by a foreign worker"
    );
    let key = (context as usize, cq as usize);
    let mut owners = OWNERS.get_or_init(Default::default).lock().unwrap();
    if let Some(previous) = owners.get(&key) {
        assert_eq!(
            previous,
            &(thread, name.into()),
            "native CQ changed polling owner"
        );
    } else {
        println!(
            "ORBITTE_WORKER_CQ {{\"context\":{},\"cq\":{},\"nic_id\":{nic},\"lane_id\":{lane_id},\"device\":\"{device}\",\"worker\":\"{name}\",\"thread\":\"{thread}\"}}",
            key.0, key.1
        );
        owners.insert(key, (thread, name.into()));
    }
}

pub(super) fn running(engine: &crate::TransferEngine) {
    let stats = engine.stats().unwrap().rdma.unwrap();
    monitor_stats(&stats);
    let closed = crate::engine::tests::source_runtime(engine)
        .context_set
        .lock()
        .unwrap()
        .closed;
    if stats.opened_progress_workers != 0 {
        assert_eq!(
            stats.opened_progress_workers,
            count() as u64,
            "context recovery restarted the transport pool"
        );
        assert_eq!(stats.progress_workers, if closed { 0 } else { count() });
    }
    let loads = crate::engine::tests::source_runtime(engine)
        .workers
        .inflight_slices();
    println!(
        "ORBITTE_WORKER_LOAD_END {{\"phase\":\"{}\",\"loads\":{loads:?}}}",
        if closed { "closed" } else { "running" }
    );
    if closed {
        assert_eq!((stats.progress_lanes, stats.active_cqs), (0, 0));
    }
    let phase = if closed { "closed" } else { "running" };
    println!(
        "ORBITTE_WORKERS_STATS {{\"phase\":\"{phase}\",\"configured\":{},\"workers\":{},\"opened\":{},\"lanes\":{},\"recoveries\":{}}}",
        count(),
        stats.progress_workers,
        stats.opened_progress_workers,
        stats.progress_lanes,
        stats.progress_recoveries
    );
}

pub(super) fn closed(stats: &RdmaStats) {
    monitor_stats(stats);
    assert_eq!((stats.progress_workers, stats.progress_lanes), (0, 0));
    assert!(stats.opened_progress_workers == 0 || stats.opened_progress_workers == count() as u64);
    println!(
        "ORBITTE_WORKERS_STATS {{\"phase\":\"closed\",\"configured\":{},\"workers\":0,\"opened\":{},\"lanes\":0}}",
        count(),
        stats.opened_progress_workers
    );
}

pub(super) fn admitted(engine: &crate::TransferEngine, expected: usize) {
    let runtime = crate::engine::tests::source_runtime(engine);
    let loads = runtime.workers.inflight_slices();
    assert_eq!(loads.len(), count());
    assert_eq!(
        loads.iter().sum::<usize>(),
        expected,
        "admitted grants lost worker load"
    );
    endpoint_arrays(runtime, expected);
    let fresh = runtime.stats().unwrap().reused_connections == 0;
    if fresh {
        assert!(
            loads.iter().max().unwrap() - loads.iter().min().unwrap() <= 1,
            "fresh slices ignored the least-loaded worker: {loads:?}"
        );
    }
    println!("ORBITTE_WORKER_LOAD {{\"fresh\":{fresh},\"slices\":{expected},\"loads\":{loads:?}}}");
}

fn monitor_stats(stats: &RdmaStats) {
    assert!(stats.monitor_workers <= 1 && stats.opened_monitor_workers <= 1);
    if stats.active_devices == 0 {
        assert_eq!(stats.monitor_workers, 0);
    }
    println!(
        "ORBITTE_MONITOR_STATS {{\"workers\":{},\"opened\":{},\"async_sources\":{},\"completion_sources\":{}}}",
        stats.monitor_workers,
        stats.opened_monitor_workers,
        stats.async_event_sources,
        stats.completion_event_sources
    );
}

fn endpoint_arrays(runtime: &RdmaTransport, expected: usize) {
    let contexts = runtime.context_set.lock().unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let mut active = 0;
    for (context, slot) in contexts.slots.iter().enumerate() {
        let mut records = Vec::new();
        for progress in slot.cq_list.iter().flatten() {
            progress
                .shared
                .control
                .lock()
                .unwrap()
                .qps
                .snapshot(&mut records);
        }
        for endpoint in slot.endpoints.native_endpoints() {
            let mut counts = vec![0; count()];
            let mut busy = 0;
            let mut max_grants = 0;
            for (record, lane, peer) in endpoint.native_qps() {
                assert!(
                    seen.insert(Arc::as_ptr(&record.connection) as usize),
                    "one physical QP belongs to two peer endpoints"
                );
                let connection = record.connection.lock().unwrap();
                if connection.closed {
                    continue;
                }
                assert_eq!(connection.incarnation, Some(peer.local));
                assert_eq!(
                    crate::transport::rdma::tests::cq_lane_owner(connection.handles.cq as usize),
                    lane
                );
                drop(connection);
                counts[lane] += 1;
                let grants = records
                    .iter()
                    .filter(|entry| Arc::ptr_eq(&entry.connection, &record.connection))
                    .filter(|entry| {
                        entry
                            .lock()
                            .unwrap()
                            .resources
                            .as_ref()
                            .is_some_and(|resources| resources.grant.is_some())
                    })
                    .count();
                busy += grants;
                max_grants = max_grants.max(grants);
            }
            active += busy;
            println!(
                "ORBITTE_ENDPOINT_ARRAY {{\"context\":{context},\"endpoint\":{},\"qps\":{},\"active\":{busy},\"max_grants_per_qp\":{max_grants},\"lanes\":{counts:?}}}",
                Arc::as_ptr(&endpoint) as usize,
                counts.iter().sum::<usize>()
            );
        }
        assert!(records.iter().all(|record| {
            record
                .lock()
                .unwrap()
                .resources
                .as_ref()
                .is_none_or(|r| r.grant.is_none())
                || seen.contains(&(Arc::as_ptr(&record.connection) as usize))
        }));
    }
    assert_eq!(
        active, expected,
        "admitted grants escaped persistent peer QP arrays"
    );
}
