//! Observe the actual native CQ caller, without manufacturing completions.
use std::collections::BTreeMap;
use std::ffi::CStr;

use super::*;

type Owners = BTreeMap<(usize, usize), (String, String)>;
static OWNERS: OnceLock<Mutex<Owners>> = OnceLock::new();

pub(super) fn count() -> usize {
    let count = env::var("ORBITTE_RDMA_WORKERS")
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
    let current = thread::current();
    let thread = format!("{:?}", current.id());
    let name = current.name().unwrap();
    assert_eq!(
        name,
        format!("orbitte-rdma-{}", nic % count()),
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
            "ORBITTE_WORKER_CQ {{\"context\":{},\"cq\":{},\"nic_id\":{nic},\"device\":\"{device}\",\"worker\":\"{name}\",\"thread\":\"{thread}\"}}",
            key.0, key.1
        );
        owners.insert(key, (thread, name.into()));
    }
}

pub(super) fn running(engine: &crate::TransferEngine) {
    let stats = engine.stats().unwrap().rdma.unwrap();
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
    if closed {
        assert_eq!((stats.progress_contexts, stats.active_cqs), (0, 0));
    }
    let phase = if closed { "closed" } else { "running" };
    println!(
        "ORBITTE_WORKERS_STATS {{\"phase\":\"{phase}\",\"configured\":{},\"workers\":{},\"opened\":{},\"contexts\":{},\"recoveries\":{}}}",
        count(),
        stats.progress_workers,
        stats.opened_progress_workers,
        stats.progress_contexts,
        stats.progress_recoveries
    );
}

pub(super) fn closed(stats: &RdmaStats) {
    assert_eq!((stats.progress_workers, stats.progress_contexts), (0, 0));
    assert!(stats.opened_progress_workers == 0 || stats.opened_progress_workers == count() as u64);
    println!(
        "ORBITTE_WORKERS_STATS {{\"phase\":\"closed\",\"configured\":{},\"workers\":0,\"opened\":{},\"contexts\":0}}",
        count(),
        stats.opened_progress_workers
    );
}
