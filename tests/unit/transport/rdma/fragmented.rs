//! Observe the real native cancellation barrier while one posted WR is unresolved.
use super::*;

struct Probe {
    memory: crate::RegisteredMemory,
    record: Arc<Record>,
    usage: Arc<Mutex<RdmaStats>>,
    pending: Vec<usize>,
    receipts: usize,
}

static PROBE: OnceLock<Mutex<Option<Probe>>> = OnceLock::new();

unsafe extern "C" fn destroy_cq(handles: *mut sys::CqHandles) -> i32 {
    let mut state = PROBE.get_or_init(Default::default).lock().unwrap();
    if let Some(probe) = state.as_mut() {
        let request = probe.record.lock().unwrap();
        let resources = request.resources.as_ref().unwrap();
        assert!(
            resources.connection.closed && resources.connection.handles.qp.is_null(),
            "the actual QP must be destroyed before its CQ barriers"
        );
        assert!(
            !resources.grant().unwrap().handles.mr.is_null(),
            "the MR must outlive the native CQ barriers"
        );
        drop(request);
        assert!(
            matches!(
                probe.memory.write(GUARD, &[0xff]),
                Err(Error::Unavailable(_))
            ),
            "the application loan ended before the native CQ drain receipt"
        );
        let pending = probe.usage.lock().unwrap().work_requests;
        assert_eq!(
            pending, 1,
            "the flushed WR debt must survive both CQ barriers"
        );
        probe.pending.push(pending);
    }
    let code = unsafe { (sys::REAL.destroy_cq)(handles) };
    if code == 0 {
        if let Some(probe) = state.as_mut() {
            probe.receipts += 1;
        }
    }
    code
}

pub(super) static VERBS: sys::Verbs = sys::Verbs {
    destroy_cq,
    ..sys::REAL
};

pub(super) fn arm(
    runtime: &RdmaTransport,
    endpoint: &RdmaEndpoint,
    memory: crate::RegisteredMemory,
) {
    let mut state = PROBE.get_or_init(Default::default).lock().unwrap();
    assert!(state.is_none());
    *state = Some(Probe {
        memory,
        record: Arc::clone(&endpoint.record),
        usage: Arc::clone(&runtime.usage),
        pending: Vec::new(),
        receipts: 0,
    });
}

pub(super) fn finish(stats: &RdmaStats) {
    let probe = PROBE.get().unwrap().lock().unwrap().take().unwrap();
    assert_eq!(probe.pending, [1, 1]);
    assert_eq!(probe.receipts, 2);
    assert_eq!(
        stats.post_calls, 1,
        "the failed request must never be replayed"
    );
    assert_eq!(stats.posted_work_requests, 3);
    assert_eq!(stats.peak_work_requests, 3);
    assert_eq!(stats.completed_reads + stats.completed_writes, 1);
    assert_eq!(
        stats.completed_read_bytes + stats.completed_write_bytes,
        4096
    );
    assert_eq!(stats.completion_errors, 1);
    assert_eq!(stats.progress_failures, 1);
    assert_eq!((stats.work_requests, stats.retained_work_requests), (0, 0));
    println!(
        "ORBITTE_FRAGMENT_DRAIN {{\"posted_wr\":3,\"completed_prefix_bytes\":4096,\"pending_wr_before_cq\":[1,1],\"native_cq_receipts\":2,\"post_calls\":1,\"qp_destroyed_before_cqs\":true,\"mr_and_loan_held_until_native_drain\":true}}"
    );
}
