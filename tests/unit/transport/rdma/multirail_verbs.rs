//! Native observers and process-private faults; payloads still use mlx5 verbs.
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, c_char, c_int};

use ibverbs_sys as ffi;

use super::*;

type PostSend = unsafe extern "C" fn(
    *mut ffi::ibv_qp,
    *mut ffi::ibv_send_wr,
    *mut *mut ffi::ibv_send_wr,
) -> c_int;

#[derive(Default)]
struct Trace {
    mode: String,
    bad_name: String,
    requester: bool,
    entered: bool,
    healthy_posted: bool,
    released: bool,
    posts: BTreeMap<String, (usize, usize)>,
    active_mrs: BTreeSet<usize>,
    registered: usize,
    deregistered: usize,
    opened: usize,
    closed: usize,
    bad_qp: usize,
    bad_mr: usize,
    bad_cqs: BTreeSet<usize>,
    qp_destroyed: bool,
    cq_receipts: usize,
    debt: Vec<usize>,
    memory: Option<crate::RegisteredMemory>,
    engine: Option<std::sync::Weak<TransferEngine>>,
    record: Option<Arc<Record>>,
    offset: usize,
    provider: Option<(usize, PostSend)>,
    provider_calls: usize,
    partial_receipts: usize,
}

static TRACE: OnceLock<(Mutex<Trace>, Condvar)> = OnceLock::new();

pub(super) fn initialize(mode: &str, bad_name: &str, requester: bool) {
    let (lock, _) = TRACE.get_or_init(Default::default);
    *lock.lock().unwrap() = Trace {
        mode: mode.into(),
        bad_name: bad_name.into(),
        requester,
        ..Default::default()
    };
}

pub(super) fn arm(engine: &Arc<crate::TransferEngine>, memory: &crate::RegisteredMemory) {
    let mut trace = TRACE.get().unwrap().0.lock().unwrap();
    trace.memory = Some(memory.clone());
    trace.engine = Some(Arc::downgrade(engine));
}

pub(super) fn wait_paused() {
    let (lock, changed) = TRACE.get().unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    let mut trace = lock.lock().unwrap();
    while !trace.entered || !trace.healthy_posted {
        let remaining = end
            .checked_duration_since(Instant::now())
            .expect("both actual rail workers must enter");
        trace = changed.wait_timeout(trace, remaining).unwrap().0;
    }
}

pub(super) fn release() {
    let (lock, changed) = TRACE.get().unwrap();
    lock.lock().unwrap().released = true;
    changed.notify_all();
}

unsafe extern "C" fn open_device(h: *mut sys::DeviceHandles, name: *const c_char) -> c_int {
    let code = unsafe { (sys::REAL.open_device)(h, name) };
    if code == 0 {
        let mut trace = TRACE.get().unwrap().0.lock().unwrap();
        trace.opened += 1;
        if trace.requester
            && trace.mode == "partial-post"
            && unsafe { CStr::from_ptr(name) }.to_str().unwrap() == trace.bad_name
        {
            let context = unsafe { (*h).context.cast::<ffi::ibv_context>() };
            trace.provider = Some((context as usize, unsafe {
                (*context).ops.post_send.unwrap()
            }));
            unsafe { (*context).ops.post_send = Some(post_send) };
        }
    }
    code
}

unsafe extern "C" fn post_send(
    qp: *mut ffi::ibv_qp,
    first: *mut ffi::ibv_send_wr,
    bad: *mut *mut ffi::ibv_send_wr,
) -> c_int {
    let post = {
        let mut trace = TRACE.get().unwrap().0.lock().unwrap();
        let (context, post) = trace.provider.unwrap();
        assert_eq!(unsafe { (*qp).context } as usize, context);
        if !matches!(
            unsafe { (*first).opcode },
            ffi::ibv_wr_opcode::IBV_WR_RDMA_READ | ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE
        ) {
            return unsafe { post(qp, first, bad) };
        }
        trace.provider_calls += 1;
        assert_eq!(
            trace.provider_calls, 1,
            "failed payload must not be replayed"
        );
        post
    };
    unsafe {
        let second = (*first).next;
        assert!(!second.is_null());
        let old = (*second).num_sge;
        (*second).num_sge = c_int::MAX;
        let code = post(qp, first, bad);
        (*second).num_sge = old;
        assert_eq!(code, libc::ENOMEM);
        assert_eq!(*bad, second);
        code
    }
}

unsafe extern "C" fn register(
    memory: *mut sys::MemoryHandles,
    device: *const sys::DeviceHandles,
    address: *mut std::ffi::c_void,
    length: usize,
    bind: bool,
) -> c_int {
    let code = unsafe { (sys::REAL.register_memory)(memory, device, address, length, bind) };
    if code == 0 {
        let mr = unsafe { &*(*memory).mr.cast::<ffi::ibv_mr>() };
        assert_eq!(mr.pd.cast::<std::ffi::c_void>(), unsafe { (*device).pd });
        let mut trace = TRACE.get().unwrap().0.lock().unwrap();
        assert!(trace.active_mrs.insert(mr as *const _ as usize));
        trace.registered += 1;
    }
    code
}

unsafe extern "C" fn post(
    h: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    requests: *const sys::WorkRequest,
    count: usize,
    local: u64,
    length: usize,
    address: u64,
    rkey: u32,
    write: c_int,
    result: *mut sys::PostResult,
) -> c_int {
    let qp = unsafe { &*(*h).qp.cast::<ffi::ibv_qp>() };
    let mr = unsafe { &*(*memory).mr.cast::<ffi::ibv_mr>() };
    assert_eq!(
        mr.pd, qp.pd,
        "payload MR must belong to the actual selected QP PD"
    );
    let name = unsafe { CStr::from_ptr(ffi::ibv_get_device_name((*qp.context).device)) }
        .to_str()
        .unwrap()
        .to_owned();
    let (lock, changed) = TRACE.get().unwrap();
    let mut trace = lock.lock().unwrap();
    let fault = matches!(trace.mode.as_str(), "cq-error" | "partial-post");
    let gated = fault || trace.mode == "paused-close";
    let bad = gated && name == trace.bad_name;
    trace.posts.entry(name.clone()).or_default().0 += 1;
    if bad && fault {
        assert!(!trace.entered, "failed payload must not be resubmitted");
    }
    if bad && !trace.entered {
        trace.entered = true;
        let engine = trace.engine.as_ref().unwrap().upgrade().unwrap();
        let contexts = source_runtime(&engine).context_set.lock().unwrap();
        let namespace = unsafe { (*requests).id } / crate::transport::rdma::WR_ID_STRIDE;
        trace.record = contexts
            .slots
            .iter()
            .filter(|slot| {
                slot.owner
                    .as_ref()
                    .is_some_and(|owner| owner.address().unwrap().device == name)
            })
            .flat_map(|slot| slot.cq_list.iter().flatten())
            .filter(|lane| lane.handle(false).is_ok_and(|cq| cq == unsafe { (*h).cq }))
            .find_map(|progress| {
                progress
                    .shared
                    .control
                    .lock()
                    .unwrap()
                    .endpoints
                    .work_owner(namespace, false)
            });
        assert!(
            trace.record.is_some(),
            "actual work namespace must have a retained owner"
        );
        drop(contexts);
        drop(engine);
        trace.bad_qp = unsafe { (*h).qp } as usize;
        trace.bad_mr = unsafe { (*memory).mr } as usize;
        trace.bad_cqs = [unsafe { (*h).cq } as usize, unsafe { (*h).receive_cq }
            as usize]
        .into();
        trace.offset = (local - trace.memory.as_ref().unwrap().base_address()) as usize;
        changed.notify_all();
        let end = Instant::now() + Duration::from_secs(15);
        while !trace.healthy_posted || (trace.mode == "paused-close" && !trace.released) {
            let remaining = end
                .checked_duration_since(Instant::now())
                .expect("sibling post or pause release missing");
            trace = changed.wait_timeout(trace, remaining).unwrap().0;
        }
    }
    let bad_key = bad && trace.mode == "cq-error";
    drop(trace);
    let code = unsafe {
        (sys::REAL.post)(
            h,
            memory,
            requests,
            count,
            local,
            length,
            address,
            if bad_key { rkey ^ 0x4000_0000 } else { rkey },
            write,
            result,
        )
    };
    let mut trace = lock.lock().unwrap();
    let accepted = unsafe { (*result).accepted };
    let bytes: usize = unsafe { std::slice::from_raw_parts(requests, accepted) }
        .iter()
        .map(|wr| wr.length as usize)
        .sum();
    trace.posts.get_mut(&name).unwrap().1 += bytes;
    if !bad && accepted > 0 {
        trace.healthy_posted = true;
        changed.notify_all();
    }
    if bad && trace.mode == "partial-post" {
        assert_eq!(
            (code, accepted, unsafe { (*result).uncertain }),
            (libc::ENOMEM, 1, false)
        );
        trace.partial_receipts += 1;
    }
    code
}

unsafe extern "C" fn destroy_qp(h: *mut sys::ConnectionHandles) -> c_int {
    let before = unsafe { (*h).qp } as usize;
    let code = unsafe { (sys::REAL.destroy_qp)(h) };
    let mut trace = TRACE.get().unwrap().0.lock().unwrap();
    if code == 0 && before != 0 && before == trace.bad_qp {
        trace.qp_destroyed = true;
    }
    code
}

unsafe extern "C" fn destroy_cq(h: *mut sys::CqHandles) -> c_int {
    let mut trace = TRACE.get().unwrap().0.lock().unwrap();
    let observed = trace.bad_cqs.contains(&(unsafe { (*h).cq } as usize))
        && matches!(trace.mode.as_str(), "cq-error" | "partial-post");
    if observed {
        assert!(
            trace.qp_destroyed,
            "actual QP destruction must precede both CQ receipts"
        );
        assert!(trace.active_mrs.contains(&trace.bad_mr));
        assert!(
            matches!(
                trace.memory.as_ref().unwrap().write(trace.offset, &[0xff]),
                Err(Error::Unavailable(_))
            ),
            "DMA loan released before native CQ drain"
        );
        let debt = {
            let record = trace.record.as_ref().unwrap().lock().unwrap();
            let resources = record.resources.as_ref().unwrap();
            assert!(resources.connection.closed && resources.connection.handles.qp.is_null());
            assert_eq!(
                resources.grant.as_ref().unwrap().handles.mr as usize,
                trace.bad_mr
            );
            record.work.as_ref().unwrap().queue.outstanding()
        };
        assert!(
            debt >= 1,
            "failed rail lost its own unresolved WR debt before CQ drain"
        );
        trace.debt.push(debt);
    }
    let code = unsafe { (sys::REAL.destroy_cq)(h) };
    if observed && code == 0 {
        trace.cq_receipts += 1;
    }
    code
}

unsafe extern "C" fn deregister(h: *mut sys::MemoryHandles) -> c_int {
    let before = unsafe { (*h).mr } as usize;
    let code = unsafe { (sys::REAL.deregister)(h) };
    if code == 0 && before != 0 {
        let mut trace = TRACE.get().unwrap().0.lock().unwrap();
        if trace.active_mrs.remove(&before) {
            trace.deregistered += 1;
        }
    }
    code
}

unsafe extern "C" fn close_device(h: *mut sys::DeviceHandles) -> c_int {
    {
        let mut trace = TRACE.get().unwrap().0.lock().unwrap();
        if let Some((context, post)) = trace.provider {
            if unsafe { (*h).context } as usize == context {
                unsafe { (*(*h).context.cast::<ffi::ibv_context>()).ops.post_send = Some(post) };
                trace.provider = None;
            }
        }
    }
    let code = unsafe { (sys::REAL.close_device)(h) };
    if code == 0 {
        TRACE.get().unwrap().0.lock().unwrap().closed += 1;
    }
    code
}

unsafe extern "C" fn poll(h: *mut sys::ConnectionHandles, out: *mut sys::Completion) -> i32 {
    unsafe {
        worker_pool::observe(h);
        (sys::REAL.poll)(h, out)
    }
}

pub(super) static VERBS: sys::Verbs = sys::Verbs {
    poll,
    open_device,
    register_memory: register,
    post,
    destroy_qp,
    destroy_cq,
    deregister,
    close_device,
    ..sys::REAL
};

pub(super) fn finish(rails: &[String], mask: u64) {
    let mut trace = TRACE.get().unwrap().0.lock().unwrap();
    assert_eq!((trace.opened, trace.closed), (2, 2));
    assert_eq!(trace.registered, trace.deregistered);
    assert!(trace.active_mrs.is_empty() && trace.provider.is_none());
    if trace.requester && trace.mode != "mr-rollback" {
        for (id, name) in rails.iter().enumerate() {
            assert_eq!(trace.posts.contains_key(name), mask & (1 << id) != 0);
        }
        if matches!(trace.mode.as_str(), "cq-error" | "partial-post") {
            assert_eq!(trace.cq_receipts, 2);
            assert_eq!(trace.debt.len(), 2);
            assert!(trace.qp_destroyed && trace.healthy_posted);
            assert_eq!(
                trace.partial_receipts,
                usize::from(trace.mode == "partial-post")
            );
        }
    }
    let posts = trace
        .posts
        .iter()
        .map(|(name, (calls, bytes))| {
            format!(
                "{{\"device\":\"{name}\",\"post_calls\":{calls},\"native_accepted_bytes\":{bytes}}}"
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "ORBITTE_MULTI_RAIL_NATIVE {{\"opened_contexts\":{},\"closed_contexts\":{},\"registered_mrs\":{},\"deregistered_mrs\":{},\"fault_cq_receipts\":{},\"pending_wr_before_cq\":{:?},\"partial_admission_receipts\":{},\"posts\":[{posts}]}}",
        trace.opened,
        trace.closed,
        trace.registered,
        trace.deregistered,
        trace.cq_receipts,
        trace.debt,
        trace.partial_receipts
    );
    trace.memory = None;
    trace.record = None;
    trace.engine = None;
}
