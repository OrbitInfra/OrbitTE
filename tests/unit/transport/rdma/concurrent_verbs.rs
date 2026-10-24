//! Delay real CQEs; observe actual providers and process-owned fault teardown.
use super::*;
use ibverbs_sys as ffi;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{CStr, c_char, c_int};

struct Owner {
    nic: usize,
    qp: usize,
    qpn: u32,
    cq: usize,
    record: Arc<Record>,
}

#[derive(Default)]
struct Trace {
    mode: String,
    requester: bool,
    fault_rail: usize,
    held: bool,
    released: bool,
    armed: bool,
    injected: usize,
    injected_qp: usize,
    injected_parent: Option<usize>,
    injected_record: Option<Arc<Record>>,
    held_cqes: BTreeMap<usize, VecDeque<sys::Completion>>,
    owners: Vec<Owner>,
    failed_cqs: BTreeSet<usize>,
    destroyed_qps: BTreeSet<usize>,
    opened: usize,
    closed: usize,
    registered: usize,
    deregistered: usize,
    active_mrs: BTreeSet<usize>,
    memory: Option<crate::RegisteredMemory>,
    accepted: [[usize; 2]; 2],
    posts: usize,
    delivered_held: usize,
    failed_cq_receipts: usize,
    owner_checks: usize,
    live_owner_checks: usize,
    drained_owner_checks: usize,
    fault_debt: Vec<usize>,
    accepted_wr: BTreeSet<(usize, u32, u64)>,
    delivered_wr: BTreeSet<(usize, u32, u64)>,
    native_error_receipts: usize,
}
static TRACE: OnceLock<Mutex<Trace>> = OnceLock::new();

pub(super) fn initialize(mode: &str, requester: bool, fault_rail: usize) {
    *TRACE.get_or_init(Default::default).lock().unwrap() = Trace {
        mode: mode.into(),
        requester,
        fault_rail,
        ..Default::default()
    };
}

pub(super) fn retain_memory(memory: &crate::RegisteredMemory) {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    trace.memory = Some(memory.clone());
}

pub(super) fn hold() {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    assert!(trace.held_cqes.values().all(VecDeque::is_empty));
    trace.held = true;
    trace.released = false;
    trace.armed = false;
    trace.accepted = [[0; 2]; 2];
    trace.owners.clear();
}

pub(super) fn initial_posted() -> bool {
    TRACE
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .accepted
        .iter()
        .flatten()
        .sum::<usize>()
        >= 4096
}

pub(super) fn admitted(records: Vec<(usize, Arc<Record>)>) {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    assert_eq!(records.len(), 4);
    trace.owners = records
        .into_iter()
        .map(|(nic, record)| {
            let qp = record
                .lock()
                .unwrap()
                .resources
                .as_ref()
                .unwrap()
                .connection
                .handles
                .qp as usize;
            let qpn = unsafe { (*(qp as *const ffi::ibv_qp)).qp_num };
            let cq = record
                .lock()
                .unwrap()
                .resources
                .as_ref()
                .unwrap()
                .connection
                .handles
                .cq as usize;
            Owner {
                nic,
                qp,
                qpn,
                cq,
                record,
            }
        })
        .collect();
    assert_eq!(
        trace
            .owners
            .iter()
            .map(|owner| owner.qp)
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
    for nic in 0..2 {
        assert_eq!(
            trace.owners.iter().filter(|owner| owner.nic == nic).count(),
            2
        );
    }
}

pub(super) fn release() {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    trace.armed = true;
    trace.released = true;
}

pub(super) fn accepted() -> [[usize; 2]; 2] {
    TRACE.get().unwrap().lock().unwrap().accepted
}

pub(super) fn injected_parent() -> usize {
    TRACE
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .injected_parent
        .unwrap()
}

unsafe extern "C" fn open_device(h: *mut sys::DeviceHandles, name: *const c_char) -> c_int {
    let code = unsafe { (sys::REAL.open_device)(h, name) };
    if code == 0 {
        TRACE.get().unwrap().lock().unwrap().opened += 1;
    }
    code
}

unsafe extern "C" fn register(
    h: *mut sys::MemoryHandles,
    device: *const sys::DeviceHandles,
    address: *mut std::ffi::c_void,
    length: usize,
    bind: bool,
) -> c_int {
    let code = unsafe { (sys::REAL.register_memory)(h, device, address, length, bind) };
    if code == 0 {
        let mr = unsafe { &*(*h).mr.cast::<ffi::ibv_mr>() };
        assert_eq!(mr.pd.cast::<std::ffi::c_void>(), unsafe { (*device).pd });
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        trace.registered += 1;
        assert!(trace.active_mrs.insert(mr as *const _ as usize));
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
    remote: u64,
    rkey: u32,
    write: c_int,
    result: *mut sys::PostResult,
) -> c_int {
    let qp = unsafe { &*(*h).qp.cast::<ffi::ibv_qp>() };
    assert_eq!(qp.pd, unsafe { (*(*memory).mr.cast::<ffi::ibv_mr>()).pd });
    let name = unsafe { CStr::from_ptr(ffi::ibv_get_device_name((*qp.context).device)) }
        .to_str()
        .unwrap();
    let rails = env::var("ORBITTE_SHARED_RAILS").unwrap();
    let nic = rails.split(',').position(|rail| rail == name).unwrap();
    let (parent, slice, inject) = {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        let offset = (local - trace.memory.as_ref().unwrap().base_address()) as usize;
        let parent = offset / BANK;
        assert!(parent < 2, "rejected parent reached native payload");
        let start = if write == 0 { DESTINATION } else { GUARD };
        let slice = (offset % BANK - start) / (PAYLOAD / 2);
        assert_eq!(length, PAYLOAD / 2);
        assert!(slice < 2);
        assert_eq!(
            nic, slice,
            "payload route changed after exact slice admission"
        );
        let inject = trace.armed
            && trace.injected == 0
            && nic == trace.fault_rail
            && matches!(trace.mode.as_str(), "concurrent-cq-error");
        if inject {
            trace.injected = 1;
            trace.injected_qp = qp as *const _ as usize;
            trace.injected_parent = Some(parent);
            trace.injected_record = Some(Arc::clone(
                &trace
                    .owners
                    .iter()
                    .find(|owner| owner.qp == trace.injected_qp)
                    .unwrap()
                    .record,
            ));
            trace.failed_cqs = [unsafe { (*h).cq } as usize, unsafe { (*h).receive_cq }
                as usize]
            .into();
        }
        trace.posts += 1;
        (parent, slice, inject)
    };
    let code = unsafe {
        (sys::REAL.post)(
            h,
            memory,
            requests,
            count,
            local,
            length,
            remote,
            if inject { rkey ^ 0x4000_0000 } else { rkey },
            write,
            result,
        )
    };
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    let accepted = unsafe { (*result).accepted };
    for request in unsafe { std::slice::from_raw_parts(requests, accepted) } {
        assert_eq!(
            request.offset as usize, trace.accepted[parent][slice],
            "payload WR replay or gap"
        );
        trace.accepted[parent][slice] += request.length as usize;
        assert!(
            trace
                .accepted_wr
                .insert((unsafe { (*h).cq } as usize, qp.qp_num, request.id)),
            "native payload WR replay"
        );
    }
    code
}

unsafe extern "C" fn send_completion(
    h: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    id: u64,
    address: u64,
    length: usize,
    rkey: u32,
    result: *mut sys::PostResult,
) -> c_int {
    let code = unsafe { (sys::REAL.send_completion)(h, memory, id, address, length, rkey, result) };
    if unsafe { (*result).accepted } != 0 {
        assert_eq!(unsafe { (*result).accepted }, 1);
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        let qp = unsafe { &*(*h).qp.cast::<ffi::ibv_qp>() };
        assert!(
            trace
                .accepted_wr
                .insert((unsafe { (*h).cq } as usize, qp.qp_num, id)),
            "native SEND WR replay"
        );
    }
    code
}

unsafe extern "C" fn poll(h: *mut sys::ConnectionHandles, out: *mut sys::Completion) -> c_int {
    let cq = unsafe { (*h).cq } as usize;
    {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        if trace.requester && trace.released {
            if let Some(completion) = trace.held_cqes.entry(cq).or_default().pop_front() {
                unsafe { out.write(completion) };
                trace.delivered_held += 1;
                delivered(&mut trace, cq, unsafe { &*out });
                return 1;
            }
        }
    }
    let code = unsafe { (sys::REAL.poll)(h, out) };
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    if code == 1 && trace.requester && trace.held && !trace.released {
        trace
            .held_cqes
            .entry(cq)
            .or_default()
            .push_back(unsafe { out.read() });
        0
    } else {
        if code == 1 {
            delivered(&mut trace, cq, unsafe { &*out });
        }
        code
    }
}

fn delivered(trace: &mut Trace, cq: usize, completion: &sys::Completion) {
    if trace
        .accepted_wr
        .contains(&(cq, completion.qpn, completion.id))
    {
        assert!(
            trace
                .delivered_wr
                .insert((cq, completion.qpn, completion.id)),
            "native CQE redelivery"
        );
        if completion.status != 0 {
            assert_eq!(completion.status, ffi::ibv_wc_status::IBV_WC_REM_ACCESS_ERR);
            trace.native_error_receipts += 1;
        }
    }
}

unsafe extern "C" fn destroy_qp(h: *mut sys::ConnectionHandles) -> c_int {
    let qp = unsafe { (*h).qp } as usize;
    let code = unsafe { (sys::REAL.destroy_qp)(h) };
    if code == 0 && qp != 0 {
        TRACE
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .destroyed_qps
            .insert(qp);
    }
    code
}

unsafe extern "C" fn destroy_cq(h: *mut sys::CqHandles) -> c_int {
    let cq = unsafe { (*h).cq } as usize;
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    let observed = trace.failed_cqs.contains(&cq);
    if observed {
        let mut checked = 0;
        let mut live = 0;
        let mut drained = 0;
        let mut bad_debt = None;
        for owner in trace
            .owners
            .iter()
            .filter(|owner| owner.nic == trace.fault_rail)
        {
            checked += 1;
            assert!(
                trace.destroyed_qps.contains(&owner.qp),
                "every admitted QP must be destroyed before its shared CQ"
            );
            let request = owner.record.lock().unwrap();
            let accepted = trace
                .accepted_wr
                .iter()
                .filter(|(cq, qpn, _)| *cq == owner.cq && *qpn == owner.qpn)
                .count();
            let delivered = trace
                .delivered_wr
                .iter()
                .filter(|(cq, qpn, _)| *cq == owner.cq && *qpn == owner.qpn)
                .count();
            let expected = accepted - delivered;
            let debt = request
                .work
                .as_ref()
                .map_or(0, |work| work.queue.outstanding());
            assert_eq!(
                debt, expected,
                "admitted parent lost unresolved WR debt before shared CQ drain"
            );
            let injected = Arc::ptr_eq(&owner.record, trace.injected_record.as_ref().unwrap());
            if let Some(grant) = request
                .resources
                .as_ref()
                .and_then(|resources| resources.grant.as_ref())
            {
                let resources = request.resources.as_ref().unwrap();
                assert!(resources.connection.closed && resources.connection.handles.qp.is_null());
                assert!(trace.active_mrs.contains(&(grant.handles.mr as usize)));
                let offset = (grant.buffer.address().as_ptr() as u64
                    - trace.memory.as_ref().unwrap().base_address())
                    as usize;
                assert!(
                    matches!(
                        trace.memory.as_ref().unwrap().write(offset, &[0xff]),
                        Err(Error::Unavailable(_))
                    ),
                    "an admitted parent's DMA loan escaped before native CQ drain"
                );
                live += 1;
            } else {
                assert!(!injected, "the failed parent escaped its CQ drain barrier");
                assert_eq!(
                    expected, 0,
                    "a retired parent still owes native completions"
                );
                assert!(matches!(
                    request.phase,
                    Phase::Drained | Phase::Ready | Phase::Closed
                ));
                if matches!(request.phase, Phase::Drained | Phase::Ready) {
                    assert!(
                        request.work.is_none()
                            && request.receive.is_none()
                            && request.finish.is_none(),
                        "reuse needs the consumed exact grant receipt"
                    );
                }
                drained += 1;
            }
            if Arc::ptr_eq(&owner.record, trace.injected_record.as_ref().unwrap()) {
                bad_debt = Some(debt);
            }
        }
        assert_eq!(checked, 2);
        let debt = bad_debt.unwrap();
        assert_eq!(
            debt, 0,
            "the error CQE has resolved the injected one-WR window"
        );
        trace.fault_debt.push(debt);
        trace.owner_checks += checked;
        trace.live_owner_checks += live;
        trace.drained_owner_checks += drained;
    }
    let code = unsafe { (sys::REAL.destroy_cq)(h) };
    if code == 0 {
        if observed {
            trace.failed_cq_receipts += 1;
        }
        trace.held_cqes.remove(&cq);
    }
    code
}

unsafe extern "C" fn deregister(h: *mut sys::MemoryHandles) -> c_int {
    let mr = unsafe { (*h).mr } as usize;
    let code = unsafe { (sys::REAL.deregister)(h) };
    if code == 0 && mr != 0 {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        if trace.active_mrs.remove(&mr) {
            trace.deregistered += 1;
        }
    }
    code
}

unsafe extern "C" fn close_device(h: *mut sys::DeviceHandles) -> c_int {
    let code = unsafe { (sys::REAL.close_device)(h) };
    if code == 0 {
        TRACE.get().unwrap().lock().unwrap().closed += 1;
    }
    code
}

pub(super) static VERBS: sys::Verbs = sys::Verbs {
    open_device,
    register_memory: register,
    post,
    send_completion,
    poll,
    destroy_qp,
    destroy_cq,
    deregister,
    close_device,
    ..sys::REAL
};

pub(super) fn finish() {
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    assert_eq!((trace.opened, trace.closed), (2, 2));
    assert_eq!(trace.registered, trace.deregistered);
    assert!(trace.active_mrs.is_empty());
    assert!(trace.held_cqes.values().all(VecDeque::is_empty));
    if trace.requester {
        assert!(trace.delivered_held > 0);
        if matches!(trace.mode.as_str(), "concurrent-cq-error") {
            assert_eq!(
                (trace.injected, trace.failed_cq_receipts, trace.owner_checks),
                (1, 2, 4)
            );
            assert_eq!(trace.fault_debt.len(), 2);
            assert_eq!(trace.native_error_receipts, 1);
        } else {
            assert!(
                trace
                    .accepted
                    .iter()
                    .flatten()
                    .all(|value| *value == PAYLOAD / 2)
            );
        }
    }
    println!(
        "ORBITTE_CONCURRENT_NATIVE {{\"opened_contexts\":{},\"closed_contexts\":{},\"registered_mrs\":{},\"deregistered_mrs\":{},\"payload_posts\":{},\"held_cqes_delivered\":{},\"fault_injections\":{},\"failed_cq_receipts\":{},\"admitted_owner_checks\":{},\"live_owner_checks\":{},\"already_drained_owner_checks\":{},\"injected_parent\":{:?},\"failed_parent_debt\":{:?},\"native_error_receipts\":{},\"accepted\":{:?}}}",
        trace.opened,
        trace.closed,
        trace.registered,
        trace.deregistered,
        trace.posts,
        trace.delivered_held,
        trace.injected,
        trace.failed_cq_receipts,
        trace.owner_checks,
        trace.live_owner_checks,
        trace.drained_owner_checks,
        trace.injected_parent.map_or(-1, |parent| parent as i32),
        trace.fault_debt,
        trace.native_error_receipts,
        trace.accepted
    );
    trace.memory = None;
    trace.owners.clear();
    trace.injected_record = None;
}
