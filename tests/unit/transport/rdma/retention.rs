//! Real dependent verbs objects force native destruction to return EBUSY.
use super::*;

pub(super) struct Retirement {
    kind: &'static str,
    attempts: usize,
    error: i32,
    context: usize,
    pd: usize,
    cq: usize,
    qp: usize,
    mw: usize,
}

impl Retirement {
    pub(super) fn new(requester: bool) -> Option<Self> {
        let kind = match env::var("ORBITTE_RAIL_MODE").as_deref() {
            Ok("large-cq-busy") => "cq",
            Ok("large-pd-busy") => "pd",
            _ => return None,
        };
        let generation: usize = env::var("ORBITTE_RESTART_GENERATION")
            .unwrap()
            .parse()
            .unwrap();
        let count: usize = env::var("ORBITTE_RESTART_GENERATIONS")
            .unwrap()
            .parse()
            .unwrap();
        if generation + 1 == count
            || requester != (env::var("ORBITTE_RESTART_KILL_ROLE").as_deref() == Ok("requester"))
        {
            return None;
        }
        Some(Self {
            kind,
            attempts: 0,
            error: 0,
            context: 0,
            pd: 0,
            cq: 0,
            qp: 0,
            mw: 0,
        })
    }

    pub(super) unsafe fn destroy_cq(
        &mut self,
        trace: &Trace,
        handles: *mut sys::CqHandles,
    ) -> Option<i32> {
        if self.kind != "cq" {
            return None;
        }
        let cq = unsafe { (*handles).cq } as usize;
        assert!(
            self.error == 0 || cq != self.cq,
            "retained CQ cleanup was retried"
        );
        let owner = trace.owners.iter().find(|owner| {
            owner.nic == trace.fault_rail && (owner.cq == cq || owner.receive_cq == cq)
        })?;
        self.attempts += 1;
        assert_eq!(self.attempts, 1, "retained CQ cleanup was retried");
        self.pd = owner.pd;
        self.context = unsafe { (*(self.pd as *const ffi::ibv_pd)).context } as usize;
        self.cq = cq;
        let mut attributes = ffi::ibv_qp_init_attr {
            send_cq: cq as *mut ffi::ibv_cq,
            recv_cq: cq as *mut ffi::ibv_cq,
            qp_type: ffi::ibv_qp_type::IBV_QPT_RC,
            cap: ffi::ibv_qp_cap {
                max_send_wr: 1,
                max_recv_wr: 1,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        self.qp =
            unsafe { ffi::ibv_create_qp(self.pd as *mut ffi::ibv_pd, &mut attributes) } as usize;
        assert_ne!(self.qp, 0, "native dependent QP creation failed");
        self.error = unsafe { (sys::REAL.destroy_cq)(handles) };
        assert_eq!(
            self.error,
            libc::EBUSY,
            "expected a real dependent-CQ error"
        );
        assert_eq!(unsafe { (*handles).cq } as usize, cq);
        self.check();
        println!(
            "ORBITTE_NATIVE_CLEANUP_ERROR {{\"kind\":\"cq\",\"code\":{},\"attempts\":{},\"cq\":{},\"pd\":{},\"blocker\":{}}}",
            self.error, self.attempts, self.cq, self.pd, self.qp
        );
        Some(self.error)
    }

    pub(super) unsafe fn dealloc_pd(
        &mut self,
        trace: &Trace,
        handles: *mut sys::DeviceHandles,
    ) -> Option<i32> {
        if self.kind != "pd" {
            return None;
        }
        let pd = unsafe { (*handles).pd } as usize;
        assert!(
            self.error == 0 || pd != self.pd,
            "retained PD cleanup was retried"
        );
        trace
            .owners
            .iter()
            .find(|owner| owner.nic == trace.fault_rail && owner.pd == pd)?;
        self.attempts += 1;
        assert_eq!(self.attempts, 1, "retained PD cleanup was retried");
        self.pd = pd;
        self.context = unsafe { (*handles).context } as usize;
        let allocate =
            unsafe { (*(self.context as *const ffi::ibv_context)).ops.alloc_mw }.unwrap();
        self.mw = unsafe { allocate(pd as *mut ffi::ibv_pd, ffi::IBV_MW_TYPE_2) } as usize;
        assert_ne!(self.mw, 0, "native dependent MW allocation failed");
        self.error = unsafe { (sys::REAL.dealloc_pd)(handles) };
        assert_eq!(
            self.error,
            libc::EBUSY,
            "expected a real dependent-PD error"
        );
        assert_eq!(unsafe { (*handles).pd } as usize, pd);
        assert_eq!(unsafe { (*handles).context } as usize, self.context);
        self.check();
        println!(
            "ORBITTE_NATIVE_CLEANUP_ERROR {{\"kind\":\"pd\",\"code\":{},\"attempts\":{},\"cq\":{},\"pd\":{},\"blocker\":{}}}",
            self.error, self.attempts, self.cq, self.pd, self.mw
        );
        Some(self.error)
    }

    pub(super) fn before_context_close(&self, handles: &sys::DeviceHandles) {
        assert!(
            self.error == 0 || handles.context as usize != self.context,
            "native context close escaped a failed child destruction"
        );
    }

    fn check(&self) {
        assert_eq!((self.error, self.attempts), (libc::EBUSY, 1));
        if self.kind == "cq" {
            let qp = self.qp as *mut ffi::ibv_qp;
            let mut observed = ffi::ibv_qp_attr::default();
            let mut attributes = ffi::ibv_qp_init_attr::default();
            assert_eq!(
                unsafe {
                    ffi::ibv_query_qp(
                        qp,
                        &mut observed,
                        ffi::ibv_qp_attr_mask::IBV_QP_STATE.0 as c_int,
                        &mut attributes,
                    )
                },
                0
            );
            assert_eq!(observed.qp_state, ffi::ibv_qp_state::IBV_QPS_RESET);
            assert_eq!(unsafe { (*qp).pd } as usize, self.pd);
            assert_eq!(unsafe { (*qp).send_cq } as usize, self.cq);
        } else {
            assert_eq!(
                unsafe { (*(self.mw as *const ffi::ibv_mw)).pd } as usize,
                self.pd
            );
        }
    }
}

pub(in super::super) fn retained(
    engine: Arc<TransferEngine>,
    memory: crate::RegisteredMemory,
    backing: Arc<HostMemory>,
    weak: std::sync::Weak<HostMemory>,
    kind: &str,
) -> RdmaStats {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let rdma = engine.stats().unwrap().rdma.unwrap();
        if (kind == "cq" && rdma.retained_cqs > 0) || (kind == "pd" && rdma.retained_devices > 0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "native destruction did not report retention"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let mut offset = None;
    let requester = {
        let trace = TRACE.get().unwrap().lock().unwrap();
        let retained = trace.retirement.as_ref().unwrap();
        retained.check();
        assert_eq!(retained.kind, kind);
        if kind == "cq" {
            assert!(trace.registered > trace.deregistered && !trace.active_mrs.is_empty());
            let request = trace.injected_record.as_ref().unwrap().lock().unwrap();
            assert!(request.phase == Phase::Retained);
            let resources = request.resources.as_ref().unwrap();
            assert!(resources.connection.closed && resources.connection.lease.is_none());
            let grant = resources.grant().unwrap();
            assert!(trace.active_mrs.contains(&(grant.handles.mr as usize)));
            offset =
                Some((grant.buffer.address().as_ptr() as u64 - memory.base_address()) as usize);
            assert!(
                request.work.as_ref().is_some_and(|w| w.queue.pending())
                    || request.receive.as_ref().is_some_and(|r| r.pending())
            );
        } else {
            assert_eq!(trace.registered, trace.deregistered);
            assert!(trace.active_mrs.is_empty());
        }
        trace.requester
    };
    if let Some(offset) = offset {
        assert!(
            matches!(memory.write(offset, &[0xff]), Err(Error::Unavailable(_))),
            "retained native debt lost its DMA loan"
        );
    }
    if kind == "cq" {
        engine.unregister_memory(&memory).unwrap();
    }
    for _ in 0..2 {
        assert!(
            matches!(
                engine.close(Duration::from_millis(50)),
                Err(Error::Timeout | Error::Unquiesced { .. })
            ),
            "retained engine falsely reported drain"
        );
    }
    let outer = engine.stats().unwrap();
    let stats = outer.rdma.unwrap();
    if kind == "cq" {
        assert!(stats.retained_cqs > 0 && stats.retained_grants > 0);
        assert!(stats.retained_work_requests + stats.retained_receive_work_requests > 0);
        assert!(stats.persistent_mrs > 0 && stats.direct_bytes > 0);
        assert!(if requester {
            outer.inflight_batches > 0
        } else {
            outer.active_source_operations > 0
        });
        assert_eq!(stats.retained_endpoints, 0);
        assert_eq!(
            stats.opened_connections - stats.closed_connections,
            stats.active_endpoints as u64
        );
    } else {
        assert!(stats.retained_devices > 0 && stats.active_devices > 0);
        assert_eq!(
            (outer.inflight_batches, outer.active_source_operations),
            (0, 0)
        );
        assert_eq!(
            (
                stats.active_cqs,
                stats.active_grants,
                stats.persistent_mrs,
                stats.work_requests,
                stats.receive_work_requests,
                stats.receive_credits
            ),
            (0, 0, 0, 0, 0, 0)
        );
    }
    {
        let mut trace = TRACE.get().unwrap().lock().unwrap();
        trace.owners.clear();
        trace.injected_record.take();
        trace.memory.take();
    }
    drop(memory);
    drop(backing);
    drop(engine);
    let backing_retained = weak.upgrade().is_some();
    assert_eq!(
        backing_retained,
        kind == "cq",
        "observer references masked backing lifetime"
    );
    let trace = TRACE.get().unwrap().lock().unwrap();
    let retained = trace.retirement.as_ref().unwrap();
    retained.check();
    println!(
        "ORBITTE_NATIVE_RETENTION {{\"kind\":\"{kind}\",\"requester\":{},\"native_error\":{},\"native_attempts\":{},\"native_blocker_qp\":{},\"native_blocker_mw\":{},\"native_pd\":{},\"native_cq\":{},\"registered_mrs\":{},\"deregistered_mrs\":{},\"retained_cqs\":{},\"retained_devices\":{},\"retained_endpoints\":{},\"retained_grants\":{},\"sq_debt\":{},\"rq_debt\":{},\"rq_credits\":{},\"inflight_batches\":{},\"source_operations\":{},\"backing_retained\":{backing_retained},\"observer_owners_removed\":true,\"post_drop_native_blocker_alive\":true,\"drained\":false}}",
        trace.requester,
        retained.error,
        retained.attempts,
        retained.qp,
        retained.mw,
        retained.pd,
        retained.cq,
        trace.registered,
        trace.deregistered,
        stats.retained_cqs,
        stats.retained_devices,
        stats.retained_endpoints,
        stats.retained_grants,
        stats.work_requests,
        stats.receive_work_requests,
        stats.receive_credits,
        outer.inflight_batches,
        outer.active_source_operations
    );
    stats
}
