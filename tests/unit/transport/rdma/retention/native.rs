//! Native destruction errors from command filtering or real dependent objects.
use super::*;

pub(in super::super) struct Retirement {
    pub(super) kind: &'static str,
    pub(super) attempts: usize,
    pub(super) error: i32,
    pub(super) context: usize,
    pub(super) pd: usize,
    pub(super) cq: usize,
    pub(super) qp: usize,
    pub(super) mw: usize,
    pub(super) channel: usize,
}

impl Retirement {
    pub(in super::super) fn new(requester: bool) -> Option<Self> {
        let kind = match env::var("ORBITE_RAIL_MODE").as_deref() {
            Ok("large-cq-busy") => "cq",
            Ok("large-pd-busy") => "pd",
            Ok("large-qp-cleanup-error") => "qp",
            Ok("large-channel-busy") => "channel",
            _ => return None,
        };
        let generation: usize = env::var("ORBITE_RESTART_GENERATION")
            .unwrap()
            .parse()
            .unwrap();
        let count: usize = env::var("ORBITE_RESTART_GENERATIONS")
            .unwrap()
            .parse()
            .unwrap();
        if generation + 1 == count
            || requester != (env::var("ORBITE_RESTART_KILL_ROLE").as_deref() == Ok("requester"))
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
            channel: 0,
        })
    }

    pub(in super::super) unsafe fn destroy_qp(
        &mut self,
        trace: &Trace,
        handles: *mut sys::ConnectionHandles,
    ) -> Option<i32> {
        // SAFETY: The connection supplies a live QP handle slot for the callback.
        let qp = unsafe { (*handles).qp } as usize;
        if self.kind != "qp" || qp == 0 {
            return None;
        }
        assert!(
            self.error == 0 || qp != self.qp,
            "retained QP cleanup was retried"
        );
        if self.error != 0 {
            return None;
        }
        let injected = trace.injected_record.as_ref()?;
        let owner = trace.owners.iter().find(|owner| {
            owner.nic == trace.fault_rail && owner.qp == qp && Arc::ptr_eq(&owner.record, injected)
        })?;
        self.qp = qp;
        self.cq = owner.cq;
        self.pd = owner.pd;
        // SAFETY: The connection owns the native ibv_qp while the fixture inspects its fields.
        self.context = unsafe { (*(qp as *const ffi::ibv_qp)).context } as usize;
        // SAFETY: The provider resource retains its device context while the fixture reads the
        // uverbs descriptor.
        uverbs_fault::deny_commands(unsafe { (*(self.context as *const ffi::ibv_context)).cmd_fd });
        self.attempts += 1;
        // SAFETY: The retiring connection exclusively owns these QP handles through the destroy
        // attempt.
        self.error = unsafe { (sys::REAL.destroy_qp)(handles) };
        assert_eq!(self.error, libc::EIO);
        // SAFETY: The connection supplies a live QP handle slot for the callback.
        assert_eq!(unsafe { (*handles).qp } as usize, qp);
        // This worker's command FD is filtered; query from the proof thread later.
        println!(
            "ORBITE_NATIVE_CLEANUP_ERROR {{\"kind\":\"qp\",\"code\":{},\"attempts\":{},\"qp\":{},\"pd\":{},\"syscall_fault_injection\":true}}",
            self.error, self.attempts, self.qp, self.pd
        );
        Some(self.error)
    }

    pub(in super::super) unsafe fn close_channel(
        &mut self,
        trace: &Trace,
        handles: *mut sys::ChannelHandles,
    ) -> Option<i32> {
        // SAFETY: The monitor owner retains this channel handle slot through the close attempt.
        let channel = unsafe { (*handles).channel } as usize;
        if self.kind != "channel" || channel == 0 {
            return None;
        }
        assert!(
            self.error == 0 || channel != self.channel,
            "retained channel cleanup was retried"
        );
        if self.error != 0 {
            return None;
        }
        // SAFETY: The recorded dependent resource keeps its provider handle and parent context
        // live at this observation.
        let context = unsafe { (*(channel as *const ffi::ibv_comp_channel)).context };
        let injected = trace.injected_record.as_ref()?;
        trace.owners.iter().find(|owner| {
            owner.nic == trace.fault_rail
                && owner.channel == channel
                && Arc::ptr_eq(&owner.record, injected)
                // SAFETY: The recorded dependent resource keeps its provider handle and parent
                // context live at this observation.
                && unsafe { (*(owner.pd as *const ffi::ibv_pd)).context } == context
        })?;
        self.context = context as usize;
        self.channel = channel;
        // A real child CQ makes libibverbs reject channel destruction. The
        // blocker is not registered with OrbiTE and remains until process exit.
        // SAFETY: The fault fixture retains the context and channel while constructing their
        // dependent CQ.
        self.cq = unsafe {
            ffi::ibv_create_cq(
                context,
                1,
                std::ptr::null_mut(),
                channel as *mut ffi::ibv_comp_channel,
                0,
            )
        } as usize;
        assert_ne!(self.cq, 0, "native channel blocker creation failed");
        self.attempts += 1;
        // SAFETY: The channel owner supplies its exclusively owned handles on this close attempt.
        self.error = unsafe { (sys::REAL.completion.unwrap().close)(handles) };
        assert_eq!(self.error, libc::EBUSY);
        // SAFETY: The monitor owner retains this channel handle slot through the close attempt.
        assert_eq!(unsafe { (*handles).channel } as usize, channel);
        self.check();
        println!(
            "ORBITE_NATIVE_CLEANUP_ERROR {{\"kind\":\"channel\",\"code\":{},\"attempts\":{},\"channel\":{},\"blocker\":{}}}",
            self.error, self.attempts, self.channel, self.cq
        );
        Some(self.error)
    }

    pub(in super::super) unsafe fn destroy_cq(
        &mut self,
        trace: &Trace,
        handles: *mut sys::CqHandles,
    ) -> Option<i32> {
        if self.kind != "cq" {
            return None;
        }
        // SAFETY: The lane or connection keeps its CQ handle slot live during observation.
        let cq = unsafe { (*handles).cq } as usize;
        assert!(
            self.error == 0 || cq != self.cq,
            "retained CQ cleanup was retried"
        );
        if self.error != 0 {
            return None;
        }
        let injected = trace.injected_record.as_ref()?;
        let owner = trace.owners.iter().find(|owner| {
            owner.nic == trace.fault_rail
                && Arc::ptr_eq(&owner.record, injected)
                && (owner.cq == cq || owner.receive_cq == cq)
        })?;
        self.attempts += 1;
        assert_eq!(self.attempts, 1, "retained CQ cleanup was retried");
        self.pd = owner.pd;
        // SAFETY: The recorded dependent resource keeps its provider handle and parent context
        // live at this observation.
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
        // SAFETY: The fixture retains the PD and both CQs; the provider can update attributes.
        let qp = unsafe { ffi::ibv_create_qp(self.pd as *mut ffi::ibv_pd, &mut attributes) };
        self.qp = qp as usize;
        assert_ne!(self.qp, 0, "native dependent QP creation failed");
        // SAFETY: The retiring lane exclusively owns these CQ handles through the destroy
        // attempt.
        self.error = unsafe { (sys::REAL.destroy_cq)(handles) };
        assert_eq!(
            self.error,
            libc::EBUSY,
            "expected a real dependent-CQ error"
        );
        // SAFETY: The lane or connection keeps its CQ handle slot live during observation.
        assert_eq!(unsafe { (*handles).cq } as usize, cq);
        self.check();
        println!(
            "ORBITE_NATIVE_CLEANUP_ERROR {{\"kind\":\"cq\",\"code\":{},\"attempts\":{},\"cq\":{},\"pd\":{},\"blocker\":{}}}",
            self.error, self.attempts, self.cq, self.pd, self.qp
        );
        Some(self.error)
    }

    pub(in super::super) unsafe fn dealloc_pd(
        &mut self,
        trace: &Trace,
        handles: *mut sys::DeviceHandles,
    ) -> Option<i32> {
        if self.kind != "pd" {
            return None;
        }
        // SAFETY: The callback borrows the live protection-domain handle from its device owner.
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
        // SAFETY: The device owner keeps this context handle slot valid through the callback.
        self.context = unsafe { (*handles).context } as usize;
        let context = self.context as *const ffi::ibv_context;
        // SAFETY: The retiring device retains this context and its provider operations table.
        let allocate = unsafe { (*context).ops.alloc_mw }.unwrap();
        // SAFETY: The function belongs to the retained context and the supplied PD stays live
        // with its dependent MW.
        self.mw = unsafe { allocate(pd as *mut ffi::ibv_pd, ffi::IBV_MW_TYPE_2) } as usize;
        assert_ne!(self.mw, 0, "native dependent MW allocation failed");
        // SAFETY: The retiring owner keeps its protection domain handles live through
        // deallocation.
        self.error = unsafe { (sys::REAL.dealloc_pd)(handles) };
        assert_eq!(
            self.error,
            libc::EBUSY,
            "expected a real dependent-PD error"
        );
        // SAFETY: The callback borrows the live protection-domain handle from its device owner.
        assert_eq!(unsafe { (*handles).pd } as usize, pd);
        // SAFETY: The device owner keeps this context handle slot valid through the callback.
        assert_eq!(unsafe { (*handles).context } as usize, self.context);
        self.check();
        println!(
            "ORBITE_NATIVE_CLEANUP_ERROR {{\"kind\":\"pd\",\"code\":{},\"attempts\":{},\"cq\":{},\"pd\":{},\"blocker\":{}}}",
            self.error, self.attempts, self.cq, self.pd, self.mw
        );
        Some(self.error)
    }

    pub(in super::super) fn before_context_close(&self, handles: &sys::DeviceHandles) {
        assert!(
            self.error == 0 || handles.context as usize != self.context,
            "native context close escaped a failed child destruction"
        );
    }

    pub(super) fn check(&self) {
        assert_eq!(
            (self.error, self.attempts),
            (
                if self.kind == "qp" {
                    libc::EIO
                } else {
                    libc::EBUSY
                },
                1
            )
        );
        if self.kind == "qp" || self.kind == "cq" {
            let qp = self.qp as *mut ffi::ibv_qp;
            let mut observed = ffi::ibv_qp_attr::default();
            let mut attributes = ffi::ibv_qp_init_attr::default();
            assert_eq!(
                // SAFETY: This fault fixture deliberately retains these native resources after
                // failed teardown, keeping their handles and dependencies live for inspection.
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
            if self.kind == "cq" {
                assert_eq!(observed.qp_state, ffi::ibv_qp_state::IBV_QPS_RESET);
            }
            // SAFETY: This fault fixture deliberately retains these native resources after failed
            // teardown, keeping their handles and dependencies live for inspection.
            assert_eq!(unsafe { (*qp).pd } as usize, self.pd);
            // SAFETY: This fault fixture deliberately retains these native resources after failed
            // teardown, keeping their handles and dependencies live for inspection.
            assert_eq!(unsafe { (*qp).send_cq } as usize, self.cq);
        } else if self.kind == "channel" {
            assert_eq!(
                // SAFETY: This fault fixture deliberately retains these native resources after
                // failed teardown, keeping their handles and dependencies live for inspection.
                unsafe { (*(self.cq as *const ffi::ibv_cq)).channel } as usize,
                self.channel
            );
            // SAFETY: This fault fixture deliberately retains these native resources after failed
            // teardown, keeping their handles and dependencies live for inspection.
            let native = unsafe { &*(self.channel as *const ffi::ibv_comp_channel) };
            assert_eq!(native.context as usize, self.context);
            // SAFETY: The retained channel still owns this fd; F_GETFD takes no pointer argument.
            assert!(native.refcnt > 0 && unsafe { libc::fcntl(native.fd, libc::F_GETFD) } >= 0);
        } else {
            assert_eq!(
                // SAFETY: This fault fixture deliberately retains these native resources after
                // failed teardown, keeping their handles and dependencies live for inspection.
                unsafe { (*(self.mw as *const ffi::ibv_mw)).pd } as usize,
                self.pd
            );
        }
    }
}
