use super::*;

pub(super) struct Signal(Mutex<Option<Result<()>>>);
impl Signal {
    pub fn status(&self) -> Result<Option<()>> {
        match &*self.0.lock().unwrap_or_else(|p| p.into_inner()) {
            None => Ok(None),
            Some(Ok(())) => Ok(Some(())),
            Some(Err(e)) => Err(e.for_report()),
        }
    }
    fn finish(&self, r: Result<()>) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = Some(r);
    }
}

enum Dispatch {
    Dma(Vec<dma::Command>),
    Kernel(Vec<u64>, usize),
}
struct Flight {
    plan: plan::Plan,
    dispatch: Dispatch,
    ready: Option<ProducerEvent>,
    event: usize,
    recorded: bool,
    producers_waited: bool,
    retained: bool,
    error: Option<Error>,
    signal: Arc<Signal>,
}
impl Flight {
    fn quarantine(&mut self, e: Error) -> Error {
        self.retained = true;
        let e = uncertain(e);
        self.signal.finish(Err(e.for_report()));
        e
    }
}

pub(super) struct Pool {
    ctx: Arc<Context>,
    stream: usize,
    thread: std::thread::ThreadId,
    stream_owner: Arc<dyn Send + Sync>,
    config: LocalCopyConfig,
    pitch: usize,
    kernel: Option<kernel::Kernel>,
    descriptor_bytes: usize,
    slots: Vec<Option<Flight>>,
    closed: bool,
    close_error: Option<Error>,
}
impl Pool {
    pub fn new(
        ctx: Arc<Context>,
        stream: usize,
        stream_owner: Arc<dyn Send + Sync>,
        config: LocalCopyConfig,
        pitch: usize,
        kernel: Option<kernel::Kernel>,
        reserve: usize,
    ) -> Self {
        Self {
            ctx,
            stream,
            thread: std::thread::current().id(),
            stream_owner,
            config,
            pitch,
            descriptor_bytes: if kernel.is_some() { reserve } else { 0 },
            kernel,
            slots: (0..config.max_inflight).map(|_| None).collect(),
            closed: false,
            close_error: None,
        }
    }
    pub fn stats(&self) -> LocalCopyStats {
        let mut s = LocalCopyStats {
            descriptor_bytes: self.descriptor_bytes,
            ..Default::default()
        };
        for f in self.slots.iter().flatten() {
            s.active += 1;
            s.payload_bytes += f.plan.bytes;
            s.retained += usize::from(f.retained);
        }
        s
    }
    pub fn progress(&mut self) -> Result<()> {
        if let Some(e) = &self.close_error {
            return Err(e.for_report());
        }
        for i in 0..self.slots.len() {
            self.poll(i);
        }
        // Drive other outstanding markers even if one slot on this stream is fenced.
        for flight in self.slots.iter().flatten().filter(|f| f.retained) {
            flight.signal.status()?;
        }
        Ok(())
    }
    pub fn poll(&mut self, i: usize) {
        let Some(f) = &mut self.slots[i] else {
            return;
        };
        if f.retained {
            return;
        }
        if !f.recorded {
            f.quarantine(Error::Internal("local copy marker missing"));
            return;
        }
        let event = f.event as sys::CUevent;
        let done = self.ctx.current(|| {
            // SAFETY: this is the worker's one recorded event generation. Only
            // this slot can query or destroy it. NOT_READY keeps every owner.
            let q = unsafe { (self.ctx.driver.event_query)(event) };
            if q == driver::Status::CUDA_ERROR_NOT_READY {
                return Ok(false);
            }
            check("cuEventQuery local copy", q)?;
            // SAFETY: successful query proves every preceding enqueue on this stream
            // retired; destruction is attempted once before publishing release.
            check("cuEventDestroy local copy", unsafe {
                (self.ctx.driver.event_destroy)(event)
            })?;
            Ok(true)
        });
        match done {
            Ok(false) => (),
            Ok(true) => {
                if f.producers_waited {
                    for loan in &f.plan.loans {
                        loan.retire_producer();
                    }
                }
                let signal = f.signal.clone();
                let result = f.error.take().map_or(Ok(()), Err);
                self.slots[i].take();
                signal.finish(result);
            }
            Err(e) => {
                f.quarantine(e);
            }
        }
    }

    fn dispatch(
        &self,
        plan: &plan::Plan,
        dir: CopyDirection,
    ) -> Result<(Dispatch, LocalCopyBackend)> {
        let choose_kernel = !plan.rows.is_empty()
            && match self.config.backend {
                LocalCopyBackend::Kernel => true,
                LocalCopyBackend::Auto => {
                    plan.independent && plan.dma.len() >= self.config.kernel_threshold
                }
                LocalCopyBackend::Dma => false,
            };
        if choose_kernel {
            if !plan.independent {
                return Err(Error::InvalidConfig(
                    "local copy kernel requires disjoint destinations",
                ));
            }
            if let Some(k) = &self.kernel {
                match k.descriptors(plan, dir) {
                    Ok(triples) => {
                        return Ok((
                            Dispatch::Kernel(
                                triples,
                                plan.rows.iter().map(|r| r.width).max().unwrap_or(0),
                            ),
                            LocalCopyBackend::Kernel,
                        ));
                    }
                    Err(Error::Unavailable(_)) if self.config.backend == LocalCopyBackend::Auto => {
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        let batch = self
            .ctx
            .driver
            .copy_batch
            .map(driver::BatchEntry::Current)
            .or_else(|| {
                self.ctx
                    .driver
                    .local
                    .batch_with_fail_index
                    .map(driver::BatchEntry::WithFailIndex)
            })
            .filter(|_| plan.independent && self.stream > 1);
        Ok((
            Dispatch::Dma(dma::commands(&plan.dma, dir, batch)),
            LocalCopyBackend::Dma,
        ))
    }

    pub fn submit(
        &mut self,
        dir: CopyDirection,
        descs: &[LocalCopyDesc],
        ready: Option<&ProducerEvent>,
    ) -> Result<(usize, Arc<Signal>, LocalCopyBackend)> {
        if self.closed {
            return Err(Error::Unavailable("local CUDA copy admission closed"));
        }
        if self.stream == 2 && self.thread != std::thread::current().id() {
            return Err(Error::Unavailable(
                "per-thread CUDA stream used on another thread",
            ));
        }
        self.progress()?;
        let i = self
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or(Error::ResourceExhausted)?;
        let ctx = self.ctx.clone();
        let stream = self.stream as sys::CUstream;
        if ready.is_some() && ctx.driver.local.wait_event.is_none() {
            return Err(Error::Unavailable("cuStreamWaitEvent"));
        }
        let plan = plan::Plan::new(&ctx, descs, dir, self.pitch, self.config.max_descriptors)?;
        let usage = self.stats();
        if plan.bytes > self.config.max_bytes - usage.descriptor_bytes - usage.payload_bytes {
            return Err(Error::ResourceExhausted);
        }
        let (dispatch, backend) = ctx.current(|| {
            check_capture(&ctx, stream)?;
            self.dispatch(&plan, dir)
        })?;
        let signal = Arc::new(Signal(Mutex::new(None)));
        // Install the actual owners before the first possibly accepted operation.
        let f = self.slots[i].insert(Flight {
            plan,
            dispatch,
            ready: ready.cloned(),
            event: 0,
            recorded: false,
            producers_waited: false,
            retained: false,
            error: None,
            signal: signal.clone(),
        });
        let submitted = ctx.current(|| {
            let mut event = std::ptr::null_mut();
            // SAFETY: private output and a timing-disabled event on the retained context.
            let status = unsafe {
                (ctx.driver.event_create)(
                    &mut event,
                    sys::CUevent_flags::CU_EVENT_DISABLE_TIMING as u32
                        | sys::CUevent_flags::CU_EVENT_BLOCKING_SYNC as u32,
                )
            };
            f.event = event as usize;
            check("cuEventCreate local copy", status)?;
            if event.is_null() {
                return Err(Error::Internal("null local copy event"));
            }
            let attempt = (|| {
                for loan in &f.plan.loans {
                    loan.enqueue_producer(stream)?;
                }
                f.producers_waited = true;
                if let Some(r) = &f.ready {
                    let wait = ctx
                        .driver
                        .local
                        .wait_event
                        .ok_or(Error::Unavailable("cuStreamWaitEvent"))?;
                    // SAFETY: the flight retains the external recorded generation through this marker.
                    check("cuStreamWaitEvent local copy", unsafe {
                        wait(stream, r.raw as _, 0)
                    })?;
                }
                match &mut f.dispatch {
                    Dispatch::Dma(ops) => ops
                        .iter_mut()
                        .try_for_each(|op| op.enqueue(&ctx, dir, stream)),
                    Dispatch::Kernel(triples, width) => self
                        .kernel
                        .as_ref()
                        .ok_or(Error::Internal("missing local kernel"))?
                        .enqueue(i, triples, *width, stream),
                }
            })();
            f.error = attempt.err();
            // SAFETY: record after every attempted copy, even a partial/failed
            // submission. A failed record cannot authorize releasing any owner.
            check("cuEventRecord local copy", unsafe {
                (ctx.driver.event_record)(event, stream)
            })?;
            f.recorded = true;
            Ok(())
        });
        if let Err(error) = submitted {
            if f.event != 0 || matches!(error, Error::Unquiesced { .. }) {
                return Err(f.quarantine(error));
            }
            self.slots[i].take();
            return Err(error);
        }
        Ok((i, signal, backend))
    }

    fn close_step(&mut self) -> Result<bool> {
        self.closed = true;
        self.progress()?;
        if self.slots.iter().any(Option::is_some) {
            return Ok(false);
        }
        if let Some(k) = &mut self.kernel {
            if let Err(e) = k.close() {
                self.close_error = Some(e.for_report());
                return Err(e);
            }
        }
        self.kernel = None;
        self.descriptor_bytes = 0;
        Ok(true)
    }
}

pub(super) struct Inner(pub Mutex<Pool>);
impl Inner {
    pub fn close(&self, timeout: Duration) -> Result<()> {
        wait_for(timeout, || {
            self.0
                .lock()
                .map_err(|_| Error::Internal("local copy pool lock"))?
                .close_step()
        })
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        if self.close(crate::transport::DESTRUCTION_TIMEOUT).is_err() {
            let p = self.0.get_mut().unwrap_or_else(|e| e.into_inner());
            // No remaining handle can supply a release receipt. Keep native
            // scratch, mappings, registrations, producer/stream and context for
            // the lifetime of the process. Never retry an uncertain destroy.
            std::mem::forget(std::mem::take(&mut p.slots));
            std::mem::forget(p.kernel.take());
            std::mem::forget(p.ctx.clone());
            std::mem::forget(p.stream_owner.clone());
        }
    }
}
