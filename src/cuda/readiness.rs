//! Producer event generations owned by the actual GPU allocation.

use std::ffi::c_void;

use super::*;

/// TENT's caller-sync event lifetime, attached to an allocation across threads.
pub(super) struct CallerSyncEvent {
    context: Arc<Context>,
    event: Option<NonNull<sys::CUevent_st>>,
    owner: Option<Arc<dyn Send + Sync>>,
    pending: bool,
    uncertain: bool,
}

// SAFETY: the allocation mutex serializes event record, consumption and reuse.
// A retained context and actual producer owner outlive every pending generation.
unsafe impl Send for CallerSyncEvent {}

impl CallerSyncEvent {
    fn new(context: &Arc<Context>) -> Self {
        Self {
            context: Arc::clone(context),
            event: None,
            owner: None,
            pending: false,
            uncertain: false,
        }
    }

    unsafe fn record(&mut self, stream: sys::CUstream, owner: Arc<dyn Send + Sync>) -> Result<()> {
        self.owner = Some(owner);
        self.pending = true;
        // Until record succeeds, an old or unrecorded event proves nothing.
        self.uncertain = true;
        let context = Arc::clone(&self.context);
        let mut recovered = false;
        let result = (|| {
            let mut actual = std::ptr::null_mut();
            // SAFETY: the caller keeps this valid stream alive. Resolve special
            // default-stream handles on the producer thread BEFORE any push.
            check("cuStreamGetCtx producer", unsafe {
                (context.driver.stream_context)(stream, &mut actual)
            })?;
            if actual != context.raw.as_ptr() {
                return Err(Error::Unavailable(
                    "producer stream belongs to another CUDA context",
                ));
            }
            context.current(|| {
                let mut capture = sys::CUstreamCaptureStatus::CU_STREAM_CAPTURE_STATUS_NONE;
                check("cuStreamIsCapturing producer", unsafe {
                    (context.driver.stream_capture)(stream, &mut capture)
                })?;
                if capture != sys::CUstreamCaptureStatus::CU_STREAM_CAPTURE_STATUS_NONE {
                    return Err(Error::Unavailable("producer stream is capturing a graph"));
                }
                if self.event.is_none() {
                    let mut event = std::ptr::null_mut();
                    let code = unsafe {
                        (context.driver.event_create)(
                            &mut event,
                            sys::CUevent_flags::CU_EVENT_DISABLE_TIMING as u32
                                | sys::CUevent_flags::CU_EVENT_BLOCKING_SYNC as u32,
                        )
                    };
                    self.event = NonNull::new(event);
                    check("cuEventCreate producer", code)?;
                }
                let event = self
                    .event
                    .ok_or(Error::Internal("CUDA producer event missing"))?;
                let code = unsafe { (context.driver.event_record)(event.as_ptr(), stream) };
                if code != SUCCESS {
                    // Only this original thread can recover a special stream.
                    // Never synchronize an older event generation after failure.
                    check("cuStreamSynchronize producer recovery", unsafe {
                        (context.driver.stream_synchronize)(stream)
                    })?;
                    recovered = true;
                }
                check("cuEventRecord producer", code)
            })
        })();
        if result.is_ok() {
            self.uncertain = false;
        } else if recovered && !context.poisoned.load(Ordering::Acquire) {
            self.pending = false;
            self.uncertain = false;
            self.owner = None;
        }
        if self.uncertain {
            result.map_err(unquiesced)
        } else {
            result
        }
    }

    fn wait(&mut self) -> Result<()> {
        if self.uncertain || self.context.poisoned.load(Ordering::Acquire) {
            return Err(Error::Unquiesced {
                operation: "CUDA producer readiness",
                code: -1,
            });
        }
        if !self.pending {
            return Ok(());
        }
        let result = self.context.current(|| {
            let event = self
                .event
                .ok_or(Error::Internal("CUDA producer event missing"))?;
            // SAFETY: the private event cannot be re-recorded until this exact
            // generation and its context restoration have completed.
            check("cuEventSynchronize producer", unsafe {
                (self.context.driver.event_synchronize)(event.as_ptr())
            })
        });
        if result.is_ok() {
            self.pending = false;
            self.owner = None;
        } else {
            self.uncertain = true;
        }
        result.map_err(unquiesced)
    }
}

impl Drop for CallerSyncEvent {
    fn drop(&mut self) {
        let mut retained =
            self.pending || self.uncertain || self.context.poisoned.load(Ordering::Acquire);
        if let Some(event) = self.event.take() {
            if !retained {
                retained = self
                    .context
                    .current(|| {
                        // SAFETY: the event is quiescent, privately owned and destroyed once.
                        check("cuEventDestroy producer", unsafe {
                            (self.context.driver.event_destroy)(event.as_ptr())
                        })
                    })
                    .is_err();
            }
            if retained {
                RETAINED_EVENT.fetch_add(1, Ordering::AcqRel);
            }
        }
        if retained {
            std::mem::forget(Arc::clone(&self.context));
            if let Some(owner) = self.owner.take() {
                std::mem::forget(owner);
            }
        }
    }
}

fn unquiesced(error: Error) -> Error {
    let (operation, code) = match error {
        Error::Backend { operation, code } | Error::Unquiesced { operation, code } => {
            (operation, code)
        }
        Error::Unavailable(operation) | Error::Internal(operation) => (operation, -1),
        _ => ("CUDA producer readiness", -1),
    };
    Error::Unquiesced { operation, code }
}

impl DeviceAllocation {
    pub(super) fn wait_producer(&mut self) -> Result<()> {
        if let Some(producer) = &mut self.producer {
            if let Err(error) = producer.wait() {
                self.uncertain = true;
                return Err(error);
            }
        }
        Ok(())
    }
}

impl CudaMemory {
    /// Capture already-enqueued producer work on the calling thread. The first
    /// local copy, staging loan, RDMA loan or IPC access waits for this marker.
    /// Aliases share one event; a new handoff first consumes the previous marker.
    /// A final drop also drains a pending marker before releasing the allocation.
    ///
    /// # Safety
    /// `stream` must be a live, noncapturing CUDA Driver/Runtime stream in this
    /// allocation's context. Special default handles use the calling thread's
    /// current context. `owner` must retain the stream and every producer input
    /// through completion and must not form an ownership cycle with this memory.
    /// Coordinate exclusive external access before queuing work: no transfer loan
    /// may exist, and no further external access is allowed before engine completion.
    /// An admission error before capture leaves that producer work with the caller;
    /// after capture starts, uncertain failures fence and retain the actual owners.
    pub unsafe fn record_producer_stream(
        &self,
        stream: *mut c_void,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<()> {
        let mut allocation = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        allocation.prepare_access(&(0..self.length))?;
        if allocation.producer.is_none() {
            allocation.producer = Some(CallerSyncEvent::new(&allocation.context));
        }
        let producer = allocation
            .producer
            .as_mut()
            .ok_or(Error::Internal("CUDA producer event missing"))?;
        // SAFETY: the caller provides the stream/producer lifetime; the allocation
        // lock excludes engine access until its event generation is published.
        let result = unsafe { producer.record(stream.cast(), owner) };
        allocation.uncertain |= matches!(result, Err(Error::Unquiesced { .. }));
        result
    }
}
