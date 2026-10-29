use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use cudarc::driver::sys;

use super::driver::{SUCCESS, check};
use super::{Context, RETAINED_EVENT, RETAINED_STREAM};
use crate::{Error, Result};

/// One allocation's serialized copies reuse a private stream and completion event.
pub(super) struct CopyStream {
    context: Arc<Context>,
    stream: Option<NonNull<sys::CUstream_st>>,
    event: Option<NonNull<sys::CUevent_st>>,
    pending: bool,
    cleanup_attempted: bool,
}
// SAFETY: the allocation mutex serializes submission, event reuse and teardown.
// Each Driver API call runs in the retained owning context.
unsafe impl Send for CopyStream {}

impl CopyStream {
    pub(super) fn new(context: &Arc<Context>) -> Result<Self> {
        let mut owner = Self {
            context: Arc::clone(context),
            stream: None,
            event: None,
            pending: false,
            cleanup_attempted: false,
        };
        let initialized = context.current(|| {
            let mut stream = std::ptr::null_mut();
            // SAFETY: initialized output; install ownership before error handling.
            let code = unsafe {
                (context.driver.stream_create)(
                    &mut stream,
                    sys::CUstream_flags::CU_STREAM_NON_BLOCKING as u32,
                )
            };
            owner.stream = NonNull::new(stream);
            check("cuStreamCreate", code)?;
            if owner.stream.is_none() {
                return Err(Error::Internal("CUDA returned a null stream"));
            }
            let mut event = std::ptr::null_mut();
            // SAFETY: the private completion event belongs to this context.
            let code = unsafe {
                (context.driver.event_create)(
                    &mut event,
                    sys::CUevent_flags::CU_EVENT_DISABLE_TIMING as u32
                        | sys::CUevent_flags::CU_EVENT_BLOCKING_SYNC as u32,
                )
            };
            owner.event = NonNull::new(event);
            check("cuEventCreate", code)?;
            if owner.event.is_none() {
                return Err(Error::Internal("CUDA returned a null event"));
            }
            Ok(())
        });
        if let Err(error) = initialized {
            if owner.close().is_err() {
                return Err(Error::Unquiesced {
                    operation: "CUDA partial copy stream cleanup",
                    code: -1,
                });
            }
            return Err(error);
        }
        Ok(owner)
    }

    pub(super) fn quiescent(&self) -> bool {
        !self.pending && !self.context.poisoned.load(Ordering::Acquire)
    }

    /// The caller retains both allocations while pending, including on errors.
    pub(super) unsafe fn copy(
        &mut self,
        device: u64,
        host: NonNull<u8>,
        length: usize,
        to_device: bool,
    ) -> Result<()> {
        // SAFETY: the caller retains the GPU and pinned host allocations.
        unsafe {
            self.submit(|driver, stream| {
                if to_device {
                    (driver.to_device)(device, host.as_ptr().cast(), length, stream)
                } else {
                    (driver.to_host)(host.as_ptr().cast(), device, length, stream)
                }
            })
        }
    }

    /// Return after enqueue/record and context restoration, before completion.
    /// Both allocations must remain owned until complete() proves drain.
    pub(super) unsafe fn begin_copy(
        &mut self,
        device: u64,
        host: NonNull<u8>,
        length: usize,
        to_device: bool,
    ) -> Result<()> {
        let context = Arc::clone(&self.context);
        context.current(|| {
            // SAFETY: the caller retains both allocations beyond this call.
            unsafe {
                self.enqueue(|driver, stream| {
                    if to_device {
                        (driver.to_device)(device, host.as_ptr().cast(), length, stream)
                    } else {
                        (driver.to_host)(host.as_ptr().cast(), device, length, stream)
                    }
                })
            }
        })
    }

    pub(super) fn complete(&mut self) -> Result<()> {
        let context = Arc::clone(&self.context);
        context.current(|| self.wait_current())
    }

    /// NOT_READY keeps this event generation and both allocations pending.
    pub(super) fn query(&mut self) -> Result<bool> {
        let context = Arc::clone(&self.context);
        let ready = context.current(|| {
            if !self.pending {
                return Err(Error::Internal("CUDA copy is not pending"));
            }
            let event = self.event.ok_or(Error::Internal("CUDA event missing"))?;
            // SAFETY: only enqueue records this event, after the owned copy;
            // no slot reuse is allowed before this generation completes.
            match unsafe { (context.driver.event_query)(event.as_ptr()) } {
                SUCCESS => Ok(true),
                sys::CUresult::CUDA_ERROR_NOT_READY => Ok(false),
                code => Err(Error::Unquiesced {
                    operation: "cuEventQuery copy",
                    code: code as i32,
                }),
            }
        })?;
        if ready {
            self.pending = false;
        }
        Ok(ready)
    }

    /// The caller retains both device allocations/mappings through completion.
    pub(super) unsafe fn copy_device(
        &mut self,
        destination: u64,
        source: u64,
        length: usize,
    ) -> Result<()> {
        // SAFETY: both UVA ranges belong to the retained current context.
        unsafe {
            self.submit(|driver, stream| (driver.copy_device)(destination, source, length, stream))
        }
    }

    unsafe fn submit(
        &mut self,
        submit: impl FnOnce(&super::driver::Driver, sys::CUstream) -> super::driver::Status,
    ) -> Result<()> {
        let context = Arc::clone(&self.context);
        context.current(|| {
            // SAFETY: synchronous callers retain both allocations through this wait.
            unsafe { self.enqueue(submit)? };
            self.wait_current()
        })
    }

    /// Requires the owning context to be current and both allocations retained.
    unsafe fn enqueue(
        &mut self,
        submit: impl FnOnce(&super::driver::Driver, sys::CUstream) -> super::driver::Status,
    ) -> Result<()> {
        if !self.quiescent() || self.cleanup_attempted {
            return Err(Error::Unquiesced {
                operation: "CUDA copy stream",
                code: -1,
            });
        }
        let stream = self.stream.ok_or(Error::Internal("CUDA stream missing"))?;
        let event = self.event.ok_or(Error::Internal("CUDA event missing"))?;
        self.pending = true;
        let submitted = submit(&self.context.driver, stream.as_ptr());
        let result = check("CUDA async copy", submitted).and_then(|()| {
            // SAFETY: both handles were created in the current context.
            check("cuEventRecord copy", unsafe {
                (self.context.driver.event_record)(event.as_ptr(), stream.as_ptr())
            })
        });
        if let Err(error) = result {
            // An old event cannot prove a failed submit/record drained.
            let drained = unsafe { (self.context.driver.stream_synchronize)(stream.as_ptr()) };
            if drained != SUCCESS {
                return Err(Error::Unquiesced {
                    operation: "cuStreamSynchronize copy recovery",
                    code: drained as i32,
                });
            }
            self.pending = false;
            return Err(error);
        }
        Ok(())
    }

    fn wait_current(&mut self) -> Result<()> {
        if !self.pending {
            return Err(Error::Internal("CUDA copy is not pending"));
        }
        let event = self.event.ok_or(Error::Internal("CUDA event missing"))?;
        // SAFETY: this event was recorded after this copy; the caller retains
        // both allocations and has made the owning context current.
        let completed = unsafe { (self.context.driver.event_synchronize)(event.as_ptr()) };
        if completed != SUCCESS {
            return Err(Error::Unquiesced {
                operation: "cuEventSynchronize copy",
                code: completed as i32,
            });
        }
        self.pending = false;
        Ok(())
    }

    pub(super) fn close(&mut self) -> Result<()> {
        if self.cleanup_attempted {
            return if self.stream.is_none() && self.event.is_none() && self.quiescent() {
                Ok(())
            } else {
                Err(Error::Unquiesced {
                    operation: "CUDA copy stream cleanup",
                    code: -1,
                })
            };
        }
        self.cleanup_attempted = true;
        if !self.quiescent() {
            return Err(Error::Unquiesced {
                operation: "CUDA copy stream cleanup",
                code: -1,
            });
        }
        let context = Arc::clone(&self.context);
        context.current(|| {
            // Destroy is cleanup, not proof of DMA completion. Clear each
            // handle only after its release; a failed attempt is never repeated.
            if let Some(event) = self.event {
                check("cuEventDestroy", unsafe {
                    (context.driver.event_destroy)(event.as_ptr())
                })?;
                self.event = None;
            }
            if let Some(stream) = self.stream {
                check("cuStreamDestroy", unsafe {
                    (context.driver.stream_destroy)(stream.as_ptr())
                })?;
                self.stream = None;
            }
            Ok(())
        })
    }
}

impl Drop for CopyStream {
    fn drop(&mut self) {
        let _ = self.close();
        if self.stream.is_some() || self.event.is_some() {
            RETAINED_STREAM.fetch_add(usize::from(self.stream.is_some()), Ordering::AcqRel);
            RETAINED_EVENT.fetch_add(usize::from(self.event.is_some()), Ordering::AcqRel);
            std::mem::forget(Arc::clone(&self.context));
        }
    }
}
