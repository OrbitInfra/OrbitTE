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
}
// SAFETY: CudaMemory's allocation mutex serializes submission, event reuse and
// teardown. Each Driver API call runs in the retained owning context.
unsafe impl Send for CopyStream {}

impl CopyStream {
    pub(super) fn new(context: &Arc<Context>) -> Result<Self> {
        let mut owner = Self {
            context: Arc::clone(context),
            stream: None,
            event: None,
            pending: false,
        };
        context.current(|| {
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
        })?;
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
        if !self.quiescent() {
            return Err(Error::Unquiesced {
                operation: "CUDA copy stream",
                code: -1,
            });
        }
        let stream = self.stream.ok_or(Error::Internal("CUDA stream missing"))?;
        let event = self.event.ok_or(Error::Internal("CUDA event missing"))?;
        let context = Arc::clone(&self.context);
        context.current(|| {
            self.pending = true;
            // SAFETY: the caller owns stable GPU/pinned bytes; the mutex prevents
            // event re-recording until this copy completes or poisons reuse.
            let submitted = unsafe {
                if to_device {
                    (context.driver.to_device)(
                        device,
                        host.as_ptr().cast(),
                        length,
                        stream.as_ptr(),
                    )
                } else {
                    (context.driver.to_host)(host.as_ptr().cast(), device, length, stream.as_ptr())
                }
            };
            let result = check("CUDA staged async copy", submitted).and_then(|()| {
                // SAFETY: both handles were created in the current context.
                check("cuEventRecord copy", unsafe {
                    (context.driver.event_record)(event.as_ptr(), stream.as_ptr())
                })
            });
            if let Err(error) = result {
                // A failed submit/record may still leave DMA in flight. An old
                // event is not a receipt for this copy; drain this stream instead.
                let drained = unsafe { (context.driver.stream_synchronize)(stream.as_ptr()) };
                if drained != SUCCESS {
                    return Err(Error::Unquiesced {
                        operation: "cuStreamSynchronize copy recovery",
                        code: drained as i32,
                    });
                }
                self.pending = false;
                return Err(error);
            }
            // SAFETY: this event was recorded after this copy, never before it.
            let completed = unsafe { (context.driver.event_synchronize)(event.as_ptr()) };
            if completed != SUCCESS {
                return Err(Error::Unquiesced {
                    operation: "cuEventSynchronize copy",
                    code: completed as i32,
                });
            }
            self.pending = false;
            Ok(())
        })
    }
}

impl Drop for CopyStream {
    fn drop(&mut self) {
        if self.quiescent() {
            let context = Arc::clone(&self.context);
            let _ = context.current(|| {
                // Destroy is cleanup, not proof of DMA completion. Each handle
                // is cleared only after its own successful release.
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
            });
        }
        if self.stream.is_some() || self.event.is_some() {
            RETAINED_STREAM.fetch_add(usize::from(self.stream.is_some()), Ordering::AcqRel);
            RETAINED_EVENT.fetch_add(usize::from(self.event.is_some()), Ordering::AcqRel);
            std::mem::forget(Arc::clone(&self.context));
        }
    }
}
