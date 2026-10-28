//! CUDA-pinned host allocation, copy stream and deallocation ownership.

use super::*;

pub(crate) struct PinnedAllocation {
    pub(super) context: Arc<Context>,
    pub(crate) address: NonNull<u8>,
    pub(crate) length: usize,
    pub(super) copy_stream: Mutex<Option<CopyStream>>,
}
// SAFETY: stable CUDA-pinned host bytes are only exposed through the shared
// HostMemory lock or privately owned staging. The allocation never reallocates.
unsafe impl Send for PinnedAllocation {}
unsafe impl Sync for PinnedAllocation {}

impl PinnedAllocation {
    pub(crate) fn check_access(&self) -> Result<()> {
        let stream = self.copy_stream.lock().map_err(|_| Error::Unquiesced {
            operation: "CUDA pinned copy stream",
            code: -1,
        })?;
        if self.context.poisoned.load(Ordering::Acquire)
            || stream.as_ref().is_some_and(|stream| !stream.quiescent())
        {
            return Err(Error::Unquiesced {
                operation: "CUDA pinned memory",
                code: -1,
            });
        }
        Ok(())
    }

    pub(super) fn new(context: &Arc<Context>, length: usize) -> Result<Self> {
        if length == 0 || length > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let mut address = std::ptr::null_mut();
        let result = context.current(|| {
            // SAFETY: output is initialized; portable pinned bytes stay owned.
            check("cuMemHostAlloc", unsafe {
                (context.driver.host_allocate)(&mut address, length, 1)
            })
        });
        if let Err(error) = result {
            if !address.is_null() {
                // A restore failure after allocation must retain its driver and
                // allocation; no caller pointer is substituted for the owner.
                RETAINED_PINNED.fetch_add(1, Ordering::AcqRel);
                std::mem::forget(Arc::clone(context));
            }
            return Err(error);
        }
        let address = NonNull::new(address.cast())
            .ok_or(Error::Internal("CUDA returned null host memory"))?;
        // SAFETY: newly allocated exclusive host-accessible bytes, before exposure.
        unsafe {
            std::ptr::write_bytes(address.as_ptr(), 0, length);
        }
        Ok(Self {
            context: Arc::clone(context),
            address,
            length,
            copy_stream: Mutex::new(None),
        })
    }
    pub(super) fn bytes(&self) -> &[u8] {
        // SAFETY: initialized stable bytes, no DMA remains at callers of this method.
        unsafe { std::slice::from_raw_parts(self.address.as_ptr(), self.length) }
    }
    pub(super) fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: exclusive staging before DMA submission.
        unsafe { std::slice::from_raw_parts_mut(self.address.as_ptr(), self.length) }
    }
}

impl Drop for PinnedAllocation {
    fn drop(&mut self) {
        let quiescent = self
            .copy_stream
            .get_mut()
            .is_ok_and(|stream| stream.as_ref().is_none_or(CopyStream::quiescent));
        if !quiescent || self.context.poisoned.load(Ordering::Acquire) {
            RETAINED_PINNED.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(Arc::clone(&self.context));
            return;
        }
        if self
            .context
            .current(|| {
                // SAFETY: the last owner frees only physically quiescent staging.
                check("cuMemFreeHost", unsafe {
                    (self.context.driver.host_free)(self.address.as_ptr().cast())
                })
            })
            .is_err()
        {
            self.context.poisoned.store(true, Ordering::Release);
            RETAINED_PINNED.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(Arc::clone(&self.context));
        }
    }
}

/// Allocate real CUDA-pinned host memory; no CUDA SDK or runtime linker dependency.
pub fn allocate_pinned_host(device: i32, length: usize) -> Result<Arc<HostMemory>> {
    let context = Context::primary(device)?;
    let allocation = Arc::new(PinnedAllocation::new(&context, length)?);
    Ok(Arc::new(HostMemory::from_cuda_pinned(allocation)))
}
