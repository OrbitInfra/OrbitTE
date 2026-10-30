//! CUDA-pinned host allocation, copy stream and deallocation ownership.

use std::mem::ManuallyDrop;

use super::*;
use crate::platform::numa::{MappedMemory, Numa};

enum Backing {
    Driver,
    Mapped(ManuallyDrop<MappedMemory>),
}

pub(crate) struct PinnedAllocation {
    pub(super) context: Arc<Context>,
    pub(crate) address: NonNull<u8>,
    pub(crate) length: usize,
    pub(crate) numa_node: Option<u32>,
    backing: Backing,
    pub(super) copy_stream: Mutex<Option<CopyStream>>,
    cleanup_attempted: bool,
    released: bool,
    native_freed: bool,
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
            return Err(if address.is_null() {
                error
            } else {
                Error::Unquiesced {
                    operation: "CUDA partial pinned allocation",
                    code: match error {
                        Error::Backend { code, .. } | Error::Unquiesced { code, .. } => code,
                        _ => -1,
                    },
                }
            });
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
            numa_node: None,
            backing: Backing::Driver,
            copy_stream: Mutex::new(None),
            cleanup_attempted: false,
            released: false,
            native_freed: false,
        })
    }
    pub(super) fn new_at(
        context: &Arc<Context>,
        length: usize,
        node: Option<u32>,
        numa: &'static Numa,
    ) -> Result<Self> {
        let Some(node) = node else {
            return Self::new(context, length);
        };
        let mut mapping = MappedMemory::new(length, node, numa)?;
        let mut attempted = false;
        let result = context.current(|| {
            attempted = true;
            // Portable registration is owned together with this fresh mapping.
            check("cuMemHostRegister", unsafe {
                (context.driver.host_register)(mapping.address.as_ptr().cast(), mapping.length, 1)
            })
        });
        if let Err(error) = result {
            if attempted {
                RETAINED_PINNED.fetch_add(1, Ordering::AcqRel);
                std::mem::forget((mapping, Arc::clone(context)));
                return Err(Error::Unquiesced {
                    operation: "CUDA host registration",
                    code: match error {
                        Error::Backend { code, .. } | Error::Unquiesced { code, .. } => code,
                        _ => -1,
                    },
                });
            }
            mapping.close()?;
            return Err(error);
        }
        Ok(Self {
            address: mapping.address,
            context: Arc::clone(context),
            length,
            numa_node: Some(node),
            backing: Backing::Mapped(ManuallyDrop::new(mapping)),
            copy_stream: Mutex::new(None),
            cleanup_attempted: false,
            released: false,
            native_freed: false,
        })
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        // SAFETY: initialized stable bytes, no DMA remains at callers of this method.
        unsafe { std::slice::from_raw_parts(self.address.as_ptr(), self.length) }
    }
    pub(crate) fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: exclusive staging before DMA submission.
        unsafe { std::slice::from_raw_parts_mut(self.address.as_ptr(), self.length) }
    }
}

impl PinnedAllocation {
    pub(crate) fn context_id(&self) -> usize {
        self.context.raw.as_ptr() as usize
    }

    pub(crate) fn retain(self) {
        RETAINED_PINNED.fetch_add(1, Ordering::AcqRel);
        std::mem::forget(self);
    }

    pub(crate) fn close(mut self) -> Result<()> {
        self.release()
    }

    fn release(&mut self) -> Result<()> {
        if self.cleanup_attempted {
            return if self.released {
                Ok(())
            } else {
                Err(Error::Unquiesced {
                    operation: "CUDA pinned cleanup",
                    code: -1,
                })
            };
        }
        // Mark the attempt before the native call: even a successful free with
        // failed context restoration is never retried by close or Drop.
        self.cleanup_attempted = true;
        let quiescent = self
            .copy_stream
            .get_mut()
            .is_ok_and(|stream| stream.as_ref().is_none_or(CopyStream::quiescent));
        let result = if !quiescent || self.context.poisoned.load(Ordering::Acquire) {
            Err(Error::Unquiesced {
                operation: "CUDA pinned memory",
                code: -1,
            })
        } else {
            let streams_closed = self
                .copy_stream
                .get_mut()
                .map_err(|_| Error::Unquiesced {
                    operation: "CUDA pinned stream cleanup",
                    code: -1,
                })
                .and_then(|slot| slot.as_mut().map_or(Ok(()), CopyStream::close));
            streams_closed.and_then(|()| {
                self.context.current(|| {
                    // Record native release before restoring the caller context.
                    let (operation, status) = match &self.backing {
                        Backing::Driver => ("cuMemFreeHost", unsafe {
                            (self.context.driver.host_free)(self.address.as_ptr().cast())
                        }),
                        Backing::Mapped(_) => ("cuMemHostUnregister", unsafe {
                            (self.context.driver.host_unregister)(self.address.as_ptr().cast())
                        }),
                    };
                    self.native_freed = status == SUCCESS;
                    check(operation, status)
                })
            })
        };
        let result = result.and_then(|()| match &mut self.backing {
            Backing::Driver => Ok(()),
            Backing::Mapped(mapping) => mapping.close(),
        });
        if result.is_ok() {
            self.released = true;
            Ok(())
        } else {
            self.context.poisoned.store(true, Ordering::Release);
            RETAINED_PINNED.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(Arc::clone(&self.context));
            Err(Error::Unquiesced {
                operation: if self.native_freed {
                    "CUDA pinned cleanup context restoration"
                } else {
                    "CUDA pinned cleanup"
                },
                code: match result {
                    Err(Error::Backend { code, .. } | Error::Unquiesced { code, .. }) => code,
                    _ => -1,
                },
            })
        }
    }
}

impl Drop for PinnedAllocation {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

/// Allocate real CUDA-pinned host memory; no CUDA SDK or runtime linker dependency.
pub fn allocate_pinned_host(device: i32, length: usize) -> Result<Arc<HostMemory>> {
    let context = Context::primary(device)?;
    let allocation = Arc::new(PinnedAllocation::new(&context, length)?);
    Ok(Arc::new(HostMemory::from_cuda_pinned(allocation)))
}
