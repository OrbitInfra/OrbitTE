//! CUDA registration consistency, range ownership and GPUDirect write visibility.

use std::ops::Range;

use super::*;

pub(super) enum Ordering {
    Device,
    Flush,
}

impl DeviceAllocation {
    pub(super) fn check_dma(&self, range: &Range<usize>) -> Result<()> {
        if self.uncertain
            || self
                .context
                .poisoned
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Unquiesced {
                operation: "CUDA DMA ownership",
                code: -1,
            });
        }
        if self
            .dma
            .iter()
            .any(|loan| range.start < loan.end && loan.start < range.end)
        {
            return Err(Error::Unavailable("CUDA range is owned by RDMA"));
        }
        Ok(())
    }

    fn prepare_rdma(&mut self) -> Result<()> {
        if self.rdma.is_some() {
            return Ok(());
        }
        let context = &self.context;
        let ordering = context.current(|| {
            let mut ordered = 0;
            check("cuDeviceGetAttribute RDMA ordering", unsafe {
                (context.driver.device_attribute)(&mut ordered,
                    sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WRITES_ORDERING,
                    context.device)
            })?;
            let ordering = if ordered >= sys::CUGPUDirectRDMAWritesOrdering::CU_GPU_DIRECT_RDMA_WRITES_ORDERING_OWNER as i32 {
                Ordering::Device
            } else {
                let mut options = 0;
                check("cuDeviceGetAttribute RDMA flush", unsafe {
                    (context.driver.device_attribute)(&mut options,
                        sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_FLUSH_WRITES_OPTIONS,
                        context.device)
                })?;
                if options & sys::CUflushGPUDirectRDMAWritesOptions::CU_FLUSH_GPU_DIRECT_RDMA_WRITES_OPTION_HOST as i32 == 0 {
                    return Err(Error::Unavailable("CUDA device cannot establish GPUDirect write visibility"));
                }
                Ordering::Flush
            };
            let enabled = 1u32;
            check("cuPointerSetAttribute SYNC_MEMOPS", unsafe {
                (context.driver.set_pointer_attribute)((&enabled as *const u32).cast(),
                    PointerAttribute::CU_POINTER_ATTRIBUTE_SYNC_MEMOPS, self.address)
            })?;
            Ok(ordering)
        })?;
        self.rdma = Some(ordering);
        Ok(())
    }
}

impl CudaMemory {
    pub(crate) fn with_rdma_context<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let mut allocation = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        if allocation.uncertain {
            return Err(Error::Unquiesced {
                operation: "CUDA prior DMA",
                code: -1,
            });
        }
        allocation.prepare_rdma()?;
        allocation.context.current(operation)
    }

    pub(crate) fn acquire_dma(&self, range: Range<usize>) -> Result<()> {
        let mut allocation = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        allocation.check_dma(&range)?;
        allocation.prepare_rdma()?;
        allocation
            .dma
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        allocation.dma.push(range);
        Ok(())
    }

    pub(crate) fn complete_dma(&self) -> Result<()> {
        let mut allocation = self.allocation.lock().map_err(|_| Error::Unquiesced {
            operation: "CUDA DMA lock",
            code: -1,
        })?;
        if allocation.uncertain {
            return Err(Error::Unquiesced {
                operation: "CUDA prior DMA",
                code: -1,
            });
        }
        let context = Arc::clone(&allocation.context);
        let result = context.current(|| match allocation.rdma {
            Some(Ordering::Device) => Ok(()),
            Some(Ordering::Flush) => check("cuFlushGPUDirectRDMAWrites", unsafe {
                (context.driver.flush_rdma)(
                    sys::CUflushGPUDirectRDMAWritesTarget::CU_FLUSH_GPU_DIRECT_RDMA_WRITES_TARGET_CURRENT_CTX,
                    sys::CUflushGPUDirectRDMAWritesScope::CU_FLUSH_GPU_DIRECT_RDMA_WRITES_TO_OWNER)
            }),
            None => Err(Error::Internal("CUDA DMA lacks registration ordering")),
        });
        if let Err(error) = result {
            allocation.uncertain = true;
            let (operation, code) = match error {
                Error::Backend { operation, code } | Error::Unquiesced { operation, code } => {
                    (operation, code)
                }
                _ => ("CUDA GPUDirect visibility", -1),
            };
            return Err(Error::Unquiesced { operation, code });
        }
        Ok(())
    }

    pub(crate) fn release_dma(&self, range: &Range<usize>) {
        self.allocation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dma
            .retain(|loan| loan != range);
    }
}
