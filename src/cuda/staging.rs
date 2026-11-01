//! Chunked local CUDA copies using the engine-owned staging pool.

use super::*;

impl CudaMemory {
    pub(crate) fn copy_staged(
        &self,
        offset: usize,
        destination: &mut [u8],
        source: Option<&[u8]>,
        manager: &Arc<crate::runtime::proxy_manager::ProxyManager>,
        deadline: std::time::Instant,
    ) -> Result<()> {
        let length = source.map_or(destination.len(), <[u8]>::len);
        let range = checked_range(offset, length, self.length)?;
        if length > 64 << 20 {
            return Err(Error::ResourceExhausted);
        }
        crate::engine::remaining(deadline)?;
        // Keep the full logical range serialized across every local chunk.
        let mut allocation = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        allocation.prepare_access(&range)?;
        let context = Arc::clone(&allocation.context);
        let node = context.staging_node(manager.location, manager.numa)?;
        let mut stage = manager.pin_stage_buffer(context.raw.as_ptr() as usize, node, |size| {
            PinnedAllocation::new_at(&context, size, node, manager.numa)
        })?;
        if allocation.copy_stream.is_none() {
            match CopyStream::new(&context) {
                Ok(stream) => allocation.copy_stream = Some(stream),
                Err(error) => {
                    if matches!(error, Error::Unquiesced { .. }) {
                        allocation.uncertain = true;
                        stage.quarantine();
                    }
                    return Err(error);
                }
            }
        }
        let mut copied = 0;
        while copied < length {
            crate::engine::remaining(deadline)?;
            let count = stage.buffer()?.length.min(length - copied);
            stage.submitting();
            let bytes = stage.buffer_mut()?;
            if let Some(source) = source {
                bytes.bytes_mut()[..count].copy_from_slice(&source[copied..copied + count]);
            }
            let stream = allocation
                .copy_stream
                .as_mut()
                .ok_or(Error::Internal("CUDA copy stream missing"))?;
            // SAFETY: the GPU range lock and exclusive pinned chunk remain
            // owned through each event wait and context restoration.
            let result = unsafe {
                stream.copy(
                    self.address + offset as u64 + copied as u64,
                    bytes.address,
                    count,
                    source.is_some(),
                )
            };
            allocation.uncertain |= !stream.quiescent();
            if allocation.uncertain || context.poisoned.load(Ordering::Acquire) {
                stage.retain();
                return match result {
                    Err(error) => Err(error),
                    Ok(()) => Err(Error::Unquiesced {
                        operation: "CUDA staged context restoration",
                        code: -1,
                    }),
                };
            }
            stage.drained();
            result?;
            if source.is_none() {
                destination[copied..copied + count]
                    .copy_from_slice(&stage.buffer()?.bytes()[..count]);
            }
            stage.completed(count);
            copied += count;
        }
        Ok(())
    }
}

/// A logical GPU range stays borrowed while network and native work progress.
/// Each pinned slot owns its stream; the allocation mutex never spans network I/O.
pub(crate) struct StagingCopy {
    memory: Arc<CudaMemory>,
    range: Range<usize>,
    address: u64,
    length: usize,
    pending: usize,
    failed: bool,
}

impl CudaMemory {
    pub(crate) fn with_staging<T>(
        self: &Arc<Self>,
        offset: usize,
        length: usize,
        manager: &Arc<crate::runtime::proxy_manager::ProxyManager>,
        depth: usize,
        operation: impl FnOnce(
            &mut StagingCopy,
            Vec<crate::runtime::proxy_manager::StageBufferLease>,
        ) -> Result<T>,
    ) -> Result<T> {
        let range = checked_range(offset, length, self.length)?;
        let mut allocation = self.allocation.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => Error::ResourceExhausted,
            std::sync::TryLockError::Poisoned(_) => Error::Internal("CUDA memory lock poisoned"),
        })?;
        allocation.prepare_access(&range)?;
        allocation
            .dma
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        let context = Arc::clone(&allocation.context);
        allocation.dma.push(range.clone());
        drop(allocation);
        let mut copy = StagingCopy {
            memory: Arc::clone(self),
            range,
            address: self.address + offset as u64,
            length,
            pending: 0,
            failed: false,
        };
        let node = context.staging_node(manager.location, manager.numa)?;
        let mut stages = Vec::with_capacity(depth);
        for _ in 0..depth {
            stages.push(
                manager.pin_stage_buffer(context.raw.as_ptr() as usize, node, |size| {
                    PinnedAllocation::new_at(&context, size, node, manager.numa)
                })?,
            );
        }
        operation(&mut copy, stages)
    }
}

impl StagingCopy {
    /// Called before taking private host/lease/stream locks. Existing accepted
    /// copies still drain even when another range has made the allocation uncertain.
    pub(crate) fn check_access(&self) -> Result<()> {
        let allocation = self
            .memory
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        if self.failed
            || allocation.uncertain
            || allocation.context.poisoned.load(Ordering::Acquire)
        {
            return Err(Error::Unquiesced {
                operation: "CUDA staging range",
                code: -1,
            });
        }
        Ok(())
    }

    /// Publish native uncertainty after releasing private host/lease/stream locks.
    pub(crate) fn publish_failure(&self) {
        if self.failed {
            self.memory
                .allocation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .uncertain = true;
        }
    }

    pub(crate) fn copy(
        &mut self,
        stage: &mut crate::runtime::proxy_manager::StageBufferLease,
        offset: usize,
        length: usize,
        to_device: bool,
    ) -> Result<()> {
        self.begin(stage, offset, length, to_device)?;
        self.complete(stage, length)
    }

    pub(crate) fn begin(
        &mut self,
        stage: &mut crate::runtime::proxy_manager::StageBufferLease,
        offset: usize,
        length: usize,
        to_device: bool,
    ) -> Result<()> {
        checked_range(offset, length, self.length)?;
        let host = stage.buffer()?;
        checked_range(0, length, host.length)?;
        host.check_access()?;
        stage.submitting();
        let result = (|| {
            let host = stage.buffer()?;
            let mut slot = host.copy_stream.lock().map_err(|_| Error::Unquiesced {
                operation: "CUDA stage stream lock",
                code: -1,
            })?;
            if slot.is_none() {
                *slot = Some(CopyStream::new(&host.context)?);
            }
            let stream = slot
                .as_mut()
                .ok_or(Error::Internal("CUDA stage stream missing"))?;
            // SAFETY: this task retains the GPU range loan and pinned lease until
            // complete, including every failure/EOF path. The private host
            // registration cannot be crossed or reused while this slot is pending.
            let result = unsafe {
                stream.begin_copy(
                    self.address + offset as u64,
                    host.address,
                    length,
                    to_device,
                )
            };
            if result.is_err() && !stream.quiescent() {
                return Err(Error::Unquiesced {
                    operation: "CUDA staged submission",
                    code: -1,
                });
            }
            result
        })();
        match &result {
            Ok(()) => self.pending += 1,
            Err(Error::Unquiesced { .. }) => {
                self.failed = true;
                stage.quarantine();
            }
            Err(_) => stage.drained(),
        }
        result
    }

    pub(crate) fn complete(
        &mut self,
        stage: &mut crate::runtime::proxy_manager::StageBufferLease,
        length: usize,
    ) -> Result<()> {
        self.progress(stage, length, true).map(|_| ())
    }

    pub(crate) fn query(
        &mut self,
        stage: &mut crate::runtime::proxy_manager::StageBufferLease,
        length: usize,
    ) -> Result<bool> {
        self.progress(stage, length, false)
    }

    fn progress(
        &mut self,
        stage: &mut crate::runtime::proxy_manager::StageBufferLease,
        length: usize,
        wait: bool,
    ) -> Result<bool> {
        // Pending slots stay inaccessible to CPU/DMA consumers. Query may
        // leave the same generation pending; errors consume it into retention.
        let result = (|| {
            let host = stage.buffer()?;
            let mut slot = host.copy_stream.lock().map_err(|_| Error::Unquiesced {
                operation: "CUDA stage stream lock",
                code: -1,
            })?;
            let stream = slot.as_mut().ok_or(Error::Unquiesced {
                operation: "CUDA stage stream missing",
                code: -1,
            })?;
            let result = if wait {
                stream.complete().map(|()| true)
            } else {
                stream.query()
            };
            if matches!(result, Ok(false)) {
                return result;
            }
            if !stream.quiescent() {
                return Err(Error::Unquiesced {
                    operation: "CUDA staged completion",
                    code: -1,
                });
            }
            result
        })();
        if matches!(result, Ok(false)) {
            return result;
        }
        self.pending -= 1;
        if matches!(result, Err(Error::Unquiesced { .. })) {
            self.failed = true;
            stage.quarantine();
        } else {
            stage.drained();
        }
        result?;
        stage.completed(length);
        Ok(true)
    }
}

impl Drop for StagingCopy {
    fn drop(&mut self) {
        let mut allocation = self
            .memory
            .allocation
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if self.failed || self.pending != 0 {
            allocation.uncertain = true;
            // Keep the actual device/context/external owner and its range loan.
            // A successful sibling must not release this unresolved native work.
            std::mem::forget(Arc::clone(&self.memory));
        } else {
            allocation.dma.retain(|range| *range != self.range);
        }
    }
}

impl Context {
    fn staging_node(
        self: &Arc<Self>,
        location: crate::StagingLocation,
        numa: &crate::platform::numa::Numa,
    ) -> Result<Option<u32>> {
        use crate::StagingLocation;
        if location == StagingLocation::Any {
            return Ok(None);
        }
        let mut nodes = (numa.nodes)()?;
        nodes.sort_unstable();
        nodes.dedup();
        let node = match location {
            StagingLocation::Any => return Ok(None),
            StagingLocation::Cpu(node) => node,
            StagingLocation::Auto => {
                let mut pci = [0 as std::ffi::c_char; 20];
                self.current(|| {
                    check("cuDeviceGetPCIBusId", unsafe {
                        (self.driver.pci_bus_id)(pci.as_mut_ptr(), pci.len() as i32, self.device)
                    })
                })?;
                let end = pci
                    .iter()
                    .position(|c| *c == 0)
                    .ok_or(Error::Protocol("CUDA PCI bus id"))?;
                let pci: String = pci[..end]
                    .iter()
                    .map(|c| (*c as u8 as char).to_ascii_lowercase())
                    .collect();
                (numa.gpu_node)(&pci)
                    .filter(|node| nodes.contains(node))
                    .or_else(|| nodes.first().copied())
                    .ok_or(Error::Unavailable("no allowed host staging node"))?
            }
        };
        if !nodes.contains(&node) {
            return Err(Error::Unavailable(
                "host staging node is not allowed or has no online memory",
            ));
        }
        Ok(Some(node))
    }
}
