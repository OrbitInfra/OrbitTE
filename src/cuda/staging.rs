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
        allocation.check_dma(&range)?;
        let context = Arc::clone(&allocation.context);
        let mut stage = manager.pin_stage_buffer(context.raw.as_ptr() as usize, |size| {
            PinnedAllocation::new(&context, size)
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
            allocation.uncertain = !stream.quiescent();
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

/// The allocation lock covers the complete remote operation. Native copies
/// borrow it directly instead of recursively entering CudaMemory's copy API.
pub(crate) struct StagingCopy<'a> {
    allocation: &'a mut DeviceAllocation,
    address: u64,
    length: usize,
    pending: usize,
    failed: bool,
}

impl CudaMemory {
    pub(crate) fn with_staging<T>(
        &self,
        offset: usize,
        length: usize,
        manager: &Arc<crate::runtime::proxy_manager::ProxyManager>,
        depth: usize,
        operation: impl FnOnce(
            &mut StagingCopy<'_>,
            Vec<crate::runtime::proxy_manager::StageBufferLease>,
        ) -> Result<T>,
    ) -> Result<T> {
        let range = checked_range(offset, length, self.length)?;
        // Network handlers must not wait behind an unrelated allocation user.
        let mut allocation = self.allocation.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => Error::ResourceExhausted,
            std::sync::TryLockError::Poisoned(_) => Error::Internal("CUDA memory lock poisoned"),
        })?;
        allocation.check_dma(&range)?;
        let context = Arc::clone(&allocation.context);
        let mut stages = Vec::with_capacity(depth);
        for _ in 0..depth {
            stages.push(
                manager.pin_stage_buffer(context.raw.as_ptr() as usize, |size| {
                    PinnedAllocation::new(&context, size)
                })?,
            );
        }
        if depth == 1 && allocation.copy_stream.is_none() {
            match CopyStream::new(&context) {
                Ok(stream) => allocation.copy_stream = Some(stream),
                Err(error) => {
                    if matches!(error, Error::Unquiesced { .. }) {
                        allocation.uncertain = true;
                        stages[0].quarantine();
                    }
                    return Err(error);
                }
            }
        }
        operation(
            &mut StagingCopy {
                allocation: &mut allocation,
                address: self.address + offset as u64,
                length,
                pending: 0,
                failed: false,
            },
            stages,
        )
    }
}

impl StagingCopy<'_> {
    pub(crate) fn copy(
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
        let address = host.address;
        stage.submitting();
        let stream = self
            .allocation
            .copy_stream
            .as_mut()
            .ok_or(Error::Internal("CUDA copy stream missing"))?;
        // SAFETY: the exclusive GPU allocation guard and pinned lease remain
        // held through event completion and context restoration. The caller
        // also holds HostMemory's access lock, excluding native DMA loans.
        let result =
            unsafe { stream.copy(self.address + offset as u64, address, length, to_device) };
        self.failed |= !stream.quiescent();
        self.allocation.uncertain = self.failed || self.pending != 0;
        if self.allocation.uncertain || self.allocation.context.poisoned.load(Ordering::Acquire) {
            stage.quarantine();
            return match result {
                Err(error) => Err(error),
                Ok(()) => Err(Error::Unquiesced {
                    operation: "CUDA delegate context restoration",
                    code: -1,
                }),
            };
        }
        stage.drained();
        result?;
        stage.completed(length);
        Ok(())
    }
}

impl StagingCopy<'_> {
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
            // SAFETY: this task retains the GPU guard and pinned lease until
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
        self.allocation.uncertain = self.failed || self.pending != 0;
        result
    }

    pub(crate) fn complete(
        &mut self,
        stage: &mut crate::runtime::proxy_manager::StageBufferLease,
        length: usize,
    ) -> Result<()> {
        // A pending slot is intentionally inaccessible to CPU/DMA consumers.
        // Wait through its stream owner before calling check_access again.
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
            let result = stream.complete();
            if !stream.quiescent() {
                return Err(Error::Unquiesced {
                    operation: "CUDA staged completion",
                    code: -1,
                });
            }
            result
        })();
        self.pending -= 1;
        if matches!(result, Err(Error::Unquiesced { .. })) {
            self.failed = true;
            stage.quarantine();
        } else {
            stage.drained();
        }
        self.allocation.uncertain = self.failed || self.pending != 0;
        result?;
        stage.completed(length);
        Ok(())
    }
}

impl Drop for StagingCopy<'_> {
    fn drop(&mut self) {
        self.allocation.uncertain |= self.failed || self.pending != 0;
    }
}
