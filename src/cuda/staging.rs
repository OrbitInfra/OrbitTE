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
            allocation.copy_stream = Some(CopyStream::new(&context)?);
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
