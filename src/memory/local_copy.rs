use super::*;
use crate::cuda::local_copy::PinnedHostRegistration;

impl HostMemory {
    /// Retain one logical slot inside a live CUDA host registration. Adjacent slots
    /// may share the registration while each keeps its own allocator owner.
    ///
    /// # Safety
    /// The requirements of `from_raw_parts` apply. The slot must be backed by
    /// this exact physical registration for its entire lifetime.
    #[allow(unsafe_code)]
    pub unsafe fn from_cuda_registered_parts(
        address: NonNull<u8>,
        length: usize,
        owner: Arc<dyn Send + Sync>,
        pin: Arc<PinnedHostRegistration>,
    ) -> Result<Self> {
        pin.offset(address.as_ptr() as usize as u64, length)?;
        // SAFETY: the caller retains and coordinates every access to this slot.
        let mut memory = unsafe { Self::from_raw_parts(address, length, owner)? };
        let backing = memory
            .backing
            .get_mut()
            .map_err(|_| Error::Internal("host backing lock"))?;
        if let HostBacking::External { pin: slot, .. } = &mut backing.bytes {
            *slot = Some(pin);
        }
        Ok(memory)
    }

    pub(crate) fn local_pin(&self) -> Result<(usize, u64)> {
        let backing = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("host backing lock"))?;
        match &backing.bytes {
            HostBacking::CudaPinned(p) => Ok((Arc::as_ptr(p) as usize, self.address)),
            HostBacking::External { pin: Some(p), .. } => Ok((Arc::as_ptr(p) as usize, p.base())),
            _ => Err(Error::Unavailable(
                "local CUDA copy requires retained pinned host memory",
            )),
        }
    }
    pub(crate) fn borrow_local(&self, ranges: &[Range<usize>]) -> Result<()> {
        let mut b = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("host backing lock"))?;
        for r in ranges {
            b.check_access(r)?;
        }
        if let HostBacking::CudaPinned(p) = &b.bytes {
            p.check_copy_state()?;
        }
        b.dma
            .try_reserve(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        b.dma.extend_from_slice(ranges);
        Ok(())
    }
    pub(crate) fn release_local(&self, ranges: &[Range<usize>]) {
        self.backing
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dma
            .retain(|r| ranges.binary_search_by_key(&r.start, |x| x.start).is_err());
    }
}
