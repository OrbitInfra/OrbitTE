//! Explicit whole-allocation consent for VMM storage supplied by a caller.
use super::*;

impl Platform {
    pub(crate) unsafe fn wrap_device(
        &self,
        device: i32,
        address: u64,
        length: usize,
        extent: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Arc<CudaMemory>> {
        let v = self.wrap(
            self.device(device)?,
            Location::Device,
            address,
            length,
            extent,
            owner,
        )?;
        Ok(Arc::new(CudaMemory::from_fabric(v, length)))
    }
    pub(crate) unsafe fn wrap_host(
        &self,
        node: u32,
        address: u64,
        length: usize,
        extent: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Arc<HostMemory>> {
        if !self.caps.egm {
            return Err(Error::Unavailable("MNNVL EGM disabled"));
        }
        let v = self.wrap(
            self.host_device(Some(node))?,
            Location::Host(node),
            address,
            length,
            extent,
            owner,
        )?;
        Ok(Arc::new(HostMemory::from_cuda_pinned(Arc::new(
            PinnedAllocation::from_fabric(v, length, node)?,
        ))))
    }
    fn wrap(
        &self,
        device: &Device,
        loc: Location,
        address: u64,
        length: usize,
        extent: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Mapping> {
        if address == 0
            || length == 0
            || length > extent
            || extent > isize::MAX as usize
            || address.checked_add(extent as u64).is_none()
        {
            return Err(Error::InvalidRange);
        }
        let mut mapping = Mapping::new(
            &device.context,
            self.api.clone(),
            self.caps.kind,
            extent,
            loc,
        );
        mapping.charge = Some(self.allocations.charge(extent)?);
        mapping.address = address;
        mapping.external = Some(owner);
        let verified = mapping.with_allocation(|_, prop| {
            let mut gran = 0;
            // SAFETY: retained allocation properties and a live writable granularity output.
            check("cuMemGetAllocationGranularity", unsafe {
                (self.api.granularity)(
                    &mut gran,
                    prop,
                    sys::CUmemAllocationGranularity_flags::CU_MEM_ALLOC_GRANULARITY_MINIMUM,
                )
            })?;
            if !gran.is_power_of_two()
                || address & (gran as u64 - 1) != 0
                || extent & (gran - 1) != 0
            {
                return Err(Error::InvalidRange);
            }
            let mut base = 0;
            let mut size = 0;
            // SAFETY: the caller owns the mapping. A supported range query writes these two outputs.
            let rc =
                unsafe { (device.context.driver.address_range)(&mut base, &mut size, address) };
            match rc {
                driver::SUCCESS if (base, size) == (address, extent) => Ok(()),
                driver::SUCCESS => Err(Error::InvalidRange),
                // Some drivers cannot query VMM VA ranges. Only this explicit exact-extent
                // API can use the caller's lifetime/extent guarantee on those drivers.
                sys::CUresult::CUDA_ERROR_INVALID_VALUE
                | sys::CUresult::CUDA_ERROR_NOT_SUPPORTED => Ok(()),
                rc => check("cuMemGetAddressRange", rc),
            }
        });
        mapping.finish_setup(verified)
    }
}
use crate::cuda::driver;
