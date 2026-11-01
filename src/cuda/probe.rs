//! CUDA platform pointer validation and context-owned locality discovery.

use super::*;
use crate::platform::numa::Numa;

pub(super) struct DeviceLocality {
    provider: &'static Numa,
    node: Option<u32>,
}

impl Context {
    pub(super) fn probe(
        address: u64,
        length: usize,
        owner: &Arc<dyn Send + Sync>,
        driver: Arc<Driver>,
    ) -> Result<Arc<Self>> {
        if address == 0
            || length == 0
            || length > isize::MAX as usize
            || address.checked_add(length as u64).is_none()
        {
            return Err(Error::InvalidRange);
        }
        let mut memory_type = 0i32;
        let mut managed = 0u32;
        let mut raw: driver::Context = std::ptr::null_mut();
        let mut device = -1i32;
        let mut attributes = [
            PointerAttribute::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
            PointerAttribute::CU_POINTER_ATTRIBUTE_IS_MANAGED,
            PointerAttribute::CU_POINTER_ATTRIBUTE_CONTEXT,
            PointerAttribute::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
        ];
        let mut outputs = [
            (&mut memory_type as *mut i32).cast(),
            (&mut managed as *mut u32).cast(),
            (&mut raw as *mut driver::Context).cast(),
            (&mut device as *mut i32).cast(),
        ];
        // SAFETY: typed outputs match these four attributes. The caller retains
        // the actual stable allocation/context throughout metadata validation.
        check("cuPointerGetAttributes", unsafe {
            (driver.pointer_attributes)(
                attributes.len() as u32,
                attributes.as_mut_ptr(),
                outputs.as_mut_ptr(),
                address,
            )
        })?;
        // Unlike the singular API, an unknown pointer can return success with
        // null/default attributes. Neither that nor a driver error proves DRAM.
        if memory_type != sys::CUmemorytype::CU_MEMORYTYPE_DEVICE as i32
            || managed != 0
            || device < 0
            || raw.is_null()
        {
            return Err(Error::Unavailable(
                "CUDA registration requires known non-managed device memory and its context",
            ));
        }
        let context = Arc::new(Self {
            driver,
            raw: NonNull::new(raw).ok_or(Error::InvalidRange)?,
            device,
            primary: false,
            owner: Some(Arc::clone(owner)),
            poisoned: AtomicBool::new(false),
            locality: Mutex::new(None),
        });
        context.current(|| {
            let mut base = 0;
            let mut size = 0;
            // SAFETY: query the actual mapped allocation, not a possibly larger
            // reserved virtual-address range returned by RANGE_SIZE attributes.
            check("cuMemGetAddressRange", unsafe {
                (context.driver.address_range)(&mut base, &mut size, address)
            })?;
            let offset = usize::try_from(address.checked_sub(base).ok_or(Error::InvalidRange)?)
                .map_err(|_| Error::InvalidRange)?;
            checked_range(offset, length, size)?;
            Ok(())
        })?;
        Ok(context)
    }

    pub(super) fn gpu_numa_node(self: &Arc<Self>, numa: &'static Numa) -> Result<Option<u32>> {
        let mut locality = self
            .locality
            .lock()
            .map_err(|_| Error::Internal("CUDA locality lock poisoned"))?;
        if let Some(cached) = locality.as_ref().filter(|v| std::ptr::eq(v.provider, numa)) {
            return Ok(cached.node);
        }
        let pci = self.current(|| self.driver.pci_bus(self.device))?;
        let node = (numa.gpu_node)(&pci);
        *locality = Some(DeviceLocality {
            provider: numa,
            node,
        });
        Ok(node)
    }
}

impl Driver {
    pub(super) fn pci_bus(&self, device: i32) -> Result<String> {
        let mut pci = [0 as std::ffi::c_char; 20];
        // SAFETY: bounded initialized output and a driver-visible device handle.
        check("cuDeviceGetPCIBusId", unsafe {
            (self.pci_bus_id)(pci.as_mut_ptr(), pci.len() as i32, device)
        })?;
        let end = pci
            .iter()
            .position(|c| *c == 0)
            .ok_or(Error::Protocol("CUDA PCI bus id"))?;
        let pci: String = pci[..end].iter().map(|c| *c as u8 as char).collect();
        crate::platform::pci::bus_id(&pci).ok_or(Error::Protocol("CUDA PCI bus id"))
    }
}

#[cfg(feature = "rdma")]
mod topology;
#[cfg(feature = "rdma")]
pub(crate) use topology::discover;
