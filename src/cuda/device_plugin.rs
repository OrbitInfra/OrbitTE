//! Checked CUDA operations for the optional native device-plugin boundary.
use super::*;

/// Enumerate devices through the same fallible table as CUDA registrations.
pub fn device_count() -> Result<i32> {
    let driver = Driver::load()?;
    let mut count = 0;
    // SAFETY: initialized output, retained driver, no engine or allocation.
    check("cuDeviceGetCount", unsafe {
        (driver.device_count)(&mut count)
    })?;
    if !(0..=64).contains(&count) {
        return Err(Error::Unavailable(
            "CUDA device count outside plugin budget",
        ));
    }
    Ok(count)
}
/// Return the CUDA driver's bounded PCI identity, without creating a context.
pub fn device_pci_bus_id(ordinal: i32) -> Result<String> {
    let driver = Driver::load()?;
    let mut device = 0;
    // SAFETY: initialized output and a retained driver.
    check("cuDeviceGet", unsafe {
        (driver.device_get)(&mut device, ordinal)
    })?;
    let mut bytes = [0 as std::ffi::c_char; 32];
    // SAFETY: initialized fixed-size output.
    check("cuDeviceGetPCIBusId", unsafe {
        (driver.pci_bus_id)(bytes.as_mut_ptr(), bytes.len() as i32, device)
    })?;
    let end = bytes
        .iter()
        .position(|&b| b == 0)
        .ok_or(Error::Unavailable("CUDA PCI identity not terminated"))?;
    String::from_utf8(bytes[..end].iter().map(|b| b.to_ne_bytes()[0]).collect())
        .map_err(|_| Error::Unavailable("CUDA PCI identity is not UTF-8"))
}
impl CudaMemory {
    /// Synchronous native-plugin copy using this allocation's exact context.
    ///
    /// # Safety
    /// `other` is a live, accessible range of `length` bytes, retained through
    /// completion. On Unquiesced it must remain retained indefinitely. Callers
    /// serialize accesses to both endpoints and cannot overlap them.
    pub unsafe fn plugin_copy_sync(
        &self,
        offset: usize,
        other: u64,
        length: usize,
        to_device: bool,
    ) -> Result<()> {
        checked_range(offset, length, self.length)?;
        if other == 0 || other.checked_add(length as u64).is_none() {
            return Err(Error::InvalidRange);
        }
        let mut allocation = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA plugin allocation lock poisoned"))?;
        if allocation.uncertain {
            return Err(Error::Unquiesced {
                operation: "CUDA plugin prior copy",
                code: -1,
            });
        }
        allocation.prepare_access(&(offset..offset + length))?;
        let context = allocation.context.clone();
        let local = self.address + offset as u64;
        let (dst, src) = if to_device {
            (local, other)
        } else {
            (other, local)
        };
        let result = context.current(|| {
            // SAFETY: both endpoints remain pinned by the native-plugin caller;
            // the allocation's context is current and the call is synchronous.
            check("cuMemcpy plugin", unsafe {
                (context.driver.copy_sync)(dst, src, length)
            })
        });
        if result.is_err() {
            allocation.uncertain = true;
            return Err(Error::Unquiesced {
                operation: "CUDA plugin copy or context restore",
                code: -1,
            });
        }
        Ok(())
    }
}
