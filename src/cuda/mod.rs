//! NVIDIA Driver API allocation and synchronized, owned host staging.

#![allow(unsafe_code)]

use std::ffi::c_void;
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::memory::checked_range;
use crate::{Error, HostMemory, Result};

mod driver;
use driver::{Driver, check};

static RETAINED_DEVICE: AtomicUsize = AtomicUsize::new(0);
static RETAINED_PINNED: AtomicUsize = AtomicUsize::new(0);
static RETAINED_CONTEXT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug)]
pub struct RetainedResources {
    pub device_allocations: usize,
    pub pinned_allocations: usize,
    pub contexts: usize,
}

/// Failed frees/restoration are retained, not reported as released memory.
pub fn retained_resources() -> RetainedResources {
    RetainedResources {
        device_allocations: RETAINED_DEVICE.load(Ordering::Acquire),
        pinned_allocations: RETAINED_PINNED.load(Ordering::Acquire),
        contexts: RETAINED_CONTEXT.load(Ordering::Acquire),
    }
}

struct Context {
    driver: Arc<Driver>,
    raw: NonNull<c_void>,
    device: i32,
    primary: bool,
    owner: Option<Arc<dyn Send + Sync>>,
    poisoned: AtomicBool,
}
// SAFETY: Driver API contexts can be pushed on engine threads. Each operation
// restores that thread's stack and allocations serialize their accesses.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    fn primary(device_ordinal: i32) -> Result<Arc<Self>> {
        let driver = Driver::load()?;
        let mut device = 0;
        // SAFETY: initialized output and a live driver library.
        check("cuDeviceGet", unsafe {
            (driver.device_get)(&mut device, device_ordinal)
        })?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: initialized output; the returned primary reference is owned.
        check("cuDevicePrimaryCtxRetain", unsafe {
            (driver.primary_retain)(&mut raw, device)
        })?;
        Ok(Arc::new(Self {
            driver,
            raw: NonNull::new(raw).ok_or(Error::Internal("CUDA returned a null context"))?,
            device,
            primary: true,
            owner: None,
            poisoned: AtomicBool::new(false),
        }))
    }

    fn current<T>(self: &Arc<Self>, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::Unquiesced {
                operation: "CUDA context restoration",
                code: -1,
            });
        }
        // SAFETY: the retained/borrowed context and driver outlive the stack push.
        check("cuCtxPushCurrent", unsafe {
            (self.driver.push)(self.raw.as_ptr())
        })?;
        let result = operation();
        let mut popped = std::ptr::null_mut();
        // SAFETY: exactly one pop pairs with this push, including operation errors.
        let code = unsafe { (self.driver.pop)(&mut popped) };
        if code != 0 || popped != self.raw.as_ptr() {
            self.poisoned.store(true, Ordering::Release);
            RETAINED_CONTEXT.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(Arc::clone(self));
            return Err(Error::Unquiesced {
                operation: "cuCtxPopCurrent restoration",
                code: if code == 0 { -1 } else { code },
            });
        }
        result
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if self.primary {
            // SAFETY: this owns one primary-context reference and no allocation
            // or context guard can remain when its final Arc reaches Drop.
            if unsafe { (self.driver.primary_release)(self.device) } != 0 {
                RETAINED_CONTEXT.fetch_add(1, Ordering::AcqRel);
                std::mem::forget(Arc::clone(&self.driver));
                if let Some(owner) = self.owner.take() {
                    std::mem::forget(owner);
                }
            }
        }
    }
}

struct DeviceAllocation {
    context: Arc<Context>,
    address: u64,
    owned: bool,
    owner: Option<Arc<dyn Send + Sync>>,
    uncertain: bool,
}

impl Drop for DeviceAllocation {
    fn drop(&mut self) {
        if self.address == 0 {
            return;
        }
        let released = !self.uncertain
            && !self.context.poisoned.load(Ordering::Acquire)
            && (!self.owned
                || self
                    .context
                    .current(|| {
                        // SAFETY: this owns the allocation; copies were synchronized.
                        check("cuMemFree", unsafe {
                            (self.context.driver.free)(self.address)
                        })
                    })
                    .is_ok());
        if !released {
            RETAINED_DEVICE.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(Arc::clone(&self.context));
            if let Some(owner) = self.owner.take() {
                std::mem::forget(owner);
            }
        }
    }
}

struct PinnedAllocation {
    context: Arc<Context>,
    address: NonNull<u8>,
    length: usize,
}
// SAFETY: stable CUDA-pinned host bytes are only exposed through the shared
// HostMemory lock or privately owned staging. The allocation never reallocates.
unsafe impl Send for PinnedAllocation {}
unsafe impl Sync for PinnedAllocation {}

impl PinnedAllocation {
    fn new(context: &Arc<Context>, length: usize) -> Result<Self> {
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
        })
    }
    fn bytes(&self) -> &[u8] {
        // SAFETY: initialized stable bytes, no DMA remains at callers of this method.
        unsafe { std::slice::from_raw_parts(self.address.as_ptr(), self.length) }
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: exclusive staging before DMA submission.
        unsafe { std::slice::from_raw_parts_mut(self.address.as_ptr(), self.length) }
    }
}

impl Drop for PinnedAllocation {
    fn drop(&mut self) {
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
    // SAFETY: the actual pinned owner retains stable initialized bytes and its
    // deallocator/context. HostMemory is the only public byte access owner.
    Ok(Arc::new(unsafe {
        HostMemory::from_raw_parts(allocation.address, length, allocation)
    }?))
}

/// Stable device allocation owner. Handoffs must synchronize external producers
/// before engine access and consumers after completion. No CPU dereference occurs.
pub struct CudaMemory {
    address: u64,
    length: usize,
    allocation: Mutex<DeviceAllocation>,
}

impl fmt::Debug for CudaMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CudaMemory")
            .field("address", &self.address)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl CudaMemory {
    pub fn allocate(device: i32, length: usize) -> Result<Self> {
        if length == 0 || length > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let context = Context::primary(device)?;
        let mut allocation = DeviceAllocation {
            context: Arc::clone(&context),
            address: 0,
            owned: true,
            owner: None,
            uncertain: false,
        };
        context.current(|| {
            // SAFETY: the owner is installed before allocation/init can fail.
            check("cuMemAlloc", unsafe {
                (context.driver.allocate)(&mut allocation.address, length)
            })?;
            let initialized = unsafe { (context.driver.memset)(allocation.address, 0, length) };
            let synced = unsafe { (context.driver.synchronize)() };
            allocation.uncertain = synced != 0;
            check("cuCtxSynchronize initialization", synced)?;
            check("cuMemsetD8", initialized)
        })?;
        Ok(Self {
            address: allocation.address,
            length,
            allocation: Mutex::new(allocation),
        })
    }

    /// # Safety
    /// The actual owner must retain the stable writable device allocation AND
    /// its CUDA context through the last registration/operation. Coordinate
    /// external streams and address reuse. Aliases share this Arc<CudaMemory>.
    pub unsafe fn from_raw_parts(
        address: u64,
        length: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Self> {
        if length == 0
            || length > isize::MAX as usize
            || address.checked_add(length as u64).is_none()
        {
            return Err(Error::InvalidRange);
        }
        Self::register(address, length, owner, Driver::load()?)
    }

    fn register(
        address: u64,
        length: usize,
        owner: Arc<dyn Send + Sync>,
        driver: Arc<Driver>,
    ) -> Result<Self> {
        let mut memory_type = 0i32;
        let mut raw = std::ptr::null_mut();
        let mut device = 0i32;
        // SAFETY: typed initialized outputs match CUDA's pointer attributes.
        check("cuPointerGetAttribute memory type", unsafe {
            (driver.pointer_attribute)((&mut memory_type as *mut i32).cast(), 2, address)
        })?;
        if memory_type != 2 {
            return Err(Error::Unavailable(
                "CUDA registration requires device memory (managed/host memory is not this profile)",
            ));
        }
        check("cuPointerGetAttribute context", unsafe {
            (driver.pointer_attribute)((&mut raw as *mut driver::Context).cast(), 1, address)
        })?;
        check("cuPointerGetAttribute device", unsafe {
            (driver.pointer_attribute)((&mut device as *mut i32).cast(), 9, address)
        })?;
        let context = Arc::new(Context {
            driver,
            raw: NonNull::new(raw).ok_or(Error::InvalidRange)?,
            device,
            primary: false,
            owner: Some(Arc::clone(&owner)),
            poisoned: AtomicBool::new(false),
        });
        context.current(|| {
            let mut base = 0;
            let mut size = 0;
            // SAFETY: this only probes driver-owned range metadata in its context.
            check("cuMemGetAddressRange", unsafe {
                (context.driver.address_range)(&mut base, &mut size, address)
            })?;
            let offset = usize::try_from(address.checked_sub(base).ok_or(Error::InvalidRange)?)
                .map_err(|_| Error::InvalidRange)?;
            checked_range(offset, length, size)?;
            Ok(())
        })?;
        Ok(Self {
            address,
            length,
            allocation: Mutex::new(DeviceAllocation {
                context,
                address,
                owned: false,
                owner: Some(owner),
                uncertain: false,
            }),
        })
    }

    pub fn base_address(&self) -> u64 {
        self.address
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn read(&self, offset: usize, destination: &mut [u8]) -> Result<()> {
        checked_range(offset, destination.len(), self.length)?;
        self.copy(offset, destination, None)
    }
    pub fn write(&self, offset: usize, source: &[u8]) -> Result<()> {
        checked_range(offset, source.len(), self.length)?;
        self.copy(offset, &mut [], Some(source))
    }

    fn copy(&self, offset: usize, destination: &mut [u8], source: Option<&[u8]>) -> Result<()> {
        let length = source.map_or(destination.len(), <[u8]>::len);
        if length > 64 << 20 {
            return Err(Error::ResourceExhausted);
        }
        let mut allocation = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        if allocation.uncertain {
            return Err(Error::Unquiesced {
                operation: "CUDA prior copy",
                code: -1,
            });
        }
        let context = Arc::clone(&allocation.context);
        let mut staging = PinnedAllocation::new(&context, length)?;
        if let Some(bytes) = source {
            staging.bytes_mut().copy_from_slice(bytes);
        }
        let result = context.current(|| {
            // SAFETY: GPU and pinned bytes remain owned across copy, synchronize,
            // restoration and any retained error. Caller host slices are never
            // handed to CUDA, so an ambiguous copy cannot outlive their borrow.
            let code = unsafe {
                if source.is_some() {
                    (context.driver.to_device)(
                        self.address + offset as u64,
                        staging.address.as_ptr().cast(),
                        length,
                    )
                } else {
                    (context.driver.to_host)(
                        staging.address.as_ptr().cast(),
                        self.address + offset as u64,
                        length,
                    )
                }
            };
            let synced = unsafe { (context.driver.synchronize)() };
            allocation.uncertain = synced != 0;
            if synced != 0 {
                return Err(Error::Unquiesced {
                    operation: "cuCtxSynchronize copy",
                    code: synced,
                });
            }
            check("CUDA staged copy", code)
        });
        if allocation.uncertain || context.poisoned.load(Ordering::Acquire) {
            RETAINED_PINNED.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(staging);
        } else {
            result?;
            if source.is_none() {
                destination.copy_from_slice(staging.bytes());
            }
            return Ok(());
        }
        match result {
            Err(error) => Err(error),
            Ok(()) => Err(Error::Unquiesced {
                operation: "CUDA context restoration",
                code: -1,
            }),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/cuda/mod.rs"]
mod tests;
