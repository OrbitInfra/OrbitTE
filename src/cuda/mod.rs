//! NVIDIA Driver API allocation and synchronized, owned host staging.

#![allow(unsafe_code)]

use std::fmt;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cudarc::driver::sys;

use crate::memory::checked_range;
use crate::{Error, HostMemory, Result};

mod driver;
pub(crate) mod ipc;
mod pinned;
#[cfg(feature = "rdma")]
mod rdma;
mod staging;
mod stream;
mod stream_pool;
use driver::{Driver, PointerAttribute, SUCCESS, check};
pub(crate) use pinned::PinnedAllocation;
pub use pinned::allocate_pinned_host;
pub(crate) use staging::StagingCopy;
use stream::CopyStream;
pub(crate) use stream_pool::{CUDAStreamHandle, CUDAStreamPool};

static RETAINED_DEVICE: AtomicUsize = AtomicUsize::new(0);
static RETAINED_PINNED: AtomicUsize = AtomicUsize::new(0);
static RETAINED_STREAM: AtomicUsize = AtomicUsize::new(0);
static RETAINED_EVENT: AtomicUsize = AtomicUsize::new(0);
static RETAINED_IPC: AtomicUsize = AtomicUsize::new(0);
static RETAINED_EXPORT: AtomicUsize = AtomicUsize::new(0);
static RETAINED_CONTEXT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Debug)]
pub struct RetainedResources {
    pub device_allocations: usize,
    pub ipc_mappings: usize,
    pub ipc_exports: usize,
    pub pinned_allocations: usize,
    pub contexts: usize,
    pub streams: usize,
    pub events: usize,
}

/// Failed frees/restoration are retained, not reported as released memory.
pub fn retained_resources() -> RetainedResources {
    RetainedResources {
        device_allocations: RETAINED_DEVICE.load(Ordering::Acquire),
        ipc_mappings: RETAINED_IPC.load(Ordering::Acquire),
        ipc_exports: RETAINED_EXPORT.load(Ordering::Acquire),
        pinned_allocations: RETAINED_PINNED.load(Ordering::Acquire),
        contexts: RETAINED_CONTEXT.load(Ordering::Acquire),
        streams: RETAINED_STREAM.load(Ordering::Acquire),
        events: RETAINED_EVENT.load(Ordering::Acquire),
    }
}

struct Context {
    driver: Arc<Driver>,
    raw: NonNull<sys::CUctx_st>,
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
        if code != SUCCESS || popped != self.raw.as_ptr() {
            self.poisoned.store(true, Ordering::Release);
            RETAINED_CONTEXT.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(Arc::clone(self));
            return Err(Error::Unquiesced {
                operation: "cuCtxPopCurrent restoration",
                code: if code == SUCCESS { -1 } else { code as i32 },
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
            if unsafe { (self.driver.primary_release)(self.device) } != SUCCESS {
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
    ipc_inflight: bool,
    copy_stream: Option<CopyStream>,
    dma: Vec<std::ops::Range<usize>>,
    ipc_extent: Option<usize>,
    #[cfg(feature = "rdma")]
    rdma: Option<rdma::Ordering>,
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
            return Err(Error::Unavailable("CUDA range has an active transfer loan"));
        }
        Ok(())
    }
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

/// Stable device allocation owner. Handoffs must synchronize external producers
/// before engine access and consumers after completion. No CPU dereference occurs.
pub struct CudaMemory {
    address: u64,
    length: usize,
    allocation: Mutex<DeviceAllocation>,
    ipc_idle: std::sync::Condvar,
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
        Self::allocate_owned(device, length, false)
    }

    /// Explicitly allow cooperative CUDA IPC export of this whole allocation.
    /// The physical allocation is rounded to 2 MiB and zeroed, including padding.
    /// IPC cannot enforce range permissions or revoke a previously shared handle.
    pub fn allocate_ipc(device: i32, length: usize) -> Result<Self> {
        Self::allocate_owned(device, length, true)
    }

    fn allocate_owned(device: i32, length: usize, ipc: bool) -> Result<Self> {
        if length == 0 || length > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let physical_length = if ipc {
            length
                .checked_add((2 << 20) - 1)
                .ok_or(Error::InvalidRange)?
                & !((2 << 20) - 1)
        } else {
            length
        };
        if physical_length > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let context = Context::primary(device)?;
        let mut allocation = DeviceAllocation {
            context: Arc::clone(&context),
            address: 0,
            owned: true,
            owner: None,
            uncertain: false,
            ipc_inflight: false,
            copy_stream: None,
            dma: Vec::new(),
            ipc_extent: ipc.then_some(physical_length),
            #[cfg(feature = "rdma")]
            rdma: None,
        };
        context.current(|| {
            // SAFETY: the owner is installed before allocation/init can fail.
            check("cuMemAlloc", unsafe {
                (context.driver.allocate)(&mut allocation.address, physical_length)
            })?;
            let initialized =
                unsafe { (context.driver.memset)(allocation.address, 0, physical_length) };
            let synced = unsafe { (context.driver.synchronize)() };
            allocation.uncertain = synced != SUCCESS;
            if synced != SUCCESS {
                return Err(Error::Unquiesced {
                    operation: "cuCtxSynchronize initialization",
                    code: synced as i32,
                });
            }
            check("cuMemsetD8", initialized)
        })?;
        Ok(Self {
            address: allocation.address,
            length,
            ipc_idle: std::sync::Condvar::new(),
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
            (driver.pointer_attribute)(
                (&mut memory_type as *mut i32).cast(),
                PointerAttribute::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                address,
            )
        })?;
        let mut managed = 0u32;
        check("cuPointerGetAttribute managed", unsafe {
            (driver.pointer_attribute)(
                (&mut managed as *mut u32).cast(),
                PointerAttribute::CU_POINTER_ATTRIBUTE_IS_MANAGED,
                address,
            )
        })?;
        if memory_type != sys::CUmemorytype::CU_MEMORYTYPE_DEVICE as i32 || managed != 0 {
            return Err(Error::Unavailable(
                "CUDA registration requires device memory (managed/host memory is not this profile)",
            ));
        }
        check("cuPointerGetAttribute context", unsafe {
            (driver.pointer_attribute)(
                (&mut raw as *mut driver::Context).cast(),
                PointerAttribute::CU_POINTER_ATTRIBUTE_CONTEXT,
                address,
            )
        })?;
        check("cuPointerGetAttribute device", unsafe {
            (driver.pointer_attribute)(
                (&mut device as *mut i32).cast(),
                PointerAttribute::CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL,
                address,
            )
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
            ipc_idle: std::sync::Condvar::new(),
            allocation: Mutex::new(DeviceAllocation {
                context,
                address,
                owned: false,
                owner: Some(owner),
                uncertain: false,
                ipc_inflight: false,
                copy_stream: None,
                dma: Vec::new(),
                ipc_extent: None,
                #[cfg(feature = "rdma")]
                rdma: None,
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
        allocation.check_dma(&(offset..offset + length))?;
        let context = Arc::clone(&allocation.context);
        let mut staging = PinnedAllocation::new(&context, length)?;
        if let Some(bytes) = source {
            staging.bytes_mut().copy_from_slice(bytes);
        }
        if allocation.copy_stream.is_none() {
            allocation.copy_stream = Some(CopyStream::new(&context)?);
        }
        let stream = allocation
            .copy_stream
            .as_mut()
            .ok_or(Error::Internal("CUDA copy stream missing"))?;
        // SAFETY: GPU and private pinned staging remain owned until the event
        // completes, or are retained below with the stream on ambiguous errors.
        let result = unsafe {
            stream.copy(
                self.address + offset as u64,
                staging.address,
                length,
                source.is_some(),
            )
        };
        allocation.uncertain |= !stream.quiescent();
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
