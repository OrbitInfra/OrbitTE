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

pub mod device_plugin;
mod driver;
pub(crate) mod fabric;
pub(crate) mod ipc;
pub mod local_copy;
mod pinned;
mod probe;
pub(crate) use probe::discover as discover_topology;
#[cfg(feature = "rdma")]
mod rdma;
mod readiness;
mod staging;
mod stream;
mod stream_pool;
use driver::{Driver, PointerAttribute, SUCCESS, check};
pub(crate) use pinned::PinnedAllocation;
pub use pinned::allocate_pinned_host;
use readiness::CallerSyncEvent;
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

pub(crate) struct Context {
    driver: Arc<Driver>,
    raw: NonNull<sys::CUctx_st>,
    device: i32,
    device_ordinal: i32,
    primary: bool,
    owner: Option<Arc<dyn Send + Sync>>,
    poisoned: AtomicBool,
    locality: Mutex<Option<probe::DeviceLocality>>,
}
// SAFETY: Driver API contexts can be pushed on engine threads. Each operation
// restores that thread's stack and allocations serialize their accesses.
unsafe impl Send for Context {}
// SAFETY: Threads push the retained context on their own context stacks; allocation locks
// serialize memory access.
unsafe impl Sync for Context {}

impl Context {
    fn primary(driver: Arc<Driver>, device_ordinal: i32) -> Result<Arc<Self>> {
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
            device_ordinal,
            primary: true,
            owner: None,
            poisoned: AtomicBool::new(false),
            locality: Mutex::new(None),
        }))
    }

    fn current<T>(self: &Arc<Self>, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::Unquiesced {
                operation: "CUDA context restoration",
                code: -1,
            });
        }
        let caller = self.thread_context()?;
        if caller == self.raw.as_ptr() {
            // No stack frame is needed when this exact context is already bound.
            let result = operation();
            return if self.thread_context().is_ok_and(|top| top == caller) {
                result
            } else {
                Err(self.restore_failure(caller, -1))
            };
        }
        // SAFETY: the retained/borrowed context and driver outlive the stack push.
        let pushed = unsafe { (self.driver.push)(self.raw.as_ptr()) };
        if pushed != SUCCESS {
            match self.thread_context() {
                Ok(top) if top == caller => {
                    return Err(Error::Backend {
                        operation: "cuCtxPushCurrent",
                        code: pushed as i32,
                    });
                }
                Ok(top) if top == self.raw.as_ptr() => (),
                _ => return Err(self.restore_failure(caller, pushed as i32)),
            }
        }
        let result = check("cuCtxPushCurrent", pushed).and_then(|()| operation());
        let mut popped = std::ptr::null_mut();
        // SAFETY: pair the successful push, or unwind a switch that changed the
        // caller even though the driver returned an error.
        let code = unsafe { (self.driver.pop)(&mut popped) };
        if code != SUCCESS
            || popped != self.raw.as_ptr()
            || !self.thread_context().is_ok_and(|top| top == caller)
        {
            return Err(
                self.restore_failure(caller, if code == SUCCESS { -1 } else { code as i32 })
            );
        }
        result
    }
    fn thread_context(&self) -> Result<driver::Context> {
        let mut value = std::ptr::null_mut();
        // SAFETY: the driver writes the current thread’s handle, possibly null.
        check("cuCtxGetCurrent", unsafe {
            (self.driver.current)(&mut value)
        })?;
        Ok(value)
    }
    fn restore_failure(self: &Arc<Self>, caller: driver::Context, code: i32) -> Error {
        // If our frame is demonstrably still on top of a distinct caller, remove
        // that frame. Replacing it with the caller would duplicate the caller's frame.
        let unbound = if caller != self.raw.as_ptr()
            && self
                .thread_context()
                .is_ok_and(|top| top == self.raw.as_ptr())
        {
            // SAFETY: the observed top is this thread's retained owner context.
            unsafe { (self.driver.set_current)(std::ptr::null_mut()) }
        } else {
            SUCCESS
        };
        let restored = if self.thread_context().is_ok_and(|top| top == caller) {
            unbound
        } else {
            // SAFETY: restore the exact saved binding, even for a null caller.
            unsafe { (self.driver.set_current)(caller) }
        };
        if !self.poisoned.swap(true, Ordering::AcqRel) {
            RETAINED_CONTEXT.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(self.clone());
        }
        Error::Unquiesced {
            operation: "cuCtxPopCurrent restoration",
            code: if restored == SUCCESS {
                code
            } else {
                restored as i32
            },
        }
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
    producer: Option<CallerSyncEvent>,
    dma: Vec<std::ops::Range<usize>>,
    ipc_extent: Option<usize>,
    fabric: Option<Box<fabric::Mapping>>,
    #[cfg(feature = "rdma")]
    rdma: Option<rdma::Ordering>,
}

impl DeviceAllocation {
    pub(super) fn prepare_access(&mut self, range: &Range<usize>) -> Result<()> {
        self.check_access(range)?;
        self.wait_producer()
    }

    fn check_access(&self, range: &Range<usize>) -> Result<()> {
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

impl DeviceAllocation {
    fn release(&mut self) -> Result<()> {
        if self.address == 0 {
            return Ok(());
        }
        let released = !self.uncertain
            && self.wait_producer().is_ok()
            && !self.context.poisoned.load(Ordering::Acquire)
            && (if let Some(v) = self.fabric.as_mut() {
                v.close().is_ok()
            } else {
                !self.owned
                    || self
                        .context
                        .current(|| {
                            // SAFETY: this owns the allocation; copies were synchronized.
                            check("cuMemFree", unsafe {
                                (self.context.driver.free)(self.address)
                            })
                        })
                        .is_ok()
            });
        if !released {
            std::mem::forget(self.fabric.take());
            RETAINED_DEVICE.fetch_add(1, Ordering::AcqRel);
            std::mem::forget(Arc::clone(&self.context));
            if let Some(owner) = self.owner.take() {
                std::mem::forget(owner);
            }
        }
        self.address = 0; // A failed destroy must never be issued a second time.
        if released {
            Ok(())
        } else {
            Err(Error::Unquiesced {
                operation: "CUDA allocation retirement",
                code: -1,
            })
        }
    }
}
impl Drop for DeviceAllocation {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

/// Stable device allocation owner. External producers must synchronize or use
/// record_producer_stream before engine access; consumers wait for completion.
/// No CPU dereference occurs.
pub struct CudaMemory {
    address: u64,
    length: usize,
    pub(crate) location: crate::MemoryLocation,
    allocation: Mutex<DeviceAllocation>,
    pub(crate) ipc_requester: bool,
    pub(crate) ipc_exportable: bool,
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

#[cfg(feature = "gds")]
pub(crate) struct GdsLoan {
    memory: Arc<CudaMemory>,
    range: Range<usize>,
}

#[cfg(feature = "gds")]
impl Drop for GdsLoan {
    fn drop(&mut self) {
        self.memory
            .allocation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dma
            .retain(|r| r != &self.range);
    }
}

#[cfg(feature = "gds")]
impl CudaMemory {
    pub(crate) fn gds_loan(self: &Arc<Self>, offset: usize, length: usize) -> Result<GdsLoan> {
        let range = checked_range(offset, length, self.length)?;
        let mut a = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA allocation poisoned"))?;
        a.prepare_access(&range)?;
        a.dma.try_reserve(1).map_err(|_| Error::ResourceExhausted)?;
        a.dma.push(range.clone());
        Ok(GdsLoan {
            memory: self.clone(),
            range,
        })
    }
    pub(crate) fn fence_gds(&self) {
        self.allocation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .uncertain = true;
    }
    pub(crate) fn gds_current<T>(&self, call: impl FnOnce() -> Result<T>) -> Result<T> {
        let allocation = self.allocation.lock().map_err(|_| Error::Unquiesced {
            operation: "CUDA allocation lock",
            code: -1,
        })?;
        if allocation.uncertain {
            return Err(Error::Unquiesced {
                operation: "CUDA file ownership",
                code: -1,
            });
        }
        let context = allocation.context.clone();
        drop(allocation);
        context.current(call)
    }
}

impl CudaMemory {
    /// Inspect a checked range using registration-time attributes. This does not
    /// probe the driver again, wait for producers, or authorize payload access.
    pub fn location(&self, offset: usize, length: usize) -> Result<crate::RangeLocation> {
        checked_range(offset, length, self.length)?;
        Ok(crate::RangeLocation {
            address: self.address + offset as u64,
            length,
            location: self.location,
        })
    }

    /// Retire an exclusively held device allocation and report its native receipt.
    pub fn release_owned(self: Arc<Self>) -> Result<()> {
        let mut memory = Arc::try_unwrap(self).map_err(|_| Error::BatchBusy)?;
        memory
            .allocation
            .get_mut()
            .map_err(|_| Error::Internal("CUDA allocation lock poisoned"))?
            .release()
    }
    /// Identify an externally retained device allocation for the C ABI. The caller
    /// must supply live host storage if CUDA reports an unrecognized host pointer.
    ///
    /// # Safety
    /// The same ownership, access and producer handoff rules as from_raw_parts
    /// apply. A CUDA error or unsupported memory kind never authorizes a host copy.
    #[allow(unsafe_code)]
    pub unsafe fn from_foreign_address(
        address: u64,
        length: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Self>> {
        let driver = match Driver::load() {
            Ok(d) => d,
            Err(
                Error::Unavailable("CUDA driver libcuda.so.1")
                | Error::Backend {
                    operation: "cuInit",
                    code: 100,
                },
            ) => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut attrs = [
            PointerAttribute::CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
            PointerAttribute::CU_POINTER_ATTRIBUTE_IS_MANAGED,
        ];
        let mut kind = 0i32;
        let mut managed = 0u32;
        let mut values = [
            (&mut kind as *mut i32).cast(),
            (&mut managed as *mut u32).cast(),
        ];
        // SAFETY: attributes and their matching output integers live through the driver call.
        check("cuPointerGetAttributes", unsafe {
            (driver.pointer_attributes)(2, attrs.as_mut_ptr(), values.as_mut_ptr(), address)
        })?;
        if managed != 0 {
            return Err(Error::Unavailable("managed CUDA memory"));
        }
        if kind == sys::CUmemorytype::CU_MEMORYTYPE_DEVICE as i32 {
            return Self::register(address, length, owner, driver).map(Some);
        }
        if kind == 0 || kind == sys::CUmemorytype::CU_MEMORYTYPE_HOST as i32 {
            return Ok(None);
        }
        Err(Error::Unavailable("unsupported CUDA pointer type"))
    }

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
        let context = Context::primary(Driver::load()?, device)?;
        let mut allocation = DeviceAllocation {
            context: Arc::clone(&context),
            address: 0,
            owned: true,
            owner: None,
            uncertain: false,
            ipc_inflight: false,
            copy_stream: None,
            producer: None,
            dma: Vec::new(),
            fabric: None,
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
                // SAFETY: cuMemAlloc just allocated physical_length bytes in this context.
                unsafe { (context.driver.memset)(allocation.address, 0, physical_length) };
            // SAFETY: The retained context is current and the allocation remains owned until
            // synchronization returns.
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
            location: crate::MemoryLocation::Cuda {
                device: context.device_ordinal,
                context: context.raw.as_ptr() as usize,
            },
            ipc_idle: std::sync::Condvar::new(),
            allocation: Mutex::new(allocation),
            ipc_requester: true,
            ipc_exportable: ipc,
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
        if address == 0
            || length == 0
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
        let context = Context::probe(address, length, &owner, driver)?;
        Ok(Self {
            address,
            length,
            location: crate::MemoryLocation::Cuda {
                device: context.device_ordinal,
                context: context.raw.as_ptr() as usize,
            },
            ipc_idle: std::sync::Condvar::new(),
            ipc_requester: false,
            ipc_exportable: false,
            allocation: Mutex::new(DeviceAllocation {
                context,
                address,
                owned: false,
                owner: Some(owner),
                uncertain: false,
                ipc_inflight: false,
                copy_stream: None,
                producer: None,
                dma: Vec::new(),
                fabric: None,
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
        allocation.prepare_access(&(offset..offset + length))?;
        let context = Arc::clone(&allocation.context);
        let mut staging = PinnedAllocation::new(&context, length)?;
        if let Some(bytes) = source {
            staging.bytes_mut().copy_from_slice(bytes);
        }
        if allocation.copy_stream.is_none() {
            match CopyStream::new(&context) {
                Ok(stream) => allocation.copy_stream = Some(stream),
                Err(error) => return staging.close().and(Err(error)),
            }
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
            if result.is_ok() && source.is_none() {
                destination.copy_from_slice(staging.bytes());
            }
            // Cleanup runs under the same exact-context contract as the copy.
            // A failed final restore must reach the caller, including on a copy error.
            return staging.close().and(result);
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
