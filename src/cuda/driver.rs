#![allow(unsafe_code)]

use std::ffi::{c_int, c_uint, c_void};
use std::sync::Arc;

use cudarc::driver::sys;
use libloading::Library;

use crate::{Error, Result};

pub(super) type Context = sys::CUcontext;
pub(super) type Status = sys::CUresult;
pub(super) use sys::CUpointer_attribute as PointerAttribute;
pub(super) const SUCCESS: Status = Status::CUDA_SUCCESS;

pub(super) type CopyBatch = unsafe extern "C" fn(
    *mut u64,
    *mut u64,
    *mut usize,
    usize,
    *mut sys::CUmemcpyAttributes,
    *mut usize,
    usize,
    sys::CUstream,
) -> Status;

// The CUDA 12.8 entry reports a failure index. It is not ABI-compatible with v2.
pub(super) type CopyBatch128 = unsafe extern "C" fn(
    *mut u64,
    *mut u64,
    *mut usize,
    usize,
    *mut sys::CUmemcpyAttributes,
    *mut usize,
    usize,
    *mut usize,
    sys::CUstream,
) -> Status;
#[derive(Clone, Copy)]
pub(super) enum BatchEntry {
    Current(CopyBatch),
    WithFailIndex(CopyBatch128),
}

#[derive(Clone, Copy)]
pub(super) struct VmmApi {
    pub count: unsafe extern "C" fn(*mut c_int) -> Status,
    pub attribute: unsafe extern "C" fn(*mut c_int, sys::CUdevice_attribute, c_int) -> Status,
    pub can_peer: unsafe extern "C" fn(*mut c_int, c_int, c_int) -> Status,
    pub enable_peer: unsafe extern "C" fn(Context, c_uint) -> Status,
    pub granularity: unsafe extern "C" fn(
        *mut usize,
        *const sys::CUmemAllocationProp,
        sys::CUmemAllocationGranularity_flags,
    ) -> Status,
    pub create:
        unsafe extern "C" fn(*mut u64, usize, *const sys::CUmemAllocationProp, u64) -> Status,
    pub retain: unsafe extern "C" fn(*mut u64, *mut c_void) -> Status,
    pub properties: unsafe extern "C" fn(*mut sys::CUmemAllocationProp, u64) -> Status,
    pub release: unsafe extern "C" fn(u64) -> Status,
    pub reserve: unsafe extern "C" fn(*mut u64, usize, usize, u64, u64) -> Status,
    pub free: unsafe extern "C" fn(u64, usize) -> Status,
    pub map: unsafe extern "C" fn(u64, usize, usize, u64, u64) -> Status,
    pub unmap: unsafe extern "C" fn(u64, usize) -> Status,
    pub access: unsafe extern "C" fn(u64, usize, *const sys::CUmemAccessDesc, usize) -> Status,
    pub export:
        unsafe extern "C" fn(*mut c_void, u64, sys::CUmemAllocationHandleType, u64) -> Status,
    pub import:
        unsafe extern "C" fn(*mut u64, *mut c_void, sys::CUmemAllocationHandleType) -> Status,
}

pub(super) type LaunchKernel = unsafe extern "C" fn(
    sys::CUfunction,
    u32,
    u32,
    u32,
    u32,
    u32,
    u32,
    u32,
    sys::CUstream,
    *mut *mut c_void,
    *mut *mut c_void,
) -> Status;

#[derive(Clone, Copy)]
pub(super) struct KernelApi {
    pub load: unsafe extern "C" fn(*mut sys::CUmodule, *const c_void) -> Status,
    pub function: unsafe extern "C" fn(
        *mut sys::CUfunction,
        sys::CUmodule,
        *const std::ffi::c_char,
    ) -> Status,
    pub unload: unsafe extern "C" fn(sys::CUmodule) -> Status,
    pub launch: LaunchKernel,
    pub mapped: unsafe extern "C" fn(*mut u64, *mut c_void, u32) -> Status,
}
#[derive(Clone, Copy, Default)]
pub(super) struct LocalApi {
    pub batch_with_fail_index: Option<CopyBatch128>,
    pub attribute: Option<unsafe extern "C" fn(*mut i32, sys::CUdevice_attribute, i32) -> Status>,
    pub copy_2d: Option<unsafe extern "C" fn(*const sys::CUDA_MEMCPY2D, sys::CUstream) -> Status>,
    pub wait_event: Option<unsafe extern "C" fn(sys::CUstream, sys::CUevent, u32) -> Status>,
    pub kernel: Option<KernelApi>,
}
pub(super) struct Driver {
    pub(super) local: LocalApi,
    pub(super) vmm: Option<VmmApi>,
    pub(super) _library: Option<Library>,
    pub init: unsafe extern "C" fn(c_uint) -> Status,
    pub device_get: unsafe extern "C" fn(*mut c_int, c_int) -> Status,
    pub device_count: unsafe extern "C" fn(*mut c_int) -> Status,
    #[cfg(feature = "rdma")]
    pub device_attribute:
        unsafe extern "C" fn(*mut c_int, sys::CUdevice_attribute, c_int) -> Status,
    #[cfg(feature = "rdma")]
    pub set_pointer_attribute: unsafe extern "C" fn(*const c_void, PointerAttribute, u64) -> Status,
    #[cfg(feature = "rdma")]
    pub flush_rdma: unsafe extern "C" fn(
        sys::CUflushGPUDirectRDMAWritesTarget,
        sys::CUflushGPUDirectRDMAWritesScope,
    ) -> Status,
    pub primary_retain: unsafe extern "C" fn(*mut Context, c_int) -> Status,
    pub primary_release: unsafe extern "C" fn(c_int) -> Status,
    pub current: unsafe extern "C" fn(*mut Context) -> Status,
    pub set_current: unsafe extern "C" fn(Context) -> Status,
    pub push: unsafe extern "C" fn(Context) -> Status,
    pub pop: unsafe extern "C" fn(*mut Context) -> Status,
    pub synchronize: unsafe extern "C" fn() -> Status,
    pub allocate: unsafe extern "C" fn(*mut u64, usize) -> Status,
    pub free: unsafe extern "C" fn(u64) -> Status,
    pub memset: unsafe extern "C" fn(u64, u8, usize) -> Status,
    pub ipc_export: unsafe extern "C" fn(*mut sys::CUipcMemHandle, u64) -> Status,
    pub ipc_open: unsafe extern "C" fn(*mut u64, sys::CUipcMemHandle, c_uint) -> Status,
    pub ipc_close: unsafe extern "C" fn(u64) -> Status,
    pub copy_batch: Option<CopyBatch>,
    pub copy_sync: unsafe extern "C" fn(u64, u64, usize) -> Status,
    pub copy_device: unsafe extern "C" fn(u64, u64, usize, sys::CUstream) -> Status,
    pub to_host: unsafe extern "C" fn(*mut c_void, u64, usize, sys::CUstream) -> Status,
    pub to_device: unsafe extern "C" fn(u64, *const c_void, usize, sys::CUstream) -> Status,
    pub stream_context: unsafe extern "C" fn(sys::CUstream, *mut Context) -> Status,
    pub stream_capture:
        unsafe extern "C" fn(sys::CUstream, *mut sys::CUstreamCaptureStatus) -> Status,
    pub stream_create: unsafe extern "C" fn(*mut sys::CUstream, c_uint) -> Status,
    pub stream_destroy: unsafe extern "C" fn(sys::CUstream) -> Status,
    pub stream_synchronize: unsafe extern "C" fn(sys::CUstream) -> Status,
    pub event_create: unsafe extern "C" fn(*mut sys::CUevent, c_uint) -> Status,
    pub event_destroy: unsafe extern "C" fn(sys::CUevent) -> Status,
    pub event_record: unsafe extern "C" fn(sys::CUevent, sys::CUstream) -> Status,
    pub event_synchronize: unsafe extern "C" fn(sys::CUevent) -> Status,
    pub event_query: unsafe extern "C" fn(sys::CUevent) -> Status,
    pub host_allocate: unsafe extern "C" fn(*mut *mut c_void, usize, c_uint) -> Status,
    pub host_free: unsafe extern "C" fn(*mut c_void) -> Status,
    pub host_register: unsafe extern "C" fn(*mut c_void, usize, c_uint) -> Status,
    pub host_unregister: unsafe extern "C" fn(*mut c_void) -> Status,
    pub pci_bus_id: unsafe extern "C" fn(*mut std::ffi::c_char, c_int, c_int) -> Status,
    pub pointer_attributes:
        unsafe extern "C" fn(c_uint, *mut PointerAttribute, *mut *mut c_void, u64) -> Status,
    pub address_range: unsafe extern "C" fn(*mut u64, *mut usize, u64) -> Status,
}

macro_rules! symbol {
    ($lib:expr, $field:ident) => {
        symbol(
            $lib,
            concat!(stringify!($field), "\0").as_bytes(),
            concat!("CUDA driver symbol ", stringify!($field)),
            sys::$field as _,
        )?
    };
}

impl VmmApi {
    fn load(library: &Library) -> Result<Self> {
        Ok(Self {
            count: symbol!(library, cuDeviceGetCount),
            attribute: symbol!(library, cuDeviceGetAttribute),
            can_peer: symbol!(library, cuDeviceCanAccessPeer),
            enable_peer: symbol!(library, cuCtxEnablePeerAccess),
            granularity: symbol!(library, cuMemGetAllocationGranularity),
            create: symbol!(library, cuMemCreate),
            retain: symbol!(library, cuMemRetainAllocationHandle),
            properties: symbol!(library, cuMemGetAllocationPropertiesFromHandle),
            release: symbol!(library, cuMemRelease),
            reserve: symbol!(library, cuMemAddressReserve),
            free: symbol!(library, cuMemAddressFree),
            map: symbol!(library, cuMemMap),
            unmap: symbol!(library, cuMemUnmap),
            access: symbol!(library, cuMemSetAccess),
            export: symbol!(library, cuMemExportToShareableHandle),
            import: symbol!(library, cuMemImportFromShareableHandle),
        })
    }
}

impl Driver {
    pub(super) fn load() -> Result<Arc<Self>> {
        Self::open(false)
    }
    pub(super) fn load_fabric() -> Result<Arc<Self>> {
        Self::open(true)
    }
    fn open(fabric: bool) -> Result<Arc<Self>> {
        // SAFETY: load only the system Driver API. The library is retained by
        // every allocation/context, including resources whose cleanup failed.
        let library = unsafe { Library::new("libcuda.so.1") }
            .map_err(|_| Error::Unavailable("CUDA driver libcuda.so.1"))?;
        Self::from_library(library, fabric)
    }

    fn from_library(library: Library, fabric: bool) -> Result<Arc<Self>> {
        let vmm = fabric.then(|| VmmApi::load(&library)).transpose()?;
        let driver = Self {
            local: LocalApi {
                // SAFETY: the unversioned Driver API symbol has the nine-argument CUDA
                // 12.8 signature above. The owning Driver retains the loaded library.
                batch_with_fail_index: unsafe {
                    library.get::<CopyBatch128>(b"cuMemcpyBatchAsync\0")
                }
                .ok()
                .map(|s| *s),
                attribute: symbol(
                    &library,
                    b"cuDeviceGetAttribute\0",
                    "cuDeviceGetAttribute",
                    sys::cuDeviceGetAttribute as _,
                )
                .ok(),
                copy_2d: symbol(
                    &library,
                    b"cuMemcpy2DAsync_v2\0",
                    "cuMemcpy2DAsync_v2",
                    sys::cuMemcpy2DAsync_v2 as _,
                )
                .ok(),
                wait_event: symbol(
                    &library,
                    b"cuStreamWaitEvent\0",
                    "cuStreamWaitEvent",
                    sys::cuStreamWaitEvent as _,
                )
                .ok(),
                kernel: (|| -> Result<_> {
                    Ok(KernelApi {
                        load: symbol!(&library, cuModuleLoadData),
                        function: symbol!(&library, cuModuleGetFunction),
                        unload: symbol!(&library, cuModuleUnload),
                        launch: symbol!(&library, cuLaunchKernel),
                        mapped: symbol!(&library, cuMemHostGetDevicePointer_v2),
                    })
                })()
                .ok(),
            },
            vmm,
            init: symbol!(&library, cuInit),
            device_get: symbol!(&library, cuDeviceGet),
            device_count: symbol!(&library, cuDeviceGetCount),
            #[cfg(feature = "rdma")]
            device_attribute: symbol!(&library, cuDeviceGetAttribute),
            #[cfg(feature = "rdma")]
            set_pointer_attribute: symbol!(&library, cuPointerSetAttribute),
            #[cfg(feature = "rdma")]
            flush_rdma: symbol!(&library, cuFlushGPUDirectRDMAWrites),
            primary_retain: symbol!(&library, cuDevicePrimaryCtxRetain),
            primary_release: symbol!(&library, cuDevicePrimaryCtxRelease_v2),
            current: symbol!(&library, cuCtxGetCurrent),
            set_current: symbol!(&library, cuCtxSetCurrent),
            push: symbol!(&library, cuCtxPushCurrent_v2),
            pop: symbol!(&library, cuCtxPopCurrent_v2),
            synchronize: symbol!(&library, cuCtxSynchronize),
            allocate: symbol!(&library, cuMemAlloc_v2),
            free: symbol!(&library, cuMemFree_v2),
            memset: symbol!(&library, cuMemsetD8_v2),
            ipc_export: symbol!(&library, cuIpcGetMemHandle),
            ipc_open: symbol!(&library, cuIpcOpenMemHandle_v2),
            ipc_close: symbol!(&library, cuIpcCloseMemHandle),
            copy_batch: batch_copy(&library),
            copy_sync: symbol!(&library, cuMemcpy),
            copy_device: symbol!(&library, cuMemcpyAsync),
            to_host: symbol!(&library, cuMemcpyDtoHAsync_v2),
            to_device: symbol!(&library, cuMemcpyHtoDAsync_v2),
            stream_context: symbol!(&library, cuStreamGetCtx),
            stream_capture: symbol!(&library, cuStreamIsCapturing),
            stream_create: symbol!(&library, cuStreamCreate),
            stream_destroy: symbol!(&library, cuStreamDestroy_v2),
            stream_synchronize: symbol!(&library, cuStreamSynchronize),
            event_create: symbol!(&library, cuEventCreate),
            event_destroy: symbol!(&library, cuEventDestroy_v2),
            event_record: symbol!(&library, cuEventRecord),
            event_synchronize: symbol!(&library, cuEventSynchronize),
            event_query: symbol!(&library, cuEventQuery),
            host_allocate: symbol!(&library, cuMemHostAlloc),
            host_free: symbol!(&library, cuMemFreeHost),
            host_register: symbol!(&library, cuMemHostRegister_v2),
            host_unregister: symbol!(&library, cuMemHostUnregister),
            pci_bus_id: symbol!(&library, cuDeviceGetPCIBusId),
            pointer_attributes: symbol!(&library, cuPointerGetAttributes),
            address_range: symbol!(&library, cuMemGetAddressRange_v2),
            _library: Some(library),
        };
        // SAFETY: CUDA driver initialization is thread safe and uses flags zero.
        check("cuInit", unsafe { (driver.init)(0) })?;
        Ok(Arc::new(driver))
    }
}

// cudarc exposes Rust wrappers for the CUDA entry points. Check each signature
// against its C function pointer at compile time without calling the wrapper.
trait DriverEntry: Copy {
    type Wrapper;
}

macro_rules! driver_entries {
    ($(($($arg:ident),*)),+) => {
        $(impl<R, $($arg),*> DriverEntry for unsafe extern "C" fn($($arg),*) -> R {
            type Wrapper = unsafe fn($($arg),*) -> R;
        })+
    };
}
driver_entries!(
    (),
    (A),
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F, G, H),
    (A, B, C, D, E, F, G, H, I, J, K)
);

fn symbol<F: DriverEntry>(
    library: &Library,
    name: &[u8],
    operation: &'static str,
    _signature: F::Wrapper,
) -> Result<F> {
    // SAFETY: the named CUDA symbol and its generated wrapper have matching
    // arguments and return type. Driver retains the library for every entry.
    unsafe { library.get::<F>(name) }
        .map(|symbol| *symbol)
        .map_err(|_| Error::Unavailable(operation))
}

// CUDA 12.8's unversioned batch entry has an extra failIdx argument. Only
// select the exact CUDA 13 ABI, and make this an optional capability.
fn batch_copy(library: &Library) -> Option<CopyBatch> {
    let version: unsafe extern "C" fn(*mut c_int) -> Status = symbol(
        library,
        b"cuDriverGetVersion\0",
        "cuDriverGetVersion",
        sys::cuDriverGetVersion as _,
    )
    .ok()?;
    let mut supported = 0;
    // SAFETY: the generated entry writes one live version output.
    if unsafe { version(&mut supported) } != SUCCESS || supported < 13000 {
        return None;
    }
    symbol(
        library,
        b"cuMemcpyBatchAsync_v2\0",
        "cuMemcpyBatchAsync_v2",
        sys::cuMemcpyBatchAsync_v2 as _,
    )
    .ok()
}

pub(super) unsafe fn enqueue_batch(
    call: BatchEntry,
    destinations: &mut [u64],
    sources: &mut [u64],
    lengths: &mut [usize],
    stream: sys::CUstream,
) -> Result<()> {
    if destinations.len() != lengths.len() || sources.len() != lengths.len() {
        return Err(Error::InvalidRange);
    }
    let location = sys::CUmemLocation {
        type_: sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_INVALID,
        id: 0,
    };
    let mut attrs = sys::CUmemcpyAttributes {
        srcAccessOrder: sys::CUmemcpySrcAccessOrder::CU_MEMCPY_SRC_ACCESS_ORDER_STREAM,
        srcLocHint: location,
        dstLocHint: location,
        flags: 0,
    };
    let mut first = 0;
    let (dst, src, sizes, n) = (
        destinations.as_mut_ptr(),
        sources.as_mut_ptr(),
        lengths.as_mut_ptr(),
        lengths.len(),
    );
    // SAFETY: equal-length arrays cover all admitted descriptors, one attribute
    // covers every entry, and the flight retains the storage through completion.
    // A reported failing index never proves any part of the batch is quiescent.
    unsafe {
        match call {
            BatchEntry::Current(call) => check(
                "cuMemcpyBatchAsync_v2",
                call(dst, src, sizes, n, &mut attrs, &mut first, 1, stream),
            ),
            BatchEntry::WithFailIndex(call) => {
                let mut failed = usize::MAX;
                check(
                    "cuMemcpyBatchAsync",
                    call(
                        dst,
                        src,
                        sizes,
                        n,
                        &mut attrs,
                        &mut first,
                        1,
                        &mut failed,
                        stream,
                    ),
                )
            }
        }
    }
}

pub(super) fn check(operation: &'static str, code: Status) -> Result<()> {
    if code == SUCCESS {
        Ok(())
    } else {
        Err(Error::Backend {
            operation,
            code: code as c_int,
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/cuda/driver.rs"]
mod tests;
