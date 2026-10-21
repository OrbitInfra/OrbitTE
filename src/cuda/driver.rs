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

pub(super) struct Driver {
    pub(super) _library: Option<Library>,
    pub init: unsafe extern "C" fn(c_uint) -> Status,
    pub device_get: unsafe extern "C" fn(*mut c_int, c_int) -> Status,
    pub primary_retain: unsafe extern "C" fn(*mut Context, c_int) -> Status,
    pub primary_release: unsafe extern "C" fn(c_int) -> Status,
    pub push: unsafe extern "C" fn(Context) -> Status,
    pub pop: unsafe extern "C" fn(*mut Context) -> Status,
    pub synchronize: unsafe extern "C" fn() -> Status,
    pub allocate: unsafe extern "C" fn(*mut u64, usize) -> Status,
    pub free: unsafe extern "C" fn(u64) -> Status,
    pub memset: unsafe extern "C" fn(u64, u8, usize) -> Status,
    pub to_host: unsafe extern "C" fn(*mut c_void, u64, usize) -> Status,
    pub to_device: unsafe extern "C" fn(u64, *const c_void, usize) -> Status,
    pub host_allocate: unsafe extern "C" fn(*mut *mut c_void, usize, c_uint) -> Status,
    pub host_free: unsafe extern "C" fn(*mut c_void) -> Status,
    pub pointer_attribute: unsafe extern "C" fn(*mut c_void, PointerAttribute, u64) -> Status,
    pub address_range: unsafe extern "C" fn(*mut u64, *mut usize, u64) -> Status,
}

impl Driver {
    pub(super) fn load() -> Result<Arc<Self>> {
        // SAFETY: load only the system Driver API. The library is retained by
        // every allocation/context, including resources whose cleanup failed.
        let library = unsafe { Library::new("libcuda.so.1") }
            .map_err(|_| Error::Unavailable("CUDA driver libcuda.so.1"))?;
        Self::from_library(library)
    }

    fn from_library(library: Library) -> Result<Arc<Self>> {
        macro_rules! symbol {
            ($field:ident) => {
                symbol(
                    &library,
                    concat!(stringify!($field), "\0").as_bytes(),
                    concat!("CUDA driver symbol ", stringify!($field)),
                    |api: &sys::Lib| api.$field,
                )?
            };
        }
        let driver = Self {
            init: symbol!(cuInit),
            device_get: symbol!(cuDeviceGet),
            primary_retain: symbol!(cuDevicePrimaryCtxRetain),
            primary_release: symbol!(cuDevicePrimaryCtxRelease_v2),
            push: symbol!(cuCtxPushCurrent_v2),
            pop: symbol!(cuCtxPopCurrent_v2),
            synchronize: symbol!(cuCtxSynchronize),
            allocate: symbol!(cuMemAlloc_v2),
            free: symbol!(cuMemFree_v2),
            memset: symbol!(cuMemsetD8_v2),
            to_host: symbol!(cuMemcpyDtoH_v2),
            to_device: symbol!(cuMemcpyHtoD_v2),
            host_allocate: symbol!(cuMemHostAlloc),
            host_free: symbol!(cuMemFreeHost),
            pointer_attribute: symbol!(cuPointerGetAttribute),
            address_range: symbol!(cuMemGetAddressRange_v2),
            _library: Some(library),
        };
        // SAFETY: CUDA driver initialization is thread safe and uses flags zero.
        check("cuInit", unsafe { (driver.init)(0) })?;
        Ok(Arc::new(driver))
    }
}

// cudarc's generated Lib supplies each function's exact type without invoking
// its eager loader: missing unrelated CUDA symbols must not panic this backend.
fn symbol<F: Copy>(
    library: &Library,
    name: &[u8],
    operation: &'static str,
    _signature: impl FnOnce(&sys::Lib) -> F,
) -> Result<F> {
    // SAFETY: the name and function type come from the same generated field;
    // Driver retains the library for at least as long as its copied entry points.
    unsafe { library.get::<F>(name) }
        .map(|symbol| *symbol)
        .map_err(|_| Error::Unavailable(operation))
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
