#![allow(unsafe_code)]

use std::ffi::{c_char, c_int, c_uint, c_void};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::{Error, Result};

pub(super) type Context = *mut c_void;
pub(super) type Status = c_int;

pub(super) struct Library(NonNull<c_void>);
// SAFETY: dlopen handles and CUDA entry points support concurrent calls. Arc
// retains the handle through every context/allocation and failed cleanup.
unsafe impl Send for Library {}
unsafe impl Sync for Library {}

#[link(name = "dl")]
unsafe extern "C" {
    fn dlopen(name: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
}
impl Drop for Library {
    fn drop(&mut self) {
        // SAFETY: this is the last library owner; no retained function can run.
        unsafe {
            dlclose(self.0.as_ptr());
        }
    }
}

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
    pub pointer_attribute: unsafe extern "C" fn(*mut c_void, c_int, u64) -> Status,
    pub address_range: unsafe extern "C" fn(*mut u64, *mut usize, u64) -> Status,
}

impl Driver {
    pub(super) fn load() -> Result<Arc<Self>> {
        // SAFETY: NUL-terminated system SONAME, RTLD_NOW | RTLD_LOCAL.
        let handle = unsafe { dlopen(c"libcuda.so.1".as_ptr(), 2) };
        let library =
            Library(NonNull::new(handle).ok_or(Error::Unavailable("CUDA driver libcuda.so.1"))?);
        macro_rules! symbol {
            ($name:literal, $ty:ty) => {{
                // SAFETY: function signatures and versioned symbol names are
                // the CUDA Driver API ABI, not the CUDA runtime API.
                let address = unsafe { dlsym(library.0.as_ptr(), $name.as_ptr().cast()) };
                if address.is_null() {
                    return Err(Error::Unavailable(concat!("CUDA driver symbol ", $name)));
                }
                unsafe { std::mem::transmute::<*mut c_void, $ty>(address) }
            }};
        }
        let driver = Self {
            init: symbol!("cuInit\0", unsafe extern "C" fn(c_uint) -> Status),
            device_get: symbol!(
                "cuDeviceGet\0",
                unsafe extern "C" fn(*mut c_int, c_int) -> Status
            ),
            primary_retain: symbol!(
                "cuDevicePrimaryCtxRetain\0",
                unsafe extern "C" fn(*mut Context, c_int) -> Status
            ),
            primary_release: symbol!(
                "cuDevicePrimaryCtxRelease_v2\0",
                unsafe extern "C" fn(c_int) -> Status
            ),
            push: symbol!(
                "cuCtxPushCurrent_v2\0",
                unsafe extern "C" fn(Context) -> Status
            ),
            pop: symbol!(
                "cuCtxPopCurrent_v2\0",
                unsafe extern "C" fn(*mut Context) -> Status
            ),
            synchronize: symbol!("cuCtxSynchronize\0", unsafe extern "C" fn() -> Status),
            allocate: symbol!(
                "cuMemAlloc_v2\0",
                unsafe extern "C" fn(*mut u64, usize) -> Status
            ),
            free: symbol!("cuMemFree_v2\0", unsafe extern "C" fn(u64) -> Status),
            memset: symbol!(
                "cuMemsetD8_v2\0",
                unsafe extern "C" fn(u64, u8, usize) -> Status
            ),
            to_host: symbol!(
                "cuMemcpyDtoH_v2\0",
                unsafe extern "C" fn(*mut c_void, u64, usize) -> Status
            ),
            to_device: symbol!(
                "cuMemcpyHtoD_v2\0",
                unsafe extern "C" fn(u64, *const c_void, usize) -> Status
            ),
            host_allocate: symbol!(
                "cuMemHostAlloc\0",
                unsafe extern "C" fn(*mut *mut c_void, usize, c_uint) -> Status
            ),
            host_free: symbol!(
                "cuMemFreeHost\0",
                unsafe extern "C" fn(*mut c_void) -> Status
            ),
            pointer_attribute: symbol!(
                "cuPointerGetAttribute\0",
                unsafe extern "C" fn(*mut c_void, c_int, u64) -> Status
            ),
            address_range: symbol!(
                "cuMemGetAddressRange_v2\0",
                unsafe extern "C" fn(*mut u64, *mut usize, u64) -> Status
            ),
            _library: Some(library),
        };
        // SAFETY: CUDA driver initialization is thread safe and uses flags zero.
        check("cuInit", unsafe { (driver.init)(0) })?;
        Ok(Arc::new(driver))
    }
}

pub(super) fn check(operation: &'static str, code: Status) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::Backend { operation, code })
    }
}
