//! Linux libcufile.so.0 batch ABI. Integer status fields accept unknown native
//! values without constructing a Rust enum. Only the owning batch publishes
//! the stable parameters and holds their referenced allocations until drain.
use std::ffi::{c_int, c_uint, c_void};
use std::sync::Arc;

use crate::{Error, Result};
use libloading::Library;

#[repr(C)]
#[derive(Clone, Copy)]
struct Status {
    err: c_int,
    cuda: c_int,
}
impl Status {
    fn check(self, operation: &'static str) -> Result<()> {
        if self.err == 0 {
            Ok(())
        } else {
            Err(Error::Backend {
                operation,
                code: self.err,
            })
        }
    }
}

#[repr(C)]
struct Descriptor {
    kind: c_int,
    handle: DescriptorHandle,
    fs_ops: *const c_void,
}
#[repr(C)]
union DescriptorHandle {
    fd: c_int,
    opaque: *mut c_void,
}

#[repr(C)]
#[derive(Default)]
struct Properties {
    major: c_uint,
    minor: c_uint,
    poll_kb: usize,
    direct_kb: usize,
    status_flags: c_uint,
    control_flags: c_uint,
    features: c_uint,
    cache_kb: c_uint,
    per_buffer_kb: c_uint,
    pinned_kb: c_uint,
    max_batch: c_uint,
    timeout_ms: c_uint,
}

// CUfileIOParams_t contains a one-member union for batch mode. Pointer-sized
// fields carry exposed Linux addresses/opaque handles; cuFile does not own them.
#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct Param {
    pub mode: c_uint,
    pub base: usize,
    pub file_offset: libc::off_t,
    pub buffer_offset: libc::off_t,
    pub size: usize,
    pub handle: usize,
    pub opcode: c_uint,
    pub cookie: usize,
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct Event {
    pub cookie: usize,
    pub status: c_uint,
    pub bytes: usize,
}

pub(super) const WAITING: u32 = 1;
pub(super) const PENDING: u32 = 2;
pub(super) const INVALID: u32 = 4;
pub(super) const CANCELED: u32 = 8;
pub(super) const COMPLETE: u32 = 16;
pub(super) const TIMEOUT: u32 = 32;
pub(super) const FAILED: u32 = 64;

pub(super) struct Profile {
    pub mode: super::GdsMode,
    pub max_native_slice: usize,
}

pub(super) trait Api: Send + Sync {
    fn pid(&self) -> u32 {
        std::process::id()
    }
    fn open(&self) -> Result<()>;
    fn profile(&self) -> Result<Profile>;
    fn register_file(&self, fd: c_int) -> Result<usize>;
    fn deregister_file(&self, handle: usize) -> Result<()>;
    fn register_buffer(&self, base: usize, length: usize) -> Result<()>;
    fn deregister_buffer(&self, base: usize) -> Result<()>;
    fn setup(&self, depth: u32) -> Result<usize>;
    fn submit(&self, handle: usize, params: &mut [Param]) -> Result<()>;
    fn poll(&self, handle: usize, events: &mut [Event]) -> Result<usize>;
    fn cancel(&self, handle: usize) -> Result<()>;
    fn destroy(&self, handle: usize) -> Result<()>;
}

struct Native {
    open: unsafe extern "C" fn() -> Status,
    properties: unsafe extern "C" fn(*mut Properties) -> Status,
    boolean: unsafe extern "C" fn(c_int, *mut u8) -> Status,
    register_file: unsafe extern "C" fn(*mut *mut c_void, *mut Descriptor) -> Status,
    deregister_file: unsafe extern "C" fn(*mut c_void),
    register_buffer: unsafe extern "C" fn(*const c_void, usize, c_int) -> Status,
    deregister_buffer: unsafe extern "C" fn(*const c_void) -> Status,
    setup: unsafe extern "C" fn(*mut *mut c_void, c_uint) -> Status,
    submit: unsafe extern "C" fn(*mut c_void, c_uint, *mut Param, c_uint) -> Status,
    poll: unsafe extern "C" fn(
        *mut c_void,
        c_uint,
        *mut c_uint,
        *mut Event,
        *mut libc::timespec,
    ) -> Status,
    cancel: unsafe extern "C" fn(*mut c_void) -> Status,
    destroy: unsafe extern "C" fn(*mut c_void),
    _library: Library,
}

#[allow(unsafe_code)]
pub(super) fn load() -> Result<Arc<dyn Api>> {
    // SAFETY: only the installed Linux cuFile SONAME is loaded, and the table owns the DSO.
    let lib = unsafe { Library::new("libcufile.so.0") }
        .map_err(|_| Error::Unavailable("libcufile.so.0"))?;
    macro_rules! symbol {
        ($name:literal) => {{
            // SAFETY: each field has this symbol's C signature from NVIDIA cufile.h.
            unsafe {
                *lib.get(concat!($name, "\0").as_bytes())
                    .map_err(|_| Error::Unavailable($name))?
            }
        }};
    }
    Ok(Arc::new(Native {
        open: symbol!("cuFileDriverOpen"),
        properties: symbol!("cuFileDriverGetProperties"),
        boolean: symbol!("cuFileGetParameterBool"),
        register_file: symbol!("cuFileHandleRegister"),
        deregister_file: symbol!("cuFileHandleDeregister"),
        register_buffer: symbol!("cuFileBufRegister"),
        deregister_buffer: symbol!("cuFileBufDeregister"),
        setup: symbol!("cuFileBatchIOSetUp"),
        submit: symbol!("cuFileBatchIOSubmit"),
        poll: symbol!("cuFileBatchIOGetStatus"),
        cancel: symbol!("cuFileBatchIOCancel"),
        destroy: symbol!("cuFileBatchIODestroy"),
        _library: lib,
    }))
}

#[allow(unsafe_code)]
impl Api for Native {
    fn open(&self) -> Result<()> {
        // SAFETY: the process-local initializer serializes driver open and owns the DSO.
        unsafe { (self.open)() }.check("cuFileDriverOpen")
    }
    fn profile(&self) -> Result<Profile> {
        let mut props = Properties::default();
        let (mut allow, mut force) = (0, 0);
        // SAFETY: these outputs have the cuFile property and C bool layouts. Query the
        // effective settings after open, so environment/config/API overrides are included.
        unsafe {
            (self.properties)(&mut props).check("cuFileDriverGetProperties")?;
            (self.boolean)(1, &mut allow).check("cuFileGetParameterBool allow compat")?;
            (self.boolean)(2, &mut force).check("cuFileGetParameterBool force compat")?;
        }
        let mode = if force != 0 {
            super::GdsMode::CompatForced
        } else if allow != 0 || props.control_flags & (1 << 1) != 0 {
            super::GdsMode::CompatAllowed
        } else {
            super::GdsMode::NativeOnly
        };
        Ok(Profile {
            mode,
            max_native_slice: (props.per_buffer_kb as usize).saturating_mul(1024),
        })
    }
    fn register_file(&self, fd: c_int) -> Result<usize> {
        let mut out = std::ptr::null_mut();
        let mut desc = Descriptor {
            kind: 1,
            handle: DescriptorHandle { fd },
            fs_ops: std::ptr::null(),
        };
        // SAFETY: fd is the entry's retained O_DIRECT descriptor; out/desc live through registration.
        unsafe { (self.register_file)(&mut out, &mut desc) }.check("cuFileHandleRegister")?;
        if out.is_null() {
            return Err(Error::Unquiesced {
                operation: "null cuFile handle",
                code: -1,
            });
        }
        Ok(out as usize)
    }
    fn deregister_file(&self, handle: usize) -> Result<()> {
        // SAFETY: all file leases drained and this owner calls the void API exactly once.
        unsafe { (self.deregister_file)(handle as *mut _) };
        Ok(())
    }
    fn register_buffer(&self, base: usize, length: usize) -> Result<()> {
        // SAFETY: the held CUDA registration covers this entire range in its current context.
        unsafe { (self.register_buffer)(base as *const _, length, 0) }.check("cuFileBufRegister")
    }
    fn deregister_buffer(&self, base: usize) -> Result<()> {
        // SAFETY: the final borrower retired and the registration is held in its CUDA context.
        unsafe { (self.deregister_buffer)(base as *const _) }.check("cuFileBufDeregister")
    }
    fn setup(&self, depth: u32) -> Result<usize> {
        let mut handle = std::ptr::null_mut();
        // SAFETY: the output belongs to this sub-batch and depth was validated.
        unsafe { (self.setup)(&mut handle, depth) }.check("cuFileBatchIOSetUp")?;
        if handle.is_null() {
            return Err(Error::Unquiesced {
                operation: "null cuFile batch",
                code: -1,
            });
        }
        Ok(handle as usize)
    }
    fn submit(&self, handle: usize, params: &mut [Param]) -> Result<()> {
        // SAFETY: the owning batch retains this fixed array, file handles and exclusive
        // memory loans through terminal receipts, including errors from this call.
        unsafe {
            (self.submit)(
                handle as *mut _,
                params.len() as u32,
                params.as_mut_ptr(),
                0,
            )
        }
        .check("cuFileBatchIOSubmit")
    }
    fn poll(&self, handle: usize, events: &mut [Event]) -> Result<usize> {
        let mut count = events.len() as u32;
        // SAFETY: the live batch owns the event output array. min_nr=0 is a nonblocking poll.
        unsafe {
            (self.poll)(
                handle as *mut _,
                0,
                &mut count,
                events.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        }
        .check("cuFileBatchIOGetStatus")?;
        Ok(count as usize)
    }
    fn cancel(&self, handle: usize) -> Result<()> {
        // SAFETY: this still-owned handle may have pending IO; cancellation is not a receipt.
        unsafe { (self.cancel)(handle as *mut _) }.check("cuFileBatchIOCancel")
    }
    fn destroy(&self, handle: usize) -> Result<()> {
        // SAFETY: every IO is terminal; the owner calls this void API at most once.
        unsafe { (self.destroy)(handle as *mut _) };
        Ok(())
    }
}
