//! Runtime symbols and a replaceable, fallible table for inline provider calls.
#![allow(unsafe_code)]
use crate::{Error, Result};
use std::{
    ffi::{CStr, CString, c_void},
    sync::Arc,
};

#[allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code
)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/fabric.rs"));
}
pub(super) use ffi::{
    orbite_fi_completion as Completion, orbite_fi_handles as Handles, orbite_fi_info as Info,
};
// These bridge records contain only pointers, integers and fixed byte arrays.
macro_rules! zero_default {
    ($($ty:ty),*) => {$ (
        impl Default for $ty {
            fn default() -> Self {
                // SAFETY: all-zero is valid for every field of these C records;
                // native outputs are checked before any handle is consumed.
                unsafe { std::mem::zeroed() }
            }
        }
    )*};
}
zero_default!(Handles, Info, Completion);
type Register = unsafe extern "C" fn(
    *const Handles,
    *mut c_void,
    usize,
    i32,
    u64,
    i32,
    *mut *mut c_void,
    *mut *mut c_void,
    *mut u64,
) -> i32;
type Post = unsafe extern "C" fn(
    *const Handles,
    i32,
    *mut c_void,
    usize,
    *mut c_void,
    u64,
    u64,
    u64,
    *mut c_void,
) -> i32;
#[derive(Clone, Copy)]
pub(super) struct Calls {
    pub open: unsafe extern "C" fn(
        *const Symbols,
        *mut c_void,
        usize,
        usize,
        i32,
        *mut Handles,
        *mut u8,
        *mut usize,
    ) -> i32,
    pub close: unsafe extern "C" fn(*mut c_void) -> i32,
    pub register: Register,
    pub insert: unsafe extern "C" fn(*const Handles, *const u8, *mut u64) -> i32,
    pub remove: unsafe extern "C" fn(*const Handles, u64) -> i32,
    pub post: Post,
    pub poll: unsafe extern "C" fn(*const Handles, *mut Completion) -> i32,
}
pub(super) type Symbols = ffi::orbite_fi_api;
pub(super) const REAL: Calls = Calls {
    open: ffi::orbite_fi_open,
    close: ffi::orbite_fi_close,
    register: ffi::orbite_fi_register,
    insert: ffi::orbite_fi_insert,
    remove: ffi::orbite_fi_remove,
    post: ffi::orbite_fi_post,
    poll: ffi::orbite_fi_poll,
};
pub(super) struct Api {
    _library: libloading::Library,
    symbols: ffi::orbite_fi_api,
    pub calls: Calls,
}
// SAFETY: symbols and the library are immutable. Providers are required to
// advertise FI_THREAD_SAFE; each context serializes endpoint/CQ mutations.
unsafe impl Send for Api {}
// SAFETY: shared table entries are immutable and retain their library.
unsafe impl Sync for Api {}
// SAFETY: native handles move only into their domain owner, behind its mutex.
unsafe impl Send for Handles {}
// SAFETY: Info contains scalars and fixed character arrays, no native pointers.
unsafe impl Send for Info {}

impl Api {
    pub(super) fn load() -> Result<Arc<Self>> {
        Self::load_from("libfabric.so.1")
    }
    pub(super) fn load_from(path: &str) -> Result<Arc<Self>> {
        // SAFETY: only an explicitly configured fabric transport loads the SDK.
        // Every live handle retains this library through Api.
        let library = unsafe { libloading::Library::new(path) }
            .map_err(|_| Error::Unavailable("libfabric.so.1 is unavailable"))?;
        let symbol = |name: &[u8]| -> Result<*mut c_void> {
            // SAFETY: the C bridge casts each named exported symbol to its
            // libfabric header prototype; symbols never outlive library.
            unsafe { library.get::<*mut c_void>(name) }
                .map(|s| *s)
                .map_err(|_| Error::Unavailable("required libfabric symbol is unavailable"))
        };
        let symbols = ffi::orbite_fi_api {
            getinfo: symbol(b"fi_getinfo\0")?,
            freeinfo: symbol(b"fi_freeinfo\0")?,
            dupinfo: symbol(b"fi_dupinfo\0")?,
            fabric: symbol(b"fi_fabric\0")?,
            version: symbol(b"fi_version\0")?,
        };
        #[cfg(test)]
        let calls = super::tests::calls(REAL);
        #[cfg(not(test))]
        let calls = REAL;
        Ok(Arc::new(Self {
            _library: library,
            symbols,
            calls,
        }))
    }

    pub(super) fn discover(self: &Arc<Self>, config: &super::FabricConfig) -> Result<InfoList> {
        let name = if !config.tcp() && config.fabric_name.is_empty() {
            "efa"
        } else {
            &config.fabric_name
        };
        let name =
            CString::new(name).map_err(|_| Error::InvalidConfig("fabric name contains NUL"))?;
        let mut list = InfoList {
            api: self.clone(),
            head: std::ptr::null_mut(),
        };
        // SAFETY: the checked table and string live through the call. list owns
        // every returned fi_info even if discovery reports an error.
        let r = unsafe {
            ffi::orbite_fi_discover(
                &self.symbols,
                i32::from(config.tcp()),
                name.as_ptr(),
                &mut list.head,
            )
        };
        check("fi_getinfo", r)?;
        if list.head.is_null() {
            return Err(Error::Unavailable(
                "libfabric provider has no eligible domain",
            ));
        }
        Ok(list)
    }
    pub(super) fn open(
        &self,
        info: *mut c_void,
        cq: usize,
        peers: usize,
        tcp: bool,
        h: &mut Handles,
        address: &mut [u8],
    ) -> Result<usize> {
        let mut len = address.len();
        // SAFETY: info is borrowed from a live InfoList. All partial outputs
        // belong to h; the Rust context closes or quarantines them on failure.
        let r = unsafe {
            (self.calls.open)(
                &self.symbols,
                info,
                cq,
                peers,
                i32::from(tcp),
                h,
                address.as_mut_ptr(),
                &mut len,
            )
        };
        check("fi_endpoint setup", r)?;
        if len == 0 || len > address.len() {
            return Err(Error::Protocol("libfabric endpoint address size"));
        }
        Ok(len)
    }
}
pub(super) struct InfoList {
    api: Arc<Api>,
    head: *mut c_void,
}
impl InfoList {
    pub(super) fn entries(&self) -> Result<Vec<(*mut c_void, Info)>> {
        let mut entries = Vec::new();
        let mut next = self.head;
        let mut seen = 0;
        while !next.is_null() {
            seen += 1;
            if seen > 4096 {
                return Err(Error::ResourceExhausted);
            }
            let mut info = Info::default();
            // SAFETY: next is in this retained fi_getinfo chain. describe checks
            // its required attributes and copies only into fixed output fields.
            let r = unsafe { ffi::orbite_fi_describe(next, &mut info) };
            if r == 0 {
                entries.push((next, info));
            }
            // SAFETY: next remains live until InfoList drops.
            next = unsafe { ffi::orbite_fi_next(next) };
        }
        Ok(entries)
    }
}
impl Drop for InfoList {
    fn drop(&mut self) {
        if !self.head.is_null() {
            // SAFETY: this list uniquely owns the allocation from fi_getinfo.
            unsafe { ffi::orbite_fi_freeinfo(&self.api.symbols, self.head) };
        }
    }
}
#[allow(clippy::unnecessary_cast)] // c_char is signed on x86_64, unsigned on aarch64.
pub(super) fn string(v: &[std::ffi::c_char]) -> Result<String> {
    let bytes: Vec<_> = v.iter().map(|&c| c as u8).collect();
    let c = CStr::from_bytes_until_nul(&bytes)
        .map_err(|_| Error::Protocol("unterminated fabric attribute"))?;
    c.to_str()
        .map(str::to_owned)
        .map_err(|_| Error::Protocol("non-UTF8 fabric attribute"))
}
pub(super) fn check(operation: &'static str, code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::Backend { operation, code })
    }
}
pub(super) fn uncertain(operation: &'static str, code: i32) -> Error {
    Error::Unquiesced { operation, code }
}
