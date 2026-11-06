//! Per-engine native plugins. Calls are serialized per plugin; allocations keep
//! both the context and dlopen reference alive through the core's drain receipt.
mod abi;
#[cfg(feature = "cuda")]
mod cuda;
use abi::{DESCRIPTOR, Descriptor, HANDLED_ERROR, Location, Table};
use libloading::Library;
use orbite::{Error, Result};
use std::{
    ffi::{CString, c_void},
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

fn unavailable() -> Error {
    Error::Unavailable("device plugin ABI, symbol or function table unavailable")
}
fn uncertain(operation: &'static str, code: i32) -> Error {
    Error::Unquiesced { operation, code }
}
struct State {
    context: usize,
    allocations: usize,
    failed: bool,
    closed: bool,
}
pub(crate) struct Plugin {
    library: ManuallyDrop<Library>,
    table: Table,
    pub class: String,
    state: Mutex<State>,
}
// SAFETY: all plugin calls use the per-context mutex. The checked ABI requires
// callbacks to support caller threads; the library outlives every callback.
unsafe impl Send for Plugin {}
// SAFETY: raw table/context access is serialized; the strings and symbols belong
// to the immutable library retained until successful context destruction.
unsafe impl Sync for Plugin {}
impl Plugin {
    fn load(path: &Path) -> Result<Arc<Self>> {
        // SAFETY: explicitly configured, trusted native library. Loading is never
        // driven by peer metadata; the library owns any initializer it executes.
        let library = unsafe { Library::new(path) }.map_err(|_| unavailable())?;
        let descriptor =
            // SAFETY: the fixed descriptor is checked before invoking a
            // potentially incompatible function that writes the table.
            unsafe { library.get::<*const Descriptor>(b"orbite_device_plugin_abi_v1\0") }
                .map_err(|_| unavailable())?;
        if descriptor.is_null() {
            return Err(unavailable());
        }
        // SAFETY: the opt-in plugin exports this fixed, immutable descriptor.
        if unsafe { **descriptor } != DESCRIPTOR {
            return Err(unavailable());
        }
        // SAFETY: exact frozen C signature, with a live dlopen owner.
        let register = unsafe {
            library.get::<unsafe extern "C" fn(*mut Table) -> i32>(b"tent_register_device_plugin\0")
        }
        .map_err(|_| unavailable())?;
        let mut table = Table::default();
        // SAFETY: one initialized table with the verified size/alignment.
        if unsafe { register(&mut table) } != 0 || !table.complete() {
            return Err(unavailable());
        }
        let class = crate::boundary::text(table.class_name, 32).map_err(|_| unavailable())?;
        if !matches!(class.as_str(), "cpu" | "cuda") {
            return Err(Error::Unavailable(
                "device plugin platform excluded; only CPU and CUDA are supported",
            ));
        }
        #[cfg(not(feature = "cuda"))]
        if class == "cuda" {
            return Err(Error::Unavailable(
                "CUDA device plugin requires the cuda feature",
            ));
        }
        // SAFETY: all slots validated before the first context/allocation call.
        let context = unsafe { table.create_plugin.ok_or_else(unavailable)?() };
        if context.is_null() {
            return Err(unavailable());
        }
        Ok(Arc::new(Self {
            library: ManuallyDrop::new(library),
            table,
            class,
            state: Mutex::new(State {
                context: context as usize,
                allocations: 0,
                failed: false,
                closed: false,
            }),
        }))
    }
    pub fn close(&self) -> Result<()> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.failed {
            return Err(uncertain("device plugin prior cleanup", -1));
        }
        if s.closed {
            return Ok(());
        }
        if s.allocations != 0 {
            return Err(Error::BatchBusy);
        }
        s.failed = true; // Fence before a callback; never repeat a failed destroy.
        // SAFETY: context is live, uniquely serialized and has no allocations.
        let rc =
            unsafe { self.table.destroy_plugin.ok_or_else(unavailable)?(s.context as *mut c_void) };
        if rc != 0 {
            return Err(uncertain("device plugin destroy", rc));
        }
        s.closed = true;
        s.failed = false;
        Ok(())
    }
    fn validate_devices(&self) -> Result<()> {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: serialized live context and complete checked table.
        let count = unsafe {
            self.table.get_device_count.ok_or_else(unavailable)?(s.context as *mut c_void)
        };
        if !(0..=64).contains(&count) || (self.class == "cuda" && count == 0) {
            return Err(unavailable());
        }
        for device in 0..count {
            let mut bus = [0 as std::ffi::c_char; 32];
            // SAFETY: initialized bounded output and a validated device index.
            let rc = unsafe {
                self.table.get_device_pci_bus_id.ok_or_else(unavailable)?(
                    s.context as *mut c_void,
                    device,
                    bus.as_mut_ptr(),
                    bus.len(),
                )
            };
            if rc != 0 || crate::boundary::text(bus.as_ptr(), bus.len()).is_err() {
                return Err(unavailable());
            }
        }
        Ok(())
    }
    pub fn allocate(
        self: &Arc<Self>,
        size: usize,
        location: &str,
    ) -> (Arc<Allocation>, Result<()>) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut address = std::ptr::null_mut();
        let mut result = Err(unavailable());
        let mut charged = false;
        if !s.failed && !s.closed && s.allocations < 65536 {
            if let (Some(allocate), Ok(location)) = (self.table.alloc, CString::new(location)) {
                s.allocations += 1;
                charged = true;
                // SAFETY: checked table, bounded caller charge and writable output.
                let rc = unsafe {
                    allocate(
                        s.context as *mut c_void,
                        &mut address,
                        size,
                        location.as_ptr(),
                    )
                };
                if rc == 0 && !address.is_null() && (address as usize).checked_add(size).is_some() {
                    result = Ok(());
                } else {
                    s.failed = true;
                    result = Err(uncertain("device plugin allocation", rc));
                }
            }
        }
        (
            Arc::new(Allocation {
                plugin: self.clone(),
                address: address as usize,
                size,
                state: Mutex::new(if result.is_ok() {
                    AllocationState::Live
                } else if charged {
                    AllocationState::Failed
                } else {
                    AllocationState::Freed
                }),
            }),
            result,
        )
    }
    fn locate(&self, address: usize, size: usize) -> Result<String> {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.failed || s.closed {
            return Err(uncertain("device plugin location after cleanup", -1));
        }
        let mut out = Location {
            start: std::ptr::null_mut(),
            length: 0,
            location: [0; 32],
        };
        // SAFETY: allocation retained by the caller, one initialized output record.
        let rc = unsafe {
            self.table.query_location.ok_or_else(unavailable)?(
                s.context as *mut c_void,
                address as *mut c_void,
                size,
                &mut out,
                1,
            )
        };
        if rc != 1 || out.start as usize != address || out.length != size {
            return Err(unavailable());
        }
        let location =
            crate::boundary::text(out.location.as_ptr(), 32).map_err(|_| unavailable())?;
        if location.split(':').next() != Some(self.class.as_str()) {
            return Err(unavailable());
        }
        Ok(location)
    }
    pub fn copy(&self, dst: usize, src: usize, size: usize) -> Result<bool> {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.failed || s.closed {
            return Err(uncertain("device plugin copy after cleanup", -1));
        }
        // SAFETY: the C memory registry holds both exclusive backing owners for
        // this entire synchronous call; arbitrary public raw pointers are refused.
        let rc = unsafe {
            self.table.memcpy_sync.ok_or_else(unavailable)?(
                s.context as *mut c_void,
                dst as *mut c_void,
                src as *mut c_void,
                size,
            )
        };
        if rc == HANDLED_ERROR {
            s.failed = true;
            return Err(uncertain("device plugin handled copy", rc));
        }
        Ok(rc == 0)
    }
}
impl Drop for Plugin {
    fn drop(&mut self) {
        if self.close().is_ok() {
            // SAFETY: destruction proved all callbacks/allocations are finished.
            unsafe { ManuallyDrop::drop(&mut self.library) };
        } else {
            log::error!("retaining unquiesced device plugin library and context");
        }
    }
}
#[derive(PartialEq, Eq)]
enum AllocationState {
    Live,
    Failed,
    Freed,
}
pub(crate) struct Allocation {
    pub plugin: Arc<Plugin>,
    pub address: usize,
    pub size: usize,
    state: Mutex<AllocationState>,
}
impl Allocation {
    pub fn location(&self) -> Result<String> {
        self.plugin.locate(self.address, self.size)
    }
    pub fn close(self: &Arc<Self>) -> Result<()> {
        if Arc::strong_count(self) != 1 {
            *self.state.lock().unwrap_or_else(|p| p.into_inner()) = AllocationState::Failed;
            return Err(uncertain("device plugin retained backing", -1));
        }
        self.retire()
    }
    fn retire(&self) -> Result<()> {
        let mut a = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if *a == AllocationState::Freed {
            return Ok(());
        }
        if *a == AllocationState::Failed {
            return Err(uncertain("device plugin prior free", -1));
        }
        *a = AllocationState::Failed;
        let mut s = self.plugin.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.failed || s.closed {
            return Err(uncertain("device plugin fenced allocation", -1));
        }
        // SAFETY: called only after all registered/native loans and backing
        // owners are released; exactly one callback for this allocation.
        let rc = unsafe {
            self.plugin.table.free.ok_or_else(unavailable)?(
                s.context as *mut c_void,
                self.address as *mut c_void,
                self.size,
            )
        };
        if rc != 0 {
            s.failed = true;
            return Err(uncertain("device plugin free", rc));
        }
        *a = AllocationState::Freed;
        s.allocations -= 1;
        Ok(())
    }
}
impl Drop for Allocation {
    fn drop(&mut self) {
        if self.retire().is_err() {
            // Keep the context and its dynamic code alive even if a caller
            // abandons a failed construction before it reaches the engine table.
            std::mem::forget(self.plugin.clone());
        }
    }
}
fn paths(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_owned()]);
    }
    let entries = std::fs::read_dir(path).map_err(|_| unavailable())?;
    let mut out = Vec::new();
    for (index, entry) in entries.enumerate() {
        if index == 1024 {
            return Err(Error::ResourceExhausted);
        }
        let entry = entry.map_err(|_| unavailable())?;
        if entry.path().extension().is_some_and(|s| s == "so")
            && entry.file_type().is_ok_and(|t| t.is_file())
        {
            if out.len() == 8 {
                return Err(Error::ResourceExhausted);
            }
            out.push(entry.path());
        }
    }
    if out.is_empty() {
        return Err(unavailable());
    }
    out.sort();
    Ok(out)
}
pub(crate) fn load(out: &mut Vec<Arc<Plugin>>, path: &Path) -> Result<()> {
    if !out.is_empty() {
        return Err(Error::InvalidConfig("device plugins already configured"));
    }
    let result = (|| {
        for path in paths(path)? {
            let plugin = Plugin::load(&path)?;
            out.push(plugin);
            let plugin = out.last().ok_or_else(unavailable)?;
            plugin.validate_devices()?;
            if out.iter().filter(|p| p.class == plugin.class).count() != 1 {
                return Err(Error::InvalidConfig("duplicate device plugin class"));
            }
        }
        Ok(())
    })();
    if let Err(e) = result {
        let mut error = e;
        for p in out.iter() {
            if let Err(e) = p.close() {
                error = e;
            }
        }
        if !matches!(error, Error::Unquiesced { .. }) {
            out.clear();
        }
        return Err(error);
    }
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn orbite_load_device_plugins(
    engine: crate::abi::Handle,
    path: *const std::ffi::c_char,
) -> i32 {
    crate::boundary::call(|| {
        let path = crate::boundary::text(path, 4096)?;
        crate::engine::with(engine, |e| {
            crate::boundary::lock(&e.memory)?.load_plugins(Path::new(&path))
        })
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn orbite_close_device_plugins(engine: crate::abi::Handle) -> i32 {
    crate::boundary::call(|| {
        crate::engine::with(engine, |e| {
            crate::boundary::lock(&e.memory)?.close_plugins()
        })
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn orbite_plugin_memcpy_sync(
    engine: crate::abi::Handle,
    dst: *mut c_void,
    src: *mut c_void,
    size: usize,
) -> i32 {
    crate::boundary::call(|| {
        crate::engine::with(engine, |e| {
            crate::boundary::lock(&e.memory)?.plugin_copy(dst, src, size)
        })
    })
}
#[cfg(test)]
#[path = "../../tests/unit/device_plugin.rs"]
mod tests;
