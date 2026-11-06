//! CUDA implementation of the frozen plugin table, backed only by OrbiTE's
//! fallible Driver API. The installed libtent is also the opt-in plugin image.
use super::abi::{DESCRIPTOR, Descriptor, HANDLED_ERROR, Location, Table};
use crate::boundary::*;
use orbite::cuda::{CudaMemory, device_plugin};
use std::{
    collections::BTreeMap,
    ffi::{c_char, c_void},
    sync::{Arc, Mutex, OnceLock},
};

struct DeviceAllocation {
    memory: Option<Arc<CudaMemory>>,
    length: usize,
}
#[derive(Default)]
struct Context {
    allocations: BTreeMap<usize, DeviceAllocation>,
    bytes: usize,
    failed: bool,
}
fn contexts() -> &'static Mutex<BTreeMap<usize, Arc<Mutex<Context>>>> {
    static CONTEXTS: OnceLock<Mutex<BTreeMap<usize, Arc<Mutex<Context>>>>> = OnceLock::new();
    CONTEXTS.get_or_init(Mutex::default)
}
fn with<T>(p: *mut c_void, f: impl FnOnce(&mut Context) -> Result<T>) -> Result<T> {
    let owner = lock(contexts())?
        .get(&(p as usize))
        .cloned()
        .ok_or(INVALID)?;
    let mut context = lock(&owner)?;
    f(&mut context)
}
fn result(f: impl FnOnce() -> Result<()>) -> i32 {
    guard(HANDLED_ERROR, || {
        f().map_or_else(|e| if e == UNKNOWN { HANDLED_ERROR } else { -1 }, |()| 0)
    })
}
unsafe extern "C" fn create() -> *mut c_void {
    guard(std::ptr::null_mut(), || {
        let make = || -> Result<_> {
            if device_plugin::device_count()? == 0 {
                return Err(UNSUPPORTED);
            }
            let mut all = lock(contexts())?;
            if all.len() >= 64 {
                return Err(FULL);
            }
            let id = crate::engine::id()?;
            all.insert(id, Arc::new(Mutex::new(Context::default())));
            Ok(id as *mut c_void)
        };
        make().unwrap_or(std::ptr::null_mut())
    })
}
unsafe extern "C" fn destroy(p: *mut c_void) -> i32 {
    result(|| {
        let mut all = lock(contexts())?;
        let owner = all.get(&(p as usize)).ok_or(INVALID)?;
        // The loader serializes callbacks. A direct native caller also cannot
        // destroy a context while an operation is using it.
        if Arc::strong_count(owner) != 1 {
            return Err(FULL);
        }
        {
            let c = lock(owner)?;
            if c.failed {
                return Err(UNKNOWN);
            }
            if !c.allocations.is_empty() {
                return Err(FULL);
            }
        }
        all.remove(&(p as usize));
        Ok(())
    })
}
unsafe extern "C" fn alloc(
    p: *mut c_void,
    out: *mut *mut c_void,
    n: usize,
    location: *const c_char,
) -> i32 {
    result(|| {
        put(out, std::ptr::null_mut())?;
        let loc = text(location, 32)?;
        let id = loc
            .strip_prefix("cuda:")
            .ok_or(UNSUPPORTED)?
            .parse::<i32>()
            .map_err(|_| INVALID)?;
        if id < 0 || n == 0 || n > 1 << 30 {
            return Err(INVALID);
        }
        with(p, |c| {
            if c.failed {
                return Err(UNKNOWN);
            }
            let total = c
                .bytes
                .checked_add(n)
                .filter(|&v| v <= 1 << 30)
                .ok_or(FULL)?;
            if c.allocations.len() >= 1024 {
                return Err(FULL);
            }
            let memory = match CudaMemory::allocate(id, n) {
                Ok(v) => v,
                Err(e) => {
                    if matches!(e, orbite::Error::Unquiesced { .. }) {
                        c.failed = true;
                        c.bytes = total;
                    }
                    return Err(e.into());
                }
            };
            let addr = memory.base_address() as usize;
            c.allocations.insert(
                addr,
                DeviceAllocation {
                    memory: Some(Arc::new(memory)),
                    length: n,
                },
            );
            c.bytes = total;
            put(out, addr as *mut c_void)
        })
    })
}
unsafe extern "C" fn free(p: *mut c_void, ptr: *mut c_void, n: usize) -> i32 {
    result(|| {
        with(p, |c| {
            if c.failed {
                return Err(UNKNOWN);
            }
            let a = c.allocations.get_mut(&(ptr as usize)).ok_or(INVALID)?;
            if a.length != n {
                return Err(INVALID);
            }
            let memory = a.memory.take().ok_or(UNKNOWN)?;
            if let Err(e) = memory.release_owned() {
                c.failed = true;
                return Err(e.into());
            }
            c.allocations.remove(&(ptr as usize));
            c.bytes -= n;
            Ok(())
        })
    })
}
fn range(c: &Context, address: usize, n: usize) -> Result<Option<(&Arc<CudaMemory>, usize)>> {
    if let Some((&base, a)) = c.allocations.range(..=address).next_back() {
        if address < base + a.length {
            if address
                .checked_add(n)
                .is_none_or(|end| end > base + a.length)
            {
                return Err(INVALID);
            }
            return Ok(Some((a.memory.as_ref().ok_or(UNKNOWN)?, address - base)));
        }
    }
    Ok(None)
}
// The raw plugin ABI is unsafe, just like TENT's raw C registration: the caller
// retains external backing throughout metadata queries and until a successful
// destroy if the provider becomes unquiesced. This is never an allocation owner
// used for a transfer; engine submission uses Memory's retained backing instead.
fn external_location(address: usize, n: usize) -> Result<Option<orbite::RangeLocation>> {
    // SAFETY: the native caller keeps this range stable for the metadata-only
    // query. No copy or native loan is published from this temporary probe.
    let memory = unsafe { CudaMemory::from_foreign_address(address as u64, n, Arc::new(())) }?;
    memory
        .map(|m| m.location(0, n).map_err(Code::from))
        .transpose()
}
unsafe extern "C" fn copy(p: *mut c_void, dst: *mut c_void, src: *mut c_void, n: usize) -> i32 {
    guard(HANDLED_ERROR, || {
        let run = || -> Result<bool> {
            with(p, |c| {
                if c.failed {
                    return Err(UNKNOWN);
                }
                if dst.is_null() || src.is_null() || n == 0 {
                    return Err(INVALID);
                }
                let destination = range(c, dst as usize, n)?;
                let source = range(c, src as usize, n)?;
                if (destination.is_none() && external_location(dst as usize, n)?.is_some())
                    || (source.is_none() && external_location(src as usize, n)?.is_some())
                {
                    // No retained provider allocation exists for this device range.
                    // This is terminal to the frozen loader, without posting a copy.
                    return Err(UNSUPPORTED);
                }
                let (memory, offset, other, to_device) = match (destination, source) {
                    (Some((m, o)), _) => (m, o, src as usize as u64, true),
                    (_, Some((m, o))) => (m, o, dst as usize as u64, false),
                    _ => return Ok(false),
                };
                // SAFETY: frozen plugin callers promise both endpoints remain live
                // and exclusive through synchronous completion (or retained on -3).
                let result = unsafe { memory.plugin_copy_sync(offset, other, n, to_device) };
                if let Err(e) = result {
                    c.failed = true;
                    return Err(e.into());
                }
                Ok(true)
            })
        };
        match run() {
            Ok(true) => 0,
            Ok(false) => -1,
            Err(_) => HANDLED_ERROR,
        }
    })
}
unsafe extern "C" fn query(
    p: *mut c_void,
    addr: *mut c_void,
    n: usize,
    out: *mut Location,
    count: usize,
) -> i32 {
    guard(-1, || {
        let run = || -> Result<i32> {
            if count == 0 {
                return Err(INVALID);
            }
            checked(out, 1)?;
            with(p, |c| {
                if c.failed {
                    return Err(UNKNOWN);
                }
                let location = if let Some((m, offset)) = range(c, addr as usize, n)? {
                    m.location(offset, n)?.location.to_string()
                } else if let Some(info) = external_location(addr as usize, n)? {
                    info.location.to_string()
                } else {
                    return Ok(0);
                };
                put(
                    out,
                    Location {
                        start: addr,
                        length: n,
                        location: fixed(&location),
                    },
                )?;
                Ok(1)
            })
        };
        run().unwrap_or(-1)
    })
}
unsafe extern "C" fn count(p: *mut c_void) -> i32 {
    guard(-1, || {
        with(p, |_| Ok(device_plugin::device_count()?)).unwrap_or(-1)
    })
}
unsafe extern "C" fn pci(p: *mut c_void, device: i32, out: *mut c_char, n: usize) -> i32 {
    result(|| {
        with(p, |_| {
            let value = device_plugin::device_pci_bus_id(device)?;
            if n <= value.len() {
                return Err(INVALID);
            }
            strncpy(out, n, &value)
        })
    })
}
#[unsafe(no_mangle)]
pub static orbite_device_plugin_abi_v1: Descriptor = DESCRIPTOR;
#[unsafe(no_mangle)]
pub extern "C" fn tent_register_device_plugin(out: *mut Table) -> i32 {
    result(|| {
        put(
            out,
            Table {
                class_name: c"cuda".as_ptr(),
                create_plugin: Some(create),
                destroy_plugin: Some(destroy),
                alloc: Some(alloc),
                free: Some(free),
                memcpy_sync: Some(copy),
                query_location: Some(query),
                get_device_count: Some(count),
                get_device_pci_bus_id: Some(pci),
            },
        )
    })
}
