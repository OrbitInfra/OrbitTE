//! VMM calls run against independent reservations mapping real memfd pages.
//! CUDA streams/events and their failure injection come from the common shim.
use super::*;
use crate::cuda::driver::{Status, VmmApi};
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd},
    sync::{OnceLock, Weak},
};
type Handle = sys::CUmemAllocationHandleType;
const BAD: Status = Status::CUDA_ERROR_INVALID_VALUE;
#[derive(Default)]
struct State {
    physical: HashMap<u64, Arc<Physical>>,
    reservations: HashMap<u64, (usize, Option<Arc<Physical>>)>,
    gpu: HashMap<i32, Weak<Device>>,
    hooks: HashMap<usize, Arc<Hooks>>,
}
struct Physical {
    id: u64,
    file: File,
    location: sys::CUmemLocation,
    kind: Handle,
}
#[derive(Default)]
pub(super) struct Hooks {
    pub fault: Mutex<Option<&'static str>>,
    pub calls: Mutex<Vec<&'static str>>,
    pub access: Mutex<Vec<Vec<(i32, i32)>>>,
}
static STATE: OnceLock<Mutex<State>> = OnceLock::new();
fn state() -> std::sync::MutexGuard<'static, State> {
    STATE.get_or_init(Default::default).lock().unwrap()
}
static NEXT: AtomicUsize = AtomicUsize::new(1);
fn next() -> u64 {
    NEXT.fetch_add(1, Ordering::Relaxed) as u64
}
fn data() -> Arc<Hooks> {
    state().hooks[&(super::super::shim::current() as *const Device as usize)].clone()
}
fn call(name: &'static str) -> bool {
    let h = data();
    h.calls.lock().unwrap().push(name);
    let mut f = h.fault.lock().unwrap();
    if *f == Some(name) {
        *f = None;
        true
    } else {
        false
    }
}
fn put<T>(p: *mut T, v: T) {
    assert!(!p.is_null());
    // SAFETY: each call below receives the typed writable output of the production table.
    unsafe { p.write(v) }
}
fn read<T: Copy>(p: *const T) -> T {
    assert!(!p.is_null());
    // SAFETY: the typed input belongs to the calling driver operation through its return.
    unsafe { p.read() }
}

#[derive(Clone)]
pub(super) struct Rig {
    pub gpus: Vec<Arc<Device>>,
    ids: Vec<i32>,
    pub hooks: Arc<Hooks>,
    pub missing: bool,
}
thread_local! {static INSTALL:RefCell<Option<Rig>>=const{RefCell::new(None)};}
impl Rig {
    pub fn new() -> Self {
        let hooks = Arc::new(Hooks::default());
        let gpus: Vec<_> = (0..2).map(|_| allocation(&[0; 16])).collect();
        let ids: Vec<_> = gpus
            .iter()
            .map(|g| {
                let id = next() as i32;
                let mut s = state();
                s.gpu.retain(|_, gpu| gpu.strong_count() != 0);
                s.gpu.insert(id, Arc::downgrade(g));
                s.hooks.insert(Arc::as_ptr(g) as usize, hooks.clone());
                id
            })
            .collect();
        Self {
            gpus,
            ids,
            hooks,
            missing: false,
        }
    }
    pub fn single(mut self) -> Self {
        self.gpus.truncate(1);
        self.ids.truncate(1);
        self
    }
    pub fn engine(&self, c: EngineConfig) -> Result<TransferEngine> {
        INSTALL.with(|slot| {
            let old = slot.replace(Some(self.clone()));
            let res = TransferEngine::new(c);
            slot.replace(old);
            res
        })
    }
    pub fn fail(&self, name: &'static str) {
        *self.hooks.fault.lock().unwrap() = Some(name)
    }
    pub fn calls(&self, name: &str) -> usize {
        self.hooks
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| **c == name)
            .count()
    }
}
pub(super) fn install(
    c: &MnnvlConfig,
    boot: SourceEpoch,
    n: usize,
    b: usize,
) -> Option<Result<Arc<Platform>>> {
    INSTALL.with(|slot| {
        let r = slot.borrow().clone()?;
        let mut driver = Arc::try_unwrap(driver()).ok()?;
        driver.host_register = host_reg;
        driver.host_unregister = host_unreg;
        driver.device_get = get_device;
        driver.primary_retain = primary;
        driver.primary_release = release_primary;
        driver.memset = zero;
        driver.address_range = address_range;
        driver.vmm = (!r.missing).then_some(VmmApi {
            count,
            attribute: attr,
            can_peer: can,
            enable_peer: enable,
            granularity: gran,
            create,
            retain,
            properties,
            release,
            reserve,
            free,
            map,
            unmap,
            access,
            export,
            import,
        });
        driver.copy_batch = Some(super::super::shim::copy_batch);
        Some(Platform::discover(Arc::new(driver), c, boot, n, b))
    })
}

unsafe extern "C" fn count(out: *mut i32) -> Status {
    INSTALL.with(|s| put(out, s.borrow().as_ref().unwrap().ids.len() as i32));
    SUCCESS
}
unsafe extern "C" fn get_device(out: *mut i32, ord: i32) -> Status {
    INSTALL.with(
        |slot| match slot.borrow().as_ref().and_then(|r| r.ids.get(ord as usize)) {
            Some(id) => {
                put(out, *id);
                SUCCESS
            }
            None => BAD,
        },
    )
}
unsafe extern "C" fn primary(out: *mut sys::CUcontext, id: i32) -> Status {
    let gpu = state().gpu[&id].upgrade().unwrap();
    put(out, Arc::into_raw(gpu) as sys::CUcontext);
    SUCCESS
}
unsafe extern "C" fn release_primary(id: i32) -> Status {
    let ptr = state().gpu[&id].as_ptr();
    // SAFETY: primary retained exactly one strong reference for this fake CUdevice.
    unsafe { Arc::decrement_strong_count(ptr) };
    SUCCESS
}
unsafe extern "C" fn attr(out: *mut i32, attribute: sys::CUdevice_attribute, id: i32) -> Status {
    use sys::CUdevice_attribute::*;
    let v = match attribute {
        CU_DEVICE_ATTRIBUTE_HOST_NUMA_ID => INSTALL.with(|slot| {
            slot.borrow()
                .as_ref()
                .unwrap()
                .ids
                .iter()
                .position(|i| *i == id)
                .unwrap() as i32
        }),
        CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED => i32::from(!call("fabric-capability")),
        CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED
        | CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED => 1,
        _ => return BAD,
    };
    put(out, v);
    SUCCESS
}
unsafe extern "C" fn can(out: *mut i32, _: i32, _: i32) -> Status {
    put(out, 1);
    SUCCESS
}
unsafe extern "C" fn enable(_: sys::CUcontext, _: u32) -> Status {
    call("enable-peer");
    SUCCESS
}
unsafe extern "C" fn gran(
    out: *mut usize,
    _: *const sys::CUmemAllocationProp,
    _: sys::CUmemAllocationGranularity_flags,
) -> Status {
    put(out, 4096);
    SUCCESS
}
unsafe extern "C" fn create(
    out: *mut u64,
    len: usize,
    p: *const sys::CUmemAllocationProp,
    _: u64,
) -> Status {
    if call("create") {
        return BAD;
    }
    let p = read(p);
    assert_eq!(
        p.type_,
        sys::CUmemAllocationType::CU_MEM_ALLOCATION_TYPE_PINNED
    );
    // SAFETY: the static C string names a new anonymous fd owned by the fixture.
    let fd = unsafe { libc::memfd_create(c"orbitte-vmm".as_ptr(), libc::MFD_CLOEXEC) };
    assert!(fd >= 0);
    // SAFETY: a successful memfd_create returns exactly one owned descriptor.
    let file = unsafe { File::from_raw_fd(fd) };
    file.set_len(len as u64).unwrap();
    let id = next();
    state().physical.insert(
        id,
        Arc::new(Physical {
            id,
            file,
            location: p.location,
            kind: p.requestedHandleTypes,
        }),
    );
    put(out, id);
    SUCCESS
}
unsafe extern "C" fn reserve(out: *mut u64, len: usize, _: usize, _: u64, _: u64) -> Status {
    if call("reserve") {
        return BAD;
    }
    // SAFETY: create a fresh inaccessible VA reservation of the supplied positive page extent.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(addr, libc::MAP_FAILED);
    state().reservations.insert(addr as u64, (len, None));
    put(out, addr as u64);
    SUCCESS
}
unsafe extern "C" fn map(addr: u64, len: usize, offset: usize, handle: u64, _: u64) -> Status {
    if call("map") {
        return BAD;
    }
    let mut s = state();
    let p = s.physical[&handle].clone();
    let (extent, mapping) = s.reservations.get_mut(&addr).unwrap();
    assert_eq!((*extent, offset), (len, 0));
    assert!(mapping.is_none());
    // SAFETY: replace only this fixture's reservation with shared pages of its retained memfd.
    let raw = unsafe {
        libc::mmap(
            addr as usize as *mut _,
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_FIXED,
            p.file.as_raw_fd(),
            0,
        )
    };
    assert_eq!(raw as u64, addr);
    *mapping = Some(p);
    drop(s);
    if call("partially-mapped") {
        BAD
    } else {
        SUCCESS
    }
}
unsafe extern "C" fn access(
    addr: u64,
    len: usize,
    p: *const sys::CUmemAccessDesc,
    n: usize,
) -> Status {
    assert_eq!(state().reservations[&addr].0, len);
    // SAFETY: production supplies n initialized access descriptors for this native call.
    let list = unsafe { std::slice::from_raw_parts(p, n) };
    for item in list {
        assert_eq!(
            item.flags,
            sys::CUmemAccess_flags::CU_MEM_ACCESS_FLAGS_PROT_READWRITE
        );
    }
    data().access.lock().unwrap().push(
        list.iter()
            .map(|x| (x.location.type_ as i32, x.location.id))
            .collect(),
    );
    if call("set-access") { BAD } else { SUCCESS }
}
unsafe extern "C" fn unmap(addr: u64, len: usize) -> Status {
    if call("unmap") {
        return BAD;
    }
    let mut s = state();
    let (n, mapped) = s.reservations.get_mut(&addr).unwrap();
    assert_eq!(*n, len);
    assert!(mapped.take().is_some());
    assert_eq!(
        // SAFETY: preserve the complete live reservation while removing its physical backing.
        unsafe {
            libc::mmap(
                addr as usize as *mut _,
                len,
                libc::PROT_NONE,
                libc::MAP_FIXED | libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        } as u64,
        addr
    );
    SUCCESS
}
unsafe extern "C" fn free(addr: u64, len: usize) -> Status {
    if call("free-va") {
        return BAD;
    }
    let mut s = state();
    let (n, mapped) = &s.reservations[&addr];
    assert_eq!(*n, len);
    if mapped.is_some() {
        return BAD;
    }
    // SAFETY: only an unmapped reservation of this exact length reaches munmap.
    assert_eq!(unsafe { libc::munmap(addr as usize as *mut _, len) }, 0);
    s.reservations.remove(&addr);
    SUCCESS
}
unsafe extern "C" fn address_range(base: *mut u64, size: *mut usize, addr: u64) -> Status {
    let mismatch = call("range-mismatch");
    let s = state();
    match s
        .reservations
        .iter()
        .find(|(b, (n, _))| addr >= **b && addr - **b < *n as u64)
    {
        Some((b, (n, _))) => {
            put(base, *b);
            put(size, if mismatch { *n + 4096 } else { *n });
            SUCCESS
        }
        None => BAD,
    }
}
unsafe extern "C" fn retain(out: *mut u64, ptr: *mut std::ffi::c_void) -> Status {
    let mut s = state();
    let p = s.reservations[&(ptr as u64)].1.as_ref().unwrap().clone();
    let id = next();
    s.physical.insert(id, p);
    put(out, id);
    SUCCESS
}
unsafe extern "C" fn properties(out: *mut sys::CUmemAllocationProp, handle: u64) -> Status {
    let mismatch = call("numa-mismatch");
    let combined = call("combined-handle-mask");
    let s = state();
    let p = &s.physical[&handle];
    let mut props = read(out);
    props.location = p.location;
    if mismatch {
        props.location.id += 1;
    }
    props.requestedHandleTypes = p.kind;
    put(out, props);
    if combined {
        // SAFETY: CUDA uses this C field as a mask. Write its integer representation
        // to the provided raw output, without constructing an invalid Rust discriminant.
        unsafe {
            std::ptr::addr_of_mut!((*out).requestedHandleTypes)
                .cast::<u32>()
                .write(
                    crate::cuda::fabric::handle_bits(Handle::CU_MEM_HANDLE_TYPE_FABRIC)
                        | crate::cuda::fabric::handle_bits(
                            Handle::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
                        ),
                )
        }
    }
    SUCCESS
}
unsafe extern "C" fn release(handle: u64) -> Status {
    if call("release") {
        return BAD;
    }
    assert!(state().physical.remove(&handle).is_some());
    SUCCESS
}
unsafe extern "C" fn export(dst: *mut std::ffi::c_void, h: u64, kind: Handle, _: u64) -> Status {
    if call("export") {
        return BAD;
    }
    let s = state();
    let p = &s.physical[&h];
    assert_eq!(p.kind, kind);
    if kind == Handle::CU_MEM_HANDLE_TYPE_FABRIC {
        let mut b = [0; 64];
        b[..8].copy_from_slice(&p.id.to_be_bytes());
        put(dst.cast(), sys::CUmemFabricHandle { data: b });
    } else {
        put(dst.cast(), p.file.try_clone().unwrap().into_raw_fd());
    }
    SUCCESS
}
unsafe extern "C" fn import(out: *mut u64, src: *mut std::ffi::c_void, kind: Handle) -> Status {
    if call("import") {
        return BAD;
    }
    let mut s = state();
    let p = if kind == Handle::CU_MEM_HANDLE_TYPE_FABRIC {
        let bytes = read(src.cast::<sys::CUmemFabricHandle>()).data;
        let id = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        s.reservations
            .values()
            .filter_map(|(_, p)| p.as_ref())
            .find(|p| p.id == id)
            .unwrap()
            .clone()
    } else {
        use std::os::unix::fs::MetadataExt;
        let fd = i32::try_from(src as usize).unwrap();
        // SAFETY: the import call borrows the duplicated POSIX fd through this call.
        let borrowed = unsafe { std::os::fd::BorrowedFd::borrow_raw(fd) };
        let file = File::from(borrowed.try_clone_to_owned().unwrap());
        let ino = file.metadata().unwrap().ino();
        // The fd value is live during import, independently of the exporter's descriptor.
        Arc::new(Physical {
            id: ino,
            file,
            kind,
            location: sys::CUmemLocation {
                type_: sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_DEVICE,
                id: 0,
            },
        })
    };
    let id = next();
    s.physical.insert(id, p);
    put(out, id);
    SUCCESS
}
unsafe extern "C" fn host_reg(p: *mut std::ffi::c_void, n: usize, flags: u32) -> Status {
    assert_eq!(flags, 0);
    assert!(
        super::super::shim::current()
            .host_registrations
            .lock()
            .unwrap()
            .insert(p as usize, n)
            .is_none()
    );
    SUCCESS
}
unsafe extern "C" fn host_unreg(p: *mut std::ffi::c_void) -> Status {
    if call("host-unregister") {
        return BAD;
    }
    assert!(
        super::super::shim::current()
            .host_registrations
            .lock()
            .unwrap()
            .remove(&(p as usize))
            .is_some()
    );
    SUCCESS
}
unsafe extern "C" fn zero(addr: u64, value: u8, len: usize) -> Status {
    assert_eq!(state().reservations[&addr].0, len);
    // SAFETY: the shim just granted writable access to the entire mapped extent.
    unsafe { std::ptr::write_bytes(addr as usize as *mut u8, value, len) };
    SUCCESS
}
