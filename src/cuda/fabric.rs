//! CUDA VMM allocation, relocation and native release receipts.
use super::{
    Context, CudaMemory, DeviceAllocation, PinnedAllocation, RETAINED_IPC,
    driver::{Driver, VmmApi, check},
};
use crate::transport::mnnvl::{Capability, Location};
use crate::{Error, HostMemory, MnnvlConfig, MnnvlHandleType, Result, SourceEpoch};
use cudarc::driver::sys;
use std::{
    ffi::c_void,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{Arc, Mutex, Weak, atomic::Ordering},
};

mod external;
mod registration;
pub(crate) use registration::{HostPin, Registration};

type Kind = sys::CUmemAllocationHandleType;
fn native_kind(t: MnnvlHandleType) -> Kind {
    match t {
        MnnvlHandleType::Fabric => Kind::CU_MEM_HANDLE_TYPE_FABRIC,
        MnnvlHandleType::PosixFd => Kind::CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR,
    }
}
pub(super) fn handle_bits(kind: Kind) -> u32 {
    // SAFETY: the CUDA ABI represents this flag set as an unsigned 32-bit
    // integer. cudarc 0.19 uses either a repr(u32) enum or a transparent mask.
    unsafe { std::mem::transmute::<Kind, u32>(kind) }
}
fn location(kind: Location, device: i32) -> sys::CUmemLocation {
    let (type_, id) = match kind {
        Location::Device => (sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_DEVICE, device),
        Location::Host(n) => (
            sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_HOST_NUMA,
            n as i32,
        ),
    };
    sys::CUmemLocation { type_, id }
}
fn device_attribute(api: &VmmApi, device: i32, attribute: sys::CUdevice_attribute) -> Result<i32> {
    let mut value = 0;
    // SAFETY: a retained local device, a Driver API attribute and a writable integer output.
    check("cuDeviceGetAttribute", unsafe {
        (api.attribute)(&mut value, attribute, device)
    })?;
    Ok(value)
}
fn allocation_properties(
    kind: MnnvlHandleType,
    loc: sys::CUmemLocation,
) -> sys::CUmemAllocationProp {
    sys::CUmemAllocationProp {
        type_: sys::CUmemAllocationType::CU_MEM_ALLOCATION_TYPE_PINNED,
        requestedHandleTypes: native_kind(kind),
        location: loc,
        win32HandleMetaData: std::ptr::null_mut(),
        allocFlags: sys::CUmemAllocationProp_st__bindgen_ty_1 {
            compressionType: 0,
            gpuDirectRDMACapable: 0,
            usage: 0,
            reserved: [0; 4],
        },
    }
}
fn uncertain(e: Error, operation: &'static str) -> Error {
    match e {
        Error::Unquiesced { .. } => e,
        Error::Backend { operation, code } => Error::Unquiesced { operation, code },
        _ => Error::Unquiesced {
            operation,
            code: -1,
        },
    }
}
pub(crate) struct Platform {
    pub(crate) caps: Capability,
    config: MnnvlConfig,
    devices: Vec<Device>,
    api: Arc<VmmApi>,
    allocations: Arc<Limits>,
}
struct Device {
    context: Arc<Context>,
    numa: Option<u32>,
}

struct Limits {
    max_count: usize,
    max_bytes: usize,
    usage: Mutex<(usize, usize)>,
}
struct Charge {
    limits: Arc<Limits>,
    bytes: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        let mut u = self.limits.usage.lock().unwrap_or_else(|p| p.into_inner());
        u.0 -= 1;
        u.1 -= self.bytes;
    }
}
impl Limits {
    fn charge(self: &Arc<Self>, bytes: usize) -> Result<Charge> {
        let mut u = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("MNNVL allocation budget"))?;
        if u.0 >= self.max_count || bytes > self.max_bytes - u.1 {
            return Err(Error::ResourceExhausted);
        }
        u.0 += 1;
        u.1 += bytes;
        Ok(Charge {
            limits: self.clone(),
            bytes,
        })
    }
}
impl Platform {
    pub(crate) fn install(
        config: &MnnvlConfig,
        boot: SourceEpoch,
        count: usize,
        bytes: usize,
    ) -> Result<Arc<Self>> {
        #[cfg(test)]
        if let Some(p) = super::tests::fabric::install(config, boot, count, bytes) {
            return p;
        }
        Self::discover(Driver::load_fabric()?, config, boot, count, bytes)
    }
    pub(super) fn discover(
        driver: Arc<Driver>,
        config: &MnnvlConfig,
        boot: SourceEpoch,
        count: usize,
        bytes: usize,
    ) -> Result<Arc<Self>> {
        use sys::CUdevice_attribute::*;
        let api = driver
            .vmm
            .ok_or(Error::Unavailable("CUDA fabric/VMM symbols"))?;
        let mut n = 0;
        // SAFETY: cuInit completed and the device count output is writable.
        check("cuDeviceGetCount", unsafe { (api.count)(&mut n) })?;
        if !(1..=256).contains(&n) {
            return Err(Error::Unavailable(
                "MNNVL needs 1..=256 visible CUDA devices",
            ));
        }
        let mut devices = Vec::new();
        for ordinal in 0..n {
            let context = Context::primary(driver.clone(), ordinal)?;
            let attr = |name| device_attribute(&api, context.device, name);
            let numa = context.current(|| {
                if attr(CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED).ok() != Some(1) {
                    return Err(Error::Unavailable(
                        "a visible GPU does not support CUDA fabric handles",
                    ));
                }
                if config.handle_type == MnnvlHandleType::PosixFd
                    && attr(CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED).ok()
                        != Some(1)
                {
                    return Err(Error::Unavailable("CUDA POSIX shareable handles"));
                }
                Ok(attr(CU_DEVICE_ATTRIBUTE_HOST_NUMA_ID)
                    .ok()
                    .and_then(|x| u32::try_from(x).ok()))
            })?;
            devices.push(Device { context, numa });
        }
        for d in &devices {
            d.context.current(|| {
                for peer in &devices {
                    if d.context.device == peer.context.device {
                        continue;
                    }
                    let mut can = 0;
                    // SAFETY: both ordinals resolve to retained local CUDA devices.
                    check("cuDeviceCanAccessPeer", unsafe {
                        (api.can_peer)(&mut can, d.context.device, peer.context.device)
                    })?;
                    if can != 0 {
                        // SAFETY: d is current and the target primary context remains retained.
                        let rc = unsafe { (api.enable_peer)(peer.context.raw.as_ptr(), 0) };
                        if rc != sys::CUresult::CUDA_ERROR_PEER_ACCESS_ALREADY_ENABLED {
                            check("cuCtxEnablePeerAccess", rc)?
                        }
                    }
                }
                Ok(())
            })?;
        }
        let domain = match config.handle_type {
            MnnvlHandleType::PosixFd => boot,
            MnnvlHandleType::Fabric => config.fabric_domain.unwrap_or(boot),
        };
        Ok(Arc::new(Self {
            caps: Capability {
                domain,
                kind: config.handle_type,
                egm: config.egm,
            },
            config: config.clone(),
            devices,
            api: Arc::new(api),
            allocations: Arc::new(Limits {
                max_count: count,
                max_bytes: bytes,
                usage: Mutex::default(),
            }),
        }))
    }
    fn device(&self, id: i32) -> Result<&Device> {
        self.devices
            .get(usize::try_from(id).map_err(|_| Error::InvalidRange)?)
            .ok_or(Error::Unavailable("MNNVL local GPU ordinal"))
    }
    fn host_device(&self, node: Option<u32>) -> Result<&Device> {
        match node {
            Some(n) => self
                .devices
                .iter()
                .find(|d| d.numa == Some(n))
                .ok_or(Error::Unavailable(
                    "no visible GPU on the EGM owning socket",
                )),
            None => self.device(0),
        }
    }
    fn access(&self, host: Option<u32>) -> Vec<sys::CUmemAccessDesc> {
        let mut locs: Vec<_> = self
            .devices
            .iter()
            .map(|d| location(Location::Device, d.context.device))
            .collect();
        if let Some(n) = host {
            locs.push(location(Location::Host(n), 0));
        }
        locs.into_iter()
            .map(|location| sys::CUmemAccessDesc {
                location,
                flags: sys::CUmemAccess_flags::CU_MEM_ACCESS_FLAGS_PROT_READWRITE,
            })
            .collect()
    }
    pub(crate) fn allocate_device(&self, id: i32, len: usize) -> Result<Arc<CudaMemory>> {
        let d = self.device(id)?;
        let v = self.allocate(d, Location::Device, len)?;
        Ok(Arc::new(CudaMemory::from_fabric(v, len)))
    }
    pub(crate) fn allocate_egm(&self, node: u32, len: usize) -> Result<Arc<HostMemory>> {
        if !self.config.egm {
            return Err(Error::Unavailable("MNNVL EGM disabled"));
        }
        let d = self.host_device(Some(node))?;
        let v = self.allocate(d, Location::Host(node), len)?;
        Ok(Arc::new(HostMemory::from_cuda_pinned(Arc::new(
            PinnedAllocation::from_fabric(v, len, node)?,
        ))))
    }
    pub(crate) fn free_device(&self, m: Arc<CudaMemory>) -> Result<()> {
        let m = Arc::try_unwrap(m)
            .map_err(|_| Error::Unavailable("MNNVL device allocation has live owners"))?;
        let mut a = m
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock"))?;
        a.prepare_access(&(0..m.len()))?;
        let v = a
            .fabric
            .as_mut()
            .filter(|v| self.owns(v))
            .ok_or(Error::ForeignRegistration)?;
        v.close()?;
        a.address = 0;
        Ok(())
    }
    fn owns(&self, v: &Mapping) -> bool {
        v.charge
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(&c.limits, &self.allocations))
    }
    pub(crate) fn free_host(&self, m: Arc<HostMemory>) -> Result<()> {
        let m =
            Arc::try_unwrap(m).map_err(|_| Error::Unavailable("EGM allocation has live owners"))?;
        let pin = m
            .owned_pin()
            .filter(|p| p.fabric().is_some_and(|v| self.owns(v)))
            .ok_or(Error::ForeignRegistration)?;
        drop(m);
        Arc::try_unwrap(pin)
            .map_err(|_| Error::Unavailable("EGM allocation has live native owners"))?
            .close()
    }
    fn allocate(&self, d: &Device, loc: Location, len: usize) -> Result<Mapping> {
        if len == 0 || len > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let mut properties = allocation_properties(self.caps.kind, location(loc, d.context.device));
        let mut gran = 0;
        d.context.current(|| {
            if loc == Location::Device {
                use sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED;
                let supported = device_attribute(
                    &self.api,
                    d.context.device,
                    CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
                )?;
                properties.allocFlags.gpuDirectRDMACapable = u8::from(supported != 0);
            }
            // SAFETY: properties match this device/NUMA location and gran is a writable output.
            check("cuMemGetAllocationGranularity", unsafe {
                (self.api.granularity)(
                    &mut gran,
                    &properties,
                    sys::CUmemAllocationGranularity_flags::CU_MEM_ALLOC_GRANULARITY_MINIMUM,
                )
            })
        })?;
        if !gran.is_power_of_two() {
            return Err(Error::Unavailable("invalid CUDA VMM granularity"));
        }
        let size = len
            .checked_next_multiple_of(gran)
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or(Error::InvalidRange)?;
        let mut m = Mapping::new(&d.context, self.api.clone(), self.caps.kind, size, loc);
        m.charge = Some(self.allocations.charge(size)?);
        let access = self.access(match loc {
            Location::Host(n) => Some(n),
            _ => None,
        });
        let res = d.context.current(|| {
            // SAFETY: storage is charged and m owns the handle output before any later failure.
            check("cuMemCreate", unsafe {
                (self.api.create)(&mut m.handle, size, &properties, 0)
            })?;
            m.map(gran, &access)?;
            m.release_handle()?;
            match loc {
                Location::Device => {
                    // SAFETY: the full newly mapped extent is writable in this context, including padding.
                    let set = unsafe { (d.context.driver.memset)(m.address, 0, size) };
                    // SAFETY: initialization must drain before publishing or rolling back the mapping.
                    check("cuCtxSynchronize VMM initialization", unsafe {
                        (d.context.driver.synchronize)()
                    })
                    .map_err(|e| uncertain(e, "VMM initialization"))?;
                    check("cuMemsetD8", set)?;
                }
                Location::Host(_) => {
                    // SAFETY: cuMemSetAccess granted this host NUMA location the whole fresh mapping.
                    unsafe { std::ptr::write_bytes(m.address as usize as *mut u8, 0, size) }
                }
            }
            Ok(())
        });
        m.finish_setup(res)
    }
    pub(crate) fn import(
        &self,
        context: &Arc<Context>,
        extent: usize,
        bytes: [u8; 64],
    ) -> Result<Mapping> {
        if extent == 0 || extent > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let mut m = Mapping::new(
            context,
            self.api.clone(),
            self.caps.kind,
            extent,
            Location::Device,
        );
        let fd = match self.caps.kind {
            MnnvlHandleType::Fabric => None,
            MnnvlHandleType::PosixFd => {
                let pid = i32::from_be_bytes(
                    bytes[..4]
                        .try_into()
                        .map_err(|_| Error::Protocol("MNNVL pid"))?,
                );
                let fd = i32::from_be_bytes(
                    bytes[4..8]
                        .try_into()
                        .map_err(|_| Error::Protocol("MNNVL fd"))?,
                );
                if pid <= 0 || fd < 0 || bytes[24..] != [0; 40] {
                    return Err(Error::Protocol("MNNVL POSIX handle"));
                }
                Some(duplicate_fd(
                    pid,
                    fd,
                    bytes[8..24]
                        .try_into()
                        .map_err(|_| Error::Protocol("MNNVL PID identity"))?,
                )?)
            }
        };
        let fabric = sys::CUmemFabricHandle { data: bytes };
        let share = fd.as_ref().map_or_else(
            || std::ptr::from_ref(&fabric).cast_mut().cast::<c_void>(),
            |f| f.as_raw_fd() as usize as *mut c_void,
        );
        let access = self.access(None);
        let res = context.current(|| {
            // SAFETY: fabric passes the opaque bytes by address; POSIX passes the live fd value itself.
            let rc =
                unsafe { (self.api.import)(&mut m.handle, share, native_kind(self.caps.kind)) };
            drop(fd);
            check("cuMemImportFromShareableHandle", rc)?;
            m.map(0, &access)
        });
        m.finish_setup(res)
    }
}

pub(super) fn pid_identity(pid: i32) -> Result<[u8; 16]> {
    use std::os::unix::fs::MetadataExt;
    let ns = std::fs::metadata(format!("/proc/{pid}/ns/pid"))?.ino();
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let ticks = stat
        .rsplit_once(')')
        .and_then(|(_, tail)| tail.split_whitespace().nth(19))
        .and_then(|t| t.parse::<u64>().ok())
        .ok_or(Error::Protocol("invalid process start time"))?;
    let mut id = [0; 16];
    id[..8].copy_from_slice(&ns.to_be_bytes());
    id[8..].copy_from_slice(&ticks.to_be_bytes());
    Ok(id)
}
pub(super) fn duplicate_fd(pid: i32, fd: i32, identity: [u8; 16]) -> Result<OwnedFd> {
    // SAFETY: pidfd_open returns a new fd for the named process, with no userspace pointers.
    let proc = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0u32) } as i32;
    if proc < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: this is the sole owner of the newly returned kernel descriptor.
    let proc = unsafe { OwnedFd::from_raw_fd(proc) };
    if pid_identity(pid)? != identity {
        return Err(Error::Unavailable("MNNVL exporter process changed"));
    }
    // SAFETY: the kernel validates both descriptors and returns a new reference or errno.
    let local = unsafe { libc::syscall(libc::SYS_pidfd_getfd, proc.as_raw_fd(), fd, 0u32) } as i32;
    if local < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: the successful duplication transferred exactly one owned fd.
    Ok(unsafe { OwnedFd::from_raw_fd(local) })
}

/// Own every intermediate native reference. An uncertain destroy is never repeated.
pub(crate) struct Mapping {
    pub(super) context: Arc<Context>,
    api: Arc<VmmApi>,
    kind: MnnvlHandleType,
    pub(crate) address: u64,
    pub(crate) size: usize,
    loc: Location,
    handle: u64,
    mapped: bool,
    uncertain: bool,
    charge: Option<Charge>,
    external: Option<Arc<dyn Send + Sync>>,
    share: Mutex<Weak<Share>>,
}
impl Mapping {
    fn new(
        ctx: &Arc<Context>,
        api: Arc<VmmApi>,
        kind: MnnvlHandleType,
        size: usize,
        loc: Location,
    ) -> Self {
        Self {
            context: ctx.clone(),
            api,
            kind,
            size,
            loc,
            address: 0,
            handle: 0,
            mapped: false,
            uncertain: false,
            charge: None,
            external: None,
            share: Mutex::default(),
        }
    }
    fn map(&mut self, alignment: usize, access: &[sys::CUmemAccessDesc]) -> Result<()> {
        if self.handle == 0 {
            return Err(Error::Protocol("CUDA returned no VMM handle"));
        }
        // SAFETY: initialized owned output, exact rounded extent, and driver-validated alignment.
        check("cuMemAddressReserve", unsafe {
            (self.api.reserve)(&mut self.address, self.size, alignment, 0, 0)
        })?;
        if self.address == 0 {
            return Err(Error::Protocol("CUDA returned null VMM address"));
        }
        // SAFETY: this handle and the unoccupied VA reservation are both owned.
        check("cuMemMap", unsafe {
            (self.api.map)(self.address, self.size, 0, self.handle, 0)
        })?;
        self.mapped = true;
        // SAFETY: the mapping and access array covering the local visible devices remain live.
        check("cuMemSetAccess", unsafe {
            (self.api.access)(self.address, self.size, access.as_ptr(), access.len())
        })
    }
    fn release_handle(&mut self) -> Result<()> {
        // SAFETY: exactly one creator/import/retained reference is consumed on success.
        check("cuMemRelease", unsafe { (self.api.release)(self.handle) })
            .map_err(|e| uncertain(e, "VMM release"))?;
        self.handle = 0;
        Ok(())
    }
    fn free_va(&mut self) -> Result<()> {
        // SAFETY: successful unmap or a never-mapped reservation is required for addressFree.
        check("cuMemAddressFree", unsafe {
            (self.api.free)(self.address, self.size)
        })?;
        self.address = 0;
        Ok(())
    }
    fn finish_setup(mut self, result: Result<()>) -> Result<Self> {
        if let Err(e) = result {
            if matches!(e, Error::Unquiesced { .. }) {
                self.quarantine()
            } else {
                self.close()?
            }
            return Err(e);
        }
        Ok(self)
    }
    fn quarantine(&mut self) {
        if !self.uncertain {
            self.uncertain = true;
            RETAINED_IPC.fetch_add(1, Ordering::Relaxed);
            std::mem::forget((
                self.context.clone(),
                self.charge.take(),
                self.external.take(),
            ));
        }
    }
    pub(crate) fn close(&mut self) -> Result<()> {
        if self.uncertain {
            return Err(Error::Unquiesced {
                operation: "CUDA VMM retirement",
                code: -1,
            });
        }
        if self.external.is_some() {
            if self.context.poisoned.load(Ordering::Acquire) {
                self.quarantine();
                return Err(Error::Unquiesced {
                    operation: "MNNVL external context",
                    code: -1,
                });
            }
            self.external.take();
            self.charge.take();
            self.address = 0;
            return Ok(());
        }
        if self.address == 0 && self.handle == 0 {
            return Ok(());
        }
        let context = self.context.clone();
        let res = context.current(|| {
            if self.mapped {
                if self.handle == 0 {
                    // SAFETY: the creator reference was released after map; reacquire it before unmap.
                    check("cuMemRetainAllocationHandle", unsafe {
                        (self.api.retain)(&mut self.handle, self.address as usize as *mut c_void)
                    })?;
                }
                // SAFETY: this owner has the complete rounded extent and a physical drain receipt.
                check("cuMemUnmap", unsafe {
                    (self.api.unmap)(self.address, self.size)
                })?;
                self.mapped = false;
            } else if self.address != 0 {
                self.free_va()?;
            }
            if self.handle != 0 {
                self.release_handle()?;
            }
            if self.address != 0 {
                self.free_va()?;
            }
            Ok(())
        });
        if let Err(e) = res {
            self.quarantine();
            return Err(uncertain(e, "CUDA VMM retirement"));
        }
        self.charge.take();
        Ok(())
    }
}
impl Drop for Mapping {
    fn drop(&mut self) {
        if !self.uncertain {
            if let Err(e) = self.close() {
                log::error!("retaining CUDA VMM allocation: {e}");
            }
        }
    }
}
pub(crate) struct Share {
    pub(crate) bytes: [u8; 64],
    pub(crate) extent: usize,
    pub(crate) location: Location,
    _fd: Option<OwnedFd>,
}
