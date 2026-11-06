use crate::{
    abi::{Buffer, Options},
    boundary::*,
};
use orbite::{HostMemory, MemoryBacking, Permission, RegisteredMemory, TransferEngine, Transport};
use std::{
    collections::BTreeMap,
    ffi::c_void,
    ptr::{self, NonNull},
    sync::{Arc, Mutex, OnceLock, Weak},
};

pub(crate) fn transport(n: i32) -> Result<Transport> {
    let n = u8::try_from(n).map_err(|_| INVALID)?;
    Transport::from_tent_id(n).ok_or(if n <= 18 { UNSUPPORTED } else { INVALID })
}
pub(crate) fn permission(n: i32) -> Result<Permission> {
    match n {
        0 => Ok(Permission::LocalOnly),
        1 => Ok(Permission::RemoteRead),
        2 => Ok(Permission::RemoteReadWrite),
        _ => Err(INVALID),
    }
}
#[derive(Clone, Copy, Default)]
enum Location {
    #[default]
    Any,
    Cpu(Option<u32>),
    Cuda(i32),
}
impl Location {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "" | "*" => Ok(Self::Any),
            "cpu" => Ok(Self::Cpu(Some(0))),
            "cuda" => Ok(Self::Cuda(0)),
            _ => {
                let (kind, id) = s.split_once(':').unwrap_or((s, "0"));
                match kind {
                    "cpu" => Ok(Self::Cpu(Some(id.parse().map_err(|_| INVALID)?))),
                    "cuda" => Ok(Self::Cuda(
                        i32::try_from(id.parse::<u32>().map_err(|_| INVALID)?)
                            .map_err(|_| INVALID)?,
                    )),
                    "mlu" | "xpu" | "ascend" | "rocm" | "hip" | "npu" | "tpu" => Err(UNSUPPORTED),
                    _ => Err(INVALID),
                }
            }
        }
    }
}
pub(crate) struct Opt {
    shm_path: String,
    shm_offset: usize,
    location: Location,
    perm: Permission,
    route: Transport,
    internal: bool,
}
impl Default for Opt {
    fn default() -> Self {
        Self {
            shm_path: String::new(),
            shm_offset: 0,
            location: Location::Any,
            perm: Permission::RemoteReadWrite,
            route: Transport::Unspec,
            internal: false,
        }
    }
}
impl Opt {
    #[cfg(feature = "python")]
    pub(crate) fn fields(
        location: &str,
        perm: i32,
        route: i32,
        path: String,
        offset: usize,
        internal: bool,
    ) -> Result<Self> {
        if location.len() >= 64
            || path.len() >= 256
            || location.contains('\0')
            || path.contains('\0')
        {
            return Err(INVALID);
        }
        Ok(Self {
            location: Location::parse(location)?,
            perm: permission(perm)?,
            route: transport(route)?,
            shm_path: path,
            shm_offset: offset,
            internal,
        })
    }
    pub(crate) fn simple(s: &str) -> Result<Self> {
        Ok(Self {
            location: Location::parse(s)?,
            ..Self::default()
        })
    }
    pub(crate) fn perms(n: i32) -> Result<Self> {
        Ok(Self {
            perm: permission(n)?,
            ..Self::default()
        })
    }
    pub(crate) fn read(p: *mut Options) -> Result<Self> {
        checked(p, 1)?;
        // SAFETY: the caller supplies one initialized options record. Read C strings only through their terminators.
        unsafe {
            let route = transport((*p).transport_type)?;
            let path = text(ptr::addr_of!((*p).shm_path).cast(), 256)?;
            Ok(Self {
                shm_path: path,
                shm_offset: (*p).shm_offset,
                location: Location::parse(&text(ptr::addr_of!((*p).location).cast(), 64)?)?,
                perm: permission((*p).permission)?,
                route,
                internal: (*p).internal != 0,
            })
        }
    }
}
#[derive(Clone)]
enum WeakBacking {
    Host(Weak<HostMemory>),
    #[cfg(feature = "cuda")]
    Cuda(Weak<orbite::cuda::CudaMemory>),
}
impl WeakBacking {
    fn new(m: &MemoryBacking) -> Self {
        match m {
            MemoryBacking::Host(v) => Self::Host(Arc::downgrade(v)),
            #[cfg(feature = "cuda")]
            MemoryBacking::Cuda(v) => Self::Cuda(Arc::downgrade(v)),
        }
    }
    fn upgrade(&self) -> Option<MemoryBacking> {
        match self {
            Self::Host(v) => v.upgrade().map(MemoryBacking::Host),
            #[cfg(feature = "cuda")]
            Self::Cuda(v) => v.upgrade().map(MemoryBacking::Cuda),
        }
    }
}
#[derive(Clone)]
pub(crate) struct Owner {
    pub keep: Arc<dyn Send + Sync>,
    pub kind: ExportKind,
}
#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "python"), allow(dead_code))]
pub(crate) enum ExportKind {
    Host,
    #[cfg(feature = "cuda")]
    Cuda {
        address: usize,
        length: usize,
        device: Option<i32>,
        stream: Option<usize>,
    },
}
struct Span {
    #[cfg(all(feature = "device-plugin", feature = "cuda"))]
    plugin: bool,
    fenced: bool,
    end: usize,
    owner: WeakBacking,
    foreign: Option<Weak<()>>,
    retained: bool,
}
fn spans() -> &'static Mutex<BTreeMap<usize, Span>> {
    static SPANS: OnceLock<Mutex<BTreeMap<usize, Span>>> = OnceLock::new();
    SPANS.get_or_init(Mutex::default)
}
fn bounds(p: *mut c_void, n: usize) -> Result<(usize, usize)> {
    if n == 0 {
        return Err(INVALID);
    }
    let start = checked(p.cast::<u8>(), n)?.as_ptr() as usize;
    Ok((start, start + n))
}
fn base(m: &MemoryBacking) -> usize {
    match m {
        MemoryBacking::Host(v) => v.base_address() as usize,
        #[cfg(feature = "cuda")]
        MemoryBacking::Cuda(v) => v.base_address() as usize,
    }
}
fn foreign(
    p: *mut c_void,
    n: usize,
    loc: Location,
    owner: Arc<dyn Send + Sync>,
) -> Result<MemoryBacking> {
    #[cfg(feature = "cuda")]
    {
        // SAFETY: the C registration contract pins the allocation until successful
        // unregister. The native probe checks kind, extent and current context.
        if let Some(gpu) = unsafe {
            orbite::cuda::CudaMemory::from_foreign_address(p as usize as u64, n, owner.clone())
        }? {
            if let Location::Cuda(id) = loc {
                if !matches!(gpu.location(0,n)?.location, orbite::MemoryLocation::Cuda{device,..} if device==id)
                {
                    return Err(INVALID);
                }
            }
            return Ok(MemoryBacking::Cuda(Arc::new(gpu)));
        }
    }
    if matches!(loc, Location::Cuda(_)) {
        return Err(UNSUPPORTED);
    }
    let p = NonNull::new(p.cast()).ok_or(INVALID)?;
    // SAFETY: C supplies initialized, writable host storage and coordinates all
    // external access until final unregister succeeds. Global spans share its
    // actual HostMemory lock. The release receipt below checks this backing.
    Ok(MemoryBacking::Host(Arc::new(unsafe {
        HostMemory::from_raw_parts(p, n, owner)
    }?)))
}
fn resolve(
    p: *mut c_void,
    n: usize,
    loc: Location,
    owner: Option<Owner>,
    allow_unowned: bool,
) -> Result<(MemoryBacking, usize, Option<Weak<()>>)> {
    let (start, end) = bounds(p, n)?;
    let lease = Arc::new(());
    let receipt = Arc::downgrade(&lease);
    let retained = owner.is_some();
    let keep: Arc<dyn Send + Sync> = Arc::new((lease, owner.as_ref().map(|o| o.keep.clone())));
    let mut all = lock(spans())?;
    all.retain(|_, e| {
        e.fenced
            || e.owner.upgrade().is_some()
            || e.foreign.as_ref().is_some_and(|w| w.strong_count() != 0)
    });
    if all.range(..end).any(|(_, s)| s.fenced && s.end > start) {
        return Err(UNKNOWN);
    }
    if all.len() >= 65536 {
        return Err(FULL);
    }
    // CUDA exporters take the strict device path. Validate the *exported* extent,
    // not just the requested subrange, against native allocation metadata.
    #[cfg(feature = "cuda")]
    let candidate = if let Some(Owner {
        kind:
            ExportKind::Cuda {
                address,
                length,
                device,
                ..
            },
        ..
    }) = &owner
    {
        if start < *address || end > address.checked_add(*length).ok_or(INVALID)? {
            return Err(INVALID);
        }
        // SAFETY: the retained interchange owner pins writable device memory and
        // its context. The core verifies kind, actual mapped extent and ordinal.
        let probed = unsafe {
            orbite::cuda::CudaMemory::from_raw_parts(*address as u64, *length, keep.clone())
        };
        let memory = match probed {
            Ok(memory) => memory,
            Err(error @ orbite::Error::Unquiesced { .. }) => {
                // An uncertain context restore retains the native owner. Fence
                // this address globally as well as the engine's Python lease.
                let export_end = address.checked_add(*length).ok_or(INVALID)?;
                for (&base, span) in all.iter_mut() {
                    if base < export_end && span.end > *address {
                        span.fenced = true;
                    }
                }
                all.entry(*address)
                    .and_modify(|span| span.end = span.end.max(export_end))
                    .or_insert(Span {
                        #[cfg(all(feature = "device-plugin", feature = "cuda"))]
                        plugin: false,
                        fenced: true,
                        end: export_end,
                        owner: WeakBacking::Cuda(Weak::new()),
                        foreign: Some(receipt.clone()),
                        retained: true,
                    });
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        if device.is_some_and(|expected| !matches!(memory.location(0, *length).map(|l| l.location), Ok(orbite::MemoryLocation::Cuda {device, ..}) if device == expected)) { return Err(INVALID); }
        Some(MemoryBacking::Cuda(Arc::new(memory)))
    } else {
        None
    };
    if let Some((&other, s)) = all.range(..end).next_back() {
        if s.end > start {
            if s.fenced {
                return Err(UNKNOWN);
            }
            if other <= start && end <= s.end {
                if !allow_unowned && !s.retained {
                    return Err(ADDRESS);
                }
                return Ok((
                    s.owner.upgrade().ok_or(UNKNOWN)?,
                    start - other,
                    s.foreign.clone(),
                ));
            }
            return Err(INVALID); // partial overlap has no common allocation owner
        }
    }
    if !allow_unowned && owner.is_none() {
        return Err(ADDRESS);
    }
    #[cfg(feature = "cuda")]
    let m = match candidate {
        Some(m) => m,
        None => foreign(p, n, loc, keep)?,
    };
    #[cfg(not(feature = "cuda"))]
    let m = foreign(p, n, loc, keep)?;
    let span_start = base(&m);
    let span_end = match &m {
        MemoryBacking::Host(v) => span_start.checked_add(v.len()),
        #[cfg(feature = "cuda")]
        MemoryBacking::Cuda(v) => span_start.checked_add(v.len()),
    }
    .ok_or(INVALID)?;
    // Do not create partially overlapping native owners from different exports.
    if all
        .iter()
        .any(|(&base, s)| base < span_end && s.end > span_start)
    {
        return Err(INVALID);
    }
    all.insert(
        span_start,
        Span {
            #[cfg(all(feature = "device-plugin", feature = "cuda"))]
            plugin: false,
            fenced: false,
            end: span_end,
            owner: WeakBacking::new(&m),
            foreign: Some(receipt.clone()),
            retained,
        },
    );
    Ok((m, start - span_start, Some(receipt)))
}
#[derive(Clone)]
enum Kind {
    #[cfg(feature = "device-plugin")]
    Plugin(Arc<crate::device_plugin::Allocation>),
    Ordinary,
    Shm,
    #[cfg(feature = "cuda")]
    Fabric,
    #[cfg(feature = "cuda")]
    Egm,
}
struct Allocation {
    memory: Option<MemoryBacking>,
    kind: Kind,
    bytes: usize,
    error: Option<Code>,
}
struct Reg {
    owners: Vec<Arc<dyn Send + Sync>>,
    backing_base: usize,
    memory: Option<RegisteredMemory>,
    foreign: Option<Weak<()>>,
    removed: usize,
    refs: usize,
    internal: bool,
    route: Transport,
    error: Option<Code>,
}
#[derive(Default)]
pub(crate) struct Memory {
    #[cfg(feature = "python")]
    deferred: std::collections::BTreeSet<usize>,
    quarantine: Vec<MemoryBacking>,
    quarantine_owners: Vec<Arc<dyn Send + Sync>>,
    alloc: BTreeMap<usize, Allocation>,
    regs: BTreeMap<(usize, usize), Reg>,
    bytes: usize,
    #[cfg(feature = "device-plugin")]
    plugins: Vec<Arc<crate::device_plugin::Plugin>>,
}

impl Memory {
    #[cfg(feature = "python")]
    pub(crate) fn defer_free(&mut self, e: &TransferEngine, addr: usize) -> Result<()> {
        match self.free(e, addr as *mut c_void) {
            Ok(()) => Ok(()),
            Err(c) if c == FULL => {
                self.deferred.insert(addr); // bounded by the allocation table
                Ok(())
            }
            Err(c) => Err(c),
        }
    }
    #[cfg(feature = "python")]
    pub(crate) fn reap_deferred(&mut self, e: &TransferEngine) {
        let addresses: Vec<_> = self.deferred.iter().copied().collect();
        for addr in addresses {
            if self.free(e, addr as *mut c_void) != Err(FULL) {
                self.deferred.remove(&addr);
            }
        }
    }
    pub fn allocate(&mut self, e: &TransferEngine, n: usize, o: Opt) -> Result<*mut c_void> {
        if n == 0 || n > isize::MAX as usize {
            return Err(INVALID);
        }
        let cfg = e.config();
        let total = self
            .bytes
            .checked_add(n)
            .filter(|v| *v <= cfg.max_registered_bytes)
            .ok_or(FULL)?;
        if self.alloc.len() >= cfg.max_registered_regions {
            return Err(FULL);
        }
        if !e.transport_installed(o.route) {
            return Err(UNSUPPORTED);
        }
        // The SHM allocator generates its own name and starts at offset zero.
        // Supplied names are only authority-checked on registration.
        if (!o.shm_path.is_empty() || o.shm_offset != 0) && o.route != Transport::Shm {
            return Err(UNSUPPORTED);
        }
        if !self.quarantine.is_empty() || !self.quarantine_owners.is_empty() {
            return Err(UNKNOWN);
        }
        let mut all = lock(spans())?;
        all.retain(|_, s| {
            s.fenced
                || s.owner.upgrade().is_some()
                || s.foreign.as_ref().is_some_and(|w| w.strong_count() != 0)
        });
        if all.len() >= 65536 {
            return Err(FULL);
        }
        #[cfg(feature = "device-plugin")]
        let plugin = self
            .plugins
            .iter()
            .find(|p| {
                p.class
                    == match o.location {
                        Location::Cuda(_) => "cuda",
                        _ => "cpu",
                    }
            })
            .cloned();
        #[cfg(feature = "device-plugin")]
        if let Some(plugin) = plugin {
            if matches!(o.route, Transport::Shm | Transport::Mnnvl) {
                return Err(UNSUPPORTED);
            }
            let location = match o.location {
                Location::Cuda(id) => format!("cuda:{id}"),
                Location::Cpu(Some(id)) => format!("cpu:{id}"),
                _ => "cpu:0".into(),
            };
            let (owner, allocated) = plugin.allocate(n, &location);
            let backing = allocated.and_then(|()| {
                let actual = owner.location()?;
                if actual != location {
                    return Err(orbite::Error::Unavailable(
                        "device plugin returned a different location",
                    ));
                }
                match o.location {
                    Location::Cuda(_) => {
                        #[cfg(feature = "cuda")]
                        {
                            // SAFETY: this allocation's plugin owner pins it until
                            // native drain. CUDA verifies the claimed type and extent.
                            let gpu = unsafe {
                                orbite::cuda::CudaMemory::from_raw_parts(
                                    owner.address as u64,
                                    n,
                                    owner.clone(),
                                )
                            }?;
                            if gpu.location(0, n)?.location.to_string() != location {
                                return Err(orbite::Error::Unavailable(
                                    "device plugin CUDA device mismatch",
                                ));
                            }
                            Ok(MemoryBacking::Cuda(Arc::new(gpu)))
                        }
                        #[cfg(not(feature = "cuda"))]
                        {
                            Err(orbite::Error::Unavailable("CUDA feature disabled"))
                        }
                    }
                    _ => {
                        // SAFETY: the checked CPU plugin supplies writable stable
                        // host storage. Its owner is held through every core loan.
                        let host = unsafe {
                            HostMemory::from_raw_parts(
                                NonNull::new(owner.address as *mut u8)
                                    .ok_or(orbite::Error::InvalidRange)?,
                                n,
                                owner.clone(),
                            )
                        }?;
                        Ok(MemoryBacking::Host(Arc::new(host)))
                    }
                }
            });
            let memory = match backing {
                Ok(memory) => memory,
                Err(error) => {
                    if matches!(error, orbite::Error::Unquiesced { .. }) || owner.close().is_err() {
                        self.quarantine_owners.push(owner);
                        self.bytes = total;
                        return Err(UNKNOWN);
                    }
                    return Err(error.into());
                }
            };
            let address = base(&memory);
            let end = address.checked_add(n).ok_or(INVALID)?;
            if let Some((_, prior)) = all.range_mut(..end).next_back() {
                if prior.end > address {
                    prior.fenced = true;
                    self.quarantine.push(memory);
                    self.quarantine_owners.push(owner);
                    self.bytes = total;
                    return Err(UNKNOWN);
                }
            }
            all.insert(
                address,
                Span {
                    #[cfg(all(feature = "device-plugin", feature = "cuda"))]
                    plugin: true,
                    fenced: false,
                    end,
                    owner: WeakBacking::new(&memory),
                    foreign: None,
                    retained: true,
                },
            );
            self.alloc.insert(
                address,
                Allocation {
                    memory: Some(memory),
                    kind: Kind::Plugin(owner),
                    bytes: n,
                    error: None,
                },
            );
            self.bytes = total;
            return Ok(address as *mut c_void);
        }
        let (m, kind) = match o.location {
            Location::Cuda(id) => {
                #[cfg(feature = "cuda")]
                {
                    if o.route == Transport::Shm || o.route == Transport::HpTcp {
                        return Err(UNSUPPORTED);
                    }
                    if o.route == Transport::Mnnvl
                        || (o.route == Transport::Unspec && e.transport_installed(Transport::Mnnvl))
                    {
                        (MemoryBacking::Cuda(e.allocate_mnnvl(id, n)?), Kind::Fabric)
                    } else {
                        let gpu = if e.transport_installed(Transport::Nvlink) {
                            orbite::cuda::CudaMemory::allocate_ipc(id, n)?
                        } else {
                            orbite::cuda::CudaMemory::allocate(id, n)?
                        };
                        (MemoryBacking::Cuda(Arc::new(gpu)), Kind::Ordinary)
                    }
                }
                #[cfg(not(feature = "cuda"))]
                {
                    let _ = id;
                    return Err(UNSUPPORTED);
                }
            }
            host => {
                #[cfg(feature = "cuda")]
                let egm = o.route == Transport::Mnnvl
                    || (o.route == Transport::Unspec && cfg.mnnvl.as_ref().is_some_and(|v| v.egm));
                #[cfg(not(feature = "cuda"))]
                let egm = false;
                if egm {
                    #[cfg(feature = "cuda")]
                    {
                        let node = match host {
                            Location::Cpu(Some(n)) => n,
                            _ => 0,
                        };
                        (MemoryBacking::Host(e.allocate_egm(node, n)?), Kind::Egm)
                    }
                    #[cfg(not(feature = "cuda"))]
                    {
                        return Err(UNSUPPORTED);
                    }
                } else if o.route == Transport::Shm
                    || (o.route == Transport::Unspec
                        && cfg.shm.enable
                        && !cfg.tcp
                        && !cfg.hp_tcp.enable
                        && !e.transport_installed(Transport::Rdma))
                {
                    (MemoryBacking::Host(e.allocate_shm_memory(n)?), Kind::Shm)
                } else {
                    let memory = if let Location::Cpu(Some(node)) = host {
                        #[cfg(target_os = "linux")]
                        {
                            HostMemory::new_on_node(n, node)?
                        }
                        #[cfg(not(target_os = "linux"))]
                        {
                            let _ = node;
                            return Err(UNSUPPORTED);
                        }
                    } else {
                        let mut b = Vec::new();
                        b.try_reserve_exact(n).map_err(|_| FULL)?;
                        b.resize(n, 0);
                        HostMemory::new(b.into_boxed_slice())?
                    };
                    (MemoryBacking::Host(Arc::new(memory)), Kind::Ordinary)
                }
            }
        };
        let address = base(&m);
        let end = address.checked_add(n).ok_or(INVALID)?;
        if let Some((_, prior)) = all.range_mut(..end).next_back() {
            if prior.end > address {
                // A provider can reuse an address after a half-completed free.
                // Neither owner can certify which native allocation is live.
                prior.fenced = true;
                self.quarantine.push(m);
                self.bytes = total;
                return Err(UNKNOWN);
            }
        }
        all.insert(
            address,
            Span {
                #[cfg(all(feature = "device-plugin", feature = "cuda"))]
                plugin: false,
                fenced: false,
                end,
                owner: WeakBacking::new(&m),
                foreign: None,
                retained: true,
            },
        );
        self.alloc.insert(
            address,
            Allocation {
                memory: Some(m),
                kind,
                bytes: n,
                error: None,
            },
        );
        self.bytes = total;
        Ok(address as *mut c_void)
    }
    pub fn free(&mut self, e: &TransferEngine, p: *mut c_void) -> Result<()> {
        let addr = p as usize;
        let a = self.alloc.get_mut(&addr).ok_or(INVALID)?;
        if let Some(error) = a.error {
            return Err(error);
        }
        let mut all = lock(spans())?;
        if all.get(&addr).is_some_and(|span| span.fenced) {
            return Err(UNKNOWN);
        }
        let m = a.memory.as_ref().ok_or(INTERNAL)?;
        let count = match m {
            MemoryBacking::Host(v) => Arc::strong_count(v),
            #[cfg(feature = "cuda")]
            MemoryBacking::Cuda(v) => Arc::strong_count(v),
        };
        if count != 1 {
            return Err(FULL);
        }
        let m = a.memory.take().ok_or(INTERNAL)?;
        // Keep the address and budget charged on failed native retirement.
        let result = match (m, a.kind.clone()) {
            (MemoryBacking::Host(h), Kind::Shm) => e.free_shm_memory(h),
            #[cfg(feature = "cuda")]
            (MemoryBacking::Host(h), Kind::Egm) => e.free_egm(h),
            #[cfg(feature = "cuda")]
            (MemoryBacking::Cuda(g), Kind::Fabric) => e.free_mnnvl(g),
            (MemoryBacking::Host(h), _) => Arc::try_unwrap(h)
                .map_err(|_| orbite::Error::BatchBusy)?
                .release_owned(),
            #[cfg(feature = "cuda")]
            (MemoryBacking::Cuda(g), _) => g.release_owned(),
        };
        #[cfg(feature = "device-plugin")]
        let result = result.and_then(|()| {
            if let Kind::Plugin(owner) = &a.kind {
                owner.close()
            } else {
                Ok(())
            }
        });
        if let Err(error) = result {
            if let Some(s) = all.get_mut(&addr) {
                s.fenced = true;
            }
            let code = Code::from(error);
            a.error = Some(code);
            return Err(code);
        }
        all.remove(&addr);
        self.bytes -= a.bytes;
        self.alloc.remove(&addr);
        #[cfg(feature = "python")]
        self.deferred.remove(&addr);
        Ok(())
    }
    pub fn register(
        &mut self,
        e: &TransferEngine,
        addrs: &[*mut c_void],
        sizes: &[usize],
        o: Opt,
    ) -> Result<()> {
        self.register_owned(e, addrs, sizes, o, &[], true)
    }
    pub(crate) fn register_owned(
        &mut self,
        e: &TransferEngine,
        addrs: &[*mut c_void],
        sizes: &[usize],
        o: Opt,
        owners: &[Option<Owner>],
        allow_unowned: bool,
    ) -> Result<()> {
        if addrs.len() != sizes.len()
            || (!owners.is_empty() && owners.len() != addrs.len())
            || addrs.len() > e.config().max_registered_regions
        {
            return Err(INVALID);
        }
        if !self.quarantine.is_empty() || !self.quarantine_owners.is_empty() {
            return Err(UNKNOWN);
        }
        let retained_count = self.quarantine_owners.len()
            + self.regs.values().map(|r| r.owners.len()).sum::<usize>();
        if retained_count
            .checked_add(owners.iter().flatten().count())
            .is_none_or(|n| n > e.config().max_registered_regions)
        {
            return Err(FULL);
        }
        if !e.transport_installed(o.route) {
            return Err(UNSUPPORTED);
        }
        let mut ranges = Vec::with_capacity(addrs.len());
        let mut witnesses = Vec::with_capacity(addrs.len());
        for (index, (&p, &n)) in addrs.iter().zip(sizes).enumerate() {
            let key = (p as usize, n);
            if let Some(r) = self.regs.get(&key) {
                if r.error.is_some()
                    || r.removed != 0
                    || r.internal != o.internal
                    || r.route != o.route
                {
                    return Err(INVALID);
                }
            }
            let resolved = resolve(
                p,
                n,
                o.location,
                owners.get(index).cloned().flatten(),
                allow_unowned,
            );
            let (memory, offset, receipt) = match resolved {
                Ok(v) => v,
                Err(error) => {
                    if error == UNKNOWN {
                        self.quarantine_owners
                            .extend(owners.iter().flatten().map(|o| o.keep.clone()));
                    }
                    return Err(error);
                }
            };
            if let Some(owner) = owners.get(index).and_then(Option::as_ref) {
                #[cfg(feature = "cuda")]
                if let ExportKind::Cuda { stream, device, .. } = owner.kind {
                    let MemoryBacking::Cuda(gpu) = &memory else {
                        return Err(INVALID);
                    };
                    if device.is_some_and(|expected| !matches!(gpu.location(offset, n).map(|l| l.location), Ok(orbite::MemoryLocation::Cuda {device, ..}) if device == expected)) { return Err(INVALID); }
                    if let Some(stream) = stream {
                        // SAFETY: CAI retains its producer stream; DLPack has
                        // ordered its work on this device's legacy default stream.
                        let result = unsafe {
                            gpu.record_exported_producer_stream(
                                stream as *mut c_void,
                                owner.keep.clone(),
                            )
                        };
                        if let Err(error) = result {
                            if matches!(error, orbite::Error::Unquiesced { .. }) {
                                if let Some(s) = lock(spans())?.get_mut(&base(&memory)) {
                                    s.fenced = true;
                                }
                                self.quarantine.push(memory);
                                self.quarantine_owners.push(owner.keep.clone());
                            }
                            return Err(error.into());
                        }
                    }
                }
                // A host exporter must never disguise a CUDA pointer, including
                // when an alias already exists in the shared span table.
                if matches!(owner.kind, ExportKind::Host)
                    && !matches!(memory, MemoryBacking::Host(_))
                {
                    return Err(INVALID);
                }
            }
            let actual = match &memory {
                MemoryBacking::Host(m) => m.location(offset, n)?.location,
                #[cfg(feature = "cuda")]
                MemoryBacking::Cuda(m) => m.location(offset, n)?.location,
            };
            match (o.location, actual) {
                (Location::Any, _) => (),
                (Location::Cpu(requested), orbite::MemoryLocation::Cpu(known))
                    if requested == known => {}
                (Location::Cpu(_), orbite::MemoryLocation::Cpu(None)) => return Err(UNSUPPORTED),
                (Location::Cuda(requested), orbite::MemoryLocation::Cuda { device, .. })
                    if requested == device => {}
                _ => return Err(INVALID),
            }
            if o.route == Transport::Shm || !o.shm_path.is_empty() || o.shm_offset != 0 {
                let name = e.shm_backing_name(&memory).ok_or(UNSUPPORTED)?;
                if (!o.shm_path.is_empty()
                    && o.shm_path.strip_prefix('/').unwrap_or(&o.shm_path) != name)
                    || (o.shm_offset != 0 && o.shm_offset != offset)
                {
                    return Err(INVALID);
                }
            }
            if o.route == Transport::HpTcp && !matches!(memory, MemoryBacking::Host(_)) {
                return Err(UNSUPPORTED);
            }
            witnesses.push((receipt, base(&memory)));
            ranges.push((memory, offset, n));
        }
        let additions: std::collections::BTreeSet<_> = addrs
            .iter()
            .zip(sizes)
            .map(|(&p, &n)| (p as usize, n))
            .filter(|k| !self.regs.contains_key(k))
            .collect();
        if self.regs.len() + additions.len() > e.config().max_registered_regions {
            return Err(FULL);
        }
        let registered = match e.register_memory_ranges(&ranges, o.perm, o.route, o.internal) {
            Ok(v) => v,
            Err(err @ orbite::Error::Unquiesced { .. }) => {
                let mut all = lock(spans())?;
                for (memory, _, _) in ranges {
                    if let Some(s) = all.get_mut(&base(&memory)) {
                        s.fenced = true;
                    }
                    self.quarantine.push(memory);
                }
                self.quarantine_owners
                    .extend(owners.iter().flatten().map(|o| o.keep.clone()));
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };
        for (index, (memory, (receipt, backing_base))) in
            registered.into_iter().zip(witnesses).enumerate()
        {
            let key = (memory.base_address() as usize, memory.len());
            let r = self.regs.entry(key).or_insert(Reg {
                backing_base,
                owners: Vec::new(),
                memory: Some(memory),
                foreign: receipt,
                removed: 0,
                refs: 0,
                internal: o.internal,
                route: o.route,
                error: None,
            });
            r.refs += 1;
            if let Some(owner) = owners.get(index).and_then(Option::as_ref) {
                r.owners.push(owner.keep.clone());
            }
        }
        Ok(())
    }
    fn key(&self, p: *mut c_void, n: usize) -> Result<(usize, usize)> {
        if p.is_null() {
            return Err(INVALID);
        }
        let addr = p as usize;
        if n != 0 {
            return self
                .regs
                .contains_key(&(addr, n))
                .then_some((addr, n))
                .ok_or(ADDRESS);
        }
        self.regs
            .range((addr, 0)..=(addr, usize::MAX))
            .next_back()
            .map(|(k, _)| *k)
            .ok_or(ADDRESS)
    }
    pub fn unregister(
        &mut self,
        e: &TransferEngine,
        addrs: &[*mut c_void],
        sizes: Option<&[usize]>,
    ) -> Result<()> {
        let mut uses = BTreeMap::new();
        for (i, &p) in addrs.iter().enumerate() {
            let key = self.key(p, sizes.map_or(0, |s| s[i]))?;
            let count = uses.entry(key).or_insert(0usize);
            *count += 1;
            if *count > self.regs[&key].refs {
                return Err(ADDRESS);
            }
        }
        self.release(e, &uses)
    }
    fn release(
        &mut self,
        e: &TransferEngine,
        uses: &BTreeMap<(usize, usize), usize>,
    ) -> Result<()> {
        // Separate admission close from acknowledgement. A retry of a partially
        // drained list must not deregister a second time or lose a finished key.
        let mut first = None;
        for (key, &n) in uses {
            let r = self.regs.get_mut(key).ok_or(ADDRESS)?;
            let result = (|| {
                if let Some(code) = r.error {
                    return Err(code);
                }
                while r.removed < n {
                    if let Err(err) = e.unregister_memory(r.memory.as_ref().ok_or(INTERNAL)?) {
                        let c = Code::from(err);
                        if c == UNKNOWN {
                            if let Some(span) = lock(spans())?.get_mut(&r.backing_base) {
                                span.fenced = true;
                            }
                        }
                        r.error = Some(c);
                        return Err(c);
                    }
                    r.removed += 1;
                }
                if r.removed == r.refs {
                    if r.memory.as_ref().is_some_and(|m| !m.exclusively_held()) {
                        return Err(FULL);
                    }
                    #[cfg(feature = "cuda")]
                    let external_backing = r.foreign.is_some();
                    #[cfg(all(feature = "cuda", feature = "device-plugin"))]
                    let external_backing = external_backing
                        || lock(spans())?
                            .get(&r.backing_base)
                            .is_some_and(|span| span.plugin);
                    #[cfg(feature = "cuda")]
                    if external_backing {
                        let backing = lock(spans())?
                            .get(&r.backing_base)
                            .and_then(|s| s.owner.upgrade());
                        if let Some(MemoryBacking::Cuda(gpu)) = backing {
                            // Do checked native cleanup while the registration
                            // and its charge still exist. Drop is not a receipt.
                            let closed = gpu
                                .trim_idle_copy_resources()
                                .and_then(|()| e.trim_idle_staging_buffers());
                            if let Err(error) = closed {
                                let code = Code::from(error);
                                if code == UNKNOWN {
                                    r.error = Some(code);
                                    if let Some(span) = lock(spans())?.get_mut(&r.backing_base) {
                                        span.fenced = true;
                                    }
                                }
                                return Err(code);
                            }
                        }
                    }
                    r.memory.take();
                }
                Ok(())
            })();
            if let Err(c) = result {
                first.get_or_insert(c);
            }
        }
        if let Some(code) = first {
            return Err(code);
        }
        for key in uses.keys() {
            let r = &self.regs[key];
            if r.removed == r.refs && r.foreign.as_ref().is_some_and(|w| w.strong_count() != 0) {
                return Err(FULL);
            }
        }
        for (key, &n) in uses {
            let r = self.regs.get_mut(key).ok_or(ADDRESS)?;
            r.refs -= n;
            r.removed -= n;
            if r.refs == 0 {
                self.regs.remove(key);
            }
        }
        Ok(())
    }
    pub fn info(&self) -> Result<Vec<Buffer>> {
        self.regs
            .values()
            .filter(|r| !r.internal && r.refs > r.removed)
            .map(|r| {
                let m = r.memory.as_ref().ok_or(INTERNAL)?;
                Ok(Buffer {
                    base: m.base_address(),
                    length: m.len() as u64,
                    location: fixed(&m.location()?.location.to_string()),
                })
            })
            .collect()
    }
    pub fn close(&mut self, e: &TransferEngine) -> Result<()> {
        if !self.quarantine.is_empty() || !self.quarantine_owners.is_empty() {
            return Err(UNKNOWN);
        }
        let refs = self.regs.iter().map(|(&k, r)| (k, r.refs)).collect();
        self.release(e, &refs)?;
        let keys: Vec<_> = self.alloc.keys().copied().collect();
        for k in keys {
            self.free(e, k as *mut c_void)?;
        }
        #[cfg(feature = "device-plugin")]
        self.close_plugins()?;
        Ok(())
    }
}

#[cfg(all(test, feature = "python"))]
#[path = "../tests/unit/python_memory.rs"]
mod python_tests;

#[cfg(feature = "device-plugin")]
impl Memory {
    pub fn load_plugins(&mut self, path: &std::path::Path) -> Result<()> {
        if !self.alloc.is_empty()
            || !self.regs.is_empty()
            || !self.quarantine_owners.is_empty()
            || !self.quarantine.is_empty()
        {
            return Err(FULL);
        }
        Ok(crate::device_plugin::load(&mut self.plugins, path)?)
    }
    pub fn close_plugins(&mut self) -> Result<()> {
        if self.alloc.values().any(|a| a.error == Some(UNKNOWN))
            || !self.quarantine_owners.is_empty()
        {
            return Err(UNKNOWN);
        }
        // Preflight every plugin first: no partial close while any allocation is
        // still retained by this engine or by another engine's alias.
        if self
            .alloc
            .values()
            .any(|a| matches!(a.kind, Kind::Plugin(_)))
            || !self.quarantine_owners.is_empty()
        {
            return Err(FULL);
        }
        let mut result = Ok(());
        for plugin in &self.plugins {
            if let Err(e) = plugin.close() {
                result = Err(Code::from(e));
            }
        }
        result?;
        self.plugins.clear();
        Ok(())
    }
    pub fn plugin_copy(&mut self, dst: *mut c_void, src: *mut c_void, size: usize) -> Result<()> {
        let (dst, end) = bounds(dst, size)?;
        let (src, src_end) = bounds(src, size)?;
        if dst < src_end && src < end {
            return Err(INVALID);
        }
        let resolve = |start, end| -> Result<_> {
            let (&base, a) = self.alloc.range(..=start).next_back().ok_or(INVALID)?;
            if base.checked_add(a.bytes).is_none_or(|n| end > n) {
                return Err(INVALID);
            }
            if let Some(e) = a.error {
                return Err(e);
            }
            // SHM/fabric allocations have their own cross-process access and
            // retirement protocols; a raw plugin callback cannot bypass them.
            if !matches!(a.kind, Kind::Ordinary | Kind::Plugin(_)) {
                return Err(UNSUPPORTED);
            }
            let memory = a.memory.as_ref().ok_or(INTERNAL)?;
            let (count, host) = match memory {
                MemoryBacking::Host(v) => (Arc::strong_count(v), true),
                #[cfg(feature = "cuda")]
                MemoryBacking::Cuda(v) => (Arc::strong_count(v), false),
            };
            if count != 1 {
                return Err(FULL);
            } // aliases/registrations/native work
            Ok((base, host))
        };
        let (db, dh) = resolve(dst, end)?;
        let (sb, sh) = resolve(src, src_end)?;
        for plugin in &self.plugins {
            match plugin.copy(dst, src, size) {
                Ok(true) => return Ok(()),
                Ok(false) => (),
                Err(e) => {
                    let mut spans = lock(spans())?;
                    for base in [db, sb] {
                        if let Some(a) = self.alloc.get_mut(&base) {
                            a.error = Some(UNKNOWN);
                        }
                        if let Some(s) = spans.get_mut(&base) {
                            s.fenced = true;
                        }
                    }
                    return Err(e.into());
                }
            }
        }
        if !dh || !sh {
            return Err(UNSUPPORTED);
        }
        // SAFETY: both ranges are checked, disjoint CPU allocations held solely
        // by this registry, with no registration, native loan or external alias.
        unsafe { ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, size) };
        Ok(())
    }
}
