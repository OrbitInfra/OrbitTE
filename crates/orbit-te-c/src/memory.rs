use crate::{
    abi::{Buffer, Options},
    boundary::*,
};
use orbit_te::{
    HostMemory, MemoryBacking, Permission, RegisteredMemory, TransferEngine, Transport,
};
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
                    "mlu" | "xpu" | "ascend" | "rocm" | "npu" | "tpu" => Err(UNSUPPORTED),
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
    Cuda(Weak<orbit_te::cuda::CudaMemory>),
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
struct Span {
    fenced: bool,
    end: usize,
    owner: WeakBacking,
    foreign: Option<Weak<()>>,
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
            orbit_te::cuda::CudaMemory::from_foreign_address(p as usize as u64, n, owner.clone())
        }? {
            if let Location::Cuda(id) = loc {
                if !matches!(gpu.location(0,n)?.location, orbit_te::MemoryLocation::Cuda{device,..} if device==id)
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
) -> Result<(MemoryBacking, usize, Option<Weak<()>>)> {
    let (start, end) = bounds(p, n)?;
    let mut all = lock(spans())?;
    all.retain(|_, e| {
        e.fenced
            || e.owner.upgrade().is_some()
            || e.foreign.as_ref().is_some_and(|w| w.strong_count() != 0)
    });
    if let Some((&other, s)) = all.range(..end).next_back() {
        if s.end > start {
            if s.fenced {
                return Err(UNKNOWN);
            }
            if other <= start && end <= s.end {
                return Ok((
                    s.owner.upgrade().ok_or(UNKNOWN)?,
                    start - other,
                    s.foreign.clone(),
                ));
            }
            return Err(INVALID); // partial overlap has no common allocation owner
        }
    }
    if all.len() >= 65536 {
        return Err(FULL);
    }
    let lease = Arc::new(());
    let receipt = Arc::downgrade(&lease);
    let m = foreign(p, n, loc, lease)?;
    all.insert(
        start,
        Span {
            fenced: false,
            end,
            owner: WeakBacking::new(&m),
            foreign: Some(receipt.clone()),
        },
    );
    Ok((m, 0, Some(receipt)))
}
#[derive(Clone, Copy)]
enum Kind {
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
    quarantine: Vec<MemoryBacking>,
    alloc: BTreeMap<usize, Allocation>,
    regs: BTreeMap<(usize, usize), Reg>,
    bytes: usize,
}

impl Memory {
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
        if !self.quarantine.is_empty() {
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
                            orbit_te::cuda::CudaMemory::allocate_ipc(id, n)?
                        } else {
                            orbit_te::cuda::CudaMemory::allocate(id, n)?
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
                fenced: false,
                end,
                owner: WeakBacking::new(&m),
                foreign: None,
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
        let result = match (m, a.kind) {
            (MemoryBacking::Host(h), Kind::Shm) => e.free_shm_memory(h),
            #[cfg(feature = "cuda")]
            (MemoryBacking::Host(h), Kind::Egm) => e.free_egm(h),
            #[cfg(feature = "cuda")]
            (MemoryBacking::Cuda(g), Kind::Fabric) => e.free_mnnvl(g),
            (MemoryBacking::Host(h), _) => Arc::try_unwrap(h)
                .map_err(|_| orbit_te::Error::BatchBusy)?
                .release_owned(),
            #[cfg(feature = "cuda")]
            (MemoryBacking::Cuda(g), _) => g.release_owned(),
        };
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
        Ok(())
    }
    pub fn register(
        &mut self,
        e: &TransferEngine,
        addrs: &[*mut c_void],
        sizes: &[usize],
        o: Opt,
    ) -> Result<()> {
        if !e.transport_installed(o.route) {
            return Err(UNSUPPORTED);
        }
        if !self.quarantine.is_empty() {
            return Err(UNKNOWN);
        }
        let mut ranges = Vec::with_capacity(addrs.len());
        let mut witnesses = Vec::with_capacity(addrs.len());
        for (&p, &n) in addrs.iter().zip(sizes) {
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
            let (memory, offset, receipt) = resolve(p, n, o.location)?;
            let actual = match &memory {
                MemoryBacking::Host(m) => m.location(offset, n)?.location,
                #[cfg(feature = "cuda")]
                MemoryBacking::Cuda(m) => m.location(offset, n)?.location,
            };
            match (o.location, actual) {
                (Location::Any, _) => (),
                (Location::Cpu(requested), orbit_te::MemoryLocation::Cpu(known))
                    if requested == known => {}
                (Location::Cpu(_), orbit_te::MemoryLocation::Cpu(None)) => return Err(UNSUPPORTED),
                (Location::Cuda(requested), orbit_te::MemoryLocation::Cuda { device, .. })
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
            Err(err @ orbit_te::Error::Unquiesced { .. }) => {
                let mut all = lock(spans())?;
                for (memory, _, _) in ranges {
                    if let Some(s) = all.get_mut(&base(&memory)) {
                        s.fenced = true;
                    }
                    self.quarantine.push(memory);
                }
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };
        for (memory, (receipt, backing_base)) in registered.into_iter().zip(witnesses) {
            let key = (memory.base_address() as usize, memory.len());
            let r = self.regs.entry(key).or_insert(Reg {
                backing_base,
                memory: Some(memory),
                foreign: receipt,
                removed: 0,
                refs: 0,
                internal: o.internal,
                route: o.route,
                error: None,
            });
            r.refs += 1;
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
        if !self.quarantine.is_empty() {
            return Err(UNKNOWN);
        }
        let refs = self.regs.iter().map(|(&k, r)| (k, r.refs)).collect();
        self.release(e, &refs)?;
        let keys: Vec<_> = self.alloc.keys().copied().collect();
        for k in keys {
            self.free(e, k as *mut c_void)?;
        }
        Ok(())
    }
}
