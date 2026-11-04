use super::{
    ShmConfig,
    mapping::{self, Budget, Local, Mapping},
    sys::{Namespace, Sys},
    wire::{Descriptor, Manifest, Name},
};
use crate::{
    Error, HostMemory, Permission, RegisteredMemory, RemoteRegion, Result, SourceEpoch, TransferOp,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(crate) struct Export {
    record: Descriptor,
    live: Option<File>,
    revoke_error: Option<i32>,
    _owner: RegisteredMemory,
    local: Arc<Local>,
}
impl Export {
    fn revoke(&mut self) -> Result<()> {
        if let Some(code) = self.revoke_error {
            return Err(Error::Unquiesced {
                operation: "SHM revoke",
                code,
            });
        }
        if let Some(fd) = &self.live {
            // Closing only our descriptor could leave the OFD locked in a
            // concurrently forked child until exec. Unlock both directions first.
            (self.local.mapping.sys.unlock)(fd, self.record.live(false), 2).map_err(|e| {
                let code = e.raw_os_error().unwrap_or(-1);
                self.revoke_error = Some(code);
                Error::Unquiesced {
                    operation: "SHM revoke",
                    code,
                }
            })?;
            drop(self.live.take());
        }
        Ok(())
    }
}
type Key = (Name, u64, u64, u64, bool);
#[derive(Default)]
struct Exports {
    closed: bool,
    entries: BTreeMap<u64, Export>,
}
#[derive(Default)]
struct Imports {
    open: bool,
    entries: HashMap<Key, Arc<Mapping>>,
    fault: Option<Error>,
}

pub(crate) struct ShmTransport {
    epoch: SourceEpoch,
    sys: &'static Sys,
    ns: Arc<Namespace>,
    local_budget: Arc<Budget>,
    peer_budget: Arc<Budget>,
    exports: Mutex<Exports>,
    imports: Mutex<Imports>,
}

impl ShmTransport {
    pub(crate) fn new(
        cfg: &ShmConfig,
        epoch: SourceEpoch,
        count: usize,
        bytes: usize,
    ) -> Result<Self> {
        #[cfg(test)]
        let sys = super::tests::sys();
        #[cfg(not(test))]
        let sys = &super::sys::NATIVE;
        Self::with_sys(cfg, epoch, count, bytes, sys)
    }
    pub(super) fn with_sys(
        cfg: &ShmConfig,
        epoch: SourceEpoch,
        count: usize,
        bytes: usize,
        sys: &'static Sys,
    ) -> Result<Self> {
        let ns = Namespace::new(&cfg.cxl_mount_path)?;
        mapping::reap_orphans(sys, &ns, count)?;
        Ok(Self {
            sys,
            ns,
            epoch,
            local_budget: Budget::new(count, bytes),
            peer_budget: Budget::new(count, bytes),
            exports: Mutex::default(),
            imports: Mutex::new(Imports {
                open: true,
                ..Default::default()
            }),
        })
    }
    pub(crate) fn allocate(&self, size: usize) -> Result<Arc<HostMemory>> {
        let local = Local::create(self.sys, &self.ns, &self.local_budget, self.epoch, size)?;
        Ok(Arc::new(HostMemory::from_shm(Arc::new(local))))
    }
    pub(crate) fn free(&self, memory: Arc<HostMemory>) -> Result<()> {
        if !memory.shared().is_some_and(|a| a.engine == self.epoch) {
            return Err(Error::ForeignRegistration);
        }
        self.sweep()?;
        Arc::try_unwrap(memory)
            .map_err(|_| Error::Unavailable("SHM allocation still has live owners"))?
            .free_shm()
    }

    fn shareable(&self, m: &RegisteredMemory) -> Option<(Arc<Local>, usize)> {
        m.shared()
            .filter(|(l, _)| m.permission() != Permission::LocalOnly && l.in_namespace(&self.ns))
    }
    #[cfg(feature = "metrics")]
    pub(crate) fn exports_memory(&self, m: &RegisteredMemory) -> bool {
        self.shareable(m).is_some()
    }
    pub(crate) fn prepare_exports(
        &self,
        registrations: &[RegisteredMemory],
    ) -> Result<Vec<Export>> {
        let mut pending = Vec::new();
        for m in registrations {
            let Some((local, offset)) = self.shareable(m) else {
                continue;
            };
            let (record, owner) = local.grant(m.id(), offset, m.permission())?;
            pending.push(Export {
                record,
                live: Some(owner),
                revoke_error: None,
                local,
                _owner: m.clone(),
            });
        }
        Ok(pending)
    }
    pub(crate) fn publish(&self, pending: Vec<Export>) {
        let mut exports = self.exports.lock().unwrap_or_else(|p| p.into_inner());
        if exports.closed {
            return;
        }
        for e in pending {
            exports.entries.insert(e.record.id, e);
        }
    }
    pub(crate) fn manifest(&self, regions: &[RemoteRegion]) -> Result<Manifest> {
        let exports = self
            .exports
            .lock()
            .map_err(|_| Error::Internal("SHM export table lock"))?;
        Ok(Manifest {
            cxl: self.ns.cxl(),
            entries: regions
                .iter()
                .filter_map(|r| {
                    exports
                        .entries
                        .get(&r.id())
                        .filter(|e| e.live.is_some() && e.revoke_error.is_none())
                        .map(|e| e.record)
                })
                .collect(),
        })
    }
    pub(crate) fn revoke(&self, id: u64) -> Result<()> {
        let mut table = self
            .exports
            .lock()
            .map_err(|_| Error::Internal("SHM export table lock"))?;
        if let Some(e) = table.entries.get_mut(&id) {
            e.revoke()?;
        }
        Self::sweep_locked(&mut table.entries)
    }
    fn sweep_locked(table: &mut BTreeMap<u64, Export>) -> Result<()> {
        let mut result = Ok(());
        table.retain(|_, e| {
            if let Some(code) = e.revoke_error {
                result = Err(Error::Unquiesced {
                    operation: "SHM revoke",
                    code,
                });
                return true;
            }
            if e.live.is_some() {
                return true;
            }
            match (e.local.mapping.sys.conflicts)(
                e.local.mapping.fd(),
                e.record.activity(),
                1,
                true,
            ) {
                Ok(held) => held,
                Err(err) => {
                    result = Err(Error::Unquiesced {
                        operation: "SHM grant retirement",
                        code: err.raw_os_error().unwrap_or(-1),
                    });
                    true
                }
            }
        });
        result
    }
    pub(crate) fn sweep(&self) -> Result<()> {
        let mut table = self
            .exports
            .lock()
            .map_err(|_| Error::Internal("SHM export table lock"))?;
        Self::sweep_locked(&mut table.entries)
    }
    pub(crate) fn fence_exports(&self) -> Result<()> {
        let mut table = self.exports.lock().unwrap_or_else(|p| p.into_inner());
        table.closed = true;
        let mut result = Ok(());
        for e in table.entries.values_mut() {
            result = result.and(e.revoke());
        }
        result
    }
    pub(crate) fn drain_exports(&self, deadline: Instant) -> Result<()> {
        loop {
            {
                let mut table = self
                    .exports
                    .lock()
                    .map_err(|_| Error::Internal("SHM export table lock"))?;
                Self::sweep_locked(&mut table.entries)?;
                if table.entries.is_empty() {
                    return Ok(());
                }
            }
            std::thread::sleep(crate::engine::remaining(deadline)?.min(Duration::from_millis(1)));
        }
    }

    pub(crate) fn prepare(
        &self,
        manifest: &Manifest,
        region: &RemoteRegion,
        op: TransferOp,
    ) -> Result<Arc<Lease>> {
        if manifest.cxl != self.ns.cxl() {
            return Err(Error::Unavailable("different SHM backing namespaces"));
        }
        let d = *manifest.get(region.id()).ok_or(Error::RegionNotFound)?;
        if !region.permission().allows(op == TransferOp::Write) {
            return Err(Error::PermissionDenied);
        }
        let write = op == TransferOp::Write;
        let key = (d.name, d.dev, d.ino, d.size, write);
        let mut imports = self
            .imports
            .lock()
            .map_err(|_| Error::Internal("SHM imports lock"))?;
        if let Some(e) = &imports.fault {
            return Err(e.for_report());
        }
        if !imports.open {
            return Err(Error::RequesterClosed);
        }
        let m = match imports.entries.get(&key) {
            Some(mapping) => mapping.clone(),
            None => {
                let size = usize::try_from(d.size).map_err(|_| Error::InvalidRange)?;
                let charge = loop {
                    match self.peer_budget.charge(size) {
                        Ok(charge) => break charge,
                        Err(Error::ResourceExhausted) if Self::evict(&mut imports)? => continue,
                        Err(error) => return Err(error),
                    }
                };
                let mapping = Arc::new(Mapping::import(self.sys, &self.ns, d, write, charge)?);
                imports.entries.insert(key, mapping.clone());
                mapping
            }
        };
        // A separate open description is the process-death receipt for this physical owner.
        // Enrol before checking the source's live lock so revocation cannot miss an admitted copy.
        let active = (self.sys.reopen)(m.fd(), false)?;
        (self.sys.lock)(&active, d.activity(), 1, false)?;
        if !(self.sys.conflicts)(&active, d.live(write), 1, false)? {
            if (self.sys.conflicts)(&active, d.live(!write), 1, false)? {
                return Err(Error::PermissionDenied);
            }
            return Err(Error::RegionNotFound);
        }
        Ok(Arc::new(Lease {
            mapping: m,
            offset: d.offset as usize,
            _active: active,
        }))
    }
    fn evict(imports: &mut Imports) -> Result<bool> {
        let Some(key) = imports
            .entries
            .iter()
            .find_map(|(k, m)| (Arc::strong_count(m) == 1).then_some(*k))
        else {
            return Ok(false);
        };
        let m = imports
            .entries
            .remove(&key)
            .ok_or(Error::Internal("SHM mapping cache"))?;
        let owner =
            Arc::try_unwrap(m).map_err(|_| Error::Internal("borrowed SHM mapping evicted"))?;
        owner
            .retire()
            .inspect_err(|e| imports.fault = Some(e.for_report()))?;
        Ok(true)
    }
    pub(crate) fn close_imports(&self, deadline: Instant) -> Result<()> {
        loop {
            {
                let mut imports = self
                    .imports
                    .lock()
                    .map_err(|_| Error::Internal("SHM imports lock"))?;
                imports.open = false;
                // Each eviction removes exactly one idle entry before attempting
                // munmap. Quarantine that entry on failure and drain independent ones.
                loop {
                    match Self::evict(&mut imports) {
                        Ok(true) | Err(Error::Unquiesced { .. }) => continue,
                        Ok(false) => break,
                        Err(error) => return Err(error),
                    }
                }
                if imports.entries.is_empty() {
                    return imports
                        .fault
                        .as_ref()
                        .map_or(Ok(()), |e| Err(e.for_report()));
                }
            }
            std::thread::sleep(crate::engine::remaining(deadline)?.min(Duration::from_millis(1)));
        }
    }
}
impl Drop for ShmTransport {
    fn drop(&mut self) {
        let fence = self.fence_exports();
        if let Err(e) =
            fence.and(self.drain_exports(Instant::now() + crate::transport::DESTRUCTION_TIMEOUT))
        {
            let table = self.exports.get_mut().unwrap_or_else(|p| p.into_inner());
            log::error!(
                "retaining {} SHM grants with unconfirmed peer copies: {e}",
                table.entries.len()
            );
            std::mem::forget(std::mem::take(&mut table.entries));
        }
    }
}

pub(crate) struct Lease {
    mapping: Arc<Mapping>,
    offset: usize,
    _active: File,
}
impl Lease {
    pub(crate) fn aliases(&self, local: &Local) -> bool {
        self.mapping.id.dev == local.mapping.id.dev && self.mapping.id.ino == local.mapping.id.ino
    }
    pub(crate) fn copy(
        &self,
        op: TransferOp,
        local: &RegisteredMemory,
        local_offset: usize,
        remote_offset: usize,
        len: usize,
        posted: &mut bool,
    ) -> Result<()> {
        let pos = self
            .offset
            .checked_add(remote_offset)
            .ok_or(Error::InvalidRange)?;
        let end = crate::memory::checked_range(pos, len, self.mapping.size)?.end;
        local.with_shm_access(local_offset, len, |ptr, start, backing| {
            let local_end = start + len;
            let alias = backing.filter(|b| self.aliases(b));
            let (_local, _peer, remote_ptr) = if let Some(b) = alias {
                let union = start.min(pos)..local_end.max(end);
                (Some(b.access(&union, true)?), None, b.address())
            } else {
                let a = backing
                    .map(|b| b.access(&(start..local_end), op == TransferOp::Read))
                    .transpose()?;
                let b = self.mapping.range_lock(pos, len, op == TransferOp::Write)?;
                (a, Some(b), self.mapping.address)
            };
            *posted = true;
            // SAFETY: checked ranges have equal length. Strong owners keep both mappings live,
            // HostAccess excludes native loans, and inode range locks cover cooperating processes.
            // Aliases use one virtual base so memmove also sees physical overlap after relocation.
            unsafe {
                let here = ptr.as_ptr().add(start);
                let there = remote_ptr.as_ptr().add(pos);
                match op {
                    TransferOp::Read => std::ptr::copy(there, here, len),
                    TransferOp::Write => std::ptr::copy(here, there, len),
                }
            }
            Ok(())
        })
    }
}
