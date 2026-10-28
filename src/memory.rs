use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::ptr::NonNull;
#[cfg(feature = "rdma")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::{Error, RemoteRegion, Result, SourceEpoch};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Permission {
    LocalOnly = 0,
    RemoteRead = 1,
    RemoteWrite = 2,
    RemoteReadWrite = 3,
}

impl Permission {
    pub(crate) fn allows(self, write: bool) -> bool {
        self as u8 & if write { 2 } else { 1 } != 0
    }

    pub(crate) fn decode(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::LocalOnly),
            1 => Ok(Self::RemoteRead),
            2 => Ok(Self::RemoteWrite),
            3 => Ok(Self::RemoteReadWrite),
            _ => Err(Error::Protocol("unknown permission bits")),
        }
    }
}

pub(crate) fn checked_range(offset: usize, length: usize, capacity: usize) -> Result<Range<usize>> {
    let end = offset.checked_add(length).ok_or(Error::InvalidRange)?;
    if length == 0 || end > capacity {
        return Err(Error::InvalidRange);
    }
    Ok(offset..end)
}

/// Stable host backing and its shared access lock. Use the same `Arc<HostMemory>`
/// for overlapping registrations, including registrations in different engines.
/// Direct RDMA leases exclude conflicting access until native teardown succeeds.
/// `HostMemory::new` does not pin pages. CUDA-pinned construction retains its
/// native allocation; RDMA registration remains a separate provider operation.
pub struct HostMemory {
    address: u64,
    length: usize,
    backing: Mutex<HostAccess>,
}

struct HostAccess {
    bytes: HostBacking,
    #[cfg(feature = "cuda")]
    cuda_uncertain: bool,
    #[cfg(feature = "rdma")]
    dma: Vec<Range<usize>>,
}

impl HostAccess {
    fn check_access(&self, range: &Range<usize>) -> Result<()> {
        #[cfg(feature = "cuda")]
        {
            if self.cuda_uncertain {
                return Err(Error::Unquiesced {
                    operation: "CUDA host memory",
                    code: -1,
                });
            }
            if let HostBacking::CudaPinned(allocation) = &self.bytes {
                allocation.check_access()?;
            }
        }
        #[cfg(feature = "rdma")]
        if self
            .dma
            .iter()
            .any(|loan| range.start < loan.end && loan.start < range.end)
        {
            return Err(Error::Unavailable("host range is owned by RDMA"));
        }
        #[cfg(not(feature = "rdma"))]
        let _ = range;
        Ok(())
    }
}

enum HostBacking {
    #[cfg(feature = "cuda")]
    CudaPinned(Arc<crate::cuda::PinnedAllocation>),
    Owned {
        address: NonNull<u8>,
        length: usize,
    },
    External {
        address: NonNull<u8>,
        _owner: Arc<dyn Send + Sync>,
    },
}

impl HostBacking {
    fn address(&self) -> NonNull<u8> {
        match self {
            Self::Owned { address, .. } | Self::External { address, .. } => *address,
            #[cfg(feature = "cuda")]
            Self::CudaPinned(allocation) => allocation.address,
        }
    }
}

// SAFETY: all CPU dereferences hold HostAccess's lock and reject overlapping
// native loans. External owners additionally promise stable host access on any
// thread. Raw owned backing avoids forming Rust slices across disjoint DMA.
#[allow(unsafe_code)]
unsafe impl Send for HostBacking {}

impl Drop for HostBacking {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        if let Self::Owned { address, length } = self {
            // SAFETY: this exact Box was consumed by HostMemory::new. Native
            // leases retain their actual owner until successful QP/MR teardown.
            unsafe {
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    address.as_ptr(),
                    *length,
                )));
            }
        }
    }
}

impl fmt::Debug for HostMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostMemory")
            .field("address", &self.address)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl HostMemory {
    #[cfg(feature = "cuda")]
    pub(crate) fn from_cuda_pinned(allocation: Arc<crate::cuda::PinnedAllocation>) -> Self {
        Self {
            address: allocation.address.as_ptr() as usize as u64,
            length: allocation.length,
            backing: Mutex::new(HostAccess {
                bytes: HostBacking::CudaPinned(allocation),
                cuda_uncertain: false,
                #[cfg(feature = "rdma")]
                dma: Vec::new(),
            }),
        }
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn cuda_pinned(&self) -> Result<Arc<crate::cuda::PinnedAllocation>> {
        let backing = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        backing.check_access(&(0..self.length))?;
        match &backing.bytes {
            HostBacking::CudaPinned(allocation) => Ok(Arc::clone(allocation)),
            _ => Err(Error::Unavailable(
                "NVLink host requester requires allocate_pinned_host",
            )),
        }
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn with_cuda_pinned<T>(
        &self,
        range: Range<usize>,
        operation: impl FnOnce(&crate::cuda::PinnedAllocation) -> Result<T>,
    ) -> Result<T> {
        let mut backing = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        backing.check_access(&range)?;
        let result = match &backing.bytes {
            HostBacking::CudaPinned(allocation) => operation(allocation),
            _ => Err(Error::Unavailable(
                "NVLink host requester requires allocate_pinned_host",
            )),
        };
        if matches!(result, Err(Error::Unquiesced { .. })) {
            backing.cuda_uncertain = true;
        }
        result
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn fence_cuda(&self) {
        self.backing
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cuda_uncertain = true;
    }

    pub fn new(bytes: Box<[u8]>) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::InvalidRange);
        }
        let length = bytes.len();
        let address = NonNull::new(Box::into_raw(bytes).cast::<u8>()).ok_or(Error::InvalidRange)?;
        Ok(Self {
            address: address.as_ptr() as usize as u64,
            length,
            backing: Mutex::new(HostAccess {
                bytes: HostBacking::Owned { address, length },
                #[cfg(feature = "cuda")]
                cuda_uncertain: false,
                #[cfg(feature = "rdma")]
                dma: Vec::new(),
            }),
        })
    }

    /// Retains an existing allocation without copying, pinning or taking over
    /// its deallocator. Registrations and admitted handlers retain this owner.
    ///
    /// # Safety
    /// `address..address+length` must be valid host memory, readable and writable
    /// on any engine thread, and stable until the last `HostMemory` is dropped.
    /// `owner` must keep that actual allocation alive, including after unregister
    /// or a timeout. No owner API may reallocate or free the retained range.
    /// Coordinate all external CPU/device accesses with these copies and native
    /// DMA leases; no live Rust reference may conflict with them. Concurrent
    /// aliases must share one `Arc<HostMemory>` instead of independent locks.
    /// A device pointer without host access does not satisfy this contract.
    #[allow(unsafe_code)]
    pub unsafe fn from_raw_parts(
        address: NonNull<u8>,
        length: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Self> {
        if length == 0
            || length > isize::MAX as usize
            || (address.as_ptr() as usize).checked_add(length).is_none()
        {
            return Err(Error::InvalidRange);
        }
        Ok(Self {
            address: address.as_ptr() as usize as u64,
            length,
            backing: Mutex::new(HostAccess {
                bytes: HostBacking::External {
                    address,
                    _owner: owner,
                },
                #[cfg(feature = "cuda")]
                cuda_uncertain: false,
                #[cfg(feature = "rdma")]
                dma: Vec::new(),
            }),
        })
    }

    pub fn base_address(&self) -> u64 {
        self.address
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Returns unavailable if a conflicting direct RDMA lease is still held.
    #[allow(unsafe_code)]
    pub fn read(&self, offset: usize, destination: &mut [u8]) -> Result<()> {
        let range = checked_range(offset, destination.len(), self.length)?;
        let backing = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        backing.check_access(&range)?;
        // SAFETY: the validated range has no conflicting DMA lease. The shared
        // lock serializes CPU copies and lease acquisition. External access is
        // coordinated by from_raw_parts' caller. No allocation-wide slice exists.
        unsafe {
            std::ptr::copy(
                backing.bytes.address().as_ptr().add(offset),
                destination.as_mut_ptr(),
                destination.len(),
            );
        }
        Ok(())
    }

    /// Returns unavailable if a conflicting direct RDMA lease is still held.
    #[allow(unsafe_code)]
    pub fn write(&self, offset: usize, source: &[u8]) -> Result<()> {
        let range = checked_range(offset, source.len(), self.length)?;
        let backing = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        backing.check_access(&range)?;
        // SAFETY: as for read; only this checked range is accessed under the
        // shared lock. Disjoint native leases do not conflict with this copy.
        unsafe {
            std::ptr::copy(
                source.as_ptr(),
                backing.bytes.address().as_ptr().add(offset),
                source.len(),
            );
        }
        Ok(())
    }
}

#[cfg(feature = "rdma")]
pub(crate) struct DmaLease {
    _registration: RegisteredMemory,
    allocation: Allocation,
    range: Range<usize>,
    address: NonNull<u8>,
}

#[cfg(feature = "rdma")]
impl DmaLease {
    pub(crate) fn registration(&self) -> RegisteredMemory {
        self._registration.clone()
    }
    pub(crate) fn complete(&self) -> Result<()> {
        match &self.allocation {
            Allocation::Host(_) => Ok(()),
            #[cfg(feature = "cuda")]
            Allocation::Cuda(memory) => memory.complete_dma(),
        }
    }

    pub(crate) fn address(&self) -> NonNull<u8> {
        self.address
    }
    pub(crate) fn len(&self) -> usize {
        self.range.len()
    }
}

#[cfg(feature = "rdma")]
impl Drop for DmaLease {
    fn drop(&mut self) {
        match &self.allocation {
            Allocation::Host(memory) => memory
                .backing
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .dma
                .retain(|range| *range != self.range),
            #[cfg(feature = "cuda")]
            Allocation::Cuda(memory) => memory.release_dma(&self.range),
        }
    }
}

/// One registered range. Clones and admitted operations retain its backing and
/// shared allocation lock. Cloning does not add an explicit registration reference.
#[derive(Clone)]
pub struct RegisteredMemory(pub(crate) Arc<Registration>);

impl fmt::Debug for RegisteredMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisteredMemory")
            .field("id", &self.id())
            .field("length", &self.len())
            .finish_non_exhaustive()
    }
}

impl RegisteredMemory {
    pub fn id(&self) -> u64 {
        self.0.descriptor.id
    }
    pub fn len(&self) -> usize {
        self.0.descriptor.length
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn base_address(&self) -> u64 {
        self.0.descriptor.address
    }
    pub fn permission(&self) -> Permission {
        self.0.descriptor.permission
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn is_active(&self) -> bool {
        self.0.active.load(Ordering::Acquire)
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn with_rdma_context<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        match &self.0.allocation {
            Allocation::Host(_) => operation(),
            #[cfg(feature = "cuda")]
            Allocation::Cuda(memory) => memory.with_rdma_context(operation),
        }
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn is_cuda(&self) -> bool {
        match &self.0.allocation {
            Allocation::Host(_) => false,
            #[cfg(feature = "cuda")]
            Allocation::Cuda(_) => true,
        }
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn dma_lease(&self, offset: usize, length: usize) -> Result<DmaLease> {
        checked_range(offset, length, self.len())?;
        let offset = self
            .0
            .allocation_offset
            .checked_add(offset)
            .ok_or(Error::InvalidRange)?;
        let range = checked_range(offset, length, self.0.allocation.len())?;
        let address =
            NonNull::new((self.0.allocation.base_address() + offset as u64) as usize as *mut u8)
                .ok_or(Error::InvalidRange)?;
        match &self.0.allocation {
            Allocation::Host(memory) => {
                let mut backing = memory
                    .backing
                    .lock()
                    .map_err(|_| Error::Internal("memory lock poisoned"))?;
                backing.check_access(&range)?;
                backing
                    .dma
                    .try_reserve(1)
                    .map_err(|_| Error::ResourceExhausted)?;
                backing.dma.push(range.clone());
            }
            #[cfg(feature = "cuda")]
            Allocation::Cuda(memory) => memory.acquire_dma(range.clone())?,
        }
        Ok(DmaLease {
            _registration: self.clone(),
            allocation: self.0.allocation.clone(),
            range,
            address,
        })
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn ipc_range(&self, offset: usize, length: usize) -> Result<(Allocation, usize)> {
        checked_range(offset, length, self.len())?;
        Ok((self.0.allocation.clone(), self.0.allocation_offset + offset))
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn cuda_range(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<(Arc<crate::cuda::CudaMemory>, usize)> {
        checked_range(offset, length, self.len())?;
        match &self.0.allocation {
            Allocation::Cuda(memory) => Ok((Arc::clone(memory), self.0.allocation_offset + offset)),
            Allocation::Host(_) => {
                Err(Error::Unavailable("NVLink IPC requires CUDA device memory"))
            }
        }
    }

    pub fn read(&self, offset: usize, destination: &mut [u8]) -> Result<()> {
        checked_range(offset, destination.len(), self.len())?;
        self.0
            .allocation
            .read(self.0.allocation_offset + offset, destination)
    }

    pub fn write(&self, offset: usize, source: &[u8]) -> Result<()> {
        checked_range(offset, source.len(), self.len())?;
        self.0
            .allocation
            .write(self.0.allocation_offset + offset, source)
    }
}

#[derive(Clone)]
pub(crate) enum Allocation {
    Host(Arc<HostMemory>),
    #[cfg(feature = "cuda")]
    Cuda(Arc<crate::cuda::CudaMemory>),
}

impl Allocation {
    fn base_address(&self) -> u64 {
        match self {
            Self::Host(memory) => memory.base_address(),
            #[cfg(feature = "cuda")]
            Self::Cuda(memory) => memory.base_address(),
        }
    }
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Host(memory) => memory.len(),
            #[cfg(feature = "cuda")]
            Self::Cuda(memory) => memory.len(),
        }
    }
    fn read(&self, offset: usize, destination: &mut [u8]) -> Result<()> {
        match self {
            Self::Host(memory) => memory.read(offset, destination),
            #[cfg(feature = "cuda")]
            Self::Cuda(memory) => memory.read(offset, destination),
        }
    }
    fn write(&self, offset: usize, source: &[u8]) -> Result<()> {
        match self {
            Self::Host(memory) => memory.write(offset, source),
            #[cfg(feature = "cuda")]
            Self::Cuda(memory) => memory.write(offset, source),
        }
    }
    fn weak(&self) -> AllocationRef {
        match self {
            Self::Host(memory) => AllocationRef::Host(Arc::downgrade(memory)),
            #[cfg(feature = "cuda")]
            Self::Cuda(memory) => AllocationRef::Cuda(Arc::downgrade(memory)),
        }
    }
}

enum AllocationRef {
    Host(Weak<HostMemory>),
    #[cfg(feature = "cuda")]
    Cuda(Weak<crate::cuda::CudaMemory>),
}
impl AllocationRef {
    fn ptr_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Host(a), Self::Host(b)) => Weak::ptr_eq(a, b),
            #[cfg(feature = "cuda")]
            (Self::Cuda(a), Self::Cuda(b)) => Weak::ptr_eq(a, b),
            #[cfg(feature = "cuda")]
            _ => false,
        }
    }
}

pub(crate) struct Registration {
    pub(crate) descriptor: RemoteRegion,
    allocation: Allocation,
    allocation_offset: usize,
    #[cfg(feature = "rdma")]
    active: AtomicBool,
    budget: Arc<Mutex<MemoryUsage>>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut usage = self
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        usage.regions -= 1;
        usage.bytes -= self.descriptor.length;
    }
}

#[derive(Debug, Default)]
struct MemoryUsage {
    regions: usize,
    bytes: usize,
}

pub(crate) struct MemoryRegistry {
    epoch: SourceEpoch,
    max_regions: usize,
    max_bytes: usize,
    budget: Arc<Mutex<MemoryUsage>>,
    entries: Mutex<RegistryEntries>,
}

struct RegistryEntries {
    next_id: u64,
    regions: BTreeMap<u64, RegistryEntry>,
    retained: Vec<RetainedRange>,
}

struct RetainedRange {
    registration: Weak<Registration>,
    allocation: AllocationRef,
    address: u64,
    end: u64,
}

struct RegistrationPlan {
    input: usize,
    address: u64,
    end: u64,
    id: u64,
    references: usize,
}

struct RegistryEntry {
    memory: RegisteredMemory,
    references: usize,
}

impl MemoryRegistry {
    pub(crate) fn new(epoch: SourceEpoch, max_regions: usize, max_bytes: usize) -> Self {
        Self {
            epoch,
            max_regions,
            max_bytes,
            budget: Arc::default(),
            entries: Mutex::new(RegistryEntries {
                next_id: 1,
                regions: BTreeMap::new(),
                retained: Vec::new(),
            }),
        }
    }

    pub(crate) fn register_host_batch(
        &self,
        ranges: &[(Arc<HostMemory>, usize, usize)],
        permission: Permission,
        publish: impl FnOnce(&[RegisteredMemory]) -> Result<()>,
    ) -> Result<Vec<RegisteredMemory>> {
        let mut allocations = Vec::new();
        allocations
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        for (allocation, offset, length) in ranges {
            allocations.push((Allocation::Host(Arc::clone(allocation)), *offset, *length));
        }
        self.register_batch(&allocations, permission, publish)
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn register_cuda_batch(
        &self,
        ranges: &[(Arc<crate::cuda::CudaMemory>, usize, usize)],
        permission: Permission,
        publish: impl FnOnce(&[RegisteredMemory]) -> Result<()>,
    ) -> Result<Vec<RegisteredMemory>> {
        let mut allocations = Vec::new();
        allocations
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        for (allocation, offset, length) in ranges {
            allocations.push((Allocation::Cuda(Arc::clone(allocation)), *offset, *length));
        }
        self.register_batch(&allocations, permission, publish)
    }

    fn register_batch(
        &self,
        ranges: &[(Allocation, usize, usize)],
        permission: Permission,
        publish: impl FnOnce(&[RegisteredMemory]) -> Result<()>,
    ) -> Result<Vec<RegisteredMemory>> {
        let mut plans: Vec<RegistrationPlan> = Vec::new();
        let mut selections = Vec::new();
        let mut registered: Vec<RegisteredMemory> = Vec::new();
        let mut unpublished = Vec::new();
        let mut result = Vec::new();
        plans
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        selections
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        unpublished
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        registered
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        result
            .try_reserve_exact(ranges.len())
            .map_err(|_| Error::ResourceExhausted)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
        entries
            .retained
            .retain(|entry| entry.registration.strong_count() != 0);
        let mut next_id = entries.next_id;
        let mut added_regions = 0usize;
        let mut added_bytes = 0usize;
        for (input, (allocation, offset, length)) in ranges.iter().enumerate() {
            checked_range(*offset, *length, allocation.len())?;
            let address = allocation
                .base_address()
                .checked_add(*offset as u64)
                .ok_or(Error::InvalidRange)?;
            let end = address
                .checked_add(*length as u64)
                .ok_or(Error::InvalidRange)?;
            let allocation_ref = allocation.weak();
            if entries.retained.iter().any(|previous| {
                address < previous.end
                    && previous.address < end
                    && !previous.allocation.ptr_eq(&allocation_ref)
            }) || plans.iter().any(|previous| {
                address < previous.end
                    && previous.address < end
                    && !ranges[previous.input].0.weak().ptr_eq(&allocation_ref)
            }) {
                return Err(Error::RegistrationConflict);
            }
            if let Some(index) = plans
                .iter()
                .position(|plan| plan.address == address && plan.end == end)
            {
                plans[index].references = plans[index]
                    .references
                    .checked_add(1)
                    .ok_or(Error::ResourceExhausted)?;
                selections.push(index);
                continue;
            }
            let existing = entries.regions.values().find(|entry| {
                entry.memory.base_address() == address && entry.memory.len() == *length
            });
            let (id, references) = if let Some(entry) = existing {
                (
                    entry.memory.id(),
                    entry
                        .references
                        .checked_add(1)
                        .ok_or(Error::ResourceExhausted)?,
                )
            } else {
                let id = next_id;
                next_id = next_id.checked_add(1).ok_or(Error::ResourceExhausted)?;
                added_regions += 1;
                added_bytes = added_bytes
                    .checked_add(*length)
                    .ok_or(Error::ResourceExhausted)?;
                (id, 1)
            };
            selections.push(plans.len());
            plans.push(RegistrationPlan {
                input,
                address,
                end,
                id,
                references,
            });
        }
        let mut usage = self
            .budget
            .lock()
            .map_err(|_| Error::Internal("memory budget lock poisoned"))?;
        let new_regions = usage
            .regions
            .checked_add(added_regions)
            .ok_or(Error::ResourceExhausted)?;
        let new_bytes = usage
            .bytes
            .checked_add(added_bytes)
            .ok_or(Error::ResourceExhausted)?;
        if new_regions > self.max_regions || new_bytes > self.max_bytes {
            return Err(Error::ResourceExhausted);
        }
        entries
            .retained
            .try_reserve(added_regions)
            .map_err(|_| Error::ResourceExhausted)?;

        // Unpublished owners are charged too: failed native cleanup can retain them.
        usage.regions = new_regions;
        usage.bytes = new_bytes;
        entries.next_id = next_id;
        drop(usage);
        for plan in &plans {
            if let Some(entry) = entries.regions.get(&plan.id) {
                registered.push(entry.memory.clone());
            } else {
                let (allocation, offset, length) = &ranges[plan.input];
                let memory = RegisteredMemory(Arc::new(Registration {
                    descriptor: RemoteRegion {
                        epoch: self.epoch,
                        id: plan.id,
                        address: plan.address,
                        length: *length,
                        permission,
                    },
                    allocation: allocation.clone(),
                    allocation_offset: *offset,
                    #[cfg(feature = "rdma")]
                    active: AtomicBool::new(true),
                    budget: Arc::clone(&self.budget),
                }));
                entries.retained.push(RetainedRange {
                    registration: Arc::downgrade(&memory.0),
                    allocation: allocation.weak(),
                    address: plan.address,
                    end: plan.end,
                });
                unpublished.push(memory.clone());
                registered.push(memory);
            }
        }
        publish(&unpublished).inspect_err(|_| {
            #[cfg(feature = "rdma")]
            for memory in &unpublished {
                memory.0.active.store(false, Ordering::Release);
            }
        })?;
        // Native preparation completed; lookup visibility and duplicate bumps commit together.
        for (plan, memory) in plans.iter().zip(&registered) {
            entries.regions.insert(
                plan.id,
                RegistryEntry {
                    memory: memory.clone(),
                    references: plan.references,
                },
            );
        }
        for index in selections {
            result.push(registered[index].clone());
        }
        Ok(result)
    }

    pub(crate) fn unregister(&self, memory: &RegisteredMemory) -> Result<()> {
        self.check_owner(memory)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
        let entry = entries
            .regions
            .get_mut(&memory.id())
            .ok_or(Error::RegionNotFound)?;
        entry.references -= 1;
        if entry.references == 0 {
            #[cfg(feature = "rdma")]
            entry.memory.0.active.store(false, Ordering::Release);
            entries.regions.remove(&memory.id());
        }
        Ok(())
    }

    pub(crate) fn check_owner(&self, memory: &RegisteredMemory) -> Result<()> {
        if memory.0.descriptor.epoch != self.epoch {
            return Err(Error::ForeignRegistration);
        }
        Ok(())
    }

    pub(crate) fn get(&self, id: u64) -> Result<RegisteredMemory> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
        entries
            .regions
            .get(&id)
            .map(|entry| entry.memory.clone())
            .ok_or(Error::RegionNotFound)
    }

    pub(crate) fn descriptors(&self) -> Result<Vec<RemoteRegion>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
        Ok(entries
            .regions
            .values()
            .filter(|entry| entry.memory.permission() != Permission::LocalOnly)
            .map(|entry| entry.memory.0.descriptor)
            .collect())
    }

    pub(crate) fn resolve_address(
        &self,
        address: u64,
        length: usize,
    ) -> Result<(RegisteredMemory, usize)> {
        if length == 0 || address.checked_add(length as u64).is_none() {
            return Err(Error::InvalidRange);
        }
        let entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
        entries
            .regions
            .values()
            .filter_map(|entry| {
                entry
                    .memory
                    .0
                    .descriptor
                    .offset_of(address, length)
                    .map(|offset| (entry.memory.clone(), offset))
            })
            .min_by_key(|(memory, _)| (memory.base_address(), Reverse(memory.len())))
            .ok_or(Error::RegionNotFound)
    }

    pub(crate) fn usage(&self) -> Result<(usize, usize)> {
        let usage = self
            .budget
            .lock()
            .map_err(|_| Error::Internal("memory budget lock poisoned"))?;
        Ok((usage.regions, usage.bytes))
    }
}

#[cfg(test)]
#[path = "../tests/unit/memory.rs"]
mod tests;
