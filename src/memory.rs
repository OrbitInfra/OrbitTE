use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::ptr::NonNull;
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
/// This owner does not pin pages or register them with an RDMA provider.
pub struct HostMemory {
    address: u64,
    length: usize,
    backing: Mutex<HostBacking>,
}

enum HostBacking {
    Owned(Box<[u8]>),
    External(ExternalMemory),
}

struct ExternalMemory {
    address: NonNull<u8>,
    _owner: Arc<dyn Send + Sync>,
}

// SAFETY: from_raw_parts requires a stable, host-accessible allocation whose
// owner can be retained/released on any thread. All dereferences hold the shared
// HostMemory lock; the caller must coordinate external CPU/device access.
#[allow(unsafe_code)]
unsafe impl Send for ExternalMemory {}

impl fmt::Debug for HostMemory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostMemory")
            .field("address", &self.address)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl HostMemory {
    pub fn new(bytes: Box<[u8]>) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::InvalidRange);
        }
        Ok(Self {
            address: bytes.as_ptr() as usize as u64,
            length: bytes.len(),
            backing: Mutex::new(HostBacking::Owned(bytes)),
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
    /// Coordinate all external CPU/device accesses with these copies; no live
    /// Rust reference may conflict with them. Concurrent aliases must share one
    /// `Arc<HostMemory>` instead of accessing through independent locks.
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
            backing: Mutex::new(HostBacking::External(ExternalMemory {
                address,
                _owner: owner,
            })),
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

    #[allow(unsafe_code)]
    pub fn read(&self, offset: usize, destination: &mut [u8]) -> Result<()> {
        let range = checked_range(offset, destination.len(), self.length)?;
        let backing = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        match &*backing {
            HostBacking::Owned(bytes) => destination.copy_from_slice(&bytes[range]),
            HostBacking::External(memory) => {
                // SAFETY: the range is within the stable allocation promised by
                // from_raw_parts. The shared lock serializes all registered aliases;
                // the caller coordinates other accesses. The output slice is valid.
                unsafe {
                    std::ptr::copy(
                        memory.address.as_ptr().add(offset),
                        destination.as_mut_ptr(),
                        destination.len(),
                    );
                }
            }
        }
        Ok(())
    }

    #[allow(unsafe_code)]
    pub fn write(&self, offset: usize, source: &[u8]) -> Result<()> {
        let range = checked_range(offset, source.len(), self.length)?;
        let mut backing = self
            .backing
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        match &mut *backing {
            HostBacking::Owned(bytes) => bytes[range].copy_from_slice(source),
            HostBacking::External(memory) => {
                // SAFETY: the validated range, retained owner and shared lock
                // satisfy from_raw_parts' write contract. The input slice is valid.
                unsafe {
                    std::ptr::copy(
                        source.as_ptr(),
                        memory.address.as_ptr().add(offset),
                        source.len(),
                    );
                }
            }
        }
        Ok(())
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

pub(crate) struct Registration {
    pub(crate) descriptor: RemoteRegion,
    allocation: Arc<HostMemory>,
    allocation_offset: usize,
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
    allocation: Weak<HostMemory>,
    address: u64,
    end: u64,
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

    pub(crate) fn register(
        &self,
        bytes: Box<[u8]>,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        let length = bytes.len();
        self.register_host(Arc::new(HostMemory::new(bytes)?), 0, length, permission)
    }

    pub(crate) fn register_host(
        &self,
        allocation: Arc<HostMemory>,
        offset: usize,
        length: usize,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        checked_range(offset, length, allocation.len())?;
        let address = allocation
            .base_address()
            .checked_add(offset as u64)
            .ok_or(Error::InvalidRange)?;
        let end = address
            .checked_add(length as u64)
            .ok_or(Error::InvalidRange)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
        entries
            .retained
            .retain(|entry| entry.registration.strong_count() != 0);
        let allocation_ref = Arc::downgrade(&allocation);
        for previous in &entries.retained {
            if address < previous.end
                && previous.address < end
                && !Weak::ptr_eq(&previous.allocation, &allocation_ref)
            {
                return Err(Error::RegistrationConflict);
            }
        }
        if let Some(entry) = entries
            .regions
            .values_mut()
            .find(|entry| entry.memory.base_address() == address && entry.memory.len() == length)
        {
            entry.references = entry
                .references
                .checked_add(1)
                .ok_or(Error::ResourceExhausted)?;
            // TENT keeps the first descriptor/permission for exact duplicates.
            return Ok(entry.memory.clone());
        }
        let next_id = entries
            .next_id
            .checked_add(1)
            .ok_or(Error::ResourceExhausted)?;
        let mut usage = self
            .budget
            .lock()
            .map_err(|_| Error::Internal("memory budget lock poisoned"))?;
        let new_bytes = usage
            .bytes
            .checked_add(length)
            .ok_or(Error::ResourceExhausted)?;
        if usage.regions >= self.max_regions || new_bytes > self.max_bytes {
            return Err(Error::ResourceExhausted);
        }
        let memory = RegisteredMemory(Arc::new(Registration {
            descriptor: RemoteRegion {
                epoch: self.epoch,
                id: entries.next_id,
                address,
                length,
                permission,
            },
            allocation,
            allocation_offset: offset,
            budget: Arc::clone(&self.budget),
        }));
        usage.regions += 1;
        usage.bytes = new_bytes;
        entries.next_id = next_id;
        entries.retained.push(RetainedRange {
            registration: Arc::downgrade(&memory.0),
            allocation: allocation_ref,
            address,
            end,
        });
        entries.regions.insert(
            memory.id(),
            RegistryEntry {
                memory: memory.clone(),
                references: 1,
            },
        );
        Ok(memory)
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
