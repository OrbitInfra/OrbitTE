use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

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

/// Owns the allocation. Clones and admitted operations keep its backing alive.
/// Local access is serialized with TCP copies; remote permissions do not restrict it.
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

    pub fn read(&self, offset: usize, destination: &mut [u8]) -> Result<()> {
        let range = checked_range(offset, destination.len(), self.len())?;
        let bytes = self
            .0
            .bytes
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        destination.copy_from_slice(&bytes[range]);
        Ok(())
    }

    pub fn write(&self, offset: usize, source: &[u8]) -> Result<()> {
        let range = checked_range(offset, source.len(), self.len())?;
        let mut bytes = self
            .0
            .bytes
            .lock()
            .map_err(|_| Error::Internal("memory lock poisoned"))?;
        bytes[range].copy_from_slice(source);
        Ok(())
    }
}

pub(crate) struct Registration {
    pub(crate) descriptor: RemoteRegion,
    bytes: Mutex<Box<[u8]>>,
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
    regions: BTreeMap<u64, RegisteredMemory>,
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
            }),
        }
    }

    pub(crate) fn register(
        &self,
        bytes: Box<[u8]>,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        if bytes.is_empty() {
            return Err(Error::InvalidRange);
        }
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
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
            .checked_add(bytes.len())
            .ok_or(Error::ResourceExhausted)?;
        if usage.regions >= self.max_regions || new_bytes > self.max_bytes {
            return Err(Error::ResourceExhausted);
        }
        let memory = RegisteredMemory(Arc::new(Registration {
            descriptor: RemoteRegion {
                epoch: self.epoch,
                id: entries.next_id,
                length: bytes.len(),
                permission,
            },
            bytes: Mutex::new(bytes),
            budget: Arc::clone(&self.budget),
        }));
        usage.regions += 1;
        usage.bytes = new_bytes;
        entries.next_id = next_id;
        entries.regions.insert(memory.id(), memory.clone());
        Ok(memory)
    }

    pub(crate) fn unregister(&self, memory: &RegisteredMemory) -> Result<()> {
        self.check_owner(memory)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("registry lock poisoned"))?;
        entries
            .regions
            .remove(&memory.id())
            .ok_or(Error::RegionNotFound)?;
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
            .cloned()
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
            .filter(|m| m.0.descriptor.permission != Permission::LocalOnly)
            .map(|m| m.0.descriptor)
            .collect())
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
