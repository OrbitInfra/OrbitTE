use std::sync::MutexGuard;

use super::*;
use crate::memory::CudaHostAccess;

/// A resolved copy keeps the actual allocation owner, including alias offsets.
pub(crate) struct IpcCopy {
    pub(crate) memory: Allocation,
    pub(crate) local_offset: usize,
    pub(crate) remote_offset: usize,
    pub(crate) length: usize,
}

pub(crate) struct IpcCopyCompletion {
    pub(crate) completed: usize,
    pub(crate) attempted: usize,
    pub(crate) result: Result<()>,
}

enum CopyAccess<'a> {
    Device(MutexGuard<'a, DeviceAllocation>),
    Host(CudaHostAccess<'a>),
}

impl CopyAccess<'_> {
    fn context(&self) -> Result<&Arc<Context>> {
        match self {
            Self::Device(allocation) => Ok(&allocation.context),
            Self::Host(access) => Ok(&access.allocation()?.context),
        }
    }

    fn with_stream(&mut self, copy: impl FnOnce(&mut CopyStream) -> Result<()>) -> Result<()> {
        match self {
            Self::Device(allocation) => {
                if allocation.copy_stream.is_none() {
                    allocation.copy_stream = Some(CopyStream::new(&allocation.context)?);
                }
                copy(
                    allocation
                        .copy_stream
                        .as_mut()
                        .ok_or(Error::Internal("CUDA stream missing"))?,
                )
            }
            Self::Host(access) => {
                let allocation = access.allocation()?;
                let mut slot = allocation
                    .copy_stream
                    .lock()
                    .map_err(|_| Error::Unquiesced {
                        operation: "CUDA pinned copy stream",
                        code: -1,
                    })?;
                if slot.is_none() {
                    *slot = Some(CopyStream::new(&allocation.context)?);
                }
                copy(
                    slot.as_mut()
                        .ok_or(Error::Internal("CUDA pinned stream missing"))?,
                )
            }
        }
    }

    fn fence(&mut self) {
        match self {
            Self::Device(allocation) => allocation.uncertain = true,
            Self::Host(access) => access.fence(),
        }
    }
}

fn owner_key(memory: &Allocation) -> usize {
    match memory {
        Allocation::Cuda(memory) => Arc::as_ptr(memory) as usize,
        Allocation::Host(memory) => Arc::as_ptr(memory) as usize,
    }
}

impl IpcMapping {
    pub(crate) fn copy_group(
        &mut self,
        copies: &[IpcCopy],
        to_remote: bool,
        mut begin: impl FnMut(usize) -> Result<()>,
    ) -> IpcCopyCompletion {
        let mut attempted = 0;
        let mut submitted = 0;
        let mut submitted_all = false;
        let result = (|| {
            let first = copies
                .first()
                .ok_or(Error::Internal("empty CUDA IPC group"))?;
            if self.address == 0 || self.pending || self.uncertain {
                return Err(Error::Unavailable("CUDA IPC import is not ready"));
            }
            if self.context.poisoned.load(Ordering::Acquire) {
                return Err(Error::Unquiesced {
                    operation: "CUDA IPC context restoration",
                    code: -1,
                });
            }
            let mut owners = Vec::new();
            owners
                .try_reserve(copies.len())
                .map_err(|_| Error::ResourceExhausted)?;
            for copy in copies {
                checked_range(copy.local_offset, copy.length, copy.memory.len())?;
                self.address
                    .checked_add(copy.remote_offset as u64)
                    .and_then(|address| address.checked_add(copy.length as u64))
                    .ok_or(Error::InvalidRange)?;
                owners.push(&copy.memory);
            }
            // Every group uses one order across CUDA and pinned host owners.
            // Aliases share an owner and must never recursively lock it.
            owners.sort_unstable_by_key(|memory| owner_key(memory));
            owners.dedup_by_key(|memory| owner_key(memory));
            let stream_owner = owners
                .binary_search_by_key(&owner_key(&first.memory), |m| owner_key(m))
                .map_err(|_| Error::Internal("CUDA stream owner missing"))?;
            let mut guards = Vec::new();
            guards
                .try_reserve(owners.len())
                .map_err(|_| Error::ResourceExhausted)?;
            for memory in &owners {
                let access = match memory {
                    Allocation::Cuda(memory) => {
                        let allocation = memory
                            .allocation
                            .lock()
                            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
                        if !allocation.owned {
                            return Err(Error::Unavailable(
                                "CUDA IPC requester requires owned CudaMemory",
                            ));
                        }
                        allocation.check_dma(&(0..memory.length))?;
                        CopyAccess::Device(allocation)
                    }
                    Allocation::Host(memory) => CopyAccess::Host(memory.lock_cuda_pinned()?),
                };
                if access.context()?.raw != self.context.raw {
                    return Err(Error::Unavailable(
                        "CUDA IPC import belongs to another context",
                    ));
                }
                guards.push(access);
            }
            self.memories
                .try_reserve(copies.len())
                .map_err(|_| Error::ResourceExhausted)?;
            // Retain the stream owner even if stream construction partially fails.
            self.memories.push(first.memory.clone());
            let result = guards[stream_owner].with_stream(|stream| {
                // Preserve native ownership if submission unwinds.
                #[allow(unused_assignments)]
                {
                    self.pending = true;
                }
                // SAFETY: all actual local owners are locked, and the source grant
                // retains remote bytes. Guards outlive enqueue, drain and restoration.
                let result = unsafe {
                    stream.copy_group(|driver, stream| {
                        for (index, copy) in copies.iter().enumerate() {
                            begin(index)?;
                            if index != 0 {
                                self.memories.push(copy.memory.clone());
                            }
                            attempted = index + 1;
                            let remote = self.address + copy.remote_offset as u64;
                            let code = match &copy.memory {
                                Allocation::Cuda(memory) => {
                                    let local = memory.address + copy.local_offset as u64;
                                    let (destination, source) = if to_remote {
                                        (remote, local)
                                    } else {
                                        (local, remote)
                                    };
                                    (driver.copy_device)(destination, source, copy.length, stream)
                                }
                                Allocation::Host(memory) => {
                                    let host = (memory.base_address() + copy.local_offset as u64)
                                        as usize
                                        as *mut std::ffi::c_void;
                                    if to_remote {
                                        (driver.to_device)(
                                            remote,
                                            host.cast_const(),
                                            copy.length,
                                            stream,
                                        )
                                    } else {
                                        (driver.to_host)(host, remote, copy.length, stream)
                                    }
                                }
                            };
                            check("CUDA IPC async copy", code)?;
                            submitted += 1;
                        }
                        submitted_all = true;
                        Ok(())
                    })
                };
                self.pending = !stream.quiescent();
                result
            });
            if matches!(result, Err(Error::Unquiesced { .. })) {
                for (owner, access) in owners.iter().zip(&mut guards) {
                    if self
                        .memories
                        .iter()
                        .any(|memory| owner_key(memory) == owner_key(owner))
                    {
                        access.fence();
                    }
                }
            }
            result
        })();
        if matches!(result, Err(Error::Unquiesced { .. })) {
            self.uncertain = true;
        }
        let completed = if result.is_ok() || (!submitted_all && !self.pending && !self.uncertain) {
            submitted
        } else {
            0
        };
        IpcCopyCompletion {
            completed,
            attempted,
            result,
        }
    }
}
