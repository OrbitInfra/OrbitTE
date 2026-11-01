use std::sync::MutexGuard;

use super::*;
use crate::cuda::CUDAStreamHandle;
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

/// In-flight group ownership can move between completion polls without memory locks.
pub(crate) struct IpcCopyGroup {
    stream: CUDAStreamHandle,
    // Unique whole-allocation loans; the flag distinguishes touched owners.
    loans: Vec<(Allocation, bool)>,
    pending: bool,
    pub(crate) attempted: usize,
    submitted: usize,
    submitted_all: bool,
}

impl IpcCopyGroup {
    pub(crate) fn query(&mut self) -> Result<bool> {
        let ready = self.stream.stream()?.query()?;
        if ready {
            self.pending = false;
        }
        Ok(ready)
    }

    /// Consume the group, releasing proven loans/stream before publishing completion.
    pub(crate) fn complete(self, result: Result<()>) -> IpcCopyCompletion {
        let result = if self.pending && !matches!(result, Err(Error::Unquiesced { .. })) {
            Err(Error::Unquiesced {
                operation: "CUDA IPC completion before drain",
                code: -1,
            })
        } else {
            result
        };
        let completed = if result.is_ok()
            || (!self.submitted_all
                && !self.pending
                && !matches!(result, Err(Error::Unquiesced { .. })))
        {
            self.submitted
        } else {
            0
        };
        IpcCopyCompletion {
            completed,
            attempted: self.attempted,
            result,
        }
    }
}

impl Drop for IpcCopyGroup {
    fn drop(&mut self) {
        for (memory, touched) in self.loans.drain(..) {
            let retain = self.pending && touched;
            match &memory {
                Allocation::Cuda(memory) => {
                    let mut allocation =
                        memory.allocation.lock().unwrap_or_else(|p| p.into_inner());
                    if retain {
                        allocation.uncertain = true;
                    } else {
                        allocation.ipc_inflight = false;
                        allocation.dma.retain(|range| *range != (0..memory.length));
                    }
                    memory.ipc_idle.notify_all();
                }
                Allocation::Host(memory) => {
                    if retain {
                        memory.fence_cuda();
                    } else {
                        memory.release_ipc();
                    }
                }
            }
            if retain {
                std::mem::forget(memory);
            }
        }
    }
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
        stream: CUDAStreamHandle,
        deadline: Instant,
        mut begin: impl FnMut(usize) -> Result<()>,
        progress: impl FnOnce(IpcCopyGroup) -> IpcCopyCompletion,
    ) -> IpcCopyCompletion {
        let mut group = IpcCopyGroup {
            stream,
            loans: Vec::new(),
            pending: false,
            attempted: 0,
            submitted: 0,
            submitted_all: false,
        };
        let result = (|| {
            if copies.is_empty() {
                return Err(Error::Internal("empty CUDA IPC group"));
            }
            let stream = group.stream.stream()?;
            if stream.context_key() != self.context.raw.as_ptr() as usize {
                return Err(Error::Unavailable("CUDA stream belongs to another context"));
            }
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
            let mut guards = Vec::new();
            guards
                .try_reserve(owners.len())
                .map_err(|_| Error::ResourceExhausted)?;
            for memory in &owners {
                let access = match memory {
                    Allocation::Cuda(memory) => {
                        let mut allocation = memory.lock_ipc(deadline)?;
                        if !allocation.owned {
                            return Err(Error::Unavailable(
                                "CUDA IPC requester requires owned CudaMemory",
                            ));
                        }
                        allocation.prepare_access(&(0..memory.length))?;
                        CopyAccess::Device(allocation)
                    }
                    Allocation::Host(memory) => {
                        CopyAccess::Host(memory.lock_cuda_pinned(deadline)?)
                    }
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
            group
                .loans
                .try_reserve(owners.len())
                .map_err(|_| Error::ResourceExhausted)?;
            for access in &mut guards {
                if let CopyAccess::Device(allocation) = access {
                    allocation
                        .dma
                        .try_reserve(1)
                        .map_err(|_| Error::ResourceExhausted)?;
                }
            }
            let mut batch = if copies.len() > 1 && self.context.driver.copy_batch.is_some() {
                super::batch::CopyBatch::prepare(copies, self.address, to_remote)?
            } else {
                None
            };
            // Commit all loans while the common lock order excludes competing access.
            for (memory, access) in owners.iter().zip(&mut guards) {
                match access {
                    CopyAccess::Device(allocation) => {
                        allocation.ipc_inflight = true;
                        allocation.dma.push(0..memory.len());
                    }
                    CopyAccess::Host(access) => access.begin_ipc(),
                }
                group.loans.push(((*memory).clone(), false));
            }
            // Preserve native ownership if submission unwinds.
            #[allow(unused_assignments)]
            {
                self.pending = true;
                group.pending = true;
            }
            // SAFETY: all actual local owners are locked, and the source grant
            // retains remote bytes. Guards protect loan publication and enqueue;
            // group loans survive every subsequent poll and context restoration.
            let result = unsafe {
                stream.begin_group(|driver, stream| {
                    let mut admission = Ok(());
                    for (index, copy) in copies.iter().enumerate() {
                        if let Err(error) = begin(index) {
                            admission = Err(error);
                            break;
                        }
                        self.memories.push(copy.memory.clone());
                        group.attempted = index + 1;
                        let owner = owners
                            .binary_search_by_key(&owner_key(&copy.memory), |m| owner_key(m))
                            .map_err(|_| Error::Internal("CUDA IPC copy owner missing"))?;
                        group.loans[owner].1 = true;
                        if batch.is_some() {
                            continue;
                        }
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
                        group.submitted += 1;
                    }
                    if let Some(batch) = &mut batch {
                        if group.attempted != 0 {
                            // A failed batch may have enqueued any subset. Do not
                            // replay it or invent a completed prefix after drain.
                            batch.enqueue(
                                driver
                                    .copy_batch
                                    .ok_or(Error::Internal("CUDA batch entry missing"))?,
                                group.attempted,
                                stream,
                            )?;
                            group.submitted = group.attempted;
                        }
                    }
                    admission?;
                    group.submitted_all = true;
                    Ok(())
                })
            };
            group.pending = !stream.quiescent();
            self.pending = group.pending;
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
        let completion = match result {
            Ok(()) => progress(group),
            Err(error) => group.complete(Err(error)),
        };
        self.pending = matches!(completion.result, Err(Error::Unquiesced { .. }));
        self.uncertain |= self.pending;
        completion
    }
}
