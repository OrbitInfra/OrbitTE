//! Cooperative whole-allocation exports and owned CUDA IPC imports.

use super::*;
use crate::memory::Allocation;

mod copy;
pub(crate) use copy::IpcCopy;

pub(crate) struct IpcExport {
    memory: Option<Arc<CudaMemory>>,
    pub(crate) handle: [u8; 64],
    pub(crate) extent: usize,
    exposed: bool,
}

impl IpcExport {
    pub(crate) fn new(memory: Arc<CudaMemory>) -> Result<Self> {
        let mut allocation = memory
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        let extent = allocation
            .ipc_extent
            .ok_or(Error::Unavailable("CUDA IPC export requires allocate_ipc"))?;
        allocation.check_dma(&(0..memory.length))?;
        allocation
            .dma
            .try_reserve(1)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut handle = sys::CUipcMemHandle { reserved: [0; 64] };
        allocation.context.current(|| {
            // SAFETY: only an initialized cuMemAlloc base with explicit export
            // consent reaches here; the whole allocation remains owned.
            check("cuIpcGetMemHandle", unsafe {
                (allocation.context.driver.ipc_export)(&mut handle, allocation.address)
            })
        })?;
        allocation.dma.push(0..memory.length);
        drop(allocation);
        Ok(Self {
            memory: Some(memory),
            handle: handle.reserved.map(|b| b as u8),
            extent,
            exposed: false,
        })
    }

    /// Call before the first socket write that can reveal any handle bytes.
    pub(crate) fn expose(&mut self) {
        self.exposed = true;
    }

    /// The peer receipt proves its matching import was closed after physical drain.
    pub(crate) fn finish(mut self) {
        self.exposed = false;
    }
}

impl Drop for IpcExport {
    fn drop(&mut self) {
        if let Some(memory) = self.memory.take() {
            if self.exposed {
                RETAINED_EXPORT.fetch_add(1, Ordering::AcqRel);
                std::mem::forget(memory);
            } else {
                memory
                    .allocation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .dma
                    .retain(|range| *range != (0..memory.length));
            }
        }
    }
}

pub(crate) struct IpcMapping {
    context: Arc<Context>,
    address: u64,
    pending: bool,
    uncertain: bool,
    memories: Vec<Allocation>,
}

impl IpcMapping {
    pub(crate) fn new(memory: &Allocation) -> Result<Self> {
        let context = memory.ipc_owner_context()?;
        Ok(Self {
            context,
            address: 0,
            pending: false,
            uncertain: false,
            memories: Vec::new(),
        })
    }

    pub(crate) fn open(&mut self, handle: [u8; 64]) -> Result<()> {
        if self.address != 0 || self.uncertain {
            return Err(Error::Internal("CUDA IPC mapping already opened"));
        }
        let context = Arc::clone(&self.context);
        context.current(|| {
            // SAFETY: the cooperative exporter retains this allocation until
            // our close receipt. Install the output before checking restoration.
            check("cuIpcOpenMemHandle", unsafe {
                (context.driver.ipc_open)(
                    &mut self.address,
                    sys::CUipcMemHandle {
                        reserved: handle.map(|b| b as _),
                    },
                    sys::CUipcMem_flags::CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS as u32,
                )
            })?;
            if self.address == 0 {
                return Err(Error::Internal("CUDA returned a null IPC mapping"));
            }
            Ok(())
        })
    }

    /// Release completed local copy owners while retaining the remote import.
    /// A later import-close failure cannot retroactively invalidate these copies.
    pub(crate) fn finish_copies(&mut self) -> Result<()> {
        if self.pending || self.uncertain || self.context.poisoned.load(Ordering::Acquire) {
            return Err(Error::Unquiesced {
                operation: "CUDA IPC cached copies",
                code: -1,
            });
        }
        self.memories.clear();
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        let result = self.close_mapping();
        if result.is_err() {
            self.uncertain = true;
        } else {
            self.memories.clear();
        }
        result
    }

    fn close_mapping(&mut self) -> Result<()> {
        if self.pending || self.uncertain || self.context.poisoned.load(Ordering::Acquire) {
            return Err(Error::Unquiesced {
                operation: "CUDA IPC mapping",
                code: -1,
            });
        }
        if self.address == 0 {
            return Ok(());
        }
        let context = Arc::clone(&self.context);
        context
            .current(|| {
                // SAFETY: this owns exactly one successful open reference. No copy
                // remains; successful close is distinct from the copy completion.
                check("cuIpcCloseMemHandle", unsafe {
                    (context.driver.ipc_close)(self.address)
                })?;
                self.address = 0;
                Ok(())
            })
            .map_err(|error| match error {
                Error::Backend { operation, code } | Error::Unquiesced { operation, code } => {
                    Error::Unquiesced { operation, code }
                }
                _ => Error::Unquiesced {
                    operation: "CUDA IPC close",
                    code: -1,
                },
            })
    }
}

impl Drop for IpcMapping {
    fn drop(&mut self) {
        // An explicit close is required before sending a receipt. Drop cannot
        // turn an uncertain import or copy into a successful release.
        if self.address != 0 || self.uncertain {
            if self.address != 0 {
                RETAINED_IPC.fetch_add(1, Ordering::AcqRel);
            }
            for memory in &self.memories {
                match memory {
                    Allocation::Cuda(memory) => {
                        memory
                            .allocation
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .uncertain = true
                    }
                    Allocation::Host(memory) => memory.fence_cuda(),
                }
            }
            std::mem::forget(std::mem::take(&mut self.memories));
            std::mem::forget(Arc::clone(&self.context));
        }
    }
}

impl CudaMemory {
    pub(crate) fn validate_ipc_requester(&self) -> Result<()> {
        let allocation = self
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
        if !allocation.owned {
            return Err(Error::Unavailable(
                "CUDA IPC requester requires owned CudaMemory",
            ));
        }
        allocation.check_dma(&(0..self.length))
    }
}

impl Allocation {
    pub(super) fn ipc_owner_context(&self) -> Result<Arc<Context>> {
        match self {
            Self::Cuda(memory) => {
                memory.validate_ipc_requester()?;
                let allocation = memory
                    .allocation
                    .lock()
                    .map_err(|_| Error::Internal("CUDA memory lock poisoned"))?;
                Ok(Arc::clone(&allocation.context))
            }
            Self::Host(memory) => Ok(Arc::clone(&memory.cuda_pinned()?.context)),
        }
    }

    /// Actual retained native context identity, including portable pinned owners.
    pub(crate) fn ipc_context(&self) -> Result<usize> {
        Ok(self.ipc_owner_context()?.raw.as_ptr() as usize)
    }
}
