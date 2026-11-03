use super::*;
use crate::RegisteredMemory;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::sync::atomic::AtomicBool;

pub(crate) struct FileHandle {
    driver: Arc<Driver>,
    fd: Arc<File>,
    pub(crate) raw: usize,
    pub(crate) mode: GdsMode,
    pub(crate) pid: u32,
    attempted: bool,
    failed: bool,
}

impl FileHandle {
    pub(crate) fn new(driver: &Arc<Driver>, fd: Arc<File>, mode: GdsMode) -> Result<Self> {
        let raw = driver.api.register_file(fd.as_raw_fd()).inspect_err(|e| {
            if matches!(e, Error::Unquiesced { .. }) {
                std::mem::forget((fd.clone(), driver.clone()));
            }
        })?;
        Ok(Self {
            driver: driver.clone(),
            fd,
            raw,
            mode,
            pid: driver.api.pid(),
            attempted: false,
            failed: false,
        })
    }
    pub(crate) fn close(&mut self) -> Result<()> {
        if self.failed {
            return Err(Error::Unquiesced {
                operation: "cuFileHandleDeregister",
                code: -1,
            });
        }
        if self.attempted || self.pid != self.driver.api.pid() {
            return Ok(());
        }
        self.attempted = true;
        self.driver.api.deregister_file(self.raw).map_err(|e| {
            self.failed = true;
            std::mem::forget((self.fd.clone(), self.driver.clone()));
            uncertain("cuFileHandleDeregister", &e)
        })
    }
}
impl Drop for FileHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct NativeBuffer {
    driver: Arc<Driver>,
    memory: Arc<crate::cuda::CudaMemory>,
    base: usize,
    pid: u32,
}

#[derive(Default)]
pub(crate) struct BufferSlot {
    native: Option<NativeBuffer>,
    users: usize,
    retired: bool,
    failed: bool,
}

impl BufferSlot {
    pub(crate) fn close(&mut self) -> Result<()> {
        if self.failed {
            return Err(Error::Unquiesced {
                operation: "cuFileBufDeregister",
                code: -1,
            });
        }
        let Some(native) = self.native.take() else {
            return Ok(());
        };
        if native.pid != native.driver.api.pid() {
            return Ok(());
        }
        native
            .memory
            .gds_current(|| native.driver.api.deregister_buffer(native.base))
            .map_err(|e| {
                self.failed = true;
                native.memory.fence_gds();
                std::mem::forget(native);
                uncertain("cuFileBufDeregister", &e)
            })
    }
}

impl GdsTransport {
    pub(crate) fn register(&self, memory: &RegisteredMemory) -> Result<()> {
        if memory.memory_type() != crate::MemoryType::Cuda || self.mode().is_err() {
            return Ok(());
        }
        let (gpu, _) = memory.cuda_range(0, memory.len())?;
        let base = memory.base_address() as usize;
        let mut slot = memory
            .0
            .gds
            .lock()
            .map_err(|_| Error::Internal("GDS buffer slot poisoned"))?;
        let result = gpu.gds_current(|| self.driver.api.register_buffer(base, memory.len()));
        if matches!(result, Err(Error::Unquiesced { .. })) {
            slot.failed = true;
            gpu.fence_gds();
            std::mem::forget((memory.clone(), self.driver.clone()));
        }
        result?;
        slot.native = Some(NativeBuffer {
            driver: self.driver.clone(),
            memory: gpu,
            base,
            pid: self.driver.api.pid(),
        });
        Ok(())
    }
}

pub(crate) struct BufferLease {
    memory: RegisteredMemory,
    active: AtomicBool,
}
impl BufferLease {
    pub(crate) fn acquire(memory: &RegisteredMemory) -> Result<Option<Self>> {
        if memory.memory_type() != crate::MemoryType::Cuda {
            return Ok(None);
        }
        let mut slot = memory
            .0
            .gds
            .lock()
            .map_err(|_| Error::Internal("GDS buffer slot poisoned"))?;
        if slot.failed
            || slot.retired
            || slot
                .native
                .as_ref()
                .is_none_or(|n| n.pid != n.driver.api.pid())
        {
            return Err(Error::Unavailable(
                "CUDA buffer is not registered with this GDS process",
            ));
        }
        slot.users = slot.users.checked_add(1).ok_or(Error::ResourceExhausted)?;
        Ok(Some(Self {
            memory: memory.clone(),
            active: AtomicBool::new(true),
        }))
    }
    pub(crate) fn retire(memory: &RegisteredMemory) -> Result<()> {
        let mut slot = memory
            .0
            .gds
            .lock()
            .map_err(|_| Error::Internal("GDS buffer slot poisoned"))?;
        if slot.failed {
            return Err(Error::Unquiesced {
                operation: "cuFileBufDeregister",
                code: -1,
            });
        }
        slot.retired = true;
        if slot.users == 0 {
            return slot
                .close()
                .inspect_err(|_| std::mem::forget(memory.clone()));
        }
        Ok(())
    }
    pub(crate) fn release(&self) -> Result<()> {
        let mut slot = self
            .memory
            .0
            .gds
            .lock()
            .map_err(|_| Error::Internal("GDS buffer slot poisoned"))?;
        if self.active.swap(false, Ordering::AcqRel) {
            slot.users -= 1;
            if slot.users == 0 && slot.retired {
                return slot
                    .close()
                    .inspect_err(|_| std::mem::forget(self.memory.clone()));
            }
        }
        if slot.failed {
            return Err(Error::Unquiesced {
                operation: "cuFileBufDeregister",
                code: -1,
            });
        }
        Ok(())
    }
}
impl Drop for BufferLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}
