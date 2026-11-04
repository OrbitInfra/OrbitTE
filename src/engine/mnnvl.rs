use super::*;

impl TransferEngine {
    pub(crate) fn mnnvl_platform(&self) -> Result<&crate::cuda::fabric::Platform> {
        self.source
            .nvlink
            .as_ref()
            .and_then(|t| t.fabric.as_deref())
            .ok_or(Error::Unavailable("MNNVL is unavailable"))
    }
    /// Allocate initialized CUDA VMM storage exportable over the configured fabric.
    pub fn allocate_mnnvl(
        &self,
        device: i32,
        length: usize,
    ) -> Result<Arc<crate::cuda::CudaMemory>> {
        let _permit = self.source.admit()?;
        self.mnnvl_platform()?.allocate_device(device, length)
    }
    /// Allocate host-NUMA VMM storage on a socket with an attached, visible GPU.
    pub fn allocate_egm(&self, numa_node: u32, length: usize) -> Result<Arc<HostMemory>> {
        let _permit = self.source.admit()?;
        self.mnnvl_platform()?.allocate_egm(numa_node, length)
    }
    /// Release the last MNNVL device allocation handle with a checked native receipt.
    pub fn free_mnnvl(&self, memory: Arc<crate::cuda::CudaMemory>) -> Result<()> {
        self.mnnvl_platform()?.free_device(memory)
    }
    /// Release the last EGM host allocation handle, including the rounded VA range.
    pub fn free_egm(&self, memory: Arc<HostMemory>) -> Result<()> {
        self.mnnvl_platform()?.free_host(memory)
    }
    /// Wrap an externally owned, initialized GPU VMM mapping for cooperative fabric export.
    ///
    /// # Safety
    /// `address` must be the base of one offset-zero VMM physical allocation of exactly
    /// `extent` bytes, writable by the visible devices. The caller consents to sharing
    /// the entire initialized extent, including padding. The supplied owner keeps the
    /// mapping and access rights alive and unchanged until the last receipt. Coordinate
    /// all external access, and use the same returned Arc for every alias.
    #[allow(unsafe_code)]
    pub unsafe fn wrap_mnnvl(
        &self,
        device: i32,
        address: u64,
        length: usize,
        extent: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Arc<crate::cuda::CudaMemory>> {
        let _permit = self.source.admit()?;
        // SAFETY: the caller supplies the complete mapping, sharing consent and lifetime above.
        unsafe {
            self.mnnvl_platform()?
                .wrap_device(device, address, length, extent, owner)
        }
    }
    /// Wrap an externally owned HOST_NUMA VMM allocation for local or remote EGM access.
    ///
    /// # Safety
    /// The conditions of `wrap_mnnvl` apply. CPU access must also be valid on the declared
    /// NUMA node for the entire mapping. No other wrapper may access the bytes concurrently.
    #[allow(unsafe_code)]
    pub unsafe fn wrap_egm(
        &self,
        node: u32,
        address: u64,
        length: usize,
        extent: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Arc<HostMemory>> {
        let _permit = self.source.admit()?;
        // SAFETY: the caller retains initialized host-accessible VMM pages and their access rights.
        unsafe {
            self.mnnvl_platform()?
                .wrap_host(node, address, length, extent, owner)
        }
    }
}
