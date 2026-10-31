use std::time::Instant;

use super::budget::MappingPermit;
use crate::Result;
use crate::cuda::ipc::IpcMapping;
use crate::memory::Allocation;

/// One relocation/import reference, charged until native close is proven.
pub(super) struct OpenedMapping {
    pub(super) mapping: IpcMapping,
    permit: Option<MappingPermit>,
}

impl OpenedMapping {
    pub(super) fn new(
        memory: &Allocation,
        permit: MappingPermit,
        deadline: Instant,
    ) -> Result<Self> {
        Ok(Self {
            mapping: IpcMapping::new(memory, deadline)?,
            permit: Some(permit),
        })
    }

    pub(super) fn close(mut self) -> Result<()> {
        self.mapping.close()?;
        self.permit.take();
        Ok(())
    }
}

impl Drop for OpenedMapping {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.retain();
        }
    }
}
