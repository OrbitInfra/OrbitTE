use crate::cuda::ipc::IpcMapping;
use crate::engine::PreparedTransfer;
use crate::memory::Allocation;
use crate::{Result, Transport};

use super::budget::MappingPermit;

/// One relocation/import reference, charged until native close is proven.
pub(super) struct NVLinkSubBatch {
    pub(super) mapping: IpcMapping,
    permit: Option<MappingPermit>,
}

impl NVLinkSubBatch {
    pub(super) fn new(memory: &Allocation, permit: MappingPermit) -> Result<Self> {
        Ok(Self {
            mapping: IpcMapping::new(memory)?,
            permit: Some(permit),
        })
    }

    pub(super) fn close(mut self) -> Result<()> {
        self.mapping.close()?;
        self.permit.take();
        Ok(())
    }
}

impl Drop for NVLinkSubBatch {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.retain();
        }
    }
}

/// Preserve request order. Never enlarge source authority beyond one region or
/// its normal transfer limit, and never reuse a mapping on another CUDA context.
pub(super) fn subbatch_len(requests: &[PreparedTransfer], max_bytes: usize) -> usize {
    let first = &requests[0];
    let context = |request: &PreparedTransfer| {
        request
            .local
            .ipc_range(request.local_offset, request.length)?
            .0
            .ipc_context()
    };
    let Ok(first_context) = context(first) else {
        return 1;
    };
    let mut start = first.remote_offset;
    let mut end = start + first.length;
    let mut count = 1;
    for request in &requests[1..] {
        if request.transport != Transport::Nvlink
            || request.remote.id != first.remote.id
            || context(request).ok() != Some(first_context)
        {
            break;
        }
        let next_start = start.min(request.remote_offset);
        let next_end = end.max(request.remote_offset + request.length);
        if next_end - next_start > max_bytes {
            break;
        }
        start = next_start;
        end = next_end;
        count += 1;
    }
    count
}
