//! Request-local buffers reuse their private registrations across chunk phases.

use super::stage_registration::{PinnedStage, StageRegistration};
use crate::cuda::StagingCopy;
use crate::engine::PreparedTransfer;
use crate::{Error, Result, TransferOp};

pub(super) enum StageBufferCache<'a> {
    Cuda {
        copy: &'a mut StagingCopy,
        slots: Vec<PinnedStage>,
    },
    Host(StageRegistration),
}

impl StageBufferCache<'_> {
    pub(super) fn width(&self) -> usize {
        match self {
            Self::Cuda { slots, .. } => slots.len(),
            Self::Host(_) => 1,
        }
    }

    pub(super) fn registration(&self, slot: usize) -> &StageRegistration {
        match self {
            Self::Cuda { slots, .. } => &slots[slot].registration,
            Self::Host(stage) => stage,
        }
    }

    pub(super) fn copy(
        &mut self,
        slot: usize,
        request: &PreparedTransfer,
        offset: usize,
        length: usize,
        operation: TransferOp,
    ) -> Result<()> {
        match self {
            Self::Cuda { copy, slots } => {
                slots[slot].copy(copy, offset, length, operation == TransferOp::Read, true)
            }
            Self::Host(stage) => stage.host.with_staging_bytes(length, |bytes| {
                if operation == TransferOp::Read {
                    request.local.write(request.local_offset + offset, bytes)
                } else {
                    request.local.read(request.local_offset + offset, bytes)
                }
            }),
        }
    }

    pub(super) fn query(&mut self, slot: usize) -> Result<bool> {
        match self {
            Self::Cuda { copy, slots } => slots[slot].query(copy),
            Self::Host(_) => Ok(true),
        }
    }

    pub(super) fn complete(&mut self, slot: usize) -> Result<()> {
        match self {
            Self::Cuda { copy, slots } => slots[slot].complete(copy).map(|_| ()),
            Self::Host(_) => Ok(()),
        }
    }

    pub(super) fn drain(&mut self) -> Result<()> {
        let mut result = Ok(());
        // A failure prevents new copies, but never skips already accepted work.
        for slot in 0..self.width() {
            let drained = self.complete(slot);
            if matches!(drained, Err(Error::Unquiesced { .. })) || result.is_ok() {
                result = drained;
            }
        }
        result
    }

    pub(super) fn finish(mut self, result: Result<()>) -> Result<()> {
        let drained = self.drain();
        let mut result = if matches!(drained, Err(Error::Unquiesced { .. })) || result.is_ok() {
            drained
        } else {
            result
        };
        match self {
            Self::Cuda { slots, .. } => {
                for stage in slots {
                    result = stage.finish(result);
                }
                result
            }
            Self::Host(mut stage) => stage.finish(result),
        }
    }
}
