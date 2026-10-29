//! Submitted native slices remain owned until bilateral completion or drain.

use super::{RdmaSlice, RdmaTransport, close_slices};
use crate::runtime::control_plane::ControlChannel;
use crate::{RegisteredMemory, Result, SourceEpoch};

pub(crate) struct RdmaTransfer {
    pub(super) slices: Vec<RdmaSlice>,
    pub(super) memory: RegisteredMemory,
    pub(super) requester: bool,
    pub(super) epoch: SourceEpoch,
}

impl RdmaTransfer {
    #[cfg(feature = "cuda")]
    pub(crate) fn query(&self) -> Result<bool> {
        if !self.requester {
            return Err(crate::Error::Internal(
                "source RDMA transfer requires a completion message",
            ));
        }
        let mut completed = true;
        for slice in &self.slices {
            completed &= slice
                .grant
                .progress
                .query(&slice.grant.record, slice.grant.claim)?;
        }
        Ok(completed)
    }

    pub(crate) fn finish(
        mut self,
        transport: &RdmaTransport,
        channel: &mut ControlChannel<'_>,
    ) -> Result<()> {
        let result = (|| {
            for slice in &mut self.slices {
                if self.requester {
                    slice.grant.progress.wait(
                        &slice.grant.record,
                        slice.grant.claim,
                        channel.deadline,
                    )?;
                    transport.finish_requester_slice(channel, slice, &self.memory)?;
                } else {
                    transport.finish_source_slice(channel, self.epoch, &self.memory, slice)?;
                }
            }
            Ok(())
        })();
        self.close(result)
    }

    pub(crate) fn close(mut self, result: Result<()>) -> Result<()> {
        close_slices(&mut self.slices, result, self.requester)
    }
}

impl RdmaTransport {
    #[cfg(feature = "cuda")]
    pub(crate) fn staging_concurrency(&self, chunk: usize) -> usize {
        let (bytes, endpoints) = if self.config.host_memory == super::super::RdmaHostMemory::Direct
        {
            (self.config.max_direct_bytes, 16)
        } else {
            (self.config.max_staging_bytes, self.config.max_endpoints)
        };
        (bytes / chunk)
            .min(endpoints)
            .min(self.config.max_grants)
            .min(self.config.max_cqes)
            .clamp(1, 16)
    }
}
