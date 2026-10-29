//! Bounded requester PRE/CROSS/POST with independent local and remote rings.

use std::sync::Arc;
use std::time::Instant;

use super::ProxyManager;
use super::stage_buffer_cache::StageBufferCache;
use super::stage_registration::{PinnedStage, StageRegistration};
use super::staging_task::StagingTask;
use crate::engine::{PreparedTransfer, Source};
use crate::memory::Allocation;
use crate::runtime::control_plane::buffer;
use crate::{Error, HostMemory, RemoteEffect, RemoteSegment, TransferOp};

impl ProxyManager {
    pub(crate) fn transfer_remote(
        self: &Arc<Self>,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        source: &Source,
        deadline: Instant,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut effect = RemoteEffect::None;
        let result = (|| {
            let mut transfer = |stages: &mut StageBufferCache<'_>| {
                StagingTask::new(operation, peer, request, source, self.chunk_size, deadline)?
                    .run(stages, &mut effect)
            };
            let (allocation, base) = request
                .local
                .allocation_range(request.local_offset, request.length)?;
            match allocation {
                Allocation::Cuda(gpu) => {
                    let width = source
                        .staging_pipeline_depth
                        .min(request.length.div_ceil(self.chunk_size));
                    gpu.with_staging(base, request.length, self, width, |copy, leases| {
                        let mut slots = Vec::with_capacity(width);
                        let built = leases.into_iter().try_for_each(|lease| {
                            slots.push(PinnedStage::new(lease, source, request.transport)?);
                            Ok(())
                        });
                        let mut stages = StageBufferCache::Cuda { copy, slots };
                        let result = built.and_then(|()| transfer(&mut stages));
                        stages.finish(result)
                    })
                }
                Allocation::Host(_) => {
                    let payload = buffer(self.chunk_size.min(request.length))?;
                    let host = Arc::new(HostMemory::new(payload.into_boxed_slice())?);
                    let mut stages = StageBufferCache::Host(StageRegistration::new(
                        host,
                        source,
                        request.transport,
                    )?);
                    let result = transfer(&mut stages);
                    stages.finish(result)
                }
            }
        })();
        result.map_err(|error| (error, effect))
    }
}
