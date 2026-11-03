use super::protocol::{Request, Status};
use crate::engine::Source;
use crate::{MemoryType, RegisteredMemory, SourceEpoch, TransferOp};

fn namespace(epoch: SourceEpoch) -> u128 {
    u128::from_be_bytes(epoch.0) % u128::from(u64::MAX)
}

/// A permutation of the nonzero registration IDs in this engine incarnation.
pub(super) fn registration(epoch: SourceEpoch, id: u64) -> u64 {
    ((namespace(epoch) + u128::from(id) - 1) % u128::from(u64::MAX)) as u64 + 1
}

pub(crate) fn host(memory: &RegisteredMemory) -> bool {
    #[cfg(feature = "cuda")]
    if memory.is_cuda_pinned_host() {
        return false;
    }
    memory.memory_type() == MemoryType::Cpu
}

pub(super) fn lease(
    source: &Source,
    frame: Request,
    len: usize,
) -> std::result::Result<(RegisteredMemory, usize), Status> {
    let (region, offset) = source
        .registry
        .resolve_address(frame.address, len)
        .map_err(|_| Status::Range)?;
    if registration(source.epoch, region.id()) != frame.registration {
        return Err(Status::Stale);
    }
    if !host(&region) {
        return Err(Status::Range);
    }
    if !matches!(
        region.permission(),
        crate::Permission::RemoteRead | crate::Permission::RemoteReadWrite
    ) || !region.permission().allows(frame.op == TransferOp::Write)
    {
        return Err(Status::Permission);
    }
    Ok((region, offset))
}
