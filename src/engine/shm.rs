use super::*;
use crate::transport::shm::Lease;

impl TransferEngine {
    pub(super) fn shm_aliases(&self, peer: &RemoteSegment, requests: &[PreparedTransfer]) -> bool {
        if self.source.shm.is_none() {
            return false;
        }
        let Some(manifest) = &peer.metadata.shm else {
            return false;
        };
        // Unlike virtual addresses, inode offsets are comparable across processes.
        let mut remote: Vec<_> = requests
            .iter()
            .filter(|r| r.selection.includes(Transport::Shm))
            .filter_map(|r| {
                let d = manifest.get(r.remote.id())?;
                let start = d.offset + r.remote_offset as u64;
                Some((d.object(), start, start + r.length as u64))
            })
            .collect();
        if remote.is_empty() {
            return false;
        }
        let mut local: Vec<_> = requests
            .iter()
            .filter_map(|r| {
                let (a, base) = r.local.shared()?;
                let start = (base + r.local_offset) as u64;
                Some((a.object(), start, start + r.length as u64))
            })
            .collect();
        remote.sort_unstable();
        local.sort_unstable();
        let (mut i, mut j) = (0, 0);
        while i < local.len() && j < remote.len() {
            let (a, x, y) = local[i];
            let (b, p, q) = remote[j];
            if a < b || (a == b && y <= p) {
                i += 1;
            } else if b < a || q <= x {
                j += 1;
            } else {
                return true;
            }
        }
        false
    }
    pub(super) fn shm_lease(
        &self,
        peer: &RemoteSegment,
        r: &PreparedTransfer,
        op: TransferOp,
    ) -> Result<Arc<Lease>> {
        if !self.source.metadata.same_machine(&peer.metadata)
            || r.local.memory_type() != crate::MemoryType::Cpu
            || r.remote.memory_type() != crate::MemoryType::Cpu
        {
            return Err(r
                .selection
                .rejected("SHM requires host memory on the same machine"));
        }
        self.source
            .shm
            .as_ref()
            .ok_or(Error::Unavailable("SHM is disabled"))?
            .prepare(
                peer.metadata.shm.as_deref().ok_or(Error::RegionNotFound)?,
                &r.remote,
                op,
            )
    }
    pub(super) fn preflight_shm(
        &self,
        peer: &RemoteSegment,
        op: TransferOp,
        requests: &mut [PreparedTransfer],
        deadline: Instant,
    ) -> Result<()> {
        for r in requests {
            if r.selection.transport != Transport::Shm {
                continue;
            }
            let mut failure = match self.shm_lease(peer, r, op) {
                Ok(lease) => {
                    r.shm = Some(lease);
                    continue;
                }
                Err(e) => e,
            };
            loop {
                let next = r.selection.initial_attempts + 1;
                let Some(mut route) = r.selection.candidate(next).filter(|_| {
                    next <= self.config.max_failover_attempts && failover::retryable(&failure)
                }) else {
                    return Err(failure);
                };
                route.initial_attempts = next;
                r.selection = route;
                match self.validate_memory_route(
                    &r.selection,
                    &r.local,
                    r.local_offset,
                    r.length,
                    false,
                    deadline,
                    &mut false,
                ) {
                    Ok(()) => break,
                    Err(e) => failure = e,
                }
            }
        }
        Ok(())
    }
}
