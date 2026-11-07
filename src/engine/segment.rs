use super::*;

impl TransferEngine {
    pub(crate) fn admit_peer(&self, peer: &RemoteSegment) -> Result<Arc<PeerPermit>> {
        self.peers.admit(
            peer.identity.as_ref().ok_or(Error::InvalidConfig(
                "open_segment on this requester before admission",
            ))?,
            peer.into(),
        )
    }

    pub(crate) fn admit_peer_handle(
        &self,
        handle: &crate::runtime::segment_manager::PeerHandle,
        key: PeerKey,
    ) -> Result<Arc<PeerPermit>> {
        self.peers.admit(handle, key)
    }

    /// Closes requester admission to one endpoint/source epoch.
    /// Existing work drains before its cached transport resources are retired.
    /// Old `RemoteSegment` snapshots cannot submit or refresh after this fence;
    /// a different authoritative epoch can be opened normally. After successful
    /// cleanup and the last descriptor/operation reference is dropped, the fence
    /// can be reclaimed. Only an explicit `open_segment` can then reopen the same
    /// authoritative epoch; Describe still checks it against the source.
    ///
    /// A timeout returns `Unquiesced` and keeps owners. Waiting again is allowed
    /// while admitted work drains. Once native cleanup has started, its result
    /// is final: uncertain destroys are never retried. The timeout bounds waits,
    /// not native provider calls that cannot be preempted.
    ///
    /// This membership operation is stronger than TENT's C/C++ `closeSegment`,
    /// which only invalidates a handle and permits reopening the same epoch.
    pub fn close_segment(
        &self,
        endpoint: SocketAddr,
        epoch: SourceEpoch,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))?;
        let peer = PeerKey { endpoint, epoch };
        self.peers.close(peer, deadline, || {
            self.retire_peer_resources(peer, deadline)
        })
    }

    fn retire_peer_resources(&self, peer: PeerKey, deadline: Instant) -> Result<()> {
        let mut failure = None;
        if let Some(credits) = &self.receiver_credits {
            if let Err(e) = credits.close_peer(peer, deadline) {
                failure.get_or_insert(e);
            }
        }
        if let Some(hp) = &self.hp_tcp {
            if let Err(e) = hp.close_peer(peer) {
                failure.get_or_insert(e);
            }
        }
        #[cfg(feature = "rdma")]
        if let Some(rdma) = &self.source.rdma {
            if let Err(e) = rdma.close_peer(peer, deadline) {
                failure.get_or_insert(e);
            }
        }
        #[cfg(feature = "cuda")]
        if let Some(nvlink) = &self.source.nvlink {
            if let Err(e) = nvlink.close_peer(peer, self.source.epoch, deadline) {
                failure.get_or_insert(e);
            }
        }
        #[cfg(target_os = "linux")]
        if let Some(shm) = &self.source.shm {
            if let Err(e) = shm.close_peer(peer) {
                failure.get_or_insert(e);
            }
        }
        #[cfg(feature = "fabric")]
        if let Some(fabric) = &self.source.fabric {
            if let Err(e) = fabric.close_peer(peer) {
                failure.get_or_insert(e);
            }
        }
        // TCP has no persistent requester sockets. Their admitted call owns the
        // socket and backing until drain. Shared registration MRs, contexts and
        // worker pools belong to the engine, not to this peer.
        failure.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/engine/segment.rs"]
mod tests;
