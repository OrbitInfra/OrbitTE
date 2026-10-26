//! A peer endpoint owns a persistent QP array across context CQ lanes.

use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use super::cq_lane::{CqLane, Record};
use super::{ConnectionId, RdmaGrant};
use crate::{Error, Result, SourceEpoch, TransferOp};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Peer {
    pub(super) route: Option<SocketAddr>,
    pub(super) source_nic_id: usize,
    pub(super) epoch: SourceEpoch,
    pub(super) local: ConnectionId,
    pub(super) connection: ConnectionId,
    pub(super) operation: TransferOp,
    pub(super) source: bool,
    pub(super) generation: u64,
}

impl Peer {
    pub(super) fn same_pair(&self, other: &Self) -> bool {
        self.route == other.route
            && self.source_nic_id == other.source_nic_id
            && self.epoch == other.epoch
            && self.local == other.local
            && self.connection == other.connection
            && self.operation == other.operation
            && self.source == other.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct EndpointKey {
    pub(super) route: Option<SocketAddr>,
    pub(super) epoch: SourceEpoch,
    pub(super) domain: [u8; 16],
    pub(super) source: bool,
    pub(super) source_nic_id: usize,
    pub(super) operation: TransferOp,
}

impl From<Peer> for EndpointKey {
    fn from(peer: Peer) -> Self {
        Self {
            route: peer.route,
            epoch: peer.epoch,
            domain: peer.connection.domain,
            source: peer.source,
            source_nic_id: peer.source_nic_id,
            operation: peer.operation,
        }
    }
}

struct Qp {
    token: Arc<()>,
    record: Option<Arc<Record>>,
    lane: Weak<CqLane>,
    remote: Option<ConnectionId>,
    peer: Option<Peer>,
}

pub(super) type Candidate = (Arc<CqLane>, Arc<Record>, Peer);

pub(super) struct RdmaEndpoint {
    pub(super) key: EndpointKey,
    qp_list: Mutex<Vec<Option<Qp>>>,
    changed: Condvar,
}

/// A unique array slot, held from admission through bilateral publication.
pub(super) struct Construction {
    endpoint: Arc<RdmaEndpoint>,
    index: usize,
    token: Arc<()>,
}

impl RdmaEndpoint {
    pub(super) fn new(key: EndpointKey, width: usize) -> Result<Self> {
        if !(1..=64).contains(&width) {
            return Err(Error::Protocol("RDMA endpoint QP width"));
        }
        let mut qp_list = Vec::new();
        qp_list
            .try_reserve_exact(width)
            .map_err(|_| Error::ResourceExhausted)?;
        qp_list.resize_with(width, || None);
        Ok(Self {
            key,
            qp_list: Mutex::new(qp_list),
            changed: Condvar::new(),
        })
    }

    pub(super) fn width(&self) -> Result<usize> {
        Ok(self.lock()?.len())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Vec<Option<Qp>>>> {
        self.qp_list
            .lock()
            .map_err(|_| Error::Internal("RDMA endpoint QP array poisoned"))
    }

    pub(super) fn matches(&self, peer: Peer) -> bool {
        self.key == peer.into()
    }

    fn reclaim(qps: &mut [Option<Qp>]) -> Result<()> {
        for entry in qps {
            if let Some(qp) = entry {
                let closed = qp
                    .record
                    .as_ref()
                    .map(|r| r.connection.lock().map(|c| c.closed))
                    .transpose()?
                    .unwrap_or(false);
                if closed {
                    *entry = None;
                }
            }
        }
        Ok(())
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        remote: Option<ConnectionId>,
    ) -> Result<Option<Construction>> {
        let mut qps = self.lock()?;
        Self::reclaim(&mut qps)?;
        // Duplicate passive bootstraps must wait for this exact construction.
        if remote.is_some() && qps.iter().flatten().any(|qp| qp.remote == remote) {
            return Ok(None);
        }
        let Some(index) = qps.iter().position(Option::is_none) else {
            return Ok(None);
        };
        let token = Arc::new(());
        qps[index] = Some(Qp {
            token: Arc::clone(&token),
            record: None,
            lane: Weak::new(),
            remote,
            peer: None,
        });
        Ok(Some(Construction {
            endpoint: Arc::clone(self),
            index,
            token,
        }))
    }

    pub(super) fn adopt(&self, grant: &RdmaGrant, peer: Peer) -> Result<()> {
        if !self.matches(peer) || grant.incarnation()? != peer.local {
            return Err(Error::Protocol("RDMA endpoint peer identity changed"));
        }
        let mut qps = self.lock()?;
        let qp = qps
            .iter_mut()
            .flatten()
            .find(|qp| {
                qp.record
                    .as_ref()
                    .is_some_and(|r| Arc::ptr_eq(&r.connection, &grant.record.connection))
            })
            .ok_or(Error::Internal("RDMA connection has no reserved QP slot"))?;
        if !qp.lane.ptr_eq(&Arc::downgrade(&grant.progress))
            || qp.remote.is_some_and(|remote| remote != peer.connection)
            || qp.peer.is_some_and(|old| !old.same_pair(&peer))
        {
            return Err(Error::Protocol("RDMA endpoint QP identity changed"));
        }
        let mut peer = peer;
        if let Some(old) = qp.peer {
            peer.generation = peer.generation.max(old.generation);
        }
        qp.remote = Some(peer.connection);
        qp.peer = Some(peer);
        qp.record = Some(Arc::clone(&grant.record));
        self.changed.notify_all();
        Ok(())
    }

    pub(super) fn prune(&self) -> Result<bool> {
        let mut qps = self.lock()?;
        Self::reclaim(&mut qps)?;
        Ok(qps.iter().all(Option::is_none))
    }

    pub(super) fn candidates(&self, matches: &impl Fn(&Peer) -> bool) -> Result<Vec<Candidate>> {
        let qps = self.lock()?;
        Ok(qps
            .iter()
            .flatten()
            .filter_map(|qp| {
                let peer = qp.peer?;
                let record = qp.record.as_ref()?;
                if !matches(&peer) || record.connection.require_live().is_err() {
                    return None;
                }
                qp.lane
                    .upgrade()
                    .map(|lane| (lane, Arc::clone(record), peer))
            })
            .collect())
    }

    pub(super) fn contains(&self, remote: ConnectionId) -> Result<bool> {
        Ok(self
            .lock()?
            .iter()
            .flatten()
            .any(|qp| qp.remote == Some(remote)))
    }

    pub(super) fn wait(&self, deadline: Instant) -> Result<()> {
        let duration = crate::engine::remaining(deadline)?.min(Duration::from_millis(10));
        // Native retirement is asynchronous; periodically recheck drain as well
        // as receiving publication/rollback notifications. No store lock is held.
        let _ = self
            .changed
            .wait_timeout(self.lock()?, duration)
            .map_err(|_| Error::Internal("RDMA endpoint construction wait poisoned"))?;
        Ok(())
    }
}

impl Construction {
    pub(super) fn attach(&self, grant: &mut RdmaGrant) -> Result<()> {
        let mut qps = self.endpoint.lock()?;
        let qp = qps[self.index]
            .as_mut()
            .filter(|qp| Arc::ptr_eq(&qp.token, &self.token))
            .ok_or(Error::Unavailable("RDMA construction reservation retired"))?;
        if qp.record.is_some() {
            return Err(Error::Internal("RDMA construction already owns a QP"));
        }
        qp.record = Some(Arc::clone(&grant.record));
        qp.lane = Arc::downgrade(&grant.progress);
        grant.endpoint = Some(Arc::clone(&self.endpoint));
        Ok(())
    }
}

impl Drop for Construction {
    fn drop(&mut self) {
        let mut qps = self
            .endpoint
            .qp_list
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(qp) = &qps[self.index] {
            if Arc::ptr_eq(&qp.token, &self.token)
                && qp.peer.is_none()
                && qp
                    .record
                    .as_ref()
                    .is_none_or(|r| r.connection.lock().is_ok_and(|c| c.closed))
            {
                qps[self.index] = None;
            }
        }
        // Failed cleanup retains the physical QP in its slot until native drain.
        self.endpoint.changed.notify_all();
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/endpoint.rs"]
mod tests;
