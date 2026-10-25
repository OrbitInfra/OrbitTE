//! A peer endpoint owns a persistent QP array across context CQ lanes.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Weak};

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
struct Identity {
    route: Option<SocketAddr>,
    epoch: SourceEpoch,
    domain: [u8; 16],
    source: bool,
    source_nic_id: usize,
}

impl From<Peer> for Identity {
    fn from(peer: Peer) -> Self {
        Self {
            route: peer.route,
            epoch: peer.epoch,
            domain: peer.connection.domain,
            source: peer.source,
            source_nic_id: peer.source_nic_id,
        }
    }
}

struct Qp {
    record: Arc<Record>,
    lane: Weak<CqLane>,
    peer: Peer,
}

pub(super) struct RdmaEndpoint {
    identity: Identity,
    qp_list: Mutex<Vec<Qp>>,
}

impl RdmaEndpoint {
    pub(super) fn new(peer: Peer) -> Self {
        Self {
            identity: peer.into(),
            qp_list: Mutex::default(),
        }
    }

    pub(super) fn matches(&self, peer: Peer) -> bool {
        self.identity == peer.into()
    }

    pub(super) fn adopt(&self, grant: &RdmaGrant, peer: Peer) -> Result<()> {
        if !self.matches(peer) || grant.incarnation()? != peer.local {
            return Err(Error::Protocol("RDMA endpoint peer identity changed"));
        }
        let mut qps = self
            .qp_list
            .lock()
            .map_err(|_| Error::Internal("RDMA endpoint QP array poisoned"))?;
        if let Some(qp) = qps
            .iter_mut()
            .find(|qp| Arc::ptr_eq(&qp.record.connection, &grant.record.connection))
        {
            if !qp.peer.same_pair(&peer) || !qp.lane.ptr_eq(&Arc::downgrade(&grant.progress)) {
                return Err(Error::Protocol("RDMA endpoint QP identity changed"));
            }
            qp.peer = peer;
            qp.record = Arc::clone(&grant.record);
        } else {
            // QPs are demand-created because one wire grant exclusively owns a QP.
            // Every actual QP is charged by the transport's aggregate limit.
            qps.try_reserve(1).map_err(|_| Error::ResourceExhausted)?;
            qps.push(Qp {
                record: Arc::clone(&grant.record),
                lane: Arc::downgrade(&grant.progress),
                peer,
            });
        }
        Ok(())
    }

    pub(super) fn prune(&self) -> Result<bool> {
        let mut qps = self
            .qp_list
            .lock()
            .map_err(|_| Error::Internal("RDMA endpoint QP array poisoned"))?;
        // A terminal bit only removes lookup permission; it never proves drain.
        let mut index = 0;
        while index < qps.len() {
            let closed = qps[index].record.connection.lock()?.closed;
            if closed {
                qps.swap_remove(index);
            } else {
                index += 1;
            }
        }
        Ok(qps.is_empty())
    }

    pub(super) fn candidates(
        &self,
        matches: &impl Fn(&Peer) -> bool,
    ) -> Result<Vec<(Arc<CqLane>, Peer)>> {
        let qps = self
            .qp_list
            .lock()
            .map_err(|_| Error::Internal("RDMA endpoint QP array poisoned"))?;
        Ok(qps
            .iter()
            .filter(|qp| matches(&qp.peer))
            .filter_map(|qp| qp.lane.upgrade().map(|lane| (lane, qp.peer)))
            .collect())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/endpoint.rs"]
mod tests;
