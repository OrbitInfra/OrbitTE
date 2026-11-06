mod buffer_registry;
mod client;
mod config;
mod protocol;
mod server;
mod task;
mod workers;

use crate::{
    Error, RemoteSegment, Result, TransferOp,
    engine::{PreparedTransfer, Source},
};
pub(crate) use buffer_registry::host;
pub(crate) use client::Receipt;
pub(crate) use config::Endpoint;
pub use config::HpTcpConfig;
use std::sync::Arc;
use std::time::Instant;
pub(crate) use task::Permit;

pub(crate) struct HpTcpTransport {
    #[cfg(test)]
    pub(crate) retired_sessions: Arc<std::sync::atomic::AtomicUsize>,
    workers: workers::Workers,
    client: client::Client,
    budget: Arc<task::Budget>,
}
impl HpTcpTransport {
    pub(crate) fn start(
        cfg: &HpTcpConfig,
        listener: std::net::TcpListener,
        source: Arc<Source>,
    ) -> Result<Self> {
        let workers = workers::Workers::start(cfg.worker_count)?;
        #[cfg(test)]
        let retired_sessions = Arc::default();
        server::start(
            &workers,
            listener,
            source,
            cfg,
            #[cfg(test)]
            Arc::clone(&retired_sessions),
        )?;
        Ok(Self {
            #[cfg(test)]
            retired_sessions,
            workers,
            client: client::Client::new(cfg.clone()),
            budget: task::Budget::new(cfg.max_outstanding_tasks, cfg.max_outstanding_bytes),
        })
    }
    pub(crate) fn stop(&self) -> Result<()> {
        self.workers.stop()
    }
    pub(crate) fn reserve(&self, length: usize) -> Result<Permit> {
        self.workers.ready()?;
        self.budget.reserve(length)
    }
    pub(crate) fn close_peer(&self, peer: crate::runtime::segment_manager::PeerKey) -> Result<()> {
        self.client.close_peer(peer)
    }
    pub(crate) fn submit(
        &self,
        op: TransferOp,
        peer: &RemoteSegment,
        r: &PreparedTransfer,
        deadline: Instant,
    ) -> Result<Receipt> {
        self.workers.ready()?;
        let end = peer
            .metadata
            .hp
            .as_deref()
            .copied()
            .ok_or(Error::Unavailable("peer did not publish HP-TCP"))?;
        let frame = protocol::Request {
            op,
            id: 0,
            registration: buffer_registry::registration(peer.epoch, r.remote.id()),
            address: r.remote.base_address() + r.remote_offset as u64,
            length: r.length as u64,
        };
        self.client.submit(
            &self.workers,
            peer.into(),
            end,
            frame,
            &r.local,
            r.local_offset,
            deadline,
            r.hp_cancel.as_ref(),
        )
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/hp_tcp/mod.rs"]
mod tests;
