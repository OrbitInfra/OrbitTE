//! RC verbs with per-NIC contexts, shared progress and owned memory grants.

#![allow(unsafe_code)]

use crate::{Error, Result};

mod buffers;
mod config;
mod context;
mod context_set;
mod cq;
mod cq_lane;
mod discovery;
mod endpoint;
mod endpoint_store;
mod events;
mod grant;
mod keys;
mod monitor;
mod path;
mod protocol;
mod qp;
mod qp_store;
mod quota;
mod rdma_transport;
mod receive;
mod slice;
mod stats;
mod sys;
mod workers;

pub use config::{RdmaConfig, RdmaCudaMemory, RdmaHostMemory, RdmaProgressMode};
pub(crate) use rdma_transport::RdmaTransport;
pub use stats::RdmaStats;

use context::RdmaContext;
use grant::{Buffer, GrantResources, RdmaGrant, Staging};
use qp::{ConnectionId, ConnectionResources, Descriptor, Phase, Resources};

pub(crate) const DESCRIPTOR_SIZE: usize = 40;
pub(super) const WR_ID_STRIDE: u64 = 128;
pub(super) const COMPLETION_MESSAGE_SIZE: usize = 141;

fn check(operation: &'static str, code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::Backend { operation, code })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/mod.rs"]
pub(crate) mod tests;
