//! RC verbs with per-NIC contexts, shared progress and owned memory grants.

#![allow(unsafe_code)]

use crate::{Error, Result};

mod buffers;
mod config;
mod context;
mod cq;
mod discovery;
mod endpoint;
mod endpoint_store;
mod events;
mod keys;
mod progress;
mod quota;
mod rail_monitor;
mod rdma_transport;
mod receive;
mod slice;
mod stats;
mod sys;
mod workers;

pub use config::{RdmaConfig, RdmaHostMemory, RdmaProgressMode};
pub(crate) use rdma_transport::RdmaTransport;
pub use stats::RdmaStats;

use context::RdmaContext;
use endpoint::{
    Buffer, ConnectionId, ConnectionResources, Descriptor, GrantResources, Phase, RdmaEndpoint,
    Resources, Staging,
};

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
