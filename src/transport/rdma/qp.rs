//! Physical RC connection, descriptor and native resources; grants own memory authority.

use std::sync::{Arc, Condvar, Mutex};

use super::{DESCRIPTOR_SIZE, GrantResources, RdmaContext, RdmaStats, sys};
use crate::{Error, Result, SourceEpoch};

pub(super) struct ConnectionLease {
    pub(super) usage: Arc<Mutex<RdmaStats>>,
    pub(super) drained: Arc<Condvar>,
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active_endpoints -= 1;
        self.drained.notify_all();
    }
}

pub(super) struct ConnectionResources {
    pub(super) device: Arc<RdmaContext>,
    pub(super) handles: sys::ConnectionHandles,
    pub(super) incarnation: Option<ConnectionId>,
    pub(super) local: Descriptor,
    pub(super) remote: Option<Descriptor>,
    pub(super) permission: u32,
    pub(super) closed: bool,
    pub(super) lease: Option<ConnectionLease>,
}

impl ConnectionResources {
    pub(super) fn close(
        &mut self,
        verbs: &sys::Verbs,
    ) -> std::result::Result<(), (&'static str, i32)> {
        if self.closed {
            return Ok(());
        }
        if let Err(Error::Unquiesced { operation, code }) = self.device.asynchronous.check() {
            return Err((operation, code));
        }
        let had_qp = !self.handles.qp.is_null();
        let code = unsafe { (verbs.destroy_qp)(&mut self.handles) };
        if code != 0 {
            return Err(("ibv_destroy_qp", code));
        }
        if had_qp {
            if let Some(lease) = &self.lease {
                lease
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .closed_connections += 1;
            }
        }
        self.closed = true;
        self.handles.cq = std::ptr::null_mut();
        self.handles.receive_cq = std::ptr::null_mut();
        drop(self.lease.take());
        Ok(())
    }
}

pub(super) struct Resources {
    pub(super) connection: ConnectionResources,
    pub(super) grant: Option<GrantResources>,
}

impl Resources {
    pub(super) fn grant(&self) -> Result<&GrantResources> {
        self.grant
            .as_ref()
            .ok_or(Error::Internal("RDMA connection has no active grant"))
    }

    pub(super) fn grant_mut(&mut self) -> Result<&mut GrantResources> {
        self.grant
            .as_mut()
            .ok_or(Error::Internal("RDMA connection has no active grant"))
    }
}

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq)]
#[repr(C)]
pub(crate) struct Descriptor {
    pub(super) address: u64,
    pub(super) rkey: u32,
    pub(super) qpn: u32,
    pub(super) psn: u32,
    pub(super) lid: u16,
    pub(super) mtu: u8,
    pub(super) link_layer: u8,
    pub(super) gid: [u8; 16],
}

impl Descriptor {
    pub(super) fn validate(&self) -> Result<()> {
        if self.address == 0 {
            return Err(Error::Protocol("invalid RDMA memory grant"));
        }
        self.validate_connection()
    }

    pub(super) fn validate_connection(&self) -> Result<()> {
        if self.qpn == 0
            || self.qpn > 0xffffff
            || self.psn > 0xffffff
            || !(1..=5).contains(&self.mtu)
            || !matches!(self.link_layer, 1 | 2)
            || (self.link_layer == 2 && self.gid == [0; 16])
        {
            return Err(Error::Protocol("invalid RDMA endpoint descriptor"));
        }
        Ok(())
    }

    pub(crate) fn connection_bytes(self) -> [u8; DESCRIPTOR_SIZE] {
        Self {
            address: 0,
            rkey: 0,
            ..self
        }
        .encode()
    }

    pub(crate) fn encode(self) -> [u8; DESCRIPTOR_SIZE] {
        let mut bytes = [0; DESCRIPTOR_SIZE];
        bytes[..8].copy_from_slice(&self.address.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.rkey.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.qpn.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.psn.to_be_bytes());
        bytes[20..22].copy_from_slice(&self.lid.to_be_bytes());
        bytes[22] = self.mtu;
        bytes[23] = self.link_layer;
        bytes[24..].copy_from_slice(&self.gid);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8; DESCRIPTOR_SIZE]) -> Result<Self> {
        let descriptor = Self::decode_fields(bytes);
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub(crate) fn decode_connection(bytes: &[u8; DESCRIPTOR_SIZE]) -> Result<Self> {
        let descriptor = Self::decode_fields(bytes);
        descriptor.validate_connection()?;
        if descriptor.address != 0 || descriptor.rkey != 0 {
            return Err(Error::Protocol("RDMA connection exposes memory authority"));
        }
        Ok(descriptor)
    }

    pub(super) fn decode_fields(bytes: &[u8; DESCRIPTOR_SIZE]) -> Self {
        let mut address = [0; 8];
        address.copy_from_slice(&bytes[..8]);
        let value = |offset| {
            let mut field = [0; 4];
            field.copy_from_slice(&bytes[offset..offset + 4]);
            u32::from_be_bytes(field)
        };
        let mut gid = [0; 16];
        gid.copy_from_slice(&bytes[24..]);
        Self {
            address: u64::from_be_bytes(address),
            rkey: value(8),
            qpn: value(12),
            psn: value(16),
            lid: u16::from_be_bytes([bytes[20], bytes[21]]),
            mtu: bytes[22],
            link_layer: bytes[23],
            gid,
        }
    }
}

// Native connection and memory owners are serialized by the progress record.
unsafe impl Send for ConnectionResources {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Phase {
    Init,
    Ready,
    Drained,
    Connected,
    Queued,
    Posted,
    Closed,
    Retained,
}

/// Connection identity is independent of reusable provider QP numbers and PSNs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConnectionId {
    pub(super) domain: [u8; 16],
    pub(super) generation: u64,
}

impl ConnectionId {
    pub(crate) fn encode(self) -> [u8; 24] {
        let mut bytes = [0; 24];
        bytes[..16].copy_from_slice(&self.domain);
        bytes[16..].copy_from_slice(&self.generation.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; 24] = bytes
            .try_into()
            .map_err(|_| Error::Protocol("RDMA connection identity size"))?;
        let mut domain = [0; 16];
        domain.copy_from_slice(&bytes[..16]);
        let mut generation = [0; 8];
        generation.copy_from_slice(&bytes[16..]);
        let generation = u64::from_be_bytes(generation);
        if domain == [0; 16] || generation == 0 {
            return Err(Error::Protocol("invalid RDMA connection identity"));
        }
        Ok(Self { domain, generation })
    }

    pub(super) fn new_domain() -> Result<[u8; 16]> {
        let domain = SourceEpoch::generate()?.0;
        if domain == [0; 16] {
            return Err(Error::Unavailable("zero RDMA protection-domain identity"));
        }
        Ok(domain)
    }

    pub(super) fn next(domain: [u8; 16], generation: &mut u64) -> Result<Self> {
        let current = *generation;
        *generation = current.checked_add(1).ok_or(Error::ResourceExhausted)?;
        Ok(Self {
            domain,
            generation: current,
        })
    }
}
