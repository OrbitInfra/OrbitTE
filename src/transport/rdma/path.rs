//! Frozen NIC/PD path metadata and static pair compatibility.

use std::net::Ipv6Addr;

use super::{Descriptor, discovery::GidType};
use crate::runtime::control_plane::{ControlChannel, Reply};
use crate::{Error, Result, SourceEpoch};

pub(super) const PATH_SIZE: usize = 187;

/// One frozen NIC/PD path. Contains no QP, MR address, lkey or rkey authority.
#[derive(Clone, Debug)]
pub(super) struct NicPath {
    pub(super) nic_id: usize,
    pub(super) name: String,
    pub(super) domain: [u8; 16],
    pub(super) gid_type: GidType,
    pub(super) descriptor: Descriptor,
}

impl NicPath {
    fn validate(&self) -> Result<()> {
        let d = self.descriptor;
        let layer = if self.gid_type == GidType::Ib { 1 } else { 2 };
        let gid = Ipv6Addr::from(d.gid);
        if self.nic_id >= 64
            || self.name.is_empty()
            || self.name.len() > 128
            || self.name.contains(['\0', '/'])
            || matches!(self.name.as_str(), "." | "..")
            || self.domain == [0; 16]
            || d.address != 0
            || d.rkey != 0
            || d.qpn != 0
            || d.psn != 0
            || !(1..=5).contains(&d.mtu)
            || d.link_layer != layer
            || (layer == 1 && d.lid == 0)
            || (layer == 2 && (gid.is_unspecified() || gid.is_multicast()))
        {
            return Err(Error::Protocol("invalid RDMA NIC path"));
        }
        Ok(())
    }

    fn encode(&self) -> Result<[u8; PATH_SIZE]> {
        self.validate()?;
        let mut bytes = [0; PATH_SIZE];
        bytes[0] = self.nic_id as u8;
        bytes[1] = self.name.len() as u8;
        bytes[2..2 + self.name.len()].copy_from_slice(self.name.as_bytes());
        bytes[130..146].copy_from_slice(&self.domain);
        bytes[146] = match self.gid_type {
            GidType::Ib => 1,
            GidType::RoceV1 => 2,
            GidType::RoceV2 => 3,
        };
        bytes[147..].copy_from_slice(&self.descriptor.encode());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != PATH_SIZE {
            return Err(Error::Protocol("RDMA NIC path length"));
        }
        let length = bytes[1] as usize;
        if length == 0 || length > 128 || bytes[2 + length..130].iter().any(|b| *b != 0) {
            return Err(Error::Protocol("RDMA NIC path name or padding"));
        }
        let name = std::str::from_utf8(&bytes[2..2 + length])
            .map_err(|_| Error::Protocol("RDMA NIC path name encoding"))?
            .to_owned();
        let mut domain = [0; 16];
        domain.copy_from_slice(&bytes[130..146]);
        let gid_type = match bytes[146] {
            1 => GidType::Ib,
            2 => GidType::RoceV1,
            3 => GidType::RoceV2,
            _ => return Err(Error::Protocol("RDMA NIC path GID type")),
        };
        let mut descriptor = [0; super::DESCRIPTOR_SIZE];
        descriptor.copy_from_slice(&bytes[147..]);
        let path = Self {
            nic_id: bytes[0] as usize,
            name,
            domain,
            gid_type,
            descriptor: Descriptor::decode_fields(&descriptor),
        };
        path.validate()?;
        Ok(path)
    }

    pub(super) fn matches_connection(&self, connection: &Descriptor) -> bool {
        self.descriptor.gid == connection.gid
            && self.descriptor.lid == connection.lid
            && self.descriptor.mtu == connection.mtu
            && self.descriptor.link_layer == connection.link_layer
    }
}

pub(super) fn write_paths(
    channel: &mut ControlChannel<'_>,
    epoch: SourceEpoch,
    paths: &[NicPath],
    qp_width: usize,
) -> Result<()> {
    if paths.is_empty() || paths.len() > 64 || qp_width > 64 {
        return Err(Error::Protocol("RDMA NIC path count"));
    }
    let payload: Vec<_> = paths.iter().map(NicPath::encode).collect::<Result<_>>()?;
    channel.reply(epoch, 0, (1 + payload.len() * PATH_SIZE) as u64)?;
    channel.write_all(&[qp_width as u8])?;
    for bytes in payload {
        channel.write_all(&bytes)?;
    }
    Ok(())
}

pub(super) fn read_paths(
    channel: &mut ControlChannel<'_>,
    epoch: SourceEpoch,
) -> Result<(Vec<NicPath>, usize)> {
    let reply = Reply::read(channel, epoch)?;
    let length = table_length(&reply)?;
    let mut bytes = vec![0; length];
    channel.read_exact(&mut bytes)?;
    decode_paths(&bytes)
}

pub(super) fn table_length(reply: &Reply) -> Result<usize> {
    if reply.status != 0
        || reply.length <= 1
        || reply.length > (1 + 64 * PATH_SIZE) as u64
        || (reply.length - 1) % PATH_SIZE as u64 != 0
    {
        return Err(Error::Protocol("RDMA NIC path table length/status"));
    }
    Ok(reply.length as usize)
}

pub(super) fn decode_paths(bytes: &[u8]) -> Result<(Vec<NicPath>, usize)> {
    if bytes.is_empty() || bytes[0] > 64 {
        return Err(Error::Protocol("RDMA peer QP width"));
    }
    let mut paths: Vec<NicPath> = Vec::new();
    for bytes in bytes[1..].chunks(PATH_SIZE) {
        let path = NicPath::decode(bytes)?;
        if paths
            .iter()
            .any(|p| p.nic_id == path.nic_id || p.name == path.name || p.domain == path.domain)
        {
            return Err(Error::Protocol("duplicate RDMA NIC path identity"));
        }
        paths.push(path);
    }
    Ok((paths, bytes[0] as usize))
}

/// Filter wire-incompatible paths. Matching NIC names wins within a profile;
/// static metadata does not prove physical reachability.
pub(super) fn compatible(local: &NicPath, remote: &NicPath) -> bool {
    local.gid_type == remote.gid_type
        && local.descriptor.link_layer == remote.descriptor.link_layer
        && (local.gid_type == GidType::Ib
            || Ipv6Addr::from(local.descriptor.gid)
                .to_ipv4_mapped()
                .is_some()
                == Ipv6Addr::from(remote.descriptor.gid)
                    .to_ipv4_mapped()
                    .is_some())
}

pub(super) fn local_device_mask(local: &[NicPath], remote: &[NicPath]) -> u64 {
    let same_name = local
        .iter()
        .any(|l| remote.iter().any(|r| compatible(l, r) && l.name == r.name));
    local
        .iter()
        .filter(|l| {
            remote
                .iter()
                .any(|r| compatible(l, r) && (!same_name || l.name == r.name))
        })
        .fold(0, |mask, l| mask | (1u64 << l.nic_id))
}

pub(super) fn remote_path<'a>(local: &NicPath, remote: &'a [NicPath]) -> Result<&'a NicPath> {
    remote
        .iter()
        .filter(|r| compatible(local, r))
        .min_by_key(|r| (r.name != local.name, r.nic_id))
        .ok_or(Error::Unavailable("no compatible remote RDMA NIC path"))
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/path.rs"]
mod tests;
