//! Frozen SegmentDesc JSON. Local keys and permissions are not TENT wire fields.

use crate::segment::{RegionProfile, SegmentMetadata};
use crate::{
    Error, MemoryType, Permission, RemoteRegion, RemoteSegment, Result, SourceEpoch, Transport,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub(super) const LIMIT: usize = 4 << 20;

#[derive(Serialize, Deserialize)]
pub(super) struct Segment {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) kind: String,
    pub(super) machine_id: String,
    pub(super) rpc_server_addr: String,
    pub(super) detail: Memory,
    /// An optional extension: a frozen peer ignores it and cannot prove this identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) orbite_source_epoch: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Memory {
    pub(super) buffers: Vec<Buffer>,
    #[serde(default)]
    pub(super) topology: String,
    pub(super) devices: Vec<Device>,
    pub(super) device_attrs: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub(super) transport_attrs: Vec<(u8, String)>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Device {
    name: String,
    lid: u16,
    gid: String,
    transport_attrs: Vec<(u8, String)>,
}

// TENT numeric map keys are serialized as arrays. Their iteration order is
// not an identity change. Preserve ordering in buffers, regions and QP arrays.
fn normalize(value: &mut serde_json::Value) -> Result<()> {
    match value {
        serde_json::Value::Object(fields) => {
            if let Some(attrs) = fields.get_mut("transport_attrs") {
                let pairs = attrs
                    .as_array_mut()
                    .ok_or(Error::Protocol("TENT transport attributes"))?;
                if pairs.iter().any(|p| {
                    p.as_array().is_none_or(|p| {
                        p.len() != 2 || p[0].as_u64().is_none() || !p[1].is_string()
                    })
                }) {
                    return Err(Error::Protocol("TENT transport attribute pair"));
                }
                pairs.sort_by_key(|p| p[0].as_u64());
                if pairs.windows(2).any(|p| p[0][0] == p[1][0]) {
                    return Err(Error::Protocol("duplicate TENT transport attribute"));
                }
            }
            for child in fields.values_mut() {
                normalize(child)?;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                normalize(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub(super) struct Buffer {
    pub(super) addr: u64,
    pub(super) length: u64,
    pub(super) location: String,
    pub(super) transports: Vec<u8>,
    pub(super) regions: Vec<Region>,
    #[serde(default)]
    pub(super) transport_attrs: Vec<(u8, String)>,
    pub(super) rkey: Vec<u32>,
    pub(super) shm_path: String,
    pub(super) mnnvl_handle: String,
    pub(super) internal: bool,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Region {
    pub(super) size: u64,
    pub(super) location: String,
}

pub(super) fn decode(
    bytes: &[u8],
    endpoint: SocketAddr,
    epoch: SourceEpoch,
) -> Result<RemoteSegment> {
    if bytes.len() > LIMIT {
        return Err(Error::ResourceExhausted);
    }
    let mut snapshot: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|_| Error::Protocol("invalid TENT segment metadata"))?;
    if snapshot.get("orbite_source_epoch").is_some() {
        return Err(Error::Unavailable(
            "OrbiTE peers require open_segment on the native endpoint, not the TENT profile",
        ));
    }
    if snapshot.get("type").and_then(serde_json::Value::as_str) == Some("file") {
        return Err(Error::Unavailable("TENT profile has no remote file owner"));
    }
    normalize(&mut snapshot)?;
    if let Some(topology) = snapshot
        .get_mut("detail")
        .and_then(|d| d.get_mut("topology"))
    {
        if let Some(text) = topology.as_str().filter(|t| !t.is_empty()) {
            let parsed: serde_json::Value =
                serde_json::from_str(text).map_err(|_| Error::Protocol("TENT topology JSON"))?;
            *topology = serde_json::Value::String(
                serde_json::to_string(&parsed)
                    .map_err(|_| Error::Internal("TENT topology serialization"))?,
            );
        }
    }
    let segment: Segment = serde_json::from_value(snapshot.clone())
        .map_err(|_| Error::Protocol("invalid TENT segment metadata"))?;
    if segment.kind != "memory" {
        return Err(Error::Unavailable(
            "TENT profile supports memory segments only",
        ));
    }
    if segment.name.is_empty()
        || segment.name.len() > 1024
        || segment.machine_id.len() > 1024
        || segment.rpc_server_addr.parse::<SocketAddr>().ok() != Some(endpoint)
    {
        // Do not follow peer-supplied RPC redirects away from the authoritative endpoint.
        return Err(Error::Protocol("TENT metadata endpoint/identity"));
    }
    if segment.detail.buffers.len() > super::super::MAX_REGIONS {
        return Err(Error::ResourceExhausted);
    }
    let mut regions = Vec::new();
    regions
        .try_reserve_exact(segment.detail.buffers.len())
        .map_err(|_| Error::ResourceExhausted)?;
    for buffer in segment.detail.buffers {
        let length = usize::try_from(buffer.length).map_err(|_| Error::InvalidRange)?;
        if buffer.addr == 0
            || length == 0
            || buffer.addr.checked_add(buffer.length).is_none()
            || buffer.location.len() > 64
            || buffer.regions.len() > super::super::MAX_REGIONS
            || buffer.transports.len() > 32
        {
            return Err(Error::Protocol("invalid TENT buffer range"));
        }
        if buffer.internal || !buffer.transports.contains(&Transport::Tcp.tent_id()) {
            continue;
        }
        let memory_type = if buffer.location == "*"
            || buffer.location == "cpu"
            || buffer.location.starts_with("cpu:")
        {
            MemoryType::Cpu
        } else if buffer.location == "cuda" || buffer.location.starts_with("cuda:") {
            MemoryType::Cuda
        } else {
            return Err(Error::Unavailable(
                "TENT buffer uses an excluded memory platform",
            ));
        };
        // TENT does not serialize permission. The source checks it at every access.
        regions.push(RemoteRegion {
            epoch,
            id: regions.len() as u64 + 1,
            address: buffer.addr,
            length,
            permission: Permission::RemoteReadWrite,
            memory_type,
            ipc_exportable: false,
            hp_registered: false,
            profile: RegionProfile {
                route: Transport::Tcp,
                node: None,
            },
            fabric: None,
        });
    }
    Ok(RemoteSegment {
        identity: None,
        endpoint,
        epoch,
        regions,
        metadata: SegmentMetadata {
            tent: Some(std::sync::Arc::new(snapshot)),
            max_transfer_bytes: 64 << 20,
            ..Default::default()
        },
    })
}

pub(super) fn encode(source: &crate::engine::Source, endpoint: SocketAddr) -> Result<Vec<u8>> {
    let (_, regions) = source.describe()?;
    let buffers = regions
        .into_iter()
        .filter(|r| r.profile.permits(Transport::Tcp))
        .map(|r| Buffer {
            addr: r.address,
            length: r.length as u64,
            location: r.location_name(),
            transports: vec![Transport::Tcp.tent_id()],
            regions: vec![Region {
                size: r.length as u64,
                location: r.location_name(),
            }],
            transport_attrs: Vec::new(),
            rkey: Vec::new(),
            shm_path: String::new(),
            mnnvl_handle: String::new(),
            internal: false,
        })
        .collect();
    let value = Segment {
        name: endpoint.to_string(),
        kind: "memory".into(),
        machine_id: source
            .metadata
            .boot
            .map_or_else(String::new, |id| id.to_string()),
        rpc_server_addr: endpoint.to_string(),
        orbite_source_epoch: Some(source.epoch.to_string()),
        detail: Memory {
            buffers,
            topology: String::new(),
            devices: Vec::new(),
            device_attrs: Default::default(),
            transport_attrs: Vec::new(),
        },
    };
    let bytes =
        serde_json::to_vec(&value).map_err(|_| Error::Internal("TENT segment serialization"))?;
    if bytes.len() > LIMIT {
        return Err(Error::ResourceExhausted);
    }
    Ok(bytes)
}

/// Bootstrap format only; OrbiTE's revocable RDMA grants are not replaced by TENT rkeys.
#[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
pub(super) struct Bootstrap {
    pub(super) local_nic_path: String,
    pub(super) peer_nic_path: String,
    pub(super) qp_num: Vec<u32>,
    pub(super) local_lid: u16,
    pub(super) local_gid: String,
    pub(super) reply_msg: String,
    pub(super) notify_qp_num: u32,
}

#[cfg(test)]
#[path = "../../../../tests/unit/runtime/tent/metadata.rs"]
mod tests;
