use std::fs;
use std::net::Ipv6Addr;
use std::os::unix::fs::FileTypeExt;
use std::path::Path;

use super::{Descriptor, RdmaConfig, sys};
use crate::platform::cpu_probe;
use crate::runtime::topology::Topology;
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GidType {
    Ib,
    RoceV1,
    RoceV2,
}

impl GidType {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Ib => "IB",
            Self::RoceV1 => "RoCE v1",
            Self::RoceV2 => "RoCE v2",
        }
    }
}

/// Selected port address and observed link state; publication waits for native drain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RdmaAddress {
    pub device: String,
    pub port: u8,
    pub active: bool,
    /// Effective rate when reported, otherwise encoded rate; None means unknown.
    pub link_speed_mbps: Option<u64>,
    pub effective_speed_mbps: Option<u64>,
    pub effective_speed_query_failures: u64,
    pub effective_speed_query_failing: bool,
    pub gid_index: u8,
    pub gid_type: GidType,
    pub net_device: Option<String>,
    pub net_device_index: u32,
    pub descriptor: Descriptor,
}

impl RdmaAddress {
    pub(super) fn same_route(&self, other: &Self) -> bool {
        self.device == other.device
            && self.port == other.port
            && self.active == other.active
            && self.gid_index == other.gid_index
            && self.gid_type == other.gid_type
            && self.net_device == other.net_device
            && self.net_device_index == other.net_device_index
            && self.descriptor == other.descriptor
    }

    pub(super) fn publish(&self, topology: &mut Topology) -> Result<()> {
        let nic = topology
            .nic_list
            .iter_mut()
            .find(|nic| nic.name == self.device)
            .ok_or(Error::Internal(
                "selected RDMA device is missing from topology",
            ))?;
        let attrs = &mut nic.device_attrs;
        for (key, rate) in [
            ("rdma.link_speed_mbps", self.link_speed_mbps),
            ("rdma.effective_speed_mbps", self.effective_speed_mbps),
        ] {
            if let Some(rate) = rate {
                attrs.insert(key.into(), rate.to_string());
            } else {
                attrs.remove(key);
            }
        }
        for (key, value) in [
            ("rdma.selected", "true".into()),
            (
                "rdma.effective_speed_query_failures",
                self.effective_speed_query_failures.to_string(),
            ),
            (
                "rdma.effective_speed_query_failing",
                self.effective_speed_query_failing.to_string(),
            ),
            ("rdma.port", self.port.to_string()),
            ("rdma.gid_index", self.gid_index.to_string()),
            ("rdma.gid", Ipv6Addr::from(self.descriptor.gid).to_string()),
            ("rdma.gid_type", self.gid_type.name().into()),
            ("rdma.net_device_index", self.net_device_index.to_string()),
            ("rdma.lid", self.descriptor.lid.to_string()),
            (
                "rdma.mtu_bytes",
                (128u32 << self.descriptor.mtu).to_string(),
            ),
            ("rdma.link_layer", self.descriptor.link_layer.to_string()),
        ] {
            attrs.insert(key.into(), value);
        }
        if let Some(net_device) = &self.net_device {
            attrs.insert("rdma.net_device".into(), net_device.clone());
        }
        Ok(())
    }
}

pub(super) fn permitted(config: &RdmaConfig, device: &str) -> bool {
    config.device.as_deref().is_none_or(|pin| pin == device)
        && if config.rdma_whitelist.is_empty() {
            !config.rdma_blacklist.iter().any(|name| name == device)
        } else {
            config.rdma_whitelist.iter().any(|name| name == device)
        }
}

fn text(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path)?.trim().to_owned())
}

pub(super) fn gid_metadata(
    sys: &Path,
    device: &str,
    port: u8,
    index: u8,
    link_layer: u8,
) -> Result<(GidType, Option<String>, u32)> {
    let attrs = sys
        .join("class/infiniband")
        .join(device)
        .join("ports")
        .join(port.to_string())
        .join("gid_attrs");
    let kind = match text(&attrs.join("types").join(index.to_string())) {
        Ok(kind) => kind,
        Err(Error::Io(error))
            if link_layer == 1
                && (error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::EINVAL)) =>
        {
            "IB/RoCE v1".into()
        }
        Err(error) => return Err(error),
    };
    let kind = match (link_layer, kind.as_str()) {
        (1, "IB/RoCE v1") => GidType::Ib,
        (2, "IB/RoCE v1") => GidType::RoceV1,
        (2, "RoCE v2") => GidType::RoceV2,
        _ => return Err(Error::Unavailable("unsupported RDMA GID type")),
    };
    let net_device = match text(&attrs.join("ndevs").join(index.to_string())) {
        Ok(value) => value,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };
    let index = if net_device.is_empty() {
        0
    } else {
        if net_device.contains('/') || matches!(net_device.as_str(), "." | "..") {
            return Err(Error::Unavailable("invalid RDMA network device"));
        }
        text(&sys.join("class/net").join(&net_device).join("ifindex"))?
            .parse::<u32>()
            .ok()
            .filter(|index| *index != 0)
            .ok_or(Error::Unavailable("invalid RDMA network device index"))?
    };
    Ok((kind, (!net_device.is_empty()).then_some(net_device), index))
}

fn accessible(dev: &Path, name: &str) -> bool {
    let path = dev.join("infiniband").join(name);
    if !fs::metadata(&path).is_ok_and(|meta| meta.file_type().is_char_device()) {
        return false;
    }
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: access only checks the existing character node; it never opens an RNIC.
    unsafe { libc::access(path.as_ptr(), libc::R_OK | libc::W_OK) == 0 }
}

pub(super) fn discover(config: &RdmaConfig) -> Result<(Topology, Vec<RdmaAddress>)> {
    let devices = sys::devices()?;
    let (topology, routes) = discover_at(config, Path::new("/sys"), Path::new("/dev"), &devices)?;
    #[cfg(feature = "cuda")]
    let topology = {
        let mut topology = topology;
        crate::cuda::discover_topology(&mut topology, Path::new("/sys"))?;
        topology
    };
    Ok((topology, routes))
}

fn discover_at(
    config: &RdmaConfig,
    sys: &Path,
    dev: &Path,
    devices: &[(String, String)],
) -> Result<(Topology, Vec<RdmaAddress>)> {
    let mut names: Vec<_> = devices
        .iter()
        // Filtering precedes every access check and every native open.
        .filter(|(name, _)| permitted(config, name))
        .filter(|(_, node)| accessible(dev, node))
        .map(|(name, _)| name.clone())
        .collect();
    names.sort();
    names.dedup();
    let mut topology = cpu_probe::discover(sys, &names);
    let mut candidates = Vec::new();
    for name in &names {
        let ports_path = sys.join("class/infiniband").join(name).join("ports");
        let Ok(ports) = fs::read_dir(&ports_path) else {
            continue;
        };
        for port in ports
            .flatten()
            .filter_map(|entry| entry.file_name().to_str()?.parse::<u8>().ok())
        {
            if port == 0 || config.port.is_some_and(|pin| pin != port) {
                continue;
            }
            candidates.extend(port_addresses(sys, name, port, config.gid_index));
        }
    }
    // A NIC is inventoried only when at least one route satisfies the selectors.
    topology
        .nic_list
        .retain(|nic| candidates.iter().any(|route| route.device == nic.name));
    topology.rank();
    candidates.sort_by_key(|route| {
        let nic = topology
            .nic_list
            .iter()
            .find(|nic| nic.name == route.device);
        let local = config.preferred_numa_node.is_some()
            && nic.is_some_and(|nic| nic.numa_node == config.preferred_numa_node);
        (
            !route.active,
            !local,
            gid_priority(sys, route),
            route.device.clone(),
            route.port,
            route.gid_index,
        )
    });
    if candidates.is_empty() {
        return Err(Error::Unavailable(
            "no accessible RDMA route matches selectors",
        ));
    }
    Ok((topology, candidates))
}

fn gid_priority(sys: &Path, route: &RdmaAddress) -> (bool, bool, bool, bool) {
    let overlay = route
        .net_device
        .as_ref()
        .is_some_and(|name| sys.join("devices/virtual/net").join(name).exists());
    let preferred = matches!(route.gid_type, GidType::Ib | GidType::RoceV2);
    (
        !preferred,
        route.net_device.is_none(),
        overlay,
        Ipv6Addr::from(route.descriptor.gid).is_unicast_link_local(),
    )
}

pub(super) fn port_addresses(
    sys: &Path,
    device: &str,
    port: u8,
    gid_index: Option<u8>,
) -> Vec<RdmaAddress> {
    let path = sys
        .join("class/infiniband")
        .join(device)
        .join("ports")
        .join(port.to_string());
    let active = match text(&path.join("state"))
        .ok()
        .and_then(|value| value.split(':').next()?.trim().parse::<u8>().ok())
    {
        Some(state @ 1..=5) => state == 4,
        _ => return Vec::new(),
    };
    let link_layer = match text(&path.join("link_layer")).as_deref() {
        Ok("InfiniBand") => 1,
        Ok("Ethernet") => 2,
        _ => return Vec::new(),
    };
    let Ok(gids) = fs::read_dir(path.join("gids")) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    for index in gids
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u8>().ok())
    {
        if gid_index.is_some_and(|pin| pin != index) {
            continue;
        }
        let Some(gid) = text(&path.join("gids").join(index.to_string()))
            .ok()
            .and_then(|value| value.parse::<Ipv6Addr>().ok())
            .filter(|gid| !gid.is_unspecified() && !gid.is_multicast())
        else {
            continue;
        };
        let Ok((gid_type, net_device, net_device_index)) =
            gid_metadata(sys, device, port, index, link_layer)
        else {
            continue;
        };
        candidates.push(RdmaAddress {
            device: device.into(),
            port,
            active,
            link_speed_mbps: None,
            effective_speed_mbps: None,
            effective_speed_query_failures: 0,
            effective_speed_query_failing: false,
            gid_index: index,
            gid_type,
            net_device,
            net_device_index,
            descriptor: Descriptor {
                gid: gid.octets(),
                link_layer,
                ..Descriptor::default()
            },
        });
    }
    candidates.sort_by_key(|route| (gid_priority(sys, route), route.gid_index));
    candidates
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/discovery.rs"]
mod tests;
