//! Linux CPU/RNIC topology discovery, matching TENT `platform/cpu_probe.cpp`.

use std::fs;
use std::path::Path;

use crate::runtime::topology::{MemEntry, MemType, NicEntry, NicType, Topology};

pub(crate) fn discover(sys: &Path, devices: &[String]) -> Topology {
    let nic_list = devices
        .iter()
        .map(|name| {
            let device = sys.join("class/infiniband").join(name).join("device");
            let pci_bus_id = device.canonicalize().ok().and_then(|path| {
                let value = path.file_name()?.to_str()?;
                super::pci::bus_id(value)
            });
            let numa_node = fs::read_to_string(device.join("numa_node"))
                .ok()
                .and_then(|value| value.trim().parse::<u32>().ok());
            NicEntry {
                name: name.clone(),
                pci_bus_id,
                kind: NicType::Rdma,
                numa_node,
                device_attrs: Default::default(),
            }
        })
        .collect();
    let mut nodes: Vec<u32> = fs::read_dir(sys.join("devices/system/node"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()?
                .strip_prefix("node")?
                .parse()
                .ok()
        })
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    let mut mem_list: Vec<_> = nodes
        .into_iter()
        .map(|node| MemEntry {
            name: format!("cpu:{node}"),
            pci_bus_id: None,
            kind: MemType::Host,
            numa_node: Some(node),
            device_list: Default::default(),
        })
        .collect();
    mem_list.push(MemEntry {
        name: "*".into(),
        pci_bus_id: None,
        kind: MemType::Host,
        numa_node: None,
        device_list: Default::default(),
    });
    let mut topology = Topology { nic_list, mem_list };
    topology.rank();
    topology
}

#[cfg(feature = "rdma")]
mod affinity;
#[cfg(all(test, feature = "rdma"))]
pub(crate) use affinity::allowed_cpus;
#[cfg(feature = "rdma")]
pub(crate) use affinity::{CpuAffinity, worker_affinities};
