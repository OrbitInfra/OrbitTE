//! Device inventory and TENT's three GPU-to-RNIC priority tiers.

use std::path::Path;

use super::*;
use crate::platform::pci;
use crate::{MemEntry, MemType, Topology};

pub(crate) fn discover(topology: &mut Topology, sys: &Path) -> Result<()> {
    let driver = match Driver::load() {
        Ok(driver) => driver,
        Err(Error::Unavailable("CUDA driver libcuda.so.1"))
        | Err(Error::Backend { code: 100, .. }) => return Ok(()),
        Err(error) => return Err(error),
    };
    driver.probe_topology(topology, sys)
}

impl Driver {
    pub(in crate::cuda) fn probe_topology(
        &self,
        topology: &mut Topology,
        sys: &Path,
    ) -> Result<()> {
        let mut count = 0;
        // SAFETY: initialized output; enumeration creates no CUDA context.
        check("cuDeviceGetCount", unsafe {
            (self.device_count)(&mut count)
        })?;
        if !(0..=65536).contains(&count) {
            return Err(Error::ResourceExhausted);
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count as usize)
            .map_err(|_| Error::ResourceExhausted)?;
        for ordinal in 0..count {
            let mut device = 0;
            // SAFETY: ordinal is bounded by this driver's visible device count.
            check("cuDeviceGet", unsafe {
                (self.device_get)(&mut device, ordinal)
            })?;
            let bus = self.pci_bus(device)?;
            let node = pci::numa_node(sys, &bus);
            let distances: Vec<_> = topology
                .nic_list
                .iter()
                .map(|nic| {
                    nic.pci_bus_id
                        .as_deref()
                        .and_then(|nic| pci::distance(sys, &bus, nic))
                })
                .collect();
            let nearest = distances.iter().flatten().copied().min();
            let mut entry = MemEntry {
                name: format!("cuda:{ordinal}"),
                pci_bus_id: Some(bus),
                kind: MemType::Cuda,
                numa_node: node,
                device_list: Default::default(),
            };
            for (id, nic) in topology.nic_list.iter().enumerate() {
                let rank = if nearest.is_some() && distances[id] == nearest {
                    0
                } else if node.is_some() && node == nic.numa_node {
                    1
                } else {
                    2
                };
                entry.device_list[rank].push(id);
            }
            entries.push(entry);
        }
        // Publish a complete snapshot only after all native queries succeed.
        topology.mem_list.extend(entries);
        Ok(())
    }
}
