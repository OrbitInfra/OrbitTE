use std::collections::BTreeMap;

/// TENT's three NIC priority ranks, ordered from preferred to fallback.
pub const DEVICE_PRIORITY_RANKS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NicType {
    Rdma,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemType {
    Host,
    Cuda,
}

/// A provider-visible NIC. Unknown PCI or NUMA placement stays unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NicEntry {
    pub name: String,
    pub pci_bus_id: Option<String>,
    pub kind: NicType,
    pub numa_node: Option<u32>,
    /// Namespaced transport metadata; only rdma.selected marks a validated path.
    pub device_attrs: BTreeMap<String, String>,
}

/// A memory location and its ranked indices into Topology::nic_list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemEntry {
    pub name: String,
    pub pci_bus_id: Option<String>,
    pub kind: MemType,
    pub numa_node: Option<u32>,
    pub device_list: [Vec<usize>; DEVICE_PRIORITY_RANKS],
}

/// An immutable inventory snapshot, not a claim about allocation placement.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Topology {
    pub nic_list: Vec<NicEntry>,
    pub mem_list: Vec<MemEntry>,
}

impl Topology {
    pub(crate) fn rank(&mut self) {
        for memory in self
            .mem_list
            .iter_mut()
            .filter(|entry| entry.kind == MemType::Host)
        {
            memory.device_list = Default::default();
            for (index, nic) in self.nic_list.iter().enumerate() {
                let rank = if memory.numa_node.is_some() && memory.numa_node == nic.numa_node {
                    0
                } else {
                    2
                };
                memory.device_list[rank].push(index);
            }
        }
    }
}
