//! TENT custom topology input, resolved against discovered physical owners.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::Read;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::{DEVICE_PRIORITY_RANKS, MemEntry, MemType, Topology};
use crate::transport::rdma::RdmaConfig;
use crate::{Error, Result};

const MAX_JSON_BYTES: usize = 1 << 20;
type Ranks = [Vec<usize>; DEVICE_PRIORITY_RANKS];
type Rows = BTreeMap<String, Ranks>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeTopology {
    nics: Vec<NativeNic>,
    mems: Vec<NativeMemory>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeNic {
    name: String,
    #[serde(rename = "type", default)]
    kind: u8,
    #[serde(rename = "pci_bus_id", default)]
    _pci_bus_id: Option<String>,
    #[serde(rename = "numa_node", default)]
    _numa_node: Option<i64>,
    #[serde(rename = "device_attrs", default)]
    _device_attrs: BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeMemory {
    name: String,
    #[serde(rename = "type")]
    kind: Option<u8>,
    #[serde(rename = "pci_bus_id", default)]
    _pci_bus_id: Option<String>,
    #[serde(rename = "numa_node", default)]
    _numa_node: Option<i64>,
    device_list: NativeRanks,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NativeRanks {
    rank0: Vec<usize>,
    rank1: Vec<usize>,
    rank2: Vec<usize>,
}

struct PriorityMatrix(BTreeMap<String, [Vec<String>; 2]>);

impl<'de> Deserialize<'de> for PriorityMatrix {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct MatrixVisitor;
        impl<'de> Visitor<'de> for MatrixVisitor {
            type Value = PriorityMatrix;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a location -> [preferred NICs, available NICs] object")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut rows = BTreeMap::new();
                while let Some((name, ranks)) = map.next_entry()? {
                    if rows.insert(name, ranks).is_some() {
                        return Err(de::Error::custom("duplicate topology location"));
                    }
                }
                Ok(PriorityMatrix(rows))
            }
        }
        deserializer.deserialize_map(MatrixVisitor)
    }
}

impl Topology {
    pub(crate) fn load_from_config(&mut self, config: &RdmaConfig) -> Result<()> {
        let rows = if let Some(json) = &config.priority_matrix {
            check_size(json)?;
            self.parse_priority_matrix(json)?
        } else if let Some(path) = &config.custom_json_path {
            let mut json = String::new();
            File::open(path)?
                .take((MAX_JSON_BYTES + 1) as u64)
                .read_to_string(&mut json)?;
            check_size(&json)?;
            self.parse_custom_topology(&json)?
        } else {
            return Ok(());
        };
        // Resolve and validate everything before changing the inventory. Neither
        // user metadata nor JSON array order may replace physical device identity.
        let mut memory = self.mem_list.clone();
        if rows.contains_key("*") && !memory.iter().any(|entry| entry.name == "*") {
            memory.push(MemEntry {
                name: "*".into(),
                pci_bus_id: None,
                kind: MemType::Host,
                numa_node: None,
                device_list: Default::default(),
            });
        }
        for entry in &mut memory {
            if let Some(ranks) = rows.get(&entry.name).or_else(|| rows.get("*")) {
                entry.device_list.clone_from(ranks);
            }
        }
        self.mem_list = memory;
        Ok(())
    }

    fn nic_id(&self, name: &str) -> Result<usize> {
        self.nic_list
            .iter()
            .position(|nic| nic.name == name)
            .ok_or(Error::InvalidConfig(
                "custom topology NIC is absent from the selected inventory",
            ))
    }

    fn memory_kind(&self, name: &str) -> Result<MemType> {
        if name == "*" {
            return Ok(MemType::Host);
        }
        self.mem_list
            .iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.kind)
            .ok_or(Error::InvalidConfig(
                "custom topology memory location is absent from discovery",
            ))
    }

    fn parse_priority_matrix(&self, json: &str) -> Result<Rows> {
        let PriorityMatrix(matrix) = serde_json::from_str(json).map_err(|_| Error::InvalidConfig(
            "priority_matrix must map unique locations to [preferred NIC names, available NIC names]",
        ))?;
        let mut rows = Rows::new();
        let mut union: [BTreeSet<usize>; 2] = Default::default();
        for (name, devices) in matrix {
            self.memory_kind(&name)?;
            let mut ranks = Ranks::default();
            for (rank, names) in devices.into_iter().enumerate() {
                for nic in names {
                    let id = self.nic_id(&nic)?;
                    ranks[rank].push(id);
                    union[rank].insert(id);
                }
            }
            validate_ranks(&ranks)?;
            rows.insert(name, ranks);
        }
        rows.entry("*".into()).or_insert_with(|| {
            [
                union[0].iter().copied().collect(),
                union[1].difference(&union[0]).copied().collect(),
                vec![],
            ]
        });
        Ok(rows)
    }

    fn parse_custom_topology(&self, json: &str) -> Result<Rows> {
        let value: serde_json::Value = serde_json::from_str(json)
            .map_err(|_| Error::InvalidConfig("custom topology must be valid JSON"))?;
        if value.get("nics").is_none() && value.get("mems").is_none() {
            return self.parse_priority_matrix(json);
        }
        let input: NativeTopology = serde_json::from_str(json)
            .map_err(|_| Error::InvalidConfig("invalid native nics/mems topology JSON"))?;
        let mut ids = Vec::with_capacity(input.nics.len());
        let mut seen = BTreeSet::new();
        for nic in input.nics {
            let id = self.nic_id(&nic.name)?;
            if nic.kind != 0 || !seen.insert(id) {
                return Err(Error::InvalidConfig(
                    "custom topology requires unique RDMA NICs",
                ));
            }
            ids.push(id);
        }
        let mut rows = Rows::new();
        for memory in input.mems {
            let kind = self.memory_kind(&memory.name)?;
            let expected = match kind {
                MemType::Host => 0,
                MemType::Cuda => 1,
            };
            if memory
                .kind
                .is_some_and(|kind| kind != expected && !(memory.name == "*" && kind == 4))
            {
                return Err(Error::InvalidConfig(
                    "custom topology memory type disagrees with discovery",
                ));
            }
            let mut ranks = [
                memory.device_list.rank0,
                memory.device_list.rank1,
                memory.device_list.rank2,
            ];
            for id in ranks.iter_mut().flatten() {
                *id = *ids.get(*id).ok_or(Error::InvalidConfig(
                    "custom topology NIC index is out of range",
                ))?;
            }
            validate_ranks(&ranks)?;
            if rows.insert(memory.name, ranks).is_some() {
                return Err(Error::InvalidConfig(
                    "duplicate custom topology memory location",
                ));
            }
        }
        Ok(rows)
    }
}

fn check_size(json: &str) -> Result<()> {
    if json.len() > MAX_JSON_BYTES {
        return Err(Error::InvalidConfig("custom topology JSON exceeds 1 MiB"));
    }
    Ok(())
}

fn validate_ranks(ranks: &Ranks) -> Result<()> {
    let mut seen = BTreeSet::new();
    if ranks.iter().flatten().any(|id| !seen.insert(*id)) {
        return Err(Error::InvalidConfig(
            "duplicate NIC in custom topology ranks",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/topology/config.rs"]
mod tests;
