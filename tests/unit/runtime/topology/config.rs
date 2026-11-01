use super::*;
use crate::{NicEntry, NicType};

fn inventory() -> Topology {
    let mut topology = Topology {
        nic_list: (0..3)
            .map(|id| NicEntry {
                name: format!("nic{id}"),
                pci_bus_id: Some(format!("0000:0{id}:00.0")),
                kind: NicType::Rdma,
                numa_node: Some(id % 2),
                device_attrs: BTreeMap::new(),
            })
            .collect(),
        mem_list: ["*", "cpu:0", "cpu:1", "cuda:0"]
            .into_iter()
            .map(|name| MemEntry {
                name: name.into(),
                pci_bus_id: None,
                kind: if name == "cuda:0" {
                    MemType::Cuda
                } else {
                    MemType::Host
                },
                numa_node: if name == "*" {
                    None
                } else {
                    Some(u32::from(name == "cpu:1"))
                },
                device_list: [vec![0], vec![1], vec![2]],
            })
            .collect(),
    };
    topology.rank();
    topology
}

struct JsonFile(std::path::PathBuf);
impl JsonFile {
    fn new(json: &str) -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "orbitte-topology-{}-{}.json",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::write(&path, json).unwrap();
        Self(path)
    }
    fn config(&self) -> RdmaConfig {
        RdmaConfig {
            custom_json_path: Some(self.0.clone()),
            ..Default::default()
        }
    }
}
impl Drop for JsonFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn matrix_resolves_stable_ids_and_synthesizes_a_disjoint_wildcard() {
    let mut topology = inventory();
    let original = topology.clone();
    topology
        .load_from_config(&RdmaConfig {
            priority_matrix: Some(
                r#"{"cpu:0":[["nic2"],["nic1"]],"cuda:0":[["nic1"],["nic2"]]}"#.into(),
            ),
            custom_json_path: Some("/nonexistent/ignored-by-inline.json".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(topology.nic_list, original.nic_list);
    for entry in &topology.mem_list {
        let before = original
            .mem_list
            .iter()
            .find(|old| old.name == entry.name)
            .unwrap();
        assert_eq!(
            (&entry.pci_bus_id, entry.numa_node, entry.kind),
            (&before.pci_bus_id, before.numa_node, before.kind)
        );
        assert_eq!(
            entry.device_list,
            match entry.name.as_str() {
                "cpu:0" => [vec![2], vec![1], vec![]],
                "cuda:0" => [vec![1], vec![2], vec![]],
                _ => [vec![1, 2], vec![], vec![]],
            }
        );
    }
}

#[test]
fn native_file_remaps_indices_and_cannot_replace_discovered_physical_metadata() {
    let file = JsonFile::new(
        r#"{
      "nics":[{"name":"nic2","type":0,"numa_node":99,"pci_bus_id":"pretend"},
              {"name":"nic0","numa_node":99}],
      "mems":[{"name":"cuda:0","type":1,"numa_node":99,"pci_bus_id":"pretend",
               "device_list":{"rank0":[0],"rank2":[1]}}]
    }"#,
    );
    let mut topology = inventory();
    let original = topology.clone();
    topology.load_from_config(&file.config()).unwrap();
    assert_eq!(topology.nic_list, original.nic_list);
    assert_eq!(&topology.mem_list[..3], &original.mem_list[..3]);
    let entry = &topology.mem_list[3];
    assert_eq!(entry.device_list, [vec![2], vec![], vec![0]]);
    assert_eq!((entry.numa_node, &entry.pci_bus_id), (Some(0), &None));
    // The file may also carry the classic two-rank matrix. An explicit wildcard
    // wins over synthesis and empty rows deliberately disable that location.
    std::fs::write(&file.0, r#"{"*":[["nic2"],[]],"cpu:0":[[],[]]}"#).unwrap();
    topology.load_from_config(&file.config()).unwrap();
    assert_eq!(topology.mem_list[1].device_list, Ranks::default());
    assert_eq!(topology.mem_list[3].device_list, [vec![2], vec![], vec![]]);
}

#[test]
fn invalid_explicit_configuration_is_atomic_and_never_falls_back() {
    let mut topology = inventory();
    let before = topology.clone();
    for json in [
        "",
        "[]",
        r#"{"cpu:0":[["nic0"]]}"#,
        r#"{"cpu:0":[[42],[]]}"#,
        r#"{"cpu:0":[["nic0"],[]],"cpu:0":[["nic1"],[]]}"#,
        r#"{"cpu:00":[["nic0"],[]]}"#,
        r#"{"cpu:99":[["nic0"],[]]}"#,
        r#"{"hip:0":[["nic0"],[]]}"#,
        r#"{"cuda:1":[["nic0"],[]]}"#,
        r#"{"cpu:0":[["missing"],[]]}"#,
        r#"{"cpu:0":[["nic0"],["nic0"]]}"#,
    ] {
        let file = JsonFile::new(r#"{"*":[["nic2"],[]]}"#);
        let mut config = file.config();
        config.priority_matrix = Some(json.into());
        assert!(
            matches!(
                topology.load_from_config(&config),
                Err(Error::InvalidConfig(_))
            ),
            "{json}"
        );
        assert_eq!(topology, before);
    }
    for json in [
        r#"{"nics":[{"name":"nic0"}],"mems":[{"name":"cpu:0","device_list":{"rank0":[1]}}]}"#,
        r#"{"nics":[{"name":"nic0"},{"name":"nic0"}],"mems":[]}"#,
        r#"{"nics":[{"name":"nic0","type":1}],"mems":[]}"#,
        r#"{"nics":[],"mems":[{"name":"cuda:0","type":0,"device_list":{}}]}"#,
        r#"{"nics":[],"mems":[{"name":"cpu:0","device_list":{}},{"name":"cpu:0","device_list":{}}]}"#,
    ] {
        let file = JsonFile::new(json);
        assert!(
            matches!(
                topology.load_from_config(&file.config()),
                Err(Error::InvalidConfig(_))
            ),
            "{json}"
        );
        assert_eq!(topology, before);
    }
    let file = JsonFile::new(&" ".repeat(MAX_JSON_BYTES + 1));
    assert!(matches!(
        topology.load_from_config(&file.config()),
        Err(Error::InvalidConfig(_))
    ));
    std::fs::remove_file(&file.0).unwrap();
    assert!(matches!(
        topology.load_from_config(&file.config()),
        Err(Error::Io(_))
    ));
    assert_eq!(topology, before);
}
