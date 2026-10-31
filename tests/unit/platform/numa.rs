use super::*;

#[test]
fn discovery_filters_memoryless_offline_disallowed_and_sparse_nodes() {
    let root = std::env::temp_dir().join(format!("orbitte-staging-numa-{}", std::process::id()));
    let mask = node_mask(130).unwrap();
    assert_eq!(mask.iter().map(|word| word.count_ones()).sum::<u32>(), 1);
    assert!(node_mask(65536).is_err());
    for (node, size, online) in [(2, 10, "1"), (130, 10, "1"), (131, 0, "1"), (132, 10, "0")] {
        let dir = root.join(format!("devices/system/node/node{node}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("meminfo"),
            format!("Node {node} MemTotal: {size} kB\n"),
        )
        .unwrap();
        std::fs::write(dir.join("online"), online).unwrap();
    }
    let mut allowed = node_mask(132).unwrap();
    allowed[130 / libc::c_ulong::BITS as usize] |= 3 << (130 % libc::c_ulong::BITS);
    assert_eq!(memory_nodes(&root, &allowed), [130]);
    let dir = root.join("bus/pci/devices/0000:82:00.0");
    std::fs::create_dir_all(&dir).unwrap();
    for (value, expected) in [("130", Some(130)), ("-1", None), ("bad", None)] {
        std::fs::write(dir.join("numa_node"), value).unwrap();
        assert_eq!(pci_node(&root, "0000:82:00.0"), expected);
    }
    assert_eq!(pci_node(&root, "../../node0"), None);
    assert_eq!(pci_node(&root, "0000:00:00.0"), None);
    std::fs::remove_dir_all(root).unwrap();
}
