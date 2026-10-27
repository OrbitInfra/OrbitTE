use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

struct Fixture {
    root: PathBuf,
    devices: Vec<(String, String)>,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "orbitte-topology-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        Self {
            root,
            devices: Vec::new(),
        }
    }
    fn put(&self, path: &str, value: &str) {
        let path = self.root.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, value).unwrap();
    }
    fn nic(&mut self, name: &str, node: i32) {
        let index = self.devices.len();
        let uverbs = format!("uverbs{index}");
        fs::create_dir_all(self.root.join("dev/infiniband")).unwrap();
        symlink("/dev/null", self.root.join("dev/infiniband").join(&uverbs)).unwrap();
        let pci = format!("sys/devices/pci0000:00/0000:{index:02x}:00.0");
        self.put(&format!("{pci}/numa_node"), &node.to_string());
        let device = self
            .root
            .join("sys/class/infiniband")
            .join(name)
            .join("device");
        fs::create_dir_all(device.parent().unwrap()).unwrap();
        symlink(self.root.join(pci), device).unwrap();
        self.devices.push((name.into(), uverbs));
    }
    fn gid(&self, name: &str, port: u8, index: u8, gid: &str, kind: &str, net: &str) {
        if !net.is_empty() {
            self.put(&format!("sys/class/net/{net}/ifindex"), "7");
        }
        let base = format!("sys/class/infiniband/{name}/ports/{port}");
        self.put(&format!("{base}/state"), "4: ACTIVE\n");
        self.put(&format!("{base}/link_layer"), "Ethernet\n");
        self.put(&format!("{base}/gids/{index}"), gid);
        self.put(&format!("{base}/gid_attrs/types/{index}"), kind);
        self.put(&format!("{base}/gid_attrs/ndevs/{index}"), net);
    }
    fn discover(&self, config: &RdmaConfig) -> Result<(Topology, Vec<RdmaAddress>)> {
        config.validate()?;
        discover_at(
            config,
            &self.root.join("sys"),
            &self.root.join("dev"),
            &self.devices,
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn provider_intersection_ranks_known_numa_and_real_ipv6_rocev2() {
    let mut f = Fixture::new();
    f.nic("mlx5_0", 0);
    f.nic("mlx5_1", 1);
    f.nic("soft", -1);
    f.put("sys/devices/system/node/node0/cpulist", "0-3");
    f.put("sys/devices/system/node/node1/cpulist", "4-7");
    for name in ["mlx5_0", "mlx5_1", "soft"] {
        f.gid(name, 1, 0, "::", "RoCE v2", "eth0");
        f.gid(name, 1, 1, "fe80::1", "RoCE v2", "eth0");
        f.gid(name, 1, 2, "2605::1", "IB/RoCE v1", "eth0");
        f.gid(name, 1, 3, "2605::1", "RoCE v2", "eth0");
        f.gid(name, 2, 0, "::ffff:192.0.2.1", "RoCE v2", "eth1");
    }
    f.gid("not_a_provider", 1, 0, "2605::2", "RoCE v2", "eth2");
    let config = RdmaConfig {
        preferred_numa_node: Some(1),
        ..Default::default()
    };
    let (topology, routes) = f.discover(&config).unwrap();
    assert_eq!(
        (&*routes[0].device, routes[0].port, routes[0].gid_index),
        ("mlx5_1", 1, 3)
    );
    assert_eq!(topology.nic_list.len(), 3);
    assert_eq!(topology.nic_list[2].numa_node, None);
    assert_eq!(
        topology.nic_list[1].pci_bus_id.as_deref(),
        Some("0000:01:00.0")
    );
    assert_eq!(
        topology.mem_list[0].device_list,
        [vec![0], vec![], vec![1, 2]]
    );
    assert_eq!(
        topology.mem_list[1].device_list,
        [vec![1], vec![], vec![0, 2]]
    );
    assert_eq!(topology.mem_list[2].name, "*");
    assert_eq!(
        topology.mem_list[2].device_list,
        [vec![], vec![], vec![0, 1, 2]]
    );
    assert!(routes.iter().all(|route| route.descriptor.gid != [0; 16]));
    let (_, without_hint) = f.discover(&RdmaConfig::default()).unwrap();
    assert_eq!(&*without_hint[0].device, "mlx5_0");
}

#[test]
fn pins_and_whitelist_are_strict_and_gid_zero_is_not_auto() {
    let mut f = Fixture::new();
    f.nic("a", 0);
    f.nic("b", 0);
    f.gid("a", 1, 0, "::ffff:192.0.2.1", "IB/RoCE v1", "eth0");
    f.gid("a", 1, 3, "2605::1", "RoCE v2", "eth0");
    f.gid("b", 2, 3, "2605::2", "RoCE v2", "eth1");
    let (_, routes) = f.discover(&RdmaConfig::new("a")).unwrap();
    assert_eq!((routes.len(), routes[0].gid_index), (1, 0));
    let mut config = RdmaConfig {
        rdma_whitelist: vec!["b".into()],
        rdma_blacklist: vec!["b".into()],
        ..Default::default()
    };
    let (topology, routes) = f.discover(&config).unwrap();
    assert_eq!(topology.nic_list.len(), 1);
    assert_eq!((&*routes[0].device, routes[0].port), ("b", 2));
    config.device = Some("a".into());
    assert!(matches!(f.discover(&config), Err(Error::InvalidConfig(_))));
    config.device = Some("b".into());
    config.port = Some(1);
    assert!(matches!(f.discover(&config), Err(Error::Unavailable(_))));
    config.port = Some(2);
    config.gid_index = Some(0);
    assert!(matches!(f.discover(&config), Err(Error::Unavailable(_))));
    config.gid_index = Some(3);
    assert_eq!(f.discover(&config).unwrap().1.len(), 1);
    // Matching routes prevent port/GID filters from concealing a missing device pin.
    f.gid("b", 1, 0, "::ffff:192.0.2.2", "IB/RoCE v1", "eth1");
    let (_, pinned) = f.discover(&RdmaConfig::new("a")).unwrap();
    assert_eq!(pinned.len(), 1);
    assert!(pinned.iter().all(|route| route.device == "a"));
    let blacklist = RdmaConfig {
        rdma_blacklist: vec!["a".into()],
        ..Default::default()
    };
    let (inventory, allowed) = f.discover(&blacklist).unwrap();
    assert_eq!(inventory.nic_list.len(), 1);
    assert!(allowed.iter().all(|route| route.device == "b"));
    for name in ["", "../a", "a/b", ".", "a\0b"] {
        assert!(RdmaConfig::new(name).validate().is_err());
    }
    assert!(
        RdmaConfig {
            port: Some(0),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn gid_ranking_prefers_physical_v2_but_keeps_ib_and_valid_fallbacks() {
    let mut f = Fixture::new();
    f.nic("a", -1);
    f.gid("a", 1, 0, "2605::1", "IB/RoCE v1", "eth0");
    f.gid("a", 1, 1, "2605::1", "RoCE v2", "vxlan0");
    f.gid("a", 1, 2, "2605::1", "RoCE v2", "");
    f.gid("a", 1, 3, "::ffff:192.0.2.1", "RoCE v2", "eth0");
    f.put("sys/devices/virtual/net/vxlan0/uevent", "");
    let (_, routes) = f.discover(&RdmaConfig::default()).unwrap();
    assert_eq!(
        routes.iter().map(|r| r.gid_index).collect::<Vec<_>>(),
        [3, 1, 2, 0]
    );
    f.put("sys/class/infiniband/a/ports/1/link_layer", "InfiniBand");
    let (_, routes) = f.discover(&RdmaConfig::default()).unwrap();
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].gid_type, GidType::Ib);
    fs::remove_file(
        f.root
            .join("sys/class/infiniband/a/ports/1/gid_attrs/types/0"),
    )
    .unwrap();
    fs::remove_file(
        f.root
            .join("sys/class/infiniband/a/ports/1/gid_attrs/ndevs/0"),
    )
    .unwrap();
    assert_eq!(
        f.discover(&RdmaConfig::default()).unwrap().1[0].net_device,
        None
    );
    f.put("sys/class/infiniband/a/ports/1/state", "2: INIT");
    let (_, routes) = f.discover(&RdmaConfig::default()).unwrap();
    assert_eq!(routes.len(), 1);
    assert!(!routes[0].active);
}

#[test]
fn removed_malformed_unaddressable_and_inaccessible_entries_never_become_candidates() {
    let mut f = Fixture::new();
    f.nic("a", -1);
    f.gid("a", 1, 0, "garbage", "RoCE v2", "eth0");
    f.gid("a", 1, 1, "ff02::1", "RoCE v2", "eth0");
    f.gid("a", 1, 255, "2605::1", "RoCE v2", "eth0");
    f.put("sys/class/infiniband/a/ports/1/gids/256", "2605::2");
    let (topology, routes) = f.discover(&RdmaConfig::default()).unwrap();
    assert_eq!((routes.len(), routes[0].gid_index), (1, 255));
    assert_eq!(topology.mem_list.len(), 1);
    fs::remove_file(
        f.root
            .join("sys/class/infiniband/a/ports/1/gid_attrs/types/255"),
    )
    .unwrap();
    assert!(f.discover(&RdmaConfig::default()).is_err());
    f.gid("a", 1, 3, "2605::1", "RoCE v2", "eth0");
    fs::remove_file(f.root.join("dev/infiniband/uverbs0")).unwrap();
    f.put("dev/infiniband/uverbs0", "not a character device");
    assert!(f.discover(&RdmaConfig::default()).is_err());
}

#[test]
fn down_ports_keep_stable_inventory_and_reselection_obeys_exact_pins() {
    let mut f = Fixture::new();
    f.nic("a", 0);
    f.nic("b", 1);
    f.gid("a", 1, 0, "2605::1", "IB/RoCE v1", "eth0");
    f.gid("a", 1, 3, "2605::1", "RoCE v2", "eth0");
    f.gid("a", 2, 0, "2605::2", "RoCE v2", "eth1");
    f.gid("b", 1, 3, "2605::3", "RoCE v2", "eth2");
    f.put("sys/class/infiniband/a/ports/1/state", "1: DOWN");
    let (topology, routes) = f.discover(&RdmaConfig::default()).unwrap();
    assert_eq!(
        topology
            .nic_list
            .iter()
            .map(|nic| nic.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(routes[0].active);
    assert_eq!(routes[0].port, 2);
    assert_eq!(routes.iter().filter(|route| !route.active).count(), 2);
    let config = RdmaConfig::new("a");
    let (pinned, routes) = f.discover(&config).unwrap();
    assert_eq!(pinned.nic_list.len(), 1);
    assert_eq!(routes.len(), 1);
    assert_eq!(
        (routes[0].port, routes[0].gid_index, routes[0].active),
        (1, 0, false)
    );

    let sys = f.root.join("sys");
    assert_eq!(port_addresses(&sys, "a", 1, None)[0].gid_index, 3);
    fs::remove_file(sys.join("class/infiniband/a/ports/1/gids/3")).unwrap();
    f.gid("a", 1, 7, "2605::7", "RoCE v2", "eth0");
    let refreshed = port_addresses(&sys, "a", 1, None);
    assert_eq!((refreshed[0].gid_index, refreshed[0].active), (7, true));
    assert!(
        refreshed
            .iter()
            .all(|route| route.device == "a" && route.port == 1)
    );
    assert!(port_addresses(&sys, "a", 1, Some(3)).is_empty());
    assert_eq!(port_addresses(&sys, "a", 1, Some(0))[0].gid_index, 0);
    for state in ["0: NOP", "6: UNKNOWN", "unreadable"] {
        f.put("sys/class/infiniband/a/ports/1/state", state);
        assert!(port_addresses(&sys, "a", 1, None).is_empty(), "{state}");
    }
}
