use super::*;
use crate::transport::rdma::RdmaConfig;
use crate::{MemEntry, MemType, NicEntry, NicType};

fn topology() -> Topology {
    let mut topology = Topology {
        nic_list: (0..4)
            .map(|id| NicEntry {
                name: format!("nic{id}"),
                pci_bus_id: None,
                kind: NicType::Rdma,
                numa_node: Some(u32::from(id == 3)),
                device_attrs: Default::default(),
            })
            .collect(),
        mem_list: vec![
            MemEntry {
                name: "cpu:0".into(),
                pci_bus_id: None,
                kind: MemType::Host,
                numa_node: Some(0),
                device_list: Default::default(),
            },
            MemEntry {
                name: "*".into(),
                pci_bus_id: None,
                kind: MemType::Host,
                numa_node: None,
                device_list: Default::default(),
            },
        ],
    };
    topology.rank();
    topology
}

fn selector() -> Arc<DeviceSelector> {
    // NIC 1 has no native context. Its index must remain an inert hole.
    DeviceSelector::new(&topology(), 0b1101, &RdmaConfig::default()).unwrap()
}

#[test]
fn masks_holes_topology_and_exact_byte_charges_share_one_allocation_path() {
    let selector = selector();
    assert!(
        selector
            .allocate(8, MemoryLocation::Cpu(None), 0b0010)
            .is_err()
    );
    assert!(selector.allocate(8, MemoryLocation::Cpu(None), 0).is_err());
    let a = selector
        .allocate(32, MemoryLocation::Cpu(Some(0)), u64::MAX)
        .unwrap();
    let b = selector
        .allocate(8, MemoryLocation::Cpu(Some(0)), u64::MAX)
        .unwrap();
    assert_eq!((a.device_id(), b.device_id()), (0, 2));
    let c = selector
        .allocate(5, MemoryLocation::Cpu(Some(0)), u64::MAX)
        .unwrap();
    assert_eq!(
        c.device_id(),
        2,
        "least in-flight bytes within the preferred tier"
    );
    let d = selector
        .allocate(7, MemoryLocation::Cpu(Some(0)), 0b1000)
        .unwrap();
    assert_eq!(
        d.device_id(),
        3,
        "a strict mask may require the fallback tier"
    );
    assert_eq!(selector.state.lock().unwrap().inflight, [32, 0, 13, 7]);
    drop((a, b, c, d));
    assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
    let ids: Vec<_> = (0..6)
        .map(|_| {
            selector
                .allocate(1, MemoryLocation::Cpu(None), u64::MAX)
                .unwrap()
                .device_id()
        })
        .collect();
    assert_eq!(
        ids,
        [0, 2, 3, 0, 2, 3],
        "equal-load ties rotate over enabled NIC IDs"
    );
}

#[test]
fn concurrent_allocations_and_overflow_never_lose_or_wrap_a_charge() {
    let selector = selector();
    let barrier = Arc::new(std::sync::Barrier::new(17));
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let selector = Arc::clone(&selector);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                let lease = selector
                    .allocate(8, MemoryLocation::Cpu(None), 0b0101)
                    .unwrap();
                barrier.wait();
                barrier.wait();
                drop(lease);
            });
        }
        barrier.wait();
        assert_eq!(selector.state.lock().unwrap().inflight, [64, 0, 64, 0]);
        barrier.wait();
    });
    let full = selector
        .allocate(usize::MAX, MemoryLocation::Cpu(None), 1)
        .unwrap();
    assert!(matches!(
        selector.allocate(1, MemoryLocation::Cpu(None), 1),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(selector.state.lock().unwrap().inflight[0], usize::MAX);
    drop(full);
    assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
}

#[test]
fn failed_native_cleanup_keeps_the_actual_device_charge_after_public_handle_drop() {
    use std::time::{Duration, Instant};
    let runtime = crate::transport::rdma::tests::runtime("direct:close0");
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    let selector = Arc::clone(
        runtime
            .context_set
            .lock()
            .unwrap()
            .selector
            .as_ref()
            .unwrap(),
    );
    assert_eq!(selector.state.lock().unwrap().inflight, [8]);
    assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
    drop(endpoint);
    assert_eq!(selector.state.lock().unwrap().inflight, [8]);
    assert!(
        runtime
            .close(Instant::now() + Duration::from_secs(1))
            .is_err()
    );
    drop(runtime);
    assert_eq!(
        selector.state.lock().unwrap().inflight,
        [8],
        "retained QP and DMA owners retain the selection charge"
    );
}

#[test]
fn gpu_priority_and_strict_numa_use_separate_tiers_and_eligibility() {
    let mut topology = topology();
    topology.nic_list[0].numa_node = None;
    topology.mem_list.push(MemEntry {
        name: "cuda:0".into(),
        pci_bus_id: None,
        kind: MemType::Cuda,
        numa_node: Some(0),
        device_list: [vec![3], vec![2], vec![0, 1]],
    });
    for strict in [false, true] {
        let selector = DeviceSelector::new(
            &topology,
            0b1101,
            &RdmaConfig {
                strict_local_numa: strict,
                ..Default::default()
            },
        )
        .unwrap();
        for context in [1, 2] {
            let location = MemoryLocation::Cuda { device: 0, context };
            assert_eq!(
                selector
                    .allocate(8, location, u64::MAX)
                    .unwrap()
                    .device_id(),
                if strict { 2 } else { 3 }
            );
            assert_eq!(selector.allocate(8, location, 0b1000).is_err(), strict);
            // Unknown NUMA is eligible; an unavailable context remains a hole.
            assert_eq!(selector.allocate(8, location, 1).unwrap().device_id(), 0);
            assert!(selector.allocate(8, location, 2).is_err());
        }
        assert_eq!(
            selector
                .allocate(
                    8,
                    MemoryLocation::Cuda {
                        device: 9,
                        context: 1
                    },
                    8
                )
                .unwrap()
                .device_id(),
            3
        );
        assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
    }
}

#[test]
fn known_cpu_numa_never_becomes_unknown_when_its_inventory_row_is_missing() {
    let mut topology = topology();
    topology.mem_list.clear();
    topology.nic_list[0].numa_node = None;
    for strict in [false, true] {
        let selector = DeviceSelector::new(
            &topology,
            0b1101,
            &RdmaConfig {
                strict_local_numa: strict,
                ..Default::default()
            },
        )
        .unwrap();
        // Node 0 has known same-node NICs despite no memory inventory row.
        assert_eq!(
            selector
                .allocate(8, MemoryLocation::Cpu(Some(0)), 4)
                .unwrap()
                .device_id(),
            2
        );
        assert_eq!(
            selector
                .allocate(8, MemoryLocation::Cpu(Some(0)), 8)
                .is_err(),
            strict
        );
        // Node 99 has no matching NIC. Only truly unknown NIC placement may pass strict.
        assert_eq!(
            selector
                .allocate(8, MemoryLocation::Cpu(Some(99)), 8)
                .is_err(),
            strict
        );
        assert!(
            selector
                .allocate(8, MemoryLocation::Cpu(Some(99)), 1)
                .is_ok()
        );
        assert!(selector.allocate(8, MemoryLocation::Cpu(None), 8).is_ok());
    }
}

#[test]
fn configured_candidates_survive_masks_holes_wildcard_and_known_numa_fallbacks() {
    let mut topology = topology();
    topology.nic_list[0].numa_node = None;
    topology
        .load_from_config(&crate::transport::rdma::RdmaConfig {
            priority_matrix: Some(
                r#"{"*":[["nic3"],["nic0"]],"cpu:0":[["nic1"],["nic2"]]}"#.into(),
            ),
            ..Default::default()
        })
        .unwrap();
    for strict in [false, true] {
        let selector = DeviceSelector::new(
            &topology,
            0b1101,
            &RdmaConfig {
                strict_local_numa: strict,
                ..Default::default()
            },
        )
        .unwrap();
        let a = selector
            .allocate(7, MemoryLocation::Cpu(Some(0)), u64::MAX)
            .unwrap();
        assert_eq!(
            a.device_id(),
            2,
            "unavailable preferred slot 1 falls back without renumbering"
        );
        assert!(
            selector
                .allocate(1, MemoryLocation::Cpu(Some(0)), 1)
                .is_err(),
            "an omitted NIC is excluded even by an explicit mask"
        );
        for node in [1, 99] {
            let lease = selector
                .allocate(9, MemoryLocation::Cpu(Some(node)), u64::MAX)
                .unwrap();
            assert_eq!(lease.device_id(), if strict && node == 99 { 0 } else { 3 });
            assert_eq!(
                selector
                    .allocate(1, MemoryLocation::Cpu(Some(node)), 8)
                    .is_err(),
                strict && node == 99
            );
        }
        selector.set_available(3, false);
        assert_eq!(
            selector
                .allocate(9, MemoryLocation::Cpu(None), u64::MAX)
                .unwrap()
                .device_id(),
            0
        );
        assert_eq!(selector.state.lock().unwrap().inflight, [0, 0, 7, 0]);
        drop(a);
        assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
    }
}

#[test]
fn adaptive_selection_uses_rate_and_backlog_while_baseline_rotates() {
    for smart in [true, false] {
        let selector = DeviceSelector::new(
            &topology(),
            0b0101,
            &RdmaConfig {
                scheduling: SchedulingParams {
                    smart_selection: smart,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
        selector.set_bandwidth(0, Some(100_000));
        selector.set_bandwidth(2, Some(400_000));
        let first = selector
            .allocate(100, MemoryLocation::Cpu(Some(0)), u64::MAX)
            .unwrap();
        assert_eq!(first.device_id(), if smart { 2 } else { 0 });
        let second = selector
            .allocate(100, MemoryLocation::Cpu(Some(0)), u64::MAX)
            .unwrap();
        assert_eq!(second.device_id(), 2);
        let third = selector
            .allocate(1000, MemoryLocation::Cpu(Some(0)), 1)
            .unwrap();
        assert_eq!(third.device_id(), 0);
        assert_eq!(
            selector
                .stats()
                .iter()
                .map(|s| s.inflight_bytes)
                .sum::<usize>(),
            1200
        );
        drop((first, second, third));
        assert_eq!(
            selector
                .stats()
                .iter()
                .map(|s| s.inflight_bytes)
                .sum::<usize>(),
            0
        );
        selector.set_available(2, false);
        assert_eq!(
            selector
                .stats()
                .iter()
                .map(|s| s.device_id)
                .collect::<Vec<_>>(),
            [0]
        );
    }
    for invalid in [f64::NAN, f64::INFINITY, -1.0, 1.01] {
        let config = RdmaConfig {
            scheduling: SchedulingParams {
                bandwidth_learning_rate: invalid,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
