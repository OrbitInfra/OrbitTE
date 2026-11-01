use super::*;
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
    DeviceSelector::new(&topology(), 0b1101, false).unwrap()
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
        let selector = DeviceSelector::new(&topology, 0b1101, strict).unwrap();
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
