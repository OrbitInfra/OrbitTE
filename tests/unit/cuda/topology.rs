use super::*;
use crate::{MemType, NicEntry, NicType, Topology};
use std::ffi::{c_char, c_int};
use std::os::unix::fs::symlink;

unsafe extern "C" fn count(out: *mut c_int) -> driver::Status {
    unsafe {
        *out = 2;
    }
    SUCCESS
}
unsafe extern "C" fn get(out: *mut c_int, ordinal: c_int) -> driver::Status {
    unsafe {
        *out = ordinal + 10;
    }
    SUCCESS
}
unsafe extern "C" fn get_failure(out: *mut c_int, ordinal: c_int) -> driver::Status {
    if ordinal == 1 {
        driver::Status::CUDA_ERROR_ILLEGAL_ADDRESS
    } else {
        unsafe { get(out, ordinal) }
    }
}
unsafe extern "C" fn bus(out: *mut c_char, len: c_int, device: c_int) -> driver::Status {
    let value = match device {
        10 => c"0000:0A:00.0",
        11 => c"0000:8A:00.0",
        _ => return driver::Status::CUDA_ERROR_INVALID_VALUE,
    };
    if (len as usize) < value.to_bytes_with_nul().len() {
        return driver::Status::CUDA_ERROR_INVALID_VALUE;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), out, value.to_bytes_with_nul().len());
    }
    SUCCESS
}

#[test]
fn cuda_inventory_maps_ordinals_to_pci_tiers_and_publishes_only_complete_snapshots() {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = std::env::temp_dir().join(format!(
        "orbitte-gpu-topology-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(root.join("bus/pci/devices")).unwrap();
    for (bus, bridge, node) in [
        ("0000:0a:00.0", "devices/pci0000:00/0000:00:01.0", 0),
        ("0000:0a:01.0", "devices/pci0000:00/0000:00:01.0", 0),
        ("0000:0b:00.0", "devices/pci0000:00/0000:00:02.0", 0),
        ("0000:8a:00.0", "devices/pci0000:80/0000:80:01.0", 1),
        ("0000:8a:01.0", "devices/pci0000:80/0000:80:01.0", 1),
    ] {
        let device = root.join(bridge).join(bus);
        std::fs::create_dir_all(&device).unwrap();
        std::fs::write(device.join("numa_node"), node.to_string()).unwrap();
        symlink(device, root.join("bus/pci/devices").join(bus)).unwrap();
    }
    let mut topology = Topology {
        nic_list: [
            ("0000:0a:01.0", Some(0)),
            ("0000:0b:00.0", Some(0)),
            ("0000:8a:01.0", Some(1)),
            ("0000:ff:01.0", None),
        ]
        .iter()
        .enumerate()
        .map(|(id, (bus, node))| NicEntry {
            name: format!("rail{id}"),
            pci_bus_id: Some((*bus).into()),
            kind: NicType::Rdma,
            numa_node: *node,
            device_attrs: Default::default(),
        })
        .collect(),
        mem_list: vec![],
    };
    let mut driver = driver();
    let native = Arc::get_mut(&mut driver).unwrap();
    native.device_count = count;
    native.device_get = get;
    native.pci_bus_id = bus;
    driver.probe_topology(&mut topology, &root).unwrap();
    assert_eq!(topology.mem_list.len(), 2);
    let gpu = &topology.mem_list[0];
    assert_eq!(
        (gpu.name.as_str(), gpu.kind, gpu.numa_node),
        ("cuda:0", MemType::Cuda, Some(0))
    );
    assert_eq!(gpu.pci_bus_id.as_deref(), Some("0000:0a:00.0"));
    assert_eq!(gpu.device_list, [vec![0], vec![1], vec![2, 3]]);
    assert_eq!(
        topology.mem_list[1].device_list,
        [vec![2], vec![], vec![0, 1, 3]]
    );
    topology.rank();
    assert_eq!(
        topology.mem_list[0].device_list,
        [vec![0], vec![1], vec![2, 3]]
    );
    let before = topology.clone();
    Arc::get_mut(&mut driver).unwrap().device_get = get_failure;
    assert!(matches!(
        driver.probe_topology(&mut topology, &root),
        Err(Error::Backend {
            operation: "cuDeviceGet",
            code: 700
        })
    ));
    assert_eq!(
        topology, before,
        "partial native enumeration cannot publish GPU rows"
    );
    std::fs::remove_dir_all(root).unwrap();
    assert!(STACK.with(|s| s.borrow().is_empty()));
}

#[test]
fn public_gpu_read_write_slices_use_owner_locations_at_both_peers() {
    use crate::engine::tests::rdma_fixture_with_runtime;
    use crate::transport::rdma::tests::gpu_topology;
    for (strict, preferred, source_device, requester_id, source_id) in [
        (false, None, 1, 1, 1),
        (true, None, 0, 1, 0),
        (true, Some(0), 1, 0, 0),
    ] {
        let source = rdma_fixture_with_runtime(gpu_topology::runtime(strict, preferred, u64::MAX));
        let requester =
            rdma_fixture_with_runtime(gpu_topology::runtime(strict, preferred, u64::MAX));
        let a = allocation(&[0x31; 32]);
        a.ordinal.store(source_device, Ordering::Release);
        let b = allocation(&[0x62; 32]);
        b.ordinal.store(1, Ordering::Release);
        let remote = source
            .register_cuda_memory_batch(&[(memory(&a), 0, 32)], Permission::RemoteReadWrite)
            .unwrap()
            .remove(0);
        let local = requester
            .register_cuda_memory_batch(&[(memory(&b), 0, 32)], Permission::LocalOnly)
            .unwrap()
            .remove(0);
        let peer = requester
            .open_segment(
                source.endpoint(),
                source.source_epoch(),
                Duration::from_secs(2),
            )
            .unwrap();
        for op in [TransferOp::Read, TransferOp::Write] {
            local.write(0, &[0x62; 32]).unwrap();
            remote.write(0, &[0x31; 32]).unwrap();
            let report = requester
                .submit_and_wait(
                    op,
                    &peer,
                    &[TransferRequest {
                        remote_staging: false,
                        transport: Transport::Rdma,
                        local_address: local.base_address() + 2,
                        remote_address: remote.base_address() + 3,
                        length: 13,
                    }],
                    Duration::from_secs(2),
                )
                .unwrap();
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            let (target, offset, payload, guard) = if op == TransferOp::Read {
                (&local, 2, 0x31, 0x62)
            } else {
                (&remote, 3, 0x62, 0x31)
            };
            let mut bytes = [0; 32];
            target.read(0, &mut bytes).unwrap();
            let mut expected = [guard; 32];
            expected[offset..offset + 13].fill(payload);
            assert_eq!(bytes, expected);
        }
        gpu_topology::assert_used(&requester, requester_id);
        gpu_topology::assert_used(&source, source_id);
        source.unregister_memory(&remote).unwrap();
        requester.unregister_memory(&local).unwrap();
        source.close(Duration::from_secs(2)).unwrap();
        requester.close(Duration::from_secs(2)).unwrap();
    }
}
