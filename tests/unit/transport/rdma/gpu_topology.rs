//! Two NUMA-local rails for owner-based CUDA dispatch controls.
use super::*;
use crate::{MemEntry, MemType, MemoryLocation, NicEntry, NicType, Topology};

fn discover(config: &RdmaConfig) -> Result<(Topology, Vec<discovery::RdmaAddress>)> {
    let (_, base) = super::discover(config)?;
    let mut topology = Topology::default();
    let mut addresses = Vec::new();
    for id in 0..2 {
        let name = format!("rail{id}");
        topology.nic_list.push(NicEntry {
            name: name.clone(),
            pci_bus_id: None,
            kind: NicType::Rdma,
            numa_node: Some(id),
            device_attrs: Default::default(),
        });
        topology.mem_list.push(MemEntry {
            name: format!("cpu:{id}"),
            pci_bus_id: None,
            kind: MemType::Host,
            numa_node: Some(id),
            device_list: Default::default(),
        });
        topology.mem_list.push(MemEntry {
            name: format!("cuda:{id}"),
            pci_bus_id: None,
            kind: MemType::Cuda,
            numa_node: Some(id),
            device_list: [vec![id as usize], vec![], vec![1 - id as usize]],
        });
        let mut address = base[0].clone();
        address.device = name;
        addresses.push(address);
    }
    topology.rank();
    Ok((topology, addresses))
}

unsafe extern "C" fn open(h: *mut sys::DeviceHandles, _: *const c_char) -> i32 {
    unsafe { super::open_device(h, c"copy".as_ptr()) }
}

pub(crate) fn runtime(
    strict: bool,
    preferred: Option<u32>,
    mask: u64,
    custom: bool,
) -> RdmaTransport {
    static VERBS: sys::Verbs = sys::Verbs {
        discover,
        open_device: open,
        ..super::VERBS
    };
    let mut runtime = super::gpu_runtime("copy");
    runtime.config.strict_local_numa = strict;
    runtime.config.preferred_numa_node = preferred;
    runtime.config.device_mask = mask;
    runtime.config.block_size = 4;
    if custom {
        runtime.config.priority_matrix = Some(r#"{"cuda:1":[["rail0"],["rail1"]]}"#.into());
    }
    runtime.verbs = &VERBS;
    runtime.registrations =
        buffers::LocalBufferManager::new(&runtime.config, Arc::clone(&runtime.usage), &VERBS);
    runtime
}

pub(crate) fn assert_used(engine: &crate::TransferEngine, id: usize, gpu_one_path: usize) {
    let runtime = crate::engine::tests::source_runtime(engine);
    let contexts = runtime.context_set.lock().unwrap();
    let used: Vec<_> = contexts
        .slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| !slot.cq_list.is_empty())
        .map(|(id, _)| id)
        .collect();
    assert_eq!(used, [id], "only the selected rail may create DMA progress");
    drop(contexts);
    let paths = runtime
        .paths(MemoryLocation::Cuda {
            device: 1,
            context: 42,
        })
        .unwrap();
    assert_eq!(paths[0].nic_id, gpu_one_path);
}
