//! Clock and topology conversion used by the Python API.
use crate::{boundary::*, engine};
use std::time::Instant;
// Sample once per submit so equal absolute deadlines remain equal for merging.
pub(crate) fn clock_pair() -> Result<(Instant, u64)> {
    let now = Instant::now();
    let mut stamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: stamp is writable and CLOCK_MONOTONIC takes no additional arguments.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut stamp) } != 0 {
        return Err(INTERNAL);
    }
    let ns = u64::try_from(stamp.tv_sec)
        .ok()
        .and_then(|v| v.checked_mul(1_000_000_000))
        .and_then(|v| v.checked_add(u64::try_from(stamp.tv_nsec).ok()?))
        .ok_or(INTERNAL)?;
    Ok((now, ns))
}

#[cfg(target_os = "linux")]
pub(crate) fn local_topology(e: &engine::Engine) -> Result<String> {
    use serde_json::json;
    let t = e.core.local_topology_snapshot()?;
    if t.nic_list.is_empty() && t.mem_list.is_empty() {
        return Ok("{}".into());
    }
    let nics:Vec<_>=t.nic_list.iter().map(|n|{
        let mut row=json!({"name":n.name,"pci_bus_id":n.pci_bus_id.as_deref().unwrap_or(""),"type":0,"numa_node":n.numa_node.map(i64::from).unwrap_or(-1)});
        if !n.device_attrs.is_empty(){row["device_attrs"]=json!(n.device_attrs);}
        row
    }).collect();
    let mems:Vec<_>=t.mem_list.iter().map(|m| json!({"name":m.name,"pci_bus_id":m.pci_bus_id.as_deref().unwrap_or(""),"numa_node":m.numa_node.map(i64::from).unwrap_or(-1),"type":match m.kind {orbite::MemType::Host=>0,orbite::MemType::Cuda=>1},"device_list":{"rank0":m.device_list[0],"rank1":m.device_list[1],"rank2":m.device_list[2]}})).collect();
    Ok(json!({"nics":nics,"mems":mems}).to_string())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn local_topology(_e: &engine::Engine) -> Result<String> {
    Ok("{}".into())
}
