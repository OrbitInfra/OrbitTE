//! Shared PCI identity, NUMA placement and bridge-distance discovery.

#[cfg(feature = "cuda")]
use std::path::Path;

pub(crate) fn bus_id(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    if bytes.len() != 12
        || bytes[4] != b':'
        || bytes[7] != b':'
        || bytes[10] != b'.'
        || bytes
            .iter()
            .enumerate()
            .any(|(i, b)| ![4, 7, 10].contains(&i) && !b.is_ascii_hexdigit())
        || u8::from_str_radix(&value[8..10], 16).ok()? > 31
        || u8::from_str_radix(&value[11..12], 16).ok()? > 7
    {
        return None;
    }
    Some(value.to_ascii_lowercase())
}

#[cfg(feature = "cuda")]
pub(crate) fn numa_node(sys: &Path, pci: &str) -> Option<u32> {
    let pci = bus_id(pci)?;
    std::fs::read_to_string(sys.join("bus/pci/devices").join(pci).join("numa_node"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[cfg(all(feature = "rdma", feature = "cuda"))]
pub(crate) fn distance(sys: &Path, left: &str, right: &str) -> Option<usize> {
    let root = sys.join("bus/pci/devices");
    let left = root.join(bus_id(left)?).canonicalize().ok()?;
    let right = root.join(bus_id(right)?).canonicalize().ok()?;
    // Devices behind the same bridge have distance zero. Compare complete path
    // components, never shared character prefixes of different PCI identities.
    let left: Vec<_> = left.parent()?.components().collect();
    let right: Vec<_> = right.parent()?.components().collect();
    let shared = left.iter().zip(&right).take_while(|(a, b)| a == b).count();
    Some(left.len() + right.len() - 2 * shared)
}
