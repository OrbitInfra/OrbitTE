//! Per-mapping host placement. No thread policy or existing pages are changed.

#![allow(unsafe_code)]

use std::path::Path;
use std::ptr::NonNull;

use crate::{Error, Result};

/// Linux discovery and mapping operations, retained by each mapped owner.
pub(crate) struct Numa {
    pub(crate) nodes: fn() -> Result<Vec<u32>>,
    pub(crate) gpu_node: fn(&str) -> Option<u32>,
    pub(crate) bind: fn(NonNull<u8>, usize, u32) -> Result<()>,
    pub(crate) unmap: fn(NonNull<u8>, usize) -> Result<()>,
}

pub(crate) const NATIVE: Numa = Numa {
    nodes: allowed_nodes,
    gpu_node: |pci| super::pci::numa_node(Path::new("/sys"), pci),
    bind,
    unmap,
};

fn memory_nodes(sys: &Path, mask: &[libc::c_ulong]) -> Vec<u32> {
    let mut nodes: Vec<u32> = std::fs::read_dir(sys.join("devices/system/node"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let node = entry
                .file_name()
                .to_str()?
                .strip_prefix("node")?
                .parse::<u32>()
                .ok()?;
            let index = node as usize / libc::c_ulong::BITS as usize;
            let bit = node % libc::c_ulong::BITS;
            if mask.get(index).is_none_or(|word| word & (1 << bit) == 0) {
                return None;
            }
            if std::fs::read_to_string(entry.path().join("online")).is_ok_and(|v| v.trim() != "1") {
                return None;
            }
            let meminfo = std::fs::read_to_string(entry.path().join("meminfo")).ok()?;
            let total = meminfo.lines().find_map(|line| {
                line.split_once("MemTotal:")
                    .and_then(|(_, v)| v.split_whitespace().next()?.parse::<u64>().ok())
            })?;
            (total > 0).then_some(node)
        })
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    nodes
}

fn allowed_nodes() -> Result<Vec<u32>> {
    let mut mask = vec![0 as libc::c_ulong; 65536 / libc::c_ulong::BITS as usize];
    // MPOL_F_MEMS_ALLOWED reads the current cpuset, without changing mempolicy.
    let status = unsafe {
        libc::syscall(
            libc::SYS_get_mempolicy,
            std::ptr::null_mut::<libc::c_int>(),
            mask.as_mut_ptr(),
            65536usize,
            std::ptr::null_mut::<libc::c_void>(),
            4usize,
        )
    };
    if status != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(memory_nodes(Path::new("/sys"), &mask))
}

pub(crate) fn page_round(length: usize) -> Result<usize> {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        return Err(Error::Unavailable("host page size"));
    }
    let page = page as usize;
    length
        .checked_add(page - 1)
        .map(|n| n / page * page)
        .filter(|n| *n != 0 && *n <= isize::MAX as usize)
        .ok_or(Error::InvalidRange)
}

fn node_mask(node: u32) -> Result<Vec<libc::c_ulong>> {
    if node >= 65536 {
        return Err(Error::InvalidConfig(
            "staging NUMA node must be below 65536",
        ));
    }
    let mut mask = vec![0; node as usize / libc::c_ulong::BITS as usize + 1];
    mask[node as usize / libc::c_ulong::BITS as usize] = 1 << (node % libc::c_ulong::BITS);
    Ok(mask)
}

fn bind(address: NonNull<u8>, length: usize, node: u32) -> Result<()> {
    let mask = node_mask(node)?;
    // MPOL_BIND | MPOL_F_STATIC_NODES; fail if already populated pages violate it.
    // maxnode is the number of meaningful mask bits, not bytes or word count.
    let code = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            address.as_ptr(),
            length,
            2 | (1 << 15),
            mask.as_ptr(),
            node as usize + 1,
            1u32,
        )
    };
    if code != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

fn unmap(address: NonNull<u8>, length: usize) -> Result<()> {
    if unsafe { libc::munmap(address.as_ptr().cast(), length) } != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

pub(crate) struct MappedMemory {
    pub(crate) address: NonNull<u8>,
    pub(crate) length: usize,
    numa: &'static Numa,
    close_attempted: bool,
    close_error: Option<i32>,
}

impl MappedMemory {
    pub(crate) fn new(length: usize, node: u32, numa: &'static Numa) -> Result<Self> {
        let length = page_round(length)?;
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
        let address = NonNull::new(raw.cast()).ok_or(Error::Internal("null host mapping"))?;
        let mut mapping = Self {
            address,
            length,
            numa,
            close_attempted: false,
            close_error: None,
        };
        if let Err(error) = (numa.bind)(address, length, node) {
            mapping.close()?;
            return Err(error);
        }
        // Fault fresh private pages only after their range policy is installed.
        unsafe {
            std::ptr::write_bytes(address.as_ptr(), 0, length);
        }
        Ok(mapping)
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        if !self.close_attempted {
            self.close_attempted = true;
            self.close_error = (self.numa.unmap)(self.address, self.length)
                .err()
                .map(|error| match error {
                    Error::Io(error) => error.raw_os_error().unwrap_or(-1),
                    Error::Backend { code, .. } | Error::Unquiesced { code, .. } => code,
                    _ => -1,
                });
        }
        match self.close_error {
            None => Ok(()),
            Some(code) => Err(Error::Unquiesced {
                operation: "staging host munmap",
                code,
            }),
        }
    }
}

impl Drop for MappedMemory {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/platform/numa.rs"]
mod tests;
