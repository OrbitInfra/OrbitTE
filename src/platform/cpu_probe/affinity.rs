//! TENT worker socket binding, restricted to the creator's allowed CPU set.

#![allow(unsafe_code)]

use std::path::Path;

use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CpuAffinity {
    pub(crate) node: Option<u32>,
    pub(crate) cpus: Vec<usize>,
}

pub(crate) fn allowed_cpus() -> Result<Vec<usize>> {
    let mut words = vec![0 as libc::c_ulong; 16];
    loop {
        let size = std::mem::size_of_val(words.as_slice());
        let code = unsafe { libc::sched_getaffinity(0, size, words.as_mut_ptr().cast()) };
        if code == 0 {
            return Ok((0..size * 8)
                .filter(|cpu| {
                    words[cpu / libc::c_ulong::BITS as usize]
                        & (1 << (cpu % libc::c_ulong::BITS as usize))
                        != 0
                })
                .collect());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINVAL) || size >= 1 << 20 {
            return Err(Error::Io(error));
        }
        words.resize(words.len() * 2, 0);
    }
}

fn cpulist(text: &str) -> Option<Vec<usize>> {
    let mut cpus = Vec::new();
    for part in text.trim().split(',').filter(|part| !part.is_empty()) {
        let mut range = part.split('-');
        let start: usize = range.next()?.parse().ok()?;
        let end: usize = range.next().map_or(Some(start), |end| end.parse().ok())?;
        if range.next().is_some() || start > end || end >= 1 << 20 {
            return None;
        }
        cpus.extend(start..=end);
    }
    cpus.sort_unstable();
    cpus.dedup();
    Some(cpus)
}

fn intersect(sys: &Path, allowed: Vec<usize>) -> Result<Vec<CpuAffinity>> {
    if allowed.is_empty() {
        return Err(Error::Unavailable("RDMA workers have no allowed CPUs"));
    }
    let mut groups: Vec<_> = std::fs::read_dir(sys.join("devices/system/node"))
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
            let text = std::fs::read_to_string(entry.path().join("cpulist")).ok()?;
            let mut cpus = cpulist(&text)?;
            cpus.retain(|cpu| allowed.binary_search(cpu).is_ok());
            (!cpus.is_empty()).then_some(CpuAffinity {
                node: Some(node),
                cpus,
            })
        })
        .collect();
    groups.sort_unstable_by_key(|group| group.node);
    // No visible NUMA topology: preserve the inherited allowed set and record no socket.
    if groups.is_empty() {
        groups.push(CpuAffinity {
            node: None,
            cpus: allowed,
        });
    }
    Ok(groups)
}

pub(crate) fn worker_affinities() -> Result<Vec<CpuAffinity>> {
    intersect(Path::new("/sys"), allowed_cpus()?)
}

impl CpuAffinity {
    pub(crate) fn bind(&self) -> Result<()> {
        let max = self
            .cpus
            .last()
            .copied()
            .ok_or(Error::InvalidConfig("empty RDMA worker CPU set"))?;
        let bits = libc::c_ulong::BITS as usize;
        let mut words = vec![0 as libc::c_ulong; (max / bits + 1).max(16)];
        for &cpu in &self.cpus {
            words[cpu / bits] |= 1 << (cpu % bits);
        }
        let code = unsafe {
            libc::sched_setaffinity(
                0,
                std::mem::size_of_val(words.as_slice()),
                words.as_ptr().cast(),
            )
        };
        if code != 0 {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
        if allowed_cpus()? != self.cpus {
            return Err(Error::Unavailable(
                "RDMA worker CPU affinity changed during startup",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/platform/affinity.rs"]
mod tests;
