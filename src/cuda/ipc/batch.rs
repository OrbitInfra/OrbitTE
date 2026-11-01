//! Native pointer arrays and dependency eligibility for CUDA 13 batch copies.

use super::copy::IpcCopy;
use super::*;

pub(super) struct CopyBatch {
    destinations: Vec<u64>,
    sources: Vec<u64>,
    lengths: Vec<usize>,
}

impl CopyBatch {
    pub(super) fn prepare(
        copies: &[IpcCopy],
        remote: u64,
        to_remote: bool,
    ) -> Result<Option<Self>> {
        let mut batch = Self {
            destinations: Vec::new(),
            sources: Vec::new(),
            lengths: Vec::new(),
        };
        let mut ranges = Vec::new();
        ranges
            .try_reserve(
                copies
                    .len()
                    .checked_mul(2)
                    .ok_or(Error::ResourceExhausted)?,
            )
            .map_err(|_| Error::ResourceExhausted)?;
        batch
            .destinations
            .try_reserve(copies.len())
            .map_err(|_| Error::ResourceExhausted)?;
        batch
            .sources
            .try_reserve(copies.len())
            .map_err(|_| Error::ResourceExhausted)?;
        batch
            .lengths
            .try_reserve(copies.len())
            .map_err(|_| Error::ResourceExhausted)?;
        for copy in copies {
            let local = match &copy.memory {
                Allocation::Cuda(memory) => memory.address,
                Allocation::Host(memory) => memory.base_address(),
            }
            .checked_add(copy.local_offset as u64)
            .ok_or(Error::InvalidRange)?;
            let remote = remote
                .checked_add(copy.remote_offset as u64)
                .ok_or(Error::InvalidRange)?;
            let (destination, source) = if to_remote {
                (remote, local)
            } else {
                (local, remote)
            };
            batch.destinations.push(destination);
            batch.sources.push(source);
            batch.lengths.push(copy.length);
            for (address, write) in [(destination, true), (source, false)] {
                let end = address
                    .checked_add(copy.length as u64)
                    .ok_or(Error::InvalidRange)?;
                if copy.length != 0 {
                    ranges.push((address, end, write));
                }
            }
        }
        // Batch members may run in any order. Overlapping reads are allowed;
        // any overlapping write needs the ordered enqueue path. Owners here
        // are owned CUDA allocations or real host pins, never foreign imports.
        ranges.sort_unstable();
        let (mut read_end, mut write_end) = (0, 0);
        for (start, end, write) in ranges {
            if write_end > start || (write && read_end > start) {
                return Ok(None);
            }
            if write {
                write_end = write_end.max(end);
            } else {
                read_end = read_end.max(end);
            }
        }
        Ok(Some(batch))
    }

    /// Owners and source authority must outlive the stream's physical completion.
    pub(super) unsafe fn enqueue(
        &mut self,
        call: driver::CopyBatch,
        count: usize,
        stream: sys::CUstream,
    ) -> Result<()> {
        let location = sys::CUmemLocation {
            type_: sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_INVALID,
            id: 0,
        };
        let mut attrs = sys::CUmemcpyAttributes {
            srcAccessOrder: sys::CUmemcpySrcAccessOrder::CU_MEMCPY_SRC_ACCESS_ORDER_STREAM,
            srcLocHint: location,
            dstLocHint: location,
            flags: 0,
        };
        let mut first = 0;
        // SAFETY: equal-length descriptor arrays cover the admitted prefix,
        // one attribute covers every entry, and the stream is non-legacy.
        check("cuMemcpyBatchAsync_v2", unsafe {
            call(
                self.destinations.as_mut_ptr(),
                self.sources.as_mut_ptr(),
                self.lengths.as_mut_ptr(),
                count,
                &mut attrs,
                &mut first,
                1,
                stream,
            )
        })
    }
}
