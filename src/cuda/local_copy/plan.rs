use super::*;
use crate::memory::Allocation;
use std::collections::BTreeMap;
use std::ops::Range;

#[derive(Clone, Copy, Debug)]
pub(super) struct Row {
    pub device: u64,
    pub host: u64,
    pub width: usize,
    pub allocation: usize,
    pub registration: usize,
    pub pin_base: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Dma {
    pub first: Row,
    pub rows: usize,
    pub device_pitch: usize,
    pub host_pitch: usize,
}

fn valid(row: Row) -> bool {
    row.device.checked_add(row.width as u64).is_some()
        && row.host.checked_add(row.width as u64).is_some()
}
fn same(a: Row, b: Row) -> bool {
    (a.allocation, a.registration) == (b.allocation, b.registration)
}

// Port of OrbitKV's contiguous_ranges then dma_copies. Both passes preserve
// descriptor order. The DMA width never includes a gap between explicit rows.
pub(super) fn compile(rows: &[Row], max_pitch: usize) -> Vec<Dma> {
    let mut contiguous: Vec<Row> = Vec::new();
    for &r in rows {
        if let Some(last) = contiguous.last_mut() {
            if same(*last, r)
                && last.device.checked_add(last.width as u64) == Some(r.device)
                && last.host.checked_add(last.width as u64) == Some(r.host)
                && valid(r)
            {
                if let Some(width) = last.width.checked_add(r.width) {
                    last.width = width;
                    continue;
                }
            }
        }
        contiguous.push(r);
    }
    let mut result = Vec::new();
    let mut input = contiguous.into_iter().peekable();
    while let Some(first) = input.next() {
        let mut copy = Dma {
            first,
            rows: 1,
            device_pitch: first.width,
            host_pitch: first.width,
        };
        let mut previous = first;
        while let Some(&next) = input.peek() {
            if first.width == 0 || next.width != first.width || !same(first, next) || !valid(next) {
                break;
            }
            let pitches = |a: u64, b: u64| {
                a.checked_sub(b)
                    .and_then(|v| usize::try_from(v).ok())
                    .filter(|v| (first.width..=max_pitch).contains(v))
            };
            let (Some(device), Some(host)) = (
                pitches(next.device, previous.device),
                pitches(next.host, previous.host),
            ) else {
                break;
            };
            if copy.rows > 1 && (device, host) != (copy.device_pitch, copy.host_pitch) {
                break;
            }
            copy.rows += 1;
            copy.device_pitch = device;
            copy.host_pitch = host;
            previous = next;
            input.next();
        }
        result.push(copy);
    }
    result
}

pub(super) struct Loan {
    memory: Allocation,
    ranges: Vec<Range<usize>>,
}
impl Loan {
    fn acquire(memory: Allocation, mut ranges: Vec<Range<usize>>) -> Result<Self> {
        ranges.sort_unstable_by_key(|r| r.start);
        let mut distinct: Vec<Range<usize>> = Vec::new();
        for r in ranges {
            if let Some(last) = distinct.last_mut() {
                if r.start <= last.end {
                    last.end = last.end.max(r.end);
                    continue;
                }
            }
            distinct.push(r);
        }
        match &memory {
            Allocation::Host(h) => h.borrow_local(&distinct)?,
            Allocation::Cuda(d) => {
                let mut a = d
                    .allocation
                    .lock()
                    .map_err(|_| Error::Internal("CUDA allocation lock"))?;
                if a.ipc_inflight {
                    return Err(Error::Unavailable("CUDA IPC owns local copy range"));
                }
                for r in &distinct {
                    a.check_access(r)?;
                }
                a.dma
                    .try_reserve(distinct.len())
                    .map_err(|_| Error::ResourceExhausted)?;
                a.dma.extend_from_slice(&distinct);
            }
        }
        Ok(Self {
            memory,
            ranges: distinct,
        })
    }
    pub fn retire_producer(&self) {
        match &self.memory {
            Allocation::Host(h) => {
                if let Some(p) = h.owned_pin() {
                    p.retire_producer();
                }
            }
            Allocation::Cuda(d) => {
                if let Some(p) = &mut d
                    .allocation
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .producer
                {
                    p.retire_wait();
                }
            }
        }
    }
    pub fn enqueue_producer(&self, stream: sys::CUstream) -> Result<()> {
        match &self.memory {
            Allocation::Host(h) => h.owned_pin().map_or(Ok(()), |p| p.enqueue_producer(stream)),
            Allocation::Cuda(d) => {
                let a = d
                    .allocation
                    .lock()
                    .map_err(|_| Error::Internal("CUDA allocation lock"))?;
                if let Some(p) = &a.producer {
                    p.enqueue_wait(stream)?;
                }
                Ok(())
            }
        }
    }
}
impl Drop for Loan {
    fn drop(&mut self) {
        match &self.memory {
            Allocation::Host(h) => h.release_local(&self.ranges),
            Allocation::Cuda(d) => d
                .allocation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .dma
                .retain(|r| {
                    self.ranges
                        .binary_search_by_key(&r.start, |x| x.start)
                        .is_err()
                }),
        }
    }
}

pub(super) struct Plan {
    pub rows: Vec<Row>,
    pub dma: Vec<Dma>,
    pub independent: bool,
    pub loans: Vec<Loan>,
    // A physical pin or GPU allocation can contain many independently retained slots.
    pub _views: Vec<LocalCopyDesc>,
    pub bytes: usize,
}

impl Plan {
    pub fn new(
        context: &Context,
        parts: &[LocalCopyDesc],
        direction: CopyDirection,
        pitch: usize,
        cap: usize,
    ) -> Result<Self> {
        if parts.len() > cap {
            return Err(Error::ResourceExhausted);
        }
        let mut rows = Vec::new();
        let mut owners: BTreeMap<usize, (Allocation, Vec<Range<usize>>)> = BTreeMap::new();
        let mut bytes = 0usize;
        for p in parts {
            if p.device.memory_type() != crate::MemoryType::Cuda
                || p.host.memory_type() != crate::MemoryType::Cpu
            {
                return Err(Error::Unavailable(
                    "local copy requires a device view and a pinned host view",
                ));
            }
            if p.device_offset > p.device.len() || p.host_offset > p.host.len() {
                return Err(Error::InvalidRange);
            }
            let empty = p.rows == 0 || p.width == 0;
            if !empty && p.rows > cap - rows.len() {
                return Err(Error::ResourceExhausted);
            }
            let extent = |pitch: usize| {
                if p.rows > 1 && pitch < p.width {
                    return Err(Error::InvalidRange);
                }
                (p.rows - 1)
                    .checked_mul(pitch)
                    .and_then(|n| n.checked_add(p.width))
                    .ok_or(Error::InvalidRange)
            };
            let span = |offset, pitch, len| {
                if empty {
                    Ok((0, len))
                } else {
                    Ok::<_, Error>((offset, extent(pitch)?))
                }
            };
            let (doff, dlen) = span(p.device_offset, p.device_pitch, p.device.len())?;
            let (hoff, hlen) = span(p.host_offset, p.host_pitch, p.host.len())?;
            let (gpu, device_offset) = p.device.cuda_range(doff, dlen)?;
            let (Allocation::Host(host), host_offset) = p.host.allocation_range(hoff, hlen)? else {
                return Err(Error::Unavailable("local copy host view"));
            };
            if gpu
                .allocation
                .lock()
                .map_err(|_| Error::Internal("CUDA allocation lock"))?
                .context
                .raw
                != context.raw
            {
                return Err(Error::Unavailable(
                    "device registration belongs to another CUDA context",
                ));
            }
            let (registration, pin_base) = host.local_pin()?;
            if empty {
                continue;
            }
            let gpu_id = Arc::as_ptr(&gpu) as usize;
            let host_id = Arc::as_ptr(&host) as usize;
            bytes = p
                .width
                .checked_mul(p.rows)
                .and_then(|n| bytes.checked_add(n))
                .ok_or(Error::ResourceExhausted)?;
            for i in 0..p.rows {
                let d = device_offset + i * p.device_pitch;
                let h = host_offset + i * p.host_pitch;
                let row = Row {
                    device: gpu
                        .address
                        .checked_add(d as u64)
                        .ok_or(Error::InvalidRange)?,
                    host: host
                        .base_address()
                        .checked_add(h as u64)
                        .ok_or(Error::InvalidRange)?,
                    width: p.width,
                    allocation: gpu_id,
                    registration,
                    pin_base,
                };
                if !valid(row) {
                    return Err(Error::InvalidRange);
                }
                rows.push(row);
                owners
                    .entry(gpu_id)
                    .or_insert_with(|| (Allocation::Cuda(gpu.clone()), Vec::new()))
                    .1
                    .push(d..d + p.width);
                owners
                    .entry(host_id)
                    .or_insert_with(|| (Allocation::Host(host.clone()), Vec::new()))
                    .1
                    .push(h..h + p.width);
            }
        }
        let dma = compile(&rows, pitch);
        let mut destinations: Vec<_> = rows
            .iter()
            .map(|r| {
                let d = match direction {
                    CopyDirection::HostToDevice => r.device,
                    CopyDirection::DeviceToHost => r.host,
                };
                (d, d + r.width as u64)
            })
            .collect();
        destinations.sort_unstable();
        let independent = destinations.windows(2).all(|pair| pair[0].1 <= pair[1].0);
        let loans = owners
            .into_values()
            .map(|(memory, ranges)| Loan::acquire(memory, ranges))
            .collect::<Result<_>>()?;
        Ok(Self {
            rows,
            dma,
            independent,
            loans,
            _views: parts.to_vec(),
            bytes,
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/cuda/local_copy/planner.rs"]
mod tests;
