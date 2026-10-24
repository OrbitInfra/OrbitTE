use std::sync::{Arc, Mutex};

use super::{RdmaStats, sys};
use crate::{Error, Result, TransferOp};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Operation {
    Read,
    Write,
    Bind,
    #[cfg(test)]
    Invalidate,
    Send,
}
impl From<TransferOp> for Operation {
    fn from(value: TransferOp) -> Self {
        match value {
            TransferOp::Read => Self::Read,
            TransferOp::Write => Self::Write,
        }
    }
}

/// One exact-range grant and native endpoint owned by a parent RDMA request.
pub(super) struct RdmaSlice {
    pub(super) endpoint: super::RdmaEndpoint,
    pub(super) binding: super::rdma_transport::Binding,
    pub(super) remote: super::Descriptor,
    pub(super) peer: super::endpoint_store::Peer,
    pub(super) offset: usize,
}

/// Frozen TENT planRdmaSlices geometry, shared by task routing and WR windows.
pub(super) fn plan_rdma_slices(length: usize, base: usize, cap: usize) -> Result<(usize, usize)> {
    if length == 0
        || length > u32::MAX as usize
        || !(1..=64 << 20).contains(&base)
        || !(1..=64).contains(&cap)
    {
        return Err(Error::InvalidRange);
    }
    let initial = length.div_ceil(base).min(cap);
    let block = length.div_ceil(initial).div_ceil(base) * base;
    let mut count = length.div_ceil(block);
    if count > 1 && (length - (count - 1) * block) * 4 < block {
        count -= 1;
    }
    Ok((block, count))
}

/// Owns the signaled-WR debt until validated CQ completion or native teardown.
pub(super) struct SendQueue {
    requests: Vec<sys::WorkRequest>,
    first_id: u64,
    cursor: usize,
    window_start: usize,
    pending: u64,
    usage: Arc<Mutex<RdmaStats>>,
}

impl SendQueue {
    pub(super) fn new(
        length: usize,
        base: usize,
        cap: usize,
        usage: Arc<Mutex<RdmaStats>>,
        first_id: u64,
    ) -> Result<Self> {
        let (block, count) = plan_rdma_slices(length, base, cap)?;
        if first_id == 0 || first_id.checked_add(63).is_none() {
            return Err(Error::InvalidRange);
        }
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceExhausted)?;
        for index in 0..count {
            let offset = index * block;
            requests.push(sys::WorkRequest {
                id: first_id + index as u64,
                offset: offset as u32,
                length: (if index + 1 == count {
                    length - offset
                } else {
                    block
                }) as u32,
            });
        }
        Ok(Self {
            requests,
            first_id,
            cursor: 0,
            window_start: 0,
            pending: 0,
            usage,
        })
    }

    pub(super) fn control(usage: Arc<Mutex<RdmaStats>>, id: u64) -> Result<Self> {
        if id == 0 {
            return Err(Error::InvalidRange);
        }
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(1)
            .map_err(|_| Error::ResourceExhausted)?;
        requests.push(sys::WorkRequest {
            id,
            offset: 0,
            length: 0,
        });
        Ok(Self {
            requests,
            first_id: id,
            cursor: 0,
            window_start: 0,
            pending: 0,
            usage,
        })
    }

    pub(super) fn reserve(&mut self, limit: usize, capacity: usize) -> Result<&[sys::WorkRequest]> {
        if !(1..=64).contains(&limit) || self.pending() || self.cursor == self.requests.len() {
            return Err(Error::Internal("RDMA window state"));
        }
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        let available = capacity.saturating_sub(usage.work_requests);
        if available == 0 {
            return Err(Error::ResourceExhausted);
        }
        let end = (self.cursor + limit.min(available)).min(self.requests.len());
        let count = end - self.cursor;
        usage.work_requests += count;
        usage.peak_work_requests = usage.peak_work_requests.max(usage.work_requests);
        self.window_start = self.cursor;
        self.cursor = end;
        self.pending = if count == 64 {
            u64::MAX
        } else {
            (1u64 << count) - 1
        };
        Ok(&self.requests[self.window_start..end])
    }

    pub(super) fn posted(&mut self, code: i32, result: sys::PostResult) -> Result<()> {
        let count = self.cursor - self.window_start;
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        usage.post_calls += 1;
        if result.uncertain || result.accepted > count || (code == 0 && result.accepted != count) {
            usage.uncertain_posts += 1;
            return if code == 0 {
                Err(Error::Protocol("RDMA inconsistent post receipt"))
            } else {
                Err(Error::Backend {
                    operation: "ibv_post_send",
                    code,
                })
            };
        }
        usage.posted_work_requests += result.accepted as u64;
        if code != 0 {
            usage.partial_posts += u64::from(result.accepted != 0);
            usage.work_requests -= count - result.accepted;
            self.pending = if result.accepted == 64 {
                u64::MAX
            } else {
                (1u64 << result.accepted) - 1
            };
            return Err(Error::Backend {
                operation: "ibv_post_send",
                code,
            });
        }
        Ok(())
    }

    pub(super) fn complete(
        &mut self,
        completion: sys::Completion,
        operation: impl Into<Operation>,
    ) -> Result<()> {
        let operation = operation.into();
        let index = completion
            .id
            .checked_sub(self.first_id)
            .and_then(|id| usize::try_from(id).ok())
            .filter(|index| *index >= self.window_start && *index < self.cursor)
            .ok_or(Error::Protocol("RDMA completion identity"))?;
        let bit = 1u64 << (index - self.window_start);
        if self.pending & bit == 0 {
            return Err(Error::Protocol("RDMA duplicate completion"));
        }
        let request = &self.requests[index];
        let expected_opcode = match operation {
            Operation::Write => 1,
            Operation::Read => 2,
            Operation::Bind => 5,
            #[cfg(test)]
            Operation::Invalidate => 6,
            Operation::Send => 0,
        };
        if completion.status == 0
            && (completion.opcode != expected_opcode
                || (operation == Operation::Read && completion.length != request.length))
        {
            return Err(Error::Protocol("RDMA completion opcode/length"));
        }
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        self.pending &= !bit;
        usage.work_requests -= 1;
        if completion.status != 0 {
            usage.completion_errors += 1;
            return Err(Error::Backend {
                operation: "RDMA work completion",
                code: completion.status as i32,
            });
        }
        if operation == Operation::Read {
            usage.completed_reads += 1;
            usage.completed_read_bytes += u64::from(request.length);
        } else if operation == Operation::Write {
            usage.completed_writes += 1;
            usage.completed_write_bytes += u64::from(request.length);
        } else if operation == Operation::Bind {
            usage.completed_window_binds += 1;
        } else if operation == Operation::Send {
            usage.completed_sends += 1;
        } else {
            usage.completed_window_invalidations += 1;
        }
        Ok(())
    }

    pub(super) fn namespace(&self) -> u64 {
        self.first_id / super::WR_ID_STRIDE
    }

    pub(super) fn pending(&self) -> bool {
        self.pending != 0
    }
    pub(super) fn outstanding(&self) -> usize {
        self.pending.count_ones() as usize
    }
    pub(super) fn finished(&self) -> bool {
        self.cursor == self.requests.len() && !self.pending()
    }

    pub(super) fn quiesce(&mut self) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.work_requests -= self.outstanding();
        self.pending = 0;
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/slice.rs"]
mod tests;
