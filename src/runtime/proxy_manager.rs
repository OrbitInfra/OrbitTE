//! Local staging chunks, independent of payload transports and task completion.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::cuda::PinnedAllocation;
use crate::engine::remaining;
use crate::memory::Allocation;
use crate::{Error, RegisteredMemory, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StagingStats {
    /// Includes construction, checked-out chunks and failed cleanup.
    pub reserved_chunks: usize,
    pub reserved_bytes: usize,
    pub idle_chunks: usize,
    pub active_chunks: usize,
    pub retained_chunks: usize,
    pub allocations: u64,
    pub reuses: u64,
    pub completed_chunks: u64,
    pub copied_bytes: u64,
}

enum StageBuffer {
    Vacant,
    Active,
    Idle(PinnedAllocation),
    Closing,
    Retained,
}

struct StageBuffers {
    open: bool,
    slots: Vec<StageBuffer>,
    allocations: u64,
    reuses: u64,
    completed_chunks: u64,
    copied_bytes: u64,
}

pub(crate) struct ProxyManager {
    chunk_size: usize,
    buffers: Mutex<StageBuffers>,
    changed: Condvar,
}

impl ProxyManager {
    pub(crate) fn new(chunk_size: usize, chunk_count: usize) -> Result<Arc<Self>> {
        if chunk_size == 0
            || chunk_size > 64 << 20
            || chunk_count == 0
            || chunk_count > 4096
            || chunk_size
                .checked_mul(chunk_count)
                .is_none_or(|bytes| bytes > 1 << 30)
        {
            return Err(Error::InvalidConfig(
                "staging chunks must be positive and total at most 1 GiB",
            ));
        }
        Ok(Arc::new(Self {
            chunk_size,
            buffers: Mutex::new(StageBuffers {
                open: true,
                slots: (0..chunk_count).map(|_| StageBuffer::Vacant).collect(),
                allocations: 0,
                reuses: 0,
                completed_chunks: 0,
                copied_bytes: 0,
            }),
            changed: Condvar::new(),
        }))
    }

    pub(crate) fn pin_stage_buffer(
        self: &Arc<Self>,
        context: usize,
        allocate: impl FnOnce(usize) -> Result<PinnedAllocation>,
    ) -> Result<StageBufferLease> {
        let (index, previous, reused) = {
            let mut state = self
                .buffers
                .lock()
                .map_err(|_| Error::Internal("staging lock poisoned"))?;
            if !state.open {
                return Err(Error::RequesterClosed);
            }
            let matching = state.slots.iter().position(
                |slot| matches!(slot, StageBuffer::Idle(buffer) if buffer.context_id() == context),
            );
            let index = matching
                .or_else(|| {
                    state
                        .slots
                        .iter()
                        .position(|s| matches!(s, StageBuffer::Vacant))
                })
                .or_else(|| {
                    state
                        .slots
                        .iter()
                        .position(|s| matches!(s, StageBuffer::Idle(_)))
                })
                .ok_or(Error::ResourceExhausted)?;
            let previous = std::mem::replace(&mut state.slots[index], StageBuffer::Active);
            if matching.is_some() {
                state.reuses += 1;
            }
            (index, previous, matching.is_some())
        };
        let buffer = if let StageBuffer::Idle(buffer) = previous {
            if buffer.context_id() == context {
                Ok(buffer)
            } else {
                // A location change never spends a second slot before the old
                // physical allocation is proven released. No driver call holds the map lock.
                buffer.close().and_then(|()| allocate(self.chunk_size))
            }
        } else {
            allocate(self.chunk_size)
        };
        match buffer {
            Ok(buffer) => {
                let lease = StageBufferLease {
                    manager: Arc::clone(self),
                    index,
                    buffer: Some(buffer),
                    safe_to_release: true,
                };
                if !reused {
                    self.buffers
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .allocations += 1;
                }
                if let Err(error) = lease.buffer()?.check_access() {
                    lease.retain();
                    return Err(error);
                }
                Ok(lease)
            }
            Err(error) => {
                self.finish_slot(
                    index,
                    if matches!(error, Error::Unquiesced { .. }) {
                        StageBuffer::Retained
                    } else {
                        StageBuffer::Vacant
                    },
                );
                Err(error)
            }
        }
    }

    fn finish_slot(&self, index: usize, slot: StageBuffer) {
        let mut state = self.buffers.lock().unwrap_or_else(|p| p.into_inner());
        state.slots[index] = slot;
        self.changed.notify_all();
    }

    pub(crate) fn stats(&self) -> Result<StagingStats> {
        let state = self
            .buffers
            .lock()
            .map_err(|_| Error::Internal("staging lock poisoned"))?;
        let mut stats = StagingStats {
            allocations: state.allocations,
            reuses: state.reuses,
            completed_chunks: state.completed_chunks,
            copied_bytes: state.copied_bytes,
            ..StagingStats::default()
        };
        for slot in &state.slots {
            match slot {
                StageBuffer::Vacant => continue,
                StageBuffer::Idle(_) => stats.idle_chunks += 1,
                StageBuffer::Active | StageBuffer::Closing => stats.active_chunks += 1,
                StageBuffer::Retained => stats.retained_chunks += 1,
            }
            stats.reserved_chunks += 1;
        }
        stats.reserved_bytes = stats.reserved_chunks * self.chunk_size;
        Ok(stats)
    }

    pub(crate) fn read(
        self: &Arc<Self>,
        memory: &RegisteredMemory,
        offset: usize,
        bytes: &mut [u8],
        deadline: Instant,
    ) -> Result<()> {
        let (owner, offset) = memory.allocation_range(offset, bytes.len())?;
        match owner {
            Allocation::Host(owner) => owner.read(offset, bytes),
            Allocation::Cuda(owner) => owner.copy_staged(offset, bytes, None, self, deadline),
        }
    }

    pub(crate) fn write(
        self: &Arc<Self>,
        memory: &RegisteredMemory,
        offset: usize,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<()> {
        let (owner, offset) = memory.allocation_range(offset, bytes.len())?;
        match owner {
            Allocation::Host(owner) => owner.write(offset, bytes),
            Allocation::Cuda(owner) => {
                owner.copy_staged(offset, &mut [], Some(bytes), self, deadline)
            }
        }
    }

    pub(crate) fn close(&self, deadline: Instant) -> Result<()> {
        let mut state = self
            .buffers
            .lock()
            .map_err(|_| Error::Internal("staging lock poisoned"))?;
        state.open = false;
        loop {
            if let Some(index) = state
                .slots
                .iter()
                .position(|s| matches!(s, StageBuffer::Idle(_)))
            {
                remaining(deadline)?;
                let StageBuffer::Idle(buffer) =
                    std::mem::replace(&mut state.slots[index], StageBuffer::Closing)
                else {
                    return Err(Error::Internal("staging idle slot changed"));
                };
                drop(state);
                let result = buffer.close();
                self.finish_slot(
                    index,
                    if result.is_ok() {
                        StageBuffer::Vacant
                    } else {
                        StageBuffer::Retained
                    },
                );
                state = self
                    .buffers
                    .lock()
                    .map_err(|_| Error::Internal("staging lock poisoned"))?;
                continue;
            }
            if state
                .slots
                .iter()
                .any(|s| matches!(s, StageBuffer::Retained))
            {
                return Err(Error::Unquiesced {
                    operation: "staging chunk retention",
                    code: -1,
                });
            }
            if state.slots.iter().all(|s| matches!(s, StageBuffer::Vacant)) {
                return Ok(());
            }
            state = self
                .changed
                .wait_timeout(state, remaining(deadline)?)
                .map_err(|_| Error::Internal("staging lock poisoned"))?
                .0;
        }
    }
}

pub(crate) struct StageBufferLease {
    manager: Arc<ProxyManager>,
    index: usize,
    buffer: Option<PinnedAllocation>,
    safe_to_release: bool,
}

impl StageBufferLease {
    pub(crate) fn buffer(&self) -> Result<&PinnedAllocation> {
        self.buffer
            .as_ref()
            .ok_or(Error::Internal("missing staging lease"))
    }
    pub(crate) fn buffer_mut(&mut self) -> Result<&mut PinnedAllocation> {
        self.buffer
            .as_mut()
            .ok_or(Error::Internal("missing staging lease"))
    }
    pub(crate) fn completed(&self, length: usize) {
        let mut state = self
            .manager
            .buffers
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state.completed_chunks = state.completed_chunks.saturating_add(1);
        state.copied_bytes = state.copied_bytes.saturating_add(length as u64);
    }
    pub(crate) fn submitting(&mut self) {
        self.safe_to_release = false;
    }
    pub(crate) fn drained(&mut self) {
        self.safe_to_release = true;
    }
    pub(crate) fn retain(mut self) {
        self.safe_to_release = false;
    }
}

impl Drop for StageBufferLease {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        if !self.safe_to_release || buffer.check_access().is_err() {
            self.manager.finish_slot(self.index, StageBuffer::Retained);
            buffer.retain();
            std::mem::forget(Arc::clone(&self.manager));
        } else {
            // Closing claims returned chunks in its drain loop, outside the map lock.
            self.manager
                .finish_slot(self.index, StageBuffer::Idle(buffer));
        }
    }
}
