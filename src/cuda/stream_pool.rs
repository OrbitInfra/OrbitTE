use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use super::CopyStream;
use crate::memory::Allocation;
use crate::{Error, Result};

enum Slot {
    Vacant,
    Busy,
    Idle(CopyStream),
    // Native cleanup was not proven. Its context/handles remain retained.
    Retained,
}

struct State {
    open: bool,
    slots: Vec<Slot>,
}

/// Transport-owned streams, matched by actual context and bounded across contexts.
pub(crate) struct CUDAStreamPool {
    state: Mutex<State>,
    available: Condvar,
}

/// Exclusive through enqueue, completion/recovery and context restoration.
pub(crate) struct CUDAStreamHandle {
    pool: Arc<CUDAStreamPool>,
    index: usize,
    stream: Option<CopyStream>,
    retained: bool,
}

impl CUDAStreamPool {
    pub(crate) fn new(limit: usize) -> Result<Arc<Self>> {
        if !(1..=4096).contains(&limit) {
            return Err(Error::InvalidConfig("NVLink stream count must be 1..=4096"));
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(limit)
            .map_err(|_| Error::ResourceExhausted)?;
        slots.resize_with(limit, || Slot::Vacant);
        Ok(Arc::new(Self {
            state: Mutex::new(State { open: true, slots }),
            available: Condvar::new(),
        }))
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        memory: &Allocation,
        deadline: Instant,
    ) -> Result<CUDAStreamHandle> {
        // No allocation access lock is held while waiting for a pool slot.
        let context = memory.ipc_owner_context()?;
        let key = context.raw.as_ptr() as usize;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("CUDA stream pool lock poisoned"))?;
        loop {
            if !state.open {
                return Err(Error::Unavailable("CUDA stream pool is closed"));
            }
            let timeout = deadline
                .checked_duration_since(Instant::now())
                .ok_or(Error::Timeout)?;
            let matching = state.slots.iter().rposition(|slot| {
                matches!(slot, Slot::Idle(stream) if stream.context_key() == key && stream.quiescent())
            });
            let index = matching
                .or_else(|| state.slots.iter().position(|s| matches!(s, Slot::Vacant)))
                .or_else(|| state.slots.iter().position(|s| matches!(s, Slot::Idle(_))));
            if let Some(index) = index {
                let previous = std::mem::replace(&mut state.slots[index], Slot::Busy);
                drop(state);
                let mut handle = CUDAStreamHandle {
                    pool: Arc::clone(self),
                    index,
                    stream: match previous {
                        Slot::Idle(stream) => Some(stream),
                        _ => None,
                    },
                    retained: false,
                };
                if matching.is_none() {
                    // Reserve before native work, including replacement cleanup.
                    handle.close_stream()?;
                    match CopyStream::new(&context) {
                        Ok(stream) => handle.stream = Some(stream),
                        Err(error) => {
                            handle.retained = matches!(error, Error::Unquiesced { .. });
                            return Err(error);
                        }
                    }
                }
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                return Ok(handle);
            }
            if !state.slots.iter().any(|s| matches!(s, Slot::Busy)) {
                return Err(Error::ResourceExhausted);
            }
            state = self
                .available
                .wait_timeout(state, timeout)
                .map_err(|_| Error::Internal("CUDA stream pool lock poisoned"))?
                .0;
        }
    }

    /// Charged slots (including construction), active leases, retained slots.
    pub(crate) fn stats(&self) -> Result<(usize, usize, usize)> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("CUDA stream pool lock poisoned"))?;
        Ok(state.slots.iter().fold((0, 0, 0), |mut counts, slot| {
            counts.0 += usize::from(!matches!(slot, Slot::Vacant));
            counts.1 += usize::from(matches!(slot, Slot::Busy));
            counts.2 += usize::from(matches!(slot, Slot::Retained));
            counts
        }))
    }

    /// Fence acquisition, wait for leases, and clean up outside the pool lock.
    /// A timeout permits another wait; uncertain native destruction is one-shot.
    pub(crate) fn close(self: &Arc<Self>, deadline: Instant) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("CUDA stream pool lock poisoned"))?;
        state.open = false;
        self.available.notify_all();
        loop {
            let idle = state.slots.iter().position(|s| matches!(s, Slot::Idle(_)));
            let busy = state.slots.iter().any(|s| matches!(s, Slot::Busy));
            if idle.is_none() && !busy {
                return if state.slots.iter().any(|s| matches!(s, Slot::Retained)) {
                    Err(Error::Unquiesced {
                        operation: "CUDA stream pool cleanup",
                        code: -1,
                    })
                } else {
                    Ok(())
                };
            }
            let timeout = deadline
                .checked_duration_since(Instant::now())
                .ok_or(Error::Timeout)?;
            if let Some(index) = idle {
                let Slot::Idle(stream) = std::mem::replace(&mut state.slots[index], Slot::Busy)
                else {
                    return Err(Error::Internal("CUDA idle stream missing"));
                };
                drop(state);
                let mut handle = CUDAStreamHandle {
                    pool: Arc::clone(self),
                    index,
                    stream: Some(stream),
                    retained: false,
                };
                // Continue cleaning other slots after a failed native destroy.
                let _ = handle.close_stream();
                drop(handle);
                state = self
                    .state
                    .lock()
                    .map_err(|_| Error::Internal("CUDA stream pool lock poisoned"))?;
            } else {
                state = self
                    .available
                    .wait_timeout(state, timeout)
                    .map_err(|_| Error::Internal("CUDA stream pool lock poisoned"))?
                    .0;
            }
        }
    }
}

impl CUDAStreamHandle {
    pub(super) fn stream(&mut self) -> Result<&mut CopyStream> {
        self.stream
            .as_mut()
            .ok_or(Error::Internal("CUDA stream lease is empty"))
    }

    fn close_stream(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            if stream.close().is_err() {
                self.retained = true;
                return Err(Error::Unquiesced {
                    operation: "CUDA pooled stream cleanup",
                    code: -1,
                });
            }
        }
        Ok(())
    }
}

impl Drop for CUDAStreamHandle {
    fn drop(&mut self) {
        let stream = self.stream.take();
        let retained = self.retained || stream.as_ref().is_some_and(|s| !s.quiescent());
        let next = if retained {
            // CopyStream retains uncertain native handles/context without retry.
            // No native operation runs under the pool mutex.
            drop(stream);
            Slot::Retained
        } else {
            stream.map_or(Slot::Vacant, Slot::Idle)
        };
        let mut state = self.pool.state.lock().unwrap_or_else(|p| p.into_inner());
        state.slots[self.index] = next;
        self.pool.available.notify_all();
    }
}
