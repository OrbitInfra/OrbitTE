//! Whole-parent NIC selection and exact byte reservations handed to native grants.

use std::sync::Arc;

use super::{DeviceLease, DeviceSelector};
use crate::{Error, MemoryLocation, Result};

/// Owns every planned charge until a slice takes its lease or the plan is dropped.
pub(in crate::transport::rdma) struct DeviceAllocation {
    selector: Arc<DeviceSelector>,
    devices: [u8; 64],
    length: usize,
    block: usize,
    count: usize,
    cursor: usize,
}

impl DeviceAllocation {
    fn slice_length(&self, index: usize) -> usize {
        if index + 1 == self.count {
            self.length - index * self.block
        } else {
            self.block
        }
    }
}

impl Iterator for DeviceAllocation {
    type Item = DeviceLease;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor == self.count {
            return None;
        }
        let lease = DeviceLease {
            selector: Arc::clone(&self.selector),
            device_id: usize::from(self.devices[self.cursor]),
            length: self.slice_length(self.cursor),
        };
        self.cursor += 1;
        Some(lease)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.cursor;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for DeviceAllocation {}

impl Drop for DeviceAllocation {
    fn drop(&mut self) {
        if self.cursor == self.count {
            return;
        }
        let mut state = self
            .selector
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for index in self.cursor..self.count {
            state.inflight[usize::from(self.devices[index])] -= self.slice_length(index);
        }
    }
}

#[derive(Clone, Copy)]
struct Candidate {
    device_id: usize,
    score: f64,
}

impl DeviceSelector {
    pub(in crate::transport::rdma) fn allocate(
        self: &Arc<Self>,
        length: usize,
        block: usize,
        count: usize,
        location: MemoryLocation,
        mask: u64,
    ) -> Result<DeviceAllocation> {
        if length == 0
            || block == 0
            || !(1..=64).contains(&count)
            || (count - 1)
                .checked_mul(block)
                .is_none_or(|prefix| prefix >= length)
        {
            return Err(Error::InvalidRange);
        }
        let priority = self.priority(location);
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("RDMA selector lock poisoned"))?;
        let device_count = state.inflight.len();
        let eligible = mask & state.available & priority.eligible;
        let mut candidates = [Candidate {
            device_id: 0,
            score: 0.0,
        }; 64];
        let mut size = 0;
        for id in 0..device_count {
            if eligible & (1u64 << id) == 0 {
                continue;
            }
            let score = if self.scheduling.smart_selection {
                ((state.inflight[id] as f64 + block as f64) / self.devices[id].bandwidth()
                    * self.scheduling.numa_tier_weights[priority.ranks[id] as usize])
                    .min(f64::MAX)
            } else {
                f64::from(priority.ranks[id])
            };
            candidates[size] = Candidate {
                device_id: id,
                score,
            };
            size += 1;
        }
        if size == 0 {
            return Err(Error::Unavailable(
                "no enabled RDMA device matches location and rail mask",
            ));
        }
        let candidates = &mut candidates[..size];
        let compare = |a: &Candidate, b: &Candidate| {
            a.score.total_cmp(&b.score).then_with(|| {
                ((a.device_id + device_count - state.cursor) % device_count)
                    .cmp(&((b.device_id + device_count - state.cursor) % device_count))
            })
        };
        let mut devices = [0; 64];
        let probe = self.scheduling.smart_selection && count > 1 && state.multipath_calls == 99;
        if count == 1 {
            // The single-grant path needs neither sorting nor a heap allocation.
            devices[0] = candidates
                .iter()
                .min_by(|a, b| compare(a, b))
                .ok_or(Error::Internal("empty RDMA candidates"))?
                .device_id as u8;
        } else if probe {
            // Rotate probe starts across parents, including when slices < NICs.
            candidates.sort_unstable_by_key(|c| {
                (c.device_id + device_count - state.probe_cursor) % device_count
            });
            for (index, device) in devices[..count].iter_mut().enumerate() {
                *device = candidates[index % size].device_id as u8;
            }
        } else {
            candidates.sort_unstable_by(compare);
            if self.scheduling.smart_selection {
                select_multi_path(
                    candidates,
                    &mut devices[..count],
                    self.scheduling.score_epsilon,
                );
            } else {
                let first_rank = candidates[0].score;
                let size = candidates
                    .iter()
                    .take_while(|c| c.score == first_rank)
                    .count();
                for (index, device) in devices[..count].iter_mut().enumerate() {
                    *device = candidates[index % size].device_id as u8;
                }
            }
        }
        // Validate every aggregate before publishing any reservation. The last
        // slice may be longer than block because planRdmaSlices folds a tail.
        let mut charges = [0usize; 64];
        for (index, &id) in devices[..count].iter().enumerate() {
            let bytes = if index + 1 == count {
                length - index * block
            } else {
                block
            };
            charges[usize::from(id)] += bytes;
        }
        for (id, &charge) in charges[..device_count].iter().enumerate() {
            state.inflight[id]
                .checked_add(charge)
                .ok_or(Error::ResourceExhausted)?;
        }
        for (id, &charge) in charges[..device_count].iter().enumerate() {
            state.inflight[id] += charge;
        }
        state.cursor = (usize::from(devices[count - 1]) + 1) % device_count;
        if self.scheduling.smart_selection && count > 1 {
            state.multipath_calls = (state.multipath_calls + 1) % 100;
            if probe {
                state.probe_cursor = state.cursor;
            }
        }
        Ok(DeviceAllocation {
            selector: Arc::clone(self),
            devices,
            length,
            block,
            count,
            cursor: 0,
        })
    }
}

/// TENT selectMultiPath: inverse-score floors, then the best NIC gets leftovers.
fn select_multi_path(candidates: &[Candidate], devices: &mut [u8], epsilon: f64) {
    // Normalizing to the best score keeps the equivalent weights in [0, 1].
    let best = (candidates[0].score + epsilon).min(f64::MAX);
    let mut weights = [0.0; 64];
    let mut total = 0.0;
    for (candidate, weight) in candidates.iter().zip(&mut weights) {
        *weight = best / (candidate.score + epsilon).min(f64::MAX);
        total += *weight;
    }
    let mut cursor = 0;
    for (candidate, weight) in candidates.iter().zip(weights) {
        let assigned =
            ((weight / total * devices.len() as f64) as usize).min(devices.len() - cursor);
        devices[cursor..cursor + assigned].fill(candidate.device_id as u8);
        cursor += assigned;
    }
    devices[cursor..].fill(candidates[0].device_id as u8);
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/quota/allocation.rs"]
mod tests;
