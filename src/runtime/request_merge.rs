//! One admitted transfer may carry several public task IDs. Segment authority,
//! operation and submission deadline are shared by construction.
use crate::engine::PreparedTransfer;
use crate::{BatchReport, TransferOp, TransferRequest};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Alias {
    pub(crate) owner: usize,
    pub(crate) offset: usize,
    pub(crate) length: usize,
}

#[derive(Debug)]
pub(crate) struct Owner {
    pub(crate) representative: usize,
    pub(crate) length: usize,
}

#[derive(Debug, Default)]
pub(crate) struct TaskMap {
    pub(crate) owners: Vec<Owner>,
    pub(crate) public: Vec<Alias>,
}

impl TaskMap {
    pub(crate) fn identity(requests: &[TransferRequest]) -> Self {
        Self {
            owners: requests
                .iter()
                .enumerate()
                .map(|(i, r)| Owner {
                    representative: i,
                    length: r.length,
                })
                .collect(),
            public: requests
                .iter()
                .enumerate()
                .map(|(i, r)| Alias {
                    owner: i,
                    offset: 0,
                    length: r.length,
                })
                .collect(),
        }
    }

    pub(crate) fn merge(
        requests: &[TransferRequest],
        transfers: &mut Vec<PreparedTransfer>,
        contracts: &[Option<&crate::runtime::qos_contract::CompiledContract>],
        may_share_memory: bool,
        operation: TransferOp,
        (limit, hp_limit): (usize, usize),
        enabled: bool,
    ) -> Self {
        if !enabled || requests.len() < 2 {
            return Self::identity(requests);
        }
        let effective = |i: usize| {
            let r = &requests[i];
            r.qos.as_ref().map_or_else(
                || {
                    contracts[i]
                        .and_then(|c| c.fields.priority)
                        .unwrap_or(r.priority)
                },
                |p| p.effective_priority(),
            )
        };
        let partitions = ordered_partitions(requests, transfers, operation, may_share_memory);
        let mut indices: Vec<usize> = (0..requests.len()).collect();
        indices.sort_unstable_by_key(|&i| {
            let r = &requests[i];
            (
                partitions[i],
                r.transport as u8,
                r.remote_address,
                r.local_address,
                i,
            )
        });
        let can_merge = |j: usize, i: usize, length: usize| {
            let (a, b) = (&requests[j], &requests[i]);
            let (left, right) = (&transfers[j], &transfers[i]);
            let limit = if matches!(
                a.transport,
                crate::Transport::HpTcp | crate::Transport::Unspec
            ) {
                limit.min(hp_limit)
            } else {
                limit
            };
            partitions[i] == partitions[j]
                && a.transport == b.transport
                && a.priority == b.priority
                && a.deadline == b.deadline
                && a.remote_staging == b.remote_staging
                && a.intent == b.intent
                && a.policy_name == b.policy_name
                && contracts[j].map(std::ptr::from_ref) == contracts[i].map(std::ptr::from_ref)
                && effective(j) == effective(i)
                && left.selection == right.selection
                && left.local.id() == right.local.id()
                && left.remote.id() == right.remote.id()
                && length.checked_add(b.length).is_some_and(|n| n <= limit)
                && a.local_address.checked_add(a.length as u64) == Some(b.local_address)
                && a.remote_address.checked_add(a.length as u64) == Some(b.remote_address)
        };
        let mut starts: Vec<usize> = Vec::new();
        let mut public = vec![Alias::default(); requests.len()];
        let mut previous = None;
        let mut length = 0;
        for i in indices {
            if !previous.is_some_and(|j| can_merge(j, i, length)) {
                starts.push(i);
                length = 0;
            }
            public[i] = Alias {
                owner: starts.len() - 1,
                offset: length,
                length: requests[i].length,
            };
            length += requests[i].length;
            previous = Some(i);
        }
        // Commit owners in first-public-task order. Each payload begins at
        // the lowest-address member, which need not be its representative.
        let mut ordinal = vec![None; starts.len()];
        let mut owners: Vec<Owner> = Vec::with_capacity(starts.len());
        let mut physical = Vec::with_capacity(starts.len());
        for (i, a) in public.iter_mut().enumerate() {
            let sorted = a.owner;
            a.owner = *ordinal[sorted].get_or_insert_with(|| {
                let slot = owners.len();
                owners.push(Owner {
                    representative: i,
                    length: 0,
                });
                physical.push(transfers[starts[sorted]].clone());
                slot
            });
            owners[a.owner].length += a.length;
        }
        for (t, o) in physical.iter_mut().zip(&owners) {
            t.length = o.length;
        }
        *transfers = physical;
        Self { owners, public }
    }

    pub(crate) fn public_report(
        &self,
        mut report: BatchReport,
        operation: TransferOp,
    ) -> BatchReport {
        let completed = report.completed_requests;
        let prefix = self
            .public
            .iter()
            .take_while(|a| a.owner < completed)
            .count();
        report.completed_requests = prefix;
        report.transferred_bytes = self.public[..prefix].iter().map(|a| a.length).sum();
        if let Some(failure) = &mut report.failure {
            let through = failure.request_index + failure.affected_requests;
            let end = self
                .public
                .iter()
                .rposition(|a| a.owner < through)
                .map_or(prefix, |i| i + 1);
            failure.request_index = prefix;
            failure.affected_requests = end.saturating_sub(prefix);
            if operation == TransferOp::Write
                && self.public[prefix..end].iter().any(|a| a.owner < completed)
            {
                failure.remote_effect = crate::RemoteEffect::Unknown;
            }
        }
        report
    }
}

// Reordering disjoint writes is safe. A destination touched twice, or a
// self-transfer that reads another piece's destination, needs public order.
// A direct alias after a staged owner must wait for the staged completion
// or failure. Fence CUDA path changes before sorting by hint and address.
fn ordered_partitions(
    reqs: &[TransferRequest],
    transfers: &[PreparedTransfer],
    op: TransferOp,
    may_share_memory: bool,
) -> Vec<usize> {
    let mut dest = Vec::with_capacity(reqs.len());
    let mut src = Vec::new();
    let mut hazards = vec![false; reqs.len()];
    for (i, r) in reqs.iter().enumerate() {
        let (from, to) = if op == TransferOp::Write {
            (r.local_address, r.remote_address)
        } else {
            (r.remote_address, r.local_address)
        };
        dest.push((to, to + r.length as u64, i));
        if may_share_memory {
            src.push((from, from + r.length as u64, i));
        }
    }
    dest.sort_unstable();
    let mut furthest = (0, 0);
    for &(start, end, i) in &dest {
        if start < furthest.0 {
            hazards[i] = true;
            hazards[furthest.1] = true;
        }
        if end > furthest.0 {
            furthest = (end, i);
        }
    }
    if may_share_memory {
        src.sort_unstable();
        mark_intersections(&src, &dest, &mut hazards);
        mark_intersections(&dest, &src, &mut hazards);
    }
    let mut partition = 0;
    let mut previous = None;
    hazards
        .into_iter()
        .zip(reqs.iter().zip(transfers))
        .map(|(hazard, (request, transfer))| {
            let local = transfer.local.memory_type();
            let remote = transfer.remote.memory_type();
            let path = (local == crate::MemoryType::Cuda || remote == crate::MemoryType::Cuda)
                .then_some((
                    transfer.selection.transport,
                    request.remote_staging,
                    local,
                    remote,
                ));
            if previous != path {
                partition += 1;
                previous = path;
            }
            if hazard {
                partition += 2;
                partition - 1
            } else {
                partition
            }
        })
        .collect()
}

// Both interval lists are in start-address order. Prefix maxima also cover
// nested registrations without treating nearby distinct ranges as aliases.
fn mark_intersections(
    ranges: &[(u64, u64, usize)],
    others: &[(u64, u64, usize)],
    hazards: &mut [bool],
) {
    let mut end = 0;
    let ends: Vec<_> = others
        .iter()
        .map(|r| {
            end = end.max(r.1);
            end
        })
        .collect();
    for &(start, end, i) in ranges {
        let n = others.partition_point(|r| r.0 < end);
        if n != 0 && ends[n - 1] > start {
            hazards[i] = true;
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/request_merge.rs"]
mod tests;
