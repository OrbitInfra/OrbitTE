//! Ready payload dispatch across every context assigned to one data worker.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::super::promotion_policy;

use super::Lane;
use crate::Priority;
use crate::transport::rdma::bw_arbitration::{ArbFlow, order_by_urgency};

pub(in crate::transport::rdma) struct QueuedWork {
    pub(in crate::transport::rdma) namespace: u64,
    pub(in crate::transport::rdma) priority: Priority,
    pub(in crate::transport::rdma) enqueued: Instant,
    pub(in crate::transport::rdma) ordered: Instant,
    pub(in crate::transport::rdma) order: usize,
    pub(in crate::transport::rdma) deadline: Instant,
    pub(in crate::transport::rdma) length: usize,
}

struct PendingPost {
    lane: usize,
    record: usize,
    work: QueuedWork,
    nic: usize,
    sequence: usize,
}

pub(super) struct PostQueue {
    entries: Vec<PendingPost>,
    flows: Vec<ArbFlow>,
    visited: Vec<usize>,
    next_promotion: Instant,
}

impl PostQueue {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            flows: Vec::with_capacity(capacity),
            visited: Vec::with_capacity(64),
            next_promotion: Instant::now(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(super) fn collect(&mut self, lanes: &[Lane], cursor: usize) {
        for offset in 0..lanes.len() {
            let index = (offset + cursor % lanes.len()) % lanes.len();
            let lane = &lanes[index];
            for offset in 0..lane.records.len() {
                let record_index = (offset + lane.cursor % lane.records.len()) % lane.records.len();
                let record = &lane.records[record_index];
                if let Some(work) = record.queued_work() {
                    self.entries.push(PendingPost {
                        lane: index,
                        record: record_index,
                        work,
                        // The record snapshot pins this identity through dispatch.
                        // No pointer is dereferenced; absent providers share FIFO.
                        nic: record
                            .send_queue
                            .bandwidth
                            .as_ref()
                            .map_or(0, |info| Arc::as_ptr(info) as usize),
                        sequence: self.entries.len(),
                    });
                }
            }
        }
        self.sort();
    }

    fn sort(&mut self) {
        self.entries.sort_unstable_by_key(|entry| {
            (
                entry.work.priority,
                entry.work.ordered,
                entry.work.order,
                entry.sequence,
            )
        });
    }

    pub(super) fn has_blocked(&self, allowed: Priority) -> bool {
        self.entries
            .last()
            .is_some_and(|entry| entry.work.priority > allowed)
    }

    pub(super) fn promote(
        &mut self,
        lanes: &[Lane],
        now: Instant,
        timeout: Duration,
        per_entry: bool,
    ) -> Option<Instant> {
        if !self.has_blocked(Priority::High) {
            return None;
        }
        if now >= self.next_promotion {
            self.next_promotion = now + promotion_policy::CHECK_INTERVAL;
            let mut changed = false;
            for (from, to) in [
                (Priority::Medium, Priority::High),
                (Priority::Low, Priority::Medium),
            ] {
                let mut head = None;
                let mut promoted = false;
                for (order, entry) in self
                    .entries
                    .iter_mut()
                    .enumerate()
                    .filter(|(_, entry)| entry.work.priority == from)
                {
                    let expired = promotion_policy::timed_out(entry.work.enqueued, now, timeout);
                    let head_expired = *head.get_or_insert(expired);
                    if (if per_entry { expired } else { head_expired })
                        && lanes[entry.lane].records[entry.record].promote(
                            entry.work.namespace,
                            from,
                            to,
                            now,
                            order,
                        )
                    {
                        entry.work.priority = to;
                        entry.work.ordered = now;
                        entry.work.order = order;
                        promoted = true;
                    }
                }
                changed |= promoted;
                if promoted && !per_entry {
                    break;
                }
            }
            if changed {
                self.sort();
            }
        }
        self.has_blocked(Priority::High)
            .then_some(self.next_promotion)
    }

    pub(super) fn dispatch(&mut self, lanes: &[Lane], arbitrate: bool, allowed: Priority) -> bool {
        let post = |entry: &PendingPost| {
            let lane = &lanes[entry.lane];
            lane.state
                .post(&lane.records[entry.record], Some(entry.work.namespace))
        };
        let mut progressed = false;
        if !arbitrate {
            for entry in self
                .entries
                .iter()
                .take_while(|entry| entry.work.priority <= allowed)
            {
                progressed |= post(entry);
            }
            return progressed;
        }
        self.visited.clear();
        let mut priority = Priority::High;
        let now = Instant::now();
        for (index, entry) in self
            .entries
            .iter()
            .enumerate()
            .take_while(|(_, entry)| entry.work.priority <= allowed)
        {
            if priority != entry.work.priority {
                priority = entry.work.priority;
                self.visited.clear();
            }
            if self.visited.contains(&entry.nic) {
                continue;
            }
            self.visited.push(entry.nic);
            self.flows.clear();
            self.flows.extend(
                self.entries[index..]
                    .iter()
                    .enumerate()
                    .take_while(|(_, candidate)| candidate.work.priority == priority)
                    .filter(|(_, candidate)| candidate.nic == entry.nic)
                    .map(|(offset, candidate)| ArbFlow {
                        index: index + offset,
                        deadline: Some(candidate.work.deadline),
                        length: candidate.work.length,
                    }),
            );
            if let Some(info) = &lanes[entry.lane].records[entry.record].send_queue.bandwidth {
                let (bandwidth, posted) = info.arbitration();
                order_by_urgency(&mut self.flows, now, bandwidth, posted);
            }
            for flow in &self.flows {
                progressed |= post(&self.entries[flow.index]);
            }
        }
        progressed
    }
}
