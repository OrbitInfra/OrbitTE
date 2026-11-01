//! Ready payload dispatch across every context assigned to one data worker.

use std::sync::Arc;
use std::time::Instant;

use super::Lane;
use crate::Priority;
use crate::transport::rdma::bw_arbitration::{ArbFlow, order_by_urgency};

pub(in crate::transport::rdma) struct QueuedWork {
    pub(in crate::transport::rdma) namespace: u64,
    pub(in crate::transport::rdma) priority: Priority,
    pub(in crate::transport::rdma) enqueued: Instant,
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
}

impl PostQueue {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            flows: Vec::with_capacity(capacity),
            visited: Vec::with_capacity(64),
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
        self.entries.sort_unstable_by_key(|entry| {
            (entry.work.priority, entry.work.enqueued, entry.sequence)
        });
    }

    pub(super) fn dispatch(&mut self, lanes: &[Lane], arbitrate: bool) -> bool {
        let post = |entry: &PendingPost| {
            let lane = &lanes[entry.lane];
            lane.state
                .post(&lane.records[entry.record], Some(entry.work.namespace))
        };
        let mut progressed = false;
        if !arbitrate {
            for entry in &self.entries {
                progressed |= post(entry);
            }
            return progressed;
        }
        self.visited.clear();
        let mut priority = Priority::High;
        let now = Instant::now();
        for (index, entry) in self.entries.iter().enumerate() {
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
