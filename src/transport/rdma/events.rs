use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Mutex, Weak};

use ibverbs_sys::{IBV_EVENT_COMM_EST, IBV_EVENT_SQ_DRAINED};

use super::cq_lane;
use crate::{Error, Result};

#[derive(Default)]
pub(super) struct Counters {
    pub(super) acknowledged_async_events: AtomicU64,
    pub(super) completion_events: AtomicU64,
}

#[derive(Default)]
pub(super) struct State {
    // Positive: event type + 1; negative: the consumer can no longer ACK.
    fault: AtomicI32,
    route_fault: AtomicBool,
    completion_fault: AtomicI32,
    progress: Mutex<Vec<Weak<cq_lane::ProgressState>>>,
}

impl State {
    pub(super) fn check(&self) -> Result<()> {
        let asynchronous = self.fault.load(Ordering::Acquire);
        let completion = self.completion_fault.load(Ordering::Acquire);
        if asynchronous >= 0 && completion != 0 {
            return Err(Error::Unquiesced {
                operation: "RDMA completion event consumer",
                code: completion,
            });
        }
        match asynchronous {
            0 if self.route_fault.load(Ordering::Acquire) => Err(Error::Backend {
                operation: "RDMA route changed",
                code: libc::ESTALE,
            }),
            0 => Ok(()),
            code if code > 0 => Err(Error::Backend {
                operation: "RDMA asynchronous event",
                code: code - 1,
            }),
            code => Err(Error::Unquiesced {
                operation: "RDMA asynchronous event consumer",
                code: -code,
            }),
        }
    }

    pub(super) fn attach(&self, progress: Weak<cq_lane::ProgressState>) {
        {
            let mut lanes = self.progress.lock().unwrap_or_else(|p| p.into_inner());
            lanes.retain(|lane| lane.strong_count() != 0);
            lanes.push(progress);
        }
        self.notify();
    }

    pub(super) fn fence_route(&self) {
        self.route_fault.store(true, Ordering::Release);
    }

    pub(super) fn notify(&self) {
        let mut lanes = self.progress.lock().unwrap_or_else(|p| p.into_inner());
        lanes.retain(|lane| {
            if let Some(lane) = lane.upgrade() {
                lane.wake();
                true
            } else {
                false
            }
        });
    }

    pub(super) fn lost_completion(&self, code: i32) {
        let _ = self.completion_fault.compare_exchange(
            0,
            code.max(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.notify();
    }

    pub(super) fn lost(&self, code: i32) {
        self.fault.store(-code.max(1), Ordering::Release);
        self.notify();
    }

    pub(super) fn event(&self, kind: u32) {
        // Only transport-independent informational events keep this fixed route
        // healthy. Unknown, port, topology and device changes fence the lane.
        if !matches!(kind, IBV_EVENT_COMM_EST | IBV_EVENT_SQ_DRAINED) {
            let code = kind.min(i32::MAX as u32 - 1) as i32 + 1;
            let _ = self
                .fault
                .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire);
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/events.rs"]
mod tests;
