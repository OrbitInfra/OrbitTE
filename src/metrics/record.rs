//! The admission owner carries one observation through retries. The worker is
//! the only writer. Native file callbacks may finish siblings independently.
use super::schema::{Metric as M, Recorder, attempt_slot, slot};
use crate::{Error, TransferOp, Transport};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering::Relaxed},
    },
    time::Instant,
};

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Failure {
    Submit,
    Poll,
    Timeout,
    Canceled,
}
impl Failure {
    fn of(error: &Error, posted: bool) -> Self {
        match error {
            Error::Timeout => Self::Timeout,
            Error::Cancelled => Self::Canceled,
            _ if posted => Self::Poll,
            _ => Self::Submit,
        }
    }
    fn failed(self) -> bool {
        matches!(self, Self::Submit | Self::Poll)
    }
    // The other two cell values represent an unstarted and a successful attempt.
    fn code(self) -> u8 {
        self as u8 + 2
    }
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    fn decoded(code: u8) -> Option<Self> {
        [Self::Submit, Self::Poll, Self::Timeout, Self::Canceled]
            .into_iter()
            .find(|r| r.code() == code)
    }
}
fn outcome(reason: Option<Failure>) -> usize {
    match reason {
        None => 0,
        Some(Failure::Submit | Failure::Poll) => 1,
        Some(Failure::Timeout) => 2,
        Some(Failure::Canceled) => 3,
    }
}
const UNTIMED_ATTEMPT: u64 = u64::MAX;

pub(crate) struct Observation {
    cells: Arc<Recorder>,
    op: TransferOp,
    length: u64,
    deadline: Option<Instant>,
    submitted: Option<Instant>,
    route: AtomicUsize,
    dispatch: AtomicU64,
    first_post: AtomicU64,
    attempt: AtomicU64,
    public: AtomicU64,
    phase: AtomicU8,
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    pub(crate) file_posted: AtomicBool,
    last: AtomicU8,
    terminal: AtomicBool,
}
impl Observation {
    pub(crate) fn new(
        cells: &Arc<Recorder>,
        op: TransferOp,
        t: Transport,
        length: usize,
        deadline: Option<Instant>,
        submitted: Option<Instant>,
    ) -> Self {
        Self {
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            file_posted: AtomicBool::default(),
            cells: cells.clone(),
            op,
            length: length as u64,
            deadline,
            submitted,
            route: AtomicUsize::new(slot(t)),
            dispatch: AtomicU64::default(),
            first_post: AtomicU64::default(),
            attempt: AtomicU64::default(),
            public: AtomicU64::default(),
            phase: AtomicU8::default(),
            last: AtomicU8::default(),
            terminal: AtomicBool::default(),
        }
    }
    pub(crate) fn accept_public(&self) {
        if self.cells.enabled.load(Relaxed) {
            let op = usize::from(self.op == TransferOp::Write);
            self.public.fetch_add(1, Relaxed);
            self.cells.add(M::LogicalStarted, op, 1);
            self.cells.add(M::LogicalInflight, op, 1);
        }
    }
    fn settle_public(&self, reason: Option<Failure>) {
        let n = self.public.swap(0, Relaxed);
        if n != 0 {
            let op = usize::from(self.op == TransferOp::Write);
            self.cells
                .add(M::LogicalOutcomes, op * 4 + outcome(reason), n);
            self.cells.cells[M::LogicalInflight as usize][op]
                .value
                .fetch_sub(n, Relaxed);
        }
    }
    // A submission may stop before invoking a trailing owner. These aliases still
    // need an outcome but never entered a transport or produced legacy byte samples.
    pub(crate) fn settle_skipped(&self, canceled: impl FnOnce() -> bool) {
        if self.public.load(Relaxed) != 0 {
            self.skipped(canceled());
        }
    }
    fn skipped(&self, canceled: bool) {
        self.settle_public(Some(if canceled {
            Failure::Canceled
        } else {
            Failure::Submit
        }));
    }

    // Zero means no event. One represents an event at the submit instant.
    fn stamp(&self) -> u64 {
        let origin = self.submitted.unwrap_or(self.cells.origin);
        origin
            .elapsed()
            .as_nanos()
            .min(u128::from(UNTIMED_ATTEMPT - 2)) as u64
            + 1
    }
    pub(crate) fn dispatched(&self) {
        if self.cells.enabled.load(Relaxed) {
            let _ = self
                .dispatch
                .compare_exchange(0, self.stamp(), Relaxed, Relaxed);
        }
    }
    pub(crate) fn begin(&self, t: Transport) {
        if self.attempt.load(Relaxed) != 0 {
            return;
        }
        let enabled = self.cells.enabled.load(Relaxed);
        let at = if enabled {
            self.stamp()
        } else {
            UNTIMED_ATTEMPT
        };
        if self
            .attempt
            .compare_exchange(0, at, Relaxed, Relaxed)
            .is_err()
        {
            return;
        }
        self.last.store(0, Relaxed);
        #[cfg(any(feature = "io-uring", feature = "gds"))]
        self.file_posted.store(false, Relaxed);
        let route = slot(t);
        // File registration can retire after preparation but before the first
        // attempt. An explicit retry already published its new route.
        let previous = self.route.swap(route, Relaxed);
        if enabled && previous != route {
            self.cells.add(
                M::Failover,
                previous * super::schema::TRANSPORTS.len() + route,
                1,
            );
        }
        if enabled {
            self.cells.add(M::Inflight, route, 1);
            self.cells.add(M::Attempts, attempt_slot(route, self.op), 1);
            let _ = self.first_post.compare_exchange(0, at, Relaxed, Relaxed);
        }
    }
    #[cfg(feature = "cuda")]
    pub(crate) fn proxy_submitted(&self) {
        if self.cells.enabled.load(Relaxed) {
            let _ = self
                .first_post
                .compare_exchange(0, self.stamp(), Relaxed, Relaxed);
        }
    }
    pub(crate) fn note_failure(&self, error: &Error, posted: bool) {
        let reason = Failure::of(error, posted);
        let _ = self
            .phase
            .compare_exchange(0, reason.code(), Relaxed, Relaxed);
    }
    pub(crate) fn retry(&self, t: Transport) {
        let next = slot(t);
        let old = self.route.swap(next, Relaxed);
        if self.cells.enabled.load(Relaxed) {
            self.cells
                .add(M::Failover, old * super::schema::TRANSPORTS.len() + next, 1);
        }
    }
    pub(crate) fn finish(&self, error: Option<&Error>, posted: bool) {
        let start = self.attempt.swap(0, Relaxed);
        if start == 0 {
            return;
        }
        let t = self.route.load(Relaxed);
        let reason = error.map(|e| Failure::of(e, posted));
        self.last.store(reason.map_or(1, Failure::code), Relaxed);
        if let Some(error) = error {
            self.note_failure(error, posted);
        }
        if start == UNTIMED_ATTEMPT {
            return;
        }
        let i = attempt_slot(t, self.op);
        self.cells
            .add(M::AttemptOutcomes, i * 4 + outcome(reason), 1);
        self.cells.cells[M::Inflight as usize][t]
            .value
            .fetch_sub(1, Relaxed);
        if reason.is_some_and(Failure::failed) {
            self.cells.add(M::AttemptFailures, i, 1);
        }
        self.cells.observe(
            M::AttemptLatency,
            i,
            self.stamp().saturating_sub(start) / 1000,
        );
    }
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    pub(crate) fn file_terminal(&self, reported: Option<&Error>) {
        if let Some(error) = reported {
            self.terminal(Some(error));
            return;
        }
        let code = self.last.load(Relaxed);
        if code != 0 {
            self.complete(Failure::decoded(code), false);
        }
    }
    pub(crate) fn terminal(&self, error: Option<&Error>) {
        self.complete(
            error.map(|e| Failure::of(e, false)),
            matches!(error, Some(Error::DeadlineInfeasible)),
        );
    }
    fn complete(&self, reason: Option<Failure>, dropped: bool) {
        if self.terminal.swap(true, Relaxed) {
            return;
        }
        self.settle_public(if dropped {
            Some(Failure::Canceled)
        } else {
            reason
        });
        if !self.cells.enabled.load(Relaxed) {
            return;
        }
        let t = self.route.load(Relaxed);
        let read = self.op == TransferOp::Read;
        let end = self.stamp();
        let latency = end.saturating_sub(1);
        let past = self
            .submitted
            .zip(self.deadline)
            .is_some_and(|(s, d)| d <= s);
        if past || dropped {
            self.cells.add(M::Infeasible, t, 1);
        }
        if let Some(mut r) = reason {
            if r.failed() {
                r = if self.phase.load(Relaxed) == Failure::Poll.code() {
                    Failure::Poll
                } else {
                    Failure::Submit
                };
            }
            self.cells.add(
                M::TaskFailures,
                t * super::schema::REASONS.len() + r as usize,
                1,
            );
            if !r.failed() {
                return;
            }
            self.cells.add(
                if read {
                    M::ReadFailures
                } else {
                    M::WriteFailures
                },
                t,
                1,
            );
        }
        self.cells.add(
            if read {
                M::ReadRequests
            } else {
                M::WriteRequests
            },
            t,
            1,
        );
        if reason.is_some() {
            return;
        }
        self.cells.add(
            if read { M::ReadBytes } else { M::WriteBytes },
            t,
            self.length,
        );
        self.cells.observe(
            if read { M::ReadSize } else { M::WriteSize },
            t,
            self.length,
        );
        // Sampling may have resumed during a request. Its bytes are known,
        // but no logical duration can be recovered without its submit timestamp.
        if self.submitted.is_none() {
            return;
        }
        if latency > 0 {
            self.cells.observe(
                if read {
                    M::ReadLatency
                } else {
                    M::WriteLatency
                },
                t,
                latency / 1000,
            );
        }
        let dispatch = self.dispatch.load(Relaxed);
        let post = self.first_post.load(Relaxed);
        if dispatch != 0 {
            self.cells.observe(M::QueueWait, t, (dispatch - 1) / 1000);
            if post != 0 {
                self.cells
                    .observe(M::Dispatch, t, post.saturating_sub(dispatch) / 1000);
                self.cells
                    .observe(M::TransportTime, t, end.saturating_sub(post) / 1000);
            }
        }
        if let Some(window) = self
            .submitted
            .zip(self.deadline)
            .and_then(|(s, d)| d.checked_duration_since(s))
            .filter(|d| !d.is_zero())
        {
            self.cells.observe(
                M::Mlu,
                t,
                (u128::from(latency) * 1000 / window.as_nanos()).min(u128::from(u64::MAX)) as u64,
            );
        }
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        // An unwinding worker still owes a terminal outcome for every recorded start.
        if self.attempt.load(Relaxed) != 0 {
            self.finish(Some(&Error::Internal("transfer worker exited")), false);
        }
        if self.public.load(Relaxed) != 0 {
            self.skipped(false);
        }
    }
}
