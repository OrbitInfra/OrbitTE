//! Ready payload metadata and native SQ/RQ posting; lifetime remains with the CQ lane.

use super::*;

impl Record {
    pub(in crate::transport::rdma) fn promote(
        &self,
        namespace: u64,
        from: crate::Priority,
        to: crate::Priority,
        now: Instant,
        order: usize,
    ) -> bool {
        let mut request = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if self.terminal.load(Ordering::Acquire)
            || request.failure.is_some()
            || request.close_requested
        {
            return false;
        }
        let Some(work) = &mut request.work else {
            return false;
        };
        if work.queue.namespace() != namespace
            || work.priority != from
            || work.queue.pending()
            || work.queue.finished()
            || now >= work.deadline
        {
            return false;
        }
        work.priority = to;
        work.promoted = Some((now, order));
        true
    }

    pub(in crate::transport::rdma) fn queued_work(
        &self,
    ) -> Option<super::super::workers::QueuedWork> {
        if self.actions.load(Ordering::Acquire) & WORK == 0 {
            return None;
        }
        let request = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if self.terminal.load(Ordering::Acquire)
            || request.failure.is_some()
            || request.close_requested
        {
            return None;
        }
        let work = request.work.as_ref()?;
        if !matches!(
            work.operation,
            slice::Operation::Read | slice::Operation::Write
        ) || work.queue.pending()
            || work.queue.finished()
        {
            return None;
        }
        Some(super::super::workers::QueuedWork {
            namespace: work.queue.namespace(),
            priority: work.priority,
            enqueued: work.enqueued,
            ordered: work.promoted.map_or(work.enqueued, |(at, _)| at),
            order: work.promoted.map_or(0, |(_, order)| order),
            deadline: work.deadline,
            length: work.queue.unposted_bytes(),
        })
    }
}

impl ProgressState {
    pub(in crate::transport::rdma) fn post(
        &self,
        record: &Arc<Record>,
        namespace: Option<u64>,
    ) -> bool {
        if namespace.is_some()
            && self
                .control
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .fault
                .is_some()
        {
            return false;
        }
        if record.actions.load(Ordering::Acquire) & WORK == 0 {
            return false;
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if record.terminal.load(Ordering::Acquire)
            || request.failure.is_some()
            || request.close_requested
        {
            return false;
        }
        let Request {
            resources,
            work,
            phase,
            ..
        } = &mut *request;
        let Some(work) = work else {
            return false;
        };
        if namespace.is_some_and(|id| id != work.queue.namespace()) {
            return false;
        }
        if Instant::now() >= work.deadline {
            if work.queue.pending() {
                drop(request);
                self.fail(Fault::Timeout);
            } else {
                request.failure = Some(Fault::Timeout);
                record.changed.notify_all();
            }
            return false;
        }
        if namespace.is_none()
            && matches!(
                work.operation,
                slice::Operation::Read | slice::Operation::Write
            )
        {
            return false;
        }
        if work.queue.pending() || work.queue.finished() {
            return false;
        }
        let Some(resources) = resources else {
            return false;
        };
        let Some(grant) = &mut resources.grant else {
            return false;
        };
        if work.operation == slice::Operation::Send && grant.message.is_none() {
            request.failure = Some(Fault::Internal("missing RDMA SEND message"));
            record.changed.notify_all();
            return false;
        }
        let requests = match work.queue.reserve(self.config.max_cqes) {
            Ok(requests) => requests,
            Err(Error::ResourceExhausted) => {
                if record.send_queue.outstanding() < self.config.max_send_wr {
                    self.wait_quota(false);
                }
                return false;
            }
            Err(error) => {
                request.failure = Some(Fault::from_error(error));
                record.changed.notify_all();
                return false;
            }
        };
        let mut posted = sys::PostResult::default();
        if let Err(error) = self.asynchronous.check() {
            drop(request);
            self.fail(Fault::from_error(error));
            return false;
        }
        *phase = Phase::Posted;
        let owner = Arc::clone(&resources.connection);
        let mut connection = owner.retiring();
        let posted_at = Instant::now();
        let code = unsafe {
            match work.operation {
                slice::Operation::Read | slice::Operation::Write => (self.verbs.post)(
                    &mut connection.handles,
                    &grant.handles,
                    requests.as_ptr(),
                    requests.len(),
                    grant.buffer.address().as_ptr() as u64,
                    grant.buffer.len(),
                    work.remote.address,
                    work.remote.rkey,
                    i32::from(work.operation == slice::Operation::Write),
                    &mut posted,
                ),
                slice::Operation::Send => {
                    let Some(message) = &grant.message else {
                        return false;
                    };
                    (self.verbs.send_completion)(
                        &mut connection.handles,
                        &message.handles,
                        requests[0].id,
                        message.memory.address.as_ptr() as u64,
                        message.memory.length,
                        work.remote.rkey,
                        message.memory.bytes()[COMPLETION_MESSAGE_SIZE - 1]
                            != super::super::protocol::MR_AUTHORITY,
                        &mut posted,
                    )
                }
                _ => (self.verbs.window)(
                    &mut connection.handles,
                    &mut grant.handles,
                    requests[0].id,
                    grant.buffer.address().as_ptr() as u64,
                    grant.buffer.len(),
                    grant.permission,
                    work.operation == slice::Operation::Bind,
                    &mut posted,
                ),
            }
        };
        drop(connection);
        if let Err(error) = work.queue.posted(code, posted, posted_at) {
            let fault = Fault::from_error(error);
            let pending = work.queue.pending();
            request.failure = Some(fault);
            drop(request);
            self.quota.released();
            if pending {
                self.fail(fault);
            } else {
                record.actions.fetch_and(!WORK, Ordering::Release);
            }
            record.changed.notify_all();
        }
        true
    }

    pub(super) fn post_receive(&self, record: &Arc<Record>) -> bool {
        if record.actions.load(Ordering::Acquire) & RECEIVE == 0 {
            return false;
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if record.terminal.load(Ordering::Acquire)
            || request.failure.is_some()
            || request.close_requested
        {
            return false;
        }
        let Request {
            resources, receive, ..
        } = &mut *request;
        let (Some(resources), Some(receive)) = (resources, receive) else {
            return false;
        };
        if receive.completed() {
            return false;
        }
        if Instant::now() >= receive.deadline {
            let pending = resources.connection.retiring().receive_queue.outstanding() != 0;
            request.failure = Some(Fault::Timeout);
            drop(request);
            if pending {
                self.fail(Fault::Timeout);
            }
            record.changed.notify_all();
            return false;
        }
        if receive.posted() {
            return false;
        }
        if let Err(error) = self.asynchronous.check() {
            drop(request);
            self.fail(Fault::from_error(error));
            return false;
        }
        let owner = Arc::clone(&resources.connection);
        let mut state = owner.retiring();
        let connection = &mut *state;
        let result = connection.receive_queue.post(
            receive.id,
            self.config.max_cqes,
            &mut connection.handles,
            self.verbs,
        );
        let pending = connection.receive_queue.outstanding() != 0;
        drop(state);
        match result {
            Err(Error::ResourceExhausted) => {
                self.wait_quota(true);
                return false;
            }
            Err(error) => {
                let fault = Fault::from_error(error);
                request.failure = Some(fault);
                drop(request);
                self.quota.released();
                if pending {
                    self.fail(fault);
                }
            }
            Ok(()) => {
                drop(request);
            }
        }
        record.changed.notify_all();
        true
    }
}
