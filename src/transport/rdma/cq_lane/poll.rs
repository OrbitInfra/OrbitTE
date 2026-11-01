use super::*;

impl ProgressState {
    pub(in crate::transport::rdma) fn wake_quota(&self) {
        if self.quota_waiting.swap(false, Ordering::AcqRel) {
            self.wake();
        }
    }

    fn wait_quota(&self, receive: bool) {
        self.quota_waiting.store(true, Ordering::Release);
        // Enrollment precedes the budget recheck: a concurrent release either
        // sees the waiter or leaves available capacity and a retained unpark token.
        let usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        let debt = if receive {
            usage.receive_work_requests
        } else {
            usage.work_requests
        };
        if debt < self.config.max_cqes {
            self.wake_quota();
        }
    }

    pub(in crate::transport::rdma) fn wake(&self) {
        if let Some(worker) = self.progress_thread.get() {
            worker.unpark();
        }
    }

    pub(super) fn signal(&self) {
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        control.dirty = true;
        self.wake();
    }

    pub(super) fn fail(&self, fault: Fault) {
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        if control.fault.is_none() {
            control.accepting = false;
            let idle = control.qps.detach_all();
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .idle_endpoints -= idle;
            control.fault = Some(fault);
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .progress_failures += 1;
        }
        control.dirty = true;
        self.wake();
    }

    pub(super) fn close_cq(&self) -> std::result::Result<(), (&'static str, i32)> {
        let mut queues = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            [control.cq.take(), control.receive_cq.take()]
        };
        for index in 0..queues.len() {
            if let Some(queue) = &mut queues[index] {
                if let Err(error) = queue.close(self.verbs) {
                    let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
                    control.cq = queues[0].take();
                    control.receive_cq = queues[1].take();
                    return Err(error);
                }
            }
            queues[index] = None;
        }
        if let Some(events) = &self.completion_events {
            events.close()?;
        }
        Ok(())
    }

    pub(super) fn next_work_namespace(&self) -> Result<u64> {
        let mut current = self.next_work_namespace.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(1)
                .filter(|next| {
                    current != 0 && next.checked_mul(super::super::WR_ID_STRIDE).is_some()
                })
                .ok_or(Error::ResourceExhausted)?;
            match self.next_work_namespace.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(current),
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn retain(&self, operation: &'static str, code: i32) {
        let records = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            if control.finished {
                return;
            }
            if control.fault.is_none() {
                self.usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .progress_failures += 1;
            }
            control.fault = Some(Fault::Unquiesced(operation, code));
            control.accepting = false;
            let idle = control.qps.detach_all();
            self.usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .idle_endpoints -= idle;
            control.qps.take_all()
        };
        let mut count = 0;
        let mut grants = 0;
        let mut bytes = 0;
        let mut debt = 0;
        let mut windows = 0;
        let mut receive_debt = 0;
        let mut message_mrs = 0;
        let mut message_bytes = 0;
        for (index, record) in records.iter().enumerate() {
            let first_qp = !records[..index]
                .iter()
                .any(|other| Arc::ptr_eq(&other.connection, &record.connection));
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if request.phase != Phase::Retained {
                count += usize::from(
                    first_qp
                        && request
                            .resources
                            .as_ref()
                            .is_some_and(|r| r.connection.retiring().lease.is_some()),
                );
                grants += usize::from(
                    request
                        .resources
                        .as_ref()
                        .is_some_and(|r| r.grant.is_some()),
                );
                bytes += request
                    .resources
                    .as_ref()
                    .and_then(|r| r.grant.as_ref())
                    .map_or(0, |g| g.buffer.len());
                if first_qp {
                    debt += record.connection.retiring().send_queue.outstanding();
                }
                if let Some(resources) = request.resources.as_ref().filter(|_| first_qp) {
                    let connection = resources.connection.retiring();
                    let queue = &connection.receive_queue;
                    receive_debt += queue.outstanding();
                    let (mrs, bytes) = queue.memory_usage();
                    message_mrs += mrs;
                    message_bytes += bytes;
                }
                if let Some(message) = request
                    .resources
                    .as_ref()
                    .and_then(|r| r.grant.as_ref())
                    .and_then(|g| g.message.as_ref())
                {
                    message_mrs += usize::from(!message.handles.mr.is_null());
                    message_bytes += message.memory.length;
                }
                windows +=
                    usize::from(request.resources.as_ref().is_some_and(|r| {
                        r.grant.as_ref().is_some_and(|g| !g.handles.mw.is_null())
                    }));
                request.phase = Phase::Retained;
                request.failure = Some(Fault::Unquiesced(operation, code));
            }
            record.changed.notify_all();
        }
        let cq = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            control.retained = Some((operation, code));
            let cq = [control.cq.take(), control.receive_cq.take()];
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.retained_endpoints += count;
            usage.retained_grants += grants;
            usage.retained_bytes += bytes;
            usage.retained_work_requests += debt;
            usage.retained_cqs += cq.iter().filter(|q| q.is_some()).count();
            usage.retained_receive_work_requests += receive_debt;
            usage.retained_message_mrs += message_mrs;
            usage.retained_message_bytes += message_bytes;
            usage.retained_windows += windows;
            control.finished = true;
            cq
        };
        // Fence every constructor before publishing completion or losing owners.
        // Actual CQ/PD, QPs/MRs, backing, DMA loans and unresolved debt survive.
        if let Some(events) = &self.completion_events {
            events.retain();
        }
        std::mem::forget((cq, records));
        self.changed.notify_all();
    }

    fn unlock(request: &mut Request) -> std::result::Result<(), (&'static str, i32)> {
        if request
            .resources
            .as_ref()
            .is_some_and(|r| r.connection.retiring().send_queue.outstanding() != 0)
            || request.work.as_ref().is_some_and(|w| w.queue.pending())
            || request
                .resources
                .as_ref()
                .is_some_and(|r| r.connection.retiring().receive_queue.outstanding() != 0)
        {
            return Err(("RDMA grant has unresolved completion debt", libc::EBUSY));
        }
        if let Some(resources) = &mut request.resources {
            if !resources.connection.retiring().closed {
                return Err(("RDMA connection lacks native drain receipt", libc::EBUSY));
            }
            if let Some(grant) = &mut resources.grant {
                if !grant.handles.mw.is_null() || !grant.handles.mr.is_null() {
                    return Err(("RDMA grant lacks native drain receipt", libc::EBUSY));
                }
                grant.buffer.release()?;
            }
        }
        request.phase = Phase::Closed;
        Ok(())
    }

    fn finish_request(&self, record: &Arc<Record>) -> std::result::Result<(), (&'static str, i32)> {
        if record.actions.load(Ordering::Acquire) & FINISH == 0 {
            return Ok(());
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if record.terminal.load(Ordering::Acquire) || request.close_requested {
            return Ok(());
        }
        if request.phase != Phase::Drained {
            return Err(("RDMA finish phase changed", libc::EIO));
        }
        let Request {
            resources,
            finish,
            receive,
            ..
        } = &mut *request;
        let resources = resources
            .as_mut()
            .ok_or(("missing RDMA finish resources", libc::EIO))?;
        let receipt = finish
            .as_ref()
            .ok_or(("missing sealed RDMA receipt", libc::EIO))?;
        let grant = resources
            .grant
            .as_mut()
            .ok_or(("missing RDMA finish grant", libc::EIO))?;
        grant.close_drained(&resources.connection.retiring(), receipt, self.verbs)?;
        if let Some(work) = receive {
            resources
                .connection
                .retiring()
                .receive_queue
                .release(work, self.verbs)?;
        }
        grant.buffer.release()?;
        drop(resources.grant.take());
        request.work = None;
        request.receive = None;
        request.message_sent = false;
        request.payload_operation = None;
        request.mixed_operations = false;
        request.finish = None;
        record.actions.store(0, Ordering::Release);
        self.usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completed_reusable_grants += 1;
        record.changed.notify_all();
        Ok(())
    }

    fn close_request(
        &self,
        record: &Arc<Record>,
    ) -> std::result::Result<bool, (&'static str, i32)> {
        if record.actions.load(Ordering::Acquire) & CLOSE == 0 {
            return Ok(false);
        }
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        if !request.close_requested || matches!(request.phase, Phase::Closed | Phase::Retained) {
            return Ok(false);
        }
        if request
            .resources
            .as_ref()
            .is_some_and(|r| r.connection.retiring().send_queue.outstanding() != 0)
            || request
                .work
                .as_ref()
                .is_some_and(|work| work.queue.pending())
            || request
                .resources
                .as_ref()
                .is_some_and(|r| r.connection.retiring().receive_queue.outstanding() != 0)
        {
            drop(request);
            self.fail(Fault::Internal("RDMA close with unresolved work"));
            return Ok(false);
        }
        let resources = request
            .resources
            .as_mut()
            .ok_or(("RDMA resources missing", libc::EIO))?;
        if let Err(Error::Unquiesced { operation, code }) = self.asynchronous.check() {
            return Err((operation, code));
        }
        resources.connection.retiring().close(self.verbs)?;
        if let Some(grant) = &mut resources.grant {
            grant.close(&resources.connection.retiring(), self.verbs)?;
        }
        resources
            .connection
            .retiring()
            .receive_queue
            .close(self.verbs)?;
        resources.connection.retiring().close_grant_pd(self.verbs)?;
        Self::unlock(&mut request)?;
        self.control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .qps
            .remove_retired(record);
        drop(request);
        record.changed.notify_all();
        Ok(true)
    }

    fn abort(&self, records: &[Arc<Record>], fault: Fault) {
        // Fence accepted constructors under the same lock as native QP creation.
        // No other thread polls or submits native WRs.
        for record in records {
            #[cfg(test)]
            tests::before_abort_lock(self, record);
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Err(Error::Unquiesced { operation, code }) = self.asynchronous.check() {
                drop(request);
                self.retain(operation, code);
                return;
            }
            request.failure.get_or_insert(fault);
            let Some(resources) = &mut request.resources else {
                continue;
            };
            let closed = resources.connection.retiring().close(self.verbs);
            if let Err((operation, code)) = closed {
                drop(request);
                self.retain(operation, code);
                return;
            }
        }
        if let Err((operation, code)) = self.close_cq() {
            self.retain(operation, code);
            return;
        }
        // Shared CQ destruction, after all QPs, is the cancellation receipt.
        for record in records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(resources) = &mut request.resources {
                resources.connection.retiring().send_queue.quiesce();
                resources.connection.retiring().receive_queue.quiesce();
            }
            if let Some(work) = &mut request.work {
                if work.queue.quiesce().is_err() {
                    drop(request);
                    self.retain("RDMA grant work before native SQ drain", libc::EBUSY);
                    return;
                }
            }
            self.quota.released();
            if let Some(resources) = &mut request.resources {
                if let Some(grant) = &mut resources.grant {
                    let closed = grant.close(&resources.connection.retiring(), self.verbs);
                    if let Err((operation, code)) = closed {
                        drop(request);
                        self.retain(operation, code);
                        return;
                    }
                }
                let closed = {
                    let mut connection = resources.connection.retiring();
                    connection
                        .receive_queue
                        .close(self.verbs)
                        .and_then(|()| connection.close_grant_pd(self.verbs))
                };
                if let Err((operation, code)) = closed {
                    drop(request);
                    self.retain(operation, code);
                    return;
                }
            }
        }
        for record in records {
            let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
            request.failure.get_or_insert(fault);
            if let Err((operation, code)) = Self::unlock(&mut request) {
                drop(request);
                self.retain(operation, code);
                return;
            }
            record.changed.notify_all();
        }
        let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        control.qps.clear_after_drain();
        control.finished = true;
        self.changed.notify_all();
    }

    fn post(&self, record: &Arc<Record>) -> bool {
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

    fn post_receive(&self, record: &Arc<Record>) -> bool {
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

    pub(in crate::transport::rdma) fn tick(
        &self,
        records: &mut Vec<Arc<Record>>,
        cursor: &mut usize,
    ) -> Schedule {
        if let Err(error) = self.asynchronous.check() {
            match error {
                Error::Unquiesced { operation, code } => {
                    self.retain(operation, code);
                    return Schedule::Finished;
                }
                error => self.fail(Fault::from_error(error)),
            }
        }
        let (fault, stopping, cqs) = {
            let mut control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            records.clear();
            control.qps.snapshot(records);
            control.dirty = false;
            (
                control.fault,
                control.stopping,
                [
                    control.cq.as_ref().map(|cq| cq.handles.cq),
                    control.receive_cq.as_ref().map(|cq| cq.handles.cq),
                ],
            )
        };
        if let Some(fault) = fault {
            self.abort(records, fault);
            return Schedule::Finished;
        }
        if stopping {
            if !records.is_empty() {
                self.fail(Fault::Unavailable("RDMA progress stopped"));
                return Schedule::Again;
            }
            if let Err((operation, code)) = self.close_cq() {
                self.retain(operation, code);
                return Schedule::Finished;
            }
            self.control
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .finished = true;
            self.changed.notify_all();
            return Schedule::Finished;
        }
        if self.completion_events.is_some() {
            let control = self.control.lock().unwrap_or_else(|p| p.into_inner());
            let armed = [control.cq.as_ref(), control.receive_cq.as_ref()]
                .into_iter()
                .flatten()
                .try_for_each(|cq| cq.arm(self.verbs));
            drop(control);
            if let Err(error) = armed {
                match error {
                    Error::Unquiesced { operation, code } => {
                        self.retain(operation, code);
                        return Schedule::Finished;
                    }
                    error => self.fail(Fault::from_error(error)),
                }
                return Schedule::Again;
            }
        }
        for record in records.iter() {
            if let Err((operation, code)) = self.finish_request(record) {
                self.retain(operation, code);
                return Schedule::Finished;
            }
        }
        let mut index = 0;
        while index < records.len() {
            match self.close_request(&records[index]) {
                Ok(true) => {
                    records.swap_remove(index);
                }
                Ok(false) => index += 1,
                Err((operation, code)) => {
                    self.retain(operation, code);
                    return Schedule::Finished;
                }
            }
        }
        self.quota_waiting.store(false, Ordering::Release);
        for index in 0..records.len() {
            if self
                .control
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .fault
                .is_some()
            {
                break;
            }
            let record = &records[(index + *cursor) % records.len()];
            self.post(record);
            self.post_receive(record);
        }
        *cursor = cursor.wrapping_add(1);
        let pending_qps = records
            .iter()
            .enumerate()
            .filter(|(index, record)| {
                record.send_queue.outstanding() != 0
                    && !records[..*index]
                        .iter()
                        .any(|other| Arc::ptr_eq(&other.connection, &record.connection))
            })
            .count();
        let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        usage.peak_pending_qps = usage.peak_pending_qps.max(pending_qps);
        let pending = usage.work_requests != 0 || usage.receive_work_requests != 0;
        drop(usage);
        let mut progressed = false;
        if (pending || self.completion_events.is_some())
            && self
                .control
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .fault
                .is_none()
        {
            let completed_at = Instant::now();
            for (receive_cq, cq) in [(false, cqs[0]), (true, cqs[1])] {
                let mut handles = sys::ConnectionHandles {
                    cq: cq.unwrap_or(std::ptr::null_mut()),
                    ..Default::default()
                };
                for _ in 0..64 {
                    let mut completion = sys::Completion::default();
                    let count = unsafe { (self.verbs.poll)(&mut handles, &mut completion) };
                    match count {
                        0 => break,
                        1 => {
                            progressed = true;
                            self.complete(completion, receive_cq, completed_at);
                        }
                        code if code < 0 => {
                            self.fail(Fault::Backend("ibv_poll_cq", code));
                            break;
                        }
                        _ => {
                            self.fail(Fault::Protocol("RDMA poll returned more than one CQE"));
                            break;
                        }
                    }
                    if self
                        .control
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .fault
                        .is_some()
                    {
                        break;
                    }
                }
            }
            let failed = self
                .control
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .fault
                .is_some();
            // This lane's QPs share one NIC. Count the entire pass before
            // sampling bytes against its common completion timestamp.
            if let Some(info) = records
                .first()
                .and_then(|record| record.send_queue.bandwidth.as_ref())
            {
                if failed {
                    info.invalidate();
                } else if progressed {
                    info.sample(completed_at);
                }
            }
        }
        let wait = if self.completion_events.is_some() {
            if progressed {
                return Schedule::Again;
            }
            records
                .iter()
                .filter_map(|record| {
                    let request = record.state.lock().unwrap_or_else(|p| p.into_inner());
                    request
                        .work
                        .as_ref()
                        .filter(|work| !work.queue.finished())
                        .map(|work| work.deadline)
                        .into_iter()
                        .chain(
                            request
                                .receive
                                .as_ref()
                                .filter(|receive| !receive.completed())
                                .map(|receive| receive.deadline),
                        )
                        .min()
                })
                .min()
        } else {
            Some(Instant::now() + Duration::from_micros(50))
        };
        records.clear();
        let control = self.control.lock().unwrap_or_else(|p| p.into_inner());
        if control.dirty || control.fault.is_some() || control.stopping {
            return Schedule::Again;
        }
        drop(control);
        if pending {
            if self.completion_events.is_some() {
                self.usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .cq_event_waits += 1;
            }
            Schedule::Wait(wait)
        } else {
            Schedule::Wait(None)
        }
    }
}
