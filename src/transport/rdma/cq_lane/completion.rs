//! CQE identity checks and dispatch to the exact grant sharing a physical QP.

use super::*;

impl ProgressState {
    pub(super) fn complete(&self, completion: sys::Completion, receive_cq: bool) {
        let Some(tag) = completion
            .id
            .checked_sub(1)
            .map(|id| id / super::super::WR_ID_STRIDE)
        else {
            self.fail(Fault::Protocol("RDMA completion identity"));
            return;
        };
        // Never hold the store lock while locking a grant or physical QP.
        // Finished grant records may disappear while their donated RQ slot lives.
        if receive_cq {
            let mut records = Vec::new();
            self.control
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .qps
                .snapshot(&mut records);
            let result = self.complete_receive(&records, completion);
            if let Err(error) = result {
                self.fail(Fault::from_error(error));
            }
            self.quota.released();
            return;
        }
        let record = self
            .control
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .qps
            .work_owner(tag, false);
        let Some(record) = record else {
            self.fail(Fault::Protocol("RDMA unknown completion owner"));
            return;
        };
        let mut request = record.state.lock().unwrap_or_else(|p| p.into_inner());
        let result = if completion.qpn != request.qpn {
            Err(Error::Protocol("RDMA completion QP identity"))
        } else if let Some(work) = &mut request.work {
            work.queue.complete(completion, work.operation)
        } else {
            Err(Error::Protocol("RDMA completion before submission"))
        };
        if result.is_ok() && !receive_cq {
            let operation = request.work.as_ref().map(|w| w.operation);
            if request.work.as_ref().is_some_and(|w| w.queue.finished()) {
                request.phase = Phase::Connected;
            }
            if operation == Some(slice::Operation::Bind) {
                if let Some(grant) = request.resources.as_mut().and_then(|r| r.grant.as_mut()) {
                    grant.window_bound = true;
                }
            }
            #[cfg(test)]
            if operation == Some(slice::Operation::Invalidate) {
                if let Some(grant) = request.resources.as_mut().and_then(|r| r.grant.as_mut()) {
                    grant.window_bound = false;
                }
            }
        }
        if let Err(error) = result {
            let fault = Fault::from_error(error);
            request.failure = Some(fault);
            drop(request);
            self.fail(fault);
        } else {
            if request
                .work
                .as_ref()
                .is_some_and(|work| work.queue.finished())
            {
                record.actions.fetch_and(!WORK, Ordering::Release);
            }
            drop(request);
        }
        self.quota.released();
        // The handler owns a strong record until all of its accesses have ended.
        record.changed.notify_all();
    }

    fn complete_receive(&self, records: &[Arc<Record>], completion: sys::Completion) -> Result<()> {
        let donor = records
            .iter()
            .find(|record| {
                record
                    .connection
                    .retiring()
                    .receive_queue
                    .owns(completion.id)
            })
            .ok_or(Error::Protocol("RDMA unknown receive owner"))?;
        let receipt = {
            let mut connection = donor.connection.lock()?;
            if completion.qpn != connection.local.qpn {
                return Err(Error::Protocol("RDMA completion QP identity"));
            }
            connection.receive_queue.complete(completion)?
        };
        for record in records {
            if !Arc::ptr_eq(&record.connection, &donor.connection) {
                continue;
            }
            let mut request = record.lock()?;
            if !request
                .receive
                .as_ref()
                .is_some_and(|work| Arc::ptr_eq(work, &receipt))
            {
                continue;
            }
            let grant = request
                .resources
                .as_mut()
                .ok_or(Error::Internal("missing RDMA receive owner"))?
                .grant_mut()?;
            if (!grant.window_bound && !grant.handles.remote_mr) || grant.rkey != receipt.rkey {
                return Err(Error::Protocol("RDMA receive changed grant authority"));
            }
            grant.window_bound = false;
            // MW authority is already invalidated. An isolated GPU MR is revoked
            // by QP/MR/PD teardown before the source ACK and loan release.
            receipt.mark_completed();
            let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            usage.completed_receives += 1;
            usage.completed_window_invalidations += u64::from(!grant.handles.remote_mr);
            drop(usage);
            drop(request);
            record.changed.notify_all();
            return Ok(());
        }
        Err(Error::Protocol("RDMA receive grant owner mismatch"))
    }
}
