//! Fence physical QPs once; observe native retirement without joining the worker.

use super::*;

fn pending(deadline: Instant) -> Result<bool> {
    remaining(deadline).map_err(|_| Error::Unquiesced {
        operation: "RDMA close deadline before quiescence",
        code: libc::ETIMEDOUT,
    })?;
    Ok(false)
}

impl CqLane {
    pub(in crate::transport::rdma) fn close_record(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<()> {
        while !self.poll_close_record(record, claim, deadline)? {
            thread::park_timeout(Duration::from_micros(100));
        }
        Ok(())
    }

    pub(in crate::transport::rdma) fn poll_close_record(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<bool> {
        // Native cleanup holds this lock. Observing it must not join the provider.
        let Some(mut request) = record.try_lock()? else {
            return pending(deadline);
        };
        if request.claim != claim {
            return Ok(true);
        }
        if request.phase == Phase::Closed {
            request.check()?;
            return Ok(true);
        }
        let mut control = match self.shared.control.try_lock() {
            Ok(control) => control,
            Err(std::sync::TryLockError::WouldBlock) => return pending(deadline),
            Err(_) => {
                return Err(Error::Unquiesced {
                    operation: "RDMA endpoint store poisoned",
                    code: libc::EIO,
                });
            }
        };
        if request.phase == Phase::Retained {
            control.stopping = true;
            control.dirty = true;
            self.shared.wake();
            if !control.finished || !control.detached {
                return pending(deadline);
            }
            let (operation, code) = control
                .retained
                .unwrap_or(("RDMA retained progress lane", libc::EIO));
            return Err(Error::Unquiesced { operation, code });
        }
        if request.close_requested {
            return pending(deadline);
        }
        if request.phase == Phase::Init
            && request
                .resources
                .as_ref()
                .is_some_and(|r| r.grant.is_none())
        {
            if let Some(idle) = control.qps.discard(record) {
                request.resources = None;
                request.phase = Phase::Closed;
                self.shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .idle_endpoints += usize::from(idle);
                return Ok(true);
            }
        }
        let affiliated = control.qps.affiliates(record);
        if affiliated.is_empty()
            && request.phase == Phase::Ready
            && request
                .resources
                .as_ref()
                .is_some_and(|r| r.grant.is_none())
            && control.fault.is_none()
        {
            return Ok(true);
        }
        // Publish all affiliated close actions together. A busy sibling yields
        // before any detach, so a retry cannot lose a sibling's close request.
        let mut siblings = Vec::with_capacity(affiliated.len());
        for owner in &affiliated {
            if !Arc::ptr_eq(owner, record) {
                let Some(state) = owner.try_lock()? else {
                    return pending(deadline);
                };
                siblings.push((owner, state));
            }
        }
        request.close_requested = true;
        for owner in &affiliated {
            if control.qps.detach(owner) {
                #[cfg(test)]
                super::tests::before_idle_debit(&self.shared);
                self.shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .idle_endpoints -= 1;
            }
        }
        for (owner, mut state) in siblings {
            if !matches!(state.phase, Phase::Closed | Phase::Retained) {
                state.close_requested = true;
                if state.resources.as_ref().is_some_and(|r| r.grant.is_some()) {
                    state
                        .failure
                        .get_or_insert(Fault::Unavailable("RDMA shared QP retired"));
                }
                owner.actions.fetch_or(CLOSE, Ordering::Release);
            }
        }
        record.actions.fetch_or(CLOSE, Ordering::Release);
        control.dirty = true;
        self.shared.wake();
        pending(deadline)
    }
}
