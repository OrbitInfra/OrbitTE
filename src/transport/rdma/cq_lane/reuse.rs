//! Grant-drain barriers, completed-record reuse and live physical-QP sharing.

use super::*;

impl CqLane {
    pub(in crate::transport::rdma) fn finish_sent(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: &[u8; COMPLETION_MESSAGE_SIZE],
        deadline: Instant,
    ) -> Result<()> {
        self.finish(record, claim, binding, None, deadline)
    }

    pub(in crate::transport::rdma) fn finish_received(
        &self,
        record: &Arc<Record>,
        claim: u64,
        done: &BoundDone,
        deadline: Instant,
    ) -> Result<()> {
        self.finish(record, claim, done.binding(), Some(done), deadline)
    }

    fn finish(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: &[u8; COMPLETION_MESSAGE_SIZE],
        done: Option<&BoundDone>,
        deadline: Instant,
    ) -> Result<()> {
        self.begin_finish(record, claim, binding, done, deadline)?;
        let mut request = record.lock()?;
        loop {
            request.check_claim(claim)?;
            if Self::grant_finished(&request)? {
                return Ok(());
            }
            let duration = remaining(deadline).map_err(|_| Error::Unquiesced {
                operation: "RDMA grant cleanup deadline",
                code: libc::ETIMEDOUT,
            })?;
            request = record
                .changed
                .wait_timeout(request, duration)
                .map_err(|_| Error::Unquiesced {
                    operation: "RDMA grant wait poisoned",
                    code: libc::EIO,
                })?
                .0;
        }
    }

    #[cfg(feature = "cuda")]
    pub(in crate::transport::rdma) fn begin_finish_sent(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: &[u8; COMPLETION_MESSAGE_SIZE],
        deadline: Instant,
    ) -> Result<()> {
        self.begin_finish(record, claim, binding, None, deadline)
    }

    #[cfg(feature = "cuda")]
    pub(in crate::transport::rdma) fn query_finish(
        &self,
        record: &Arc<Record>,
        claim: u64,
    ) -> Result<bool> {
        record.require_live()?;
        let request = record.lock()?;
        request.check_claim(claim)?;
        Self::grant_finished(&request)
    }

    fn grant_finished(request: &Request) -> Result<bool> {
        if request.phase != Phase::Drained {
            return Err(Error::Internal("RDMA grant cleanup query before drain"));
        }
        Ok(request.finish.is_none()
            && request.work.is_none()
            && request
                .resources
                .as_ref()
                .is_some_and(|r| r.grant.is_none()))
    }

    fn begin_finish(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: &[u8; COMPLETION_MESSAGE_SIZE],
        done: Option<&BoundDone>,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        record.require_live()?;
        let mut request = record.lock()?;
        request.check_claim(claim)?;
        if request.phase != Phase::Connected || request.finish.is_some() {
            return Err(Error::Internal("RDMA grant cannot finish"));
        }
        let resources = request
            .resources
            .as_ref()
            .ok_or(Error::Internal("missing RDMA connection"))?;
        let grant = resources.grant()?;
        if !matches!(grant.buffer, Buffer::Direct { .. })
            || binding[COMPLETION_MESSAGE_SIZE - 1] != 1
        {
            return Err(Error::Protocol(
                "only negotiated host Direct grants can retain a QP",
            ));
        }
        let work = request
            .work
            .as_ref()
            .ok_or(Error::Internal("missing RDMA completion work"))?;
        if !work.queue.finished() || work.queue.pending() || grant.window_bound {
            return Err(Error::Internal("RDMA operation lacks completion drain"));
        }
        let connection = resources
            .connection
            .lock()?
            .incarnation
            .ok_or(Error::Internal("missing connection incarnation"))?;
        let identity_offset = crate::runtime::control_plane::REQUEST_SIZE
            + if grant.permission == 0 { 40 } else { 0 };
        let native_identity_matches =
            binding[identity_offset + 16..identity_offset + 40] == connection.encode();
        let length_matches =
            crate::runtime::control_plane::u64_at(binding, 44) == grant.buffer.len() as u64;
        let generation_matches = grant.permission == 0
            || crate::runtime::control_plane::u64_at(binding, COMPLETION_MESSAGE_SIZE - 9)
                == grant.generation;
        let operation_matches = match grant.permission {
            0 => matches!(binding[10], 4 | 5),
            1 => binding[10] == 4,
            2 => binding[10] == 5,
            _ => false,
        };
        if !native_identity_matches || !length_matches || !generation_matches || !operation_matches
        {
            return Err(Error::Protocol(
                "RDMA operation receipt changed native grant identity",
            ));
        }
        let valid = if grant.permission == 0 {
            done.is_none()
                && request.message_sent
                && !request.mixed_operations
                && request
                    .payload_operation
                    .is_some_and(|op| binding[10] == if op == TransferOp::Read { 4 } else { 5 })
                && work.operation == slice::Operation::Send
                && request.receive.is_none()
                && grant
                    .message
                    .as_ref()
                    .is_some_and(|m| m.memory.bytes() == binding)
        } else {
            done.is_some()
                && !request.message_sent
                && work.operation == slice::Operation::Bind
                && request.receive.as_ref().is_some_and(|r| {
                    r.completed() && r.expected == *binding && r.rkey == grant.rkey
                })
        };
        if !valid {
            return Err(Error::Protocol("RDMA grant lacks sealed operation receipt"));
        }
        request.finish = Some(GrantDrain {
            connection,
            generation: grant.generation,
            permission: grant.permission,
        });
        request.phase = Phase::Drained;
        record.actions.fetch_or(FINISH, Ordering::Release);
        drop(request);
        self.shared.signal();
        Ok(())
    }

    pub(in crate::transport::rdma) fn claim(
        self: &Arc<Self>,
        matches: impl Fn(&Peer) -> bool,
        deadline: Instant,
    ) -> Result<Option<RdmaGrant>> {
        remaining(deadline)?;
        let claimed = {
            let mut control = self
                .shared
                .control
                .lock()
                .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
            self.shared.asynchronous.check()?;
            if let Some(fault) = control.fault {
                return Err(fault.error());
            }
            if !control.accepting {
                return Err(Error::Unavailable("RDMA claims are fenced"));
            }
            let claimed = control.qps.claim(matches);
            if claimed.as_ref().is_some_and(|(_, _, was_idle)| *was_idle) {
                self.shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .idle_endpoints -= 1;
            }
            claimed
        };
        let Some((record, peer, _)) = claimed else {
            return Ok(None);
        };
        let (local, incarnation, claim) = {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check()?;
            if request.phase != Phase::Ready || request.close_requested {
                let old = request.claim;
                drop(request);
                self.close_record(&record, old, deadline)?;
                return Err(Error::Unavailable("claimed RDMA QP is retiring"));
            }
            let claim = match request.claim.checked_add(1) {
                Some(claim) => claim,
                None => {
                    let old = request.claim;
                    drop(request);
                    self.close_record(&record, old, deadline)?;
                    return Err(Error::ResourceExhausted);
                }
            };
            let resources = request
                .resources
                .as_ref()
                .ok_or(Error::Internal("missing idle connection"))?;
            if resources.grant.is_some() {
                return Err(Error::Internal("idle connection retains a grant"));
            }
            let connection = resources.connection.lock()?;
            let local = connection.local;
            let incarnation = connection.incarnation;
            drop(connection);
            request.claim = claim;
            request.phase = Phase::Init;
            (local, incarnation, claim)
        };
        self.shared
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reused_connections += 1;
        Ok(Some(RdmaGrant {
            record,
            progress: Arc::clone(self),
            local,
            incarnation,
            config: self.shared.config.clone(),
            deadline,
            output: None,
            claim,
            released: false,
            cached_peer: Some(peer),
            endpoint: None,
            construction: None,
        }))
    }

    pub(in crate::transport::rdma) fn share(
        self: &Arc<Self>,
        record: &Arc<Record>,
        peer: Peer,
        deadline: Instant,
    ) -> Result<RdmaGrant> {
        remaining(deadline)?;
        let (local, incarnation) = {
            let connection = record.connection.lock()?;
            record.connection.require_live()?;
            if connection.closed
                || connection.incarnation != Some(peer.local)
                || connection.remote.is_none()
            {
                return Err(Error::Unavailable("RDMA shared connection is not ready"));
            }
            (connection.local, connection.incarnation)
        };
        let record = self.register(Box::new(Resources {
            connection: Arc::clone(&record.connection),
            grant: None,
        }))?;
        self.shared
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reused_connections += 1;
        Ok(RdmaGrant {
            record,
            progress: Arc::clone(self),
            local,
            incarnation,
            config: self.shared.config.clone(),
            deadline,
            output: None,
            claim: 1,
            released: false,
            cached_peer: Some(peer),
            endpoint: None,
            construction: None,
        })
    }

    pub(in crate::transport::rdma) fn cache(
        &self,
        record: &Arc<Record>,
        claim: u64,
        peer: Peer,
    ) -> Result<bool> {
        let mut request = record.lock()?;
        request.check_claim(claim)?;
        record.require_live()?;
        if request.phase != Phase::Drained
            || request.close_requested
            || request.finish.is_some()
            || request.resources.as_ref().is_none_or(|r| r.grant.is_some())
            || request.work.is_some()
            || request.receive.is_some()
            || record.actions.load(Ordering::Acquire) != 0
        {
            return Err(Error::Internal("RDMA READY retains grant state"));
        }
        let mut control = self
            .shared
            .control
            .lock()
            .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
        if let Some(fault) = control.fault {
            return Err(fault.error());
        }
        if !control.accepting {
            return Ok(false);
        }
        let inserted = control.qps.ready(record, peer, claim)?;
        request.phase = Phase::Ready;
        self.shared
            .usage
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .idle_endpoints += usize::from(inserted);
        Ok(true)
    }

    pub(in crate::transport::rdma) fn evict_idle(
        &self,
        matches: impl Fn(&Peer) -> bool,
        deadline: Instant,
    ) -> Result<bool> {
        let record = {
            let mut control = self
                .shared
                .control
                .lock()
                .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
            if control.fault.is_some() {
                return Ok(false);
            }
            let record = control.qps.evict(matches);
            if record.is_some() {
                let mut usage = self.shared.usage.lock().unwrap_or_else(|p| p.into_inner());
                usage.idle_endpoints -= 1;
                usage.evicted_connections += 1;
            }
            record
        };
        let Some(record) = record else {
            return Ok(false);
        };
        let claim = record.lock()?.claim;
        match self.close_record(&record, claim, deadline) {
            Err(error @ Error::Unquiesced { .. }) => return Err(error),
            Err(error) => {
                // Eviction releases capacity after the actual barrier, even if
                // the lane records a logical fault during native retirement.
                if record.lock()?.phase != Phase::Closed {
                    return Err(error);
                }
            }
            Ok(()) => {}
        }
        Ok(true)
    }

    pub(in crate::transport::rdma) fn retire_idle(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<()> {
        let retired = {
            let mut control = self
                .shared
                .control
                .lock()
                .map_err(|_| Error::Internal("RDMA endpoint store poisoned"))?;
            let retired = control.qps.retire_idle(record, claim);
            if retired {
                #[cfg(test)]
                tests::before_idle_debit(&self.shared);
                self.shared
                    .usage
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .idle_endpoints -= 1;
            }
            retired
        };
        if retired {
            self.close_record(record, claim, deadline)?;
        }
        Ok(())
    }
}
