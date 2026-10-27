//! Grant work admission, message buffers and completion waits on a CQ lane.

use super::*;

impl CqLane {
    pub(in crate::transport::rdma) fn submit(
        &self,
        record: &Arc<Record>,
        claim: u64,
        remote: Descriptor,
        operation: TransferOp,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected || request.message_sent {
                return Err(Error::Internal("RDMA QP is not ready"));
            }
            let resources = request
                .resources
                .as_ref()
                .ok_or(Error::Internal("missing RDMA endpoint"))?;
            if remote
                .address
                .checked_add(resources.grant()?.buffer.len() as u64)
                .is_none()
            {
                return Err(Error::InvalidRange);
            }
            let cap = if operation == TransferOp::Write {
                32
            } else {
                resources.grant()?.max_read_slices
            };
            let queue = slice::SendWork::new(
                resources.grant()?.buffer.len(),
                self.shared.config.block_size,
                cap,
                Arc::clone(&record.send_queue),
                self.shared.next_work_namespace()? * crate::transport::rdma::WR_ID_STRIDE + 1,
            )?;
            record
                .send_namespace
                .store(queue.namespace(), Ordering::Release);
            request.mixed_operations |= request
                .payload_operation
                .is_some_and(|previous| previous != operation);
            request.payload_operation = Some(operation);
            request.work = Some(Work {
                queue,
                operation: operation.into(),
                remote,
                deadline,
            });
            request.phase = Phase::Queued;
        }
        record.actions.fetch_or(WORK, Ordering::Release);
        self.shared.signal();
        Ok(())
    }

    pub(in crate::transport::rdma) fn window(
        &self,
        record: &Arc<Record>,
        claim: u64,
        operation: slice::Operation,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected {
                return Err(Error::Internal("MW QP is not ready"));
            }
            let resources = request
                .resources
                .as_mut()
                .ok_or(Error::Internal("missing MW owner"))?;
            if resources.grant_mut()?.handles.mw.is_null() {
                return Err(Error::Unavailable("source MW is unavailable"));
            }
            let slot = match operation {
                slice::Operation::Bind if !resources.grant_mut()?.window_used => {
                    resources.grant_mut()?.window_used = true;
                    65
                }
                #[cfg(test)]
                slice::Operation::Invalidate if resources.grant_mut()?.window_bound => 66,
                _ => return Err(Error::Internal("MW authorization state")),
            };
            let namespace = self.shared.next_work_namespace()?;
            record.send_namespace.store(namespace, Ordering::Release);
            let remote = Descriptor {
                address: resources.grant_mut()?.buffer.address().as_ptr() as u64,
                rkey: resources.grant_mut()?.handles.window_key,
                ..Default::default()
            };
            let send_queue = Arc::clone(&record.send_queue);
            request.work = Some(Work {
                queue: slice::SendWork::control(
                    send_queue,
                    namespace * crate::transport::rdma::WR_ID_STRIDE + slot,
                )?,
                operation,
                remote,
                deadline,
            });
            request.phase = Phase::Queued;
        }
        self.wait(record, claim, deadline)
    }

    fn prepare_message(&self, request: &mut Request) -> Result<()> {
        let resources = request
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA grant"))?;
        let grant = resources.grant_mut()?;
        if grant.message.is_some() {
            return Err(Error::Internal("RDMA message already prepared"));
        }
        grant.message = Some(MessageBuffer::new(
            Arc::clone(&self.shared.usage),
            self.shared.config.max_grants + self.shared.config.max_cqes,
        )?);
        let message = grant
            .message
            .as_mut()
            .ok_or(Error::Internal("missing RDMA message"))?;
        let registered = grant.device.register_message(
            &mut message.handles,
            message.memory.address,
            message.memory.length,
            std::ptr::null_mut(),
        );
        message.registered();
        registered
    }

    pub(in crate::transport::rdma) fn post_receive(
        &self,
        record: &Arc<Record>,
        claim: u64,
        expected: [u8; COMPLETION_MESSAGE_SIZE],
        credits: crate::transport::rdma::receive::ReceiveCredits,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected
                || request.receive.is_some()
                || !request.resources.as_ref().is_some_and(|r| {
                    r.grant.as_ref().is_some_and(|g| {
                        g.window_bound || (g.handles.remote_mr && !g.handles.mr.is_null())
                    })
                })
            {
                return Err(Error::Internal("RDMA receive before source grant"));
            }
            let namespace = self.shared.next_work_namespace()?;
            let resources = request
                .resources
                .as_mut()
                .ok_or(Error::Internal("missing RDMA grant"))?;
            let receive = ReceiveWork::new(
                namespace * crate::transport::rdma::WR_ID_STRIDE + 1,
                expected,
                resources.grant()?.rkey,
                deadline,
            );
            let mut connection = resources.connection.lock()?;
            let device = Arc::clone(&connection.device);
            let private_pd = connection.handles.private_pd;
            connection.receive_queue.prepare(
                Arc::clone(&receive),
                credits,
                &device,
                self.shared.config.max_grants + self.shared.config.max_cqes,
                private_pd,
            )?;
            drop(connection);
            request.receive = Some(receive);
            record.receive_namespace.store(namespace, Ordering::Release);
        }
        record.actions.fetch_or(RECEIVE, Ordering::Release);
        self.shared.signal();
        self.wait_receive(record, claim, deadline, false)
    }

    pub(in crate::transport::rdma) fn receive_completion(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<()> {
        self.wait_receive(record, claim, deadline, true)
    }

    fn wait_receive(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
        completed: bool,
    ) -> Result<()> {
        let mut request = record.lock()?;
        loop {
            request.check_claim(claim)?;
            let receive = request
                .receive
                .as_ref()
                .ok_or(Error::Internal("missing RDMA receive"))?;
            if if completed {
                receive.completed()
            } else {
                receive.posted()
            } {
                return Ok(());
            }
            let duration = match remaining(deadline) {
                Ok(duration) => duration,
                Err(error) => {
                    let pending = record.connection.lock()?.receive_queue.outstanding() != 0;
                    request.failure = Some(Fault::Timeout);
                    drop(request);
                    if pending {
                        self.shared.fail(Fault::Timeout);
                    } else {
                        self.shared.signal();
                    }
                    return Err(error);
                }
            };
            request = record
                .changed
                .wait_timeout(request, duration)
                .map_err(|_| Error::Unquiesced {
                    operation: "RDMA receive wait poisoned",
                    code: libc::EIO,
                })?
                .0;
        }
    }

    pub(in crate::transport::rdma) fn send_completion(
        &self,
        record: &Arc<Record>,
        claim: u64,
        binding: [u8; COMPLETION_MESSAGE_SIZE],
        rkey: u32,
        deadline: Instant,
    ) -> Result<()> {
        remaining(deadline)?;
        self.shared.asynchronous.check()?;
        record.require_live()?;
        {
            let mut request = record.lock()?;
            record.require_live()?;
            request.check_claim(claim)?;
            if request.phase != Phase::Connected
                || request.message_sent
                || !request.work.as_ref().is_some_and(|w| {
                    matches!(
                        w.operation,
                        slice::Operation::Read | slice::Operation::Write
                    ) && w.queue.finished()
                })
            {
                return Err(Error::Internal("RDMA completion before payload CQ drain"));
            }
            if request.work.as_ref().is_none_or(|w| w.remote.rkey != rkey) {
                return Err(Error::Protocol(
                    "completion invalidation changed its payload key",
                ));
            }
            let namespace = self.shared.next_work_namespace()?;
            self.prepare_message(&mut request)?;
            let send_queue = Arc::clone(&record.send_queue);
            let message = request
                .resources
                .as_mut()
                .and_then(|r| r.grant.as_mut())
                .and_then(|g| g.message.as_mut())
                .ok_or(Error::Internal("missing RDMA message"))?;
            message.memory.bytes_mut().copy_from_slice(&binding);
            request.message_sent = true;
            request.work = Some(Work {
                queue: slice::SendWork::control(
                    send_queue,
                    namespace * crate::transport::rdma::WR_ID_STRIDE + 1,
                )?,
                operation: slice::Operation::Send,
                remote: Descriptor {
                    rkey,
                    ..Descriptor::default()
                },
                deadline,
            });
            record.send_namespace.store(namespace, Ordering::Release);
            request.phase = Phase::Queued;
        }
        self.wait(record, claim, deadline)
    }

    pub(in crate::transport::rdma) fn wait(
        &self,
        record: &Arc<Record>,
        claim: u64,
        deadline: Instant,
    ) -> Result<()> {
        record.actions.fetch_or(WORK, Ordering::Release);
        self.shared.signal();
        let mut request = record.lock()?;
        loop {
            request.check_claim(claim)?;
            if request
                .work
                .as_ref()
                .is_some_and(|work| work.queue.finished())
            {
                return Ok(());
            }
            let duration = match remaining(deadline) {
                Ok(duration) => duration,
                Err(error) => {
                    let pending = request
                        .work
                        .as_ref()
                        .is_some_and(|work| work.queue.pending());
                    request.failure = Some(Fault::Timeout);
                    drop(request);
                    if pending {
                        self.shared.fail(Fault::Timeout);
                    } else {
                        self.shared.signal();
                    }
                    return Err(error);
                }
            };
            request = record
                .changed
                .wait_timeout(request, duration)
                .map_err(|_| Error::Unquiesced {
                    operation: "RDMA progress wait poisoned",
                    code: libc::EIO,
                })?
                .0;
        }
    }
}
