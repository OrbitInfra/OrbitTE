//! A transport attempt returns to its physical owner before another candidate
//! can access the request. Keep the original registration, permit and deadline.
use super::*;

pub(super) fn retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::Backend { .. }
            | Error::ResourceExhausted
            | Error::Unavailable(_)
            | Error::TransportPolicy { .. }
            | Error::RegionNotFound
    ) || matches!(error, Error::Io(e) if !matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock))
}

impl TransferEngine {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn validate_memory_route(
        &self,
        selection: &SelectionResult,
        local: &RegisteredMemory,
        local_offset: usize,
        length: usize,
        staging: bool,
        deadline: Instant,
        rdma_ready: &mut bool,
    ) -> Result<()> {
        let _ = (local, local_offset, length, deadline, &rdma_ready);
        let transport = selection.transport;
        if transport == Transport::HpTcp && (staging || !crate::transport::hp_tcp::host(local)) {
            return Err(selection.rejected("HP-TCP requires ordinary host memory"));
        }
        if staging && matches!(transport, Transport::Nvlink | Transport::Mnnvl) {
            return Err(Error::Unavailable("remote staging requires TCP or RDMA"));
        }
        #[cfg(not(feature = "cuda"))]
        if staging {
            return Err(Error::Unavailable(
                "remote staging requires --features cuda",
            ));
        }
        if transport == Transport::Rdma {
            #[cfg(feature = "rdma")]
            {
                if selection.device_mask != u64::MAX {
                    self.source
                        .rdma
                        .as_ref()
                        .ok_or_else(|| selection.rejected("RDMA is unavailable"))?
                        .check_policy_paths(
                            (!staging)
                                .then(|| local.location())
                                .transpose()?
                                .map(|r| r.location),
                            selection,
                        )?;
                }
                if !*rdma_ready {
                    self.source
                        .rdma
                        .as_ref()
                        .ok_or(Error::Unavailable("RDMA is not configured"))?
                        .check_ready()
                        .map_err(|error| match error {
                            Error::Unavailable(reason) => selection.rejected(reason),
                            error => error,
                        })?;
                    *rdma_ready = true;
                }
            }
            #[cfg(not(feature = "rdma"))]
            return Err(Error::Unavailable("build with --features rdma"));
        }
        if matches!(transport, Transport::Nvlink | Transport::Mnnvl) {
            #[cfg(feature = "cuda")]
            {
                let owner = self
                    .source
                    .nvlink
                    .as_ref()
                    .ok_or(Error::Unavailable("NVLink is not configured"))?;
                if owner.fabric.is_some() != (transport == Transport::Mnnvl)
                    || (transport == Transport::Mnnvl
                        && !local.0.fabric.get().is_some_and(|r| r.local))
                {
                    return Err(
                        selection.rejected("CUDA transport is unavailable on this registration")
                    );
                }
                local
                    .allocation_range(local_offset, length)?
                    .0
                    .ipc_context(deadline)?;
            }
            #[cfg(not(feature = "cuda"))]
            return Err(Error::Unavailable("build with --features cuda"));
        }
        Ok(())
    }
}

impl TransferEngine {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn memory_attempt(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[PreparedTransfer],
        owners: &[OwnerAdmission],
        owner_deadline: Instant,
        credit: Option<&crate::runtime::receiver_credit::CreditReservation>,
        progress: Option<(&BatchProgress, usize)>,
        posted: &mut bool,
    ) -> std::result::Result<usize, TransferFailure> {
        let request = &requests[0];
        let _ = (credit, progress);
        let single = |result: std::result::Result<(), (Error, RemoteEffect)>| {
            result
                .map(|()| 1)
                .map_err(|(error, remote_effect)| TransferFailure {
                    request_index: 0,
                    affected_requests: 1,
                    error,
                    remote_effect,
                })
        };
        #[cfg(feature = "metrics")]
        if let Some(m) = &owners[0].metrics {
            m.begin(request.selection.transport);
        }
        match request.selection.transport {
            Transport::IoUring | Transport::Gds | Transport::Unspec => single(Err((
                Error::Internal("unresolved transport"),
                RemoteEffect::None,
            ))),
            #[cfg(feature = "cuda")]
            Transport::Tcp | Transport::Rdma if request.remote_staging => {
                single(self.source.proxy.transfer_remote(
                    (operation, owner_deadline),
                    credit,
                    peer,
                    request,
                    &self.source,
                    posted,
                ))
            }
            Transport::Shm => {
                #[cfg(target_os = "linux")]
                {
                    single(
                        (|| {
                            let lease = match &request.shm {
                                Some(lease) => lease.clone(),
                                None => self.shm_lease(peer, request, operation)?,
                            };
                            lease.copy(
                                operation,
                                &request.local,
                                request.local_offset,
                                request.remote_offset,
                                request.length,
                                posted,
                            )
                        })()
                        .map_err(|e| {
                            (
                                e,
                                if *posted && operation == TransferOp::Write {
                                    RemoteEffect::Unknown
                                } else {
                                    RemoteEffect::None
                                },
                            )
                        }),
                    )
                }
                #[cfg(not(target_os = "linux"))]
                {
                    single(Err((
                        Error::Unavailable("SHM requires Linux"),
                        RemoteEffect::None,
                    )))
                }
            }
            Transport::HpTcp => single((|| {
                let hp = self
                    .hp_tcp
                    .as_ref()
                    .ok_or((Error::Unavailable("HP-TCP is disabled"), RemoteEffect::None))?;
                owners[0]
                    .admit_hp(hp, request.length)
                    .map_err(|e| (e, RemoteEffect::None))?;
                hp.submit(operation, peer, request, owner_deadline)
                    .map_err(|e| (e, RemoteEffect::None))?
                    .wait(posted)
            })()),
            Transport::Tcp => single(TcpTransport::transfer(
                operation,
                peer,
                request,
                owner_deadline,
                posted,
                #[cfg(feature = "cuda")]
                &self.source.proxy,
            )),
            Transport::Nvlink | Transport::Mnnvl => {
                #[cfg(feature = "cuda")]
                {
                    match &self.source.nvlink {
                        Some(transport) => transport.transfer(
                            (operation, owner_deadline),
                            (peer, requests),
                            self.source.epoch,
                            posted,
                            |next| {
                                let allowed = progress.is_none_or(|(progress, first_task)| {
                                    progress.begin_task(first_task + next)
                                });
                                #[cfg(feature = "metrics")]
                                if allowed {
                                    if let Some(m) = &owners[next].metrics {
                                        m.dispatched();
                                        m.begin(request.selection.transport);
                                    }
                                }
                                allowed
                            },
                        ),
                        None => single(Err((
                            Error::Unavailable("NVLink is not configured"),
                            RemoteEffect::None,
                        ))),
                    }
                }
                #[cfg(not(feature = "cuda"))]
                {
                    single(Err((
                        Error::Unavailable("build with --features cuda"),
                        RemoteEffect::None,
                    )))
                }
            }
            Transport::Rdma => {
                #[cfg(feature = "rdma")]
                {
                    single(match &self.source.rdma {
                        Some(transport) => transport.transfer(
                            operation,
                            peer,
                            request,
                            self.source.epoch,
                            owner_deadline,
                            posted,
                        ),
                        None => Err((
                            Error::Unavailable("RDMA is not configured"),
                            RemoteEffect::None,
                        )),
                    })
                }
                #[cfg(not(feature = "rdma"))]
                {
                    single(Err((
                        Error::Unavailable("build with --features rdma"),
                        RemoteEffect::None,
                    )))
                }
            }
        }
    }
}

impl TransferEngine {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn retry_memory_group(
        &self,
        op: TransferOp,
        peer: &RemoteSegment,
        group: &[PreparedTransfer],
        owners: &[OwnerAdmission],
        deadline: Instant,
        credit: Option<&crate::runtime::receiver_credit::CreditReservation>,
        progress: Option<(&BatchProgress, usize)>,
        posted: bool,
        failure: TransferFailure,
    ) -> std::result::Result<usize, TransferFailure> {
        // RDMA returns only after close_slices/abort has reaped/flushed its QP
        // and returned its loan. NVLink joins its CUDA completion and cleanup.
        // ProxyManager joins its session and releases the stage registrations.
        // TCP drops its socket and private READ snapshot on this call boundary.
        // A failed physical release is Unquiesced, regardless of original cause.
        if !retryable(&failure.error)
            || failure.remote_effect == RemoteEffect::Unknown
            || (posted && op == TransferOp::Write)
            || Instant::now() >= deadline
        {
            return Err(failure);
        }
        let r = &group[failure.request_index];
        #[cfg(feature = "cuda")]
        let staged = r.remote_staging;
        #[cfg(not(feature = "cuda"))]
        let staged = false;
        if self.config.max_failover_attempts <= r.selection.initial_attempts
            || (!r.selection.has_fallback()
                && !staged
                && !matches!(failure.error, Error::RegionNotFound))
        {
            return Err(failure);
        }
        let end = failure.request_index + failure.affected_requests;
        for (i, request) in group
            .iter()
            .enumerate()
            .take(end)
            .skip(failure.request_index)
        {
            let local = request.local.base_address() + request.local_offset as u64;
            let remote = request.remote.address + request.remote_offset as u64;
            if (posted || staged)
                && local.abs_diff(remote) < request.length as u64
                && self
                    .source
                    .metadata
                    .boot
                    .zip(peer.metadata.boot)
                    .is_none_or(|(a, b)| a == b)
            {
                return Err(TransferFailure {
                    request_index: i,
                    affected_requests: end - i,
                    error: failure.error,
                    remote_effect: failure.remote_effect,
                });
            }
            let progress = progress.map(|(p, base)| (p, base + i));
            if let Err((error, effect)) = self.retry_memory_owner(
                op,
                peer,
                request,
                &owners[i],
                deadline,
                credit,
                progress,
                posted,
                &failure.error,
            ) {
                return Err(TransferFailure {
                    request_index: i,
                    affected_requests: end - i,
                    error,
                    remote_effect: effect,
                });
            }
        }
        Ok(end)
    }

    #[allow(clippy::too_many_arguments)]
    fn retry_memory_owner(
        &self,
        op: TransferOp,
        peer: &RemoteSegment,
        original: &PreparedTransfer,
        owner: &OwnerAdmission,
        deadline: Instant,
        credit: Option<&crate::runtime::receiver_credit::CreditReservation>,
        progress: Option<(&BatchProgress, usize)>,
        mut posted: bool,
        first_error: &Error,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut count = original.selection.initial_attempts;
        let mut request = original.clone();
        let mut refreshed = None;
        let mut error = first_error.for_report();
        while count < self.config.max_failover_attempts
            && Instant::now() < deadline
            && retryable(&error)
        {
            count += 1;
            let refresh = matches!(error, Error::RegionNotFound) && refreshed.is_none();
            #[cfg(feature = "cuda")]
            let staging = request.remote_staging;
            #[cfg(not(feature = "cuda"))]
            let staging = false;
            let next = if refresh || staging {
                request.selection.clone()
            } else {
                let Some(route) = original.selection.candidate(count) else {
                    break;
                };
                route
            };
            if posted
                && progress
                    .is_some_and(|(p, i)| !p.retry_on_progress(i, error.for_report(), deadline))
            {
                if progress.is_some_and(|(p, i)| p.cancel_requested(i)) {
                    error = Error::Cancelled;
                }
                break;
            }
            #[cfg(feature = "cuda")]
            if !refresh {
                request.remote_staging = false;
            }
            if refresh {
                let snapshot = control_plane::open_segment(peer.endpoint, peer.epoch, deadline)
                    .map_err(|e| (e, RemoteEffect::None))?;
                let address = request.remote.address + request.remote_offset as u64;
                let (current, offset) = snapshot
                    .resolve_address(address, request.length)
                    .map_err(|e| (e, RemoteEffect::None))?;
                if current != request.remote || offset != request.remote_offset {
                    return Err((Error::RegionNotFound, RemoteEffect::None));
                }
                refreshed = Some(snapshot);
            }
            request.selection = next;
            if let Some((p, i)) = progress {
                let running = if request.selection.transport == Transport::HpTcp {
                    request.hp_cancel.clone().map_or(
                        crate::batch::RunningCancel::Unsupported,
                        crate::batch::RunningCancel::HpTcp,
                    )
                } else {
                    crate::batch::RunningCancel::Unsupported
                };
                #[cfg(feature = "rdma")]
                let running = if request.selection.transport == Transport::Rdma {
                    request
                        .rdma_cancel
                        .clone()
                        .map_or(running, crate::batch::RunningCancel::Rdma)
                } else {
                    running
                };
                if !p.start_retry(i, request.selection.transport, count, running) {
                    return Err((Error::Cancelled, RemoteEffect::None));
                }
            }
            log::debug!(
                "retrying physical transfer on {:?}, attempt {}",
                request.selection.transport,
                count
            );
            #[cfg(feature = "metrics")]
            if let Some(m) = &owner.metrics {
                m.retry(request.selection.transport);
                m.begin(request.selection.transport);
            }
            owner.reroute(&request.selection, {
                #[cfg(feature = "cuda")]
                {
                    request.remote_staging
                }
                #[cfg(not(feature = "cuda"))]
                {
                    false
                }
            });
            posted = false;
            let result = self
                .validate_memory_route(
                    &request.selection,
                    &request.local,
                    request.local_offset,
                    request.length,
                    #[cfg(feature = "cuda")]
                    request.remote_staging,
                    #[cfg(not(feature = "cuda"))]
                    false,
                    deadline,
                    &mut false,
                )
                .map_err(|error| TransferFailure {
                    request_index: 0,
                    affected_requests: 1,
                    error,
                    remote_effect: RemoteEffect::None,
                })
                .and_then(|()| {
                    self.memory_attempt(
                        op,
                        refreshed.as_ref().unwrap_or(peer),
                        std::slice::from_ref(&request),
                        std::slice::from_ref(owner),
                        deadline,
                        credit,
                        progress,
                        &mut posted,
                    )
                });
            #[cfg(feature = "metrics")]
            if let Some(m) = &owner.metrics {
                m.finish(result.as_ref().err().map(|f| &f.error), posted);
            }
            match result {
                Ok(_) => return Ok(()),
                Err(f)
                    if f.remote_effect == RemoteEffect::Unknown
                        || (op == TransferOp::Write && posted) =>
                {
                    return Err((f.error, RemoteEffect::Unknown));
                }
                Err(f) => error = f.error,
            }
        }
        Err((error, RemoteEffect::None))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/engine/failover.rs"]
mod tests;
