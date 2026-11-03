use super::*;

/// Publish independent HP-TCP owners together. Keep queue/credit turns, deadlines
/// and overlapping memory operations in the original submission order.
impl TransferEngine {
    pub(super) fn prepare_hp_charge(
        &self,
        r: &mut PreparedTransfer,
        deadline: Instant,
    ) -> Result<Option<crate::transport::hp_tcp::Permit>> {
        if r.selection.transport != Transport::HpTcp {
            return Ok(None);
        }
        let hp = self
            .hp_tcp
            .as_ref()
            .ok_or(Error::Unavailable("HP-TCP is disabled"))?;
        let mut error = match hp.reserve(r.length) {
            Ok(permit) => return Ok(Some(permit)),
            Err(e) => e,
        };
        let mut i = r.selection.initial_attempts;
        while i < self.config.max_failover_attempts && failover::retryable(&error) {
            i += 1;
            let Some(mut next) = r.selection.candidate(i) else {
                break;
            };
            match self.validate_memory_route(
                &next,
                &r.local,
                r.local_offset,
                r.length,
                false,
                deadline,
                &mut false,
            ) {
                Ok(()) => {
                    next.initial_attempts = i;
                    r.selection = next;
                    return Ok(None);
                }
                Err(e) => error = e,
            }
        }
        Err(error)
    }

    pub(super) fn hp_group_len(
        &self,
        sub: &PreparedSubmission,
        requests: &[PreparedTransfer],
        first: usize,
    ) -> usize {
        if requests[0].selection.transport != Transport::HpTcp {
            return 0;
        }
        let limit = sub
            .native_group(first)
            .min(self.config.hp_tcp.connections_per_peer);
        let mut ranges = Vec::new();
        for r in requests.iter().take(limit) {
            if r.selection.transport != Transport::HpTcp {
                break;
            }
            let local = r.local.base_address() + r.local_offset as u64;
            let remote = r.remote.base_address() + r.remote_offset as u64;
            let here = [
                (local, local + r.length as u64),
                (remote, remote + r.length as u64),
            ];
            if here
                .iter()
                .any(|&(a, b)| ranges.iter().any(|&(c, d)| a < d && c < b))
            {
                break;
            }
            ranges.extend(here);
        }
        ranges.len() / 2
    }

    pub(super) fn hp_group(
        &self,
        sub: &PreparedSubmission,
        peer: &RemoteSegment,
        group: &[PreparedTransfer],
        first: usize,
        progress: Option<(&BatchProgress, usize)>,
    ) -> BatchReport {
        let op = sub.operation;
        let mut pending = Vec::with_capacity(group.len());
        for (i, r) in group.iter().enumerate() {
            let owner = &sub.owners[first + i];
            let receipt = owner
                .start(|| progress.is_some_and(|(p, base)| p.cancel_requested(base + first + i)))
                .and_then(|()| {
                    if progress.is_some_and(|(p, base)| !p.begin_task(base + first + i)) {
                        return Err(Error::Cancelled);
                    }
                    #[cfg(feature = "metrics")]
                    if let Some(m) = &owner.metrics {
                        m.dispatched();
                        m.begin(Transport::HpTcp);
                    }
                    self.hp_tcp
                        .as_ref()
                        .ok_or(Error::Unavailable("HP-TCP is disabled"))?
                        .submit(op, peer, r, owner.deadline)
                });
            pending.push(receipt);
        }
        let mut report = BatchReport {
            failure: None,
            completed_requests: 0,
            transferred_bytes: 0,
            locally_drained: true,
            notification: NotificationStatus::NotRequested,
        };
        let mut posted_span = false;
        for (i, receipt) in pending.into_iter().enumerate() {
            let mut posted = false;
            let owner = &sub.owners[first + i];
            let result = receipt
                .map_err(|e| (e, RemoteEffect::None))
                .and_then(|r| r.wait(&mut posted));
            #[cfg(feature = "metrics")]
            if let Some(m) = &owner.metrics {
                m.finish(result.as_ref().err().map(|f| &f.0), posted);
            }
            let position = progress.map(|(p, base)| (p, base + first + i));
            let mut result = result.map_err(|(error, remote_effect)| TransferFailure {
                request_index: 0,
                affected_requests: 1,
                error,
                remote_effect,
            });
            if let Err(f) = result {
                result = self
                    .retry_memory_group(
                        op,
                        peer,
                        &group[i..=i],
                        std::slice::from_ref(owner),
                        owner.deadline,
                        None,
                        position,
                        posted,
                        f,
                    )
                    .map(|_| ());
            }
            #[cfg(feature = "metrics")]
            if let Some(m) = &owner.metrics {
                m.terminal(result.as_ref().err().map(|f| &f.error));
            }
            match result {
                Ok(()) => {
                    if report.failure.is_none() {
                        report.completed_requests += 1;
                        report.transferred_bytes += group[i].length;
                    }
                    if let Some((p, index)) = position {
                        p.complete_task(index, group[i].length);
                    }
                    sub.retire_owners(first + i, 1, 1);
                }
                Err(f) => {
                    let drained = !matches!(f.error, Error::Unquiesced { .. });
                    if !drained {
                        report.locally_drained = false;
                        sub.retain_window(first..first + group.len());
                    } else {
                        sub.retire_owners(first + i, 1, 0);
                    }
                    if let Some((p, index)) = position {
                        p.fail_owners(
                            index,
                            TransferFailure {
                                request_index: 0,
                                affected_requests: 1,
                                error: f.error.for_report(),
                                remote_effect: f.remote_effect,
                            },
                            drained,
                        );
                    }
                    if report.failure.is_none() {
                        report.failure = Some(TransferFailure {
                            request_index: i,
                            affected_requests: group.len() - i,
                            ..f
                        });
                    }
                }
            }
            if report.failure.is_some() {
                posted_span |= posted;
            }
        }
        if op == TransferOp::Write && posted_span {
            if let Some(f) = &mut report.failure {
                f.remote_effect = RemoteEffect::Unknown;
            }
        }
        report
    }
}
