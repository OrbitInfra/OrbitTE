//! A native file batch can return independent CQEs, even after the first
//! failed public task. Keep each receipt until the backend has finished
//! draining before deciding which individual owner may try its next route.
use super::*;
use crate::segment::file::FileRequest;
use std::cell::RefCell;

#[derive(Default)]
struct Outcome {
    posted: bool,
    bytes: usize,
    error: Option<Error>,
    effect: Option<RemoteEffect>,
    complete: bool,
}

pub(crate) struct FileAttempts<'a> {
    progress: Option<(&'a BatchProgress, usize)>,
    outcomes: Option<RefCell<Vec<Outcome>>>,
}

impl FileAttempts<'_> {
    pub(crate) fn begin_file_group(&self, i: usize, count: usize) -> usize {
        self.progress
            .map_or(count, |(p, b)| p.begin_file_group(b + i, count))
    }
    #[cfg(feature = "gds")]
    pub(crate) fn cancel_requested(&self, i: usize) -> bool {
        self.progress
            .is_some_and(|(p, b)| p.cancel_requested(b + i))
    }
    #[cfg(feature = "gds")]
    pub(crate) fn file_progress(&self, i: usize, bytes: usize) {
        if let Some((p, b)) = self.progress {
            p.file_progress(b + i, bytes);
        }
    }
    pub(crate) fn posted(&self, i: usize) {
        if let Some(v) = &self.outcomes {
            v.borrow_mut()[i].posted = true;
        }
    }
    #[cfg(feature = "io-uring")]
    pub(crate) fn complete_task(&self, i: usize, bytes: usize) {
        self.complete_file_task(i, bytes, true);
    }
    pub(crate) fn complete_file_task(&self, i: usize, bytes: usize, drained: bool) {
        if let Some(v) = &self.outcomes {
            let mut list = v.borrow_mut();
            list[i].complete = true;
            list[i].bytes = bytes;
            list[i].error = None;
        }
        if let Some((p, b)) = self.progress {
            p.complete_file_task(b + i, bytes, drained);
        }
    }
    pub(crate) fn fail_file_task(
        &self,
        i: usize,
        bytes: usize,
        error: Error,
        effect: RemoteEffect,
        drained: bool,
    ) {
        if let Some(v) = &self.outcomes {
            let mut list = v.borrow_mut();
            let t = &mut list[i];
            t.bytes = bytes;
            t.effect = Some(effect);
            if drained && effect == RemoteEffect::None && retryable_file(&error) {
                t.error = Some(error);
                return;
            }
            t.error = Some(error.for_report());
        }
        if let Some((p, b)) = self.progress {
            p.fail_file_task(b + i, bytes, error, effect, drained);
        }
    }
}

fn retryable_file(error: &Error) -> bool {
    // EOF, truncation and short WRITE are real file results. Reopening a
    // different provider cannot turn an observed short transfer into success.
    failover::retryable(error)
        && !matches!(error, Error::Io(e) if matches!(e.kind(),
        std::io::ErrorKind::UnexpectedEof|std::io::ErrorKind::WriteZero))
}

impl TransferEngine {
    #[allow(clippy::too_many_arguments)]
    fn file_attempt(
        &self,
        op: TransferOp,
        transport: Transport,
        group: &[FileRequest],
        deadline: Instant,
        progress: (&FileAttempts<'_>, usize),
        _submission: &PreparedSubmission,
    ) -> BatchReport {
        let mut result = BatchReport {
            completed_requests: 0,
            transferred_bytes: 0,
            failure: None,
            locally_drained: true,
            notification: NotificationStatus::NotRequested,
        };
        let executed = match transport {
            #[cfg(feature = "io-uring")]
            Transport::IoUring => self.io_uring.as_ref().map(|io| {
                io.execute(
                    op,
                    group,
                    deadline,
                    Some(progress),
                    &mut result,
                    #[cfg(feature = "cuda")]
                    (&self.source.proxy, self.config.max_batch_requests),
                )
            }),
            #[cfg(feature = "gds")]
            Transport::Gds => self.gds.as_ref().map(|gds| {
                gds.execute(
                    op,
                    group,
                    deadline,
                    Some(progress),
                    &mut result,
                    &_submission.gds_pending,
                )
            }),
            _ => None,
        };
        if executed.is_none() {
            result.failure = Some(TransferFailure {
                request_index: 0,
                affected_requests: 1,
                error: Error::Unavailable("selected file transport is unavailable"),
                remote_effect: RemoteEffect::None,
            });
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn file_group(
        &self,
        op: TransferOp,
        transport: Transport,
        group: &[FileRequest],
        deadline: Instant,
        progress: Option<(&BatchProgress, usize)>,
        submission: &PreparedSubmission,
    ) -> BatchReport {
        let eligible = self.config.max_failover_attempts != 0 && group[0].selection.has_fallback();
        let attempts = FileAttempts {
            progress,
            outcomes: eligible
                .then(|| RefCell::new((0..group.len()).map(|_| Outcome::default()).collect())),
        };
        let mut report =
            self.file_attempt(op, transport, group, deadline, (&attempts, 0), submission);
        let Some(records) = &attempts.outcomes else {
            return report;
        };
        if !report.locally_drained {
            if let Some((p, base)) = progress {
                for (i, t) in records.borrow_mut().iter_mut().enumerate() {
                    if let Some(e) = t.error.take() {
                        p.fail_file_task(
                            base + i,
                            t.bytes,
                            e,
                            t.effect.unwrap_or(RemoteEffect::None),
                            false,
                        );
                    }
                }
            }
            return report;
        }
        let Some(failure) = report.failure.take() else {
            return report;
        };
        let none_posted = !records.borrow().iter().any(|t| t.posted);
        let mut first_failure = None;
        let mut last_effect = 0;
        let mut last_attempt = 0;
        for (i, r) in group.iter().enumerate() {
            let mut task = std::mem::take(&mut records.borrow_mut()[i]);
            if !task.complete && task.error.is_none() && (none_posted || i == failure.request_index)
            {
                task.error = Some(failure.error.for_report());
            }
            if task.posted || task.complete {
                last_attempt = i + 1;
            }
            if task.posted && op == TransferOp::Write {
                last_effect = i + 1;
            }
            let mut count = r.selection.initial_attempts;
            while let Some(e) = task.error.as_ref() {
                if count >= self.config.max_failover_attempts
                    || !report.locally_drained
                    || Instant::now() >= deadline
                    || !retryable_file(e)
                    || (op == TransferOp::Write && task.posted)
                    || task.effect == Some(RemoteEffect::Unknown)
                {
                    break;
                }
                count += 1;
                let Some(next) = r.selection.candidate(count) else {
                    break;
                };
                if let Some((p, b)) = progress {
                    if task.posted && !p.retry_on_progress(b + i, e.for_report(), deadline) {
                        break;
                    }
                    if !p.start_retry(
                        b + i,
                        next.transport,
                        count,
                        crate::batch::RunningCancel::File,
                    ) {
                        task.error = Some(Error::Cancelled);
                        break;
                    }
                }
                log::debug!(
                    "retrying file transfer on {:?}, attempt {}",
                    next.transport,
                    count
                );
                let retry = self.file_attempt(
                    op,
                    next.transport,
                    std::slice::from_ref(r),
                    deadline,
                    (&attempts, i),
                    submission,
                );
                report.locally_drained = retry.locally_drained;
                task = std::mem::take(&mut records.borrow_mut()[i]);
                if let Some(f) = retry.failure {
                    task.error = Some(f.error);
                    task.effect = Some(f.remote_effect);
                }
                if task.posted || task.complete {
                    last_attempt = i + 1;
                }
                if task.posted && op == TransferOp::Write {
                    last_effect = i + 1;
                }
            }
            if let Some(error) = task.error {
                if let Some((p, b)) = progress {
                    p.fail_file_task(
                        b + i,
                        task.bytes,
                        error.for_report(),
                        task.effect.unwrap_or(RemoteEffect::None),
                        report.locally_drained,
                    );
                }
                if first_failure.is_none() {
                    first_failure = Some(TransferFailure {
                        request_index: i,
                        affected_requests: 1,
                        error,
                        remote_effect: task.effect.unwrap_or(RemoteEffect::None),
                    });
                }
            } else if !task.complete && first_failure.is_none() {
                first_failure = Some(TransferFailure {
                    request_index: i,
                    affected_requests: 1,
                    error: Error::Cancelled,
                    remote_effect: RemoteEffect::None,
                });
            }
        }
        let prefix = first_failure
            .as_ref()
            .map_or(group.len(), |f| f.request_index);
        if let Some(f) = &mut first_failure {
            f.affected_requests = last_attempt.saturating_sub(prefix).max(1);
            if last_effect > prefix {
                f.remote_effect = RemoteEffect::Unknown;
            }
        }
        report.completed_requests = prefix;
        report.transferred_bytes = group[..prefix].iter().map(|r| r.length).sum();
        report.failure = first_failure;
        report
    }
}
