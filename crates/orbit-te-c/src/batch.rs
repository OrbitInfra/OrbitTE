use super::{abi, boundary::*, engine::Engine, memory::transport, segments::Target};
use orbit_te::{Error, Notification, Priority, TaskState, TaskStatus, TransferOp, TransferRequest};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

impl Engine {
    pub fn alloc_batch(&self, n: usize) -> Result<u64> {
        let batch = self.core.allocate_batch(n)?;
        let id = super::engine::id()? as u64;
        lock(&self.batches)?.insert(id, Arc::new(Mutex::new(batch)));
        Ok(id)
    }
    pub fn free_batch(&self, id: u64) -> Result<()> {
        let batch = self.batch(id)?;
        lock(&batch)?.free()?;
        lock(&self.batches)?.remove(&id);
        Ok(())
    }
    pub fn submit(
        &self,
        id: u64,
        raw: &[abi::Request],
        notify: Option<Notification>,
    ) -> Result<()> {
        let batch = self.batch(id)?;
        let mut batch = lock(&batch)?;
        if raw.len()
            > batch
                .capacity()?
                .saturating_sub(batch.status()?.submitted_tasks)
        {
            return Err(FULL);
        }
        let mut targets = HashMap::<u64, Target>::new();
        let mut requests = Vec::with_capacity(raw.len());
        let mut runs = Vec::new();
        for r in raw {
            let op = match r.opcode {
                0 => TransferOp::Read,
                1 => TransferOp::Write,
                _ => return Err(INVALID),
            };
            let hint = transport(r.transport_hint)?;
            let priority = match r.priority {
                0 => Priority::High,
                1 => Priority::Medium,
                2 => Priority::Low,
                _ => return Err(INVALID),
            };
            let n = usize::try_from(r.length).map_err(|_| INVALID)?;
            let source = r.source as usize;
            if source == 0
                || n == 0
                || source.checked_add(n).is_none()
                || r.target_offset.checked_add(r.length).is_none()
            {
                return Err(INVALID);
            }
            if let std::collections::hash_map::Entry::Vacant(e) = targets.entry(r.target_id) {
                e.insert(self.target(r.target_id)?);
            }
            if runs
                .last()
                .is_none_or(|&(prior, target, _)| prior != op || target != r.target_id)
            {
                runs.push((op, r.target_id, requests.len()));
            }
            requests.push(
                TransferRequest::new(hint, source as u64, r.target_offset, n)
                    .with_priority(priority),
            );
        }
        let mut groups = Vec::with_capacity(runs.len());
        for (i, &(op, id, start)) in runs.iter().enumerate() {
            let end = runs.get(i + 1).map_or(requests.len(), |g| g.2);
            groups.push((
                op,
                targets.get(&id).ok_or(INTERNAL)?.view(),
                &requests[start..end],
            ));
        }
        batch.submit_groups(&groups, notify.as_ref(), self.timeout())?;
        Ok(())
    }
}
pub(crate) fn terminal(tasks: &[TaskStatus]) -> Result<(i32, i32)> {
    let mut first = Code(0);
    let mut known = true;
    for task in tasks {
        let (code, certain) = match (task.state, task.failure.as_deref()) {
            (TaskState::Waiting | TaskState::Running, _) => return Err(FULL),
            (TaskState::Completed, _) => (Code(0), true),
            (TaskState::Failed, Some(f))
                if !matches!(
                    f.error,
                    Error::Timeout | Error::Cancelled | Error::DeadlineInfeasible
                ) =>
            {
                let certain = f.remote_effect == orbit_te::RemoteEffect::None;
                let code = match &f.error {
                    Error::Io(_) | Error::Protocol(_) if !certain => UNKNOWN,
                    Error::Io(io)
                        if matches!(
                            io.kind(),
                            std::io::ErrorKind::ConnectionRefused
                                | std::io::ErrorKind::NetworkUnreachable
                                | std::io::ErrorKind::HostUnreachable
                                | std::io::ErrorKind::AddrNotAvailable
                        ) =>
                    {
                        UNSENT
                    }
                    e => Code::from(e),
                };
                (code, certain && code != UNKNOWN)
            }
            _ => (UNKNOWN, false),
        };
        if first.0 == 0 || code == UNKNOWN {
            first = code;
        }
        known &= certain;
    }
    Ok((first.0, i32::from(known)))
}

pub(crate) fn status(t: &TaskStatus) -> abi::Status {
    let state = match t.state {
        TaskState::Waiting => 0,
        TaskState::Running => 1,
        TaskState::Cancelled => 3,
        TaskState::Completed => 4,
        TaskState::Failed | TaskState::Skipped => match t.failure.as_ref().map(|f| &f.error) {
            Some(Error::Timeout | Error::DeadlineInfeasible) => 5,
            Some(Error::Cancelled) => 3,
            _ => 6,
        },
    };
    abi::Status {
        status: state,
        transferred_bytes: t.transferred_bytes as u64,
    }
}
pub(crate) fn overall(tasks: &[TaskStatus]) -> abi::Status {
    let mut bytes = 0;
    let mut pending = false;
    let mut failure = None;
    for t in tasks {
        let s = status(t);
        match s.status {
            0 | 1 => pending = true,
            4 => bytes += s.transferred_bytes,
            n => failure = Some(failure.map_or(n, |old: i32| old.max(n))),
        }
    }
    abi::Status {
        status: if pending { 1 } else { failure.unwrap_or(4) },
        transferred_bytes: bytes,
    }
}
