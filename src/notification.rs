use crate::runtime::control_plane::{self, Operation};
use crate::{EngineConfig, RemoteSegment, SourceEpoch, TransferEngine};
use std::collections::VecDeque;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::{Error, Result};

/// An application-defined name and message, matching TENT's notification fields.
/// Strings may be empty; their UTF-8 byte lengths count against engine budgets.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Notification {
    pub name: String,
    pub message: String,
}

impl Notification {
    pub(crate) fn byte_len(&self) -> Result<usize> {
        self.name
            .len()
            .checked_add(self.message.len())
            .ok_or(Error::ResourceExhausted)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotificationEffect {
    /// No notification could have been enqueued by this attempt.
    None,
    /// The body was attempted but no final receipt was received. Do not replay.
    Unknown,
}

#[derive(Debug)]
pub struct NotificationFailure {
    pub error: Error,
    pub effect: NotificationEffect,
}

impl From<Error> for NotificationFailure {
    fn from(error: Error) -> Self {
        Self {
            error,
            effect: NotificationEffect::None,
        }
    }
}

impl fmt::Display for NotificationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "notification failed ({:?}): {}", self.effect, self.error)
    }
}

impl std::error::Error for NotificationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Notification completion is separate from committed transfer bytes.
#[derive(Debug)]
pub enum NotificationStatus {
    NotRequested,
    /// An attached notification was skipped for an empty or failed batch.
    Skipped,
    /// The receiver enqueued the message. Application consumption is separate.
    Delivered,
    Failed(NotificationFailure),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct Target {
    pub(crate) endpoint: SocketAddr,
    pub(crate) epoch: SourceEpoch,
}

impl From<&RemoteSegment> for Target {
    fn from(peer: &RemoteSegment) -> Self {
        Self {
            endpoint: peer.endpoint(),
            epoch: peer.source_epoch(),
        }
    }
}

pub(crate) type Delivery = std::result::Result<(), NotificationFailure>;

#[derive(Debug)]
pub(crate) struct AttemptFailure {
    pub(crate) outcome: NotificationFailure,
    pub(crate) retryable: bool,
}
impl From<NotificationFailure> for AttemptFailure {
    fn from(outcome: NotificationFailure) -> Self {
        let retryable = outcome.effect == NotificationEffect::None
            && matches!(
                outcome.error,
                Error::Io(_) | Error::Unavailable(_) | Error::ResourceExhausted
            );
        Self { outcome, retryable }
    }
}
impl From<Error> for AttemptFailure {
    fn from(error: Error) -> Self {
        NotificationFailure::from(error).into()
    }
}
pub(crate) type Attempt = std::result::Result<(), AttemptFailure>;

/// Only installed, implemented notification channels participate. A transport
/// with no notification queue has nothing to drain.
pub(crate) trait NotificationChannel: Send + Sync {
    fn send(&self, _peer: Target, _message: &Notification, _deadline: Instant) -> Option<Delivery> {
        None
    }
    fn drain(&self, _received: &mut Vec<Notification>) -> Result<()> {
        Ok(())
    }
}

struct TcpNotifications;
impl NotificationChannel for TcpNotifications {
    fn send(&self, peer: Target, message: &Notification, deadline: Instant) -> Option<Delivery> {
        Some(control_plane::notify(
            Operation::Notify,
            peer,
            message,
            deadline,
        ))
    }
}

pub(crate) struct NotificationRoutes {
    pub(crate) channels: Vec<Box<dyn NotificationChannel>>,
    fallback: bool,
    // Outstanding hooks share a finite destination and message-byte budget.
    outbox: Arc<Mutex<(usize, usize)>>,
    max_targets: usize,
    max_bytes: usize,
}
impl NotificationRoutes {
    pub(crate) fn new(config: &EngineConfig) -> Self {
        let mut channels: Vec<Box<dyn NotificationChannel>> = Vec::new();
        if config.tcp_notifications {
            channels.push(Box::new(TcpNotifications));
        }
        Self {
            channels,
            fallback: config.notification_rpc_fallback,
            outbox: Arc::default(),
            max_targets: config.max_pending_notifications,
            max_bytes: config.max_pending_notification_bytes,
        }
    }
    pub(crate) fn send(&self, peer: Target, message: &Notification, deadline: Instant) -> Attempt {
        let mut last = None;
        for channel in &self.channels {
            let Some(attempt) = channel.send(peer, message, deadline) else {
                continue;
            };
            match attempt {
                Err(f)
                    if self.fallback
                        && f.effect == NotificationEffect::None
                        && matches!(f.error, Error::Unavailable(_)) =>
                {
                    last = Some(f.error)
                }
                result => return result.map_err(Into::into),
            }
        }
        if !self.fallback {
            return Err(Error::Unavailable("notification channel").into());
        }
        control_plane::notify(Operation::NotifyRpc, peer, message, deadline).map_err(|rpc| {
            // Preserve both the last transport diagnostic and the actual RPC
            // disposition: a stale epoch or lost ACK cannot become retryable.
            let mut failure = AttemptFailure::from(rpc);
            if let Some(error) = last {
                failure.outcome.error = error;
            }
            failure
        })
    }
    pub(crate) fn receive(&self, inbox: &NotificationInbox) -> Result<Vec<Notification>> {
        let mut received = Vec::new();
        let mut last = None;
        for channel in &self.channels {
            if let Err(error) = channel.drain(&mut received) {
                last = Some(error);
            }
        }
        // TCP, control RPC and self share the same FIFO and reservation limits.
        if let Err(error) = inbox.drain(&mut received) {
            last = Some(error);
        }
        match last {
            Some(error) if received.is_empty() => Err(error),
            _ => Ok(received),
        }
    }
    pub(crate) fn hook(
        &self,
        local: Target,
        peers: &[RemoteSegment],
        bytes: usize,
    ) -> Result<NotificationHook> {
        self.hook_targets(local, peers.iter().map(Target::from), bytes)
    }

    pub(crate) fn hook_targets(
        &self,
        local: Target,
        peers: impl ExactSizeIterator<Item = Target>,
        bytes: usize,
    ) -> Result<NotificationHook> {
        let count = peers.len();
        if count == 0 {
            return Err(Error::InvalidConfig("notification target list is empty"));
        }
        let mut usage = self
            .outbox
            .lock()
            .map_err(|_| Error::Internal("notification outbox poisoned"))?;
        if peers.len() > self.max_targets - usage.0 || bytes > self.max_bytes - usage.1 {
            return Err(Error::ResourceExhausted);
        }
        let mut targets = Vec::new();
        targets
            .try_reserve_exact(peers.len())
            .map_err(|_| Error::ResourceExhausted)?;
        targets.extend(peers);
        targets.sort_unstable_by_key(|t| (*t != local, t.endpoint, t.epoch.0));
        targets.dedup();
        // Charge the allocated slots even if duplicate destinations collapse.
        usage.0 += count;
        usage.1 += bytes;
        Ok(NotificationHook {
            targets,
            permanent: None,
            charge: self.outbox.clone(),
            count,
            bytes,
        })
    }
}

pub(crate) struct NotificationHook {
    targets: Vec<Target>,
    permanent: Option<Arc<NotificationStatus>>,
    charge: Arc<Mutex<(usize, usize)>>,
    count: usize,
    bytes: usize,
}
impl NotificationHook {
    pub(crate) fn pending(&self) -> bool {
        !self.targets.is_empty()
    }
    pub(crate) fn attempt(
        &mut self,
        engine: &TransferEngine,
        message: &Notification,
        deadline: Instant,
    ) -> Arc<NotificationStatus> {
        let mut failure = None;
        self.targets.retain(|peer| {
            let Err(f) = engine.notify_target(*peer, message, deadline) else {
                return false;
            };
            // Retry only a provably unposted attempt. A lost receipt, stale
            // epoch or malformed request cannot become eligible on another poll.
            let retry = f.retryable;
            let status = Arc::new(NotificationStatus::Failed(f.outcome));
            if !retry && self.permanent.is_none() {
                self.permanent = Some(status.clone());
            }
            failure = Some(status);
            retry && Instant::now() < deadline
        });
        self.permanent
            .clone()
            .or(failure)
            .unwrap_or_else(|| Arc::new(NotificationStatus::Delivered))
    }
}
impl Drop for NotificationHook {
    fn drop(&mut self) {
        let mut usage = self.charge.lock().unwrap_or_else(|e| e.into_inner());
        usage.0 -= self.count;
        usage.1 -= self.bytes;
    }
}

pub(crate) type Receiver = for<'a> fn(NotificationPermit<'a>, Notification) -> Result<()>;

pub(crate) struct NotificationInbox {
    max_count: usize,
    max_bytes: usize,
    max_message_bytes: usize,
    state: Mutex<InboxState>,
    receiver: RwLock<Option<Receiver>>,
}

#[derive(Default)]
struct InboxState {
    queued: VecDeque<Notification>,
    retained_count: usize,
    retained_bytes: usize,
}

impl NotificationInbox {
    pub(crate) fn new(max_count: usize, max_bytes: usize, max_message_bytes: usize) -> Self {
        Self {
            max_count,
            max_bytes,
            max_message_bytes,
            state: Mutex::default(),
            receiver: RwLock::default(),
        }
    }

    pub(crate) fn set_receiver(&self, receiver: Option<Receiver>) {
        *self.receiver.write().unwrap_or_else(|e| e.into_inner()) = receiver;
    }

    pub(crate) fn deliver(
        &self,
        permit: NotificationPermit<'_>,
        message: Notification,
    ) -> Result<()> {
        let receiver = self
            .receiver
            .read()
            .map_err(|_| Error::Internal("notification callback poisoned"))?;
        let callback = receiver.ok_or(Error::Unavailable("notification callback"))?;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(permit, message)))
            .map_err(|_| Error::Internal("notification callback panicked"))?
    }

    /// Reserve queue capacity before acknowledging readiness to receive a body.
    /// Reservations remain charged until committed or the handler actually exits.
    pub(crate) fn reserve(
        &self,
        name_bytes: usize,
        message_bytes: usize,
    ) -> Result<NotificationPermit<'_>> {
        let bytes = name_bytes
            .checked_add(message_bytes)
            .filter(|bytes| *bytes <= self.max_message_bytes)
            .ok_or(Error::ResourceExhausted)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("notification lock poisoned"))?;
        let count = state
            .retained_count
            .checked_add(1)
            .ok_or(Error::ResourceExhausted)?;
        let total = state
            .retained_bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceExhausted)?;
        if count > self.max_count || total > self.max_bytes {
            return Err(Error::ResourceExhausted);
        }
        let additional = count - state.queued.len();
        state
            .queued
            .try_reserve(additional)
            .map_err(|_| Error::ResourceExhausted)?;
        state.retained_count = count;
        state.retained_bytes = total;
        Ok(NotificationPermit {
            inbox: self,
            bytes,
            committed: false,
        })
    }

    pub(crate) fn push(&self, notification: &Notification) -> Result<()> {
        let permit = self.reserve(notification.name.len(), notification.message.len())?;
        fn copy(value: &str) -> Result<String> {
            let mut result = String::new();
            result
                .try_reserve_exact(value.len())
                .map_err(|_| Error::ResourceExhausted)?;
            result.push_str(value);
            Ok(result)
        }
        permit.commit(Notification {
            name: copy(&notification.name)?,
            message: copy(&notification.message)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn receive(&self) -> Result<Vec<Notification>> {
        let mut result = Vec::new();
        self.drain(&mut result)?;
        Ok(result)
    }
    fn drain(&self, result: &mut Vec<Notification>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("notification lock poisoned"))?;
        result
            .try_reserve_exact(state.queued.len())
            .map_err(|_| Error::ResourceExhausted)?;
        while let Some(notification) = state.queued.pop_front() {
            state.retained_count -= 1;
            state.retained_bytes -= notification.name.len() + notification.message.len();
            result.push(notification);
        }
        Ok(())
    }

    pub(crate) fn usage(&self) -> Result<(usize, usize, usize)> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("notification lock poisoned"))?;
        Ok((
            state.queued.len(),
            state.retained_count,
            state.retained_bytes,
        ))
    }
}

pub(crate) struct NotificationPermit<'a> {
    inbox: &'a NotificationInbox,
    bytes: usize,
    committed: bool,
}

impl NotificationPermit<'_> {
    pub(crate) fn commit(mut self, notification: Notification) -> Result<()> {
        debug_assert_eq!(
            self.bytes,
            notification.name.len() + notification.message.len()
        );
        let mut state = self
            .inbox
            .state
            .lock()
            .map_err(|_| Error::Internal("notification lock poisoned"))?;
        state.queued.push_back(notification);
        self.committed = true;
        Ok(())
    }
}

impl Drop for NotificationPermit<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let mut state = self
                .inbox
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.retained_count -= 1;
            state.retained_bytes -= self.bytes;
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/notification.rs"]
mod tests;
