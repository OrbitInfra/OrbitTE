use std::collections::VecDeque;
use std::fmt;
use std::sync::Mutex;

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

pub(crate) struct NotificationInbox {
    max_count: usize,
    max_bytes: usize,
    max_message_bytes: usize,
    state: Mutex<InboxState>,
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
        }
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

    pub(crate) fn receive(&self) -> Result<Vec<Notification>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("notification lock poisoned"))?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(state.queued.len())
            .map_err(|_| Error::ResourceExhausted)?;
        while let Some(notification) = state.queued.pop_front() {
            state.retained_count -= 1;
            state.retained_bytes -= notification.name.len() + notification.message.len();
            result.push(notification);
        }
        Ok(result)
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
