//! Transport-owned relocation slots. CUDA work and control I/O run outside the map lock.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use crate::engine::remaining;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
use crate::{Error, Result, SourceEpoch};

use super::{Identity, NVLinkTransport, mapping::OpenedMapping};

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) struct RelocationKey {
    pub endpoint: SocketAddr,
    pub epoch: SourceEpoch,
    pub region: u64,
    pub context: usize,
}

pub(super) struct CachedMapping {
    pub token: SourceEpoch,
    pub mapping: Option<OpenedMapping>,
    pub grant: Option<([u8; 64], usize, usize, Identity)>,
    pub closing: bool,
    pub uncertain: bool,
}

impl CachedMapping {
    pub(super) fn new() -> Result<Self> {
        Ok(Self {
            token: SourceEpoch::generate()?,
            mapping: None,
            grant: None,
            closing: false,
            uncertain: false,
        })
    }

    pub(super) fn close(
        &mut self,
        transport: &NVLinkTransport,
        key: RelocationKey,
        requester: SourceEpoch,
        deadline: Instant,
    ) -> Result<()> {
        self.closing = true;
        if self.uncertain {
            return Err(Error::Unquiesced {
                operation: "CUDA IPC cached import",
                code: -1,
            });
        }
        if let Some(mapping) = self.mapping.take() {
            if let Err(error) = mapping.close() {
                self.uncertain = true;
                return Err(error);
            }
        }
        // Only the native close is one-shot. An idempotent release receipt may
        // be retried after an ACK is lost; it never authorizes a new mapping.
        loop {
            let mut channel = ControlChannel::connect(key.endpoint, deadline)?;
            channel.write_all(
                &Request {
                    operation: Operation::NvlinkClose,
                    epoch: key.epoch,
                    region: key.region,
                    offset: 0,
                    length: 0,
                }
                .encode(),
            )?;
            channel.write_all(&transport.cache_hello(requester, self.token, false))?;
            match Reply::read(&mut channel, key.epoch) {
                Err(Error::ResourceExhausted) => {
                    // The previous transfer handler may still be observing EOF.
                    std::thread::sleep(
                        remaining(deadline)?.min(std::time::Duration::from_millis(1)),
                    );
                }
                Ok(reply) if reply.status == 0 && reply.length == 0 => return Ok(()),
                Ok(_) => return Err(Error::Protocol("NVLink cached close acknowledgement")),
                Err(error) => return Err(error),
            }
        }
    }
}

// None is an exclusively checked-out slot, not a missing mapping. The key stays
// charged while its first grant/open is in flight, preventing duplicate opens.
pub(super) struct RelocationCache {
    pub limit: usize,
    entries: Mutex<HashMap<RelocationKey, Option<CachedMapping>>>,
    changed: Condvar,
}

impl RelocationCache {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: Mutex::default(),
            changed: Condvar::new(),
        }
    }

    pub(super) fn acquire(
        &self,
        key: RelocationKey,
        deadline: Instant,
    ) -> Result<RelocationLease<'_>> {
        self.checkout(key, deadline, true)?
            .ok_or(Error::Internal("missing reserved relocation"))
    }

    pub(super) fn existing(
        &self,
        key: RelocationKey,
        deadline: Instant,
    ) -> Result<Option<RelocationLease<'_>>> {
        self.checkout(key, deadline, false)
    }

    fn checkout(
        &self,
        key: RelocationKey,
        deadline: Instant,
        create: bool,
    ) -> Result<Option<RelocationLease<'_>>> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("NVLink relocation lock poisoned"))?;
        loop {
            remaining(deadline)?;
            if let Some(slot) = entries.get_mut(&key) {
                if let Some(entry) = slot.take() {
                    return Ok(Some(RelocationLease {
                        cache: self,
                        key,
                        entry: Some(entry),
                    }));
                }
                entries = self
                    .changed
                    .wait_timeout(entries, remaining(deadline)?)
                    .map_err(|_| Error::Internal("NVLink relocation lock poisoned"))?
                    .0;
            } else {
                if !create {
                    return Ok(None);
                }
                if entries.len() >= self.limit {
                    return Err(Error::ResourceExhausted);
                }
                entries
                    .try_reserve(1)
                    .map_err(|_| Error::ResourceExhausted)?;
                let entry = CachedMapping::new()?;
                entries.insert(key, None);
                return Ok(Some(RelocationLease {
                    cache: self,
                    key,
                    entry: Some(entry),
                }));
            }
        }
    }

    pub(super) fn keys(&self) -> Result<Vec<RelocationKey>> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| Error::Internal("NVLink relocation lock poisoned"))?
            .keys()
            .copied()
            .collect())
    }
}

pub(super) struct RelocationLease<'a> {
    cache: &'a RelocationCache,
    pub key: RelocationKey,
    pub entry: Option<CachedMapping>,
}

impl Drop for RelocationLease<'_> {
    fn drop(&mut self) {
        let mut entries = self.cache.entries.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = self.entry.take() {
            entries.insert(self.key, Some(entry));
        } else {
            entries.remove(&self.key);
        }
        self.cache.changed.notify_all();
    }
}
