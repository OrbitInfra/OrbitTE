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
    pub may_be_exposed: bool,
    pub closing: bool,
    pub uncertain: bool,
    idle_since: Instant,
}

impl CachedMapping {
    pub(super) fn new() -> Result<Self> {
        Ok(Self {
            token: SourceEpoch::generate()?,
            mapping: None,
            grant: None,
            may_be_exposed: false,
            closing: false,
            uncertain: false,
            idle_since: Instant::now(),
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
        // Without an answer to the first no-handle offer, dropping the original
        // socket is sufficient: any late reservation cannot become exposed.
        if !self.may_be_exposed {
            return Ok(());
        }
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

// A replacement reserves its new key while the old key still owns capacity.
// Boxed idle owners keep the map and lease moves small; hits allocate nothing.
enum RelocationSlot {
    Idle(Box<CachedMapping>),
    CheckedOut,
    Reclaiming(RelocationKey),
}

pub(super) struct RelocationCache {
    pub limit: usize,
    entries: Mutex<HashMap<RelocationKey, RelocationSlot>>,
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
        transport: &NVLinkTransport,
        requester: SourceEpoch,
        deadline: Instant,
    ) -> Result<RelocationLease<'_>> {
        self.checkout(key, deadline, Some((transport, requester)))?
            .ok_or(Error::Internal("missing reserved relocation"))
    }

    pub(super) fn existing(
        &self,
        key: RelocationKey,
        deadline: Instant,
    ) -> Result<Option<RelocationLease<'_>>> {
        self.checkout(key, deadline, None)
    }

    fn checkout(
        &self,
        key: RelocationKey,
        deadline: Instant,
        create: Option<(&NVLinkTransport, SourceEpoch)>,
    ) -> Result<Option<RelocationLease<'_>>> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("NVLink relocation lock poisoned"))?;
        loop {
            remaining(deadline)?;
            if let Some(slot) = entries.get_mut(&key) {
                if matches!(slot, RelocationSlot::Idle(_)) {
                    let RelocationSlot::Idle(entry) =
                        std::mem::replace(slot, RelocationSlot::CheckedOut)
                    else {
                        return Err(Error::Internal("missing idle relocation"));
                    };
                    return Ok(Some(RelocationLease {
                        cache: self,
                        key,
                        entry: Some(entry),
                    }));
                }
            } else {
                let Some((transport, requester)) = create else {
                    return Ok(None);
                };
                // Only misses scan. A target reservation must be checked even
                // if an unrelated close freed capacity during reclamation.
                let replacing = entries.values().any(
                    |slot| matches!(slot, RelocationSlot::Reclaiming(target) if *target == key),
                );
                if !replacing {
                    if entries.len() < self.limit {
                        entries
                            .try_reserve(1)
                            .map_err(|_| Error::ResourceExhausted)?;
                        let entry = Box::new(CachedMapping::new()?);
                        entries.insert(key, RelocationSlot::CheckedOut);
                        return Ok(Some(RelocationLease {
                            cache: self,
                            key,
                            entry: Some(entry),
                        }));
                    }
                    let oldest = entries
                        .iter_mut()
                        .filter_map(|(key, slot)| match slot {
                            RelocationSlot::Idle(entry) if !entry.uncertain => {
                                Some((*key, entry.idle_since, slot))
                            }
                            _ => None,
                        })
                        .min_by_key(|(_, since, _)| *since);
                    let Some((old_key, _, slot)) = oldest else {
                        return Err(Error::ResourceExhausted);
                    };
                    let RelocationSlot::Idle(entry) =
                        std::mem::replace(slot, RelocationSlot::Reclaiming(key))
                    else {
                        return Err(Error::Internal("missing reclaimable relocation"));
                    };
                    let mut lease = RelocationLease {
                        cache: self,
                        key: old_key,
                        entry: Some(entry),
                    };
                    drop(entries);
                    let entry = lease
                        .entry
                        .as_mut()
                        .ok_or(Error::Internal("missing reclaimed relocation"))?;
                    entry.close(transport, old_key, requester, deadline)?;
                    **entry = CachedMapping::new()?;
                    let mut entries = self
                        .entries
                        .lock()
                        .map_err(|_| Error::Internal("NVLink relocation lock poisoned"))?;
                    entries.remove(&old_key);
                    entries.insert(key, RelocationSlot::CheckedOut);
                    lease.key = key;
                    self.changed.notify_all();
                    return Ok(Some(lease));
                }
            }
            entries = self
                .changed
                .wait_timeout(entries, remaining(deadline)?)
                .map_err(|_| Error::Internal("NVLink relocation lock poisoned"))?
                .0;
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
    pub entry: Option<Box<CachedMapping>>,
}

impl Drop for RelocationLease<'_> {
    fn drop(&mut self) {
        let mut entries = self.cache.entries.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(mut entry) = self.entry.take() {
            entry.idle_since = Instant::now();
            entries.insert(self.key, RelocationSlot::Idle(entry));
        } else {
            entries.remove(&self.key);
        }
        self.cache.changed.notify_all();
    }
}
