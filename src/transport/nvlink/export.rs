//! Export lifetime is independent of any one transfer handler or registered view.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

use crate::cuda::ipc::IpcExport;
use crate::engine::remaining;
use crate::runtime::control_plane::{ControlChannel, Request};
use crate::{Error, RegisteredMemory, Result, SourceEpoch};

use super::budget::MappingPermit;
use super::{BINDING_SIZE, GRANT_SIZE, Identity, NVLinkTransport, RECEIPT_SIZE};

pub(super) const CACHE_HELLO_SIZE: usize = 69;

struct ExportEntry {
    export: Option<IpcExport>,
    memory: Option<RegisteredMemory>,
    permit: Option<MappingPermit>,
    identity: Identity,
    requester: SourceEpoch,
    active: bool,
    failed: bool,
}

impl Drop for ExportEntry {
    fn drop(&mut self) {
        if self.export.is_some() {
            // Dropping an engine is not a receipt from the importer.
            std::mem::forget(self.memory.take());
            if let Some(permit) = self.permit.take() {
                permit.retain();
            }
        }
    }
}

pub(crate) struct ExportCache {
    limit: usize,
    entries: Mutex<HashMap<SourceEpoch, ExportEntry>>,
    changed: Condvar,
}

impl ExportCache {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: Mutex::default(),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn wait_empty(&self, deadline: Instant) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::Internal("NVLink export lock poisoned"))?;
        while !entries.is_empty() {
            entries = self
                .changed
                .wait_timeout(entries, remaining(deadline)?)
                .map_err(|_| Error::Internal("NVLink export lock poisoned"))?
                .0;
        }
        Ok(())
    }
}

impl NVLinkTransport {
    pub(super) fn cache_hello(
        &self,
        requester: SourceEpoch,
        token: SourceEpoch,
        reuse: bool,
    ) -> [u8; CACHE_HELLO_SIZE] {
        let mut hello = [0; CACHE_HELLO_SIZE];
        hello[..4].copy_from_slice(b"NVL2");
        self.identity.encode(&mut hello[4..36]);
        hello[36..52].copy_from_slice(&requester.0);
        hello[52..68].copy_from_slice(&token.0);
        hello[68] = u8::from(reuse);
        hello
    }

    fn read_cache_hello(
        &self,
        channel: &mut ControlChannel<'_>,
    ) -> Result<(Identity, SourceEpoch, SourceEpoch, bool)> {
        if self.exports.limit == 0 {
            return Err(Error::Unavailable("NVLink cache is not configured"));
        }
        let mut hello = [0; CACHE_HELLO_SIZE];
        channel.read_exact(&mut hello)?;
        if &hello[..4] != b"NVL2" || hello[68] > 1 {
            return Err(Error::Protocol("NVLink cache hello"));
        }
        let identity = Identity::decode(&hello[4..36]);
        self.identity.validate_peer(identity)?;
        let mut requester = [0; 16];
        let mut token = [0; 16];
        requester.copy_from_slice(&hello[36..52]);
        token.copy_from_slice(&hello[52..68]);
        Ok((
            identity,
            SourceEpoch(requester),
            SourceEpoch(token),
            hello[68] == 1,
        ))
    }

    pub(super) fn serve_cached(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let (identity, requester, token, reuse) = self.read_cache_hello(channel)?;
        let (allocation, allocation_offset) = memory.cuda_range(offset, length)?;
        let nonce = SourceEpoch::generate()?;
        let mut grant = [0; GRANT_SIZE];
        grant[..4].copy_from_slice(b"NVL1");
        self.identity.encode(&mut grant[4..36]);
        grant[36..88].copy_from_slice(&request.encode());
        grant[88..104].copy_from_slice(&requester.0);
        grant[104..120].copy_from_slice(&nonce.0);
        {
            let mut entries = self
                .exports
                .entries
                .lock()
                .map_err(|_| Error::Internal("NVLink export lock poisoned"))?;
            if !reuse {
                if entries.contains_key(&token) {
                    return Err(Error::Protocol("duplicate NVLink cache token"));
                }
                if entries.len() >= self.exports.limit {
                    return Err(Error::ResourceExhausted);
                }
                entries
                    .try_reserve(1)
                    .map_err(|_| Error::ResourceExhausted)?;
                let export = IpcExport::new(allocation)?;
                let permit = self.budget.reserve(export.extent, true)?;
                entries.insert(
                    token,
                    ExportEntry {
                        export: Some(export),
                        memory: Some(memory.clone()),
                        permit: Some(permit),
                        identity,
                        requester,
                        active: false,
                        failed: false,
                    },
                );
            }
            let entry = entries.get_mut(&token).ok_or(Error::RegionNotFound)?;
            if entry.identity != identity
                || entry.requester != requester
                || entry.memory.as_ref().map(RegisteredMemory::id) != Some(request.region)
            {
                return Err(Error::Protocol("NVLink cache owner binding"));
            }
            if entry.active {
                return Err(Error::ResourceExhausted);
            }
            if entry.failed {
                return Err(Error::Unavailable("NVLink export awaits close receipt"));
            }
            let export = entry
                .export
                .as_mut()
                .ok_or(Error::Internal("missing CUDA export"))?;
            grant[120..128].copy_from_slice(&(export.extent as u64).to_be_bytes());
            grant[128..136].copy_from_slice(&(allocation_offset as u64).to_be_bytes());
            grant[136..].copy_from_slice(&export.handle);
            entry.active = true;
        }
        let mut exposed = reuse;
        let completion = (|| {
            if !reuse {
                // A close can overtake a queued initial request. No handle is
                // exposed until the requester answers this offer, sent only
                // after the active entry exists. A requester that already
                // closed its original socket cannot answer a late offer.
                channel.reply(epoch, 12, 0)?;
                let mut ready = [0; 1];
                channel.read_exact(&mut ready)?;
                if ready != [0] {
                    return Err(Error::Protocol("NVLink cache ready marker"));
                }
                let mut entries = self
                    .exports
                    .entries
                    .lock()
                    .map_err(|_| Error::Internal("NVLink export lock poisoned"))?;
                entries
                    .get_mut(&token)
                    .and_then(|entry| entry.export.as_mut())
                    .ok_or(Error::Internal("missing active CUDA export"))?
                    .expose();
                exposed = true;
            }
            channel.reply(epoch, 0, GRANT_SIZE as u64)?;
            channel.write_all(&grant)?;
            let mut receipt = [0; RECEIPT_SIZE];
            channel.read_exact(&mut receipt)?;
            if receipt[..BINDING_SIZE] != grant[36..120] || receipt[BINDING_SIZE] > 1 {
                return Err(Error::Protocol("NVLink cached completion binding"));
            }
            Ok(())
        })();
        {
            let mut entries = self
                .exports
                .entries
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let entry = entries
                .get_mut(&token)
                .ok_or(Error::Internal("missing active CUDA export"))?;
            entry.active = false;
            entry.failed = completion.is_err();
            if !exposed {
                if let Some(mut entry) = entries.remove(&token) {
                    if let Some(export) = entry.export.take() {
                        export.finish();
                    }
                }
                self.exports.changed.notify_all();
            }
        }
        // This handler's admission is released on EOF. The export table remains
        // the lifetime owner, including after unregister or source admission close.
        completion?;
        channel.reply(epoch, 0, 0)
    }

    pub(crate) fn serve_close(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
    ) -> Result<()> {
        if request.offset != 0 || request.length != 0 {
            return Err(Error::Protocol("NVLink close has range"));
        }
        let (identity, requester, token, reuse) = self.read_cache_hello(channel)?;
        if reuse {
            return Err(Error::Protocol("NVLink close reuse flag"));
        }
        let mut entries = self
            .exports
            .entries
            .lock()
            .map_err(|_| Error::Internal("NVLink export lock poisoned"))?;
        if let Some(entry) = entries.get(&token) {
            if entry.identity != identity
                || entry.requester != requester
                || entry.memory.as_ref().map(RegisteredMemory::id) != Some(request.region)
            {
                return Err(Error::Protocol("NVLink close owner binding"));
            }
            if entry.active {
                return Err(Error::ResourceExhausted);
            }
        }
        if let Some(mut entry) = entries.remove(&token) {
            if let Some(export) = entry.export.take() {
                export.finish();
            }
        }
        self.exports.changed.notify_all();
        drop(entries);
        channel.reply(epoch, 0, 0)
    }
}
