use std::ptr::NonNull;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use super::{
    ConnectionId, ConnectionResources, DESCRIPTOR_SIZE, Descriptor, Phase, RdmaConfig, RdmaContext,
    RdmaStats, buffers, cq_lane, slice, sys,
};
use crate::{Error, RegisteredMemory, Result, TransferOp};

pub(super) struct GrantLease {
    pub(super) _selection: super::quota::DeviceLease,
    pub(super) usage: Arc<Mutex<RdmaStats>>,
    pub(super) length: usize,
    pub(super) is_direct: bool,
    pub(super) drained: Arc<Condvar>,
}

impl Drop for GrantLease {
    fn drop(&mut self) {
        let mut usage = self.usage.lock().unwrap_or_else(|p| p.into_inner());
        usage.active_grants -= 1;
        if self.is_direct {
            usage.direct_bytes -= self.length;
        } else {
            usage.staging_bytes -= self.length;
        }
        self.drained.notify_all();
    }
}

pub(super) enum Buffer {
    Staged(Staging),
    Direct {
        loan: Option<crate::memory::HostDmaLease>,
        address: NonNull<u8>,
        length: usize,
    },
}

impl Buffer {
    pub(super) fn address(&self) -> NonNull<u8> {
        match self {
            Self::Staged(bytes) => bytes.address,
            Self::Direct { address, .. } => *address,
        }
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Staged(bytes) => bytes.len(),
            Self::Direct { length, .. } => *length,
        }
    }
}

pub(super) struct Staging {
    pub(super) address: NonNull<u8>,
    pub(super) length: usize,
}
impl Staging {
    pub(super) fn new(length: usize) -> Result<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| Error::ResourceExhausted)?;
        bytes.resize(length, 0);
        // No Rust reference/Box to DMA bytes remains while the RNIC owns them.
        let address = NonNull::new(Box::into_raw(bytes.into_boxed_slice()).cast::<u8>())
            .ok_or(Error::ResourceExhausted)?;
        Ok(Self { address, length })
    }
    pub(super) fn len(&self) -> usize {
        self.length
    }
    pub(super) fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: payloads are initialized before exposure; message bytes are
        // initialized before their SEND is posted. No DMA owns either range.
        unsafe { std::slice::from_raw_parts_mut(self.address.as_ptr(), self.length) }
    }
    pub(super) fn bytes(&self) -> &[u8] {
        // SAFETY: payloads require native close. A private receive message is
        // inspected only after its successful actual RQ CQE, without reposting.
        unsafe { std::slice::from_raw_parts(self.address.as_ptr(), self.length) }
    }
}
impl Drop for Staging {
    fn drop(&mut self) {
        // SAFETY: successful native close precedes release. Failed close retains
        // the entire Resources owner without running this deallocator.
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                self.address.as_ptr(),
                self.length,
            )));
        }
    }
}

// Grant state is serialized by its record; connection calls also lock the shared QP.
unsafe impl Send for GrantResources {}

pub(super) struct GrantResources {
    pub(super) _worker: super::workers::SliceLease,
    pub(super) device: Arc<RdmaContext>,
    pub(super) handles: sys::MemoryHandles,
    pub(super) cached_mr: Option<buffers::MrLease>,
    pub(super) buffer: Buffer,
    pub(super) _lease: GrantLease,
    pub(super) max_read_slices: usize,
    pub(super) permission: u32,
    pub(super) generation: u64,
    pub(super) rkey: u32,
    pub(super) connection: Option<ConnectionId>,
    pub(super) window_used: bool,
    pub(super) window_bound: bool,
    pub(super) message: Option<super::receive::MessageBuffer>,
}

impl GrantResources {
    pub(super) fn close(
        &mut self,
        connection: &ConnectionResources,
        verbs: &sys::Verbs,
    ) -> std::result::Result<(), (&'static str, i32)> {
        if !connection.closed
            || !Arc::ptr_eq(&self.device, &connection.device)
            || self.connection != connection.incarnation
        {
            return Err(("RDMA grant before connection drain", libc::EBUSY));
        }
        self.close_native(verbs)
    }

    pub(super) fn close_drained(
        &mut self,
        connection: &ConnectionResources,
        receipt: &cq_lane::GrantDrain,
        verbs: &sys::Verbs,
    ) -> std::result::Result<(), (&'static str, i32)> {
        if !Arc::ptr_eq(&self.device, &connection.device) || !receipt.matches(connection, self) {
            return Err(("RDMA grant lacks sealed operation receipt", libc::EBUSY));
        }
        self.close_native(verbs)
    }

    fn close_native(&mut self, verbs: &sys::Verbs) -> std::result::Result<(), (&'static str, i32)> {
        if let Err(Error::Unquiesced { operation, code }) = self.device.asynchronous.check() {
            return Err((operation, code));
        }
        if !self.handles.mw.is_null() {
            let code = unsafe { (verbs.dealloc_window)(&mut self.handles) };
            if code != 0 {
                return Err(("ibv_dealloc_mw", code));
            }
            self._lease
                .usage
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .active_windows -= 1;
        }
        if let Some(cached) = &mut self.cached_mr {
            cached.release().map_err(|error| match error {
                Error::Unquiesced { operation, code } => (operation, code),
                _ => ("RDMA MR borrow release", libc::EIO),
            })?;
            self.handles.mr = std::ptr::null_mut();
        } else {
            let code = unsafe { (verbs.deregister)(&mut self.handles) };
            if code != 0 {
                return Err(("ibv_dereg_mr", code));
            }
        }
        if let Some(message) = &mut self.message {
            message.close(verbs)?;
        }
        self.message = None;
        Ok(())
    }
}

pub(crate) struct RdmaGrant {
    pub(super) record: Arc<cq_lane::Record>,
    pub(super) progress: Arc<cq_lane::CqLane>,
    pub(super) local: Descriptor,
    pub(super) incarnation: Option<ConnectionId>,
    pub(super) config: RdmaConfig,
    pub(super) deadline: Instant,
    pub(super) output: Option<Box<GrantResources>>,
    pub(super) claim: u64,
    pub(super) released: bool,
    pub(super) cached_peer: Option<super::endpoint::Peer>,
    pub(super) endpoint: Option<Arc<super::endpoint::RdmaEndpoint>>,
}

impl RdmaGrant {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, cq_lane::Request>> {
        if self.released {
            return Err(Error::Unavailable("RDMA grant handle has been consumed"));
        }
        let state = self.record.lock()?;
        state.check_claim(self.claim)?;
        Ok(state)
    }

    pub(super) fn open(&mut self, permission: u32, psn: u32) -> Result<()> {
        self.record.require_live()?;
        let mut state = self.lock()?;
        self.record.require_live()?;
        state.check()?;
        if state.phase != Phase::Init {
            return Err(Error::Unavailable("RDMA endpoint is closed"));
        }
        let resources = state
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        let device = Arc::clone(&resources.connection.lock()?.device);
        let mut local = Descriptor::default();
        let (incarnation, _) =
            match device.open_endpoint(resources, &self.config, permission, psn, &mut local) {
                Ok(value) => value,
                Err(error) => {
                    drop(state);
                    device.asynchronous.notify();
                    return Err(error);
                }
            };
        local.validate()?;
        let connection = Descriptor {
            address: 0,
            rkey: 0,
            ..local
        };
        resources.grant_mut()?.rkey = local.rkey;
        state.qpn = connection.qpn;
        drop(state);
        self.incarnation = Some(incarnation);
        self.local = connection;
        Ok(())
    }

    pub(crate) fn incarnation(&self) -> Result<ConnectionId> {
        self.incarnation
            .ok_or(Error::Internal("missing RDMA connection identity"))
    }

    pub(crate) fn grant_generation(&self) -> Result<u64> {
        let state = self.lock()?;
        state.check()?;
        let generation = state
            .resources
            .as_ref()
            .and_then(|r| r.grant.as_ref())
            .filter(|g| g.permission != 0)
            .map_or(0, |g| g.generation);
        if generation == 0 {
            return Err(Error::Internal("endpoint owns no source grant generation"));
        }
        Ok(generation)
    }

    pub(crate) fn connection(&self) -> [u8; DESCRIPTOR_SIZE] {
        Descriptor {
            address: 0,
            rkey: 0,
            ..self.local
        }
        .encode()
    }

    pub(crate) fn grant(&self) -> Result<Descriptor> {
        self.record.require_live()?;
        let request = self.lock()?;
        request.check()?;
        if !request
            .resources
            .as_ref()
            .is_some_and(|r| r.grant.as_ref().is_some_and(|g| g.window_bound))
            || request.phase != Phase::Connected
        {
            return Err(Error::Unavailable("source MW has no completed bind"));
        }
        let resources = request
            .resources
            .as_ref()
            .ok_or(Error::Internal("missing RDMA grant owner"))?;
        Ok(Descriptor {
            address: resources.grant()?.buffer.address().as_ptr() as u64,
            rkey: resources.grant()?.rkey,
            ..resources.connection.lock()?.local
        })
    }

    pub(super) fn post_receive(
        &mut self,
        binding: &[u8; super::COMPLETION_MESSAGE_SIZE],
        credits: super::receive::ReceiveCredits,
    ) -> Result<()> {
        self.progress
            .post_receive(&self.record, self.claim, *binding, credits, self.deadline)
    }

    pub(crate) fn receive_completion(&self) -> Result<()> {
        self.progress
            .receive_completion(&self.record, self.claim, self.deadline)
    }

    pub(crate) fn send_completion(
        &mut self,
        binding: &[u8; super::COMPLETION_MESSAGE_SIZE],
        rkey: u32,
    ) -> Result<()> {
        self.progress
            .send_completion(&self.record, self.claim, *binding, rkey, self.deadline)
    }

    pub(crate) fn bind_window(&mut self) -> Result<()> {
        self.progress.window(
            &self.record,
            self.claim,
            slice::Operation::Bind,
            self.deadline,
        )
    }

    #[cfg(test)]
    pub(crate) fn invalidate_window(&mut self) -> Result<()> {
        self.progress.window(
            &self.record,
            self.claim,
            slice::Operation::Invalidate,
            self.deadline,
        )
    }

    pub(crate) fn fill(&mut self, memory: &RegisteredMemory, offset: usize) -> Result<()> {
        self.record.require_live()?;
        let mut state = self.lock()?;
        self.record.require_live()?;
        state.check()?;
        if state.phase != Phase::Init {
            return Err(Error::Internal("RDMA staging already exposed"));
        }
        match &mut state
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA staging"))?
            .grant_mut()?
            .buffer
        {
            Buffer::Staged(bytes) => memory.read(offset, bytes.bytes_mut()),
            Buffer::Direct { .. } => Ok(()),
        }
    }

    pub(crate) fn copy_back(&self, memory: &RegisteredMemory, offset: usize) -> Result<()> {
        let state = self.lock()?;
        state.check()?;
        if state.phase != Phase::Closed {
            return Err(Error::Internal("RDMA buffer is not physically quiescent"));
        }
        match &self
            .output
            .as_ref()
            .ok_or(Error::Internal("missing RDMA buffer"))?
            .buffer
        {
            Buffer::Staged(bytes) => memory.write(offset, bytes.bytes()),
            Buffer::Direct { .. } => Ok(()),
        }
    }

    pub(crate) fn connect(&mut self, remote: &Descriptor) -> Result<()> {
        remote.validate_connection()?;
        self.record.require_live()?;
        let device = {
            let state = self.lock()?;
            Arc::clone(
                &state
                    .resources
                    .as_ref()
                    .ok_or(Error::Internal("missing RDMA endpoint"))?
                    .connection
                    .lock()?
                    .device,
            )
        };
        device.check_address()?;
        let mut state = self.lock()?;
        self.record.require_live()?;
        state.check()?;
        if state.phase != Phase::Init || remote.link_layer != self.local.link_layer {
            return Err(Error::Protocol("RDMA state or link layer mismatch"));
        }
        let resources = state
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        let owner = Arc::clone(&resources.connection);
        let mut connection = owner.lock()?;
        owner.require_live()?;
        if let Some(existing) = connection.remote {
            if existing.connection_bytes() != remote.connection_bytes() {
                return Err(Error::Protocol("cached RDMA peer descriptor changed"));
            }
            state.phase = Phase::Connected;
            return Ok(());
        }
        let device = Arc::clone(&connection.device);
        let local = connection.local;
        if let Err(error) = device.connect(&mut connection.handles, &local, remote, &self.config) {
            drop(state);
            device.asynchronous.notify();
            return Err(error);
        }
        connection.remote = Some(*remote);
        state.phase = Phase::Connected;
        Ok(())
    }

    pub(crate) fn transfer(
        &mut self,
        remote: &Descriptor,
        operation: TransferOp,
        deadline: Instant,
    ) -> Result<()> {
        self.progress
            .submit(&self.record, self.claim, *remote, operation, deadline)?;
        self.progress.wait(&self.record, self.claim, deadline)
    }

    pub(super) fn finish_sent(
        &mut self,
        binding: &[u8; super::COMPLETION_MESSAGE_SIZE],
    ) -> Result<()> {
        self.progress
            .finish_sent(&self.record, self.claim, binding, self.deadline)
    }

    pub(super) fn finish_received(&mut self, done: &super::protocol::BoundDone) -> Result<()> {
        self.progress
            .finish_received(&self.record, self.claim, done, self.deadline)
    }

    pub(super) fn cache(&mut self, peer: super::endpoint::Peer) -> Result<()> {
        if self.released
            || self.output.is_some()
            || peer.local != self.incarnation()?
            || peer.generation == 0
        {
            return Err(Error::Internal("RDMA idle claim ownership"));
        }
        if let Some(expected) = self.cached_peer {
            if !expected.same_pair(&peer) || peer.generation <= expected.generation {
                return Err(Error::Protocol(
                    "RDMA cached pair or grant generation changed",
                ));
            }
        }
        if !self.progress.cache(&self.record, self.claim, peer)? {
            self.close()?;
        }
        self.cached_peer = Some(peer);
        self.released = true;
        Ok(())
    }

    pub(super) fn retire_idle(&self) -> Result<()> {
        self.progress
            .retire_idle(&self.record, self.claim, self.deadline)
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }
        let result = self
            .progress
            .close_record(&self.record, self.claim, self.deadline);
        let mut request = self.record.lock()?;
        // A consumed/stale handle has no authority over the next grant.
        if request.claim == self.claim && request.phase == Phase::Closed {
            if let Some(mut resources) = request.resources.take() {
                self.output = resources.grant.take().map(Box::new);
            }
        }
        result
    }
}

impl Drop for RdmaGrant {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/grant.rs"]
mod tests;
