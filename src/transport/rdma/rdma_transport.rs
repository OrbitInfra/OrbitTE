use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::endpoint_store::Peer;
use super::{
    ContextSet, ContextSlot, RdmaConfig, RdmaHostMemory, RdmaStats, RdmaTransport, buffers,
    context::RdmaContext,
    endpoint::{
        Buffer, ConnectionLease, ConnectionResources, GrantLease, GrantResources, Resources,
        Staging,
    },
    quota::{DeviceLease, DeviceSelector},
    rail_monitor::{self, NicPath},
    sys, workers,
};
use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{
    ControlChannel, Operation, REQUEST_SIZE, Reply, Request, u64_at,
};
use crate::transport::rdma::{ConnectionId, DESCRIPTOR_SIZE, Descriptor, RdmaEndpoint};
use crate::{
    Error, RegisteredMemory, RemoteEffect, RemoteSegment, Result, SourceEpoch, TransferOp,
};

impl RdmaTransport {
    pub(crate) fn new(config: RdmaConfig) -> Result<Arc<Self>> {
        config.validate()?;
        let usage = Arc::default();
        let registrations =
            buffers::LocalBufferManager::new(&config, Arc::clone(&usage), &sys::REAL);
        Ok(Arc::new(Self {
            config,
            usage,
            counters: Arc::default(),
            verbs: &sys::REAL,
            context_set: Mutex::default(),
            drained: Arc::default(),
            registrations,
        }))
    }

    pub(crate) fn stats(&self) -> Result<RdmaStats> {
        let mut stats = *self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        stats.acknowledged_async_events = self
            .counters
            .acknowledged_async_events
            .load(Ordering::Relaxed);
        stats.completion_events = self.counters.completion_events.load(Ordering::Relaxed);
        Ok(stats)
    }

    pub(crate) fn topology(&self) -> Result<Option<crate::Topology>> {
        Ok(self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?
            .topology
            .clone())
    }

    pub(super) fn initialize_contexts(&self, contexts: &mut ContextSet) -> Result<()> {
        if contexts.closed {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        if let Some((operation, code)) = contexts.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        if contexts.topology.is_some() {
            return Ok(());
        }
        let (mut topology, candidates) = (self.verbs.discover)(&self.config)?;
        if topology.nic_list.len() > 64 {
            return Err(Error::InvalidConfig(
                "RDMA device masks support at most 64 topology NICs",
            ));
        }
        contexts
            .slots
            .resize_with(topology.nic_list.len(), ContextSlot::default);
        let mut available = 0;
        for (id, slot) in contexts.slots.iter_mut().enumerate() {
            let name = topology.nic_list[id].name.clone();
            for address in candidates.iter().filter(|address| address.device == name) {
                match RdmaContext::open_selected(
                    &self.config,
                    self.verbs,
                    Arc::clone(&self.usage),
                    Arc::clone(&self.counters),
                    address.clone(),
                ) {
                    Ok(owner) => {
                        owner.address()?.publish(&mut topology)?;
                        slot.owner = Some(owner);
                        available |= 1u64 << id;
                        break;
                    }
                    Err(Error::Unquiesced { operation, code }) => {
                        contexts.failure = Some((operation, code));
                        return Err(Error::Unquiesced { operation, code });
                    }
                    Err(_) => continue,
                }
            }
        }
        contexts.selector = Some(DeviceSelector::new(topology.clone(), available));
        contexts.topology = Some(topology);
        Ok(())
    }

    fn paths(&self) -> Result<Vec<NicPath>> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        self.initialize_contexts(&mut contexts)?;
        let topology = contexts
            .topology
            .as_ref()
            .ok_or(Error::Internal("missing RDMA topology"))?;
        let mut paths = Vec::new();
        for (id, slot) in contexts.slots.iter().enumerate() {
            if self.config.device_mask & (1u64 << id) == 0 {
                continue;
            }
            if let Some(owner) = &slot.owner {
                if owner.asynchronous.check().is_err() {
                    continue;
                }
                let address = owner.address()?;
                paths.push(NicPath {
                    nic_id: id,
                    name: topology.nic_list[id].name.clone(),
                    domain: owner.domain()?,
                    gid_type: address.gid_type,
                    descriptor: address.descriptor,
                });
            }
        }
        if paths.is_empty() {
            return Err(Error::Unavailable(
                "no enabled RDMA context matches device mask",
            ));
        }
        Ok(paths)
    }

    pub(super) fn allocate(&self, length: usize, mask: u64) -> Result<DeviceLease> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        self.initialize_contexts(&mut contexts)?;
        contexts
            .selector
            .as_ref()
            .ok_or(Error::Internal("missing RDMA device selector"))?
            .allocate(
                length,
                self.config.preferred_numa_node,
                mask & self.config.device_mask,
            )
    }

    fn device_at(&self, id: usize) -> Result<Arc<RdmaContext>> {
        let contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        if contexts.closed {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        if let Some((operation, code)) = contexts.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        contexts
            .slots
            .get(id)
            .and_then(|slot| slot.owner.clone())
            .ok_or(Error::Unavailable("RDMA context slot is disabled"))
    }

    pub(super) fn progress(&self, device: Arc<RdmaContext>) -> Result<Arc<workers::Workers>> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        self.initialize_contexts(&mut contexts)?;
        let slot = contexts
            .slots
            .iter_mut()
            .find(|slot| {
                slot.owner
                    .as_ref()
                    .is_some_and(|owner| Arc::ptr_eq(owner, &device))
            })
            .ok_or(Error::Internal(
                "RDMA progress context is not owned by this transport",
            ))?;
        if let Some(progress) = &slot.progress {
            return Ok(Arc::clone(progress));
        }
        match workers::Workers::new(&self.config, device, Arc::clone(&self.usage), self.verbs) {
            Ok(progress) => {
                slot.progress = Some(Arc::clone(&progress));
                Ok(progress)
            }
            Err(error) => {
                if let Error::Unquiesced { operation, code } = error {
                    contexts.failure = Some((operation, code));
                }
                Err(error)
            }
        }
    }

    fn evict_idle(&self, deadline: Instant) -> Result<bool> {
        let workers: Vec<_> = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?
            .slots
            .iter()
            .filter_map(|slot| slot.progress.clone())
            .collect();
        for progress in workers {
            if progress.evict_idle(deadline)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn close(&self, deadline: Instant) -> Result<()> {
        let (owners, failure) = {
            let mut contexts = self
                .context_set
                .lock()
                .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
            contexts.closed = true;
            (
                contexts
                    .slots
                    .iter()
                    .map(|slot| (slot.owner.clone(), slot.progress.clone()))
                    .collect::<Vec<_>>(),
                contexts.failure,
            )
        };
        let mut error = None;
        for (_, progress) in &owners {
            if let Some(progress) = progress {
                if let Err(fault) = progress.fence(deadline) {
                    error.get_or_insert(fault);
                }
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        // A partially constructed retained CQ can own a PD without a Workers
        // handle in its slot. Fencing siblings cannot prove that PD quiescent.
        if let Some((operation, code)) = failure {
            return Err(Error::Unquiesced { operation, code });
        }
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        loop {
            if usage.retained_endpoints != 0 || usage.retained_grants != 0 {
                return Err(Error::Unquiesced {
                    operation: "RDMA endpoint resources retained",
                    code: libc::EIO,
                });
            }
            if usage.active_endpoints == 0 && usage.active_grants == 0 {
                break;
            }
            usage = self
                .drained
                .wait_timeout(usage, remaining(deadline)?)
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                .0;
        }
        drop(usage);
        self.registrations.close(deadline)?;
        for (owner, progress) in owners {
            if let Some(progress) = progress {
                if let Err(fault) = progress.close(deadline) {
                    error.get_or_insert(fault);
                    continue;
                }
            }
            if let Some(owner) = owner {
                if let Err(fault) = owner.close() {
                    error.get_or_insert(fault);
                }
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        contexts.slots.clear();
        contexts.topology = None;
        contexts.selector = None;
        Ok(())
    }

    pub(super) fn endpoint_on(
        &self,
        selection: DeviceLease,
        memory: &RegisteredMemory,
        range: std::ops::Range<usize>,
        permission: u32,
        deadline: Instant,
        reuse: Option<RdmaEndpoint>,
    ) -> Result<RdmaEndpoint> {
        remaining(deadline)?;
        let offset = range.start;
        let length = range.end.checked_sub(offset).ok_or(Error::InvalidRange)?;
        if length != selection.length() {
            return Err(Error::Internal("RDMA selection length mismatch"));
        }
        if length == 0 || length > u32::MAX as usize {
            return Err(Error::InvalidRange);
        }
        crate::memory::checked_range(offset, length, memory.len())?;
        let direct = if self.config.host_memory == RdmaHostMemory::Direct {
            memory.host_dma_lease(offset, length)?
        } else {
            None
        };
        let is_direct = direct.is_some();
        if reuse.is_some() && !is_direct {
            return Err(Error::Protocol(
                "RDMA reuse requires host Direct on both peers",
            ));
        }
        let grant_lease = {
            let mut usage = self
                .usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            let (charged, limit) = if is_direct {
                (usage.direct_bytes, self.config.max_direct_bytes)
            } else {
                (usage.staging_bytes, self.config.max_staging_bytes)
            };
            let bytes = charged
                .checked_add(length)
                .ok_or(Error::ResourceExhausted)?;
            if usage.retained_endpoints != 0
                || usage.retained_grants != 0
                || usage.retained_devices != 0
                || usage.retained_cqs != 0
                || usage.retained_completion_channels != 0
                || usage.retained_persistent_mrs != 0
                || usage.progress_failures != 0
            {
                return Err(Error::Unavailable(
                    "RDMA admission is poisoned by retained native resources",
                ));
            }
            if bytes > limit {
                return Err(Error::ResourceExhausted);
            }
            usage.active_grants += 1;
            if is_direct {
                usage.direct_bytes = bytes;
            } else {
                usage.staging_bytes = bytes;
            }
            GrantLease {
                usage: Arc::clone(&self.usage),
                length,
                is_direct,
                drained: Arc::clone(&self.drained),
                _selection: selection,
            }
        };
        let buffer = match direct {
            Some(loan) => Buffer::Direct {
                address: loan.address(),
                length: loan.len(),
                loan: Some(loan),
            },
            None => Buffer::Staged(Staging::new(length)?),
        };
        let device = self.device_at(grant_lease._selection.device_id())?;
        let progress = self.progress(Arc::clone(&device))?;
        let new_connection_lease = if reuse.is_none() {
            Some(loop {
                let mut usage = self
                    .usage
                    .lock()
                    .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
                if usage.active_endpoints < self.config.max_endpoints {
                    usage.active_endpoints += 1;
                    break ConnectionLease {
                        usage: Arc::clone(&self.usage),
                        drained: Arc::clone(&self.drained),
                    };
                }
                drop(usage);
                if !self.evict_idle(deadline)? {
                    return Err(Error::ResourceExhausted);
                }
            })
        } else {
            None
        };
        let cached_mr = if is_direct {
            Some(self.registrations.acquire(
                Arc::clone(&device),
                memory,
                permission != 0,
                deadline,
            )?)
        } else {
            None
        };
        let mut memory_handles = sys::MemoryHandles::default();
        if let Some(registration) = &cached_mr {
            memory_handles.mr = registration.handle()?;
        }
        let mut grant = GrantResources {
            device: Arc::clone(&device),
            handles: memory_handles,
            cached_mr,
            buffer,
            _lease: grant_lease,
            max_read_slices: if memory.is_cuda() { 32 } else { 64 },
            permission,
            generation: 0,
            rkey: 0,
            connection: None,
            window_used: false,
            window_bound: false,
            message: None,
        };
        let mut endpoint = if let Some(endpoint) = reuse {
            {
                let mut request = endpoint.record.lock()?;
                request.check_claim(endpoint.claim)?;
                let resources = request
                    .resources
                    .as_mut()
                    .ok_or(Error::Internal("missing cached connection"))?;
                if resources.grant.is_some() || !Arc::ptr_eq(&resources.connection.device, &device)
                {
                    return Err(Error::Internal("RDMA reuse grant ownership"));
                }
                if resources.connection.permission != permission {
                    return Err(Error::Protocol("RDMA reuse changed QP capability"));
                }
                grant.connection = resources.connection.incarnation;
                resources.grant = Some(grant);
            }
            endpoint
        } else {
            let connection_lease = new_connection_lease
                .ok_or(Error::Internal("missing physical connection reservation"))?;
            let connection_handles = sys::ConnectionHandles {
                cq: progress.handle(false)?,
                receive_cq: progress.handle(true)?,
                ..Default::default()
            };
            let record = progress.register(Box::new(Resources {
                connection: ConnectionResources {
                    device,
                    handles: connection_handles,
                    incarnation: None,
                    local: Descriptor::default(),
                    remote: None,
                    permission,
                    closed: false,
                    lease: Some(connection_lease),
                },
                grant: Some(grant),
            }))?;
            RdmaEndpoint {
                record,
                progress,
                local: Descriptor::default(),
                incarnation: None,
                config: self.config.clone(),
                deadline,
                output: None,
                claim: 1,
                released: false,
                cached_peer: None,
            }
        };
        #[cfg(test)]
        super::tests::before_reused_open(self, &endpoint);
        let epoch = SourceEpoch::generate()?;
        let psn = u32::from_be_bytes([0, epoch.0[0], epoch.0[1], epoch.0[2]]);
        if let Err(error) = endpoint.open(permission, psn) {
            endpoint.close()?;
            return Err(error);
        }
        if is_direct {
            self.usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                .opened_direct_endpoints += 1;
        }
        Ok(endpoint)
    }

    fn host_direct(&self, memory: &RegisteredMemory) -> bool {
        self.config.host_memory == RdmaHostMemory::Direct && !memory.is_cuda()
    }
}

const IDENTITY_SIZE: usize = 40;
const HELLO_SIZE: usize = 1 + 2 * IDENTITY_SIZE + 16 + 8;
const OFFER_SIZE: usize = IDENTITY_SIZE + 8 + DESCRIPTOR_SIZE + 1;
const BINDING_SIZE: usize = REQUEST_SIZE + 2 * IDENTITY_SIZE + 8 + 1;
const _: () = assert!(BINDING_SIZE == super::COMPLETION_MESSAGE_SIZE);
const ACCEPT: u8 = 1;
const GRANT: u8 = 2;
const DONE: u8 = 3;
const ACK: u8 = 4;
const READY: u8 = 5;
const CACHE_MISS: u8 = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
    epoch: SourceEpoch,
    connection: ConnectionId,
}

impl Identity {
    fn local(endpoint: &RdmaEndpoint, epoch: SourceEpoch) -> Result<Self> {
        Ok(Self {
            epoch,
            connection: endpoint.incarnation()?,
        })
    }

    fn encode(self) -> [u8; IDENTITY_SIZE] {
        let mut bytes = [0; IDENTITY_SIZE];
        bytes[..16].copy_from_slice(&self.epoch.0);
        bytes[16..].copy_from_slice(&self.connection.encode());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != IDENTITY_SIZE {
            return Err(Error::Protocol("RDMA identity size"));
        }
        let mut epoch = [0; 16];
        epoch.copy_from_slice(&bytes[..16]);
        if epoch == [0; 16] {
            return Err(Error::Protocol("zero RDMA process identity"));
        }
        Ok(Self {
            epoch: SourceEpoch(epoch),
            connection: ConnectionId::decode(&bytes[16..])?,
        })
    }
}

/// Rendezvous both persistent connection owners before creating any authority.
struct Hello {
    requester: Identity,
    source: Option<Identity>,
    expected_domain: [u8; 16],
    direct: bool,
    previous_grant: u64,
}

impl Hello {
    fn write(&self, channel: &mut ControlChannel<'_>) -> Result<()> {
        let mut bytes = [0; HELLO_SIZE];
        bytes[0] = u8::from(self.direct) | (u8::from(self.source.is_some()) << 1);
        bytes[1..1 + IDENTITY_SIZE].copy_from_slice(&self.requester.encode());
        if let Some(source) = self.source {
            bytes[1 + IDENTITY_SIZE..1 + 2 * IDENTITY_SIZE].copy_from_slice(&source.encode());
        }
        bytes[HELLO_SIZE - 24..HELLO_SIZE - 8].copy_from_slice(&self.expected_domain);
        bytes[HELLO_SIZE - 8..].copy_from_slice(&self.previous_grant.to_be_bytes());
        channel.write_all(&bytes)
    }

    fn read(channel: &mut ControlChannel<'_>, epoch: SourceEpoch) -> Result<Self> {
        let mut bytes = [0; HELLO_SIZE];
        channel.read_exact(&mut bytes)?;
        if bytes[0] & !3 != 0 {
            return Err(Error::Protocol("RDMA rendezvous flags"));
        }
        let requester = Identity::decode(&bytes[1..1 + IDENTITY_SIZE])?;
        let source_bytes = &bytes[1 + IDENTITY_SIZE..1 + 2 * IDENTITY_SIZE];
        let source = if bytes[0] & 2 != 0 {
            let source = Identity::decode(source_bytes)?;
            if source.epoch != epoch {
                return Err(Error::EpochMismatch);
            }
            if bytes[0] & 1 == 0 {
                return Err(Error::Protocol("RDMA reuse without host Direct"));
            }
            Some(source)
        } else {
            if source_bytes.iter().any(|b| *b != 0) {
                return Err(Error::Protocol(
                    "RDMA fresh rendezvous has a cached identity",
                ));
            }
            None
        };
        let mut expected_domain = [0; 16];
        expected_domain.copy_from_slice(&bytes[HELLO_SIZE - 24..HELLO_SIZE - 8]);
        let previous_grant = u64_at(&bytes, HELLO_SIZE - 8);
        if source.is_some() != (previous_grant != 0) {
            return Err(Error::Protocol("RDMA rendezvous grant generation"));
        }
        if source.is_some_and(|s| s.connection.domain != expected_domain) {
            return Err(Error::Protocol("RDMA rendezvous changed its PD domain"));
        }
        Ok(Self {
            requester,
            source,
            expected_domain,
            direct: bytes[0] & 1 != 0,
            previous_grant,
        })
    }
}

/// Exact request, native pair, generation and negotiated release profile.
struct Binding([u8; BINDING_SIZE]);

/// Constructed only after a successful bytewise bound DONE on the control channel.
pub(super) struct BoundDone([u8; BINDING_SIZE]);
impl BoundDone {
    pub(super) fn binding(&self) -> &[u8; BINDING_SIZE] {
        &self.0
    }
}

impl Binding {
    fn new(
        request: &Request,
        source: Identity,
        requester: Identity,
        grant: u64,
        cacheable: bool,
    ) -> Result<Self> {
        if grant == 0 {
            return Err(Error::Protocol("zero RDMA grant generation"));
        }
        let mut bytes = [0; BINDING_SIZE];
        bytes[..REQUEST_SIZE].copy_from_slice(&request.encode());
        bytes[REQUEST_SIZE..REQUEST_SIZE + IDENTITY_SIZE].copy_from_slice(&source.encode());
        bytes[REQUEST_SIZE + IDENTITY_SIZE..REQUEST_SIZE + 2 * IDENTITY_SIZE]
            .copy_from_slice(&requester.encode());
        bytes[BINDING_SIZE - 9..BINDING_SIZE - 1].copy_from_slice(&grant.to_be_bytes());
        bytes[BINDING_SIZE - 1] = u8::from(cacheable);
        Ok(Self(bytes))
    }

    fn accept(
        channel: &mut ControlChannel<'_>,
        request: &Request,
        source: Identity,
        requester: Identity,
        grant: u64,
        cacheable: bool,
    ) -> Result<Self> {
        let binding = Self::new(request, source, requester, grant, cacheable)?;
        binding.read(channel, ACCEPT)?;
        Ok(binding)
    }

    fn write(&self, channel: &mut ControlChannel<'_>, phase: u8) -> Result<()> {
        let mut bytes = [0; 1 + BINDING_SIZE];
        bytes[0] = phase;
        bytes[1..].copy_from_slice(&self.0);
        channel.write_all(&bytes)
    }

    fn read(&self, channel: &mut ControlChannel<'_>, phase: u8) -> Result<()> {
        let mut bytes = [0; 1 + BINDING_SIZE];
        channel.read_exact(&mut bytes)?;
        if bytes[0] != phase || bytes[1..] != self.0 {
            return Err(Error::Protocol("RDMA phase or grant binding mismatch"));
        }
        Ok(())
    }

    fn done(&self, channel: &mut ControlChannel<'_>) -> Result<BoundDone> {
        self.read(channel, DONE)?;
        Ok(BoundDone(self.0))
    }
}

impl RdmaTransport {
    pub(crate) fn transfer(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        deadline: Instant,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut effect = RemoteEffect::None;
        let result = (|| {
            let direct = self.host_direct(&request.local);
            let wire = Request {
                operation: if operation == TransferOp::Read {
                    Operation::RdmaRead
                } else {
                    Operation::RdmaWrite
                },
                epoch: peer.epoch,
                region: request.remote.id,
                offset: request.remote_offset as u64,
                length: request.length as u64,
            };
            let mut pinned = None;
            for attempt in 0..2 {
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                channel.write_all(&wire.encode())?;
                let mut remote_paths = rail_monitor::read_paths(&mut channel, peer.epoch)?;
                let mut local_paths = self.paths()?;
                if let Some((local_domain, remote_domain)) = pinned {
                    local_paths.retain(|path| path.domain == local_domain);
                    remote_paths.retain(|path| path.domain == remote_domain);
                }
                let selection = self.allocate(
                    request.length,
                    rail_monitor::local_device_mask(&local_paths, &remote_paths),
                )?;
                let local_path = local_paths
                    .iter()
                    .find(|path| path.nic_id == selection.device_id())
                    .ok_or(Error::Internal("RDMA selector returned a masked NIC path"))?;
                let remote_path = rail_monitor::remote_path(local_path, &remote_paths)?;
                let expected_domain = remote_path.domain;
                pinned = Some((local_path.domain, expected_domain));
                let device = self.device_at(selection.device_id())?;
                device.check_address()?;
                let progress = self.progress(device)?;
                let mut reuse = if direct && attempt == 0 {
                    progress.claim(
                        |p| {
                            !p.source
                                && p.route == Some(peer.endpoint)
                                && p.epoch == peer.epoch
                                && p.operation == operation
                                && p.source_nic_id == remote_path.nic_id
                        },
                        deadline,
                    )?
                } else {
                    None
                };
                if reuse
                    .as_ref()
                    .and_then(|endpoint| endpoint.cached_peer)
                    .is_some_and(|p| p.connection.domain != expected_domain)
                {
                    if let Some(endpoint) = &mut reuse {
                        endpoint.close()?;
                    }
                    return Err(Error::Protocol(
                        "RDMA path table replaced a cached protection domain",
                    ));
                }
                let mut endpoint = self.endpoint_on(
                    selection,
                    &request.local,
                    request.local_offset..request.local_offset + request.length,
                    0,
                    deadline,
                    reuse,
                )?;
                let cached = endpoint.cached_peer;
                let mut miss = false;
                let transfer = (|| {
                    if operation == TransferOp::Write {
                        endpoint.fill(&request.local, request.local_offset)?;
                    }
                    Hello {
                        requester: Identity::local(&endpoint, epoch)?,
                        source: cached.map(|p| Identity {
                            epoch: p.epoch,
                            connection: p.connection,
                        }),
                        expected_domain,
                        direct,
                        previous_grant: cached.map_or(0, |p| p.generation),
                    }
                    .write(&mut channel)?;
                    let reply = Reply::read(&mut channel, peer.epoch)?;
                    if reply.status == CACHE_MISS
                        && reply.length == 0
                        && cached.is_some()
                        && attempt == 0
                    {
                        miss = true;
                        return Ok(());
                    }
                    if reply.status != 0 || reply.length != OFFER_SIZE as u64 {
                        return Err(Error::Protocol("RDMA bootstrap length/status"));
                    }
                    let mut offer = [0; OFFER_SIZE];
                    channel.read_exact(&mut offer)?;
                    let remote_identity = Identity::decode(&offer[..IDENTITY_SIZE])?;
                    if remote_identity.epoch != peer.epoch {
                        return Err(Error::EpochMismatch);
                    }
                    if expected_domain != [0; 16]
                        && remote_identity.connection.domain != expected_domain
                    {
                        return Err(Error::Protocol("RDMA source changed its protection domain"));
                    }
                    if cached.is_some_and(|p| remote_identity.connection != p.connection) {
                        return Err(Error::Protocol("RDMA source changed cached connection"));
                    }
                    let cacheable = match offer[OFFER_SIZE - 1] {
                        0 if cached.is_none() => false,
                        1 if direct => true,
                        _ => return Err(Error::Protocol("RDMA release profile mismatch")),
                    };
                    let grant = u64_at(&offer, IDENTITY_SIZE);
                    if cached.is_some_and(|p| grant <= p.generation) {
                        return Err(Error::Protocol(
                            "RDMA source reused an old grant generation",
                        ));
                    }
                    let mut descriptor = [0; DESCRIPTOR_SIZE];
                    descriptor.copy_from_slice(&offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1]);
                    let connection = Descriptor::decode_connection(&descriptor)?;
                    if !remote_path.matches_connection(&connection) {
                        return Err(Error::Protocol(
                            "RDMA source changed its advertised NIC path",
                        ));
                    }
                    let binding = Binding::new(
                        &wire,
                        remote_identity,
                        Identity::local(&endpoint, epoch)?,
                        grant,
                        cacheable,
                    )?;
                    binding.write(&mut channel, ACCEPT)?;
                    channel.write_all(&endpoint.connection())?;
                    endpoint.connect(&connection)?;
                    let ready = Reply::read(&mut channel, peer.epoch)?;
                    if ready.status != 9 || ready.length != 0 {
                        return Err(Error::Protocol("RDMA missing ready response"));
                    }
                    binding.read(&mut channel, GRANT)?;
                    channel.read_exact(&mut descriptor)?;
                    let remote = Descriptor::decode(&descriptor)?;
                    if remote.connection_bytes() != connection.connection_bytes() {
                        return Err(Error::Protocol("RDMA grant changed its connection"));
                    }
                    // A WRITE can change direct application bytes before DONE/ACK.
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                    }
                    endpoint.transfer(&remote, operation, deadline)?;
                    endpoint.send_completion(&binding.0, remote.rkey)?;
                    if cacheable {
                        endpoint.finish_sent(&binding.0)?;
                    } else {
                        endpoint.close()?;
                    }
                    binding.write(&mut channel, DONE)?;
                    let receipt = Reply::read(&mut channel, peer.epoch)?;
                    if receipt.status != 0 || receipt.length != 0 {
                        return Err(Error::Protocol("RDMA completion acknowledgement"));
                    }
                    binding.read(&mut channel, ACK)?;
                    if cacheable {
                        binding.read(&mut channel, READY)?;
                        endpoint.cache(Peer {
                            route: Some(peer.endpoint),
                            source_nic_id: remote_path.nic_id,
                            epoch: peer.epoch,
                            local: endpoint.incarnation()?,
                            connection: remote_identity.connection,
                            operation,
                            source: false,
                            generation: grant,
                        })?;
                    }
                    Ok(())
                })();
                #[cfg(test)]
                crate::transport::rdma::tests::before_requester_close(&mut endpoint, &transfer);
                endpoint.close()?;
                transfer?;
                if miss {
                    // Only an explicit pre-authority cache miss can start a fresh pair.
                    drop(endpoint);
                    continue;
                }
                if operation == TransferOp::Read && !endpoint.released {
                    remaining(deadline)?;
                    endpoint.copy_back(&request.local, request.local_offset)?;
                }
                return Ok(());
            }
            Err(Error::Protocol("RDMA repeated cache miss"))
        })();
        result.map_err(|error| (error, effect))
    }

    pub(crate) fn serve(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &crate::RegisteredMemory,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let writing = request.operation == Operation::RdmaWrite;
        let operation = if writing {
            TransferOp::Write
        } else {
            TransferOp::Read
        };
        let paths = self.paths()?;
        rail_monitor::write_paths(channel, epoch, &paths)?;
        let hello = Hello::read(channel, epoch)?;
        let cacheable = hello.direct && self.host_direct(memory);
        let mask = paths
            .iter()
            .filter(|path| path.domain == hello.expected_domain)
            .fold(0, |mask, path| mask | (1u64 << path.nic_id));
        if mask == 0 {
            return Err(Error::Protocol(
                "RDMA rendezvous protection domain mismatch",
            ));
        }
        let selection = self.allocate(length, mask)?;
        let source_nic_id = selection.device_id();
        let device = self.device_at(source_nic_id)?;
        device.check_address()?;
        let progress = self.progress(device)?;
        let reuse = if let Some(source) = hello.source {
            if !cacheable {
                return channel.reply(epoch, CACHE_MISS, 0);
            }
            let reuse = progress.claim(
                |p| {
                    p.source
                        && p.local == source.connection
                        && p.epoch == hello.requester.epoch
                        && p.connection == hello.requester.connection
                        && p.operation == operation
                        && p.generation == hello.previous_grant
                },
                channel.deadline,
            )?;
            if reuse.is_none() {
                return channel.reply(epoch, CACHE_MISS, 0);
            }
            reuse
        } else {
            None
        };
        let mut endpoint = self.endpoint_on(
            selection,
            memory,
            offset..offset + length,
            if writing { 2 } else { 1 },
            channel.deadline,
            reuse,
        )?;
        let transfer: Result<Binding> = (|| {
            if !writing {
                endpoint.fill(memory, offset)?;
            }
            let identity = Identity::local(&endpoint, epoch)?;
            let grant = endpoint.grant_generation()?;
            let mut offer = [0; OFFER_SIZE];
            offer[..IDENTITY_SIZE].copy_from_slice(&identity.encode());
            offer[IDENTITY_SIZE..IDENTITY_SIZE + 8].copy_from_slice(&grant.to_be_bytes());
            offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1].copy_from_slice(&endpoint.connection());
            offer[OFFER_SIZE - 1] = u8::from(cacheable);
            channel.reply(epoch, 0, OFFER_SIZE as u64)?;
            channel.write_all(&offer)?;
            let binding = Binding::accept(
                channel,
                request,
                identity,
                hello.requester,
                grant,
                cacheable,
            )?;
            let mut bytes = [0; DESCRIPTOR_SIZE];
            channel.read_exact(&mut bytes)?;
            endpoint.connect(&Descriptor::decode_connection(&bytes)?)?;
            endpoint.bind_window()?;
            endpoint.post_receive(&binding.0)?;
            channel.reply(epoch, 9, 0)?;
            binding.write(channel, GRANT)?;
            channel.write_all(&endpoint.grant()?.encode())?;
            let done = binding.done(channel)?;
            endpoint.receive_completion()?;
            if cacheable {
                endpoint.finish_received(&done)?;
            }
            Ok(binding)
        })();
        let closed = if cacheable && transfer.is_ok() {
            Ok(())
        } else {
            endpoint.close()
        };
        if matches!(&closed, Err(Error::Unquiesced { .. })) {
            return closed;
        }
        let binding = transfer?;
        closed?;
        if !cacheable {
            if writing {
                remaining(channel.deadline)?;
                endpoint.copy_back(memory, offset)?;
            }
            drop(endpoint.output.take());
        }
        #[cfg(test)]
        crate::transport::rdma::tests::before_source_ack(self);
        channel.reply(epoch, 0, 0)?;
        binding.write(channel, ACK)?;
        if cacheable {
            endpoint.cache(Peer {
                route: None,
                source_nic_id,
                epoch: hello.requester.epoch,
                local: endpoint.incarnation()?,
                connection: hello.requester.connection,
                operation,
                source: true,
                generation: u64_at(&binding.0, BINDING_SIZE - 9),
            })?;
            if let Err(error) = binding.write(channel, READY) {
                endpoint.retire_idle()?;
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/rdma_transport.rs"]
mod tests;
