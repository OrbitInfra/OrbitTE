use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::endpoint_store::Peer;
use super::{
    RdmaConfig, RdmaHostMemory, RdmaStats, buffers,
    context_set::ContextSet,
    endpoint::{
        Buffer, ConnectionLease, ConnectionResources, GrantLease, GrantResources, Resources,
        Staging,
    },
    events, quota,
    quota::DeviceLease,
    rail_monitor::{self, NicPath},
    slice::{RdmaSlice, plan_rdma_slices},
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

pub(crate) struct RdmaTransport {
    pub(super) config: RdmaConfig,
    pub(super) usage: Arc<Mutex<RdmaStats>>,
    pub(super) counters: Arc<events::Counters>,
    pub(super) workers: Arc<workers::Workers>,
    pub(super) cq_quota: Arc<quota::CqQuota>,
    pub(super) verbs: &'static sys::Verbs,
    pub(super) context_set: Mutex<ContextSet>,
    pub(super) drained: Arc<Condvar>,
    pub(crate) registrations: Arc<buffers::LocalBufferManager>,
}

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
            workers: Arc::default(),
            cq_quota: Arc::default(),
            verbs: &sys::REAL,
            context_set: Mutex::default(),
            drained: Arc::default(),
            registrations,
        }))
    }

    pub(crate) fn register_memory(
        &self,
        memory: &[RegisteredMemory],
        deadline: Instant,
    ) -> Result<()> {
        if memory.is_empty() || self.config.host_memory != RdmaHostMemory::Direct {
            return Ok(());
        }
        let devices = {
            let mut contexts = self
                .context_set
                .lock()
                .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
            self.initialize_contexts(&mut contexts)?;
            let mut devices = Vec::new();
            let mut available = 0;
            for (id, slot) in contexts.slots.iter().enumerate() {
                if let Some(owner) = &slot.owner {
                    owner.asynchronous.check()?;
                    available |= 1u64 << id;
                    devices.push(Arc::clone(owner));
                }
            }
            if available & self.config.device_mask == 0 {
                return Err(Error::Unavailable(
                    "no enabled RDMA context matches device mask",
                ));
            }
            devices
        };
        self.registrations.publish(&devices, memory, deadline)
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
                || usage.progress_failures != usage.progress_recoveries
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
        let (progress, worker_lease) = if let Some(endpoint) = &reuse {
            (
                Arc::clone(&endpoint.progress),
                self.workers.reserve(&endpoint.progress)?,
            )
        } else {
            self.workers.select(&self.cq_list(Arc::clone(&device))?)?
        };
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
            _worker: worker_lease,
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
pub(super) struct Binding([u8; BINDING_SIZE]);

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

// Slice descriptors are relative to the parent range; identities stay NicID/PD-bound.
struct SliceRequest {
    offset: usize,
    length: usize,
    domain: [u8; 16],
}

impl SliceRequest {
    fn write(&self, channel: &mut ControlChannel<'_>) -> Result<()> {
        let mut bytes = [0; 24];
        bytes[..4].copy_from_slice(&(self.offset as u32).to_be_bytes());
        bytes[4..8].copy_from_slice(&(self.length as u32).to_be_bytes());
        bytes[8..].copy_from_slice(&self.domain);
        channel.write_all(&bytes)
    }

    fn read_plan(
        channel: &mut ControlChannel<'_>,
        length: usize,
        limit: u8,
        paths: &[NicPath],
    ) -> Result<Vec<Self>> {
        let mut count = [0];
        channel.read_exact(&mut count)?;
        if count[0] == 0 || count[0] > limit {
            return Err(Error::Protocol("RDMA slice count exceeds its grant window"));
        }
        let mut slices = Vec::new();
        slices
            .try_reserve_exact(count[0] as usize)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut end = 0usize;
        for _ in 0..count[0] {
            let mut bytes = [0; 24];
            channel.read_exact(&mut bytes)?;
            let offset = u32::from_be_bytes(
                bytes[..4]
                    .try_into()
                    .map_err(|_| Error::Internal("RDMA slice offset size"))?,
            ) as usize;
            let size = u32::from_be_bytes(
                bytes[4..8]
                    .try_into()
                    .map_err(|_| Error::Internal("RDMA slice length size"))?,
            ) as usize;
            let mut domain = [0; 16];
            domain.copy_from_slice(&bytes[8..]);
            if offset != end || size == 0 || !paths.iter().any(|path| path.domain == domain) {
                return Err(Error::Protocol("RDMA slice plan range or PD identity"));
            }
            end = offset
                .checked_add(size)
                .filter(|end| *end <= length)
                .ok_or(Error::Protocol("RDMA slice plan exceeds parent range"))?;
            slices.push(Self {
                offset,
                length: size,
                domain,
            });
        }
        if end != length {
            return Err(Error::Protocol(
                "RDMA slice plan does not cover parent range",
            ));
        }
        Ok(slices)
    }
}

fn data_request(
    operation: TransferOp,
    peer: &RemoteSegment,
    request: &PreparedTransfer,
) -> Request {
    Request {
        operation: if operation == TransferOp::Read {
            Operation::RdmaRead
        } else {
            Operation::RdmaWrite
        },
        epoch: peer.epoch,
        region: request.remote.id,
        offset: request.remote_offset as u64,
        length: request.length as u64,
    }
}

const ENDPOINT_DESTROY_TIMEOUT: Duration = Duration::from_secs(30);

fn close_slices(slices: &mut [RdmaSlice], mut result: Result<()>, requester: bool) -> Result<()> {
    let retirement_deadline = Instant::now() + ENDPOINT_DESTROY_TIMEOUT;
    for slice in slices {
        slice.endpoint.deadline = retirement_deadline;
        #[cfg(test)]
        if requester {
            super::tests::before_requester_close(&mut slice.endpoint, &result);
        }
        let _ = requester;
        if let Err(error) = slice.endpoint.close() {
            if result.is_ok() || matches!(error, Error::Unquiesced { .. }) {
                result = Err(error);
            }
        }
    }
    result
}

impl RdmaTransport {
    fn requester_endpoint(
        &self,
        channel: &ControlChannel<'_>,
        wire: &Request,
        request: &PreparedTransfer,
        selection: DeviceLease,
        remote_path: &NicPath,
        reuse_pair: bool,
    ) -> Result<RdmaEndpoint> {
        let route = channel.stream.peer_addr()?;
        let operation = if wire.operation == Operation::RdmaRead {
            TransferOp::Read
        } else {
            TransferOp::Write
        };
        let direct = self.host_direct(&request.local);
        let device = self.device_at(selection.device_id())?;
        device.check_address()?;
        let mut reuse = if direct && reuse_pair {
            self.claim(
                device,
                |peer| {
                    !peer.source
                        && peer.route == Some(route)
                        && peer.epoch == wire.epoch
                        && peer.operation == operation
                        && peer.source_nic_id == remote_path.nic_id
                },
                channel.deadline,
            )?
        } else {
            None
        };
        if reuse
            .as_ref()
            .and_then(|endpoint| endpoint.cached_peer)
            .is_some_and(|peer| peer.connection.domain != remote_path.domain)
        {
            if let Some(endpoint) = &mut reuse {
                endpoint.close()?;
            }
            return Err(Error::Protocol(
                "RDMA path table replaced a cached protection domain",
            ));
        }
        let endpoint = self.endpoint_on(
            selection,
            &request.local,
            request.local_offset..request.local_offset + request.length,
            0,
            channel.deadline,
            reuse,
        )?;
        Ok(endpoint)
    }

    fn requester_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        wire: &Request,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        remote_path: &NicPath,
        mut endpoint: RdmaEndpoint,
    ) -> Result<Option<RdmaSlice>> {
        let route = channel.stream.peer_addr()?;
        let operation = if wire.operation == Operation::RdmaRead {
            TransferOp::Read
        } else {
            TransferOp::Write
        };
        let direct = self.host_direct(&request.local);
        let cached = endpoint.cached_peer;
        let prepared = (|| {
            if operation == TransferOp::Write {
                endpoint.fill(&request.local, request.local_offset)?;
            }
            Hello {
                requester: Identity::local(&endpoint, epoch)?,
                source: cached.map(|peer| Identity {
                    epoch: peer.epoch,
                    connection: peer.connection,
                }),
                expected_domain: remote_path.domain,
                direct,
                previous_grant: cached.map_or(0, |peer| peer.generation),
            }
            .write(channel)?;
            let reply = Reply::read(channel, wire.epoch)?;
            if reply.status == CACHE_MISS && reply.length == 0 && cached.is_some() {
                return Ok(None);
            }
            if reply.status != 0 || reply.length != OFFER_SIZE as u64 {
                return Err(Error::Protocol("RDMA bootstrap length/status"));
            }
            let mut offer = [0; OFFER_SIZE];
            channel.read_exact(&mut offer)?;
            let remote_identity = Identity::decode(&offer[..IDENTITY_SIZE])?;
            if remote_identity.epoch != wire.epoch {
                return Err(Error::EpochMismatch);
            }
            if remote_identity.connection.domain != remote_path.domain {
                return Err(Error::Protocol("RDMA source changed its protection domain"));
            }
            if cached.is_some_and(|peer| remote_identity.connection != peer.connection) {
                return Err(Error::Protocol("RDMA source changed cached connection"));
            }
            let cacheable = match offer[OFFER_SIZE - 1] {
                0 if cached.is_none() => false,
                1 if direct => true,
                _ => return Err(Error::Protocol("RDMA release profile mismatch")),
            };
            let grant = u64_at(&offer, IDENTITY_SIZE);
            if cached.is_some_and(|peer| grant <= peer.generation) {
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
                wire,
                remote_identity,
                Identity::local(&endpoint, epoch)?,
                grant,
                cacheable,
            )?;
            binding.write(channel, ACCEPT)?;
            channel.write_all(&endpoint.connection())?;
            endpoint.connect(&connection)?;
            let ready = Reply::read(channel, wire.epoch)?;
            if ready.status != 9 || ready.length != 0 {
                return Err(Error::Protocol("RDMA missing ready response"));
            }
            binding.read(channel, GRANT)?;
            channel.read_exact(&mut descriptor)?;
            let remote = Descriptor::decode(&descriptor)?;
            if remote.connection_bytes() != connection.connection_bytes() {
                return Err(Error::Protocol("RDMA grant changed its connection"));
            }
            let peer = Peer {
                route: Some(route),
                source_nic_id: remote_path.nic_id,
                epoch: wire.epoch,
                local: endpoint.incarnation()?,
                connection: remote_identity.connection,
                operation,
                source: false,
                generation: grant,
            };
            Ok(Some((binding, remote, peer)))
        })();
        match prepared {
            Ok(Some((binding, remote, peer))) => Ok(Some(RdmaSlice {
                endpoint,
                binding,
                remote,
                peer,
                offset: request.local_offset,
            })),
            result => {
                endpoint.close()?;
                result.map(|_| None)
            }
        }
    }

    fn finish_requester_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        slice: &mut RdmaSlice,
        memory: &RegisteredMemory,
    ) -> Result<()> {
        let endpoint = &mut slice.endpoint;
        endpoint.send_completion(&slice.binding.0, slice.remote.rkey)?;
        let cacheable = slice.binding.0[BINDING_SIZE - 1] == 1;
        if cacheable {
            endpoint.finish_sent(&slice.binding.0)?;
        } else {
            endpoint.close()?;
        }
        slice.binding.write(channel, DONE)?;
        let receipt = Reply::read(channel, slice.peer.epoch)?;
        if receipt.status != 0 || receipt.length != 0 {
            return Err(Error::Protocol("RDMA completion acknowledgement"));
        }
        slice.binding.read(channel, ACK)?;
        if cacheable {
            slice.binding.read(channel, READY)?;
            endpoint.cache(slice.peer)?;
        } else if slice.peer.operation == TransferOp::Read {
            remaining(channel.deadline)?;
            endpoint.copy_back(memory, slice.offset)?;
        }
        drop(endpoint.output.take());
        Ok(())
    }

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
            let local_paths = self.paths()?;
            if (local_paths.len() > 1 || self.config.num_lanes > 1)
                && plan_rdma_slices(request.length, self.config.block_size, 64)?.1 > 1
            {
                return self.transfer_slices(
                    operation,
                    peer,
                    request,
                    epoch,
                    deadline,
                    &mut effect,
                );
            }
            let wire = data_request(operation, peer, request);
            let mut pinned = None;
            for attempt in 0..2 {
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                channel.write_all(&wire.encode())?;
                let mut remote_paths = rail_monitor::read_paths(&mut channel, peer.epoch)?;
                let mut paths = local_paths.clone();
                if let Some((local, remote)) = pinned {
                    paths.retain(|path| path.domain == local);
                    remote_paths.retain(|path| path.domain == remote);
                }
                let selection = self.allocate(
                    request.length,
                    rail_monitor::local_device_mask(&paths, &remote_paths),
                )?;
                let local = paths
                    .iter()
                    .find(|path| path.nic_id == selection.device_id())
                    .ok_or(Error::Internal("RDMA selector returned a masked NIC path"))?;
                let remote = rail_monitor::remote_path(local, &remote_paths)?;
                pinned = Some((local.domain, remote.domain));
                let endpoint = self.requester_endpoint(
                    &channel,
                    &wire,
                    request,
                    selection,
                    remote,
                    attempt == 0,
                )?;
                let Some(mut slice) =
                    self.requester_slice(&mut channel, &wire, request, epoch, remote, endpoint)?
                else {
                    continue;
                };
                let transfer = (|| {
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                    }
                    slice
                        .endpoint
                        .transfer(&slice.remote, operation, deadline)?;
                    self.finish_requester_slice(&mut channel, &mut slice, &request.local)
                })();
                return close_slices(std::slice::from_mut(&mut slice), transfer, true);
            }
            Err(Error::Protocol("RDMA repeated cache miss"))
        })();
        result.map_err(|error| (error, effect))
    }

    fn transfer_slices(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        deadline: Instant,
        effect: &mut RemoteEffect,
    ) -> Result<()> {
        let local_paths = self.paths()?;
        let mut wire = data_request(operation, peer, request);
        wire.operation = if operation == TransferOp::Read {
            Operation::RdmaReadSlices
        } else {
            Operation::RdmaWriteSlices
        };
        let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
        channel.write_all(&wire.encode())?;
        let remote_paths = rail_monitor::read_paths(&mut channel, peer.epoch)?;
        let mut remote_limit = [0];
        channel.read_exact(&mut remote_limit)?;
        if !(1..=64).contains(&remote_limit[0]) {
            return Err(Error::Protocol("RDMA slice window limit"));
        }
        let cap = usize::from(remote_limit[0])
            .min(self.config.max_endpoints)
            .min(self.config.max_cqes)
            .min(
                if operation == TransferOp::Write || request.local.is_cuda() {
                    32
                } else {
                    64
                },
            );
        let (block, count) = plan_rdma_slices(request.length, self.config.block_size, cap)?;
        let mask = rail_monitor::local_device_mask(&local_paths, &remote_paths);
        let mut plan = Vec::new();
        plan.try_reserve_exact(count)
            .map_err(|_| Error::ResourceExhausted)?;
        for index in 0..count {
            let offset = index * block;
            let length = if index + 1 == count {
                request.length - offset
            } else {
                block
            };
            let selection = self.allocate(length, mask)?;
            let local = local_paths
                .iter()
                .find(|path| path.nic_id == selection.device_id())
                .ok_or(Error::Internal("RDMA selector returned a masked NIC path"))?;
            let remote = rail_monitor::remote_path(local, &remote_paths)?;
            plan.push((
                SliceRequest {
                    offset,
                    length,
                    domain: remote.domain,
                },
                selection,
                remote,
                local.nic_id,
            ));
        }
        channel.write_all(&[count as u8])?;
        for (range, _, _, _) in &plan {
            range.write(&mut channel)?;
        }
        let admitted = Reply::read(&mut channel, peer.epoch)?;
        if admitted.status != 9 || admitted.length != 0 {
            return Err(Error::Protocol("RDMA parent receive admission"));
        }
        let mut slices = Vec::new();
        slices
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceExhausted)?;
        let result = (|| {
            for (range, selection, remote, local_id) in plan {
                let child = PreparedTransfer {
                    transport: request.transport,
                    local: request.local.clone(),
                    local_offset: request.local_offset + range.offset,
                    remote: request.remote,
                    remote_offset: request.remote_offset + range.offset,
                    length: range.length,
                };
                let child_wire = data_request(operation, peer, &child);
                let mut selection = Some(selection);
                let mut prepared = None;
                for attempt in 0..2 {
                    let lease = match selection.take() {
                        Some(lease) => lease,
                        None => self.allocate(range.length, 1u64 << local_id)?,
                    };
                    let endpoint = self.requester_endpoint(
                        &channel,
                        &child_wire,
                        &child,
                        lease,
                        remote,
                        attempt == 0,
                    )?;
                    if let Some(slice) = self.requester_slice(
                        &mut channel,
                        &child_wire,
                        &child,
                        epoch,
                        remote,
                        endpoint,
                    )? {
                        prepared = Some(slice);
                        break;
                    }
                }
                slices.push(prepared.ok_or(Error::Protocol("RDMA repeated slice cache miss"))?);
            }
            // Every grant is ready before payload admission. Transport-level
            // workers post and poll all QPs; no per-slice requester thread exists.
            for slice in &slices {
                if operation == TransferOp::Write {
                    *effect = RemoteEffect::Unknown;
                }
                slice.endpoint.progress.submit(
                    &slice.endpoint.record,
                    slice.endpoint.claim,
                    slice.remote,
                    operation,
                    deadline,
                )?;
            }
            for slice in &mut slices {
                slice.endpoint.progress.wait(
                    &slice.endpoint.record,
                    slice.endpoint.claim,
                    deadline,
                )?;
                self.finish_requester_slice(&mut channel, slice, &request.local)?;
            }
            Ok(())
        })();
        close_slices(&mut slices, result, true)
    }

    fn source_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        request: &Request,
        memory: &RegisteredMemory,
        paths: &[NicPath],
        expected: Option<[u8; 16]>,
        credits: &mut super::receive::ReceiveCredits,
    ) -> Result<Option<RdmaSlice>> {
        let epoch = request.epoch;
        let offset = usize::try_from(request.offset).map_err(|_| Error::InvalidRange)?;
        let length = usize::try_from(request.length).map_err(|_| Error::InvalidRange)?;
        let writing = request.operation == Operation::RdmaWrite;
        let operation = if writing {
            TransferOp::Write
        } else {
            TransferOp::Read
        };
        let hello = Hello::read(channel, epoch)?;
        if expected.is_some_and(|domain| domain != hello.expected_domain) {
            return Err(Error::Protocol("RDMA slice rendezvous changed planned PD"));
        }
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
        let reuse = if let Some(source) = hello.source {
            if !cacheable {
                channel.reply(epoch, CACHE_MISS, 0)?;
                return Ok(None);
            }
            let reuse = self.claim(
                device,
                |peer| {
                    peer.source
                        && peer.local == source.connection
                        && peer.epoch == hello.requester.epoch
                        && peer.connection == hello.requester.connection
                        && peer.operation == operation
                        && peer.generation == hello.previous_grant
                },
                channel.deadline,
            )?;
            if reuse.is_none() {
                channel.reply(epoch, CACHE_MISS, 0)?;
                return Ok(None);
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
        let prepared = (|| {
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
            let remote = Descriptor::decode_connection(&bytes)?;
            endpoint.connect(&remote)?;
            endpoint.bind_window()?;
            endpoint.post_receive(&binding.0, credits.take()?)?;
            channel.reply(epoch, 9, 0)?;
            binding.write(channel, GRANT)?;
            channel.write_all(&endpoint.grant()?.encode())?;
            let peer = Peer {
                route: None,
                source_nic_id,
                epoch: hello.requester.epoch,
                local: endpoint.incarnation()?,
                connection: hello.requester.connection,
                operation,
                source: true,
                generation: grant,
            };
            Ok((binding, remote, peer))
        })();
        match prepared {
            Ok((binding, remote, peer)) => Ok(Some(RdmaSlice {
                endpoint,
                binding,
                remote,
                peer,
                offset,
            })),
            Err(error) => {
                endpoint.close()?;
                Err(error)
            }
        }
    }

    fn finish_source_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        memory: &RegisteredMemory,
        slice: &mut RdmaSlice,
    ) -> Result<()> {
        let done = slice.binding.done(channel)?;
        let endpoint = &mut slice.endpoint;
        endpoint.receive_completion()?;
        let cacheable = slice.binding.0[BINDING_SIZE - 1] == 1;
        if cacheable {
            endpoint.finish_received(&done)?;
        } else {
            endpoint.close()?;
            if slice.peer.operation == TransferOp::Write {
                remaining(channel.deadline)?;
                endpoint.copy_back(memory, slice.offset)?;
            }
            drop(endpoint.output.take());
        }
        #[cfg(test)]
        super::tests::before_source_ack(self);
        channel.reply(epoch, 0, 0)?;
        slice.binding.write(channel, ACK)?;
        if cacheable {
            endpoint.cache(slice.peer)?;
            if let Err(error) = slice.binding.write(channel, READY) {
                endpoint.retire_idle()?;
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn serve(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
    ) -> Result<()> {
        if request.epoch != epoch {
            return Err(Error::EpochMismatch);
        }
        let offset = usize::try_from(request.offset).map_err(|_| Error::InvalidRange)?;
        let length = usize::try_from(request.length).map_err(|_| Error::InvalidRange)?;
        crate::memory::checked_range(offset, length, memory.len())?;
        let paths = self.paths()?;
        let multi = matches!(
            request.operation,
            Operation::RdmaReadSlices | Operation::RdmaWriteSlices
        );
        let plan = if multi {
            rail_monitor::write_paths(channel, epoch, &paths)?;
            let limit = self.config.max_endpoints.min(self.config.max_cqes).min(64) as u8;
            channel.write_all(&[limit])?;
            SliceRequest::read_plan(channel, length, limit, &paths)?
        } else {
            Vec::new()
        };
        let mut credits = super::receive::ReceiveCredits::acquire(
            Arc::clone(&self.usage),
            self.config.max_cqes,
            if multi { plan.len() } else { 1 },
        )?;
        if multi {
            channel.reply(epoch, 9, 0)?;
        } else {
            rail_monitor::write_paths(channel, epoch, &paths)?;
        }
        let mut slices = Vec::new();
        let result = (|| {
            if multi {
                slices
                    .try_reserve_exact(plan.len())
                    .map_err(|_| Error::ResourceExhausted)?;
                for range in plan {
                    let child = Request {
                        operation: if request.operation == Operation::RdmaReadSlices {
                            Operation::RdmaRead
                        } else {
                            Operation::RdmaWrite
                        },
                        epoch: request.epoch,
                        region: request.region,
                        offset: request.offset + range.offset as u64,
                        length: range.length as u64,
                    };
                    let mut prepared = None;
                    for _ in 0..2 {
                        if let Some(slice) = self.source_slice(
                            channel,
                            &child,
                            memory,
                            &paths,
                            Some(range.domain),
                            &mut credits,
                        )? {
                            prepared = Some(slice);
                            break;
                        }
                    }
                    slices.push(prepared.ok_or(Error::Protocol("RDMA repeated slice cache miss"))?);
                }
            } else if let Some(slice) =
                self.source_slice(channel, request, memory, &paths, None, &mut credits)?
            {
                slices.push(slice);
            } else {
                return Ok(());
            }
            for slice in &mut slices {
                self.finish_source_slice(channel, epoch, memory, slice)?;
            }
            Ok(())
        })();
        close_slices(&mut slices, result, false)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/rdma_transport.rs"]
mod tests;
