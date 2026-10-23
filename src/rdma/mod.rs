//! Opt-in single-rail RC verbs with owned direct host registration or staging.

#![allow(unsafe_code)]

use std::ptr::NonNull;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::engine::remaining;
use crate::{Error, RegisteredMemory, Result, SourceEpoch, TransferOp};

mod cq;
mod device;
mod events;
mod keys;
mod progress;
mod registration;
mod sys;

use device::Device;

pub(crate) const DESCRIPTOR_SIZE: usize = 40;
pub(super) const WR_ID_STRIDE: u64 = 128;

/// Host-memory policy. Device allocations continue to use owned host staging.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RdmaHostMemory {
    /// Register the actual host range. Registration errors never select staging.
    #[default]
    Direct,
    /// Isolate application bytes in private host staging until completion.
    Staged,
}

/// Explicit RNIC selection. This profile creates an RC endpoint per request.
#[derive(Clone, Debug)]
pub struct RdmaConfig {
    pub device: String,
    pub port: u8,
    pub gid_index: u8,
    pub traffic_class: u8,
    pub hop_limit: u8,
    pub max_endpoints: usize,
    pub max_staging_bytes: usize,
    pub host_memory: RdmaHostMemory,
    pub max_direct_bytes: usize,
    /// TENT's base slice size. Large requests adapt to a bounded slice count.
    pub block_size: usize,
    /// Maximum signaled work requests in one request QP window (1..=64).
    pub max_send_wr: usize,
    /// Physical shared CQ capacity across all request QPs (1..=4096).
    pub max_cqes: usize,
    pub max_cached_mrs: usize,
    pub max_cached_mr_bytes: usize,
    /// Historical MW indices retained without eviction until the PD is destroyed.
    pub max_window_key_indices: usize,
}

impl RdmaConfig {
    pub fn new(device: impl Into<String>) -> Self {
        Self {
            device: device.into(),
            port: 1,
            gid_index: 0,
            traffic_class: 0,
            hop_limit: 1,
            max_endpoints: 64,
            max_staging_bytes: 64 << 20,
            host_memory: RdmaHostMemory::Direct,
            max_direct_bytes: 64 << 20,
            block_size: 64 << 10,
            max_send_wr: 16,
            max_cqes: 1024,
            max_cached_mrs: 64,
            max_cached_mr_bytes: 64 << 20,
            max_window_key_indices: 65_536,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.device.is_empty()
            || self.device.len() > 128
            || self.device.contains('\0')
            || self.port == 0
            || self.hop_limit == 0
            || self.max_cached_mrs == 0
            || self.max_cached_mr_bytes == 0
            || !(1..=1 << 24).contains(&self.max_window_key_indices)
            || !(1..=4096).contains(&self.max_cqes)
            || !(1..=4096).contains(&self.max_endpoints)
            || self.max_staging_bytes == 0
            || self.max_direct_bytes == 0
            || !(1..=64 << 20).contains(&self.block_size)
            || !(1..=64).contains(&self.max_send_wr)
        {
            return Err(Error::InvalidConfig(
                "RDMA needs an explicit device, port, hop limit and positive budgets",
            ));
        }
        Ok(())
    }
}

/// A retained endpoint is charged and blocks further RDMA admission. It can
/// still own pinned pages or RNIC resources after native teardown failed.
#[derive(Clone, Copy, Debug, Default)]
pub struct RdmaStats {
    /// Shared context/PD owners, including partially constructed retained ones.
    pub active_devices: usize,
    pub retained_devices: usize,
    /// Successful context/PD constructions during this runtime lifetime.
    pub opened_devices: u64,
    pub active_windows: usize,
    pub opened_windows: u64,
    /// Historical native MW indices charged for the lifetime of their PD.
    pub window_key_indices: usize,
    /// Full rkeys burned before an endpoint can submit BIND, including failures.
    pub burned_window_keys: u64,
    pub window_key_exhaustions: u64,
    pub retained_windows: usize,
    pub completed_window_binds: u64,
    pub completed_window_invalidations: u64,
    pub active_cqs: usize,
    pub retained_cqs: usize,
    pub opened_cqs: u64,
    pub progress_workers: usize,
    pub async_event_workers: usize,
    pub acknowledged_async_events: u64,
    /// A fatal shared-lane fault permanently fences new RDMA admission.
    pub progress_failures: u64,
    pub peak_pending_qps: usize,
    pub active_endpoints: usize,
    pub staging_bytes: usize,
    /// Direct host bytes charged through endpoint release, including retention.
    pub direct_bytes: usize,
    /// Successfully constructed direct host endpoints during this runtime.
    pub opened_direct_endpoints: u64,
    pub retained_endpoints: usize,
    pub retained_bytes: usize,
    /// Local-only MR owners, including creation, closing and retained failures.
    pub persistent_mrs: usize,
    pub persistent_mr_bytes: usize,
    pub retained_persistent_mrs: usize,
    pub opened_persistent_mrs: u64,
    pub reused_persistent_mrs: u64,
    pub evicted_persistent_mrs: u64,
    pub completed_reads: u64,
    pub completed_writes: u64,
    pub completed_read_bytes: u64,
    pub completed_write_bytes: u64,
    /// Reserved or possibly posted WR slots pending CQ validation or teardown.
    pub work_requests: usize,
    pub retained_work_requests: usize,
    pub peak_work_requests: usize,
    pub posted_work_requests: u64,
    pub post_calls: u64,
    pub partial_posts: u64,
    pub uncertain_posts: u64,
    pub completion_errors: u64,
}

pub(crate) struct Runtime {
    config: RdmaConfig,
    usage: Arc<Mutex<RdmaStats>>,
    verbs: &'static sys::Verbs,
    device: Mutex<DeviceCache>,
    drained: Arc<Condvar>,
    pub(crate) registrations: Arc<registration::HostRegistrations>,
}

#[derive(Default)]
struct DeviceCache {
    closed: bool,
    owner: Option<Arc<Device>>,
    progress: Option<Arc<cq::Progress>>,
    failure: Option<(&'static str, i32)>,
}

impl Runtime {
    pub(crate) fn new(config: RdmaConfig) -> Result<Arc<Self>> {
        config.validate()?;
        let usage = Arc::default();
        let registrations =
            registration::HostRegistrations::new(&config, Arc::clone(&usage), &sys::REAL);
        Ok(Arc::new(Self {
            config,
            usage,
            verbs: &sys::REAL,
            device: Mutex::default(),
            drained: Arc::default(),
            registrations,
        }))
    }

    pub(crate) fn stats(&self) -> Result<RdmaStats> {
        self.usage
            .lock()
            .map(|usage| *usage)
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))
    }

    fn device(&self) -> Result<Arc<Device>> {
        let mut cache = self
            .device
            .lock()
            .map_err(|_| Error::Internal("RDMA device cache lock poisoned"))?;
        if cache.closed {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        if let Some((operation, code)) = cache.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        if let Some(owner) = &cache.owner {
            return Ok(Arc::clone(owner));
        }
        match Device::open(&self.config, self.verbs, Arc::clone(&self.usage)) {
            Ok(owner) => {
                cache.owner = Some(Arc::clone(&owner));
                Ok(owner)
            }
            Err(error) => {
                if let Error::Unquiesced { operation, code } = error {
                    cache.failure = Some((operation, code));
                }
                Err(error)
            }
        }
    }

    fn progress(&self, device: Arc<Device>) -> Result<Arc<cq::Progress>> {
        let mut cache = self
            .device
            .lock()
            .map_err(|_| Error::Internal("RDMA device cache lock poisoned"))?;
        if cache.closed {
            return Err(Error::Unavailable("RDMA runtime is closed"));
        }
        if let Some((operation, code)) = cache.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        if let Some(progress) = &cache.progress {
            return Ok(Arc::clone(progress));
        }
        match cq::Progress::new(&self.config, device, Arc::clone(&self.usage), self.verbs) {
            Ok(progress) => {
                cache.progress = Some(Arc::clone(&progress));
                Ok(progress)
            }
            Err(error) => {
                if let Error::Unquiesced { operation, code } = error {
                    cache.failure = Some((operation, code));
                }
                Err(error)
            }
        }
    }

    pub(crate) fn close(&self, deadline: Instant) -> Result<()> {
        let (owner, progress) = {
            let mut cache = self
                .device
                .lock()
                .map_err(|_| Error::Internal("RDMA device cache lock poisoned"))?;
            cache.closed = true;
            if let Some((operation, code)) = cache.failure {
                return Err(Error::Unquiesced { operation, code });
            }
            (cache.owner.clone(), cache.progress.clone())
        };
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        loop {
            if usage.retained_endpoints != 0 {
                return Err(Error::Unquiesced {
                    operation: "RDMA endpoint resources retained",
                    code: libc::EIO,
                });
            }
            if usage.active_endpoints == 0 {
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
        if let Some(progress) = progress {
            progress.close(deadline)?;
        }
        if let Some(owner) = owner {
            owner.close()?;
        }
        let mut cache = self
            .device
            .lock()
            .map_err(|_| Error::Internal("RDMA device cache lock poisoned"))?;
        cache.progress = None;
        cache.owner = None;
        Ok(())
    }

    pub(crate) fn endpoint(
        &self,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
        permission: u32,
        deadline: Instant,
    ) -> Result<Endpoint> {
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
        let lease = {
            let mut usage = self
                .usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            let (charged, limit) = if is_direct {
                (&mut usage.direct_bytes, self.config.max_direct_bytes)
            } else {
                (&mut usage.staging_bytes, self.config.max_staging_bytes)
            };
            let bytes = charged
                .checked_add(length)
                .ok_or(Error::ResourceExhausted)?;
            let over_limit = bytes > limit;
            if usage.retained_endpoints != 0
                || usage.retained_devices != 0
                || usage.retained_cqs != 0
                || usage.retained_persistent_mrs != 0
                || usage.progress_failures != 0
            {
                return Err(Error::Unavailable(
                    "RDMA admission is poisoned by retained native resources",
                ));
            }
            if usage.active_endpoints >= self.config.max_endpoints || over_limit {
                return Err(Error::ResourceExhausted);
            }
            usage.active_endpoints += 1;
            if is_direct {
                usage.direct_bytes = bytes;
            } else {
                usage.staging_bytes = bytes;
            }
            Lease {
                usage: Arc::clone(&self.usage),
                length,
                is_direct,
                drained: Arc::clone(&self.drained),
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
        let epoch = SourceEpoch::generate()?;
        let psn = u32::from_be_bytes([0, epoch.0[0], epoch.0[1], epoch.0[2]]);
        let device = self.device()?;
        let progress = self.progress(Arc::clone(&device))?;
        let cached_mr = if is_direct && permission == 0 {
            Some(
                self.registrations
                    .acquire(Arc::clone(&device), memory, deadline)?,
            )
        } else {
            None
        };
        let mut handles = sys::Handles {
            cq: progress.handle()?,
            ..Default::default()
        };
        if let Some(registration) = &cached_mr {
            handles.mr = registration.handle()?;
        }
        let record = progress.register(Box::new(Resources {
            device: Arc::clone(&device),
            handles,
            cached_mr,
            buffer,
            _lease: lease,
            max_read_slices: if memory.is_cuda() { 32 } else { 64 },
            permission,
            window_used: false,
            window_bound: false,
        }))?;
        let mut endpoint = Endpoint {
            record,
            progress,
            local: Descriptor::default(),
            incarnation: None,
            grant_generation: 0,
            config: self.config.clone(),
            verbs: self.verbs,
            deadline,
            resources: None,
        };
        let initialized = endpoint.open(permission, psn);
        if let Err(error) = initialized {
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
}

struct Lease {
    usage: Arc<Mutex<RdmaStats>>,
    length: usize,
    is_direct: bool,
    drained: Arc<Condvar>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.active_endpoints -= 1;
        if self.is_direct {
            usage.direct_bytes -= self.length;
        } else {
            usage.staging_bytes -= self.length;
        }
        self.drained.notify_all();
    }
}

enum Buffer {
    Staged(Staging),
    Direct {
        loan: Option<crate::memory::HostDmaLease>,
        address: NonNull<u8>,
        length: usize,
    },
}

impl Buffer {
    fn address(&self) -> NonNull<u8> {
        match self {
            Self::Staged(bytes) => bytes.address,
            Self::Direct { address, .. } => *address,
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Staged(bytes) => bytes.len(),
            Self::Direct { length, .. } => *length,
        }
    }
}

struct Staging {
    address: NonNull<u8>,
    length: usize,
}
impl Staging {
    fn new(length: usize) -> Result<Self> {
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
    fn len(&self) -> usize {
        self.length
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: only Init endpoints before descriptor exposure call this;
        // native DMA has not started and the allocation is exclusively owned.
        unsafe { std::slice::from_raw_parts_mut(self.address.as_ptr(), self.length) }
    }
    fn bytes(&self) -> &[u8] {
        // SAFETY: only Closed endpoints expose a slice, after QP/MR destruction.
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

struct Resources {
    device: Arc<Device>,
    handles: sys::Handles,
    cached_mr: Option<registration::MrLease>,
    buffer: Buffer,
    _lease: Lease,
    max_read_slices: usize,
    permission: u32,
    window_used: bool,
    window_bound: bool,
}

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
pub(crate) struct Descriptor {
    address: u64,
    rkey: u32,
    qpn: u32,
    psn: u32,
    lid: u16,
    mtu: u8,
    link_layer: u8,
    gid: [u8; 16],
}

impl Descriptor {
    fn validate(&self) -> Result<()> {
        if self.address == 0 {
            return Err(Error::Protocol("invalid RDMA memory grant"));
        }
        self.validate_connection()
    }

    fn validate_connection(&self) -> Result<()> {
        if self.qpn == 0
            || self.qpn > 0xffffff
            || self.psn > 0xffffff
            || !(1..=5).contains(&self.mtu)
            || !matches!(self.link_layer, 1 | 2)
            || (self.link_layer == 2 && self.gid == [0; 16])
        {
            return Err(Error::Protocol("invalid RDMA endpoint descriptor"));
        }
        Ok(())
    }

    pub(crate) fn connection_bytes(self) -> [u8; DESCRIPTOR_SIZE] {
        Self {
            address: 0,
            rkey: 0,
            ..self
        }
        .encode()
    }

    pub(crate) fn encode(self) -> [u8; DESCRIPTOR_SIZE] {
        let mut bytes = [0; DESCRIPTOR_SIZE];
        bytes[..8].copy_from_slice(&self.address.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.rkey.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.qpn.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.psn.to_be_bytes());
        bytes[20..22].copy_from_slice(&self.lid.to_be_bytes());
        bytes[22] = self.mtu;
        bytes[23] = self.link_layer;
        bytes[24..].copy_from_slice(&self.gid);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8; DESCRIPTOR_SIZE]) -> Result<Self> {
        let descriptor = Self::decode_fields(bytes);
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub(crate) fn decode_connection(bytes: &[u8; DESCRIPTOR_SIZE]) -> Result<Self> {
        let descriptor = Self::decode_fields(bytes);
        descriptor.validate_connection()?;
        if descriptor.address != 0 || descriptor.rkey != 0 {
            return Err(Error::Protocol("RDMA connection exposes memory authority"));
        }
        Ok(descriptor)
    }

    fn decode_fields(bytes: &[u8; DESCRIPTOR_SIZE]) -> Self {
        let mut address = [0; 8];
        address.copy_from_slice(&bytes[..8]);
        let value = |offset| {
            let mut field = [0; 4];
            field.copy_from_slice(&bytes[offset..offset + 4]);
            u32::from_be_bytes(field)
        };
        let mut gid = [0; 16];
        gid.copy_from_slice(&bytes[24..]);
        Self {
            address: u64::from_be_bytes(address),
            rkey: value(8),
            qpn: value(12),
            psn: value(16),
            lid: u16::from_be_bytes([bytes[20], bytes[21]]),
            mtu: bytes[22],
            link_layer: bytes[23],
            gid,
        }
    }
}

// All request-native state is serialized by its progress record mutex.
unsafe impl Send for Resources {}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Init,
    Connected,
    Queued,
    Posted,
    Closed,
    Retained,
}

/// Connection identity is independent of reusable provider QP numbers and PSNs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConnectionId {
    domain: [u8; 16],
    generation: u64,
}

impl ConnectionId {
    pub(crate) fn encode(self) -> [u8; 24] {
        let mut bytes = [0; 24];
        bytes[..16].copy_from_slice(&self.domain);
        bytes[16..].copy_from_slice(&self.generation.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; 24] = bytes
            .try_into()
            .map_err(|_| Error::Protocol("RDMA connection identity size"))?;
        let mut domain = [0; 16];
        domain.copy_from_slice(&bytes[..16]);
        let mut generation = [0; 8];
        generation.copy_from_slice(&bytes[16..]);
        let generation = u64::from_be_bytes(generation);
        if domain == [0; 16] || generation == 0 {
            return Err(Error::Protocol("invalid RDMA connection identity"));
        }
        Ok(Self { domain, generation })
    }

    fn new_domain() -> Result<[u8; 16]> {
        let domain = SourceEpoch::generate()?.0;
        if domain == [0; 16] {
            return Err(Error::Unavailable("zero RDMA protection-domain identity"));
        }
        Ok(domain)
    }

    fn next(domain: [u8; 16], generation: &mut u64) -> Result<Self> {
        let current = *generation;
        *generation = current.checked_add(1).ok_or(Error::ResourceExhausted)?;
        Ok(Self {
            domain,
            generation: current,
        })
    }
}

pub(crate) struct Endpoint {
    record: Arc<cq::Record>,
    progress: Arc<cq::Progress>,
    local: Descriptor,
    incarnation: Option<ConnectionId>,
    grant_generation: u64,
    config: RdmaConfig,
    verbs: &'static sys::Verbs,
    deadline: Instant,
    resources: Option<Box<Resources>>,
}

impl Endpoint {
    fn open(&mut self, permission: u32, psn: u32) -> Result<()> {
        let mut state = self.record.lock()?;
        state.check()?;
        if state.phase != Phase::Init {
            return Err(Error::Unavailable("RDMA endpoint is closed"));
        }
        let resources = state
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        let device = Arc::clone(&resources.device);
        let (incarnation, grant_generation) =
            device.open_endpoint(resources, &self.config, permission, psn, &mut self.local)?;
        self.incarnation = Some(incarnation);
        self.grant_generation = grant_generation;
        self.local.validate()?;
        state.qpn = self.local.qpn;
        Ok(())
    }

    pub(crate) fn incarnation(&self) -> Result<ConnectionId> {
        self.incarnation
            .ok_or(Error::Internal("missing RDMA connection identity"))
    }

    pub(crate) fn grant_generation(&self) -> Result<u64> {
        if self.grant_generation == 0 {
            return Err(Error::Internal("endpoint owns no source grant generation"));
        }
        Ok(self.grant_generation)
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
        let request = self.record.lock()?;
        request.check()?;
        if !request.resources.as_ref().is_some_and(|r| r.window_bound)
            || request.phase != Phase::Connected
        {
            return Err(Error::Unavailable("source MW has no completed bind"));
        }
        Ok(self.local)
    }

    pub(crate) fn bind_window(&mut self) -> Result<()> {
        self.progress
            .window(&self.record, progress::Operation::Bind, self.deadline)
    }

    pub(crate) fn invalidate_window(&mut self) -> Result<()> {
        self.progress
            .window(&self.record, progress::Operation::Invalidate, self.deadline)
    }

    pub(crate) fn fill(&mut self, memory: &RegisteredMemory, offset: usize) -> Result<()> {
        let mut state = self.record.lock()?;
        state.check()?;
        if state.phase != Phase::Init {
            return Err(Error::Internal("RDMA staging already exposed"));
        }
        match &mut state
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA staging"))?
            .buffer
        {
            Buffer::Staged(bytes) => memory.read(offset, bytes.bytes_mut()),
            Buffer::Direct { .. } => Ok(()),
        }
    }

    pub(crate) fn copy_back(&self, memory: &RegisteredMemory, offset: usize) -> Result<()> {
        let state = self.record.lock()?;
        state.check()?;
        if state.phase != Phase::Closed {
            return Err(Error::Internal("RDMA buffer is not physically quiescent"));
        }
        match &self
            .resources
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
        let mut state = self.record.lock()?;
        state.check()?;
        if state.phase != Phase::Init || remote.link_layer != self.local.link_layer {
            return Err(Error::Protocol("RDMA state or link layer mismatch"));
        }
        let resources = state
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        check("ibv_modify_qp RTR/RTS", unsafe {
            (self.verbs.connect)(
                &mut resources.handles,
                &self.local,
                remote,
                self.config.port,
                self.config.gid_index,
                self.config.traffic_class,
                self.config.hop_limit,
            )
        })?;
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
            .transfer(&self.record, *remote, operation, deadline)
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        let result = self.progress.close_record(&self.record, self.deadline);
        let mut request = self.record.lock()?;
        if request.phase == Phase::Closed {
            if let Some(resources) = request.resources.take() {
                self.resources = Some(resources);
            }
        }
        result
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn check(operation: &'static str, code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::Backend { operation, code })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/rdma/mod.rs"]
pub(crate) mod tests;
