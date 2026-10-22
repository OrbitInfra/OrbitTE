//! Opt-in single-rail RC verbs with owned direct host registration or staging.

#![allow(unsafe_code)]

use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::engine::remaining;
use crate::{Error, RegisteredMemory, Result, SourceEpoch, TransferOp};

mod device;
mod progress;
mod sys;

use device::Device;

pub(crate) const DESCRIPTOR_SIZE: usize = 40;

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
    /// Maximum signaled work requests in one exclusive QP/CQ window (1..=64).
    pub max_send_wr: usize,
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
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.device.is_empty()
            || self.device.len() > 128
            || self.device.contains('\0')
            || self.port == 0
            || self.hop_limit == 0
            || self.max_endpoints == 0
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
    pub active_endpoints: usize,
    pub staging_bytes: usize,
    /// Direct host bytes charged through endpoint release, including retention.
    pub direct_bytes: usize,
    /// Successfully constructed direct host endpoints during this runtime.
    pub opened_direct_endpoints: u64,
    pub retained_endpoints: usize,
    pub retained_bytes: usize,
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
}

#[derive(Default)]
struct DeviceCache {
    closed: bool,
    owner: Option<Arc<Device>>,
    failure: Option<(&'static str, i32)>,
}

impl Runtime {
    pub(crate) fn new(config: RdmaConfig) -> Result<Arc<Self>> {
        config.validate()?;
        Ok(Arc::new(Self {
            config,
            usage: Arc::default(),
            verbs: &sys::REAL,
            device: Mutex::default(),
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

    pub(crate) fn close(&self) -> Result<()> {
        let mut cache = self
            .device
            .lock()
            .map_err(|_| Error::Internal("RDMA device cache lock poisoned"))?;
        cache.closed = true;
        if let Some((operation, code)) = cache.failure {
            return Err(Error::Unquiesced { operation, code });
        }
        let usage = self.stats()?;
        if usage.retained_endpoints != 0 {
            return Err(Error::Unavailable("RDMA endpoints retain native resources"));
        }
        if usage.active_endpoints != 0 {
            return Err(Error::Timeout);
        }
        if let Some(owner) = &cache.owner {
            owner.close()?;
        }
        cache.owner = None;
        Ok(())
    }

    pub(crate) fn endpoint(
        &self,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
        permission: u32,
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
            if usage.retained_endpoints != 0 || usage.retained_devices != 0 {
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
        let mut endpoint = Endpoint {
            resources: Some(Box::new(Resources {
                _device: Arc::clone(&device),
                handles: sys::Handles::default(),
                buffer,
                lease,
                progress: None,
                max_read_slices: if memory.is_cuda() { 32 } else { 64 },
            })),
            local: Descriptor::default(),
            config: self.config.clone(),
            verbs: self.verbs,
            phase: Phase::Init,
        };
        let initialized = device.open_endpoint(&mut endpoint, permission, psn);
        if let Err(error) = initialized.and_then(|()| endpoint.local.validate()) {
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
    _device: Arc<Device>,
    handles: sys::Handles,
    buffer: Buffer,
    lease: Lease,
    progress: Option<progress::SendQueue>,
    max_read_slices: usize,
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
        if self.address == 0
            || self.qpn == 0
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
        let mut address = [0; 8];
        address.copy_from_slice(&bytes[..8]);
        let value = |offset| {
            let mut field = [0; 4];
            field.copy_from_slice(&bytes[offset..offset + 4]);
            u32::from_be_bytes(field)
        };
        let mut gid = [0; 16];
        gid.copy_from_slice(&bytes[24..]);
        let descriptor = Self {
            address: u64::from_be_bytes(address),
            rkey: value(8),
            qpn: value(12),
            psn: value(16),
            lid: u16::from_be_bytes([bytes[20], bytes[21]]),
            mtu: bytes[22],
            link_layer: bytes[23],
            gid,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Init,
    Connected,
    Posted,
    Closed,
}

pub(crate) struct Endpoint {
    resources: Option<Box<Resources>>,
    local: Descriptor,
    config: RdmaConfig,
    verbs: &'static sys::Verbs,
    phase: Phase,
}

impl Endpoint {
    pub(crate) fn has_resources(&self) -> bool {
        self.resources.is_some()
    }

    pub(crate) fn descriptor(&self) -> Descriptor {
        self.local
    }

    pub(crate) fn fill(&mut self, memory: &RegisteredMemory, offset: usize) -> Result<()> {
        if self.phase != Phase::Init {
            return Err(Error::Internal("RDMA staging already exposed"));
        }
        let resources = self
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA staging"))?;
        match &mut resources.buffer {
            Buffer::Staged(bytes) => memory.read(offset, bytes.bytes_mut()),
            Buffer::Direct { .. } => Ok(()),
        }
    }

    pub(crate) fn copy_back(&self, memory: &RegisteredMemory, offset: usize) -> Result<()> {
        if self.phase != Phase::Closed {
            return Err(Error::Internal("RDMA buffer is not physically quiescent"));
        }
        let resources = self
            .resources
            .as_ref()
            .ok_or(Error::Internal("missing RDMA buffer"))?;
        match &resources.buffer {
            Buffer::Staged(bytes) => memory.write(offset, bytes.bytes()),
            Buffer::Direct { .. } => Ok(()),
        }
    }

    pub(crate) fn connect(&mut self, remote: &Descriptor) -> Result<()> {
        remote.validate()?;
        if self.phase != Phase::Init || remote.link_layer != self.local.link_layer {
            return Err(Error::Protocol("RDMA state or link layer mismatch"));
        }
        let resources = self
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        // SAFETY: native handles remain exclusively owned; descriptor fields
        // are validated and both descriptors live across the call.
        let code = unsafe {
            (self.verbs.connect)(
                &mut resources.handles,
                &self.local,
                remote,
                self.config.port,
                self.config.gid_index,
                self.config.traffic_class,
                self.config.hop_limit,
            )
        };
        check("ibv_modify_qp RTR/RTS", code)?;
        self.phase = Phase::Connected;
        Ok(())
    }

    pub(crate) fn transfer(
        &mut self,
        remote: &Descriptor,
        operation: TransferOp,
        deadline: Instant,
    ) -> Result<()> {
        if self.phase != Phase::Connected {
            return Err(Error::Internal("RDMA QP is not ready"));
        }
        remaining(deadline)?;
        let resources = self
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        if remote
            .address
            .checked_add(resources.buffer.len() as u64)
            .is_none()
        {
            return Err(Error::InvalidRange);
        }
        let cap = if operation == TransferOp::Write {
            32
        } else {
            resources.max_read_slices
        };
        resources.progress = Some(progress::SendQueue::new(
            resources.buffer.len(),
            self.config.block_size,
            cap,
            Arc::clone(&resources.lease.usage),
        )?);
        let queue = resources
            .progress
            .as_mut()
            .ok_or(Error::Internal("missing RDMA send queue"))?;
        while !queue.finished() {
            remaining(deadline)?;
            let requests = queue.reserve(self.config.max_send_wr)?;
            let mut posted = sys::PostResult::default();
            // SAFETY: Resources owns stable MR/QP/backing and this queue. The
            // native call consumes descriptors but may retain access to bytes.
            let code = unsafe {
                (self.verbs.post)(
                    &mut resources.handles,
                    requests.as_ptr(),
                    requests.len(),
                    remote.address,
                    remote.rkey,
                    i32::from(operation == TransferOp::Write),
                    &mut posted,
                )
            };
            self.phase = Phase::Posted;
            queue.posted(code, posted)?;
            while queue.pending() {
                remaining(deadline)?;
                let mut completion = sys::Completion::default();
                // SAFETY: this thread exclusively polls its live endpoint CQ.
                let count = unsafe { (self.verbs.poll)(&mut resources.handles, &mut completion) };
                match count {
                    0 => thread::sleep(remaining(deadline)?.min(Duration::from_micros(50))),
                    1 => queue.complete(completion, operation)?,
                    count if count < 0 => {
                        return Err(Error::Backend {
                            operation: "ibv_poll_cq",
                            code: count,
                        });
                    }
                    _ => return Err(Error::Protocol("RDMA poll returned more than one CQE")),
                }
            }
        }
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        let resources = self
            .resources
            .as_mut()
            .ok_or(Error::Internal("missing RDMA endpoint"))?;
        if let Err(Error::Backend { operation, code }) =
            close_handles(self.verbs, &mut resources.handles)
        {
            self.retain();
            return Err(Error::Unquiesced { operation, code });
        }
        if let Some(queue) = &mut resources.progress {
            queue.quiesce();
        }
        if let Buffer::Direct { loan, .. } = &mut resources.buffer {
            // Release CPU/alias exclusion before a source completion ACK. No
            // handle can touch these bytes after all native destroys succeeded.
            drop(loan.take());
        }
        self.phase = Phase::Closed;
        Ok(())
    }
}

fn close_handles(verbs: &sys::Verbs, handles: &mut sys::Handles) -> Result<()> {
    for (operation, close) in [
        ("ibv_destroy_qp", verbs.destroy_qp),
        ("ibv_dereg_mr", verbs.deregister),
        ("ibv_destroy_cq", verbs.destroy_cq),
    ] {
        // SAFETY: each bridge checks for a null handle and clears only on native
        // success. Stop on error so dependent resources cannot be freed early.
        check(operation, unsafe { close(handles) })?;
    }
    Ok(())
}

impl Endpoint {
    fn retain(&mut self) {
        if let Some(resources) = self.resources.take() {
            {
                let mut usage = resources
                    .lease
                    .usage
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                usage.retained_endpoints += 1;
                usage.retained_bytes += resources.buffer.len();
                usage.retained_work_requests += resources
                    .progress
                    .as_ref()
                    .map_or(0, |queue| queue.outstanding());
            }
            std::mem::forget(resources);
        }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        if self.resources.is_some() {
            let _ = self.close();
        }
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
