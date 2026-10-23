//! Opt-in single-rail RC verbs with owned direct host registration or staging.

#![allow(unsafe_code)]

use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::engine::remaining;
use crate::{Error, RegisteredMemory, Result, SourceEpoch};

mod buffers;
mod context;
mod cq;
mod endpoint;
mod endpoint_store;
mod events;
mod keys;
mod rdma_transport;
mod receive;
mod slice;
mod sys;
mod workers;

use context::RdmaContext;
use endpoint::{
    Buffer, ConnectionId, ConnectionLease, ConnectionResources, Descriptor, GrantLease,
    GrantResources, Phase, RdmaEndpoint, Resources, Staging,
};

pub(crate) const DESCRIPTOR_SIZE: usize = 40;
pub(super) const WR_ID_STRIDE: u64 = 128;
pub(super) const COMPLETION_MESSAGE_SIZE: usize = 141;

/// Host-memory policy. CUDA allocations continue to use owned host staging.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RdmaHostMemory {
    /// Register the actual host range. Registration errors never select staging.
    #[default]
    Direct,
    /// Isolate application bytes in private host staging until completion.
    Staged,
}

/// Explicit RNIC selection. Host Direct peers can retain role-specific RC QPs.
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
    /// Capacity of each shared send/receive CQ (1..=4096), charged separately.
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
    pub receive_work_requests: usize,
    pub retained_receive_work_requests: usize,
    pub peak_receive_work_requests: usize,
    pub receive_post_calls: u64,
    pub completed_receives: u64,
    pub completed_sends: u64,
    pub active_message_mrs: usize,
    pub retained_message_mrs: usize,
    pub message_bytes: usize,
    pub retained_message_bytes: usize,
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
    pub idle_endpoints: usize,
    pub opened_connections: u64,
    pub closed_connections: u64,
    pub reused_connections: u64,
    pub evicted_connections: u64,
    pub completed_reusable_grants: u64,
    /// Per-request memory owners, including outputs after physical QP close.
    pub active_grants: usize,
    pub retained_grants: usize,
    pub staging_bytes: usize,
    /// Direct host bytes charged through endpoint release, including retention.
    pub direct_bytes: usize,
    /// Successfully constructed direct host endpoints during this runtime.
    pub opened_direct_endpoints: u64,
    pub retained_endpoints: usize,
    pub retained_bytes: usize,
    /// All cached physical MRs, including creation, closing and retained failures.
    pub persistent_mrs: usize,
    pub persistent_mr_bytes: usize,
    pub retained_persistent_mrs: usize,
    pub opened_persistent_mrs: u64,
    pub reused_persistent_mrs: u64,
    pub evicted_persistent_mrs: u64,
    /// MW-bind parent subset of persistent_mrs; never remote-enabled MRs.
    pub persistent_source_mrs: usize,
    pub opened_source_mrs: u64,
    pub reused_source_mrs: u64,
    /// Source parents retained by failed deregistration. Failed MW cleanup keeps
    /// its parent borrow charged in persistent_source_mrs without a dereg attempt.
    pub retained_source_mrs: usize,
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

pub(crate) struct RdmaTransport {
    config: RdmaConfig,
    usage: Arc<Mutex<RdmaStats>>,
    verbs: &'static sys::Verbs,
    device: Mutex<ContextCache>,
    drained: Arc<Condvar>,
    pub(crate) registrations: Arc<buffers::LocalBufferManager>,
}

#[derive(Default)]
struct ContextCache {
    closed: bool,
    owner: Option<Arc<RdmaContext>>,
    progress: Option<Arc<workers::Workers>>,
    failure: Option<(&'static str, i32)>,
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

    fn device(&self) -> Result<Arc<RdmaContext>> {
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
        match RdmaContext::open(&self.config, self.verbs, Arc::clone(&self.usage)) {
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

    fn progress(&self, device: Arc<RdmaContext>) -> Result<Arc<workers::Workers>> {
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
        match workers::Workers::new(&self.config, device, Arc::clone(&self.usage), self.verbs) {
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
        if let Some(progress) = &progress {
            progress.fence(deadline)?;
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
        reuse: Option<RdmaEndpoint>,
    ) -> Result<RdmaEndpoint> {
        remaining(deadline)?;
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
        let device = self.device()?;
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
                if !progress.evict_idle(deadline)? {
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
                verbs: self.verbs,
                deadline,
                output: None,
                claim: 1,
                released: false,
                cached_peer: None,
            }
        };
        #[cfg(test)]
        tests::before_reused_open(self, &endpoint);
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

fn check(operation: &'static str, code: i32) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(Error::Backend { operation, code })
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/mod.rs"]
pub(crate) mod tests;
