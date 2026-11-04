//! RDMA transport entry points: registration, parent dispatch and source serving.

use crate::runtime::transport_selector::{SelectionResult, qp_pools::QpLayout};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::{
    RdmaConfig, RdmaStats, buffers,
    context_set::ContextSet,
    events,
    grant::admission::GrantCredits,
    path,
    protocol::SliceRequest,
    quota,
    receive::ReceiveCredits,
    slice::{RdmaSlice, plan_rdma_slices},
    sys, workers,
};
use crate::engine::PreparedTransfer;
use crate::runtime::control_plane::{ControlChannel, Operation, Request};
use crate::{
    Error, RegisteredMemory, RemoteEffect, RemoteSegment, Result, SourceEpoch, TransferOp,
};

pub(crate) struct RdmaTransport {
    #[cfg(feature = "cuda")]
    pub(crate) proxy: Arc<crate::runtime::proxy_manager::ProxyManager>,
    pub(crate) qp_layout: Option<Arc<QpLayout>>,
    pub(super) config: RdmaConfig,
    pub(super) usage: Arc<Mutex<RdmaStats>>,
    pub(super) counters: Arc<events::Counters>,
    pub(super) workers: Arc<workers::Workers>,
    pub(super) cq_quota: Arc<quota::CqQuota>,
    pub(super) verbs: &'static sys::Verbs,
    pub(super) context_set: Arc<Mutex<ContextSet>>,
    pub(super) drained: Arc<Condvar>,
    pub(crate) registrations: Arc<buffers::LocalBufferManager>,
}

/// Source admission is all-or-nothing before the first slice grant is offered.
struct ParentAdmission {
    grants: Arc<GrantCredits>,
    receive: ReceiveCredits,
}

impl ParentAdmission {
    fn acquire(transport: &RdmaTransport, count: usize) -> Result<Self> {
        let grants = GrantCredits::acquire(
            Arc::clone(&transport.usage),
            transport.config.max_grants,
            count,
        )?;
        let receive = ReceiveCredits::acquire(
            Arc::clone(&transport.usage),
            transport.config.max_cqes,
            count,
        )?;
        Ok(Self { grants, receive })
    }
}

impl RdmaTransport {
    pub(crate) fn new(
        config: RdmaConfig,
        qp_layout: Option<Arc<QpLayout>>,
        #[cfg(feature = "cuda")] proxy: Arc<crate::runtime::proxy_manager::ProxyManager>,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        let usage = Arc::default();
        let registrations =
            buffers::LocalBufferManager::new(&config, Arc::clone(&usage), &sys::REAL);
        Ok(Arc::new(Self {
            #[cfg(feature = "cuda")]
            proxy,
            config,
            qp_layout,
            usage,
            counters: Arc::default(),
            workers: Arc::default(),
            cq_quota: Arc::default(),
            verbs: &sys::REAL,
            context_set: Arc::default(),
            drained: Arc::default(),
            registrations,
        }))
    }

    fn qp_width(&self) -> usize {
        self.qp_layout
            .as_ref()
            .map_or(self.config.num_lanes, |layout| layout.width)
    }

    fn peer_width(&self, advertised: usize) -> Result<usize> {
        if self.qp_layout.is_some() && advertised != 0 && advertised != self.qp_width() {
            return Err(Error::Protocol("RDMA peer QP pool width mismatch"));
        }
        Ok(advertised.min(self.qp_width()))
    }

    fn route_grant(
        &self,
        grant: &mut super::grant::GrantLease,
        selection: &SelectionResult,
    ) -> Result<()> {
        let Some(layout) = &self.qp_layout else {
            return Ok(());
        };
        let device = self.device_at(grant._selection.device_id())?;
        let (lane, worker) = self.workers.select(&self.cq_list(device)?)?;
        let candidate = lane
            .worker_id
            .get()
            .ok_or(Error::Internal("RDMA candidate has no worker"))?;
        let range = selection.qp_pool.unwrap_or_else(|| layout.range(None));
        grant.qp_index = Some(range.index(*candidate as i32));
        grant.policy_worker = Some(worker);
        Ok(())
    }

    pub(crate) fn policy_devices(&self) -> Result<Vec<String>> {
        let mut contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        match self.initialize_contexts(&mut contexts) {
            Ok(()) => (),
            Err(Error::Unavailable(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        }
        Ok(contexts
            .topology
            .as_ref()
            .ok_or(Error::Internal("missing RDMA topology"))?
            .nic_list
            .iter()
            .map(|nic| nic.name.clone())
            .collect())
    }

    pub(crate) fn check_policy_paths(
        &self,
        location: Option<crate::MemoryLocation>,
        selection: &SelectionResult,
    ) -> Result<()> {
        let contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        let eligible = contexts.selector.as_ref().map_or(0, |devices| {
            self.config.device_mask
                & selection.device_mask
                & location.map_or(u64::MAX, |loc| {
                    devices
                        .priority(
                            self.config
                                .preferred_numa_node
                                .map_or(loc, |node| crate::MemoryLocation::Cpu(Some(node))),
                        )
                        .eligible
                })
        });
        if contexts.slots.iter().enumerate().any(|(i, slot)| {
            eligible & (1u64 << i) != 0
                && slot
                    .owner
                    .as_ref()
                    .is_some_and(|nic| nic.asynchronous.check().is_ok())
        }) {
            Ok(())
        } else {
            Err(selection.rejected("device mask excludes every eligible RDMA NIC"))
        }
    }

    fn request_paths(
        &self,
        location: crate::MemoryLocation,
        selection: &SelectionResult,
    ) -> Result<Vec<path::NicPath>> {
        let mut paths = self.paths(location).map_err(|error| match error {
            Error::Unavailable(reason) => selection.rejected(reason),
            other => other,
        })?;
        paths.retain(|p| selection.device_mask & (1u64 << p.nic_id) != 0);
        if paths.is_empty() {
            return Err(selection.rejected("device mask excludes every eligible RDMA NIC"));
        }
        Ok(paths)
    }

    pub(crate) fn register_memory(
        &self,
        memory: &[RegisteredMemory],
        deadline: Instant,
    ) -> Result<()> {
        let memory: Vec<_> = memory
            .iter()
            .filter(|memory| self.direct(memory))
            .cloned()
            .collect();
        if memory.is_empty() {
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
                    owner.asynchronous.check_device()?;
                    available |= 1u64 << id;
                    devices.push(Arc::clone(owner));
                }
            }
            if available & self.config.device_mask == 0 {
                return Err(Error::Unavailable("no RDMA context matches device mask"));
            }
            devices
        };
        self.registrations.publish(&devices, &memory, deadline)
    }

    /// Submission readiness is sampled once; native dispatch rechecks its owners.
    pub(crate) fn check_ready(&self) -> Result<()> {
        self.recover_progress()?;
        let usage = self
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        if usage.retained_endpoints != 0
            || usage.retained_devices != 0
            || usage.retained_cqs != 0
            || usage.retained_persistent_mrs != 0
            || usage.progress_failures != usage.progress_recoveries
        {
            return Err(Error::Unavailable("RDMA retained native resources"));
        }
        Ok(())
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn rail_health_stats(&self) -> RdmaStats {
        let mut stats = RdmaStats::default();
        self.workers.monitor.rails.snapshot(&mut stats);
        stats
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
        self.workers.monitor.stats(&mut stats);
        self.workers.monitor.rails.snapshot(&mut stats);
        Ok(stats)
    }

    pub(crate) fn admission_bandwidth(&self, mask: u64) -> f64 {
        // Recovery may own this lock. Missing evidence must never cause a drop.
        let Ok(contexts) = self.context_set.try_lock() else {
            return 0.0;
        };
        if contexts.closed {
            return 0.0;
        }
        contexts
            .selector
            .as_ref()
            .map_or(0.0, |selector| selector.admission_bandwidth(mask))
    }
    #[cfg(feature = "metrics")]
    pub(crate) fn quota_totals(&self) -> (u64, u64) {
        let contexts = self.context_set.lock().unwrap_or_else(|p| p.into_inner());
        contexts
            .selector
            .as_ref()
            .map_or((0, contexts.retired_allocated_bytes), |s| s.quota_totals())
    }
    pub(crate) fn nic_load_stats(&self) -> Result<Vec<super::NicLoadStats>> {
        let contexts = self
            .context_set
            .lock()
            .map_err(|_| Error::Internal("RDMA context set lock poisoned"))?;
        if contexts.closed {
            return Ok(Vec::new());
        }
        let mut stats = contexts
            .selector
            .as_ref()
            .map_or_else(Vec::new, |s| s.stats());
        drop(contexts);
        self.workers.monitor.rails.nic_snapshot(&mut stats);
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

fn close_slices(slices: &mut [RdmaSlice], mut result: Result<()>, requester: bool) -> Result<()> {
    let retirement_deadline = Instant::now() + crate::transport::DESTRUCTION_TIMEOUT;
    for slice in slices {
        slice.grant.deadline = retirement_deadline;
        #[cfg(test)]
        if requester {
            super::tests::before_requester_close(&mut slice.grant, &result);
        }
        let _ = requester;
        if let Err(error) = slice.grant.close() {
            if result.is_ok() || matches!(error, Error::Unquiesced { .. }) {
                result = Err(error);
            }
        }
    }
    result
}

impl RdmaTransport {
    pub(crate) fn wake_cancel(&self) {
        self.workers.wake_cancel();
    }

    pub(crate) fn transfer(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        deadline: Instant,
        posted: &mut bool,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let mut effect = RemoteEffect::None;
        let result = (|| {
            if request
                .rdma_cancel
                .as_ref()
                .is_some_and(|c| c.load(Ordering::Acquire))
            {
                return Err(Error::Cancelled);
            }
            let location = request.local.location()?.location;
            let local_paths = self.request_paths(location, &request.selection)?;
            if (local_paths.len() > 1 || self.qp_width() > 1)
                && plan_rdma_slices(request.length, self.config.block_size, 64)?.1 > 1
            {
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                return self.transfer_slices(
                    operation,
                    peer,
                    request,
                    epoch,
                    &mut channel,
                    (&mut effect, posted),
                );
            }
            let credits =
                GrantCredits::acquire(Arc::clone(&self.usage), self.config.max_grants, 1)?;
            let wire = data_request(operation, peer, request);
            let mut pinned = None;
            // A recovered peer can discard every cached QP in this array.
            // Each miss closes one stale pair before another bootstrap; no
            // payload has been submitted and the original deadline still applies.
            for _ in 0..=self.qp_width() {
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                channel.write_all(&wire.encode())?;
                let (mut remote_paths, remote_width) = path::read_paths(&mut channel, peer.epoch)?;
                let width = self.peer_width(remote_width)?;
                let mut paths = self.request_paths(location, &request.selection)?;
                if let Some((local, remote)) = pinned {
                    paths.retain(|path| path.domain == local);
                    remote_paths.retain(|path| path.domain == remote);
                }
                let rails = &self.workers.monitor.rails;
                let (mask, _) = rails.mask(peer.epoch, &paths, &remote_paths, rails.now())?;
                if mask == 0 {
                    return Err(request
                        .selection
                        .rejected("no permitted NIC has a compatible peer path"));
                }
                let selection = self.allocate(request.length, location, mask)?;
                let local = paths
                    .iter()
                    .find(|path| path.nic_id == selection.device_id())
                    .ok_or(Error::Internal("RDMA selector returned a masked NIC path"))?;
                let (remote, rail) = rails
                    .admit(peer.epoch, local, &remote_paths, rails.now())
                    .map_err(|e| match e {
                        Error::Unavailable(reason) => request.selection.rejected(reason),
                        e => e,
                    })?;
                pinned = Some((local.domain, remote.domain));
                let setup = requester::RequesterSetup::new(
                    wire,
                    request.clone(),
                    epoch,
                    remote.clone(),
                    {
                        let mut lease = self.reserve_grant(selection, &request.local, &credits)?;
                        self.route_grant(&mut lease, &request.selection)?;
                        lease
                    },
                    width,
                );
                let pending = self
                    .requester_slice(&mut channel, setup)
                    .inspect_err(|e| rail.setup_failed(e))?;
                let Some(mut slice) = pending else {
                    continue;
                };
                let transfer = (|| {
                    *posted = true;
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                    }
                    slice
                        .grant
                        .transfer(&slice.remote, operation, request, deadline)?;
                    self.finish_requester_slice(&mut channel, &mut slice, &request.local)
                })();
                let drained = close_slices(std::slice::from_mut(&mut slice), transfer, true);
                rail.receipt(&drained);
                return drained;
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
        channel: &mut ControlChannel<'_>,
        (effect, posted): (&mut RemoteEffect, &mut bool),
    ) -> Result<()> {
        let transfer = self
            .submit_slices(operation, peer, request, epoch, channel, 1)
            .map_err(|(error, observed, started)| {
                *posted = started;
                *effect = observed;
                error
            })?;
        if operation == TransferOp::Write {
            *effect = RemoteEffect::Unknown;
        }
        *posted = true;
        transfer.finish(self, channel)
    }

    pub(crate) fn submit_slices(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        channel: &mut ControlChannel<'_>,
        concurrency: usize,
    ) -> std::result::Result<RdmaTransfer, (Error, RemoteEffect, bool)> {
        let mut setup = RdmaSetup::new(self, operation, peer, request, epoch, concurrency)
            .map_err(|error| (error, RemoteEffect::None, false))?;
        loop {
            let mut advanced = false;
            match setup.progress(self, channel, &mut advanced) {
                Ok(Some(transfer)) => return Ok(transfer),
                Ok(None) => {
                    if !advanced {
                        std::thread::park_timeout(Duration::from_micros(100));
                    }
                }
                Err(error) => {
                    let effect = setup.effect;
                    let posted = setup.payload_started;
                    return Err((setup.abort(self, error), effect, posted));
                }
            }
        }
    }

    pub(crate) fn serve(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
    ) -> Result<()> {
        match self.prepare_source(channel, epoch, request, memory, 1)? {
            Some(transfer) => transfer.finish(self, channel),
            None => Ok(()),
        }
    }

    pub(crate) fn prepare_source(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
        concurrency: usize,
    ) -> Result<Option<RdmaTransfer>> {
        if !(1..=16).contains(&concurrency) {
            return Err(Error::InvalidConfig(
                "RDMA CROSS concurrency must be 1..=16",
            ));
        }
        if request.epoch != epoch {
            return Err(Error::EpochMismatch);
        }
        let offset = usize::try_from(request.offset).map_err(|_| Error::InvalidRange)?;
        let length = usize::try_from(request.length).map_err(|_| Error::InvalidRange)?;
        crate::memory::checked_range(offset, length, memory.len())?;
        let paths = self.paths(memory.location()?.location)?;
        let multi = matches!(
            request.operation,
            Operation::RdmaReadSlices | Operation::RdmaWriteSlices
        );
        let qp_width = if self.direct(memory) && !memory.is_cuda() {
            self.qp_width()
        } else {
            0
        };
        let plan = if multi {
            path::write_paths(channel, epoch, &paths, qp_width)?;
            let grants = if self.direct(memory) && !memory.is_cuda() {
                self.config.max_grants
            } else {
                self.config.max_grants.min(self.config.max_endpoints)
            };
            let limit = (grants.min(self.config.max_cqes) / concurrency).min(64) as u8;
            if limit == 0 {
                return Err(Error::ResourceExhausted);
            }
            channel.write_all(&[limit])?;
            SliceRequest::read_plan(channel, length, limit, &paths)?
        } else {
            Vec::new()
        };
        let mut admission = ParentAdmission::acquire(self, if multi { plan.len() } else { 1 })?;
        if multi {
            channel.reply(epoch, 9, 0)?;
        } else {
            path::write_paths(channel, epoch, &paths, qp_width)?;
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
                    for _ in 0..=qp_width {
                        if let Some(slice) = self.source_slice(
                            channel,
                            &child,
                            memory,
                            &paths,
                            Some(range.domain),
                            &mut admission,
                        )? {
                            prepared = Some(slice);
                            break;
                        }
                    }
                    slices.push(prepared.ok_or(Error::Protocol("RDMA repeated slice cache miss"))?);
                }
            } else if let Some(slice) =
                self.source_slice(channel, request, memory, &paths, None, &mut admission)?
            {
                slices.push(slice);
            } else {
                return Ok(());
            }
            Ok(())
        })();
        if let Err(error) = result {
            close_slices(&mut slices, Err(error), false)?;
        }
        Ok((!slices.is_empty()).then(|| RdmaTransfer {
            slices,
            memory: memory.clone(),
            requester: false,
            epoch,
            #[cfg(feature = "cuda")]
            finish: finish::FinishProgress::default(),
        }))
    }
}

impl Drop for RdmaTransport {
    fn drop(&mut self) {
        // Detach and wait for the current tick before the last context owner can
        // disappear. Native owners must never be dropped by their ACK thread.
        self.workers.monitor.unwatch_contexts();
    }
}

pub(super) mod connections;
mod contexts;
#[cfg(feature = "cuda")]
mod finish;
mod handshake;
mod requester;
mod setup;
mod transfer;
pub(crate) use setup::RdmaSetup;
pub(crate) use transfer::RdmaTransfer;

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/rdma_transport.rs"]
mod tests;
