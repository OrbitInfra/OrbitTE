//! RDMA transport entry points: registration, parent dispatch and source serving.

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
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
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
            context_set: Arc::default(),
            drained: Arc::default(),
            registrations,
        }))
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

const ENDPOINT_DESTROY_TIMEOUT: Duration = Duration::from_secs(30);

fn close_slices(slices: &mut [RdmaSlice], mut result: Result<()>, requester: bool) -> Result<()> {
    let retirement_deadline = Instant::now() + ENDPOINT_DESTROY_TIMEOUT;
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
            let credits =
                GrantCredits::acquire(Arc::clone(&self.usage), self.config.max_grants, 1)?;
            let wire = data_request(operation, peer, request);
            let mut pinned = None;
            // A recovered peer can discard every cached QP in this array.
            // Each miss closes one stale pair before another bootstrap; no
            // payload has been submitted and the original deadline still applies.
            for _ in 0..=self.config.num_lanes {
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                channel.write_all(&wire.encode())?;
                let (mut remote_paths, remote_width) = path::read_paths(&mut channel, peer.epoch)?;
                let mut paths = local_paths.clone();
                if let Some((local, remote)) = pinned {
                    paths.retain(|path| path.domain == local);
                    remote_paths.retain(|path| path.domain == remote);
                }
                let selection = self.allocate(
                    request.length,
                    path::local_device_mask(&paths, &remote_paths),
                )?;
                let local = paths
                    .iter()
                    .find(|path| path.nic_id == selection.device_id())
                    .ok_or(Error::Internal("RDMA selector returned a masked NIC path"))?;
                let remote = path::remote_path(local, &remote_paths)?;
                pinned = Some((local.domain, remote.domain));
                let endpoint = self.requester_grant(
                    &channel,
                    &wire,
                    request,
                    self.reserve_grant(selection, &request.local, &credits)?,
                    remote,
                    remote_width.min(self.config.num_lanes),
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
                    slice.grant.transfer(&slice.remote, operation, deadline)?;
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
        let (remote_paths, remote_width) = path::read_paths(&mut channel, peer.epoch)?;
        let mut remote_limit = [0];
        channel.read_exact(&mut remote_limit)?;
        if !(1..=64).contains(&remote_limit[0]) {
            return Err(Error::Protocol("RDMA slice window limit"));
        }
        let cap = usize::from(remote_limit[0])
            .min(if self.direct(&request.local) {
                self.config.max_grants
            } else {
                self.config.max_grants.min(self.config.max_endpoints)
            })
            .min(self.config.max_cqes)
            .min(
                if operation == TransferOp::Write || request.local.is_cuda() {
                    32
                } else {
                    64
                },
            );
        let (block, count) = plan_rdma_slices(request.length, self.config.block_size, cap)?;
        let credits =
            GrantCredits::acquire(Arc::clone(&self.usage), self.config.max_grants, count)?;
        let mask = path::local_device_mask(&local_paths, &remote_paths);
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
            let remote = path::remote_path(local, &remote_paths)?;
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
                for _ in 0..=remote_width.min(self.config.num_lanes) {
                    let lease = match selection.take() {
                        Some(lease) => lease,
                        None => self.allocate(range.length, 1u64 << local_id)?,
                    };
                    let endpoint = self.requester_grant(
                        &channel,
                        &child_wire,
                        &child,
                        self.reserve_grant(lease, &child.local, &credits)?,
                        remote,
                        remote_width.min(self.config.num_lanes),
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
                slice.grant.progress.submit(
                    &slice.grant.record,
                    slice.grant.claim,
                    slice.remote,
                    operation,
                    deadline,
                )?;
            }
            for slice in &mut slices {
                slice
                    .grant
                    .progress
                    .wait(&slice.grant.record, slice.grant.claim, deadline)?;
                self.finish_requester_slice(&mut channel, slice, &request.local)?;
            }
            Ok(())
        })();
        close_slices(&mut slices, result, true)
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
        let qp_width = if self.direct(memory) && !memory.is_cuda() {
            self.config.num_lanes
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
            let limit = grants.min(self.config.max_cqes).min(64) as u8;
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
            for slice in &mut slices {
                self.finish_source_slice(channel, epoch, memory, slice)?;
            }
            Ok(())
        })();
        close_slices(&mut slices, result, false)
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
mod handshake;

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/rdma_transport.rs"]
mod tests;
