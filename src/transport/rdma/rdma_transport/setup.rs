//! Parent admission and requester slice setup before any payload can be submitted.

use std::collections::VecDeque;
use std::sync::Arc;

use super::{RdmaTransfer, RdmaTransport, close_slices, data_request, requester::RequesterSetup};
use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{ControlChannel, Operation, REPLY_SIZE, Reply, Request};
use crate::transport::rdma::{
    grant::admission::GrantCredits,
    path::{self, NicPath},
    protocol::SliceRequest,
    quota::DeviceLease,
    slice::{RdmaSlice, plan_rdma_slices},
};
use crate::{Error, RemoteEffect, RemoteSegment, Result, SourceEpoch, TransferOp};

enum Phase {
    Request,
    PathsReply,
    Paths,
    Window,
    Plan,
    Admission,
    Prepare,
    Slice,
    Submit(usize),
    Complete,
}

struct PlannedSlice {
    range: SliceRequest,
    selection: Option<DeviceLease>,
    remote: NicPath,
    local_id: usize,
}

pub(crate) struct RdmaSetup {
    operation: TransferOp,
    request: PreparedTransfer,
    wire: Request,
    epoch: SourceEpoch,
    concurrency: usize,
    local_paths: Vec<NicPath>,
    remote_paths: Vec<NicPath>,
    remote_width: usize,
    phase: Phase,
    bytes: Vec<u8>,
    position: usize,
    credits: Option<Arc<GrantCredits>>,
    plan: VecDeque<PlannedSlice>,
    active: Option<RequesterSetup>,
    retries: usize,
    slices: Vec<RdmaSlice>,
    pub(super) effect: RemoteEffect,
}

impl RdmaSetup {
    pub(crate) fn new(
        transport: &RdmaTransport,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        concurrency: usize,
    ) -> Result<Self> {
        if !(1..=16).contains(&concurrency) {
            return Err(Error::InvalidConfig(
                "RDMA CROSS concurrency must be 1..=16",
            ));
        }
        let mut wire = data_request(operation, peer, request);
        wire.operation = if operation == TransferOp::Read {
            Operation::RdmaReadSlices
        } else {
            Operation::RdmaWriteSlices
        };
        Ok(Self {
            operation,
            request: request.clone(),
            wire,
            epoch,
            concurrency,
            local_paths: transport.paths()?,
            remote_paths: Vec::new(),
            remote_width: 0,
            phase: Phase::Request,
            bytes: wire.encode().to_vec(),
            position: 0,
            credits: None,
            plan: VecDeque::new(),
            active: None,
            retries: 0,
            slices: Vec::new(),
            effect: RemoteEffect::None,
        })
    }

    pub(crate) fn progress(
        &mut self,
        transport: &RdmaTransport,
        channel: &mut ControlChannel<'_>,
        advanced: &mut bool,
    ) -> Result<Option<RdmaTransfer>> {
        remaining(channel.deadline)?;
        let write = matches!(self.phase, Phase::Request | Phase::Plan);
        if write
            || matches!(
                self.phase,
                Phase::PathsReply | Phase::Paths | Phase::Window | Phase::Admission
            )
        {
            let count = if write {
                channel.poll_write(&self.bytes[self.position..])?
            } else {
                channel.poll_read(&mut self.bytes[self.position..])?
            };
            self.position += count;
            *advanced |= count != 0;
            if self.position != self.bytes.len() {
                return Ok(None);
            }
        }
        self.phase = match self.phase {
            Phase::Request => {
                self.bytes.resize(REPLY_SIZE, 0);
                Phase::PathsReply
            }
            Phase::PathsReply => {
                let mut bytes = [0; REPLY_SIZE];
                bytes.copy_from_slice(&self.bytes);
                let reply = Reply::decode(&bytes, self.wire.epoch)?;
                self.bytes.resize(path::table_length(&reply)?, 0);
                Phase::Paths
            }
            Phase::Paths => {
                (self.remote_paths, self.remote_width) = path::decode_paths(&self.bytes)?;
                self.bytes.resize(1, 0);
                Phase::Window
            }
            Phase::Window => {
                self.plan(transport, self.bytes[0])?;
                self.bytes.clear();
                self.bytes.push(self.plan.len() as u8);
                for entry in &self.plan {
                    self.bytes.extend_from_slice(&entry.range.encode());
                }
                Phase::Plan
            }
            Phase::Plan => {
                self.bytes.resize(REPLY_SIZE, 0);
                Phase::Admission
            }
            Phase::Admission => {
                let mut bytes = [0; REPLY_SIZE];
                bytes.copy_from_slice(&self.bytes);
                let reply = Reply::decode(&bytes, self.wire.epoch)?;
                if reply.status != 9 || reply.length != 0 {
                    return Err(Error::Protocol("RDMA parent receive admission"));
                }
                Phase::Prepare
            }
            Phase::Prepare => {
                if let Some(entry) = self.plan.front_mut() {
                    if self.retries > self.remote_width.min(transport.config.num_lanes) {
                        return Err(Error::Protocol("RDMA repeated slice cache miss"));
                    }
                    let child = PreparedTransfer {
                        transport: self.request.transport,
                        local: self.request.local.clone(),
                        local_offset: self.request.local_offset + entry.range.offset,
                        remote: self.request.remote,
                        remote_offset: self.request.remote_offset + entry.range.offset,
                        length: entry.range.length,
                    };
                    let wire = Request {
                        operation: if self.operation == TransferOp::Read {
                            Operation::RdmaRead
                        } else {
                            Operation::RdmaWrite
                        },
                        epoch: self.wire.epoch,
                        region: self.wire.region,
                        offset: child.remote_offset as u64,
                        length: child.length as u64,
                    };
                    let selection = match entry.selection.take() {
                        Some(selection) => selection,
                        None => transport.allocate(entry.range.length, 1u64 << entry.local_id)?,
                    };
                    let endpoint = transport.requester_grant(
                        channel,
                        &wire,
                        &child,
                        transport.reserve_grant(
                            selection,
                            &child.local,
                            self.credits
                                .as_ref()
                                .ok_or(Error::Internal("missing parent grant credits"))?,
                        )?,
                        &entry.remote,
                        self.remote_width.min(transport.config.num_lanes),
                    )?;
                    self.active = Some(RequesterSetup::new(
                        wire,
                        child,
                        self.epoch,
                        entry.remote.clone(),
                        endpoint,
                    ));
                    Phase::Slice
                } else {
                    Phase::Submit(0)
                }
            }
            Phase::Slice => {
                if !self
                    .active
                    .as_mut()
                    .ok_or(Error::Internal("missing requester setup"))?
                    .progress(transport, channel, advanced)?
                {
                    return Ok(None);
                }
                let setup = self
                    .active
                    .take()
                    .ok_or(Error::Internal("missing requester setup"))?;
                if let Some(slice) = setup.finish()? {
                    self.slices.push(slice);
                    self.plan.pop_front();
                    self.retries = 0;
                } else {
                    self.retries += 1;
                }
                Phase::Prepare
            }
            Phase::Submit(index) => {
                if let Some(slice) = self.slices.get(index) {
                    // All grants have been validated before the first payload.
                    if self.operation == TransferOp::Write {
                        self.effect = RemoteEffect::Unknown;
                    }
                    slice.grant.progress.submit(
                        &slice.grant.record,
                        slice.grant.claim,
                        slice.remote,
                        self.operation,
                        channel.deadline,
                    )?;
                    Phase::Submit(index + 1)
                } else {
                    self.phase = Phase::Complete;
                    *advanced = true;
                    return Ok(Some(RdmaTransfer {
                        slices: std::mem::take(&mut self.slices),
                        memory: self.request.local.clone(),
                        requester: true,
                        epoch: self.wire.epoch,
                        #[cfg(feature = "cuda")]
                        finish: super::finish::FinishProgress::default(),
                    }));
                }
            }
            Phase::Complete => return Err(Error::Internal("RDMA parent setup already completed")),
        };
        self.position = 0;
        *advanced = true;
        Ok(None)
    }

    fn plan(&mut self, transport: &RdmaTransport, remote_limit: u8) -> Result<()> {
        if !(1..=64).contains(&remote_limit) {
            return Err(Error::Protocol("RDMA slice window limit"));
        }
        let cap = usize::from(remote_limit)
            .min(
                (if transport.direct(&self.request.local) {
                    transport.config.max_grants
                } else {
                    transport
                        .config
                        .max_grants
                        .min(transport.config.max_endpoints)
                }) / self.concurrency,
            )
            .min(transport.config.max_cqes / self.concurrency)
            .min(
                if self.operation == TransferOp::Write || self.request.local.is_cuda() {
                    32
                } else {
                    64
                },
            );
        let (block, count) =
            plan_rdma_slices(self.request.length, transport.config.block_size, cap)?;
        self.credits = Some(GrantCredits::acquire(
            Arc::clone(&transport.usage),
            transport.config.max_grants,
            count,
        )?);
        self.plan
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceExhausted)?;
        self.slices
            .try_reserve_exact(count)
            .map_err(|_| Error::ResourceExhausted)?;
        let mask = path::local_device_mask(&self.local_paths, &self.remote_paths);
        for index in 0..count {
            let offset = index * block;
            let length = if index + 1 == count {
                self.request.length - offset
            } else {
                block
            };
            let selection = transport.allocate(length, mask)?;
            let local = self
                .local_paths
                .iter()
                .find(|path| path.nic_id == selection.device_id())
                .ok_or(Error::Internal("RDMA selector returned a masked NIC path"))?;
            let remote = path::remote_path(local, &self.remote_paths)?;
            self.plan.push_back(PlannedSlice {
                range: SliceRequest {
                    offset,
                    length,
                    domain: remote.domain,
                },
                selection: Some(selection),
                remote: remote.clone(),
                local_id: local.nic_id,
            });
        }
        Ok(())
    }

    pub(crate) fn abort(mut self, transport: &RdmaTransport, mut error: Error) -> Error {
        if let Some(active) = self.active.take() {
            error = active.abort(transport, error);
        }
        match close_slices(&mut self.slices, Err(error), true) {
            Err(error) => error,
            Ok(()) => Error::Internal("RDMA setup lost its error"),
        }
    }
}
