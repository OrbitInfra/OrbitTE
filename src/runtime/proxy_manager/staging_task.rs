//! One session's bounded CROSS owners and scoped remote phase progress.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use super::remote_stage_operation::{ControlOperation, RemoteStageOperation};
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::runtime::control_plane::{
    Connecting, ControlChannel, Operation, REQUEST_SIZE, Reply, Request, Stop,
};
use crate::transport::tcp::TcpTask;
use crate::{
    Error, Permission, RegisteredMemory, RemoteEffect, RemoteRegion, RemoteSegment, Result,
    SourceEpoch, TransferOp, Transport,
};

pub(super) const WORKER_STOPPED: &str = "staging worker stopped before completion";

pub(super) enum Command {
    Cross {
        local: RegisteredMemory,
        offset: usize,
    },
    Unpin,
}

pub(super) struct StagingTask<'a> {
    channel: ControlChannel<'a>,
    remote: RemoteStageOperation,
    operation: TransferOp,
    peer: &'a RemoteSegment,
    request: &'a PreparedTransfer,
    source: &'a Source,
    pub(super) chunk: usize,
    remote_width: usize,
    cross_width: usize,
}

impl<'a> StagingTask<'a> {
    pub(super) fn new(
        operation: TransferOp,
        peer: &'a RemoteSegment,
        request: &'a PreparedTransfer,
        source: &'a Source,
        chunk_size: usize,
        deadline: Instant,
        stopping: &'a AtomicBool,
    ) -> Result<Self> {
        let cross_limit = match request.transport {
            #[cfg(feature = "rdma")]
            Transport::Rdma => source
                .rdma
                .as_ref()
                .ok_or(Error::Unavailable("RDMA is not configured"))?
                .staging_concurrency(chunk_size),
            _ => 1,
        };
        let mut connecting = Connecting::new(peer.endpoint, deadline, Stop::Task(stopping))?;
        let mut idle = 0u32;
        let mut channel = loop {
            if let Some(channel) = connecting.poll()? {
                break channel;
            }
            if idle < 128 {
                std::thread::yield_now();
            } else {
                let delay = Duration::from_micros((1u64 << (idle - 128).min(8)).min(200));
                std::thread::park_timeout(delay.min(remaining(deadline)?));
            }
            idle = idle.saturating_add(1);
        };
        channel.write_all(
            &Request {
                operation: if operation == TransferOp::Read {
                    Operation::PinRead
                } else {
                    Operation::PinWrite
                },
                epoch: peer.epoch,
                region: request.remote.id,
                offset: request.remote_offset as u64,
                length: request.length as u64,
            }
            .encode(),
        )?;
        channel.write_all(&[
            match request.transport {
                Transport::Tcp => 0,
                Transport::Rdma => 1,
                _ => return Err(Error::Unavailable("remote staging requires TCP or RDMA")),
            },
            source.staging_pipeline_depth as u8,
        ])?;
        channel.write_all(&(chunk_size as u64).to_be_bytes())?;
        channel.write_all(&[cross_limit as u8])?;
        let pin = Reply::read(&mut channel, peer.epoch)?;
        let chunk = usize::try_from(pin.length).map_err(|_| Error::InvalidRange)?;
        if pin.status != 0 || chunk == 0 || chunk > chunk_size {
            return Err(Error::Protocol("Pin chunk size"));
        }
        let mut nonce = [0; 16];
        channel.read_exact(&mut nonce)?;
        let mut width = [0; 2];
        channel.read_exact(&mut width)?;
        let remote_width = width[0] as usize;
        if remote_width == 0
            || remote_width > source.staging_pipeline_depth
            || remote_width > request.length.div_ceil(chunk)
        {
            return Err(Error::Protocol("Pin pipeline depth"));
        }
        let cross_width = width[1] as usize;
        if cross_width == 0 || cross_width > cross_limit || cross_width > remote_width {
            return Err(Error::Protocol("Pin CROSS depth"));
        }
        if stopping.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        remaining(deadline)?;
        Ok(Self {
            channel,
            remote: RemoteStageOperation {
                nonce: SourceEpoch(nonce),
                region: request.remote.id,
            },
            operation,
            peer,
            request,
            source,
            chunk,
            remote_width,
            cross_width,
        })
    }

    pub(super) fn progress(
        mut self,
        commands: Receiver<Command>,
        complete: SyncSender<usize>,
        stopping: &AtomicBool,
        effect: &mut RemoteEffect,
    ) -> Result<()> {
        let mut inflight: Vec<Option<Cross>> = (0..self.remote_width).map(|_| None).collect();
        let result =
            self.transfer_event_loop(&commands, &complete, stopping, effect, &mut inflight);
        #[cfg(feature = "rdma")]
        let mut result = result;
        #[cfg(feature = "rdma")]
        for cross in inflight.iter_mut().flatten() {
            if let Some(setup) = cross.setup.take() {
                let error = result
                    .err()
                    .unwrap_or(Error::Internal("unfinished RDMA setup"));
                result = Err(match self.source.rdma.as_ref() {
                    Some(transport) => setup.abort(transport, error),
                    None => Error::Internal("missing RDMA setup transport"),
                });
            }
        }
        for cross in inflight.into_iter().flatten() {
            #[cfg(feature = "rdma")]
            if let Some(transfer) = cross.rdma {
                result = transfer.close(result);
            }
            drop(cross.local);
        }
        result
    }

    fn transfer_event_loop(
        &mut self,
        commands: &Receiver<Command>,
        complete: &SyncSender<usize>,
        stopping: &AtomicBool,
        effect: &mut RemoteEffect,
        inflight: &mut [Option<Cross>],
    ) -> Result<()> {
        let mut remote_operations = vec![None; self.remote_width];
        let mut delegates = VecDeque::new();
        let mut queries = VecDeque::new();
        if self.operation == TransferOp::Read {
            for slot in 0..self.remote_width {
                delegates.push_back((slot * self.chunk, None));
            }
        }
        let mut control: Option<(ControlAction, ControlOperation)> = None;
        let mut expected = 0;
        let mut pending = None;
        let mut query_cursor = 0;
        let mut idle = 0u32;
        loop {
            remaining(self.channel.deadline)?;
            if stopping.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            let mut progress = false;
            let mut finished = None;
            if let Some((action, operation)) = control.as_mut() {
                let mut advanced = false;
                #[allow(unused_mut)]
                let mut ready = operation.progress_send(&mut self.channel, &mut advanced)?;
                #[cfg(feature = "rdma")]
                if ready {
                    match action {
                        ControlAction::Cross { offset } => {
                            let cross = inflight[(*offset / self.chunk) % self.remote_width]
                                .as_mut()
                                .ok_or(Error::Internal("missing staging CROSS"))?;
                            if let Some(setup) = cross.setup.as_mut() {
                                if let Some(transfer) = setup.progress(
                                    self.source
                                        .rdma
                                        .as_ref()
                                        .ok_or(Error::Unavailable("RDMA is not configured"))?,
                                    &mut self.channel,
                                    &mut advanced,
                                )? {
                                    cross.rdma = Some(transfer);
                                    cross.setup = None;
                                } else {
                                    ready = false;
                                }
                            }
                        }
                        ControlAction::Finish { offset } => {
                            let cross = inflight[(*offset / self.chunk) % self.remote_width]
                                .as_mut()
                                .ok_or(Error::Internal("missing staging CROSS"))?;
                            if let Some(transfer) = cross.rdma.as_mut() {
                                ready = transfer.progress_finish(
                                    self.source
                                        .rdma
                                        .as_ref()
                                        .ok_or(Error::Unavailable("RDMA is not configured"))?,
                                    &mut self.channel,
                                    &mut advanced,
                                )?;
                            }
                        }
                        _ => {}
                    }
                }

                let result = if ready {
                    operation.progress(&mut self.channel, &mut advanced)?
                } else {
                    None
                };
                if !matches!(action, ControlAction::Query { .. }) {
                    progress |= advanced;
                }
                if let ControlAction::Delegate { completion, .. } = action {
                    if operation.sent() {
                        if let Some(offset) = completion.take() {
                            complete.send(offset).map_err(|_| Error::Cancelled)?;
                        }
                    }
                }
                if let Some(ready) = result {
                    let (action, _) = control
                        .take()
                        .ok_or(Error::Internal("missing control operation"))?;
                    match action {
                        ControlAction::Delegate { offset, .. } => {
                            remote_operations[(offset / self.chunk) % self.remote_width] =
                                Some(offset);
                            queries.push_back((offset / self.chunk) % self.remote_width);
                        }
                        ControlAction::Query { slot } => {
                            if ready {
                                remote_operations[slot] = None;
                            }
                        }
                        #[cfg(feature = "rdma")]
                        ControlAction::Cross { .. } => {}
                        ControlAction::Finish { offset } => {
                            let slot = (offset / self.chunk) % self.remote_width;
                            let cross = inflight[slot]
                                .take()
                                .ok_or(Error::Internal("missing staging CROSS"))?;
                            #[cfg(feature = "rdma")]
                            if let Some(transfer) = cross.rdma {
                                transfer.close(Ok(()))?;
                            }
                            drop(cross.local);
                            finished = Some(offset);
                        }
                        ControlAction::Unpin => return Ok(()),
                    }
                    // A full NOT_READY query is not phase progress.
                    progress |= ready;
                }
            }
            let mut tcp_active = false;
            for cross in inflight.iter_mut().flatten() {
                if let Some(tcp) = cross.tcp.as_mut() {
                    tcp_active = true;
                    if control.is_none()
                        && tcp.progress(&mut self.channel, &self.source.proxy, &mut progress)?
                    {
                        let offset = cross.offset;
                        let length = self.chunk.min(self.request.length - offset);
                        control = Some((
                            ControlAction::Finish { offset },
                            self.remote.submit(Operation::FinishCross, offset, length),
                        ));
                        progress = true;
                    }
                } else if !control.as_ref().is_some_and(|(action, _)| {
                    matches!(action, ControlAction::Finish { offset } if *offset == cross.offset)
                }) {
                    // The active finish owns its SEND/drain phase; all other
                    // native payloads remain observable during partial frames.
                    cross.ready = cross.query()?;
                }
            }
            if control.is_none() && !tcp_active && finished.is_none() {
                if let Some(slot) = inflight
                    .iter()
                    .position(|cross| cross.as_ref().is_some_and(|c| c.ready))
                {
                    let offset = inflight[slot]
                        .as_ref()
                        .ok_or(Error::Internal("missing staging CROSS"))?
                        .offset;
                    control = Some((
                        ControlAction::Finish { offset },
                        self.remote.submit(
                            Operation::FinishCross,
                            offset,
                            self.chunk.min(self.request.length - offset),
                        ),
                    ));
                    progress = true;
                }
            }
            if let Some(offset) = finished {
                let next = if self.operation == TransferOp::Write {
                    Some(offset)
                } else {
                    let next = offset + self.chunk * self.remote_width;
                    (next < self.request.length).then_some(next)
                };
                if let Some(next) = next {
                    delegates.push_back((next, Some(offset)));
                } else {
                    complete.send(offset).map_err(|_| Error::Cancelled)?;
                }
            }
            if control.is_none() && !tcp_active {
                if let Some((offset, completion)) = delegates.pop_front() {
                    if self.operation == TransferOp::Write {
                        *effect = RemoteEffect::Unknown;
                    }
                    control = Some((
                        ControlAction::Delegate { offset, completion },
                        self.remote.submit(
                            Operation::Delegate,
                            offset,
                            self.chunk.min(self.request.length - offset),
                        ),
                    ));
                    progress = true;
                } else if let Some(slot) = queries.pop_front() {
                    let offset = remote_operations[slot]
                        .ok_or(Error::Internal("missing remote operation"))?;
                    control = Some((
                        ControlAction::Query { slot },
                        self.remote.submit(
                            Operation::QueryDelegate,
                            offset,
                            self.chunk.min(self.request.length - offset),
                        ),
                    ));
                } else {
                    'admission: {
                        if inflight.iter().filter(|slot| slot.is_some()).count() >= self.cross_width
                        {
                            break 'admission;
                        }
                        if stopping.load(Ordering::Acquire) {
                            return Err(Error::Cancelled);
                        }
                        let command = match pending.take() {
                            Some(command) => command,
                            None => match commands.try_recv() {
                                Ok(command) => command,
                                Err(mpsc::TryRecvError::Empty) => break 'admission,
                                Err(mpsc::TryRecvError::Disconnected) => {
                                    return Err(Error::Cancelled);
                                }
                            },
                        };
                        match command {
                            Command::Cross { local, offset } => {
                                if offset != expected || offset >= self.request.length {
                                    return Err(Error::Internal("staging CROSS submission order"));
                                }
                                let slot = (offset / self.chunk) % self.remote_width;
                                if inflight[slot].is_some() || remote_operations[slot].is_some() {
                                    pending = Some(Command::Cross { local, offset });
                                    break 'admission;
                                }
                                let length = self.chunk.min(self.request.length - offset);
                                inflight[slot] = Some(self.cross(local, offset, length)?);
                                expected += length;
                                progress = true;
                                if self.request.transport == Transport::Tcp {
                                    tcp_active = true;
                                } else {
                                    #[cfg(feature = "rdma")]
                                    {
                                        control = Some((
                                            ControlAction::Cross { offset },
                                            self.remote.submit(Operation::Cross, offset, length),
                                        ));
                                    }
                                }
                            }
                            Command::Unpin => {
                                if expected != self.request.length
                                    || inflight.iter().any(Option::is_some)
                                {
                                    return Err(Error::Internal("staging Unpin before all CROSS"));
                                }
                                if remote_operations.iter().any(Option::is_some) {
                                    pending = Some(Command::Unpin);
                                    break 'admission;
                                }
                                control = Some((
                                    ControlAction::Unpin,
                                    self.remote.submit(Operation::Unpin, expected, 0),
                                ));
                                progress = true;
                            }
                        }
                    }
                    if control.is_none() && !tcp_active {
                        for _ in 0..self.remote_width {
                            let slot = query_cursor;
                            query_cursor = (query_cursor + 1) % self.remote_width;
                            if let Some(offset) = remote_operations[slot] {
                                control = Some((
                                    ControlAction::Query { slot },
                                    self.remote.submit(
                                        Operation::QueryDelegate,
                                        offset,
                                        self.chunk.min(self.request.length - offset),
                                    ),
                                ));
                                break;
                            }
                        }
                    }
                }
            }
            if progress {
                idle = 0;
            } else {
                if idle < 128 {
                    std::thread::yield_now();
                } else {
                    let delay = Duration::from_micros((1u64 << (idle - 128).min(8)).min(200));
                    std::thread::park_timeout(delay.min(remaining(self.channel.deadline)?));
                }
                idle = idle.saturating_add(1);
            }
        }
    }

    fn cross(&mut self, local: RegisteredMemory, offset: usize, length: usize) -> Result<Cross> {
        let cross = PreparedTransfer {
            #[cfg(feature = "cuda")]
            remote_staging: false,
            transport: self.request.transport,
            local,
            local_offset: 0,
            remote: RemoteRegion {
                epoch: self.peer.epoch,
                id: self
                    .remote
                    .stage_id((offset / self.chunk) % self.remote_width),
                address: 0,
                length: self.chunk,
                permission: Permission::RemoteReadWrite,
            },
            remote_offset: 0,
            length,
        };
        if cross.transport == Transport::Tcp {
            let task = TcpTask::new(
                self.operation,
                self.peer,
                &cross,
                self.channel.deadline,
                &self.source.proxy,
            )?;
            let header = Request {
                operation: Operation::Cross,
                epoch: self.remote.nonce,
                region: self.remote.region,
                offset: offset as u64,
                length: length as u64,
            }
            .encode();
            return Ok(Cross {
                local: cross.local,
                offset,
                ready: false,
                tcp: Some(TcpCross {
                    header,
                    sent: 0,
                    task,
                    receipt: self.remote.receive(Operation::Cross, offset, length),
                }),
                #[cfg(feature = "rdma")]
                rdma: None,
                #[cfg(feature = "rdma")]
                setup: None,
            });
        }
        #[cfg(not(feature = "rdma"))]
        {
            Err(Error::Unavailable("build with --features rdma"))
        }
        #[cfg(feature = "rdma")]
        {
            if cross.transport != Transport::Rdma {
                return Err(Error::Unavailable("remote staging requires TCP or RDMA"));
            }
            let setup = crate::transport::rdma::RdmaSetup::new(
                self.source
                    .rdma
                    .as_ref()
                    .ok_or(Error::Unavailable("RDMA is not configured"))?,
                self.operation,
                self.peer,
                &cross,
                self.source.epoch,
                self.cross_width,
            )?;
            Ok(Cross {
                local: cross.local,
                offset,
                ready: false,
                tcp: None,
                rdma: None,
                setup: Some(setup),
            })
        }
    }
}

struct Cross {
    local: RegisteredMemory,
    offset: usize,
    ready: bool,
    tcp: Option<TcpCross>,
    #[cfg(feature = "rdma")]
    rdma: Option<crate::transport::rdma::RdmaTransfer>,
    #[cfg(feature = "rdma")]
    setup: Option<crate::transport::rdma::RdmaSetup>,
}

impl Cross {
    fn query(&self) -> Result<bool> {
        #[cfg(feature = "rdma")]
        if self.setup.is_some() {
            return Ok(false);
        }
        #[cfg(feature = "rdma")]
        if let Some(transfer) = &self.rdma {
            return transfer.query();
        }
        Ok(true)
    }
}

enum ControlAction {
    #[cfg(feature = "rdma")]
    Cross {
        offset: usize,
    },
    Delegate {
        offset: usize,
        completion: Option<usize>,
    },
    Query {
        slot: usize,
    },
    Finish {
        offset: usize,
    },
    Unpin,
}

struct TcpCross {
    header: [u8; REQUEST_SIZE],
    sent: usize,
    task: TcpTask,
    receipt: ControlOperation,
}

impl TcpCross {
    fn progress(
        &mut self,
        channel: &mut ControlChannel<'_>,
        proxy: &std::sync::Arc<super::ProxyManager>,
        advanced: &mut bool,
    ) -> Result<bool> {
        if self.sent != REQUEST_SIZE {
            let count = channel.poll_write(&self.header[self.sent..])?;
            self.sent += count;
            *advanced |= count != 0;
            return Ok(false);
        }
        if !self.task.progress(channel, advanced, proxy)? {
            return Ok(false);
        }
        Ok(self.receipt.progress(channel, advanced)?.is_some())
    }
}

pub(super) fn receive<T>(receiver: &Receiver<T>, deadline: Instant) -> Result<T> {
    receiver
        .recv_timeout(remaining(deadline)?)
        .map_err(|error| match error {
            mpsc::RecvTimeoutError::Timeout => Error::Timeout,
            mpsc::RecvTimeoutError::Disconnected => Error::Internal(WORKER_STOPPED),
        })
}
