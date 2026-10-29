//! One session's bounded CROSS owners and scoped remote phase progress.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use super::remote_stage_operation::RemoteStageOperation;
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
use crate::transport::tcp::TcpTransport;
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
        let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
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
        if self.operation == TransferOp::Read {
            for slot in 0..self.remote_width {
                if stopping.load(Ordering::Acquire) {
                    return Err(Error::Cancelled);
                }
                self.delegate(slot * self.chunk)?;
            }
        }
        let mut expected = 0;
        let mut pending = None;
        let mut idle = 0u32;
        loop {
            remaining(self.channel.deadline)?;
            if stopping.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            let mut progress = false;
            while inflight.iter().filter(|slot| slot.is_some()).count() < self.cross_width {
                if stopping.load(Ordering::Acquire) {
                    return Err(Error::Cancelled);
                }
                let command = match pending.take() {
                    Some(command) => command,
                    None => match commands.try_recv() {
                        Ok(command) => command,
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => return Err(Error::Cancelled),
                    },
                };
                match command {
                    Command::Cross { local, offset } => {
                        if offset != expected || offset >= self.request.length {
                            return Err(Error::Internal("staging CROSS submission order"));
                        }
                        let slot = (offset / self.chunk) % self.remote_width;
                        if inflight[slot].is_some() {
                            pending = Some(Command::Cross { local, offset });
                            break;
                        }
                        let length = self.chunk.min(self.request.length - offset);
                        inflight[slot] = Some(self.cross(local, offset, length)?);
                        expected += length;
                        progress = true;
                    }
                    Command::Unpin => {
                        if expected != self.request.length || inflight.iter().any(Option::is_some) {
                            return Err(Error::Internal("staging Unpin before all CROSS"));
                        }
                        self.remote
                            .send(&mut self.channel, Operation::Unpin, expected, 0)?;
                        return self.remote.receipt(
                            &mut self.channel,
                            Operation::Unpin,
                            expected,
                            0,
                        );
                    }
                }
            }
            for slot in inflight.iter_mut() {
                let Some(cross) = slot.as_ref() else { continue };
                if !cross.query()? {
                    continue;
                }
                let cross = slot
                    .take()
                    .ok_or(Error::Internal("missing staging CROSS"))?;
                let offset = cross.offset;
                self.finish_cross(cross)?;
                if stopping.load(Ordering::Acquire) {
                    return Err(Error::Cancelled);
                }
                let next = if self.operation == TransferOp::Write {
                    Some(offset)
                } else {
                    let next = offset + self.chunk * self.remote_width;
                    (next < self.request.length).then_some(next)
                };
                if let Some(next) = next {
                    if self.operation == TransferOp::Write {
                        *effect = RemoteEffect::Unknown;
                    }
                    self.remote.send(
                        &mut self.channel,
                        Operation::Delegate,
                        next,
                        self.chunk.min(self.request.length - next),
                    )?;
                }
                complete.send(offset).map_err(|_| Error::Cancelled)?;
                if let Some(next) = next {
                    self.remote.receipt(
                        &mut self.channel,
                        Operation::Delegate,
                        next,
                        self.chunk.min(self.request.length - next),
                    )?;
                }
                progress = true;
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

    fn finish_cross(&mut self, cross: Cross) -> Result<()> {
        let offset = cross.offset;
        let length = self.chunk.min(self.request.length - offset);
        #[cfg(feature = "rdma")]
        let mut rdma = cross.rdma;
        let result = (|| {
            self.remote
                .send(&mut self.channel, Operation::FinishCross, offset, length)?;
            #[cfg(feature = "rdma")]
            if let Some(transfer) = rdma.take() {
                transfer.finish(
                    self.source
                        .rdma
                        .as_ref()
                        .ok_or(Error::Unavailable("RDMA is not configured"))?,
                    &mut self.channel,
                )?;
            }
            self.remote
                .receipt(&mut self.channel, Operation::FinishCross, offset, length)
        })();
        #[cfg(feature = "rdma")]
        if let Some(transfer) = rdma {
            return transfer.close(result);
        }
        result
    }

    fn delegate(&mut self, offset: usize) -> Result<()> {
        let length = self.chunk.min(self.request.length - offset);
        self.remote
            .send(&mut self.channel, Operation::Delegate, offset, length)?;
        self.remote
            .receipt(&mut self.channel, Operation::Delegate, offset, length)
    }

    fn cross(&mut self, local: RegisteredMemory, offset: usize, length: usize) -> Result<Cross> {
        self.remote
            .send(&mut self.channel, Operation::Cross, offset, length)?;
        let cross = PreparedTransfer {
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
        #[cfg(feature = "rdma")]
        let mut rdma = None;
        match cross.transport {
            Transport::Tcp => TcpTransport::transfer_on(
                self.operation,
                self.peer,
                &cross,
                &mut self.channel,
                &self.source.proxy,
            )
            .map_err(|(error, _)| error)?,
            Transport::Rdma => {
                #[cfg(feature = "rdma")]
                {
                    rdma = Some(
                        self.source
                            .rdma
                            .as_ref()
                            .ok_or(Error::Unavailable("RDMA is not configured"))?
                            .submit_slices(
                                self.operation,
                                self.peer,
                                &cross,
                                self.source.epoch,
                                &mut self.channel,
                                self.cross_width,
                            )
                            .map_err(|(error, _)| error)?,
                    );
                }
                #[cfg(not(feature = "rdma"))]
                return Err(Error::Unavailable("build with --features rdma"));
            }
            _ => return Err(Error::Unavailable("remote staging requires TCP or RDMA")),
        }
        let receipt = self
            .remote
            .receipt(&mut self.channel, Operation::Cross, offset, length);
        #[cfg(feature = "rdma")]
        if let Err(error) = receipt {
            if let Some(transfer) = rdma {
                transfer.close(Err(error))?;
                return Err(Error::Internal("failed CROSS lost its error"));
            }
            return Err(error);
        }
        #[cfg(not(feature = "rdma"))]
        receipt?;
        Ok(Cross {
            local: cross.local,
            offset,
            #[cfg(feature = "rdma")]
            rdma,
        })
    }
}

struct Cross {
    local: RegisteredMemory,
    offset: usize,
    #[cfg(feature = "rdma")]
    rdma: Option<crate::transport::rdma::RdmaTransfer>,
}

impl Cross {
    fn query(&self) -> Result<bool> {
        #[cfg(feature = "rdma")]
        if let Some(transfer) = &self.rdma {
            return transfer.query();
        }
        Ok(true)
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
