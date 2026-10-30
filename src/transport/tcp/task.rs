//! One TCP request owns its partial frames, payload snapshot and destination.

use std::time::Instant;

use super::TcpTransport;
use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{
    ControlChannel, Operation, REPLY_SIZE, REQUEST_SIZE, Reply, Request,
};
use crate::{Error, RemoteEffect, RemoteSegment, Result, TransferOp};
pub(crate) struct TcpTask {
    request: PreparedTransfer,
    frame: [u8; REQUEST_SIZE],
    reply: [u8; REPLY_SIZE],
    payload: Vec<u8>,
    state: State,
    position: usize,
    operation: TransferOp,
    effect: RemoteEffect,
}

enum State {
    Request,
    Reply,
    Payload,
    Commit,
    Ack,
    Done,
}

impl TcpTask {
    pub(crate) fn new(
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        deadline: Instant,
        #[cfg(feature = "cuda")] proxy: &std::sync::Arc<
            crate::runtime::proxy_manager::ProxyManager,
        >,
    ) -> Result<Self> {
        let payload = TcpTransport::payload(
            operation,
            request,
            deadline,
            #[cfg(feature = "cuda")]
            proxy,
        )?;
        Ok(Self {
            request: request.clone(),
            frame: Request {
                operation: if operation == TransferOp::Read {
                    Operation::TcpRead
                } else {
                    Operation::TcpWrite
                },
                epoch: peer.epoch,
                region: request.remote.id,
                offset: request.remote_offset as u64,
                length: request.length as u64,
            }
            .encode(),
            reply: [0; REPLY_SIZE],
            payload,
            state: State::Request,
            position: 0,
            operation,
            effect: RemoteEffect::None,
        })
    }

    pub(crate) fn effect(&self) -> RemoteEffect {
        self.effect
    }

    /// One phase and at most 64 KiB of socket I/O per turn.
    pub(crate) fn progress(
        &mut self,
        channel: &mut ControlChannel<'_>,
        advanced: &mut bool,
        #[cfg(feature = "cuda")] proxy: &std::sync::Arc<
            crate::runtime::proxy_manager::ProxyManager,
        >,
    ) -> Result<bool> {
        remaining(channel.deadline)?;
        let next = match self.state {
            State::Request => {
                let count = channel.poll_write(&self.frame[self.position..])?;
                self.position += count;
                *advanced |= count != 0;
                (self.position == self.frame.len()).then_some(State::Reply)
            }
            State::Reply | State::Ack => {
                let count = channel.poll_read(&mut self.reply[self.position..])?;
                self.position += count;
                *advanced |= count != 0;
                if self.position != self.reply.len() {
                    return Ok(false);
                }
                let reply = Reply::decode(&self.reply, self.request.remote.epoch)?;
                if matches!(self.state, State::Ack) {
                    if reply.status != 0 || reply.length != 0 {
                        return Err(Error::Protocol("WRITE acknowledgement length/status"));
                    }
                    Some(State::Done)
                } else {
                    if self.operation == TransferOp::Read {
                        if reply.status != 0 || reply.length != self.payload.len() as u64 {
                            return Err(Error::Protocol("READ payload length/status"));
                        }
                    } else if reply.status != 9 {
                        return Err(Error::Protocol("WRITE missing ready response"));
                    }
                    Some(State::Payload)
                }
            }
            State::Payload => {
                let count = if self.operation == TransferOp::Read {
                    channel.poll_read(&mut self.payload[self.position..])?
                } else {
                    self.effect = RemoteEffect::Unknown;
                    channel.poll_write(&self.payload[self.position..])?
                };
                self.position += count;
                *advanced |= count != 0;
                (self.position == self.payload.len()).then_some(
                    if self.operation == TransferOp::Read {
                        State::Commit
                    } else {
                        State::Ack
                    },
                )
            }
            State::Commit => {
                #[cfg(feature = "cuda")]
                proxy.write(
                    &self.request.local,
                    self.request.local_offset,
                    &self.payload,
                    channel.deadline,
                )?;
                #[cfg(not(feature = "cuda"))]
                self.request
                    .local
                    .write(self.request.local_offset, &self.payload)?;
                Some(State::Done)
            }
            State::Done => return Ok(true),
        };
        if let Some(next) = next {
            self.state = next;
            self.position = 0;
            *advanced = true;
        }
        Ok(matches!(self.state, State::Done))
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/tcp/task.rs"]
mod tests;
