//! Requester completion progress; the transfer keeps every native slice until drain.

use super::{RdmaTransfer, RdmaTransport};
use crate::engine::remaining;
use crate::runtime::control_plane::{ControlChannel, REPLY_SIZE, Reply};
use crate::transport::rdma::protocol::{ACK, BINDING_SIZE, DONE, READY};
use crate::{Error, Result, TransferOp};

#[derive(Default)]
enum Phase {
    #[default]
    Send,
    Completion,
    Drain,
    Done,
    Reply,
    Ack,
    Ready,
    Release,
}

pub(super) struct FinishProgress {
    slice: usize,
    phase: Phase,
    position: usize,
    bytes: [u8; 1 + BINDING_SIZE],
}

impl Default for FinishProgress {
    fn default() -> Self {
        Self {
            slice: 0,
            phase: Phase::Send,
            position: 0,
            bytes: [0; 1 + BINDING_SIZE],
        }
    }
}

impl RdmaTransfer {
    pub(crate) fn progress_finish(
        &mut self,
        transport: &RdmaTransport,
        channel: &mut ControlChannel<'_>,
        advanced: &mut bool,
    ) -> Result<bool> {
        remaining(channel.deadline)?;
        if !self.requester {
            return Err(Error::Internal("source RDMA finish requires bound DONE"));
        }
        let progress = &mut self.finish;
        let Some(slice) = self.slices.get_mut(progress.slice) else {
            return Ok(true);
        };
        let grant = &mut slice.grant;
        let cacheable = slice.binding.0[BINDING_SIZE - 1] == 1;
        let phase = match progress.phase {
            Phase::Send => {
                grant.progress.submit_completion(
                    &grant.record,
                    grant.claim,
                    slice.binding.0,
                    slice.remote.rkey,
                    channel.deadline,
                )?;
                Phase::Completion
            }
            Phase::Completion => {
                if !grant
                    .progress
                    .query_completion(&grant.record, grant.claim)?
                {
                    return Ok(false);
                }
                if cacheable {
                    grant.progress.begin_finish_sent(
                        &grant.record,
                        grant.claim,
                        &slice.binding.0,
                        channel.deadline,
                    )?;
                    Phase::Drain
                } else {
                    // Retirement of nonreusable QPs retains its existing join.
                    grant.close()?;
                    Phase::Done
                }
            }
            Phase::Drain => {
                if !grant.progress.query_finish(&grant.record, grant.claim)? {
                    return Ok(false);
                }
                Phase::Done
            }
            Phase::Done => {
                let count = channel.poll_write(&slice.binding.frame(DONE)[progress.position..])?;
                progress.position += count;
                *advanced |= count != 0;
                if progress.position != progress.bytes.len() {
                    return Ok(false);
                }
                Phase::Reply
            }
            Phase::Reply => {
                let count =
                    channel.poll_read(&mut progress.bytes[progress.position..REPLY_SIZE])?;
                progress.position += count;
                *advanced |= count != 0;
                if progress.position != REPLY_SIZE {
                    return Ok(false);
                }
                let mut bytes = [0; REPLY_SIZE];
                bytes.copy_from_slice(&progress.bytes[..REPLY_SIZE]);
                let reply = Reply::decode(&bytes, slice.peer.epoch)?;
                if reply.status != 0 || reply.length != 0 {
                    return Err(Error::Protocol("RDMA completion acknowledgement"));
                }
                Phase::Ack
            }
            Phase::Ack | Phase::Ready => {
                let count = channel.poll_read(&mut progress.bytes[progress.position..])?;
                progress.position += count;
                *advanced |= count != 0;
                if progress.position != progress.bytes.len() {
                    return Ok(false);
                }
                let ack = matches!(progress.phase, Phase::Ack);
                slice
                    .binding
                    .validate(&progress.bytes, if ack { ACK } else { READY })?;
                if ack && cacheable {
                    Phase::Ready
                } else {
                    Phase::Release
                }
            }
            Phase::Release => {
                if cacheable {
                    grant.cache(slice.peer)?;
                } else if slice.peer.operation == TransferOp::Read {
                    grant.copy_back(&self.memory, slice.offset, &transport.proxy)?;
                }
                drop(grant.output.take());
                progress.slice += 1;
                Phase::Send
            }
        };
        progress.phase = phase;
        progress.position = 0;
        *advanced = true;
        Ok(progress.slice == self.slices.len())
    }
}
