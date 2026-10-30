//! Scoped PRE/CROSS/POST protocol and completion receipts.

use super::ProxyManager;
use crate::engine::remaining;
use crate::runtime::control_plane::{
    ControlChannel, Operation, REPLY_SIZE, REQUEST_SIZE, Reply, Request,
};
use crate::{Error, Result, SourceEpoch};

pub(super) fn read_request(channel: &mut ControlChannel<'_>) -> Result<Request> {
    let mut bytes = [0; REQUEST_SIZE];
    channel.read_exact(&mut bytes)?;
    Request::decode(&bytes)
}

/// Each frame is scoped to the original registration, operation, chunk and
/// freshly pinned session. Each slot accepts only its current generation and phase.
pub(super) struct RemoteStageOperation {
    pub(super) nonce: SourceEpoch,
    pub(super) region: u64,
}

impl RemoteStageOperation {
    #[cfg(feature = "rdma")]
    pub(super) fn receipt(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let reply = Reply::read(channel, self.nonce)?;
        if reply.status != 0 || reply.length != REQUEST_SIZE as u64 {
            return Err(Error::Protocol("staging operation receipt"));
        }
        let request = read_request(channel)?;
        self.validate(&request, operation, offset, length)?;
        remaining(channel.deadline)?;
        Ok(())
    }

    pub(super) fn respond(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        channel.reply(self.nonce, 0, REQUEST_SIZE as u64)?;
        self.send(channel, operation, offset, length)
    }

    pub(super) fn submit(
        &self,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> ControlOperation {
        ControlOperation::new(self.frame(operation, offset, length), false)
    }

    pub(super) fn receive(
        &self,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> ControlOperation {
        ControlOperation::new(self.frame(operation, offset, length), true)
    }

    pub(super) fn respond_progress(
        &self,
        channel: &mut ControlChannel<'_>,
        offset: usize,
        length: usize,
        completed: bool,
    ) -> Result<()> {
        channel.reply(self.nonce, 0, (REQUEST_SIZE + 1) as u64)?;
        self.send(channel, Operation::QueryDelegate, offset, length)?;
        channel.write_all(&[u8::from(completed)])
    }

    fn frame(&self, operation: Operation, offset: usize, length: usize) -> Request {
        Request {
            operation,
            epoch: self.nonce,
            region: self.region,
            offset: offset as u64,
            length: length as u64,
        }
    }

    pub(super) fn validate(
        &self,
        request: &Request,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        if request.operation != operation
            || request.epoch != self.nonce
            || request.region != self.region
            || request.offset != offset as u64
            || request.length != length as u64
        {
            return Err(Error::Protocol("staging operation scope or order"));
        }
        Ok(())
    }

    pub(super) fn send(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        channel.write_all(&self.frame(operation, offset, length).encode())
    }

    pub(super) fn stage_id(&self, slot: usize) -> u64 {
        crate::runtime::control_plane::u64_at(&self.nonce.0, 0).wrapping_add(slot as u64)
    }
}

impl ProxyManager {
    pub(super) fn completed_remote(&self, operation: Operation) {
        let mut state = self.buffers.lock().unwrap_or_else(|p| p.into_inner());
        let count = match operation {
            Operation::PinRead | Operation::PinWrite => &mut state.remote_pins,
            Operation::Delegate => &mut state.remote_delegates,
            Operation::Cross => &mut state.remote_crosses,
            Operation::Unpin => &mut state.remote_unpins,
            _ => return,
        };
        *count = count.saturating_add(1);
    }
}

/// The single framing owner until the complete scoped response is consumed.
pub(super) struct ControlOperation {
    request: Request,
    sent: usize,
    reply: [u8; REPLY_SIZE],
    reply_read: usize,
    payload: [u8; REQUEST_SIZE + 1],
    payload_read: usize,
}

impl ControlOperation {
    fn new(request: Request, received_only: bool) -> Self {
        Self {
            request,
            sent: if received_only { REQUEST_SIZE } else { 0 },
            reply: [0; REPLY_SIZE],
            reply_read: 0,
            payload: [0; REQUEST_SIZE + 1],
            payload_read: 0,
        }
    }

    pub(super) fn sent(&self) -> bool {
        self.sent == REQUEST_SIZE
    }

    pub(super) fn progress_send(
        &mut self,
        channel: &mut ControlChannel<'_>,
        advanced: &mut bool,
    ) -> Result<bool> {
        if !self.sent() {
            let count = channel.poll_write(&self.request.encode()[self.sent..])?;
            self.sent += count;
            *advanced |= count != 0;
        }
        Ok(self.sent())
    }

    pub(super) fn progress(
        &mut self,
        channel: &mut ControlChannel<'_>,
        advanced: &mut bool,
    ) -> Result<Option<bool>> {
        if !self.sent() {
            self.progress_send(channel, advanced)?;
            // Let the caller publish CROSS after Delegate send, before waiting
            // on its acceptance receipt.
            return Ok(None);
        }
        let query = self.request.operation == Operation::QueryDelegate;
        let length = REQUEST_SIZE + usize::from(query);
        if self.reply_read != REPLY_SIZE {
            let count = channel.poll_read(&mut self.reply[self.reply_read..])?;
            self.reply_read += count;
            *advanced |= count != 0;
            if self.reply_read != REPLY_SIZE {
                return Ok(None);
            }
            let reply = Reply::decode(&self.reply, self.request.epoch)?;
            if reply.status != 0 || reply.length != length as u64 {
                return Err(Error::Protocol("staging progress receipt"));
            }
        }
        if self.payload_read != length {
            let count = channel.poll_read(&mut self.payload[self.payload_read..length])?;
            self.payload_read += count;
            *advanced |= count != 0;
            if self.payload_read != length {
                return Ok(None);
            }
        }
        let mut frame = [0; REQUEST_SIZE];
        frame.copy_from_slice(&self.payload[..REQUEST_SIZE]);
        let remote = RemoteStageOperation {
            nonce: self.request.epoch,
            region: self.request.region,
        };
        remote.validate(
            &Request::decode(&frame)?,
            self.request.operation,
            self.request.offset as usize,
            self.request.length as usize,
        )?;
        remaining(channel.deadline)?;
        if !query {
            return Ok(Some(true));
        }
        match self.payload[REQUEST_SIZE] {
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            _ => Err(Error::Protocol("staging progress result")),
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/remote_stage_operation.rs"]
mod tests;
