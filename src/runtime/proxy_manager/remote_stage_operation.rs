//! Scoped PRE/CROSS/POST protocol and completion receipts.

use super::ProxyManager;
use crate::engine::remaining;
use crate::runtime::control_plane::{ControlChannel, Operation, REQUEST_SIZE, Reply, Request};
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
        self.expect(channel, operation, offset, length)
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

    fn frame(&self, operation: Operation, offset: usize, length: usize) -> Request {
        Request {
            operation,
            epoch: self.nonce,
            region: self.region,
            offset: offset as u64,
            length: length as u64,
        }
    }

    pub(super) fn expect(
        &self,
        channel: &mut ControlChannel<'_>,
        operation: Operation,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let request = read_request(channel)?;
        self.validate(&request, operation, offset, length)?;
        remaining(channel.deadline)?;
        Ok(())
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
