use std::time::Instant;

use super::RdmaTransport;
use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{
    ControlChannel, Operation, REQUEST_SIZE, Reply, Request, u64_at,
};
use crate::transport::rdma::{ConnectionId, DESCRIPTOR_SIZE, Descriptor, RdmaEndpoint};
use crate::{Error, RemoteEffect, RemoteSegment, Result, SourceEpoch, TransferOp};

const IDENTITY_SIZE: usize = 40;
const OFFER_SIZE: usize = IDENTITY_SIZE + 8 + DESCRIPTOR_SIZE;
const BINDING_SIZE: usize = REQUEST_SIZE + 2 * IDENTITY_SIZE + 8;
const _: () = assert!(BINDING_SIZE == super::COMPLETION_MESSAGE_SIZE);
const ACCEPT: u8 = 1;
const GRANT: u8 = 2;
const DONE: u8 = 3;
const ACK: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Identity {
    epoch: SourceEpoch,
    connection: ConnectionId,
}

impl Identity {
    fn local(endpoint: &RdmaEndpoint, epoch: SourceEpoch) -> Result<Self> {
        Ok(Self {
            epoch,
            connection: endpoint.incarnation()?,
        })
    }

    fn encode(self) -> [u8; IDENTITY_SIZE] {
        let mut bytes = [0; IDENTITY_SIZE];
        bytes[..16].copy_from_slice(&self.epoch.0);
        bytes[16..].copy_from_slice(&self.connection.encode());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != IDENTITY_SIZE {
            return Err(Error::Protocol("RDMA identity size"));
        }
        let mut epoch = [0; 16];
        epoch.copy_from_slice(&bytes[..16]);
        Ok(Self {
            epoch: SourceEpoch(epoch),
            connection: ConnectionId::decode(&bytes[16..])?,
        })
    }
}

/// The exact request and both native connection owners fence every grant phase.
struct Binding([u8; BINDING_SIZE]);

impl Binding {
    fn new(request: &Request, source: Identity, requester: Identity, grant: u64) -> Result<Self> {
        if grant == 0 {
            return Err(Error::Protocol("zero RDMA grant generation"));
        }
        let mut bytes = [0; BINDING_SIZE];
        bytes[..REQUEST_SIZE].copy_from_slice(&request.encode());
        bytes[REQUEST_SIZE..REQUEST_SIZE + IDENTITY_SIZE].copy_from_slice(&source.encode());
        bytes[REQUEST_SIZE + IDENTITY_SIZE..BINDING_SIZE - 8].copy_from_slice(&requester.encode());
        bytes[BINDING_SIZE - 8..].copy_from_slice(&grant.to_be_bytes());
        Ok(Self(bytes))
    }

    fn accept(
        channel: &mut ControlChannel<'_>,
        request: &Request,
        source: Identity,
        grant: u64,
    ) -> Result<Self> {
        let mut bytes = [0; 1 + BINDING_SIZE];
        channel.read_exact(&mut bytes)?;
        if bytes[0] != ACCEPT {
            return Err(Error::Protocol("RDMA accept phase"));
        }
        let requester =
            Identity::decode(&bytes[1 + REQUEST_SIZE + IDENTITY_SIZE..1 + BINDING_SIZE - 8])?;
        let binding = Self::new(request, source, requester, grant)?;
        if bytes[1..] != binding.0 {
            return Err(Error::Protocol("RDMA accept request or source incarnation"));
        }
        Ok(binding)
    }

    fn write(&self, channel: &mut ControlChannel<'_>, phase: u8) -> Result<()> {
        let mut bytes = [0; 1 + BINDING_SIZE];
        bytes[0] = phase;
        bytes[1..].copy_from_slice(&self.0);
        channel.write_all(&bytes)
    }

    fn read(&self, channel: &mut ControlChannel<'_>, phase: u8) -> Result<()> {
        let mut bytes = [0; 1 + BINDING_SIZE];
        channel.read_exact(&mut bytes)?;
        if bytes[0] != phase || bytes[1..] != self.0 {
            return Err(Error::Protocol("RDMA phase or grant binding mismatch"));
        }
        Ok(())
    }
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
            let mut endpoint = self.endpoint(
                &request.local,
                request.local_offset,
                request.length,
                0,
                deadline,
            )?;
            let transfer = (|| {
                if operation == TransferOp::Write {
                    endpoint.fill(&request.local, request.local_offset)?;
                }
                let wire = Request {
                    operation: if operation == TransferOp::Read {
                        Operation::RdmaRead
                    } else {
                        Operation::RdmaWrite
                    },
                    epoch: peer.epoch,
                    region: request.remote.id,
                    offset: request.remote_offset as u64,
                    length: request.length as u64,
                };
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                channel.write_all(&wire.encode())?;
                let reply = Reply::read(&mut channel, peer.epoch)?;
                if reply.status != 0 || reply.length != OFFER_SIZE as u64 {
                    return Err(Error::Protocol("RDMA bootstrap length/status"));
                }
                let mut offer = [0; OFFER_SIZE];
                channel.read_exact(&mut offer)?;
                let remote_identity = Identity::decode(&offer[..IDENTITY_SIZE])?;
                if remote_identity.epoch != peer.epoch {
                    return Err(Error::EpochMismatch);
                }
                let grant = u64_at(&offer, IDENTITY_SIZE);
                let mut descriptor = [0; DESCRIPTOR_SIZE];
                descriptor.copy_from_slice(&offer[IDENTITY_SIZE + 8..]);
                let connection = Descriptor::decode_connection(&descriptor)?;
                let binding = Binding::new(
                    &wire,
                    remote_identity,
                    Identity::local(&endpoint, epoch)?,
                    grant,
                )?;
                binding.write(&mut channel, ACCEPT)?;
                channel.write_all(&endpoint.connection())?;
                endpoint.connect(&connection)?;
                let ready = Reply::read(&mut channel, peer.epoch)?;
                if ready.status != 9 {
                    return Err(Error::Protocol("RDMA missing ready response"));
                }
                binding.read(&mut channel, GRANT)?;
                channel.read_exact(&mut descriptor)?;
                let remote = Descriptor::decode(&descriptor)?;
                if remote.connection_bytes() != connection.connection_bytes() {
                    return Err(Error::Protocol("RDMA grant changed its connection"));
                }
                // A peer may expose direct application backing. Once a WRITE is
                // attempted it can change remote bytes even without DONE or ACK.
                if operation == TransferOp::Write {
                    effect = RemoteEffect::Unknown;
                }
                endpoint.transfer(&remote, operation, deadline)?;
                endpoint.send_completion(&binding.0, remote.rkey)?;
                endpoint.close()?;
                binding.write(&mut channel, DONE)?;
                let receipt = Reply::read(&mut channel, peer.epoch)?;
                if receipt.status != 0 || receipt.length != 0 {
                    return Err(Error::Protocol("RDMA completion acknowledgement"));
                }
                binding.read(&mut channel, ACK)?;
                Ok(())
            })();
            #[cfg(test)]
            crate::transport::rdma::tests::before_requester_close(&mut endpoint, &transfer);
            endpoint.close()?;
            transfer?;
            if operation == TransferOp::Read {
                remaining(deadline)?;
                endpoint.copy_back(&request.local, request.local_offset)?;
            }
            Ok(())
        })();
        result.map_err(|error| (error, effect))
    }

    pub(crate) fn serve(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &crate::RegisteredMemory,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let writing = request.operation == Operation::RdmaWrite;
        let mut endpoint = self.endpoint(
            memory,
            offset,
            length,
            if writing { 2 } else { 1 },
            channel.deadline,
        )?;
        let transfer: Result<Binding> = (|| {
            if !writing {
                endpoint.fill(memory, offset)?;
            }
            let identity = Identity::local(&endpoint, epoch)?;
            let grant = endpoint.grant_generation()?;
            let mut offer = [0; OFFER_SIZE];
            offer[..IDENTITY_SIZE].copy_from_slice(&identity.encode());
            offer[IDENTITY_SIZE..IDENTITY_SIZE + 8].copy_from_slice(&grant.to_be_bytes());
            offer[IDENTITY_SIZE + 8..].copy_from_slice(&endpoint.connection());
            channel.reply(epoch, 0, OFFER_SIZE as u64)?;
            channel.write_all(&offer)?;
            let binding = Binding::accept(channel, request, identity, grant)?;
            let mut bytes = [0; DESCRIPTOR_SIZE];
            channel.read_exact(&mut bytes)?;
            endpoint.connect(&Descriptor::decode_connection(&bytes)?)?;
            endpoint.bind_window()?;
            endpoint.post_receive(&binding.0)?;
            channel.reply(epoch, 9, 0)?;
            binding.write(channel, GRANT)?;
            channel.write_all(&endpoint.grant()?.encode())?;
            binding.read(channel, DONE)?;
            endpoint.receive_completion()?;
            Ok(binding)
        })();
        let closed = endpoint.close();
        if matches!(&closed, Err(Error::Unquiesced { .. })) {
            return closed;
        }
        let binding = transfer?;
        closed?;
        if writing {
            remaining(channel.deadline)?;
            endpoint.copy_back(memory, offset)?;
        }
        drop(endpoint);
        #[cfg(test)]
        crate::transport::rdma::tests::before_source_ack(self);
        channel.reply(epoch, 0, 0)?;
        binding.write(channel, ACK)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/rdma_transport.rs"]
mod tests;
