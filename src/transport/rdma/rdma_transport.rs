use std::time::Instant;

use super::RdmaTransport;
use super::endpoint_store::Peer;
use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{
    ControlChannel, Operation, REQUEST_SIZE, Reply, Request, u64_at,
};
use crate::transport::rdma::{ConnectionId, DESCRIPTOR_SIZE, Descriptor, RdmaEndpoint};
use crate::{Error, RemoteEffect, RemoteSegment, Result, SourceEpoch, TransferOp};

const IDENTITY_SIZE: usize = 40;
const HELLO_SIZE: usize = 1 + 2 * IDENTITY_SIZE + 16 + 8;
const OFFER_SIZE: usize = IDENTITY_SIZE + 8 + DESCRIPTOR_SIZE + 1;
const BINDING_SIZE: usize = REQUEST_SIZE + 2 * IDENTITY_SIZE + 8 + 1;
const _: () = assert!(BINDING_SIZE == super::COMPLETION_MESSAGE_SIZE);
const ACCEPT: u8 = 1;
const GRANT: u8 = 2;
const DONE: u8 = 3;
const ACK: u8 = 4;
const READY: u8 = 5;
const CACHE_MISS: u8 = 12;

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
        if epoch == [0; 16] {
            return Err(Error::Protocol("zero RDMA process identity"));
        }
        Ok(Self {
            epoch: SourceEpoch(epoch),
            connection: ConnectionId::decode(&bytes[16..])?,
        })
    }
}

/// Rendezvous both persistent connection owners before creating any authority.
struct Hello {
    requester: Identity,
    source: Option<Identity>,
    expected_domain: [u8; 16],
    direct: bool,
    previous_grant: u64,
}

impl Hello {
    fn write(&self, channel: &mut ControlChannel<'_>) -> Result<()> {
        let mut bytes = [0; HELLO_SIZE];
        bytes[0] = u8::from(self.direct) | (u8::from(self.source.is_some()) << 1);
        bytes[1..1 + IDENTITY_SIZE].copy_from_slice(&self.requester.encode());
        if let Some(source) = self.source {
            bytes[1 + IDENTITY_SIZE..1 + 2 * IDENTITY_SIZE].copy_from_slice(&source.encode());
        }
        bytes[HELLO_SIZE - 24..HELLO_SIZE - 8].copy_from_slice(&self.expected_domain);
        bytes[HELLO_SIZE - 8..].copy_from_slice(&self.previous_grant.to_be_bytes());
        channel.write_all(&bytes)
    }

    fn read(channel: &mut ControlChannel<'_>, epoch: SourceEpoch) -> Result<Self> {
        let mut bytes = [0; HELLO_SIZE];
        channel.read_exact(&mut bytes)?;
        if bytes[0] & !3 != 0 {
            return Err(Error::Protocol("RDMA rendezvous flags"));
        }
        let requester = Identity::decode(&bytes[1..1 + IDENTITY_SIZE])?;
        let source_bytes = &bytes[1 + IDENTITY_SIZE..1 + 2 * IDENTITY_SIZE];
        let source = if bytes[0] & 2 != 0 {
            let source = Identity::decode(source_bytes)?;
            if source.epoch != epoch {
                return Err(Error::EpochMismatch);
            }
            if bytes[0] & 1 == 0 {
                return Err(Error::Protocol("RDMA reuse without host Direct"));
            }
            Some(source)
        } else {
            if source_bytes.iter().any(|b| *b != 0) {
                return Err(Error::Protocol(
                    "RDMA fresh rendezvous has a cached identity",
                ));
            }
            None
        };
        let mut expected_domain = [0; 16];
        expected_domain.copy_from_slice(&bytes[HELLO_SIZE - 24..HELLO_SIZE - 8]);
        let previous_grant = u64_at(&bytes, HELLO_SIZE - 8);
        if source.is_some() != (previous_grant != 0) {
            return Err(Error::Protocol("RDMA rendezvous grant generation"));
        }
        if source.is_some_and(|s| s.connection.domain != expected_domain) {
            return Err(Error::Protocol("RDMA rendezvous changed its PD domain"));
        }
        Ok(Self {
            requester,
            source,
            expected_domain,
            direct: bytes[0] & 1 != 0,
            previous_grant,
        })
    }
}

/// Exact request, native pair, generation and negotiated release profile.
struct Binding([u8; BINDING_SIZE]);

/// Constructed only after a successful bytewise bound DONE on the control channel.
pub(super) struct BoundDone([u8; BINDING_SIZE]);
impl BoundDone {
    pub(super) fn binding(&self) -> &[u8; BINDING_SIZE] {
        &self.0
    }
}

impl Binding {
    fn new(
        request: &Request,
        source: Identity,
        requester: Identity,
        grant: u64,
        cacheable: bool,
    ) -> Result<Self> {
        if grant == 0 {
            return Err(Error::Protocol("zero RDMA grant generation"));
        }
        let mut bytes = [0; BINDING_SIZE];
        bytes[..REQUEST_SIZE].copy_from_slice(&request.encode());
        bytes[REQUEST_SIZE..REQUEST_SIZE + IDENTITY_SIZE].copy_from_slice(&source.encode());
        bytes[REQUEST_SIZE + IDENTITY_SIZE..REQUEST_SIZE + 2 * IDENTITY_SIZE]
            .copy_from_slice(&requester.encode());
        bytes[BINDING_SIZE - 9..BINDING_SIZE - 1].copy_from_slice(&grant.to_be_bytes());
        bytes[BINDING_SIZE - 1] = u8::from(cacheable);
        Ok(Self(bytes))
    }

    fn accept(
        channel: &mut ControlChannel<'_>,
        request: &Request,
        source: Identity,
        requester: Identity,
        grant: u64,
        cacheable: bool,
    ) -> Result<Self> {
        let binding = Self::new(request, source, requester, grant, cacheable)?;
        binding.read(channel, ACCEPT)?;
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

    fn done(&self, channel: &mut ControlChannel<'_>) -> Result<BoundDone> {
        self.read(channel, DONE)?;
        Ok(BoundDone(self.0))
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
            let direct = self.host_direct(&request.local);
            let device = self.device()?;
            device.check_address()?;
            let progress = self.progress(device)?;
            let mut reuse = if direct {
                progress.claim(
                    |p| {
                        !p.source
                            && p.route == Some(peer.endpoint)
                            && p.epoch == peer.epoch
                            && p.operation == operation
                    },
                    deadline,
                )?
            } else {
                None
            };
            let mut expected_domain = reuse
                .as_ref()
                .and_then(|e| e.cached_peer)
                .map_or([0; 16], |p| p.connection.domain);
            for attempt in 0..2 {
                let mut endpoint = self.endpoint(
                    &request.local,
                    request.local_offset,
                    request.length,
                    0,
                    deadline,
                    reuse.take(),
                )?;
                let cached = endpoint.cached_peer;
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
                let mut miss = false;
                let transfer = (|| {
                    if operation == TransferOp::Write {
                        endpoint.fill(&request.local, request.local_offset)?;
                    }
                    let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                    channel.write_all(&wire.encode())?;
                    Hello {
                        requester: Identity::local(&endpoint, epoch)?,
                        source: cached.map(|p| Identity {
                            epoch: p.epoch,
                            connection: p.connection,
                        }),
                        expected_domain,
                        direct,
                        previous_grant: cached.map_or(0, |p| p.generation),
                    }
                    .write(&mut channel)?;
                    let reply = Reply::read(&mut channel, peer.epoch)?;
                    if reply.status == CACHE_MISS
                        && reply.length == 0
                        && cached.is_some()
                        && attempt == 0
                    {
                        miss = true;
                        return Ok(());
                    }
                    if reply.status != 0 || reply.length != OFFER_SIZE as u64 {
                        return Err(Error::Protocol("RDMA bootstrap length/status"));
                    }
                    let mut offer = [0; OFFER_SIZE];
                    channel.read_exact(&mut offer)?;
                    let remote_identity = Identity::decode(&offer[..IDENTITY_SIZE])?;
                    if remote_identity.epoch != peer.epoch {
                        return Err(Error::EpochMismatch);
                    }
                    if expected_domain != [0; 16]
                        && remote_identity.connection.domain != expected_domain
                    {
                        return Err(Error::Protocol("RDMA source changed its protection domain"));
                    }
                    if cached.is_some_and(|p| remote_identity.connection != p.connection) {
                        return Err(Error::Protocol("RDMA source changed cached connection"));
                    }
                    let cacheable = match offer[OFFER_SIZE - 1] {
                        0 if cached.is_none() => false,
                        1 if direct => true,
                        _ => return Err(Error::Protocol("RDMA release profile mismatch")),
                    };
                    let grant = u64_at(&offer, IDENTITY_SIZE);
                    if cached.is_some_and(|p| grant <= p.generation) {
                        return Err(Error::Protocol(
                            "RDMA source reused an old grant generation",
                        ));
                    }
                    let mut descriptor = [0; DESCRIPTOR_SIZE];
                    descriptor.copy_from_slice(&offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1]);
                    let connection = Descriptor::decode_connection(&descriptor)?;
                    let binding = Binding::new(
                        &wire,
                        remote_identity,
                        Identity::local(&endpoint, epoch)?,
                        grant,
                        cacheable,
                    )?;
                    binding.write(&mut channel, ACCEPT)?;
                    channel.write_all(&endpoint.connection())?;
                    endpoint.connect(&connection)?;
                    let ready = Reply::read(&mut channel, peer.epoch)?;
                    if ready.status != 9 || ready.length != 0 {
                        return Err(Error::Protocol("RDMA missing ready response"));
                    }
                    binding.read(&mut channel, GRANT)?;
                    channel.read_exact(&mut descriptor)?;
                    let remote = Descriptor::decode(&descriptor)?;
                    if remote.connection_bytes() != connection.connection_bytes() {
                        return Err(Error::Protocol("RDMA grant changed its connection"));
                    }
                    // A WRITE can change direct application bytes before DONE/ACK.
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                    }
                    endpoint.transfer(&remote, operation, deadline)?;
                    endpoint.send_completion(&binding.0, remote.rkey)?;
                    if cacheable {
                        endpoint.finish_sent(&binding.0)?;
                    } else {
                        endpoint.close()?;
                    }
                    binding.write(&mut channel, DONE)?;
                    let receipt = Reply::read(&mut channel, peer.epoch)?;
                    if receipt.status != 0 || receipt.length != 0 {
                        return Err(Error::Protocol("RDMA completion acknowledgement"));
                    }
                    binding.read(&mut channel, ACK)?;
                    if cacheable {
                        binding.read(&mut channel, READY)?;
                        endpoint.cache(Peer {
                            route: Some(peer.endpoint),
                            epoch: peer.epoch,
                            local: endpoint.incarnation()?,
                            connection: remote_identity.connection,
                            operation,
                            source: false,
                            generation: grant,
                        })?;
                    }
                    Ok(())
                })();
                #[cfg(test)]
                crate::transport::rdma::tests::before_requester_close(&mut endpoint, &transfer);
                endpoint.close()?;
                transfer?;
                if miss {
                    // Only an explicit pre-authority cache miss can start a fresh pair.
                    expected_domain = cached
                        .ok_or(Error::Internal("missing stale RDMA pair"))?
                        .connection
                        .domain;
                    drop(endpoint);
                    continue;
                }
                if operation == TransferOp::Read && !endpoint.released {
                    remaining(deadline)?;
                    endpoint.copy_back(&request.local, request.local_offset)?;
                }
                return Ok(());
            }
            Err(Error::Protocol("RDMA repeated cache miss"))
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
        let operation = if writing {
            TransferOp::Write
        } else {
            TransferOp::Read
        };
        let hello = Hello::read(channel, epoch)?;
        let cacheable = hello.direct && self.host_direct(memory);
        let device = self.device()?;
        device.check_address()?;
        if hello.expected_domain != [0; 16] && hello.expected_domain != device.domain()? {
            return Err(Error::Protocol(
                "RDMA rendezvous protection domain mismatch",
            ));
        }
        let progress = self.progress(device)?;
        let reuse = if let Some(source) = hello.source {
            if !cacheable {
                return channel.reply(epoch, CACHE_MISS, 0);
            }
            let reuse = progress.claim(
                |p| {
                    p.source
                        && p.local == source.connection
                        && p.epoch == hello.requester.epoch
                        && p.connection == hello.requester.connection
                        && p.operation == operation
                        && p.generation == hello.previous_grant
                },
                channel.deadline,
            )?;
            if reuse.is_none() {
                return channel.reply(epoch, CACHE_MISS, 0);
            }
            reuse
        } else {
            None
        };
        let mut endpoint = self.endpoint(
            memory,
            offset,
            length,
            if writing { 2 } else { 1 },
            channel.deadline,
            reuse,
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
            offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1].copy_from_slice(&endpoint.connection());
            offer[OFFER_SIZE - 1] = u8::from(cacheable);
            channel.reply(epoch, 0, OFFER_SIZE as u64)?;
            channel.write_all(&offer)?;
            let binding = Binding::accept(
                channel,
                request,
                identity,
                hello.requester,
                grant,
                cacheable,
            )?;
            let mut bytes = [0; DESCRIPTOR_SIZE];
            channel.read_exact(&mut bytes)?;
            endpoint.connect(&Descriptor::decode_connection(&bytes)?)?;
            endpoint.bind_window()?;
            endpoint.post_receive(&binding.0)?;
            channel.reply(epoch, 9, 0)?;
            binding.write(channel, GRANT)?;
            channel.write_all(&endpoint.grant()?.encode())?;
            let done = binding.done(channel)?;
            endpoint.receive_completion()?;
            if cacheable {
                endpoint.finish_received(&done)?;
            }
            Ok(binding)
        })();
        let closed = if cacheable && transfer.is_ok() {
            Ok(())
        } else {
            endpoint.close()
        };
        if matches!(&closed, Err(Error::Unquiesced { .. })) {
            return closed;
        }
        let binding = transfer?;
        closed?;
        if !cacheable {
            if writing {
                remaining(channel.deadline)?;
                endpoint.copy_back(memory, offset)?;
            }
            drop(endpoint.output.take());
        }
        #[cfg(test)]
        crate::transport::rdma::tests::before_source_ack(self);
        channel.reply(epoch, 0, 0)?;
        binding.write(channel, ACK)?;
        if cacheable {
            endpoint.cache(Peer {
                route: None,
                epoch: hello.requester.epoch,
                local: endpoint.incarnation()?,
                connection: hello.requester.connection,
                operation,
                source: true,
                generation: u64_at(&binding.0, BINDING_SIZE - 9),
            })?;
            if let Err(error) = binding.write(channel, READY) {
                endpoint.retire_idle()?;
                return Err(error);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/rdma_transport.rs"]
mod tests;
