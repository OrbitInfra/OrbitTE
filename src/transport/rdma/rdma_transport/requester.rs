//! One requester's connection/grant rendezvous, advanced one frame at a time.

use std::time::Duration;

use super::RdmaTransport;
use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{
    ControlChannel, Operation, REPLY_SIZE, Reply, Request, u64_at,
};
use crate::transport::rdma::{
    DESCRIPTOR_SIZE, Descriptor,
    endpoint::Peer,
    grant::RdmaGrant,
    path::NicPath,
    protocol::{
        ACCEPT, BINDING_SIZE, Binding, CACHE_MISS, GRANT, HELLO_SIZE, Hello, IDENTITY_SIZE,
        Identity, MR_AUTHORITY, OFFER_SIZE,
    },
    slice::RdmaSlice,
};
use crate::{Error, Result, SourceEpoch, TransferOp};

enum Phase {
    Prepare,
    Hello,
    OfferReply,
    Offer,
    Accept,
    Connection,
    Connect,
    Ready,
    Grant,
    Descriptor,
    Complete,
}

struct Offer {
    binding: Binding,
    connection: Descriptor,
    peer: Peer,
    cacheable: bool,
}

pub(super) struct RequesterSetup {
    endpoint: RdmaGrant,
    wire: Request,
    request: PreparedTransfer,
    epoch: SourceEpoch,
    remote_path: NicPath,
    phase: Phase,
    bytes: [u8; 1 + BINDING_SIZE],
    position: usize,
    offer: Option<Offer>,
    remote: Option<Descriptor>,
}

impl RequesterSetup {
    pub(super) fn new(
        wire: Request,
        request: PreparedTransfer,
        epoch: SourceEpoch,
        remote_path: NicPath,
        endpoint: RdmaGrant,
    ) -> Self {
        Self {
            endpoint,
            wire,
            request,
            epoch,
            remote_path,
            phase: Phase::Prepare,
            bytes: [0; 1 + BINDING_SIZE],
            position: 0,
            offer: None,
            remote: None,
        }
    }

    pub(super) fn progress(
        &mut self,
        transport: &RdmaTransport,
        channel: &mut ControlChannel<'_>,
        advanced: &mut bool,
    ) -> Result<bool> {
        remaining(channel.deadline)?;
        let write_length = match self.phase {
            Phase::Hello => HELLO_SIZE,
            Phase::Accept => 1 + BINDING_SIZE,
            Phase::Connection => DESCRIPTOR_SIZE,
            _ => 0,
        };
        let read_length = match self.phase {
            Phase::OfferReply | Phase::Ready => REPLY_SIZE,
            Phase::Offer => OFFER_SIZE,
            Phase::Grant => 1 + BINDING_SIZE,
            Phase::Descriptor => DESCRIPTOR_SIZE,
            _ => 0,
        };
        if write_length != 0 || read_length != 0 {
            let count = if write_length != 0 {
                channel.poll_write(&self.bytes[self.position..write_length])?
            } else {
                channel.poll_read(&mut self.bytes[self.position..read_length])?
            };
            self.position += count;
            *advanced |= count != 0;
            if self.position != write_length.max(read_length) {
                return Ok(false);
            }
        }
        self.phase = match self.phase {
            Phase::Prepare => {
                if self.wire.operation == Operation::RdmaWrite {
                    self.endpoint.fill(
                        &self.request.local,
                        self.request.local_offset,
                        #[cfg(feature = "cuda")]
                        &transport.proxy,
                    )?;
                }
                let cached = self.endpoint.cached_peer;
                let hello = Hello {
                    requester: Identity::local(&self.endpoint, self.epoch)?,
                    source: cached.map(|peer| Identity {
                        epoch: peer.epoch,
                        connection: peer.connection,
                    }),
                    expected_domain: self.remote_path.domain,
                    direct: transport.direct(&self.request.local),
                    qp_width: self
                        .endpoint
                        .endpoint
                        .as_ref()
                        .map_or(Ok(0), |owner| owner.width())?,
                    previous_grant: cached.map_or(0, |peer| peer.generation),
                };
                self.bytes[..HELLO_SIZE].copy_from_slice(&hello.encode()?);
                Phase::Hello
            }
            Phase::Hello => Phase::OfferReply,
            Phase::OfferReply => {
                let mut bytes = [0; REPLY_SIZE];
                bytes.copy_from_slice(&self.bytes[..REPLY_SIZE]);
                let reply = Reply::decode(&bytes, self.wire.epoch)?;
                if reply.status == CACHE_MISS
                    && reply.length == 0
                    && self.endpoint.cached_peer.is_some()
                {
                    Phase::Complete
                } else if reply.status == 0 && reply.length == OFFER_SIZE as u64 {
                    Phase::Offer
                } else {
                    return Err(Error::Protocol("RDMA bootstrap length/status"));
                }
            }
            Phase::Offer => {
                let offer = self.decode_offer(transport, channel)?;
                self.bytes.copy_from_slice(&offer.binding.frame(ACCEPT));
                self.offer = Some(offer);
                Phase::Accept
            }
            Phase::Accept => {
                self.bytes[..DESCRIPTOR_SIZE].copy_from_slice(&self.endpoint.connection());
                Phase::Connection
            }
            Phase::Connection => Phase::Connect,
            Phase::Connect => {
                let offer = self
                    .offer
                    .as_ref()
                    .ok_or(Error::Internal("missing RDMA offer"))?;
                self.endpoint.connect(&offer.connection)?;
                Phase::Ready
            }
            Phase::Ready => {
                let mut bytes = [0; REPLY_SIZE];
                bytes.copy_from_slice(&self.bytes[..REPLY_SIZE]);
                let ready = Reply::decode(&bytes, self.wire.epoch)?;
                if ready.status != 9 || ready.length != 0 {
                    return Err(Error::Protocol("RDMA missing ready response"));
                }
                Phase::Grant
            }
            Phase::Grant => {
                self.offer
                    .as_ref()
                    .ok_or(Error::Internal("missing RDMA offer"))?
                    .binding
                    .validate(&self.bytes, GRANT)?;
                Phase::Descriptor
            }
            Phase::Descriptor => {
                let mut bytes = [0; DESCRIPTOR_SIZE];
                bytes.copy_from_slice(&self.bytes[..DESCRIPTOR_SIZE]);
                let remote = Descriptor::decode(&bytes)?;
                let offer = self
                    .offer
                    .as_ref()
                    .ok_or(Error::Internal("missing RDMA offer"))?;
                if remote.connection_bytes() != offer.connection.connection_bytes() {
                    return Err(Error::Protocol("RDMA grant changed its connection"));
                }
                if offer.cacheable {
                    self.endpoint.publish_peer(offer.peer)?;
                }
                self.remote = Some(remote);
                Phase::Complete
            }
            Phase::Complete => return Err(Error::Internal("RDMA setup already completed")),
        };
        self.position = 0;
        *advanced = true;
        Ok(matches!(self.phase, Phase::Complete))
    }

    fn decode_offer(
        &self,
        transport: &RdmaTransport,
        channel: &ControlChannel<'_>,
    ) -> Result<Offer> {
        let remote_identity = Identity::decode(&self.bytes[..IDENTITY_SIZE])?;
        if remote_identity.epoch != self.wire.epoch {
            return Err(Error::EpochMismatch);
        }
        let profile = self.bytes[OFFER_SIZE - 1];
        if profile != MR_AUTHORITY && remote_identity.connection.domain != self.remote_path.domain {
            return Err(Error::Protocol("RDMA source changed its protection domain"));
        }
        let cached = self.endpoint.cached_peer;
        if cached.is_some_and(|peer| remote_identity.connection != peer.connection) {
            return Err(Error::Protocol("RDMA source changed cached connection"));
        }
        let cacheable = match profile {
            0 | MR_AUTHORITY if self.endpoint.endpoint.is_none() => false,
            1 if transport.direct(&self.request.local) && self.endpoint.endpoint.is_some() => true,
            _ => return Err(Error::Protocol("RDMA release profile mismatch")),
        };
        let grant = u64_at(&self.bytes, IDENTITY_SIZE);
        if cached.is_some_and(|peer| grant <= peer.generation) {
            return Err(Error::Protocol(
                "RDMA source reused an old grant generation",
            ));
        }
        let mut bytes = [0; DESCRIPTOR_SIZE];
        bytes.copy_from_slice(&self.bytes[IDENTITY_SIZE + 8..OFFER_SIZE - 1]);
        let connection = Descriptor::decode_connection(&bytes)?;
        if !self.remote_path.matches_connection(&connection) {
            return Err(Error::Protocol(
                "RDMA source changed its advertised NIC path",
            ));
        }
        Ok(Offer {
            binding: Binding::new(
                &self.wire,
                remote_identity,
                Identity::local(&self.endpoint, self.epoch)?,
                grant,
                profile,
            )?,
            connection,
            peer: Peer {
                route: Some(channel.stream.peer_addr()?),
                source_nic_id: self.remote_path.nic_id,
                epoch: self.wire.epoch,
                local: self.endpoint.incarnation()?,
                connection: remote_identity.connection,
                operation: if self.wire.operation == Operation::RdmaRead {
                    TransferOp::Read
                } else {
                    TransferOp::Write
                },
                source: false,
                generation: grant,
            },
            cacheable,
        })
    }

    pub(super) fn finish(mut self) -> Result<Option<RdmaSlice>> {
        if !matches!(self.phase, Phase::Complete) {
            return Err(self
                .endpoint
                .abort(Error::Internal("RDMA setup is not complete")));
        }
        match (self.offer, self.remote) {
            (Some(offer), Some(remote)) => Ok(Some(RdmaSlice {
                grant: self.endpoint,
                binding: offer.binding,
                remote,
                peer: offer.peer,
                offset: self.request.local_offset,
            })),
            (None, None) => {
                self.endpoint.close()?;
                Ok(None)
            }
            _ => Err(self
                .endpoint
                .abort(Error::Internal("incomplete RDMA grant"))),
        }
    }

    pub(super) fn abort(mut self, transport: &RdmaTransport, error: Error) -> Error {
        let _ = transport;
        #[cfg(all(test, feature = "cuda"))]
        crate::transport::rdma::cq_lane::tests::before_staging_abort(transport, &self.endpoint);
        self.endpoint.abort(error)
    }
}

impl RdmaTransport {
    pub(super) fn requester_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        wire: &Request,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        remote_path: &NicPath,
        endpoint: RdmaGrant,
    ) -> Result<Option<RdmaSlice>> {
        let mut setup =
            RequesterSetup::new(*wire, request.clone(), epoch, remote_path.clone(), endpoint);
        loop {
            let mut advanced = false;
            match setup.progress(self, channel, &mut advanced) {
                Ok(true) => return setup.finish(),
                Ok(false) => {
                    if !advanced {
                        std::thread::park_timeout(Duration::from_micros(100));
                    }
                }
                Err(error) => return Err(setup.abort(self, error)),
            }
        }
    }
}
