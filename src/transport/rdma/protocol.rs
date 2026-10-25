//! OrbitTE v13 rendezvous and exact-grant receipts carried by the control service.
//! These messages are distinct from TENT's native metadata and wire formats.

use super::DESCRIPTOR_SIZE;
use super::grant::RdmaGrant;
use super::path::NicPath;
use super::qp::ConnectionId;
use crate::runtime::control_plane::{ControlChannel, REQUEST_SIZE, Request, u64_at};
use crate::{Error, Result, SourceEpoch};

pub(super) const IDENTITY_SIZE: usize = 40;
pub(super) const HELLO_SIZE: usize = 1 + 2 * IDENTITY_SIZE + 16 + 8;
pub(super) const OFFER_SIZE: usize = IDENTITY_SIZE + 8 + DESCRIPTOR_SIZE + 1;
pub(super) const BINDING_SIZE: usize = REQUEST_SIZE + 2 * IDENTITY_SIZE + 8 + 1;
const _: () = assert!(BINDING_SIZE == super::COMPLETION_MESSAGE_SIZE);
pub(super) const ACCEPT: u8 = 1;
pub(super) const GRANT: u8 = 2;
pub(super) const DONE: u8 = 3;
pub(super) const ACK: u8 = 4;
pub(super) const READY: u8 = 5;
pub(super) const CACHE_MISS: u8 = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Identity {
    pub(super) epoch: SourceEpoch,
    pub(super) connection: ConnectionId,
}

impl Identity {
    pub(super) fn local(endpoint: &RdmaGrant, epoch: SourceEpoch) -> Result<Self> {
        Ok(Self {
            epoch,
            connection: endpoint.incarnation()?,
        })
    }

    pub(super) fn encode(self) -> [u8; IDENTITY_SIZE] {
        let mut bytes = [0; IDENTITY_SIZE];
        bytes[..16].copy_from_slice(&self.epoch.0);
        bytes[16..].copy_from_slice(&self.connection.encode());
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
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
pub(super) struct Hello {
    pub(super) requester: Identity,
    pub(super) source: Option<Identity>,
    pub(super) expected_domain: [u8; 16],
    pub(super) direct: bool,
    /// Last observed grant on this pair; it is a lower bound, not an exclusive claim.
    pub(super) previous_grant: u64,
}

impl Hello {
    pub(super) fn write(&self, channel: &mut ControlChannel<'_>) -> Result<()> {
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

    pub(super) fn read(channel: &mut ControlChannel<'_>, epoch: SourceEpoch) -> Result<Self> {
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
pub(super) struct Binding(pub(super) [u8; BINDING_SIZE]);

/// Constructed only after a successful bytewise bound DONE on the control channel.
pub(super) struct BoundDone([u8; BINDING_SIZE]);
impl BoundDone {
    pub(super) fn binding(&self) -> &[u8; BINDING_SIZE] {
        &self.0
    }
}

impl Binding {
    pub(super) fn new(
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

    pub(super) fn accept(
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

    pub(super) fn write(&self, channel: &mut ControlChannel<'_>, phase: u8) -> Result<()> {
        let mut bytes = [0; 1 + BINDING_SIZE];
        bytes[0] = phase;
        bytes[1..].copy_from_slice(&self.0);
        channel.write_all(&bytes)
    }

    pub(super) fn read(&self, channel: &mut ControlChannel<'_>, phase: u8) -> Result<()> {
        let mut bytes = [0; 1 + BINDING_SIZE];
        channel.read_exact(&mut bytes)?;
        if bytes[0] != phase || bytes[1..] != self.0 {
            return Err(Error::Protocol("RDMA phase or grant binding mismatch"));
        }
        Ok(())
    }

    pub(super) fn done(&self, channel: &mut ControlChannel<'_>) -> Result<BoundDone> {
        self.read(channel, DONE)?;
        Ok(BoundDone(self.0))
    }
}

// Slice descriptors are relative to the parent range; identities stay NicID/PD-bound.
pub(super) struct SliceRequest {
    pub(super) offset: usize,
    pub(super) length: usize,
    pub(super) domain: [u8; 16],
}

impl SliceRequest {
    pub(super) fn write(&self, channel: &mut ControlChannel<'_>) -> Result<()> {
        let mut bytes = [0; 24];
        bytes[..4].copy_from_slice(&(self.offset as u32).to_be_bytes());
        bytes[4..8].copy_from_slice(&(self.length as u32).to_be_bytes());
        bytes[8..].copy_from_slice(&self.domain);
        channel.write_all(&bytes)
    }

    pub(super) fn read_plan(
        channel: &mut ControlChannel<'_>,
        length: usize,
        limit: u8,
        paths: &[NicPath],
    ) -> Result<Vec<Self>> {
        let mut count = [0];
        channel.read_exact(&mut count)?;
        if count[0] == 0 || count[0] > limit {
            return Err(Error::Protocol("RDMA slice count exceeds its grant window"));
        }
        let mut slices = Vec::new();
        slices
            .try_reserve_exact(count[0] as usize)
            .map_err(|_| Error::ResourceExhausted)?;
        let mut end = 0usize;
        for _ in 0..count[0] {
            let mut bytes = [0; 24];
            channel.read_exact(&mut bytes)?;
            let offset = u32::from_be_bytes(
                bytes[..4]
                    .try_into()
                    .map_err(|_| Error::Internal("RDMA slice offset size"))?,
            ) as usize;
            let size = u32::from_be_bytes(
                bytes[4..8]
                    .try_into()
                    .map_err(|_| Error::Internal("RDMA slice length size"))?,
            ) as usize;
            let mut domain = [0; 16];
            domain.copy_from_slice(&bytes[8..]);
            if offset != end || size == 0 || !paths.iter().any(|path| path.domain == domain) {
                return Err(Error::Protocol("RDMA slice plan range or PD identity"));
            }
            end = offset
                .checked_add(size)
                .filter(|end| *end <= length)
                .ok_or(Error::Protocol("RDMA slice plan exceeds parent range"))?;
            slices.push(Self {
                offset,
                length: size,
                domain,
            });
        }
        if end != length {
            return Err(Error::Protocol(
                "RDMA slice plan does not cover parent range",
            ));
        }
        Ok(slices)
    }
}
