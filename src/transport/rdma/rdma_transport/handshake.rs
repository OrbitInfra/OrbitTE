//! Transport admission, grant construction and bilateral connection rendezvous.

use std::sync::Arc;
use std::time::Instant;

use super::{RdmaTransport, connections::Connection};
use crate::engine::remaining;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
use crate::transport::rdma::{
    DESCRIPTOR_SIZE, RdmaHostMemory,
    endpoint::{EndpointKey, Peer},
    grant::{GrantLease, RdmaGrant, admission::GrantCredits},
    path::NicPath,
    protocol::{
        ACK, BINDING_SIZE, Binding, CACHE_MISS, DONE, GRANT, Hello, IDENTITY_SIZE, Identity,
        OFFER_SIZE, READY,
    },
    qp::Descriptor,
    quota::DeviceLease,
    slice::RdmaSlice,
};
use crate::{Error, RegisteredMemory, Result, SourceEpoch, TransferOp};

impl RdmaTransport {
    pub(in crate::transport::rdma) fn prepare_grant(
        &self,
        grant_lease: GrantLease,
        memory: &RegisteredMemory,
        range: std::ops::Range<usize>,
        permission: u32,
        deadline: Instant,
        connection: Connection,
    ) -> Result<RdmaGrant> {
        let mut construction = super::super::grant::construction::GrantConstruction::new(
            self,
            grant_lease,
            memory,
            range,
            permission,
            deadline,
            connection,
        )?;
        loop {
            match construction.poll(self, deadline) {
                Ok(Some(grant)) => return Ok(grant),
                Ok(None) => std::thread::park_timeout(std::time::Duration::from_micros(100)),
                Err(error) => return Err(construction.abort(error)),
            }
        }
    }

    pub(in crate::transport::rdma) fn reserve_grant(
        &self,
        selection: DeviceLease,
        memory: &RegisteredMemory,
        credits: &Arc<GrantCredits>,
    ) -> Result<GrantLease> {
        let length = selection.length();
        let is_direct = self.direct(memory);
        {
            let mut usage = self
                .usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            let (charged, limit) = if is_direct {
                (usage.direct_bytes, self.config.max_direct_bytes)
            } else {
                (usage.staging_bytes, self.config.max_staging_bytes)
            };
            let bytes = charged
                .checked_add(length)
                .ok_or(Error::ResourceExhausted)?;
            if usage.retained_endpoints != 0
                || usage.retained_grants != 0
                || usage.retained_devices != 0
                || usage.retained_cqs != 0
                || usage.retained_completion_channels != 0
                || usage.retained_persistent_mrs != 0
                || usage.progress_failures != usage.progress_recoveries
            {
                return Err(Error::Unavailable(
                    "RDMA admission is poisoned by retained native resources",
                ));
            }
            if bytes > limit || usage.active_grants >= self.config.max_grants {
                return Err(Error::ResourceExhausted);
            }
            let credit = credits.take()?;
            usage.active_grants += 1;
            if is_direct {
                usage.direct_bytes = bytes;
            } else {
                usage.staging_bytes = bytes;
            }
            Ok(GrantLease {
                _credit: credit,
                usage: Arc::clone(&self.usage),
                length,
                is_direct,
                drained: Arc::clone(&self.drained),
                _selection: selection,
            })
        }
    }

    pub(in crate::transport::rdma) fn direct(&self, memory: &RegisteredMemory) -> bool {
        if memory.is_cuda() {
            self.config.cuda_memory == crate::transport::rdma::RdmaCudaMemory::Direct
        } else {
            self.config.host_memory == RdmaHostMemory::Direct
        }
    }

    pub(super) fn finish_requester_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        slice: &mut RdmaSlice,
        memory: &RegisteredMemory,
    ) -> Result<()> {
        let endpoint = &mut slice.grant;
        endpoint.send_completion(&slice.binding.0, slice.remote.rkey)?;
        let cacheable = slice.binding.0[BINDING_SIZE - 1] == 1;
        if cacheable {
            endpoint.finish_sent(&slice.binding.0)?;
        } else {
            endpoint.close()?;
        }
        slice.binding.write(channel, DONE)?;
        let receipt = Reply::read(channel, slice.peer.epoch)?;
        if receipt.status != 0 || receipt.length != 0 {
            return Err(Error::Protocol("RDMA completion acknowledgement"));
        }
        slice.binding.read(channel, ACK)?;
        if cacheable {
            slice.binding.read(channel, READY)?;
            endpoint.cache(slice.peer)?;
        } else if slice.peer.operation == TransferOp::Read {
            remaining(channel.deadline)?;
            endpoint.copy_back(
                memory,
                slice.offset,
                #[cfg(feature = "cuda")]
                &self.proxy,
            )?;
        }
        drop(endpoint.output.take());
        Ok(())
    }

    pub(super) fn source_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        request: &Request,
        memory: &RegisteredMemory,
        paths: &[NicPath],
        expected: Option<[u8; 16]>,
        admission: &mut super::ParentAdmission,
    ) -> Result<Option<RdmaSlice>> {
        let epoch = request.epoch;
        let offset = usize::try_from(request.offset).map_err(|_| Error::InvalidRange)?;
        let length = usize::try_from(request.length).map_err(|_| Error::InvalidRange)?;
        let writing = request.operation == Operation::RdmaWrite;
        let operation = if writing {
            TransferOp::Write
        } else {
            TransferOp::Read
        };
        let hello = Hello::read(channel, epoch)?;
        if expected.is_some_and(|domain| domain != hello.expected_domain) {
            return Err(Error::Protocol("RDMA slice rendezvous changed planned PD"));
        }
        let remote_mr = self.direct(memory) && memory.is_cuda();
        let cacheable = hello.direct && self.direct(memory) && !remote_mr;
        if cacheable != (hello.qp_width != 0) || hello.qp_width > self.config.num_lanes {
            return Err(Error::Protocol("RDMA bootstrap QP width/profile mismatch"));
        }
        let mask = paths
            .iter()
            .filter(|path| path.domain == hello.expected_domain)
            .fold(0, |mask, path| mask | (1u64 << path.nic_id));
        if mask == 0 {
            return Err(Error::Protocol(
                "RDMA rendezvous protection domain mismatch",
            ));
        }
        let selection = self.allocate(length, memory.location()?.location, mask)?;
        let source_nic_id = selection.device_id();
        let grant_lease = self.reserve_grant(selection, memory, &admission.grants)?;
        let device = self.device_at(source_nic_id)?;
        device.check_address()?;
        let connection = if let Some(source) = hello.source {
            if !cacheable {
                channel.reply(epoch, CACHE_MISS, 0)?;
                return Ok(None);
            }
            let reuse = self.claim(
                device,
                |peer| {
                    peer.source
                        && peer.local == source.connection
                        && peer.epoch == hello.requester.epoch
                        && peer.connection == hello.requester.connection
                        && peer.operation == operation
                        && peer.generation >= hello.previous_grant
                },
                channel.deadline,
            )?;
            let Some(reuse) = reuse else {
                channel.reply(epoch, CACHE_MISS, 0)?;
                return Ok(None);
            };
            if reuse
                .endpoint
                .as_ref()
                .map_or(Ok(0), |owner| owner.width())?
                != hello.qp_width
            {
                return Err(Error::Protocol("RDMA reused pair changed QP width"));
            }
            Connection::Reuse(Box::new(reuse))
        } else if cacheable {
            self.connection(
                device,
                EndpointKey {
                    route: None,
                    epoch: hello.requester.epoch,
                    domain: hello.requester.connection.domain,
                    operation,
                    source: true,
                    source_nic_id,
                },
                hello.qp_width,
                Some(hello.requester.connection),
                channel.deadline,
            )?
        } else {
            Connection::Fresh(None)
        };
        let mut endpoint = self.prepare_grant(
            grant_lease,
            memory,
            offset..offset + length,
            if writing { 2 } else { 1 },
            channel.deadline,
            connection,
        )?;
        let prepared = (|| {
            if !writing {
                endpoint.fill(
                    memory,
                    offset,
                    #[cfg(feature = "cuda")]
                    &self.proxy,
                )?;
            }
            let identity = Identity::local(&endpoint, epoch)?;
            let grant = endpoint.grant_generation()?;
            let mut offer = [0; OFFER_SIZE];
            offer[..IDENTITY_SIZE].copy_from_slice(&identity.encode());
            offer[IDENTITY_SIZE..IDENTITY_SIZE + 8].copy_from_slice(&grant.to_be_bytes());
            offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1].copy_from_slice(&endpoint.connection());
            offer[OFFER_SIZE - 1] = if remote_mr {
                super::super::protocol::MR_AUTHORITY
            } else {
                u8::from(cacheable)
            };
            channel.reply(epoch, 0, OFFER_SIZE as u64)?;
            channel.write_all(&offer)?;
            let profile = offer[OFFER_SIZE - 1];
            let binding =
                Binding::accept(channel, request, identity, hello.requester, grant, profile)?;
            let mut bytes = [0; DESCRIPTOR_SIZE];
            channel.read_exact(&mut bytes)?;
            let remote = Descriptor::decode_connection(&bytes)?;
            endpoint.connect(&remote)?;
            if !remote_mr {
                endpoint.bind_window()?;
            }
            endpoint.post_receive(&binding.0, admission.receive.take()?)?;
            let peer = Peer {
                route: None,
                source_nic_id,
                epoch: hello.requester.epoch,
                local: endpoint.incarnation()?,
                connection: hello.requester.connection,
                operation,
                source: true,
                generation: grant,
            };
            if cacheable {
                endpoint.publish_peer(peer)?;
            }
            channel.reply(epoch, 9, 0)?;
            binding.write(channel, GRANT)?;
            channel.write_all(&endpoint.grant()?.encode())?;
            Ok((binding, remote, peer))
        })();
        match prepared {
            Ok((binding, remote, peer)) => Ok(Some(RdmaSlice {
                grant: endpoint,
                binding,
                remote,
                peer,
                offset,
            })),
            Err(error) => {
                #[cfg(all(test, feature = "cuda"))]
                crate::transport::rdma::cq_lane::tests::before_staging_abort(self, &endpoint);
                Err(endpoint.abort(error))
            }
        }
    }

    pub(super) fn finish_source_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        memory: &RegisteredMemory,
        slice: &mut RdmaSlice,
    ) -> Result<()> {
        let done = slice.binding.done(channel)?;
        let endpoint = &mut slice.grant;
        endpoint.receive_completion()?;
        let cacheable = slice.binding.0[BINDING_SIZE - 1] == 1;
        if cacheable {
            endpoint.finish_received(&done)?;
        } else {
            endpoint.close()?;
            if slice.peer.operation == TransferOp::Write {
                remaining(channel.deadline)?;
                endpoint.copy_back(
                    memory,
                    slice.offset,
                    #[cfg(feature = "cuda")]
                    &self.proxy,
                )?;
            }
            drop(endpoint.output.take());
        }
        #[cfg(test)]
        crate::transport::rdma::tests::before_source_ack(self);
        channel.reply(epoch, 0, 0)?;
        slice.binding.write(channel, ACK)?;
        if cacheable {
            endpoint.cache(slice.peer)?;
            if let Err(error) = slice.binding.write(channel, READY) {
                endpoint.retire_idle()?;
                return Err(error);
            }
        }
        Ok(())
    }
}
