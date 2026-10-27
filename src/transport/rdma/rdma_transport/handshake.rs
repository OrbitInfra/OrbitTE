//! Transport admission, grant construction and bilateral connection rendezvous.

use std::sync::Arc;
use std::time::Instant;

use super::{RdmaTransport, connections::Connection};
use crate::engine::{PreparedTransfer, remaining};
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request, u64_at};
use crate::transport::rdma::{
    DESCRIPTOR_SIZE, RdmaHostMemory,
    endpoint::{EndpointKey, Peer},
    grant::{Buffer, GrantLease, GrantResources, RdmaGrant, Staging, admission::GrantCredits},
    path::NicPath,
    protocol::{
        ACCEPT, ACK, BINDING_SIZE, Binding, CACHE_MISS, DONE, GRANT, Hello, IDENTITY_SIZE,
        Identity, OFFER_SIZE, READY,
    },
    qp::{ConnectionLease, ConnectionResources, Descriptor, Resources},
    quota::DeviceLease,
    slice::RdmaSlice,
    sys,
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
        remaining(deadline)?;
        let (reuse, construction) = match connection {
            Connection::Reuse(grant) => (Some(*grant), None),
            Connection::Fresh(slot) => (None, slot),
        };
        let offset = range.start;
        let length = range.end.checked_sub(offset).ok_or(Error::InvalidRange)?;
        if length != grant_lease.length {
            return Err(Error::Internal("RDMA selection length mismatch"));
        }
        if length == 0 || length > u32::MAX as usize {
            return Err(Error::InvalidRange);
        }
        crate::memory::checked_range(offset, length, memory.len())?;
        let direct = if self.direct(memory) {
            Some(memory.dma_lease(offset, length)?)
        } else {
            None
        };
        let is_direct = direct.is_some();
        if reuse.is_some() && !is_direct {
            return Err(Error::Protocol(
                "RDMA reuse requires host Direct on both peers",
            ));
        }
        if is_direct != grant_lease.is_direct {
            return Err(Error::Internal("RDMA grant memory admission mismatch"));
        }
        let buffer = match direct {
            Some(loan) => Buffer::Direct {
                address: loan.address(),
                length: loan.len(),
                loan: Some(loan),
            },
            None => Buffer::Staged(Staging::new(length)?),
        };
        let device = self.device_at(grant_lease._selection.device_id())?;
        let (progress, worker_lease) = if let Some(endpoint) = &reuse {
            (
                Arc::clone(&endpoint.progress),
                self.workers.reserve(&endpoint.progress)?,
            )
        } else {
            self.workers.select(&self.cq_list(Arc::clone(&device))?)?
        };
        let new_connection_lease = if reuse.is_none() {
            Some(loop {
                let mut usage = self
                    .usage
                    .lock()
                    .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
                if usage.active_endpoints < self.config.max_endpoints {
                    usage.active_endpoints += 1;
                    break ConnectionLease {
                        usage: Arc::clone(&self.usage),
                        drained: Arc::clone(&self.drained),
                    };
                }
                drop(usage);
                if !self.evict_idle(deadline)? {
                    return Err(Error::ResourceExhausted);
                }
            })
        } else {
            None
        };
        let remote_mr = is_direct && memory.is_cuda() && permission != 0;
        let cached_mr = if is_direct && !remote_mr {
            Some(self.registrations.acquire(
                Arc::clone(&device),
                memory,
                permission != 0,
                deadline,
            )?)
        } else {
            None
        };
        let mut memory_handles = sys::MemoryHandles {
            remote_mr,
            ..Default::default()
        };
        if let Some(registration) = &cached_mr {
            memory_handles.mr = registration.handle()?;
        }
        let mut grant = GrantResources {
            _worker: worker_lease,
            device: Arc::clone(&device),
            handles: memory_handles,
            cached_mr,
            buffer,
            _lease: grant_lease,
            max_read_slices: if memory.is_cuda() { 32 } else { 64 },
            permission,
            generation: 0,
            rkey: 0,
            connection: None,
            window_used: false,
            window_bound: false,
            message: None,
        };
        let mut endpoint = if let Some(endpoint) = reuse {
            {
                let mut request = endpoint.record.lock()?;
                request.check_claim(endpoint.claim)?;
                let resources = request
                    .resources
                    .as_mut()
                    .ok_or(Error::Internal("missing cached connection"))?;
                let connection = resources.connection.lock()?;
                if resources.grant.is_some() || !Arc::ptr_eq(&connection.device, &device) {
                    return Err(Error::Internal("RDMA reuse grant ownership"));
                }
                if connection.permission != permission {
                    return Err(Error::Protocol("RDMA reuse changed QP capability"));
                }
                grant.connection = connection.incarnation;
                drop(connection);
                resources.grant = Some(grant);
            }
            endpoint
        } else {
            let connection_lease = new_connection_lease
                .ok_or(Error::Internal("missing physical connection reservation"))?;
            let connection_handles = sys::ConnectionHandles {
                cq: progress.handle(false)?,
                receive_cq: progress.handle(true)?,
                ..Default::default()
            };
            let record = progress.register(Box::new(Resources {
                connection: super::super::qp::RdmaQp::new(ConnectionResources {
                    receive_queue: super::super::qp::receive_queue::ReceiveQueue::new(
                        self.config.max_cqes,
                        self.config.max_grants,
                        Arc::clone(&self.usage),
                    )?,
                    device,
                    handles: connection_handles,
                    send_queue: super::super::qp::send_queue::SendQueue::new(
                        self.config.max_send_wr,
                        Arc::clone(&self.usage),
                    )?,
                    incarnation: None,
                    local: Descriptor::default(),
                    remote: None,
                    permission,
                    closed: false,
                    lease: Some(connection_lease),
                }),
                grant: Some(grant),
            }))?;
            RdmaGrant {
                record,
                progress,
                local: Descriptor::default(),
                incarnation: None,
                config: self.config.clone(),
                deadline,
                output: None,
                claim: 1,
                released: false,
                cached_peer: None,
                endpoint: None,
                construction: None,
            }
        };
        if let Some(slot) = construction {
            slot.attach(&mut endpoint)?;
            endpoint.construction = Some(slot);
        }
        #[cfg(test)]
        crate::transport::rdma::tests::before_grant_open(self, &endpoint);
        let epoch = SourceEpoch::generate()?;
        let psn = u32::from_be_bytes([0, epoch.0[0], epoch.0[1], epoch.0[2]]);
        if let Err(error) = endpoint.open(permission, psn) {
            endpoint.close()?;
            return Err(error);
        }
        if is_direct {
            self.usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                .opened_direct_endpoints += 1;
        }
        Ok(endpoint)
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

    pub(super) fn direct(&self, memory: &RegisteredMemory) -> bool {
        if memory.is_cuda() {
            self.config.cuda_memory == crate::transport::rdma::RdmaCudaMemory::Direct
        } else {
            self.config.host_memory == RdmaHostMemory::Direct
        }
    }

    pub(super) fn requester_grant(
        &self,
        channel: &ControlChannel<'_>,
        wire: &Request,
        request: &PreparedTransfer,
        grant_lease: GrantLease,
        remote_path: &NicPath,
        width: usize,
    ) -> Result<RdmaGrant> {
        let route = channel.stream.peer_addr()?;
        let operation = if wire.operation == Operation::RdmaRead {
            TransferOp::Read
        } else {
            TransferOp::Write
        };
        let direct = self.direct(&request.local);
        let device = self.device_at(grant_lease._selection.device_id())?;
        device.check_address()?;
        let connection = if direct && width != 0 {
            self.connection(
                device,
                EndpointKey {
                    route: Some(route),
                    epoch: wire.epoch,
                    domain: remote_path.domain,
                    operation,
                    source: false,
                    source_nic_id: remote_path.nic_id,
                },
                width,
                None,
                channel.deadline,
            )?
        } else {
            Connection::Fresh(None)
        };
        let endpoint = self.prepare_grant(
            grant_lease,
            &request.local,
            request.local_offset..request.local_offset + request.length,
            0,
            channel.deadline,
            connection,
        )?;
        Ok(endpoint)
    }

    pub(super) fn requester_slice(
        &self,
        channel: &mut ControlChannel<'_>,
        wire: &Request,
        request: &PreparedTransfer,
        epoch: SourceEpoch,
        remote_path: &NicPath,
        mut endpoint: RdmaGrant,
    ) -> Result<Option<RdmaSlice>> {
        let route = channel.stream.peer_addr()?;
        let operation = if wire.operation == Operation::RdmaRead {
            TransferOp::Read
        } else {
            TransferOp::Write
        };
        let direct = self.direct(&request.local);
        let cached = endpoint.cached_peer;
        let prepared = (|| {
            if operation == TransferOp::Write {
                endpoint.fill(&request.local, request.local_offset)?;
            }
            Hello {
                requester: Identity::local(&endpoint, epoch)?,
                source: cached.map(|peer| Identity {
                    epoch: peer.epoch,
                    connection: peer.connection,
                }),
                expected_domain: remote_path.domain,
                direct,
                qp_width: endpoint
                    .endpoint
                    .as_ref()
                    .map_or(Ok(0), |owner| owner.width())?,
                previous_grant: cached.map_or(0, |peer| peer.generation),
            }
            .write(channel)?;
            let reply = Reply::read(channel, wire.epoch)?;
            if reply.status == CACHE_MISS && reply.length == 0 && cached.is_some() {
                return Ok(None);
            }
            if reply.status != 0 || reply.length != OFFER_SIZE as u64 {
                return Err(Error::Protocol("RDMA bootstrap length/status"));
            }
            let mut offer = [0; OFFER_SIZE];
            channel.read_exact(&mut offer)?;
            let remote_identity = Identity::decode(&offer[..IDENTITY_SIZE])?;
            if remote_identity.epoch != wire.epoch {
                return Err(Error::EpochMismatch);
            }
            let profile = offer[OFFER_SIZE - 1];
            if profile != super::super::protocol::MR_AUTHORITY
                && remote_identity.connection.domain != remote_path.domain
            {
                return Err(Error::Protocol("RDMA source changed its protection domain"));
            }
            if cached.is_some_and(|peer| remote_identity.connection != peer.connection) {
                return Err(Error::Protocol("RDMA source changed cached connection"));
            }
            let cacheable = match offer[OFFER_SIZE - 1] {
                0 | super::super::protocol::MR_AUTHORITY if endpoint.endpoint.is_none() => false,
                1 if direct && endpoint.endpoint.is_some() => true,
                _ => return Err(Error::Protocol("RDMA release profile mismatch")),
            };
            let grant = u64_at(&offer, IDENTITY_SIZE);
            if cached.is_some_and(|peer| grant <= peer.generation) {
                return Err(Error::Protocol(
                    "RDMA source reused an old grant generation",
                ));
            }
            let mut descriptor = [0; DESCRIPTOR_SIZE];
            descriptor.copy_from_slice(&offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1]);
            let connection = Descriptor::decode_connection(&descriptor)?;
            if !remote_path.matches_connection(&connection) {
                return Err(Error::Protocol(
                    "RDMA source changed its advertised NIC path",
                ));
            }
            let binding = Binding::new(
                wire,
                remote_identity,
                Identity::local(&endpoint, epoch)?,
                grant,
                profile,
            )?;
            binding.write(channel, ACCEPT)?;
            channel.write_all(&endpoint.connection())?;
            endpoint.connect(&connection)?;
            let ready = Reply::read(channel, wire.epoch)?;
            if ready.status != 9 || ready.length != 0 {
                return Err(Error::Protocol("RDMA missing ready response"));
            }
            binding.read(channel, GRANT)?;
            channel.read_exact(&mut descriptor)?;
            let remote = Descriptor::decode(&descriptor)?;
            if remote.connection_bytes() != connection.connection_bytes() {
                return Err(Error::Protocol("RDMA grant changed its connection"));
            }
            let peer = Peer {
                route: Some(route),
                source_nic_id: remote_path.nic_id,
                epoch: wire.epoch,
                local: endpoint.incarnation()?,
                connection: remote_identity.connection,
                operation,
                source: false,
                generation: grant,
            };
            if cacheable {
                endpoint.publish_peer(peer)?;
            }
            Ok(Some((binding, remote, peer)))
        })();
        match prepared {
            Ok(Some((binding, remote, peer))) => Ok(Some(RdmaSlice {
                grant: endpoint,
                binding,
                remote,
                peer,
                offset: request.local_offset,
            })),
            result => {
                endpoint.close()?;
                result.map(|_| None)
            }
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
            endpoint.copy_back(memory, slice.offset)?;
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
        let selection = self.allocate(length, mask)?;
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
                endpoint.fill(memory, offset)?;
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
                endpoint.close()?;
                Err(error)
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
                endpoint.copy_back(memory, slice.offset)?;
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
