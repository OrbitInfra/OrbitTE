//! Owns admitted grant resources until an MR and native QP are ready.

use std::sync::Arc;
use std::time::Instant;

use super::{Buffer, GrantLease, GrantResources, RdmaGrant, Staging};
use crate::engine::remaining;
use crate::transport::rdma::{
    cq_lane::CqLane,
    qp::{ConnectionLease, ConnectionResources, Descriptor, Resources},
    rdma_transport::{RdmaTransport, connections::Connection},
    sys,
};
use crate::{Error, RegisteredMemory, Result, SourceEpoch};

pub(in crate::transport::rdma) struct GrantConstruction {
    grant: Option<GrantResources>,
    progress: Arc<CqLane>,
    connection: Option<Connection>,
    connection_lease: Option<ConnectionLease>,
    memory: RegisteredMemory,
    is_direct: bool,
}

impl GrantConstruction {
    pub(in crate::transport::rdma) fn new(
        transport: &RdmaTransport,
        grant_lease: GrantLease,
        memory: &RegisteredMemory,
        range: std::ops::Range<usize>,
        permission: u32,
        deadline: Instant,
        connection: Connection,
    ) -> Result<Self> {
        let initialized = (|| {
            remaining(deadline)?;
            let reuse = match &connection {
                Connection::Reuse(grant) => Some(grant),
                Connection::Fresh(_) => None,
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
            let direct = if transport.direct(memory) {
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
            let device = transport.device_at(grant_lease._selection.device_id())?;
            let (progress, worker_lease) = if let Some(endpoint) = &reuse {
                (
                    Arc::clone(&endpoint.progress),
                    transport.workers.reserve(&endpoint.progress)?,
                )
            } else {
                transport
                    .workers
                    .select(&transport.cq_list(Arc::clone(&device))?)?
            };
            let new_connection_lease = if reuse.is_none() {
                Some(loop {
                    let mut usage = transport
                        .usage
                        .lock()
                        .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
                    if usage.active_endpoints < transport.config.max_endpoints {
                        usage.active_endpoints += 1;
                        break ConnectionLease {
                            usage: Arc::clone(&transport.usage),
                            drained: Arc::clone(&transport.drained),
                        };
                    }
                    drop(usage);
                    if !transport.evict_idle(deadline)? {
                        return Err(Error::ResourceExhausted);
                    }
                })
            } else {
                None
            };

            let remote_mr = is_direct && memory.is_cuda() && permission != 0;
            let grant = GrantResources {
                _worker: worker_lease,
                device,
                handles: sys::MemoryHandles {
                    remote_mr,
                    ..Default::default()
                },
                cached_mr: None,
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
            Ok((grant, progress, new_connection_lease, is_direct))
        })();
        let (grant, progress, connection_lease, is_direct) = match initialized {
            Ok(value) => value,
            Err(error) => return Err(abort_connection(Some(connection), error)),
        };
        Ok(Self {
            grant: Some(grant),
            progress,
            connection: Some(connection),
            connection_lease,
            memory: memory.clone(),
            is_direct,
        })
    }

    pub(in crate::transport::rdma) fn poll(
        &mut self,
        transport: &RdmaTransport,
        deadline: Instant,
    ) -> Result<Option<RdmaGrant>> {
        remaining(deadline)?;
        let grant = self
            .grant
            .as_mut()
            .ok_or(Error::Internal("grant construction is consumed"))?;
        if self.is_direct && !grant.handles.remote_mr && grant.cached_mr.is_none() {
            let Some(registration) = transport.registrations.poll_acquire(
                Arc::clone(&grant.device),
                &self.memory,
                grant.permission != 0,
                deadline,
            )?
            else {
                return Ok(None);
            };
            grant.cached_mr = Some(registration);
            grant.handles.mr = grant
                .cached_mr
                .as_ref()
                .ok_or(Error::Internal("missing prepared MR"))?
                .handle()?;
        }
        self.open(transport, deadline).map(Some)
    }

    fn open(&mut self, transport: &RdmaTransport, deadline: Instant) -> Result<RdmaGrant> {
        remaining(deadline)?;
        let grant = self
            .grant
            .as_ref()
            .ok_or(Error::Internal("missing prepared grant"))?;
        let permission = grant.permission;
        let device = Arc::clone(&grant.device);
        let connection = self
            .connection
            .take()
            .ok_or(Error::Internal("missing prepared connection"))?;
        let (mut endpoint, construction) = match connection {
            Connection::Reuse(endpoint) => (*endpoint, None),
            Connection::Fresh(construction) => {
                let handles = sys::ConnectionHandles {
                    cq: self.progress.handle(false)?,
                    receive_cq: self.progress.handle(true)?,
                    ..Default::default()
                };
                let receive_queue = crate::transport::rdma::qp::receive_queue::ReceiveQueue::new(
                    transport.config.max_cqes,
                    transport.config.max_grants,
                    Arc::clone(&transport.usage),
                )?;
                let send_queue = crate::transport::rdma::qp::send_queue::SendQueue::new(
                    transport.config.max_send_wr,
                    Arc::clone(&transport.usage),
                )?;
                let record =
                    self.progress.register(Box::new(Resources {
                        connection: crate::transport::rdma::qp::RdmaQp::new(ConnectionResources {
                            receive_queue,
                            device: Arc::clone(&device),
                            handles,
                            send_queue,
                            incarnation: None,
                            local: Descriptor::default(),
                            remote: None,
                            permission,
                            closed: false,
                            lease: Some(self.connection_lease.take().ok_or(Error::Internal(
                                "missing prepared connection reservation",
                            ))?),
                        }),
                        grant: None,
                    }))?;
                (
                    RdmaGrant {
                        record,
                        progress: Arc::clone(&self.progress),
                        local: Descriptor::default(),
                        incarnation: None,
                        config: transport.config.clone(),
                        deadline,
                        output: None,
                        claim: 1,
                        released: false,
                        cached_peer: None,
                        endpoint: None,
                        construction: None,
                    },
                    construction,
                )
            }
        };
        #[cfg(test)]
        tests::before_attach(transport, &endpoint);
        let initialized = (|| {
            {
                let mut request = endpoint.record.lock()?;
                request.check_claim(endpoint.claim)?;
                let resources = request
                    .resources
                    .as_mut()
                    .ok_or(Error::Internal("missing prepared connection"))?;
                let connection = resources.connection.lock()?;
                if resources.grant.is_some() || !Arc::ptr_eq(&connection.device, &device) {
                    return Err(Error::Internal("RDMA grant ownership"));
                }
                if connection.permission != permission {
                    return Err(Error::Protocol("RDMA reuse changed QP capability"));
                }
                let grant = self
                    .grant
                    .as_mut()
                    .ok_or(Error::Internal("missing prepared grant"))?;
                grant.connection = connection.incarnation;
                drop(connection);
                resources.grant = self.grant.take();
            }
            if let Some(slot) = construction {
                slot.attach(&mut endpoint)?;
                endpoint.construction = Some(slot);
            }
            #[cfg(test)]
            crate::transport::rdma::tests::before_grant_open(transport, &endpoint);
            let epoch = SourceEpoch::generate()?;
            let psn = u32::from_be_bytes([0, epoch.0[0], epoch.0[1], epoch.0[2]]);
            endpoint.open(permission, psn)?;
            if self.is_direct {
                transport
                    .usage
                    .lock()
                    .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                    .opened_direct_endpoints += 1;
            }
            Ok(())
        })();
        match initialized {
            Ok(()) => Ok(endpoint),
            Err(error) => Err(endpoint.abort(error)),
        }
    }

    pub(in crate::transport::rdma) fn abort(&mut self, error: Error) -> Error {
        let mut error = abort_connection(self.connection.take(), error);
        if let Some(mut grant) = self.grant.take() {
            if let Some(registration) = &mut grant.cached_mr {
                if let Err(cleanup) = registration.release() {
                    if !matches!(error, Error::Unquiesced { .. })
                        || matches!(cleanup, Error::Unquiesced { .. })
                    {
                        error = cleanup;
                    }
                }
            }
        }
        self.connection_lease = None;
        error
    }
}

fn abort_connection(connection: Option<Connection>, error: Error) -> Error {
    match connection {
        Some(Connection::Reuse(mut endpoint)) => endpoint.abort(error),
        _ => error,
    }
}

impl Drop for GrantConstruction {
    fn drop(&mut self) {
        let _ = self.abort(Error::Cancelled);
    }
}

#[cfg(test)]
#[path = "../../../../tests/unit/transport/rdma/grant/construction.rs"]
mod tests;
