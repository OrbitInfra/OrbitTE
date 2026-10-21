use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::memory::{MemoryRegistry, checked_range};
use crate::tcp::{self, TcpTransport};
use crate::{
    EngineConfig, Error, HostMemory, Permission, RegisteredMemory, RemoteRegion, RemoteSegment,
    Result, SourceEpoch,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferOp {
    Read,
    Write,
}

/// One CPU transfer. The owned local handle survives concurrent unregister.
#[derive(Clone, Debug)]
pub struct TransferRequest {
    pub local: RegisteredMemory,
    pub local_offset: usize,
    pub remote: RemoteRegion,
    pub remote_offset: usize,
    pub length: usize,
}

/// Whether the failed request may have modified remote memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoteEffect {
    None,
    /// A WRITE body was attempted but its final acknowledgement was not received.
    /// Do not automatically replay it or treat a timeout as remote quiescence.
    Unknown,
}

#[derive(Debug)]
pub struct TransferFailure {
    pub request_index: usize,
    pub error: Error,
    pub remote_effect: RemoteEffect,
}

/// Requests run in order and stop on the first failure. Earlier completed
/// requests remain committed; later requests were never attempted.
#[derive(Debug)]
pub struct BatchReport {
    pub completed_requests: usize,
    pub transferred_bytes: usize,
    pub failure: Option<TransferFailure>,
}

impl BatchReport {
    pub fn is_complete(&self) -> bool {
        self.failure.is_none()
    }
}

/// Independently sampled resource counters, including unregistered allocations
/// still retained by local handles or admitted handlers. Not a global snapshot.
#[derive(Clone, Copy, Debug)]
pub struct EngineStats {
    pub retained_regions: usize,
    pub retained_bytes: usize,
    pub active_source_operations: usize,
    pub inflight_batches: usize,
    pub inflight_bytes: usize,
}

/// Engine, source registry and bounded TCP handler ownership.
pub struct TransferEngine {
    config: EngineConfig,
    source: Arc<Source>,
    transport: TcpTransport,
    batches: Mutex<BatchUsage>,
}

impl TransferEngine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        config.validate()?;
        let epoch = SourceEpoch::generate()?;
        let source = Arc::new(Source {
            epoch,
            registry: MemoryRegistry::new(
                epoch,
                config.max_registered_regions,
                config.max_registered_bytes,
            ),
            admission: Mutex::new(SourceAdmission {
                open: true,
                active: 0,
            }),
            drained: Condvar::new(),
        });
        let transport = TcpTransport::start(Arc::clone(&source), &config)?;
        Ok(Self {
            config,
            source,
            transport,
            batches: Mutex::default(),
        })
    }

    pub fn endpoint(&self) -> SocketAddr {
        self.transport.endpoint()
    }
    pub fn source_epoch(&self) -> SourceEpoch {
        self.source.epoch
    }

    pub fn stats(&self) -> Result<EngineStats> {
        let (retained_regions, retained_bytes) = self.source.registry.usage()?;
        let active_source_operations = self
            .source
            .admission
            .lock()
            .map_err(|_| Error::Internal("source lock poisoned"))?
            .active;
        let batches = self
            .batches
            .lock()
            .map_err(|_| Error::Internal("batch lock poisoned"))?;
        Ok(EngineStats {
            retained_regions,
            retained_bytes,
            active_source_operations,
            inflight_batches: batches.count,
            inflight_bytes: batches.bytes,
        })
    }

    pub fn register_memory(
        &self,
        bytes: Box<[u8]>,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        let _admission = self.source.admit()?;
        self.source.registry.register(bytes, permission)
    }

    /// Registers an existing host allocation range. Exact duplicate ranges share
    /// a registration reference and retain the first permission, as in TENT.
    /// Overlapping ranges must share the same HostMemory access lock.
    pub fn register_host_memory(
        &self,
        allocation: Arc<HostMemory>,
        offset: usize,
        length: usize,
        permission: Permission,
    ) -> Result<RegisteredMemory> {
        let _admission = self.source.admit()?;
        self.source
            .registry
            .register_host(allocation, offset, length, permission)
    }

    /// Resolves an absolute local address to an owned registered range and offset.
    /// Matches TENT's address order, preferring the longer range at equal bases.
    pub fn resolve_local_address(
        &self,
        address: u64,
        length: usize,
    ) -> Result<(RegisteredMemory, usize)> {
        self.source.registry.resolve_address(address, length)
    }

    /// Removes one explicit registration reference; the final reference removes
    /// visibility for new remote requests. Already admitted requests and
    /// local handles keep the allocation and its budget charge until last use.
    /// Call `close_source` to establish source-handler quiescence first.
    pub fn unregister_memory(&self, memory: &RegisteredMemory) -> Result<()> {
        self.source.registry.unregister(memory)
    }

    /// Open/refresh using an epoch supplied by the authoritative source owner.
    /// Discovery and a newly responding process cannot silently replace it.
    pub fn open_segment(
        &self,
        endpoint: SocketAddr,
        expected_epoch: SourceEpoch,
        timeout: Duration,
    ) -> Result<RemoteSegment> {
        let deadline = deadline(timeout)?;
        let _admission = self.admit_batch(0)?;
        tcp::open_segment(endpoint, expected_epoch, deadline)
    }

    /// Validates the entire batch before sending any request. Admission and
    /// validation failures return `Err`; execution failures are in the report.
    /// READ commits each local slice only after its full response arrives.
    /// WRITE uses a final source acknowledgement and never retries automatically.
    pub fn submit_and_wait(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        requests: &[TransferRequest],
        timeout: Duration,
    ) -> Result<BatchReport> {
        if requests.len() > self.config.max_batch_requests {
            return Err(Error::ResourceExhausted);
        }
        let deadline = deadline(timeout)?;
        let mut total_bytes = 0usize;
        for request in requests {
            self.source.registry.check_owner(&request.local)?;
            self.source.registry.get(request.local.id())?;
            if request.remote.epoch != peer.source_epoch() {
                return Err(Error::ForeignRegistration);
            }
            if !peer
                .regions()
                .iter()
                .any(|region| region.id == request.remote.id)
            {
                return Err(Error::RegionNotFound);
            }
            checked_range(request.local_offset, request.length, request.local.len())?;
            checked_range(request.remote_offset, request.length, request.remote.len())?;
            if !request
                .remote
                .permission
                .allows(operation == TransferOp::Write)
            {
                return Err(Error::PermissionDenied);
            }
            if request.length > self.config.max_transfer_bytes {
                return Err(Error::ResourceExhausted);
            }
            total_bytes = total_bytes
                .checked_add(request.length)
                .ok_or(Error::ResourceExhausted)?;
        }
        let _admission = self.admit_batch(total_bytes)?;
        let mut report = BatchReport {
            completed_requests: 0,
            transferred_bytes: 0,
            failure: None,
        };
        for (index, request) in requests.iter().enumerate() {
            if let Err((error, remote_effect)) = tcp::transfer(operation, peer, request, deadline) {
                report.failure = Some(TransferFailure {
                    request_index: index,
                    error,
                    remote_effect,
                });
                break;
            }
            report.completed_requests += 1;
            report.transferred_bytes += request.length;
        }
        Ok(report)
    }

    /// Closes source admission permanently, then waits for handlers already
    /// admitted to release backing references. Timeout leaves admission closed;
    /// a later call may wait again. It does not claim that a remote requester
    /// received a WRITE acknowledgement. Requester operations remain available.
    pub fn close_source(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))?;
        let mut admission = self
            .source
            .admission
            .lock()
            .map_err(|_| Error::Internal("source lock poisoned"))?;
        admission.open = false;
        while admission.active != 0 {
            let remaining = remaining(deadline)?;
            let (guard, _) = self
                .source
                .drained
                .wait_timeout(admission, remaining)
                .map_err(|_| Error::Internal("source lock poisoned"))?;
            admission = guard;
        }
        Ok(())
    }

    fn admit_batch(&self, bytes: usize) -> Result<BatchPermit<'_>> {
        let mut usage = self
            .batches
            .lock()
            .map_err(|_| Error::Internal("batch lock poisoned"))?;
        let new_bytes = usage
            .bytes
            .checked_add(bytes)
            .ok_or(Error::ResourceExhausted)?;
        if usage.count >= self.config.max_inflight_batches
            || new_bytes > self.config.max_inflight_bytes
        {
            return Err(Error::ResourceExhausted);
        }
        usage.count += 1;
        usage.bytes = new_bytes;
        Ok(BatchPermit {
            batches: &self.batches,
            bytes,
        })
    }
}

pub(crate) fn deadline(timeout: Duration) -> Result<Instant> {
    if timeout.is_zero() {
        return Err(Error::Timeout);
    }
    Instant::now()
        .checked_add(timeout)
        .ok_or(Error::InvalidConfig("deadline overflows monotonic clock"))
}

pub(crate) fn remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::Timeout);
    }
    Ok(remaining)
}

pub(crate) struct Source {
    pub(crate) epoch: SourceEpoch,
    pub(crate) registry: MemoryRegistry,
    admission: Mutex<SourceAdmission>,
    drained: Condvar,
}

struct SourceAdmission {
    open: bool,
    active: usize,
}

impl Source {
    pub(crate) fn admit(&self) -> Result<SourcePermit<'_>> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| Error::Internal("source lock poisoned"))?;
        if !admission.open {
            return Err(Error::SourceClosed);
        }
        admission.active += 1;
        Ok(SourcePermit(self))
    }
}

pub(crate) struct SourcePermit<'a>(&'a Source);

impl Drop for SourcePermit<'_> {
    fn drop(&mut self) {
        let mut admission = self
            .0
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        admission.active -= 1;
        if admission.active == 0 {
            self.0.drained.notify_all();
        }
    }
}

#[derive(Default)]
struct BatchUsage {
    count: usize,
    bytes: usize,
}

struct BatchPermit<'a> {
    batches: &'a Mutex<BatchUsage>,
    bytes: usize,
}

impl Drop for BatchPermit<'_> {
    fn drop(&mut self) {
        let mut usage = self
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        usage.count -= 1;
        usage.bytes -= self.bytes;
    }
}

#[cfg(test)]
#[path = "../tests/unit/engine.rs"]
mod tests;
