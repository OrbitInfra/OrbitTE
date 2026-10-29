//! Private host registrations live exactly as long as the staging operation.

use std::sync::{Arc, Mutex};

use super::StageBufferLease;
use crate::cuda::StagingCopy;

use crate::engine::Source;
use crate::memory::MemoryRegistry;
use crate::{Error, HostMemory, Permission, RegisteredMemory, Result, SourceEpoch, Transport};

pub(super) struct StageRegistration {
    registry: MemoryRegistry,
    memory: Option<RegisteredMemory>,
    pub(super) host: Arc<HostMemory>,
    unregistered: bool,
    #[cfg(feature = "rdma")]
    rdma: Option<Arc<crate::transport::rdma::RdmaTransport>>,
}

impl StageRegistration {
    pub(super) fn new(
        host: Arc<HostMemory>,
        source: &Source,
        transport: Transport,
    ) -> Result<Self> {
        #[cfg(feature = "rdma")]
        let rdma = if transport == Transport::Rdma {
            Some(Arc::clone(
                source
                    .rdma
                    .as_ref()
                    .ok_or(Error::Unavailable("RDMA is not configured"))?,
            ))
        } else {
            None
        };
        let _ = (source, transport);
        let registry = MemoryRegistry::new(SourceEpoch::generate()?, 1, host.len());
        let memory = registry
            .register_host_batch(
                &[(Arc::clone(&host), 0, host.len())],
                Permission::RemoteReadWrite,
                |_| Ok(()),
            )?
            .remove(0);
        Ok(Self {
            registry,
            memory: Some(memory),
            host,
            unregistered: false,
            #[cfg(feature = "rdma")]
            rdma,
        })
    }

    pub(super) fn memory(&self) -> Result<&RegisteredMemory> {
        self.memory
            .as_ref()
            .ok_or(Error::Internal("staging registration is closed"))
    }

    pub(super) fn finish(&mut self, result: Result<()>) -> Result<()> {
        // An unproven native leg still owns the private bytes and its MR. Its
        // owner performs retention; do not retire or reuse them on this path.
        if matches!(result, Err(Error::Unquiesced { .. })) {
            return result;
        }
        let close = self.close().map_err(|error| match error {
            Error::Unquiesced { .. } => error,
            _ => Error::Unquiesced {
                operation: "staging registration cleanup",
                code: -1,
            },
        });
        close.and(result)
    }

    fn close(&mut self) -> Result<()> {
        let memory = self.memory()?;
        if !self.unregistered {
            self.registry.unregister(memory)?;
            self.unregistered = true;
        }
        let memory = self.memory()?;
        #[cfg(feature = "rdma")]
        if let Some(rdma) = &self.rdma {
            rdma.registrations.retire(memory)?;
        }
        // This private registration was never advertised or handed to the
        // application. An extra reference belongs to a native lease, retained
        // cleanup or construction owner. retire() alone is not a drain proof.
        if Arc::strong_count(&memory.0) != 1 {
            return Err(Error::Unquiesced {
                operation: "staging registration still borrowed",
                code: -1,
            });
        }
        self.memory = None;
        Ok(())
    }
}

impl Drop for StageRegistration {
    fn drop(&mut self) {
        if let Some(memory) = self.memory.take() {
            // Preserve actual backing and native owner after an unfinished
            // operation, including unwinding. Neither EOF nor Drop proves drain.
            std::mem::forget((memory, Arc::clone(&self.host)));
            #[cfg(feature = "rdma")]
            std::mem::forget(self.rdma.take());
        }
    }
}

/// Private registration and pinned backing are retired together, only after
/// both CUDA copies and transport DMA have drained.
pub(super) struct PinnedStage {
    pub(super) registration: StageRegistration,
    lease: Arc<Mutex<StageBufferLease>>,
    pending: Option<usize>,
}

impl PinnedStage {
    pub(super) fn new(
        lease: StageBufferLease,
        source: &Source,
        transport: Transport,
    ) -> Result<Self> {
        let length = lease.buffer()?.length;
        let address = lease.buffer()?.address;
        let lease = Arc::new(Mutex::new(lease));
        // SAFETY: this actual pinned lease backs the registration. HostAccess
        // excludes transport DMA while the range owner submits local CUDA work.
        #[allow(unsafe_code)]
        let host = Arc::new(unsafe { HostMemory::from_raw_parts(address, length, lease.clone())? });
        Ok(Self {
            registration: StageRegistration::new(host, source, transport)?,
            lease,
            pending: None,
        })
    }

    pub(super) fn copy(
        &mut self,
        copy: &mut StagingCopy,
        offset: usize,
        length: usize,
        to_device: bool,
        pipeline: bool,
    ) -> Result<()> {
        if self.pending.is_some() {
            return Err(Error::Internal("staging chunk still pending"));
        }
        copy.check_access()?;
        let result = self.registration.host.with_staging_access(length, || {
            let mut lease = self
                .lease
                .lock()
                .map_err(|_| Error::Internal("staging lease lock poisoned"))?;
            if pipeline {
                copy.begin(&mut lease, offset, length, to_device)
            } else {
                copy.copy(&mut lease, offset, length, to_device)
            }
        });
        if result.is_ok() && pipeline {
            self.pending = Some(length);
        }
        copy.publish_failure();
        result
    }

    pub(super) fn query(&mut self, copy: &mut StagingCopy) -> Result<bool> {
        let Some(length) = self.pending else {
            return Ok(true);
        };
        let result = copy.query(
            &mut self.lease.lock().unwrap_or_else(|p| p.into_inner()),
            length,
        );
        if !matches!(result, Ok(false)) {
            self.pending = None;
        }
        copy.publish_failure();
        result
    }

    pub(super) fn complete(&mut self, copy: &mut StagingCopy) -> Result<bool> {
        let Some(length) = self.pending.take() else {
            return Ok(false);
        };
        let result = copy.complete(
            &mut self.lease.lock().unwrap_or_else(|p| p.into_inner()),
            length,
        );
        copy.publish_failure();
        result.map(|()| true)
    }

    pub(super) fn finish(mut self, result: Result<()>) -> Result<()> {
        let result = if self.pending.is_some() {
            Err(Error::Unquiesced {
                operation: "staging copy still pending",
                code: -1,
            })
        } else {
            result
        };
        let result = self.registration.finish(result);
        if matches!(result, Err(Error::Unquiesced { .. })) {
            self.lease
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .quarantine();
            return result;
        }
        drop(self.registration);
        let lease = Arc::try_unwrap(self.lease).map_err(|lease| {
            lease.lock().unwrap_or_else(|p| p.into_inner()).quarantine();
            std::mem::forget(lease);
            Error::Unquiesced {
                operation: "pinned stage still borrowed",
                code: -1,
            }
        })?;
        lease
            .into_inner()
            .unwrap_or_else(|p| p.into_inner())
            .release()
            .and(result)
    }
}
