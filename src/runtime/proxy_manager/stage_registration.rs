//! Private host registrations live exactly as long as the staging operation.

use std::sync::Arc;

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
