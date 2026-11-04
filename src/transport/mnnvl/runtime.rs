use crate::cuda::{fabric::Platform, ipc::IpcExport};
use crate::segment::ProcessIdentity;
use crate::transport::nvlink::NVLinkTransport;
use crate::{EngineConfig, Error, MnnvlConfig, RegisteredMemory, Result};
use std::time::Instant;

impl NVLinkTransport {
    pub(crate) fn install_mnnvl(c: &MnnvlConfig, engine: &EngineConfig) -> Result<Self> {
        let mut owner = Self::new(
            engine.max_inflight_bytes,
            engine.nvlink_cache_entries,
            engine.nvlink_stream_count,
        )?;
        owner.fabric = Some(Platform::install(
            c,
            owner.identity.machine,
            engine.max_registered_regions,
            engine.max_registered_bytes,
        )?);
        Ok(owner)
    }
    pub(crate) fn wire_identity(&self) -> ProcessIdentity {
        ProcessIdentity {
            machine: self
                .fabric
                .as_ref()
                .map_or(self.identity.machine, |f| f.caps.domain),
            process: self.identity.process,
        }
    }
    pub(crate) fn validate_identity(&self, peer: ProcessIdentity) -> Result<()> {
        if self.fabric.is_none() {
            return self.identity.validate_peer(peer);
        }
        if peer.machine != self.wire_identity().machine {
            return Err(Error::Unavailable("different MNNVL fabric domains"));
        }
        Ok(())
    }
    pub(crate) fn grant_magic(&self) -> &'static [u8; 4] {
        if self.fabric.is_some() {
            b"MNFG"
        } else {
            b"NVL1"
        }
    }
    pub(crate) fn cache_magic(&self) -> &'static [u8; 4] {
        if self.fabric.is_some() {
            b"MNFC"
        } else {
            b"NVL2"
        }
    }
    pub(crate) fn export(
        &self,
        m: &RegisteredMemory,
        offset: usize,
        len: usize,
        deadline: Instant,
    ) -> Result<(IpcExport, usize)> {
        if self.fabric.is_some() {
            let share =
                m.0.fabric
                    .get()
                    .and_then(|r| r.share.as_ref())
                    .ok_or(Error::Unavailable("region has no MNNVL export"))?;
            let (owner, at) = m.allocation_range(offset, len)?;
            Ok((IpcExport::fabric(owner, share, deadline)?, at))
        } else {
            let (owner, at) = m.cuda_range(offset, len)?;
            Ok((IpcExport::new(owner)?, at))
        }
    }
}
