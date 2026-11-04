//! Multi-node NVLink capability and cooperative relocation.
#[cfg(feature = "cuda")]
mod runtime;
use crate::{Error, MemoryType, Result, SourceEpoch};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MnnvlHandleType {
    #[default]
    Fabric,
    PosixFd,
}

/// Typed counterparts of TENT's NVLink handle, host registration and threshold
/// settings and transports/mnnvl/egm. Selecting this profile replaces legacy IPC.
#[derive(Clone, Debug)]
pub struct MnnvlConfig {
    pub handle_type: MnnvlHandleType,
    pub host_register: bool,
    pub egm: bool,
    /// Same fabric/IMEX domain on cooperating hosts. None restricts placement to this boot.
    pub fabric_domain: Option<SourceEpoch>,
    /// Frozen TENT configuration units (128 KiB). Both sides of the threshold use event completion.
    pub async_memcpy_threshold: usize,
}
impl Default for MnnvlConfig {
    fn default() -> Self {
        Self {
            handle_type: MnnvlHandleType::Fabric,
            host_register: true,
            egm: false,
            fabric_domain: None,
            async_memcpy_threshold: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Location {
    Device,
    Host(u32),
}
impl Location {
    pub(crate) fn wire(location: Option<Self>) -> u32 {
        match location {
            None => 0,
            Some(Self::Device) => 1,
            Some(Self::Host(node)) => 0x8000_0000 | node,
        }
    }
    pub(crate) fn decode(
        value: u32,
        memory: MemoryType,
        caps: Option<Capability>,
    ) -> Result<Option<Self>> {
        match (value, memory, caps) {
            (0, _, _) => Ok(None),
            (1, MemoryType::Cuda, Some(_)) => Ok(Some(Self::Device)),
            (n, MemoryType::Cpu, Some(c)) if c.egm && n & 0x8000_0000 != 0 => {
                Ok(Some(Self::Host(n & 0x7fff_ffff)))
            }
            _ => Err(Error::Protocol("invalid MNNVL registration location")),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Capability {
    pub domain: SourceEpoch,
    pub kind: MnnvlHandleType,
    pub egm: bool,
}
impl Capability {
    pub const SIZE: usize = 24;
    pub fn encode(self, b: &mut [u8]) {
        b[..16].copy_from_slice(&self.domain.0);
        b[16] = u8::from(self.kind == MnnvlHandleType::PosixFd);
        b[17] = u8::from(self.egm);
        b[18..Self::SIZE].fill(0);
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() < Self::SIZE
            || b[..16] == [0; 16]
            || b[16] > 1
            || b[17] > 1
            || b[18..Self::SIZE] != [0; 6]
        {
            return Err(Error::Protocol("invalid MNNVL capability"));
        }
        let mut domain = [0; 16];
        domain.copy_from_slice(&b[..16]);
        Ok(Self {
            domain: SourceEpoch(domain),
            kind: if b[16] == 0 {
                MnnvlHandleType::Fabric
            } else {
                MnnvlHandleType::PosixFd
            },
            egm: b[17] == 1,
        })
    }
    pub fn reaches(self, peer: Self, same_host: bool, location: Location) -> bool {
        self.kind == peer.kind
            && self.domain == peer.domain
            && (self.kind == MnnvlHandleType::Fabric || same_host)
            && (location == Location::Device || (self.egm && peer.egm))
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/mnnvl.rs"]
mod tests;
