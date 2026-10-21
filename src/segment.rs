use std::cmp::Reverse;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::str::FromStr;

use crate::{Error, Permission, Result};

/// Random source-instance identity, not an authentication credential.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SourceEpoch(pub(crate) [u8; 16]);

impl SourceEpoch {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0; 16];
        File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        Ok(Self(bytes))
    }
}

impl fmt::Display for SourceEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for SourceEpoch {
    type Err = Error;

    fn from_str(input: &str) -> Result<Self> {
        if input.len() != 32 || !input.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::Protocol("epoch must contain 32 hexadecimal digits"));
        }
        let mut bytes = [0; 16];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&input[index * 2..index * 2 + 2], 16)
                .map_err(|_| Error::Protocol("epoch must contain 32 hexadecimal digits"))?;
        }
        Ok(Self(bytes))
    }
}

/// A source-owned registration ID and bounds, fenced to one source instance.
#[derive(Clone, Copy, Debug)]
pub struct RemoteRegion {
    pub(crate) epoch: SourceEpoch,
    pub(crate) id: u64,
    pub(crate) address: u64,
    pub(crate) length: usize,
    pub(crate) permission: Permission,
}

impl RemoteRegion {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    pub fn permission(&self) -> Permission {
        self.permission
    }
    pub fn base_address(&self) -> u64 {
        self.address
    }

    pub(crate) fn offset_of(&self, address: u64, length: usize) -> Option<usize> {
        let offset = usize::try_from(address.checked_sub(self.address)?).ok()?;
        let end = offset.checked_add(length)?;
        (length != 0 && end <= self.length).then_some(offset)
    }
}

/// A descriptor snapshot. Refresh explicitly with the same authoritative epoch.
#[derive(Clone, Debug)]
pub struct RemoteSegment {
    pub(crate) endpoint: SocketAddr,
    pub(crate) epoch: SourceEpoch,
    pub(crate) regions: Vec<RemoteRegion>,
}

impl RemoteSegment {
    pub fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
    pub fn source_epoch(&self) -> SourceEpoch {
        self.epoch
    }
    pub fn regions(&self) -> &[RemoteRegion] {
        &self.regions
    }

    /// Resolves a source-granted absolute address within this epoch's descriptor
    /// snapshot. Matches TENT's first containing buffer in address/length order.
    /// This lookup does not acquire a source grant or refresh authority.
    pub fn resolve_address(&self, address: u64, length: usize) -> Result<(RemoteRegion, usize)> {
        if length == 0 || address.checked_add(length as u64).is_none() {
            return Err(Error::InvalidRange);
        }
        self.regions
            .iter()
            .filter_map(|region| {
                region
                    .offset_of(address, length)
                    .map(|offset| (*region, offset))
            })
            .min_by_key(|(region, _)| (region.base_address(), Reverse(region.len())))
            .ok_or(Error::RegionNotFound)
    }
}

#[cfg(test)]
#[path = "../tests/unit/segment.rs"]
mod tests;
