use crate::{Error, Result, SourceEpoch};

/// Host boot identity and process nonce, shared with the CUDA IPC handshake.
/// These are placement hints, not authentication or a remote memory grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessIdentity {
    pub(crate) machine: SourceEpoch,
    pub(crate) process: SourceEpoch,
}

impl ProcessIdentity {
    #[cfg(feature = "cuda")]
    pub(crate) fn local() -> Result<Self> {
        use std::sync::OnceLock;
        static IDENTITY: OnceLock<(u32, ProcessIdentity)> = OnceLock::new();
        if let Some((pid, identity)) = IDENTITY.get() {
            if *pid != std::process::id() {
                return Err(Error::Unavailable("CUDA IPC after fork"));
            }
            return Ok(*identity);
        }
        let machine = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
            .trim()
            .replace('-', "")
            .parse()?;
        let candidate = Self {
            machine,
            process: SourceEpoch::generate()?,
        };
        let (_, identity) = IDENTITY.get_or_init(|| (std::process::id(), candidate));
        Ok(*identity)
    }

    pub(crate) fn encode(&self, bytes: &mut [u8]) {
        bytes[..16].copy_from_slice(&self.machine.0);
        bytes[16..32].copy_from_slice(&self.process.0);
    }

    pub(crate) fn decode(bytes: &[u8]) -> Self {
        let mut machine = [0; 16];
        let mut process = [0; 16];
        machine.copy_from_slice(&bytes[..16]);
        process.copy_from_slice(&bytes[16..32]);
        Self {
            machine: SourceEpoch(machine),
            process: SourceEpoch(process),
        }
    }

    #[cfg(feature = "cuda")]
    pub(crate) fn validate_peer(&self, peer: Self) -> Result<()> {
        if self.machine != peer.machine {
            return Err(Error::Unavailable(
                "CUDA IPC requires the same physical host",
            ));
        }
        if self.process == peer.process {
            return Err(Error::Unavailable("CUDA IPC requires a different process"));
        }
        Ok(())
    }
}

/// Describe capabilities are fixed at engine creation, independent of current
/// provider health. Execution still checks authoritative owners and readiness.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SegmentMetadata {
    pub(crate) rdma: bool,
    pub(crate) remote_staging: bool,
    pub(crate) nvlink: Option<ProcessIdentity>,
}

impl SegmentMetadata {
    pub(crate) const WIRE_SIZE: usize = 36;

    pub(crate) fn encode(&self, bytes: &mut [u8]) {
        bytes.fill(0);
        bytes[0] = u8::from(self.rdma)
            | (u8::from(self.remote_staging) << 1)
            | (u8::from(self.nvlink.is_some()) << 2);
        if let Some(identity) = self.nvlink {
            identity.encode(&mut bytes[4..]);
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::WIRE_SIZE
            || bytes[0] & !7 != 0
            || bytes[1..4].iter().any(|b| *b != 0)
            || (bytes[0] & 4 == 0 && bytes[4..].iter().any(|b| *b != 0))
        {
            return Err(Error::Protocol("invalid segment capabilities"));
        }
        Ok(Self {
            rdma: bytes[0] & 1 != 0,
            remote_staging: bytes[0] & 2 != 0,
            nvlink: (bytes[0] & 4 != 0).then(|| ProcessIdentity::decode(&bytes[4..])),
        })
    }
}
