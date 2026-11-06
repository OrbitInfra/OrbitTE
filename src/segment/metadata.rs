use crate::{Error, Result, SourceEpoch};

/// Host boot identity and process nonce, shared with the CUDA IPC handshake.
/// These are placement hints, not authentication or a remote memory grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessIdentity {
    pub(crate) machine: SourceEpoch,
    pub(crate) process: SourceEpoch,
}

impl ProcessIdentity {
    pub(crate) fn boot_id() -> Result<SourceEpoch> {
        // If unavailable, the engine cannot establish same-machine placement.
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
            .trim()
            .replace('-', "")
            .parse()
    }

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
        let machine = Self::boot_id()?;
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
#[derive(Clone, Debug, Default)]
pub(crate) struct SegmentMetadata {
    pub(crate) tcp_disabled: bool,
    pub(crate) mnnvl: Option<crate::transport::mnnvl::Capability>,
    pub(crate) shm: Option<std::sync::Arc<crate::transport::shm::wire::Manifest>>,
    pub(crate) hp: Option<std::sync::Arc<crate::transport::hp_tcp::Endpoint>>,
    pub(crate) max_transfer_bytes: u64,
    pub(crate) boot: Option<SourceEpoch>,
    pub(crate) rdma: bool,
    pub(crate) rdma_direct_gpu: bool,
    pub(crate) remote_staging: bool,
    pub(crate) nvlink: Option<ProcessIdentity>,
}

impl SegmentMetadata {
    pub(crate) const WIRE_SIZE: usize = 44;

    pub(crate) fn same_machine(&self, peer: &Self) -> bool {
        let machine = |segment: &Self| segment.nvlink.map(|id| id.machine).or(segment.boot);
        machine(self)
            .zip(machine(peer))
            .is_some_and(|(a, b)| a == b)
    }

    pub(crate) fn encode(&self, bytes: &mut [u8]) {
        bytes.fill(0);
        bytes[36..44].copy_from_slice(&self.max_transfer_bytes.to_be_bytes());
        bytes[0] = (u8::from(self.rdma_direct_gpu) << 7)
            | (u8::from(self.mnnvl.is_some()) << 6)
            | (u8::from(self.shm.is_some()) << 5)
            | (u8::from(self.tcp_disabled) << 4)
            | u8::from(self.rdma)
            | (u8::from(self.remote_staging) << 1)
            | (u8::from(self.nvlink.is_some()) << 2);
        if let Some(identity) = self.nvlink {
            identity.encode(&mut bytes[4..36]);
        } else if let Some(boot) = self.boot {
            bytes[0] |= 8;
            bytes[4..20].copy_from_slice(&boot.0);
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::WIRE_SIZE
            || (bytes[0] & 128 != 0 && bytes[0] & 1 == 0)
            || (bytes[0] & 64 != 0 && bytes[0] & 12 != 8)
            || bytes[0] & 12 == 12
            || bytes[1..4].iter().any(|b| *b != 0)
            || (bytes[0] & 12 == 0 && bytes[4..20].iter().any(|b| *b != 0))
            || (bytes[0] & 4 == 0 && bytes[20..36].iter().any(|b| *b != 0))
            || (bytes[0] & 12 != 0 && bytes[4..20] == [0; 16])
            || (bytes[0] & 4 != 0 && bytes[20..36] == [0; 16])
        {
            return Err(Error::Protocol("invalid segment capabilities"));
        }
        let identity = ProcessIdentity::decode(&bytes[4..36]);
        let mut cap = [0; 8];
        cap.copy_from_slice(&bytes[36..44]);
        Ok(Self {
            max_transfer_bytes: u64::from_be_bytes(cap),
            tcp_disabled: bytes[0] & 16 != 0,
            hp: None,
            mnnvl: None,
            shm: (bytes[0] & 32 != 0).then(Default::default),
            boot: (bytes[0] & 12 != 0).then_some(identity.machine),
            rdma: bytes[0] & 1 != 0,
            rdma_direct_gpu: bytes[0] & 128 != 0,
            remote_staging: bytes[0] & 2 != 0,
            nvlink: (bytes[0] & 4 != 0).then_some(identity),
        })
    }
}
