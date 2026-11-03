//! TENT first-match policy selection, resolved once before submission admission.
mod config;

use crate::segment::SegmentMetadata;
use crate::{Error, MemoryType, Priority, RemoteRegion, Result, Transport};

pub use config::{SegmentType, SelectionPolicy, TransportSelectorConfig};

const DEFAULT_TRANSPORTS: [Transport; 3] = [Transport::Nvlink, Transport::Rdma, Transport::Tcp];

pub(crate) struct SelectionContext<'a> {
    pub(crate) local_memory: MemoryType,
    pub(crate) local_ipc: bool,
    pub(crate) remote: &'a RemoteRegion,
    pub(crate) local_segment: SegmentMetadata,
    pub(crate) remote_segment: SegmentMetadata,
    pub(crate) transfer_size: usize,
    pub(crate) priority: Priority,
    pub(crate) remote_staging: bool,
}

impl SelectionContext<'_> {
    fn available(&self, transport: Transport) -> bool {
        if self.remote_staging
            && (!cfg!(feature = "cuda")
                || !self.remote_segment.remote_staging
                || self.remote.memory_type != MemoryType::Cuda
                || !matches!(transport, Transport::Tcp | Transport::Rdma))
        {
            return false;
        }
        match transport {
            Transport::Unspec | Transport::IoUring => false,
            Transport::Tcp => true,
            Transport::Rdma => self.local_segment.rdma && self.remote_segment.rdma,
            Transport::Nvlink => {
                self.local_ipc
                    && self.remote.ipc_exportable
                    && self
                        .local_segment
                        .nvlink
                        .zip(self.remote_segment.nvlink)
                        .is_some_and(|(local, remote)| {
                            local.machine == remote.machine && local.process != remote.process
                        })
            }
        }
    }
}

pub(crate) struct TransportSelector {
    policies: Box<[SelectionPolicy]>,
}

impl TransportSelector {
    pub(crate) fn new(config: &TransportSelectorConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            policies: config.policy.clone().into_boxed_slice(),
        })
    }

    pub(crate) fn select(
        &self,
        context: &SelectionContext<'_>,
        hint: Transport,
    ) -> Result<Transport> {
        let candidates = self.candidates(
            SegmentType::Memory,
            context.local_memory,
            context.remote.memory_type,
            context.transfer_size,
            context.priority,
        )?;
        Self::select_candidate(candidates, hint, |t| context.available(t))
    }

    fn candidates(
        &self,
        segment: SegmentType,
        local: MemoryType,
        remote: MemoryType,
        size: usize,
        priority: Priority,
    ) -> Result<&[Transport]> {
        let defaults: &[Transport] = match segment {
            SegmentType::Memory => &DEFAULT_TRANSPORTS,
            SegmentType::File => &[Transport::IoUring],
        };
        if self.policies.is_empty() {
            return Ok(defaults);
        }
        let policy = self
            .policies
            .iter()
            .find(|policy| {
                policy.segment_type == segment
                    && policy.local_memory.is_none_or(|kind| kind == local)
                    && policy.remote_memory.is_none_or(|kind| kind == remote)
                    && policy.min_size.is_none_or(|min| size >= min)
                    && policy.max_size.is_none_or(|max| size <= max)
                    && policy.priority.is_none_or(|level| level == priority)
            })
            .ok_or(Error::Unavailable("no matching transport selection policy"))?;
        Ok(if policy.transports.is_empty() {
            defaults
        } else {
            &policy.transports
        })
    }

    #[cfg(feature = "io-uring")]
    pub(crate) fn select_file(
        selector: Option<&Self>,
        local: MemoryType,
        size: usize,
        priority: Priority,
        hint: Transport,
    ) -> Result<Transport> {
        let candidates = match selector {
            Some(selector) => {
                selector.candidates(SegmentType::File, local, MemoryType::Cpu, size, priority)?
            }
            None => &[Transport::IoUring],
        };
        Self::select_candidate(candidates, hint, |t| {
            t == Transport::IoUring && (local == MemoryType::Cpu || cfg!(feature = "cuda"))
        })
    }

    pub(crate) fn select_default(context: &SelectionContext<'_>) -> Result<Transport> {
        Self::select_candidate(&DEFAULT_TRANSPORTS, Transport::Unspec, |t| {
            context.available(t)
        })
    }

    fn select_candidate(
        candidates: &[Transport],
        hint: Transport,
        available: impl Fn(Transport) -> bool,
    ) -> Result<Transport> {
        if hint != Transport::Unspec {
            return if candidates.contains(&hint) && available(hint) {
                Ok(hint)
            } else {
                Err(Error::Unavailable(
                    "transport hint is excluded or unavailable",
                ))
            };
        }
        candidates
            .iter()
            .copied()
            .find(|candidate| available(*candidate))
            .ok_or(Error::Unavailable(
                "matching policy has no available transport",
            ))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/transport_selector.rs"]
mod tests;
