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
        if self.priority != Priority::High && transport != Transport::Rdma {
            return false;
        }
        if self.remote_staging
            && (!cfg!(feature = "cuda")
                || !self.remote_segment.remote_staging
                || self.remote.memory_type != MemoryType::Cuda
                || !matches!(transport, Transport::Tcp | Transport::Rdma))
        {
            return false;
        }
        match transport {
            Transport::Unspec => false,
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
        let candidates = if self.policies.is_empty() {
            &DEFAULT_TRANSPORTS[..]
        } else {
            let policy = self
                .policies
                .iter()
                .find(|policy| {
                    policy.segment_type == SegmentType::Memory
                        && policy
                            .local_memory
                            .is_none_or(|kind| kind == context.local_memory)
                        && policy
                            .remote_memory
                            .is_none_or(|kind| kind == context.remote.memory_type)
                        && policy
                            .min_size
                            .is_none_or(|size| context.transfer_size >= size)
                        && policy
                            .max_size
                            .is_none_or(|size| context.transfer_size <= size)
                        && policy
                            .priority
                            .is_none_or(|priority| priority == context.priority)
                })
                .ok_or(Error::Unavailable("no matching transport selection policy"))?;
            if policy.transports.is_empty() {
                &DEFAULT_TRANSPORTS[..]
            } else {
                &policy.transports
            }
        };
        Self::select_candidate(context, candidates, hint)
    }

    pub(crate) fn select_default(context: &SelectionContext<'_>) -> Result<Transport> {
        Self::select_candidate(context, &DEFAULT_TRANSPORTS, Transport::Unspec)
    }

    fn select_candidate(
        context: &SelectionContext<'_>,
        candidates: &[Transport],
        hint: Transport,
    ) -> Result<Transport> {
        if hint != Transport::Unspec {
            return if candidates.contains(&hint) && context.available(hint) {
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
            .find(|candidate| context.available(*candidate))
            .ok_or(Error::Unavailable(
                "matching policy has no available transport",
            ))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/transport_selector.rs"]
mod tests;
