//! TENT first-match policy selection, resolved once before submission admission.
mod config;
pub(crate) mod qp_pools;

use qp_pools::{QpLayout, QpRange};
use std::io::Write;
use std::sync::Arc;

use crate::segment::SegmentMetadata;
use crate::{Error, MemoryType, Priority, RemoteRegion, Result, Transport};

pub use config::{IntentType, SegmentType, SelectionPolicy, TransportSelectorConfig};
pub use qp_pools::QpPool;

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
            Transport::Unspec | Transport::IoUring | Transport::Gds => false,
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

/// A nonempty policy name of at most 256 UTF-8 bytes. Clones share its storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyName(Arc<str>);

impl PolicyName {
    pub fn new(name: &str) -> Result<Self> {
        if name.is_empty() || name.len() > 256 {
            return Err(Error::InvalidConfig(
                "policy name must contain 1..=256 bytes",
            ));
        }
        Ok(Self(Arc::from(name)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectionResult {
    pub(crate) transport: Transport,
    pub(crate) device_mask: u64,
    pub(crate) qp_pool: Option<QpRange>,
    // Policy SL/TC are diagnostic only. Native QPs take SL/TC from their pool.
    pub(crate) service_level: Option<u8>,
    pub(crate) traffic_class: Option<u8>,
    policy_index: Option<u8>,
    policy: Option<PolicyName>,
    named: bool,
    intent: IntentType,
}

impl SelectionResult {
    pub(crate) fn plain(transport: Transport) -> Self {
        Self {
            transport,
            device_mask: u64::MAX,
            qp_pool: None,
            service_level: None,
            traffic_class: None,
            policy_index: None,
            policy: None,
            named: false,
            intent: IntentType::Unspec,
        }
    }

    pub(crate) fn rejected(&self, reason: &'static str) -> Error {
        match &self.policy {
            Some(name) => Error::TransportPolicy {
                policy: name.clone(),
                reason,
            },
            None => Error::Unavailable(reason),
        }
    }
}

struct CompiledRule {
    rule: SelectionPolicy,
    result: SelectionResult,
}

pub(crate) struct TransportSelector {
    policies: Vec<CompiledRule>,
    force_tcp: bool,
    legacy: bool,
    pub(crate) unresolved_devices: usize,
}

pub(crate) struct Filters {
    pub(crate) segment: SegmentType,
    pub(crate) local: MemoryType,
    pub(crate) remote: MemoryType,
    pub(crate) size: usize,
    pub(crate) priority: Priority,
    pub(crate) same_machine: bool,
}

impl SelectionContext<'_> {
    pub(crate) fn filters(&self) -> Filters {
        Filters {
            segment: SegmentType::Memory,
            local: self.local_memory,
            remote: self.remote.memory_type,
            size: self.transfer_size,
            priority: self.priority,
            same_machine: self.local_segment.same_machine(self.remote_segment),
        }
    }

    pub(crate) fn select_bound(
        &self,
        selector: Option<&TransportSelector>,
        request: &crate::TransferRequest,
        bound: Option<u8>,
    ) -> Result<SelectionResult> {
        TransportSelector::select(selector, self.filters(), request, bound, |t| {
            self.available(t)
        })
    }
}

impl TransportSelector {
    pub(crate) fn compile(
        config: &TransportSelectorConfig,
        devices: Option<&[String]>,
        layout: &QpLayout,
    ) -> Result<Self> {
        config.validate()?;
        let mut policies = Vec::with_capacity(config.policy.len());
        let mut unresolved_devices = 0;
        for (index, rule) in config.policy.iter().enumerate() {
            let mut result = SelectionResult::plain(Transport::Unspec);
            result.policy = Some(PolicyName::new(&rule.name)?);
            result.policy_index = Some(index as u8);
            if let Some(names) = devices.filter(|_| !rule.devices.is_empty()) {
                let mut mask = 0;
                let mut missing = Vec::new();
                for name in &rule.devices {
                    match names.iter().position(|nic| nic == name) {
                        Some(i) if i < 64 => mask |= 1u64 << i,
                        _ => missing.push(name),
                    }
                }
                unresolved_devices += missing.len();
                if mask == 0 {
                    let _ = writeln!(
                        std::io::stderr().lock(),
                        "error: policy {}: no device resolved ({missing:?}), allowing all devices",
                        rule.name
                    );
                } else {
                    result.device_mask = mask;
                    if !missing.is_empty() {
                        let _ = writeln!(
                            std::io::stderr().lock(),
                            "warning: policy {}: unresolved devices {missing:?}, using the resolved subset",
                            rule.name
                        );
                    }
                }
            }
            result.qp_pool = Some(layout.range(rule.qp_pool.as_deref()));
            result.service_level = rule.service_level.filter(|&sl| sl <= 15);
            result.traffic_class = rule.traffic_class;
            if rule.service_level.is_some() || rule.traffic_class.is_some() {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "warning: policy {}: policy SL/TC have no effect, configure them on the endpoint QP pool",
                    rule.name
                );
            }
            policies.push(CompiledRule {
                rule: rule.clone(),
                result,
            });
        }
        Ok(Self {
            policies,
            force_tcp: config.force_tcp,
            legacy: config.use_legacy_transport_selection,
            unresolved_devices,
        })
    }

    // Bind the whole group before a file lease can open its descriptor. Unnamed
    // groups allocate nothing here; the sparse entries contain only indices.
    pub(crate) fn bindings(
        selector: Option<&Self>,
        segment: SegmentType,
        requests: &[crate::TransferRequest],
    ) -> Result<Vec<(usize, u8)>> {
        let mut bindings = Vec::new();
        for (i, request) in requests.iter().enumerate() {
            if let Some(name) = &request.policy_name {
                if let Some(index) = Self::bind(selector, segment, name)? {
                    bindings
                        .try_reserve(1)
                        .map_err(|_| Error::ResourceExhausted)?;
                    bindings.push((i, index));
                }
            }
        }
        Ok(bindings)
    }

    fn bind(
        selector: Option<&Self>,
        segment: SegmentType,
        name: &PolicyName,
    ) -> Result<Option<u8>> {
        let rules = selector.map_or(&[][..], |s| s.policies.as_slice());
        if rules.is_empty() {
            let default_name = match segment {
                SegmentType::Memory => "memory_default",
                SegmentType::File => "file_storage",
            };
            if name.as_str() == default_name {
                return Ok(None);
            }
        }
        let rule = rules
            .iter()
            .find(|p| p.rule.name == name.as_str())
            .ok_or_else(|| Error::TransportPolicy {
                policy: name.clone(),
                reason: "unknown policy",
            })?;
        if rule.rule.segment_type != segment {
            return Err(rule.result.rejected("policy targets another segment type"));
        }
        Ok(rule.result.policy_index)
    }

    pub(crate) fn select(
        selector: Option<&Self>,
        ctx: Filters,
        request: &crate::TransferRequest,
        bound: Option<u8>,
        available: impl Fn(Transport) -> bool,
    ) -> Result<SelectionResult> {
        let defaults: &[Transport] = match ctx.segment {
            SegmentType::Memory => &DEFAULT_TRANSPORTS,
            SegmentType::File => &[Transport::Gds, Transport::IoUring],
        };
        let rules = selector.map_or(&[][..], |s| s.policies.as_slice());
        let named = request.policy_name.as_ref();
        let mut result = SelectionResult::plain(Transport::Unspec);
        // Explicit binding always names one compiled rule. Legacy routing bypasses
        // automatic matches; force_tcp overrides only memory routing.
        let legacy = selector
            .is_some_and(|s| s.legacy && !(s.force_tcp && ctx.segment == SegmentType::Memory));
        let candidates = if rules.is_empty() || (legacy && named.is_none()) {
            result.policy.clone_from(&request.policy_name);
            defaults
        } else {
            let rule = if let Some(index) = bound {
                rules
                    .get(usize::from(index))
                    .ok_or(Error::Internal("invalid compiled policy index"))?
            } else {
                rules
                    .iter()
                    .find(|p| {
                        let rule = &p.rule;
                        rule.segment_type == ctx.segment
                            && rule.local_memory.is_none_or(|kind| kind == ctx.local)
                            && rule.remote_memory.is_none_or(|kind| kind == ctx.remote)
                            && rule.min_size.is_none_or(|min| ctx.size >= min)
                            && rule.max_size.is_none_or(|max| ctx.size <= max)
                            && rule.priority.is_none_or(|level| level == ctx.priority)
                            && rule
                                .intent_type
                                .is_none_or(|intent| intent == request.intent)
                            && rule
                                .same_machine
                                .is_none_or(|same| same == ctx.same_machine)
                    })
                    .ok_or(Error::Unavailable("no matching transport selection policy"))?
            };
            result = rule.result.clone();
            if rule.rule.transports.is_empty() {
                defaults
            } else {
                &rule.rule.transports
            }
        };
        let candidates =
            if ctx.segment == SegmentType::Memory && selector.is_some_and(|s| s.force_tcp) {
                &[Transport::Tcp][..]
            } else {
                candidates
            };
        let reject = |reason, memory, file| {
            if result.policy.is_some() {
                result.rejected(reason)
            } else {
                Error::Unavailable(match ctx.segment {
                    SegmentType::Memory => memory,
                    SegmentType::File => file,
                })
            }
        };
        let hint = request.transport;
        result.transport = if hint == Transport::Unspec {
            candidates
                .iter()
                .copied()
                .find(|t| available(*t))
                .ok_or_else(|| {
                    reject(
                        "no eligible transport",
                        "memory_default: no eligible transport",
                        "file_storage: no eligible transport",
                    )
                })?
        } else if !candidates.contains(&hint) {
            return Err(reject(
                "transport hint is excluded by the policy",
                "memory_default: transport hint is excluded",
                "file_storage: transport hint is excluded",
            ));
        } else if !available(hint) {
            return Err(reject(
                "hinted transport is unavailable on these owners",
                "memory_default: hinted transport is unavailable",
                "file_storage: hinted transport is unavailable",
            ));
        } else {
            hint
        };
        result.named = named.is_some();
        result.intent = request.intent;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/transport_selector.rs"]
mod tests;
