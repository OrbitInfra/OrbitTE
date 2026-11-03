mod admission;
mod config;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::runtime::admission_queue::LocalTransferAdmissionQueue;
use crate::{Error, Priority, Result, SourceEpoch};

use admission::QosQuota;
pub(crate) use admission::{QosCharge, QosPermit, QosReservation};

const MAX_CONTRACTS: usize = 4096;

/// Supported TENT action allowlist. Empty or absent actions preserve rejection;
/// allowing delay requires EngineConfig::runtime_queue. If both are allowed,
/// temporary quota pressure delays; impossible groups still fail admission.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QosDegradedActions {
    pub delay: bool,
    pub reject: bool,
}

/// None inherits the less specific layer. An effective None is unlimited;
/// Some(0) is a hard zero budget. Counts cover admitted public data requests
/// until their actual owner drains.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QosPolicyFields {
    pub priority: Option<Priority>,
    pub max_inflight_bytes: Option<usize>,
    pub max_inflight_requests: Option<usize>,
    pub allowed_degraded_actions: Option<QosDegradedActions>,
}

impl QosPolicyFields {
    fn overlay(self, specific: Self) -> Self {
        Self {
            priority: specific.priority.or(self.priority),
            max_inflight_bytes: specific.max_inflight_bytes.or(self.max_inflight_bytes),
            max_inflight_requests: specific
                .max_inflight_requests
                .or(self.max_inflight_requests),
            allowed_degraded_actions: specific
                .allowed_degraded_actions
                .or(self.allowed_degraded_actions),
        }
    }

    fn bounded(self) -> bool {
        self.max_inflight_bytes.is_some() || self.max_inflight_requests.is_some()
    }
}

#[derive(Clone, Debug, Default)]
pub struct QosTenantContract {
    pub name: String,
    pub defaults: QosPolicyFields,
    pub intents: BTreeMap<String, QosPolicyFields>,
}

/// Typed TENT-style global -> tenant -> intent resolution. EngineConfig::qos
/// opts in; budgets aggregate by the resolved contract, not by a parent tree.
#[derive(Clone, Debug)]
pub struct QosConfig {
    pub version: u32,
    pub defaults: QosPolicyFields,
    pub tenants: Vec<QosTenantContract>,
}

impl Default for QosConfig {
    fn default() -> Self {
        Self {
            version: 1,
            defaults: QosPolicyFields::default(),
            tenants: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct QosRequestContext {
    pub tenant_id: String,
    pub intent: String,
    /// Used only when no contract layer supplies a priority.
    pub requested_priority: Priority,
}

impl Default for QosRequestContext {
    fn default() -> Self {
        Self {
            tenant_id: "default".into(),
            intent: "unspec".into(),
            requested_priority: Priority::High,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QosUsage {
    pub owners: usize,
    pub inflight_bytes: usize,
    pub inflight_requests: usize,
}

#[derive(Debug)]
pub(crate) struct CompiledContract {
    name: String,
    pub(crate) fields: QosPolicyFields,
    slot: usize,
}

#[derive(Debug)]
struct Resolution {
    engine: SourceEpoch,
    tenant_id: String,
    intent: String,
    requested_priority: Priority,
    matched: bool,
    contract: Option<Arc<CompiledContract>>,
    quota: Option<Arc<QosQuota>>,
}

/// Immutable engine-bound resolution. Clone it into requests to reuse the
/// compiled policy and budget slot without resolving names on each submission.
#[derive(Clone, Debug)]
pub struct EffectiveQosPolicy(Arc<Resolution>);

impl EffectiveQosPolicy {
    pub fn enabled(&self) -> bool {
        self.0.contract.is_some()
    }
    pub fn matched(&self) -> bool {
        self.0.matched
    }
    pub fn tenant_id(&self) -> &str {
        &self.0.tenant_id
    }
    pub fn intent(&self) -> &str {
        &self.0.intent
    }
    pub fn requested_priority(&self) -> Priority {
        self.0.requested_priority
    }
    pub fn effective_priority(&self) -> Priority {
        self.fields().priority.unwrap_or(self.0.requested_priority)
    }
    pub fn matched_contract(&self) -> &str {
        self.0
            .contract
            .as_ref()
            .map_or("compatibility_default", |contract| &contract.name)
    }
    pub fn fields(&self) -> QosPolicyFields {
        self.0
            .contract
            .as_ref()
            .map_or(QosPolicyFields::default(), |contract| contract.fields)
    }
    /// Unbounded policies are not charged and return None.
    pub fn usage(&self) -> Option<QosUsage> {
        let contract = self.0.contract.as_ref()?;
        if !contract.fields.bounded() {
            return None;
        }
        let quota = self.0.quota.as_ref()?;
        Some(quota.usage(contract.slot))
    }

    pub(crate) fn contract(&self, engine: SourceEpoch) -> Result<Option<&CompiledContract>> {
        if self.0.engine != engine {
            return Err(Error::ForeignQosPolicy);
        }
        Ok(self.0.contract.as_deref())
    }
}

struct TenantContract {
    defaults: Arc<CompiledContract>,
    intents: HashMap<String, Arc<CompiledContract>>,
}

/// Owns the immutable resolution table and a single atomic admission ledger.
pub(crate) struct QosContractResolver {
    global: Arc<CompiledContract>,
    tenants: HashMap<String, TenantContract>,
    default: Arc<CompiledContract>,
    quota: Option<Arc<QosQuota>>,
}

fn normalize(value: &str, fallback: &str) -> Result<String> {
    if value.len() > 256 {
        return Err(Error::InvalidConfig("QoS identifiers must fit 256 bytes"));
    }
    let value = value.trim_matches(|c: char| c.is_ascii_whitespace());
    let value = if value.is_empty() { fallback } else { value };
    if value.is_empty() {
        return Err(Error::InvalidConfig("empty QoS contract name"));
    }
    Ok(value.to_ascii_lowercase())
}

impl QosContractResolver {
    pub(crate) fn new(
        config: &QosConfig,
        queue: Option<&Arc<LocalTransferAdmissionQueue>>,
    ) -> Result<Self> {
        if config.version != 1 {
            return Err(Error::InvalidConfig("unsupported QoS version"));
        }
        let mut limits = Vec::new();
        let mut compile =
            |name: String, fields: QosPolicyFields| -> Result<Arc<CompiledContract>> {
                if limits.len() == MAX_CONTRACTS {
                    return Err(Error::InvalidConfig("at most 4096 QoS contracts"));
                }
                let slot = limits.len();
                limits.push(fields);
                Ok(Arc::new(CompiledContract { name, fields, slot }))
            };
        let global = compile("global_default".into(), config.defaults)?;
        let mut tenants = HashMap::new();
        for tenant in &config.tenants {
            let name = normalize(&tenant.name, "")?;
            if tenants.contains_key(&name) {
                return Err(Error::InvalidConfig("duplicate QoS tenant"));
            }
            let fields = config.defaults.overlay(tenant.defaults);
            let defaults = compile(format!("{name}.default"), fields)?;
            let mut intents = HashMap::new();
            for (intent, policy) in &tenant.intents {
                let intent = normalize(intent, "")?;
                if intents.contains_key(&intent) {
                    return Err(Error::InvalidConfig("duplicate QoS intent"));
                }
                let contract = compile(format!("{name}.{intent}"), fields.overlay(*policy))?;
                intents.insert(intent, contract);
            }
            tenants.insert(name, TenantContract { defaults, intents });
        }
        let delay = limits.iter().any(|policy| {
            policy
                .allowed_degraded_actions
                .is_some_and(|actions| actions.delay)
        });
        if delay && queue.is_none() {
            return Err(Error::InvalidConfig("QoS delay requires runtime_queue"));
        }
        let quota = limits
            .iter()
            .any(|policy| policy.bounded())
            .then(|| Arc::new(QosQuota::new(limits, queue.filter(|_| delay))));
        let mut resolver = Self {
            default: Arc::clone(&global),
            global,
            tenants,
            quota,
        };
        resolver.default = Arc::clone(resolver.lookup("default", "unspec").0);
        Ok(resolver)
    }

    fn lookup(&self, tenant: &str, intent: &str) -> (&Arc<CompiledContract>, bool) {
        match self.tenants.get(tenant) {
            None => (&self.global, false),
            Some(tenant) => match tenant.intents.get(intent) {
                Some(contract) => (contract, true),
                None => (&tenant.defaults, false),
            },
        }
    }

    pub(crate) fn default_contract(&self) -> &CompiledContract {
        &self.default
    }

    pub(crate) fn resolve(
        resolver: Option<&Self>,
        engine: SourceEpoch,
        context: &QosRequestContext,
    ) -> Result<EffectiveQosPolicy> {
        let tenant_id = normalize(&context.tenant_id, "default")?;
        let intent = normalize(&context.intent, "unspec")?;
        let (contract, matched) = match resolver {
            Some(resolver) => {
                let (contract, matched) = resolver.lookup(&tenant_id, &intent);
                (Some(Arc::clone(contract)), matched)
            }
            None => (None, false),
        };
        Ok(EffectiveQosPolicy(Arc::new(Resolution {
            engine,
            tenant_id,
            intent,
            requested_priority: context.requested_priority,
            contract,
            matched,
            quota: resolver.and_then(|resolver| resolver.quota.clone()),
        })))
    }

    pub(crate) fn admit(&self, charges: Vec<QosCharge>) -> Result<Option<QosPermit>> {
        if charges.is_empty() {
            return Ok(None);
        }
        self.quota
            .as_ref()
            .ok_or(Error::Internal("QoS quota missing"))?
            .admit(charges)
            .map(Some)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime/qos_contract.rs"]
mod tests;
