use std::collections::HashSet;

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::runtime::config::Object;
use crate::{Error, MemoryType, Priority, Result, Transport};

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SegmentType {
    #[default]
    Memory,
    /// Local file policy. Requires a Linux file provider.
    File,
}

/// Business intent attached to a transfer, independent of its QoS contract.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IntentType {
    #[default]
    Unspec,
    ForegroundGet,
    BackgroundPrefetch,
    Migration,
    Checkpoint,
    WeightLoading,
    StagingInternal,
}

fn intent<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<IntentType>, D::Error> {
    use IntentType::*;
    let names = [
        ("unspec", Unspec),
        ("foreground_get", ForegroundGet),
        ("background_prefetch", BackgroundPrefetch),
        ("migration", Migration),
        ("checkpoint", Checkpoint),
        ("weight_loading", WeightLoading),
        ("staging_internal", StagingInternal),
    ];
    let value = Value::deserialize(d)?;
    let intent = match &value {
        Value::String(s) if s.eq_ignore_ascii_case("intent_unspec") => Some(Unspec),
        Value::String(s) => names
            .iter()
            .find(|(name, _)| s.eq_ignore_ascii_case(name))
            .map(|(_, i)| *i),
        Value::Number(n) => n
            .as_u64()
            .and_then(|n| names.get(usize::try_from(n).ok()?))
            .map(|(_, i)| *i),
        _ => None,
    };
    intent
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("invalid intent_type"))
}

/// Ordered TENT memory-selection rule. None matches any value; sizes are inclusive.
/// Unknown or unconsumed JSON fields fail configuration instead of broadening it.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionPolicy {
    #[serde(default = "default_name")]
    pub name: String,
    pub segment_type: SegmentType,
    #[serde(default, deserialize_with = "memory")]
    pub local_memory: Option<MemoryType>,
    #[serde(default, deserialize_with = "memory")]
    pub remote_memory: Option<MemoryType>,
    #[serde(default, deserialize_with = "present")]
    pub min_size: Option<usize>,
    #[serde(default, deserialize_with = "present")]
    pub max_size: Option<usize>,
    #[serde(default, deserialize_with = "priority")]
    pub priority: Option<Priority>,
    #[serde(default, deserialize_with = "intent")]
    pub intent_type: Option<IntentType>,
    #[serde(default, deserialize_with = "present")]
    pub same_machine: Option<bool>,
    #[serde(default)]
    pub devices: Vec<String>,
    #[serde(default, deserialize_with = "pool_name")]
    pub qp_pool: Option<String>,
    #[serde(default, deserialize_with = "sl")]
    pub service_level: Option<u8>,
    #[serde(default, deserialize_with = "tc")]
    pub traffic_class: Option<u8>,
    /// Candidate order. Empty uses the registered routes for this segment type.
    #[serde(default, deserialize_with = "transports")]
    pub transports: Vec<Transport>,
}

impl Default for SelectionPolicy {
    fn default() -> Self {
        Self {
            name: default_name(),
            segment_type: SegmentType::Memory,
            local_memory: None,
            remote_memory: None,
            min_size: None,
            max_size: None,
            priority: None,
            intent_type: None,
            same_machine: None,
            devices: Vec::new(),
            qp_pool: None,
            service_level: None,
            traffic_class: None,
            transports: Vec::new(),
        }
    }
}

/// Opt-in selector for all requests, including explicit transport hints.
/// With no rules, use the default eligible candidate order.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(from = "Root")]
pub struct TransportSelectorConfig {
    pub force_tcp: bool,
    pub use_legacy_transport_selection: bool,
    pub qp_mul_factor: Option<usize>,
    pub qp_pools: Vec<super::QpPool>,
    pub policy: Vec<SelectionPolicy>,
}

#[derive(Deserialize, Default)]
struct Endpoint {
    qp_mul_factor: Option<usize>,
    #[serde(default)]
    qp_pools: Vec<super::QpPool>,
}
#[derive(Deserialize, Default)]
struct Rdma {
    #[serde(default)]
    endpoint: Endpoint,
}
#[derive(Deserialize, Default)]
struct Transports {
    #[serde(default)]
    force_tcp: bool,
    #[serde(default)]
    rdma: Rdma,
}
#[derive(Deserialize)]
struct Root {
    #[serde(default)]
    policy: Vec<Object<SelectionPolicy>>,
    #[serde(default)]
    transports: Transports,
    #[serde(default)]
    use_legacy_transport_selection: bool,
}

impl From<Root> for TransportSelectorConfig {
    fn from(root: Root) -> Self {
        let endpoint = root.transports.rdma.endpoint;
        Self {
            force_tcp: root.transports.force_tcp,
            use_legacy_transport_selection: root.use_legacy_transport_selection,
            qp_mul_factor: endpoint.qp_mul_factor,
            qp_pools: endpoint.qp_pools,
            policy: root.policy.into_iter().map(|Object(p)| p).collect(),
        }
    }
}

impl TransportSelectorConfig {
    /// Read the selector and RDMA endpoint sections of a JSON object of at most 1 MiB.
    pub fn from_json(json: &str) -> Result<Self> {
        if json.len() > 1 << 20 {
            return Err(Error::InvalidSelectorConfig("JSON exceeds 1 MiB".into()));
        }
        let Object(config): Object<Self> = serde_json::from_str(json).map_err(|error| {
            // Only re-read invalid input to identify the policy that failed to parse.
            if let Ok(root) = serde_json::from_str::<Value>(json) {
                if let Some(policies) = root.get("policy").and_then(Value::as_array) {
                    for policy in policies {
                        if serde_json::from_value::<Object<SelectionPolicy>>(policy.clone())
                            .is_err()
                        {
                            let name = policy
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("memory_default");
                            return Error::InvalidSelectorConfig(format!("policy {name}: {error}"));
                        }
                    }
                }
            }
            Error::InvalidSelectorConfig(error.to_string())
        })?;
        config.validate()?;
        Ok(config)
    }

    pub(super) fn validate(&self) -> Result<()> {
        if self.policy.len() > 256 {
            return Err(Error::InvalidConfig("at most 256 transport policies"));
        }
        let mut names = HashSet::new();
        for policy in &self.policy {
            if policy.name.is_empty() || policy.name.len() > 256 || !names.insert(&policy.name) {
                return Err(invalid(policy, "name must be unique and fit 256 bytes"));
            }
            if policy.segment_type == SegmentType::File
                && !cfg!(any(feature = "io-uring", feature = "gds"))
            {
                return Err(invalid(policy, "file selection requires io-uring or gds"));
            }
            if policy
                .min_size
                .zip(policy.max_size)
                .is_some_and(|(min, max)| min > max)
            {
                return Err(invalid(policy, "min_size exceeds max_size"));
            }
            if policy.transports.len() > 6
                || policy.transports.iter().enumerate().any(|(i, transport)| {
                    *transport == Transport::Unspec
                        || matches!(transport, Transport::IoUring | Transport::Gds)
                            != (policy.segment_type == SegmentType::File)
                        || policy.transports[..i].contains(transport)
                })
            {
                return Err(invalid(
                    policy,
                    "expected unique concrete transports for this segment type",
                ));
            }
        }
        Ok(())
    }
}

fn default_name() -> String {
    "memory_default".into()
}

fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

fn memory<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<MemoryType>, D::Error> {
    match String::deserialize(deserializer)?.as_str() {
        "*" => Ok(None),
        "cpu" => Ok(Some(MemoryType::Cpu)),
        "cuda" => Ok(Some(MemoryType::Cuda)),
        _ => Err(serde::de::Error::custom(
            "memory pattern must be cpu, cuda or *",
        )),
    }
}

fn priority<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Priority>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let level = match &value {
        Value::Number(number) => number.as_u64(),
        Value::String(name) => match name.to_ascii_lowercase().as_str() {
            "high" | "0" => Some(0),
            "medium" | "1" => Some(1),
            "low" | "2" => Some(2),
            _ => None,
        },
        _ => None,
    };
    match level {
        Some(0) => Ok(Some(Priority::High)),
        Some(1) => Ok(Some(Priority::Medium)),
        Some(2) => Ok(Some(Priority::Low)),
        _ => Err(serde::de::Error::custom(
            "priority must be high/medium/low or 0..=2",
        )),
    }
}

fn transports<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Transport>, D::Error> {
    Vec::<String>::deserialize(deserializer)?
        .into_iter()
        .map(|name| match name.as_str() {
            "tcp" => Ok(Transport::Tcp),
            "hp_tcp" => Ok(Transport::HpTcp),
            "shm" => Ok(Transport::Shm),
            "rdma" => Ok(Transport::Rdma),
            "nvlink" => Ok(Transport::Nvlink),
            "mnnvl" => Ok(Transport::Mnnvl),
            "io_uring" => Ok(Transport::IoUring),
            "gds" => Ok(Transport::Gds),
            _ => Err(serde::de::Error::custom(format!(
                "unsupported transport {name:?}"
            ))),
        })
        .collect()
}

fn invalid(policy: &SelectionPolicy, reason: &str) -> Error {
    Error::InvalidSelectorConfig(format!("policy {}: {reason}", policy.name))
}

fn pool_name<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<String>, D::Error> {
    Ok(Value::deserialize(d)?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned))
}

fn sl<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<u8>, D::Error> {
    Ok(tc(d)?.filter(|n| *n <= 15))
}

fn tc<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<u8>, D::Error> {
    Ok(u8::try_from(i64::deserialize(d)?).ok())
}
