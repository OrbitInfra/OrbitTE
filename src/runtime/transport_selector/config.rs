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
    /// Reserved TENT segment type; rejected until file execution is implemented.
    File,
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
    /// Candidate order. Empty uses eligible NVLink -> RDMA -> TCP.
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
            transports: Vec::new(),
        }
    }
}

/// Opt-in selector for all requests, including explicit transport hints.
/// With no rules, use the default eligible candidate order.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct TransportSelectorConfig {
    #[serde(default, deserialize_with = "policies")]
    pub policy: Vec<SelectionPolicy>,
}

impl TransportSelectorConfig {
    /// Read TENT's `policy` array from a JSON object of at most 1 MiB. Other
    /// engine sections are left to their own loaders; policy fields are strict.
    pub fn from_json(json: &str) -> Result<Self> {
        if json.len() > 1 << 20 {
            return Err(Error::InvalidSelectorConfig("JSON exceeds 1 MiB".into()));
        }
        let Object(config): Object<Self> = serde_json::from_str(json)
            .map_err(|error| Error::InvalidSelectorConfig(error.to_string()))?;
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
                return Err(Error::InvalidConfig(
                    "transport policy names must be unique and fit 256 bytes",
                ));
            }
            if policy.segment_type != SegmentType::Memory {
                return Err(Error::InvalidConfig(
                    "file segment transport selection is not implemented",
                ));
            }
            if policy
                .min_size
                .zip(policy.max_size)
                .is_some_and(|(min, max)| min > max)
            {
                return Err(Error::InvalidConfig(
                    "transport policy min_size exceeds max_size",
                ));
            }
            if policy.transports.len() > 3
                || policy.transports.iter().enumerate().any(|(i, transport)| {
                    *transport == Transport::Unspec || policy.transports[..i].contains(transport)
                })
            {
                return Err(Error::InvalidConfig(
                    "transport candidates must be unique concrete transports",
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
            "rdma" => Ok(Transport::Rdma),
            "nvlink" => Ok(Transport::Nvlink),
            _ => Err(serde::de::Error::custom(
                "transport must be tcp, rdma or nvlink",
            )),
        })
        .collect()
}

fn policies<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<SelectionPolicy>, D::Error> {
    Vec::<Object<SelectionPolicy>>::deserialize(deserializer)
        .map(|policies| policies.into_iter().map(|Object(policy)| policy).collect())
}
