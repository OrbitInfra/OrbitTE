//! Cold-path loading of the supported TENT `qos` configuration section.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::marker::PhantomData;
use std::path::Path;

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::{
    MAX_CONTRACTS, QosConfig, QosDegradedActions, QosPolicyFields, QosTenantContract, normalize,
};
use crate::{Error, Priority, Result};

const MAX_JSON_BYTES: usize = 1 << 20;

// Serde structs also accept positional sequences; TENT policy nodes must be objects.
#[derive(Default)]
struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
            type Value = Object<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a QoS configuration object")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                T::deserialize(de::value::MapAccessDeserializer::new(map)).map(Object)
            }
        }
        deserializer.deserialize_map(ObjectVisitor(PhantomData))
    }
}

// Other engine settings are outside this loader's responsibility.
#[derive(Deserialize)]
struct EngineDocument {
    #[serde(default)]
    qos: Option<Object<QosDocument>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QosDocument {
    #[serde(default = "version_one")]
    version: u32,
    #[serde(default, deserialize_with = "present")]
    defaults: Option<Object<Policy>>,
    #[serde(default, deserialize_with = "present")]
    tenants: Option<Vec<Object<Tenant>>>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Policy {
    #[serde(deserialize_with = "priority")]
    priority: Option<Priority>,
    #[serde(deserialize_with = "bytes")]
    max_inflight_bytes: Option<usize>,
    #[serde(deserialize_with = "requests")]
    max_inflight_requests: Option<usize>,
    #[serde(deserialize_with = "actions")]
    allowed_degraded_actions: Option<QosDegradedActions>,
}

impl From<Policy> for QosPolicyFields {
    fn from(policy: Policy) -> Self {
        Self {
            priority: policy.priority,
            max_inflight_bytes: policy.max_inflight_bytes,
            max_inflight_requests: policy.max_inflight_requests,
            allowed_degraded_actions: policy.allowed_degraded_actions,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Tenant {
    name: String,
    #[serde(default)]
    defaults: Object<Policy>,
    #[serde(default, deserialize_with = "intents")]
    intents: BTreeMap<String, QosPolicyFields>,
}

impl QosConfig {
    /// Load only the `qos` section of a TENT-format engine JSON document.
    /// Missing/null QoS, an empty object, or version alone returns None.
    /// Other engine settings are ignored; unknown/unimplemented QoS fields fail.
    /// At most 1 MiB is accepted. No engine state or files are changed.
    pub fn from_json(json: &str) -> Result<Option<Self>> {
        if json.len() > MAX_JSON_BYTES {
            return Err(Error::InvalidQosConfig("JSON exceeds 1 MiB".into()));
        }
        let Object(document): Object<EngineDocument> = serde_json::from_str(json)
            .map_err(|error| Error::InvalidQosConfig(error.to_string()))?;
        let Some(Object(qos)) = document.qos else {
            return Ok(None);
        };
        if qos.version != 1 {
            return Err(Error::InvalidQosConfig("unsupported qos.version".into()));
        }
        if qos.defaults.is_none() && qos.tenants.is_none() {
            return Ok(None);
        }
        let mut config = Self {
            version: qos.version,
            defaults: qos.defaults.unwrap_or_default().0.into(),
            tenants: Vec::new(),
        };
        let mut names = BTreeSet::new();
        let mut contracts = 1usize;
        for Object(tenant) in qos.tenants.unwrap_or_default() {
            let name = normalize(&tenant.name, "")?;
            if !names.insert(name.clone()) {
                return Err(Error::InvalidQosConfig(format!(
                    "duplicate QoS tenant: {name}"
                )));
            }
            contracts = contracts
                .checked_add(1 + tenant.intents.len())
                .ok_or(Error::ResourceExhausted)?;
            if contracts > MAX_CONTRACTS {
                return Err(Error::InvalidConfig("at most 4096 QoS contracts"));
            }
            config.tenants.push(QosTenantContract {
                name,
                defaults: tenant.defaults.0.into(),
                intents: tenant.intents,
            });
        }
        Ok(Some(config))
    }

    /// Read at most 1 MiB plus an overflow sentinel, then load the `qos` section.
    /// Relative paths use the caller's working directory; no environment is read.
    pub fn from_json_file(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let mut json = String::new();
        File::open(path)?
            .take((MAX_JSON_BYTES + 1) as u64)
            .read_to_string(&mut json)?;
        Self::from_json(&json)
    }
}

fn version_one() -> u32 {
    1
}

// Missing fields inherit, but an explicit JSON null is an invalid policy value.
fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

fn priority<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Priority>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let priority = match value {
        Value::Number(number) => match number.as_u64() {
            Some(0) => Some(Priority::High),
            Some(1) => Some(Priority::Medium),
            Some(2) => Some(Priority::Low),
            _ => None,
        },
        Value::String(text) => match text.trim_ascii().to_ascii_lowercase().as_str() {
            "high" | "0" => Some(Priority::High),
            "medium" | "1" => Some(Priority::Medium),
            "low" | "2" => Some(Priority::Low),
            _ => None,
        },
        _ => None,
    };
    priority
        .map(Some)
        .ok_or_else(|| de::Error::custom("priority must be high/medium/low or the integer 0/1/2"))
}

fn bytes<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let count = match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => {
            let text = text.trim_ascii();
            let end = text.bytes().take_while(u8::is_ascii_digit).count();
            let number = text[..end].parse::<u64>().ok();
            let multiplier = match text[end..].trim_ascii().to_ascii_lowercase().as_str() {
                "" | "b" => Some(1),
                "kb" | "kib" => Some(1 << 10),
                "mb" | "mib" => Some(1 << 20),
                "gb" | "gib" => Some(1 << 30),
                "tb" | "tib" => Some(1u64 << 40),
                _ => None,
            };
            number.zip(multiplier).and_then(|(n, m)| n.checked_mul(m))
        }
        _ => None,
    };
    count
        .and_then(|n| usize::try_from(n).ok())
        .map(Some)
        .ok_or_else(|| {
            de::Error::custom(
                "max_inflight_bytes must fit usize: nonnegative integer or integer B/KiB/MiB/GiB/TiB (KB/MB/GB/TB also use 1024)",
            )
        })
}

fn requests<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error> {
    let count = u64::deserialize(deserializer)?;
    usize::try_from(count).map(Some).map_err(|_| {
        de::Error::custom("max_inflight_requests must be a nonnegative integer fitting usize")
    })
}

fn actions<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<QosDegradedActions>, D::Error> {
    let values = Vec::<String>::deserialize(deserializer)?;
    let mut actions = QosDegradedActions::default();
    for value in values {
        match value.trim_ascii().to_ascii_lowercase().as_str() {
            "delay" => actions.delay = true,
            "reject" => actions.reject = true,
            _ => {
                return Err(de::Error::custom(format!(
                    "unsupported allowed_degraded_actions entry: {value}; only delay/reject are implemented"
                )));
            }
        }
    }
    Ok(Some(actions))
}

fn intents<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, QosPolicyFields>, D::Error> {
    struct Intents;
    impl<'de> Visitor<'de> for Intents {
        type Value = BTreeMap<String, QosPolicyFields>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an object mapping unique normalized intents to QoS policy fields")
        }
        fn visit_map<M: MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut intents = BTreeMap::new();
            while let Some((name, Object(policy))) = map.next_entry::<String, Object<Policy>>()? {
                let name = normalize(&name, "").map_err(de::Error::custom)?;
                if intents.insert(name, policy.into()).is_some() {
                    return Err(de::Error::custom("duplicate normalized QoS intent"));
                }
            }
            Ok(intents)
        }
    }
    deserializer.deserialize_map(Intents)
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/qos_contract/config.rs"]
mod tests;
