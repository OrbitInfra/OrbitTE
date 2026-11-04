//! TENT bootstrap compatibility. All I/O happens before any engine owner starts.
mod document;
mod environment;
mod providers;
#[cfg(feature = "rdma")]
mod rdma;

use crate::{EngineConfig, Error, Result, RuntimeQueueConfig};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{ffi::OsString, net::ToSocketAddrs, path::Path, time::Duration};

/// P2P constructor identity captured by the compatibility loader. The Rust
/// control service continues to fence registrations by endpoint and epoch.
#[derive(Clone)]
pub struct TentSettings {
    pub local_segment_name: String,
    /// P2P retains this constructor value but does not contact a metadata store.
    pub metadata_servers: String,
    pub verbose: bool,
}
impl std::fmt::Debug for TentSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TentSettings")
            .field("verbose", &self.verbose)
            .finish_non_exhaustive()
    }
}

impl TentSettings {
    pub(crate) fn log_startup(&self, config: &EngineConfig) {
        if self.verbose {
            // No metadata credentials, peer names or registration addresses.
            log::info!(
                "TENT P2P engine started with {} RPC workers, TCP={}, HP-TCP={}, SHM={}",
                config.handler_threads,
                config.tcp,
                config.hp_tcp.enable,
                config.shm.enable
            );
        }
    }
}

fn invalid(path: &str, reason: &'static str) -> Error {
    Error::TentConfig {
        path: path.to_owned(),
        reason,
    }
}

fn typed<T: DeserializeOwned>(path: &str, v: Value) -> Result<T> {
    serde_json::from_value(v)
        .map_err(|_| invalid(path, "unexpected type or value outside the supported range"))
}

impl EngineConfig {
    /// Resolve MC_TENT_CONF and legacy environment aliases into a reusable configuration.
    pub fn from_tent_env() -> Result<Self> {
        load(json!({}), |name| std::env::var_os(name))
    }

    /// Resolve an explicit constructor document. MC_TENT_CONF can replace its
    /// root. Explicit constructor identity, bind, whitelist and force-TCP
    /// settings follow TENT's constructor precedence.
    pub fn from_tent_json(json: &str) -> Result<Self> {
        load(document::parse(json, "constructor")?, |name| {
            std::env::var_os(name)
        })
    }

    /// Construct from the frozen C wrapper's thread-local JSON text and textual
    /// overrides. A malformed JSON string takes the historical warning/default
    /// path. A valid document still has to satisfy the normal S17 schema.
    pub fn from_tent_settings<'a>(
        text: &str,
        fields: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self> {
        if text.len() > document::MAX_BYTES {
            return Err(invalid("constructor", "JSON exceeds 1 MiB"));
        }
        let mut root = if text.is_empty() {
            json!({})
        } else {
            match document::parse(text, "constructor") {
                Ok(value) => value,
                Err(_) if serde_json::from_str::<serde::de::IgnoredAny>(text).is_err() => {
                    log::warn!("invalid TENT C constructor JSON, using defaults");
                    json!({})
                }
                Err(e) => return Err(e),
            }
        };
        let mut bytes = text.len();
        for (path, value) in fields {
            document::check_key(path)?;
            bytes = bytes.saturating_add(path.len()).saturating_add(value.len());
            if bytes > document::MAX_BYTES {
                return Err(invalid("constructor", "settings exceed 1 MiB"));
            }
            document::set(&mut root, path, environment::scalar(value))?;
        }
        load(root, |key| std::env::var_os(key))
    }

    /// As from_tent_json, reading a bounded UTF-8 regular file.
    pub fn from_tent_file(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_tent_json(&document::read(path.as_ref(), "constructor")?)
    }
}

fn load(base: Value, env: impl Fn(&str) -> Option<OsString>) -> Result<EngineConfig> {
    let (root, mnnvl) = environment::resolve(base, env)?;
    materialize(root, mnnvl)
}

// Walk only configuration namespaces. Policy, QoS, pools and custom topology
// own their object/array schemas, including dynamic tenant and device names.
fn leaves(
    root: Value,
    prefix: &str,
    visit: &mut impl FnMut(&str, Value) -> Result<()>,
) -> Result<()> {
    let Value::Object(fields) = root else {
        return Err(invalid(prefix, "expected an object"));
    };
    for (key, v) in fields {
        if key.contains('/') {
            return Err(invalid(
                prefix,
                "slash aliases must be full paths at the root",
            ));
        }
        document::check_key(&key)?;
        let path = if prefix.is_empty() {
            key
        } else {
            format!("{prefix}/{key}")
        };
        match path.as_str() {
            "metrics"
            | "notification"
            | "runtime_queue"
            | "staging"
            | "topology"
            | "transports"
            | "transports/rdma"
            | "transports/rdma/device"
            | "transports/rdma/endpoint"
            | "transports/rdma/workers"
            | "transports/gds"
            | "transports/shm"
            | "transports/tcp"
            | "transports/fabric"
            | "transports/io_uring"
            | "transports/mnnvl"
            | "transports/nvlink"
            | "transports/ascend_direct"
            | "transports/ub"
            | "transports/sunrise_link"
            | "transports/hylink"
            | "transports/tpu"
            | "transports/xpu"
            | "transports/mpcomm"
            | "transports/memfabric"
            | "transports/mlu" => leaves(v, &path, visit)?,
            _ => visit(&path, v)?,
        }
    }
    Ok(())
}

fn integer<T: TryFrom<i64>>(path: &str, value: Value) -> Result<T> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.trim_start().parse().ok())
        .and_then(|n| T::try_from(n).ok())
        .ok_or_else(|| invalid(path, "expected an integer or integer string in range"))
}

fn materialize(root: Value, mnnvl: bool) -> Result<EngineConfig> {
    let mut c = EngineConfig::defaults();
    let mut settings = TentSettings {
        local_segment_name: String::new(),
        metadata_servers: String::new(),
        verbose: false,
    };
    let mut queue = RuntimeQueueConfig::default();
    let mut queue_on = false;
    let mut host = String::new();
    let mut port = 0;
    let tcp_explicit = document::get(&root, "transports/tcp/enable") == Some(&Value::Bool(true));
    c.handler_threads = if tcp_explicit {
        std::thread::available_parallelism()
            .map_or(4, usize::from)
            .clamp(4, 8)
    } else {
        1
    };
    // Give each existing parser just its own inputs. It remains the authority
    // on field types, allowed actions, bounded pools and policy names.
    let mut selector = json!({});
    for key in [
        "policy",
        "use_legacy_transport_selection",
        "transports/force_tcp",
        "transports/rdma/endpoint/qp_pools",
        "transports/rdma/endpoint/qp_mul_factor",
    ] {
        if let Some(v) = document::get(&root, key) {
            if key == "transports/rdma/endpoint/qp_mul_factor" && v.as_i64() == Some(-1) {
                continue;
            }
            document::set(&mut selector, key, v.clone())?;
        }
    }
    if selector != json!({}) {
        c.selector = Some(crate::TransportSelectorConfig::from_json(
            &selector.to_string(),
        )?);
    }
    if let Some(v) = root.get("qos") {
        c.qos = crate::QosConfig::from_json(&json!({"qos":v}).to_string())?;
    }
    let mut providers = providers::Providers::new(&mut c, &root, mnnvl)?;
    #[cfg(feature = "rdma")]
    let mut rdma = rdma::Settings::new(&root)?;

    leaves(root, "", &mut |p, v| {
        match p {
            "enable_auto_failover_on_poll" => c.enable_auto_failover_on_poll = typed(p, v)?,
            "enable_progress_worker" => c.enable_progress_worker = typed(p, v)?,
            "enable_runtime_queue" => queue_on = typed(p, v)?,
            "merge_requests" => c.merge_requests = typed(p, v)?,
            "max_failover_attempts" => c.max_failover_attempts = typed(p, v)?,
            "metadata_type" => {
                if typed::<String>(p, v)? != "p2p" {
                    return Err(invalid(p, "only P2P metadata is supported"));
                }
            }
            "metadata_servers" => settings.metadata_servers = typed(p, v)?,
            "local_segment_name" => settings.local_segment_name = typed(p, v)?,
            "verbose" => settings.verbose = typed(p, v)?,
            "rpc_server_hostname" => host = typed(p, v)?,
            "rpc_server_port" => port = integer(p, v)?,
            "rpc_server_threads" => {
                c.handler_threads = integer(p, v)?;
                if !(1..=128).contains(&c.handler_threads) {
                    return Err(invalid(p, "worker count must be in 1..=128"));
                }
            }
            "notification/rpc_fallback" => c.notification_rpc_fallback = typed(p, v)?,
            "runtime_queue/progress_fallback_interval_us" => {
                c.progress_fallback_interval = Duration::from_micros(typed(p, v)?)
            }
            "runtime_queue/deadline_aware" => queue.deadline_aware = typed(p, v)?,
            "runtime_queue/staging_byte_reserve" => queue.staging_byte_reserve = typed(p, v)?,
            "runtime_queue/staging_owner_reserve" => queue.staging_owner_reserve = typed(p, v)?,
            "runtime_queue/max_outstanding_bytes" => queue.max_outstanding_bytes = typed(p, v)?,
            "runtime_queue/max_outstanding_owners" => queue.max_outstanding_owners = typed(p, v)?,
            "runtime_queue/max_dispatch_bytes" => queue.max_dispatch_bytes = typed(p, v)?,
            "runtime_queue/max_dispatch_owners" => queue.max_dispatch_owners = typed(p, v)?,
            "runtime_queue/promotion_slack_ns" => queue.promotion_slack_ns = typed(p, v)?,
            "runtime_queue/mlu_local_threshold" => queue.mlu_local_threshold = typed(p, v)?,
            "transports/tcp/enable" => c.tcp = typed(p, v)?,
            "transports/shm/enable" => c.shm.enable = typed(p, v)?,
            "transports/shm/cxl_mount_path" => c.shm.cxl_mount_path = typed(p, v)?,
            "transports/hp_tcp" => {
                if !v.is_object() {
                    return Err(invalid(p, "expected an object"));
                }
                c.hp_tcp = typed(p, v)?;
            }
            "policy" | "qos" | "use_legacy_transport_selection" | "transports/force_tcp" => (),
            #[cfg(feature = "rdma")]
            _ if p.starts_with("transports/rdma/") || p.starts_with("topology/") => {
                rdma.set(p, v)?
            }
            _ => providers.set(&mut c, p, v)?,
        }
        Ok(())
    })?;
    providers.finish(&mut c);
    #[cfg(feature = "rdma")]
    rdma.finish(&mut c)?;
    c.bind_addr = if host.is_empty() {
        (c.bind_addr.ip(), port).into()
    } else {
        (host.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.next())
            .ok_or_else(|| invalid("rpc_server_hostname", "cannot resolve bind hostname"))?
    };
    c.enable_progress_worker |= queue_on;
    c.runtime_queue = queue_on.then_some(queue);
    c.tent = Some(settings);
    c.validate()?;
    crate::TransferEngine::qp_layout(&c)?;
    Ok(c)
}

#[cfg(test)]
#[path = "../../../tests/unit/config/tent.rs"]
mod tests;
