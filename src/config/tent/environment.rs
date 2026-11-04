use super::document::{MAX_BYTES, get, parse, read, set};
use super::{Result, invalid};
use serde_json::Value;
use std::ffi::OsString;
use std::path::Path;

#[derive(Clone, Copy)]
enum Parser {
    Scalar,
    Bool,
    List,
    Metric,
}
use Parser::*;

// NCCL_IB_GID_INDEX deliberately follows MC_GID_INDEX.
const ALIASES: &[(&str, &str, Parser)] = &[
    ("MC_CUSTOM_TOPO_JSON", "topology/custom_json_path", Scalar),
    (
        "MC_DISABLE_GPU_DIRECT_RDMA",
        "transports/rdma/disable_gpu_direct_rdma",
        Bool,
    ),
    ("MC_GID_INDEX", "transports/rdma/device/gid_index", Scalar),
    (
        "MC_IB_PCI_RELAXED_ORDERING",
        "transports/rdma/pci_relaxed_ordering",
        Scalar,
    ),
    ("MC_IB_PORT", "transports/rdma/device/port", Scalar),
    ("MC_IB_SL", "transports/rdma/endpoint/service_level", Scalar),
    ("MC_IB_TC", "transports/rdma/endpoint/traffic_class", Scalar),
    (
        "MC_IOURING_PINNED_STAGING",
        "transports/io_uring/pinned_staging",
        Bool,
    ),
    (
        "MC_IOURING_STAGING_SLOTS",
        "transports/io_uring/staging_slots",
        Scalar,
    ),
    (
        "MC_IOURING_STAGING_SLOT_KB",
        "transports/io_uring/staging_slot_kb",
        Scalar,
    ),
    (
        "MC_LOG_RDMA_SLICE_AFFINITY",
        "transports/rdma/log_slice_affinity",
        Bool,
    ),
    (
        "MC_MAX_CQE_PER_CTX",
        "transports/rdma/device/max_cqe",
        Scalar,
    ),
    (
        "MC_MAX_EP_PER_CTX",
        "transports/rdma/endpoint/endpoint_store_cap",
        Scalar,
    ),
    (
        "MC_MAX_INLINE",
        "transports/rdma/endpoint/max_inline_bytes",
        Scalar,
    ),
    ("MC_MAX_SGE", "transports/rdma/endpoint/max_sge", Scalar),
    ("MC_MAX_WR", "transports/rdma/endpoint/max_qp_wr", Scalar),
    ("MC_MNNVL_EGM", "transports/mnnvl/egm", Bool),
    ("MC_MTU", "transports/rdma/endpoint/path_mtu", Scalar),
    (
        "MC_NUM_COMP_CHANNELS_PER_CTX",
        "transports/rdma/device/num_comp_channels",
        Scalar,
    ),
    (
        "MC_NUM_CQ_PER_CTX",
        "transports/rdma/device/num_cq_list",
        Scalar,
    ),
    (
        "MC_NUM_QP_PER_EP",
        "transports/rdma/endpoint/qp_mul_factor",
        Scalar,
    ),
    (
        "MC_PKEY_INDEX",
        "transports/rdma/endpoint/pkey_index",
        Scalar,
    ),
    (
        "MC_RDMA_BIND_ADDRESS",
        "transports/rdma/bind_address",
        Scalar,
    ),
    (
        "MC_RETRY_CNT",
        "transports/rdma/workers/max_retry_count",
        Scalar,
    ),
    (
        "MC_SLICE_SIZE",
        "transports/rdma/workers/block_size",
        Scalar,
    ),
    (
        "MC_STRICT_LOCAL_NUMA",
        "transports/rdma/strict_local_numa",
        Bool,
    ),
    (
        "MC_TENT_RDMA_SIGNAL_INTERVAL",
        "transports/rdma/endpoint/signal_interval",
        Scalar,
    ),
    ("MC_TENT_RPC_THREADS", "rpc_server_threads", Scalar),
    ("MC_TE_FILTERS", "topology/rdma_whitelist", List),
    ("MC_TE_FILTERS_EXCLUDE", "topology/rdma_blacklist", List),
    (
        "MC_WORKERS_PER_CTX",
        "transports/rdma/workers/num_workers",
        Scalar,
    ),
    ("MOONCAKE_LOCAL_HOSTNAME", "rpc_server_hostname", Scalar),
    (
        "NCCL_IB_GID_INDEX",
        "transports/rdma/device/gid_index",
        Scalar,
    ),
    ("TENT_METRICS_ENABLED", "metrics/enabled", Metric),
    ("TENT_METRICS_HTTP_HOST", "metrics/http_host", Metric),
    ("TENT_METRICS_HTTP_PORT", "metrics/http_port", Metric),
    (
        "TENT_METRICS_HTTP_SERVER_THREADS",
        "metrics/http_server_threads",
        Metric,
    ),
    (
        "TENT_METRICS_REPORT_INTERVAL",
        "metrics/report_interval_seconds",
        Metric,
    ),
];

const PRESERVED: &[&str] = &[
    "metadata_type",
    "metadata_servers",
    "local_segment_name",
    "rpc_server_hostname",
    "rpc_server_port",
    "topology/rdma_whitelist",
    "transports/ascend_direct/agent_mode",
    "transports/ascend_direct/store_te_init",
    "transports/ascend_direct/fabric_mem",
    "transports/memfabric/store_te_init",
];

fn scalar(s: &str) -> Value {
    if s.eq_ignore_ascii_case("true") {
        true.into()
    } else if s.eq_ignore_ascii_case("false") {
        false.into()
    } else if let Ok(n) = s.trim_start().parse::<i64>() {
        n.into()
    } else {
        s.into()
    }
}
fn force(root: &mut Value) -> Result<()> {
    for (key, value) in [
        ("force_tcp", true),
        ("tcp/enable", true),
        ("rdma/enable", false),
        ("hp_tcp/enable", false),
        ("fabric/enable", false),
    ] {
        set(root, &format!("transports/{key}"), value.into())?;
    }
    Ok(())
}

pub(super) fn resolve(
    base: Value,
    lookup: impl Fn(&str) -> Option<OsString>,
) -> Result<(Value, bool)> {
    let source = lookup("MC_TENT_CONF").filter(|s| !s.is_empty());
    let replacement = if let Some(s) = source {
        if s.len() > MAX_BYTES {
            return Err(invalid("MC_TENT_CONF", "input exceeds 1 MiB"));
        }
        let path = Path::new(&s);
        if path.exists() {
            Some(parse(&read(path, "MC_TENT_CONF")?, "MC_TENT_CONF")?)
        } else {
            Some(parse(
                s.to_str().ok_or_else(|| {
                    invalid("MC_TENT_CONF", "expected UTF-8 JSON or an existing file")
                })?,
                "MC_TENT_CONF",
            )?)
        }
    } else {
        None
    };
    let preserved_force = get(&base, "transports/force_tcp") == Some(&Value::Bool(true));
    let preserved: Vec<_> = PRESERVED
        .iter()
        .filter_map(|&key| get(&base, key).map(|value| (key, value.clone())))
        .collect();
    let mut root = replacement.unwrap_or(base);
    for &(name, key, parser) in ALIASES {
        if matches!(
            name,
            "MC_IOURING_STAGING_SLOTS" | "MC_IOURING_STAGING_SLOT_KB"
        ) && get(&root, "transports/io_uring/pinned_staging") != Some(&Value::Bool(true))
        {
            continue;
        }
        if matches!(parser, Metric) && get(&root, key).is_some() {
            continue;
        }
        let Some(raw) = lookup(name) else {
            continue;
        };
        let s = raw
            .to_str()
            .filter(|s| s.len() <= MAX_BYTES)
            .ok_or_else(|| invalid(name, "expected bounded UTF-8"))?;
        let value = match parser {
            Scalar => scalar(s),
            Bool => {
                let Some(b) = crate::config::legacy_bool(s) else {
                    log::warn!("invalid boolean in {name}, keeping configured value");
                    continue;
                };
                b.into()
            }
            List => {
                let names: Vec<_> = s
                    .split(',')
                    .map(|v| v.trim_matches([' ', '\t']))
                    .filter(|v| !v.is_empty())
                    .collect();
                if names.is_empty() {
                    continue;
                }
                names.into()
            }
            Metric => match key {
                "metrics/http_host" => s.into(),
                "metrics/enabled" => crate::config::legacy_bool(s)
                    .ok_or_else(|| invalid(key, "expected a boolean"))?
                    .into(),
                _ => s
                    .parse::<u64>()
                    .map_err(|_| invalid(key, "expected an unsigned integer"))?
                    .into(),
            },
        };
        set(&mut root, key, value)?;
    }
    for (key, val) in preserved {
        set(&mut root, key, val)?;
    }
    if lookup("MC_FORCE_TCP").is_some() || preserved_force {
        force(&mut root)?;
    }
    // OrbitTE has no shared ylt RPC I/O pool. Never read provider credentials
    // or SDK environment variables for absent Redis, libfabric or vendor owners.
    for name in ["MC_RPC_CLIENT_IO_THREADS", "MC_TE_RPC_CLIENT_IO_THREADS"] {
        if lookup(name).is_some() {
            return Err(invalid(name, "shared RPC client pool is unsupported"));
        }
    }
    Ok((root, lookup("MC_ENABLE_MNNVL").is_some()))
}
