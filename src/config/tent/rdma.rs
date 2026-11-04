use super::{EngineConfig, Result, document, invalid, typed};
use crate::transport::rdma::RdmaConfig;
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

pub(super) struct Settings {
    config: RdmaConfig,
    enabled: bool,
    custom_file: Option<PathBuf>,
    rail_file: Option<PathBuf>,
}

impl Settings {
    pub(super) fn new(root: &Value) -> Result<Self> {
        let mut lanes = None;
        for key in [
            "num_lanes",
            "device/num_cq_list",
            "endpoint/qp_mul_factor",
            "workers/num_workers",
        ] {
            let path = format!("transports/rdma/{key}");
            if let Some(v) = document::get(root, &path) {
                let n = v
                    .as_i64()
                    .ok_or_else(|| invalid(&path, "expected an integer lane count"))?;
                if n == -1 {
                    continue;
                }
                if !(1..=64).contains(&n) || lanes.is_some_and(|old| old != n) {
                    return Err(invalid(
                        &path,
                        "CQ, QP and worker lane counts must agree and be in 1..=64",
                    ));
                }
                lanes = Some(n);
            }
        }
        let config = RdmaConfig {
            num_lanes: lanes.unwrap_or(6) as usize,
            port: Some(1),
            hop_limit: 16,
            max_cqes: 4096,
            // Clamp absent upstream SQ and endpoint defaults to the owner's hard limits.
            max_send_wr: 64,
            max_endpoints: 4096,
            ..Default::default()
        };
        Ok(Self {
            config,
            enabled: true,
            custom_file: None,
            rail_file: None,
        })
    }
    pub(super) fn set(&mut self, p: &str, v: Value) -> Result<()> {
        let r = &mut self.config;
        match p.strip_prefix("transports/rdma/").unwrap_or(p) {
            "enable" => self.enabled = typed(p, v)?,
            "num_lanes"
            | "device/num_cq_list"
            | "endpoint/qp_mul_factor"
            | "workers/num_workers"
            | "endpoint/qp_pools" => (),
            "device/gid_index" => {
                let n: i64 = typed(p, v)?;
                r.gid_index = if n == -1 {
                    None
                } else {
                    Some(
                        u8::try_from(n)
                            .map_err(|_| invalid(p, "expected -1 or an unsigned GID index"))?,
                    )
                };
            }
            "device/port" => r.port = Some(typed(p, v)?),
            "device/max_cqe" => r.max_cqes = typed(p, v)?,
            "endpoint/endpoint_store_cap" => r.max_endpoints = typed(p, v)?,
            "endpoint/hop_limit" => r.hop_limit = typed(p, v)?,
            "endpoint/max_dest_rd_atomic" => r.max_dest_rd_atomic = typed(p, v)?,
            "endpoint/max_rd_atomic" => r.max_rd_atomic = typed(p, v)?,
            "endpoint/max_qp_wr" => r.max_send_wr = typed(p, v)?,
            "endpoint/signal_interval" => r.signal_interval = typed(p, v)?,
            "endpoint/service_level" => r.service_level = typed(p, v)?,
            "endpoint/traffic_class" => r.traffic_class = typed(p, v)?,
            "workers/block_size" => r.block_size = typed(p, v)?,
            "workers/rail_topo_path" => self.rail_file = Some(typed(p, v)?),
            "disable_gpu_direct_rdma" => r.disable_gpu_direct_rdma = typed(p, v)?,
            "log_slice_affinity" => r.log_slice_affinity = typed(p, v)?,
            "strict_local_numa" => r.strict_local_numa = typed(p, v)?,
            "deadline_bw_arbitration" => r.deadline_bw_arbitration = typed(p, v)?,
            "priority_promotion_per_entry" => r.priority_promotion_per_entry = typed(p, v)?,
            "priority_promotion_timeout_us" => {
                r.priority_promotion_timeout = Duration::from_micros(typed(p, v)?)
            }
            "slot_rotation_interval_ms" => {
                r.slot_rotation_interval = Duration::from_millis(typed(p, v)?)
            }
            "shared_quota_shm_path" => r.shared_quota_shm_path = Some(typed(p, v)?),
            "rail_error_threshold" => r.rail_error_threshold = typed(p, v)?,
            "rail_cooldown_secs" => r.rail_cooldown = Duration::from_secs(typed(p, v)?),
            "rail_error_window_secs" => r.rail_error_window = Duration::from_secs(typed(p, v)?),
            "rail_probe_interval_secs" => r.rail_probe_interval = Duration::from_secs(typed(p, v)?),
            "enable_smart_scheduling" => r.scheduling.smart_selection = typed(p, v)?,
            "bandwidth_learning_rate" => r.scheduling.bandwidth_learning_rate = typed(p, v)?,
            "transmit_bandwidth_learning_rate" => {
                r.scheduling.transmit_bandwidth_learning_rate = typed(p, v)?
            }
            "default_bandwidth_gbps" => r.scheduling.default_bandwidth_gbps = typed(p, v)?,
            "max_bandwidth_gbps" => r.scheduling.max_bandwidth_gbps = typed(p, v)?,
            "min_bandwidth_gbps" => r.scheduling.min_bandwidth_gbps = typed(p, v)?,
            "score_epsilon" => r.scheduling.score_epsilon = typed(p, v)?,
            "ewma_min_bandwidth_multiplier" => r.scheduling.ewma_min_multiplier = typed(p, v)?,
            "ewma_max_bandwidth_multiplier" => r.scheduling.ewma_max_multiplier = typed(p, v)?,
            "transmit_meter_interval_ns" => {
                r.scheduling.transmit_meter_interval = Duration::from_nanos(typed(p, v)?)
            }
            "transmit_meter_max_interval_ns" => {
                r.scheduling.transmit_meter_max_interval = Duration::from_nanos(typed(p, v)?)
            }
            "numa_penalties" if v.as_array().is_some_and(Vec::is_empty) => (),
            "numa_penalties" => r.scheduling.numa_tier_weights = typed(p, v)?,
            "topology/custom_json_path" => self.custom_file = Some(typed(p, v)?),
            "topology/priority_matrix" => {
                if !crate::Topology::check_matrix_shape(&v) {
                    return Err(invalid(p, "expected a topology matrix object"));
                }
                r.priority_matrix = Some(v.to_string());
            }
            "topology/rdma_whitelist" => r.rdma_whitelist = typed(p, v)?,
            "topology/rdma_blacklist" => r.rdma_blacklist = typed(p, v)?,
            _ => return Err(invalid(p, "unsupported RDMA configuration field")),
        }
        Ok(())
    }
    pub(super) fn finish(mut self, c: &mut EngineConfig) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let read = |file: Option<&Path>, key| {
            file.filter(|p| !p.as_os_str().is_empty())
                .map(|p| document::read(p, key))
                .transpose()
        };
        if self.config.priority_matrix.is_none() {
            self.config.custom_topology_json =
                read(self.custom_file.as_deref(), "topology/custom_json_path")?;
        }
        self.config.rail_topology = read(
            self.rail_file.as_deref(),
            "transports/rdma/workers/rail_topo_path",
        )?;
        c.rdma = Some(self.config);
        Ok(())
    }
}
