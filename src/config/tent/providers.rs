#[cfg(any(
    feature = "io-uring",
    feature = "gds",
    feature = "cuda",
    feature = "fabric"
))]
use super::typed;
use super::{EngineConfig, Result, invalid};
use serde_json::Value;

fn known_enable(path: &str) -> bool {
    matches!(
        path,
        "transports/fabric/enable"
            | "transports/rdma/enable"
            | "transports/nvlink/enable"
            | "transports/mnnvl/enable"
            | "transports/gds/enable"
            | "transports/io_uring/enable"
    )
}

pub(super) struct Providers {
    #[cfg(feature = "fabric")]
    libfabric: crate::FabricConfig,
    #[cfg(feature = "fabric")]
    libfabric_enabled: bool,
    #[cfg(feature = "cuda")]
    fabric: crate::MnnvlConfig,
    #[cfg(feature = "cuda")]
    fabric_branch: bool,
    #[cfg(feature = "cuda")]
    fabric_enabled: bool,
}

impl Providers {
    pub(super) fn new(c: &mut EngineConfig, root: &Value, fabric_branch: bool) -> Result<Self> {
        #[cfg(not(feature = "cuda"))]
        if fabric_branch
            && super::document::get(root, "transports/mnnvl/enable") != Some(&Value::Bool(false))
        {
            return Err(invalid("MC_ENABLE_MNNVL", "requires the cuda feature"));
        }
        #[cfg(feature = "metrics")]
        {
            c.metrics = c.metrics.resolve(Some(root), |_| Ok(None))?;
        }
        #[cfg(feature = "gds")]
        {
            c.gds_enabled = false;
        }
        let _ = (c, root);
        Ok(Self {
            #[cfg(feature = "fabric")]
            libfabric: Default::default(),
            #[cfg(feature = "fabric")]
            libfabric_enabled: false,
            #[cfg(feature = "cuda")]
            fabric: Default::default(),
            #[cfg(feature = "cuda")]
            fabric_branch,
            #[cfg(feature = "cuda")]
            fabric_enabled: true,
        })
    }
    pub(super) fn set(&mut self, c: &mut EngineConfig, p: &str, v: Value) -> Result<()> {
        let _ = &c;
        match p {
            #[cfg(feature = "fabric")]
            "transports/fabric/provider" => {
                self.libfabric.provider = match typed::<String>(p, v)?.as_str() {
                    "auto" => crate::FabricProvider::Auto,
                    "efa" => crate::FabricProvider::Efa,
                    "tcp" => crate::FabricProvider::Tcp,
                    _ => return Err(invalid(p, "expected auto, efa or tcp")),
                }
            }
            #[cfg(feature = "fabric")]
            "transports/fabric/enable" => self.libfabric_enabled = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/devices" => self.libfabric.devices = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/fabric_name" => self.libfabric.fabric_name = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/idle_sleep_us" => self.libfabric.idle_sleep_us = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/max_mr_size" => self.libfabric.max_mr_size = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/max_posted_ops" => self.libfabric.max_posted_ops = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/max_pte_entries" => self.libfabric.max_pte_entries = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/max_register_threads" => {
                self.libfabric.max_register_threads = typed(p, v)?
            }
            #[cfg(feature = "fabric")]
            "transports/fabric/op_timeout_ms" => self.libfabric.op_timeout_ms = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/post_timeout_ms" => self.libfabric.post_timeout_ms = typed(p, v)?,
            #[cfg(feature = "fabric")]
            "transports/fabric/quiesce_timeout_ms" => {
                self.libfabric.quiesce_timeout_ms = typed(p, v)?
            }
            #[cfg(feature = "fabric")]
            "transports/fabric/slice_size" => self.libfabric.slice_size = typed(p, v)?,
            #[cfg(feature = "metrics")]
            "metrics/enabled"
            | "metrics/http_host"
            | "metrics/http_port"
            | "metrics/http_server_threads"
            | "metrics/report_interval_seconds" => (),
            #[cfg(feature = "io-uring")]
            "transports/io_uring/enable" => c.io_uring_enabled = typed(p, v)?,
            #[cfg(feature = "io-uring")]
            "transports/io_uring/pinned_staging" => c.io_uring.pinned_staging = typed(p, v)?,
            #[cfg(feature = "io-uring")]
            "transports/io_uring/staging_slot_kb" => c.io_uring.staging_slot_kb = typed(p, v)?,
            #[cfg(feature = "io-uring")]
            "transports/io_uring/staging_slots" => c.io_uring.staging_slots = typed(p, v)?,
            #[cfg(feature = "gds")]
            "transports/gds/enable" => c.gds_enabled = typed(p, v)?,
            #[cfg(feature = "gds")]
            "transports/gds/io_batch_depth" => c.gds_batch_depth = typed(p, v)?,
            #[cfg(feature = "cuda")]
            "transports/mnnvl/enable" => self.fabric_enabled = typed(p, v)?,
            #[cfg(feature = "cuda")]
            "transports/mnnvl/egm" => self.fabric.egm = typed(p, v)?,
            #[cfg(feature = "cuda")]
            "transports/nvlink/enable" => c.nvlink = typed(p, v)?,
            #[cfg(feature = "cuda")]
            "transports/nvlink/host_register" => {
                let enabled = typed(p, v)?;
                if enabled && !self.fabric_branch {
                    return Err(invalid(p, "legacy IPC cannot export host registrations"));
                }
                self.fabric.host_register = enabled;
            }
            #[cfg(feature = "cuda")]
            "transports/nvlink/handle_type" => {
                self.fabric.handle_type = match v.as_str() {
                    Some("fabric") => crate::MnnvlHandleType::Fabric,
                    Some("posix_fd") => {
                        return Err(invalid(p, "POSIX FD fabric sharing is unsupported"));
                    }
                    _ => return Err(invalid(p, "expected fabric")),
                };
            }
            #[cfg(feature = "cuda")]
            "transports/nvlink/async_memcpy_threshold" => {
                let n = typed(p, v)?;
                if self.fabric_branch {
                    self.fabric.async_memcpy_threshold = n;
                } else {
                    c.nvlink_async_memcpy_threshold_kb = n;
                }
            }
            // A portable configuration can turn off an owner that was not built.
            _ if known_enable(p) && v == Value::Bool(false) => (),
            #[cfg(not(feature = "metrics"))]
            "metrics/enabled" if v == Value::Bool(false) => (),
            _ => return Err(invalid(p, "unsupported configuration field in this build")),
        }
        Ok(())
    }
    pub(super) fn finish(self, c: &mut EngineConfig) -> Result<()> {
        #[cfg(feature = "fabric")]
        {
            self.libfabric.validate()?;
            c.fabric = self.libfabric_enabled.then_some(self.libfabric);
        }
        let _ = &c;
        #[cfg(feature = "cuda")]
        if self.fabric_branch {
            c.nvlink = false;
            c.mnnvl = self.fabric_enabled.then_some(self.fabric);
        }
        Ok(())
    }
}
