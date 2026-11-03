//! Scrape resource owners without exposing peers, addresses, keys or user labels.
use super::*;
use crate::metrics::{Metrics, Owners};

impl TransferEngine {
    /// Access the engine's metrics when enabled at construction.
    pub fn metrics(&self) -> Option<&Metrics> {
        self.metrics.as_ref()
    }

    pub(super) fn record_registration_routes(&self, memory: &[RegisteredMemory]) {
        use crate::metrics::transport_bit;
        for m in memory {
            #[allow(unused_mut)]
            let mut mask = transport_bit(Transport::Tcp);
            #[cfg(feature = "rdma")]
            if self.source.rdma.as_ref().is_some_and(|r| r.direct(m)) {
                mask |= transport_bit(Transport::Rdma);
            }
            #[cfg(feature = "cuda")]
            if self.source.nvlink.is_some() && m.ipc_requester() {
                mask |= transport_bit(Transport::Nvlink);
            }
            #[cfg(feature = "io-uring")]
            if self.io_uring.is_some() {
                mask |= transport_bit(Transport::IoUring);
            }
            #[cfg(feature = "gds")]
            if self.file_available(Transport::Gds, Some(m)) {
                mask |= transport_bit(Transport::Gds);
            }
            m.0.metric_transports
                .store(mask, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub(super) fn install_metrics(mut self, cfg: &crate::metrics::MetricsConfig) -> Result<Self> {
        if !cfg.enabled {
            return Ok(self);
        }
        #[cfg(feature = "rdma")]
        let rdma = self.source.rdma.as_ref().map(Arc::downgrade);
        let queue = self.runtime_queue.clone();
        let credits = self.receiver_credits.clone();
        let qos = self.qos.as_ref().map(QosContractResolver::metrics_sampler);
        let allocations = self.allocations.clone();
        let batches = self.batches.clone();
        let metrics = Metrics::start(cfg, move || {
            let mut snapshot = Owners::default();
            if let Ok((active, _, workers, deferred, _)) = allocations.usage() {
                snapshot.gauges.extend([
                    ("orbitte_allocated_batches", active as u64),
                    ("orbitte_requester_workers", workers as u64),
                    ("orbitte_deferred_batches", deferred as u64),
                ]);
            }
            let usage = batches.usage.lock().unwrap_or_else(|p| p.into_inner());
            snapshot.gauges.extend([
                ("orbitte_inflight_batches", usage.count as u64),
                ("orbitte_inflight_bytes", usage.bytes as u64),
            ]);
            drop(usage);
            if let Some(q) = &queue {
                let s = q.stats();
                snapshot.gauges.extend([
                    ("orbitte_queue_owners", s.queued_owners as u64),
                    ("orbitte_queue_bytes", s.queued_bytes as u64),
                    ("orbitte_dispatching_owners", s.dispatching_owners as u64),
                    ("orbitte_dispatching_bytes", s.dispatching_bytes as u64),
                ]);
                snapshot.counters.push((
                    "orbitte_queue_deadline_drops_total",
                    s.deadline_infeasible_drops,
                ));
            }
            if let Some(q) = &qos {
                let u = q();
                snapshot.gauges.extend([
                    ("orbitte_qos_owners", u.owners as u64),
                    ("orbitte_qos_inflight_bytes", u.inflight_bytes as u64),
                    ("orbitte_qos_inflight_requests", u.inflight_requests as u64),
                ]);
            }
            if let Some(c) = &credits {
                let (a, r) = c.metric_owners();
                snapshot.gauges.extend([
                    ("orbitte_receiver_credit_active_owners", a as u64),
                    ("orbitte_receiver_credit_reserved_owners", r as u64),
                ]);
            }
            #[cfg(feature = "rdma")]
            if let Some(r) = rdma.as_ref().and_then(std::sync::Weak::upgrade) {
                let s = r.rail_health_stats();
                snapshot
                    .gauges
                    .push(("orbitte_rdma_open_rails", s.open_rails as u64));
                snapshot.counters.extend([
                    ("orbitte_rdma_rail_errors_total", s.rail_errors),
                    ("orbitte_rdma_rail_probes_total", s.rail_probes),
                    ("orbitte_rdma_rail_recoveries_total", s.rail_recoveries),
                ]);
            }
            snapshot
        })?;
        let _ = self
            .allocations
            .metrics
            .set(Arc::downgrade(metrics.recorder()));
        let _ = self
            .source
            .registry
            .metrics
            .set(Arc::downgrade(metrics.recorder()));
        self.metrics = Some(metrics);
        Ok(self)
    }
}
