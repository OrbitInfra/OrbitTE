//! Scrape resource owners without exposing peers, addresses, keys or user labels.
use super::*;
use crate::metrics::{Metrics, Owners};

impl TransferEngine {
    /// Access the engine's metrics when enabled at construction.
    pub fn metrics(&self) -> Option<&Metrics> {
        self.metrics.as_ref()
    }

    /// Exporter state, including a disabled-at-construction engine. Reading it does not count as a scrape.
    pub fn metrics_status(&self) -> serde_json::Value {
        crate::metrics::pipeline_status(self.metrics())
    }

    pub(super) fn record_registration_routes(&self, memory: &[RegisteredMemory]) {
        use crate::metrics::transport_bit;
        for m in memory {
            #[allow(unused_mut)]
            let mut mask = if self.config.tcp {
                transport_bit(Transport::Tcp)
            } else {
                0
            };
            if self.hp_tcp.is_some() && crate::transport::hp_tcp::host(m) {
                mask |= transport_bit(Transport::HpTcp);
            }
            #[cfg(target_os = "linux")]
            if self
                .source
                .shm
                .as_ref()
                .is_some_and(|s| s.exports_memory(m))
            {
                mask |= transport_bit(Transport::Shm);
            }
            #[cfg(feature = "rdma")]
            if self.source.rdma.as_ref().is_some_and(|r| r.direct(m)) {
                mask |= transport_bit(Transport::Rdma);
            }
            #[cfg(feature = "cuda")]
            if self.source.metadata.nvlink.is_some() && m.0.descriptor.ipc_exportable {
                mask |= transport_bit(Transport::Nvlink);
            }
            #[cfg(feature = "cuda")]
            if m.0
                .fabric
                .get()
                .is_some_and(|slot| slot.local || slot.share.is_some())
            {
                mask |= transport_bit(Transport::Mnnvl);
            }
            #[cfg(feature = "gds")]
            if m.memory_type() == crate::MemoryType::Cuda
                && crate::transport::gds::BufferSlot::supports(m)
            {
                mask |= transport_bit(Transport::Gds);
            }
            let route = m.0.descriptor.profile.route;
            if route != Transport::Unspec {
                mask &= transport_bit(route);
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
        let issuer = self.source.credit_issuer.clone();
        let qos = self.qos.as_ref().map(QosContractResolver::metrics_sampler);
        let allocations = self.allocations.clone();
        let batches = self.batches.clone();
        let metrics = Metrics::start(cfg, move || {
            let mut snapshot = Owners::default();
            if let Ok((active, _, workers, deferred, _)) = allocations.usage() {
                snapshot.gauges.extend([
                    ("orbite_allocated_batches", active as u64),
                    ("orbite_requester_workers", workers as u64),
                    ("orbite_deferred_batches", deferred as u64),
                ]);
            }
            let usage = batches.usage.lock().unwrap_or_else(|p| p.into_inner());
            snapshot.gauges.extend([
                ("orbite_inflight_batches", usage.count as u64),
                ("orbite_inflight_bytes", usage.bytes as u64),
            ]);
            drop(usage);
            if let Some(q) = &queue {
                let s = q.stats();
                snapshot.gauges.extend([
                    ("orbite_queue_owners", s.queued_owners as u64),
                    ("orbite_queue_bytes", s.queued_bytes as u64),
                    ("orbite_dispatching_owners", s.dispatching_owners as u64),
                    ("orbite_dispatching_bytes", s.dispatching_bytes as u64),
                ]);
                snapshot.counters.push((
                    "orbite_queue_deadline_drops_total",
                    s.deadline_infeasible_drops,
                ));
            }
            if let Some(q) = &qos {
                let u = q();
                snapshot.gauges.extend([
                    ("orbite_qos_owners", u.owners as u64),
                    ("orbite_qos_inflight_bytes", u.inflight_bytes as u64),
                    ("orbite_qos_inflight_requests", u.inflight_requests as u64),
                ]);
            }
            if let Some(c) = &credits {
                let (a, r, balances) = c.metric_usage();
                for ((available, consumed), balance) in [
                    (
                        "orbite_receiver_credit_available_bytes",
                        "orbite_receiver_credit_consumed_bytes",
                    ),
                    (
                        "orbite_receiver_credit_available_requests",
                        "orbite_receiver_credit_consumed_requests",
                    ),
                    (
                        "orbite_receiver_credit_available_staging_slots",
                        "orbite_receiver_credit_consumed_staging_slots",
                    ),
                    (
                        "orbite_receiver_credit_available_consumer_slots",
                        "orbite_receiver_credit_consumed_consumer_slots",
                    ),
                ]
                .into_iter()
                .zip(balances)
                {
                    snapshot
                        .gauges
                        .extend([(available, balance.available), (consumed, balance.consumed)]);
                }
                snapshot.gauges.extend([
                    ("orbite_receiver_credit_active_owners", a as u64),
                    ("orbite_receiver_credit_reserved_owners", r as u64),
                ]);
            }
            if let Some(issuer) = &issuer {
                let s = issuer.stats();
                snapshot.gauges.extend([
                    ("orbite_receiver_active_senders", s.active_senders as u64),
                    ("orbite_receiver_reserved_bytes", s.reserved_bytes),
                    ("orbite_receiver_reserved_requests", s.reserved_requests),
                    (
                        "orbite_receiver_reserved_staging_slots",
                        s.reserved_staging_slots,
                    ),
                    (
                        "orbite_receiver_reserved_consumer_slots",
                        s.reserved_consumer_slots,
                    ),
                    (
                        "orbite_receiver_active_staging_slots",
                        s.active_staging_slots,
                    ),
                    (
                        "orbite_receiver_active_consumer_slots",
                        s.active_consumer_slots,
                    ),
                    (
                        "orbite_receiver_retained_staging_slots",
                        s.retained_staging_slots,
                    ),
                    (
                        "orbite_receiver_retained_consumer_slots",
                        s.retained_consumer_slots,
                    ),
                ]);
                snapshot.counters.push((
                    "orbite_receiver_retired_senders_total",
                    s.retired_senders as u64,
                ));
            }
            #[cfg(feature = "rdma")]
            if let Some(r) = rdma.as_ref().and_then(std::sync::Weak::upgrade) {
                let (inflight, allocated) = r.quota_totals();
                snapshot
                    .gauges
                    .push(("orbite_rdma_inflight_bytes", inflight));
                snapshot
                    .counters
                    .push(("orbite_rdma_allocated_bytes_total", allocated));
                let s = r.rail_health_stats();
                snapshot
                    .gauges
                    .push(("orbite_rdma_open_rails", s.open_rails as u64));
                snapshot.counters.extend([
                    ("orbite_rdma_rail_errors_total", s.rail_errors),
                    ("orbite_rdma_rail_probes_total", s.rail_probes),
                    ("orbite_rdma_rail_recoveries_total", s.rail_recoveries),
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
