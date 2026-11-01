use std::time::Duration;

use crate::{Error, Result};

/// TENT DeviceSelector scheduling parameters. Rates are in Gbit/s; EWMA
/// coefficients weight the old estimate (zero learns entirely from the sample).
#[derive(Clone, Debug)]
pub struct SchedulingParams {
    pub smart_selection: bool,
    pub numa_tier_weights: [f64; 3],
    pub score_epsilon: f64,
    pub bandwidth_learning_rate: f64,
    pub transmit_bandwidth_learning_rate: f64,
    pub default_bandwidth_gbps: f64,
    pub min_bandwidth_gbps: f64,
    pub max_bandwidth_gbps: f64,
    pub ewma_min_multiplier: f64,
    pub ewma_max_multiplier: f64,
    pub transmit_meter_interval: Duration,
    pub transmit_meter_max_interval: Duration,
}

impl Default for SchedulingParams {
    fn default() -> Self {
        Self {
            smart_selection: true,
            numa_tier_weights: [1.0, 5.0, 10.0],
            score_epsilon: 1e-12,
            bandwidth_learning_rate: 0.01,
            transmit_bandwidth_learning_rate: 0.9,
            default_bandwidth_gbps: 400.0,
            min_bandwidth_gbps: 10.0,
            max_bandwidth_gbps: 800.0,
            ewma_min_multiplier: 0.1,
            ewma_max_multiplier: 10.0,
            transmit_meter_interval: Duration::from_millis(10),
            transmit_meter_max_interval: Duration::from_millis(50),
        }
    }
}

impl SchedulingParams {
    pub(crate) fn validate(&self) -> Result<()> {
        let positive = [
            self.score_epsilon,
            self.default_bandwidth_gbps,
            self.min_bandwidth_gbps,
            self.max_bandwidth_gbps,
            self.ewma_min_multiplier,
            self.ewma_max_multiplier,
        ];
        if positive
            .into_iter()
            .chain(self.numa_tier_weights)
            .any(|v| !v.is_finite() || v <= 0.0)
            || [
                self.bandwidth_learning_rate,
                self.transmit_bandwidth_learning_rate,
            ]
            .into_iter()
            .any(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
            || !(self.min_bandwidth_gbps..=self.max_bandwidth_gbps)
                .contains(&self.default_bandwidth_gbps)
            || self.ewma_min_multiplier > self.ewma_max_multiplier
            || !(self.max_bandwidth_gbps * 1.25e8 * self.ewma_max_multiplier).is_finite()
            || self.min_bandwidth_gbps * 1.25e8 * self.ewma_min_multiplier < 1.0
            || self
                .numa_tier_weights
                .iter()
                .any(|w| !(w * usize::MAX as f64).is_finite())
            || self.transmit_meter_interval.is_zero()
            || self.transmit_meter_max_interval < self.transmit_meter_interval
        {
            return Err(Error::InvalidConfig(
                "invalid RDMA bandwidth scheduling parameters",
            ));
        }
        Ok(())
    }

    pub(super) fn seed(&self, mbps: Option<u64>) -> f64 {
        let gbps = mbps.map_or(0.0, |v| v as f64 / 1000.0);
        if (self.min_bandwidth_gbps..=self.max_bandwidth_gbps).contains(&gbps) {
            gbps * 1.25e8
        } else {
            self.default_bandwidth_gbps * 1.25e8
        }
    }

    pub(super) fn learn(&self, old: f64, observed: f64, alpha: f64, seed: f64) -> f64 {
        (alpha * old + (1.0 - alpha) * observed).clamp(
            seed * self.ewma_min_multiplier,
            seed * self.ewma_max_multiplier,
        )
    }
}
