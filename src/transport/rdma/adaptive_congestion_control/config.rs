//! Startup configuration for the optional TENT shared RDMA congestion core.
use crate::{Error, Result};
use std::ffi::OsString;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode {
    #[default]
    Off,
    Observe,
    Enforce,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub mode: Mode,
    pub min_window_bytes: u64,
    pub max_window_bytes: u64,
    /// Retained for compatibility with the release's startup configuration.
    pub target_drain_time_ns: u64,
    pub high_pressure_epochs: u32,
    pub low_pressure_epochs: u32,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Off,
            min_window_bytes: 4 << 20,
            max_window_bytes: 64 << 20,
            target_drain_time_ns: 2_000_000,
            high_pressure_epochs: 2,
            low_pressure_epochs: 3,
        }
    }
}

const MODE: &str = "MC_ADAPTIVE_CONGESTION_CONTROL_MODE";
const MIN: &str = "MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES";
const MAX: &str = "MC_ADAPTIVE_CONGESTION_CONTROL_MAX_WINDOW_BYTES";
const DRAIN: &str = "MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US";
const HIGH: &str = "MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS";
const LOW: &str = "MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS";

impl Config {
    /// Read only the six supported startup variables. Creating a transfer engine
    /// does not read them or enable a live congestion policy.
    pub fn from_environment() -> Result<Self> {
        Self::from_lookup(|key| std::env::var_os(key))
    }
    pub(super) fn from_lookup(mut get: impl FnMut(&str) -> Option<OsString>) -> Result<Self> {
        let defaults = Self::default();
        let mode = match get(MODE).as_deref().map(|s| s.to_str()) {
            None | Some(Some("off")) => Mode::Off,
            Some(Some("observe")) => Mode::Observe,
            Some(Some("enforce")) => Mode::Enforce,
            _ => return Err(Error::InvalidConfig(MODE)),
        };
        let mut positive = |key: &'static str, default: u64| -> Result<u64> {
            let Some(value) = get(key) else {
                return Ok(default);
            };
            value
                .to_str()
                .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&n| n != 0)
                .ok_or(Error::InvalidConfig(key))
        };
        let config = Self {
            mode,
            min_window_bytes: positive(MIN, defaults.min_window_bytes)?,
            max_window_bytes: positive(MAX, defaults.max_window_bytes)?,
            target_drain_time_ns: positive(DRAIN, defaults.target_drain_time_ns / 1000)?
                .checked_mul(1000)
                .ok_or(Error::InvalidConfig(DRAIN))?,
            high_pressure_epochs: positive(HIGH, u64::from(defaults.high_pressure_epochs))?
                .try_into()
                .map_err(|_| Error::InvalidConfig(HIGH))?,
            low_pressure_epochs: positive(LOW, u64::from(defaults.low_pressure_epochs))?
                .try_into()
                .map_err(|_| Error::InvalidConfig(LOW))?,
        };
        config.validate()?;
        Ok(config)
    }
    pub(super) fn validate(self) -> Result<()> {
        if self.min_window_bytes == 0 || self.min_window_bytes > self.max_window_bytes {
            return Err(Error::InvalidConfig("adaptive congestion window range"));
        }
        if self.target_drain_time_ns == 0 {
            return Err(Error::InvalidConfig(DRAIN));
        }
        if self.high_pressure_epochs == 0 {
            return Err(Error::InvalidConfig(HIGH));
        }
        if self.low_pressure_epochs == 0 {
            return Err(Error::InvalidConfig(LOW));
        }
        Ok(())
    }
}
