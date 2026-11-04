use crate::{Error, Result};

#[derive(Clone, Copy, Debug)]
pub struct IoUringConfig {
    pub pinned_staging: bool,
    /// KiB per slot, rounded up to a 4 KiB boundary.
    pub staging_slot_kb: usize,
    pub staging_slots: usize,
}
impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            pinned_staging: false,
            staging_slot_kb: 4096,
            staging_slots: 32,
        }
    }
}
impl IoUringConfig {
    pub(crate) fn with_environment(self) -> Result<Self> {
        self.resolve(|key| std::env::var_os(key))
    }

    pub(super) fn resolve(
        mut self,
        mut get: impl FnMut(&str) -> Option<std::ffi::OsString>,
    ) -> Result<Self> {
        if let Some(value) = get("MC_IOURING_PINNED_STAGING") {
            match value.to_str().and_then(crate::config::legacy_bool) {
                Some(enabled) => self.pinned_staging = enabled,
                _ => log::warn!("invalid MC_IOURING_PINNED_STAGING, retaining configured value"),
            }
        }
        if self.pinned_staging {
            for (key, field) in [
                ("MC_IOURING_STAGING_SLOT_KB", &mut self.staging_slot_kb),
                ("MC_IOURING_STAGING_SLOTS", &mut self.staging_slots),
            ] {
                if let Some(value) = get(key) {
                    *field = value
                        .to_str()
                        .and_then(|s| s.trim_start().parse::<usize>().ok())
                        .filter(|n| *n > 0)
                        .ok_or(Error::InvalidConfig(key))?;
                }
            }
        }
        Ok(self)
    }

    pub(crate) fn slot_size(self) -> Result<usize> {
        if !self.pinned_staging {
            return Ok(0);
        }
        let size = self
            .staging_slot_kb
            .checked_mul(1024)
            .and_then(|n| n.checked_add(4095))
            .map(|n| n & !4095);
        size.filter(|&n| {
            n > 0
                && (1..=4096).contains(&self.staging_slots)
                && n.checked_mul(self.staging_slots)
                    .is_some_and(|b| b <= 1 << 30)
        })
        .ok_or(Error::InvalidConfig(
            "io_uring staging slots must be positive and total at most 1 GiB",
        ))
    }
}
