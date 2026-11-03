//! TENT endpoint QP indices partitioned into contiguous, named segments.
use serde::Deserialize;

use crate::{Error, Result};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QpPool {
    pub name: String,
    pub num_qp: usize,
    pub service_level: Option<u8>,
    pub traffic_class: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QpRange {
    pub(crate) begin: usize,
    pub(crate) count: usize,
}

impl QpRange {
    #[cfg(any(feature = "rdma", test))]
    pub(crate) fn index(self, candidate: i32) -> usize {
        self.begin + candidate.max(0) as usize % self.count
    }
}

pub(crate) struct QpLayout {
    pub(crate) width: usize,
    pools: Vec<(QpPool, QpRange)>,
}

impl QpLayout {
    pub(crate) fn new(pools: &[QpPool], flat: usize) -> Result<Self> {
        let mut segments = Vec::new();
        let mut width = 0usize;
        for pool in pools {
            if pool.name.is_empty()
                || pool.name.len() > 256
                || pool.service_level.is_some_and(|sl| sl > 15)
                || pool.num_qp == 0
                || pool.num_qp > 64 - width
                || segments
                    .iter()
                    .any(|(p, _): &(QpPool, QpRange)| p.name == pool.name)
            {
                return Err(Error::InvalidSelectorConfig(format!(
                    "QP pool {:?}: expected a unique bounded name, positive count, total width <= 64 and SL <= 15",
                    pool.name
                )));
            }
            segments.push((
                pool.clone(),
                QpRange {
                    begin: width,
                    count: pool.num_qp,
                },
            ));
            width += pool.num_qp;
        }
        if segments.is_empty() {
            width = flat;
            if !(1..=64).contains(&width) {
                return Err(Error::InvalidConfig(
                    "endpoint qp_mul_factor must be in 1..=64",
                ));
            }
        }
        Ok(Self {
            width,
            pools: segments,
        })
    }

    pub(crate) fn range(&self, name: Option<&str>) -> QpRange {
        self.pools
            .iter()
            .find(|(p, _)| Some(&*p.name) == name)
            .map_or(
                QpRange {
                    begin: 0,
                    count: self.width,
                },
                |(_, range)| *range,
            )
    }

    #[cfg(test)]
    pub(crate) fn has_pools(&self) -> bool {
        !self.pools.is_empty()
    }

    #[cfg(any(feature = "rdma", test))]
    pub(crate) fn pool_at(&self, index: usize) -> Option<&QpPool> {
        self.pools
            .iter()
            .find(|(_, range)| (range.begin..range.begin + range.count).contains(&index))
            .map(|(pool, _)| pool)
    }
}
