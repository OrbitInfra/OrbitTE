use std::collections::HashMap;

use crate::{Error, Result};

/// Burned full rkeys survive MW deallocation for the entire native PD lifetime.
pub(super) struct Keys {
    indices: HashMap<u32, [u64; 4]>,
    limit: usize,
}

impl Keys {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            indices: HashMap::new(),
            limit,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.indices.len()
    }

    pub(super) fn exhausted(&self, native_key: u32) -> bool {
        self.indices.get(&(native_key & !255)).is_some_and(|used| {
            (1..=255).all(|increment| {
                let tag = native_key.wrapping_add(increment) & 255;
                used[(tag / 64) as usize] & (1u64 << (tag % 64)) != 0
            })
        })
    }

    fn tags(&mut self, native_key: u32) -> Result<&mut [u64; 4]> {
        let index = native_key & !255;
        if !self.indices.contains_key(&index) {
            if self.indices.len() >= self.limit {
                return Err(Error::ResourceExhausted);
            }
            self.indices
                .try_reserve(1)
                .map_err(|_| Error::ResourceExhausted)?;
            self.indices.insert(index, [0; 4]);
        }
        self.indices
            .get_mut(&index)
            .ok_or(Error::Internal("missing RDMA key index"))
    }
    /// Reserve a provider-named key before the firmware may make it accessible.
    pub(super) fn claim_exact(&mut self, key: u32) -> Result<bool> {
        let used = self.tags(key)?;
        let tag = key & 255;
        let word = &mut used[(tag / 64) as usize];
        let bit = 1u64 << (tag % 64);
        let fresh = *word & bit == 0;
        *word |= bit;
        Ok(fresh)
    }
    pub(super) fn burn(&mut self, native_key: u32) -> Result<u32> {
        let index = native_key & !255;
        let used = self.tags(native_key)?;
        // ibv_inc_rkey changes only the low eight bits. The provider may hand
        // this same index back after MW destruction, even with a different tag.
        // A BIND always changes the native initial key, even if that tag has
        // never been published. Do not rely on a provider accepting equal keys.
        for increment in 1..=255 {
            let tag = native_key.wrapping_add(increment) & 255;
            let word = &mut used[(tag / 64) as usize];
            let bit = 1u64 << (tag % 64);
            if *word & bit == 0 {
                *word |= bit;
                return Ok(index | tag);
            }
        }
        Err(Error::ResourceExhausted)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/keys.rs"]
mod tests;
