//! Owner-derived memory classification and bounded location metadata.

use std::fmt;

/// TENT platform memory types supported by this build's owner API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryType {
    Cpu,
    Cuda,
}

/// An owner's location, not permission to access its bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryLocation {
    /// Unknown placement is `None`, including ordinary and portable pinned DRAM.
    Cpu(Option<u32>),
    /// CUDA-visible ordinal and process-local native context identity. The
    /// numeric context does not retain or authorize use of a native handle.
    Cuda { device: i32, context: usize },
}

impl MemoryLocation {
    pub fn memory_type(self) -> MemoryType {
        match self {
            Self::Cpu(_) => MemoryType::Cpu,
            Self::Cuda { .. } => MemoryType::Cuda,
        }
    }
}

impl fmt::Display for MemoryLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cpu(Some(node)) => write!(f, "cpu:{node}"),
            Self::Cpu(None) => f.write_str("*"),
            Self::Cuda { device, .. } => write!(f, "cuda:{device}"),
        }
    }
}

/// A checked subrange of a live owner. This metadata does not hold a transfer
/// loan, wait for a producer, or extend the lifetime of the memory/context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeLocation {
    pub address: u64,
    pub length: usize,
    pub location: MemoryLocation,
}
