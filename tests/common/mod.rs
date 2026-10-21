#![allow(unsafe_code)]

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use crate::HostMemory;

pub struct Allocation {
    bytes: UnsafeCell<Box<[u8]>>,
    drops: Arc<AtomicUsize>,
}

// SAFETY: the only pointer is handed to one HostMemory. No safe API exposes the
// bytes, so all subsequent accesses use that object's shared lock. The box never
// moves/reallocates after pointer extraction and outlives every retained owner.
unsafe impl Sync for Allocation {}

impl Drop for Allocation {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct ExternalFixture {
    pub memory: Arc<HostMemory>,
    pub owner: Weak<Allocation>,
    pub drops: Arc<AtomicUsize>,
}

pub fn external_memory(bytes: &[u8]) -> ExternalFixture {
    let drops = Arc::new(AtomicUsize::new(0));
    let allocation = Arc::new(Allocation {
        bytes: UnsafeCell::new(bytes.to_vec().into_boxed_slice()),
        drops: Arc::clone(&drops),
    });
    // SAFETY: this is the only initial access, before HostMemory is constructed.
    let bytes = unsafe { &mut *allocation.bytes.get() };
    let address = NonNull::new(bytes.as_mut_ptr()).unwrap();
    let owner = Arc::downgrade(&allocation);
    // SAFETY: the private box remains stable and its owner cannot expose any
    // uncoordinated byte access. HostMemory retains the actual allocation.
    let memory =
        Arc::new(unsafe { HostMemory::from_raw_parts(address, bytes.len(), allocation) }.unwrap());
    ExternalFixture {
        memory,
        owner,
        drops,
    }
}
