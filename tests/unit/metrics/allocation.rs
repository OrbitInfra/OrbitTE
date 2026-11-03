//! Count only this test thread's allocations inside recording, excluding the
//! request machinery, which has independent allocation and ownership contracts.
#![allow(unsafe_code)]
use super::*;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};
struct Count;
thread_local! {static ALLOCATIONS:Cell<Option<u64>>=const{Cell::new(None)};}
fn allocated() {
    let _ = ALLOCATIONS.try_with(|c| {
        if let Some(n) = c.get() {
            c.set(Some(n + 1));
        }
    });
}
// SAFETY: all pointers and layouts are passed unchanged to the system allocator.
// The thread-local counter has constant initialization and uses no allocation.
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        // SAFETY: forward the caller's allocation contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        // SAFETY: forward the caller's allocation and zero-fill contract.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: p and layout belong to the same system allocation.
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, n: usize) -> *mut u8 {
        allocated();
        // SAFETY: forward the original allocation and requested size.
        unsafe { System.realloc(p, layout, n) }
    }
}
#[global_allocator]
static COUNT: Count = Count;
#[test]
fn first_use_retry_and_completion_do_not_allocate() {
    let cells = Arc::new(Recorder::new());
    ALLOCATIONS.with(|n| n.set(Some(0)));
    let o = Observation::new(
        &cells,
        TransferOp::Read,
        Transport::Rdma,
        8192,
        None,
        Some(Instant::now()),
    );
    o.dispatched();
    o.begin(Transport::Rdma);
    o.finish(Some(&Error::Unavailable("post rejected")), false);
    o.retry(Transport::Tcp);
    o.begin(Transport::Tcp);
    o.finish(None, true);
    o.terminal(None);
    assert_eq!(ALLOCATIONS.with(|n| n.replace(None)), Some(0));
}
