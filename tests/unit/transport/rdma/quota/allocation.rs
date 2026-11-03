use super::*;
use crate::transport::rdma::{RdmaConfig, SchedulingParams};

fn selector(smart: bool, strict: bool) -> Arc<DeviceSelector> {
    let selector = DeviceSelector::new(
        &super::super::tests::topology(),
        0b1101,
        &RdmaConfig {
            strict_local_numa: strict,
            scheduling: SchedulingParams {
                smart_selection: smart,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    selector.set_bandwidth(0, Some(100_000));
    selector.set_bandwidth(2, Some(400_000));
    selector.set_bandwidth(3, Some(10_000));
    selector
}

#[test]
fn weighted_parent_reserves_exact_folded_bytes_and_transfers_only_consumed_charges() {
    let selector = selector(true, false);
    let mut plan = selector
        .allocate(41, 8, 5, MemoryLocation::Cpu(None), 0b0101)
        .unwrap();
    assert_eq!(plan.devices[..5], [2, 2, 2, 0, 2]);
    assert_eq!(selector.state.lock().unwrap().inflight, [8, 0, 33, 0]);
    let first = plan.next().unwrap();
    assert_eq!((first.device_id(), first.length()), (2, 8));
    drop(plan);
    assert_eq!(selector.state.lock().unwrap().inflight, [0, 0, 8, 0]);
    selector.set_available(2, false);
    assert_eq!(selector.state.lock().unwrap().inflight[2], 8);
    drop(first);
    assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
    selector.set_available(2, true);
    // Backlog belongs to the whole parent and influences the next allocation.
    let held = selector
        .allocate(10_000, 10_000, 1, MemoryLocation::Cpu(None), 4)
        .unwrap();
    let plan = selector
        .allocate(41, 8, 5, MemoryLocation::Cpu(None), 0b0101)
        .unwrap();
    assert_eq!(plan.devices[..5], [0; 5]);
    drop((held, plan));
    assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
}

#[test]
fn failed_parent_is_atomic_and_probe_respects_masks_holes_numa_and_short_plans() {
    let selector = selector(true, false);
    let full = selector
        .allocate(usize::MAX, usize::MAX, 1, MemoryLocation::Cpu(None), 4)
        .unwrap();
    selector.state.lock().unwrap().multipath_calls = 99;
    // Probe would charge NIC 0 before overflowing NIC 2: neither may change.
    assert!(matches!(
        selector.allocate(2, 1, 2, MemoryLocation::Cpu(None), 0b0101),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(
        selector.state.lock().unwrap().inflight,
        [0, 0, usize::MAX, 0]
    );
    assert_eq!(selector.state.lock().unwrap().multipath_calls, 99);
    drop(full);
    for (length, block, count) in [
        (0, 1, 1),
        (8, 0, 2),
        (1, 1, 0),
        (65, 1, 65),
        (8, 8, 2),
        (usize::MAX, usize::MAX, 3),
    ] {
        assert!(matches!(
            selector.allocate(length, block, count, MemoryLocation::Cpu(None), u64::MAX),
            Err(Error::InvalidRange)
        ));
    }
    assert!(
        selector
            .allocate(8, 4, 2, MemoryLocation::Cpu(None), 2)
            .is_err()
    );
    assert_eq!(selector.state.lock().unwrap().multipath_calls, 99);
    let probe = selector
        .allocate(8, 4, 2, MemoryLocation::Cpu(None), u64::MAX)
        .unwrap();
    assert_eq!(probe.devices[..2], [0, 2]);
    drop(probe);
    for _ in 0..99 {
        drop(
            selector
                .allocate(8, 4, 2, MemoryLocation::Cpu(None), u64::MAX)
                .unwrap(),
        );
    }
    let probe = selector
        .allocate(8, 4, 2, MemoryLocation::Cpu(None), u64::MAX)
        .unwrap();
    assert_eq!(
        probe.devices[..2],
        [3, 0],
        "short parents rotate the probe start beyond faster NICs"
    );
    drop(probe);
    let strict = self::selector(true, true);
    for _ in 0..99 {
        drop(
            strict
                .allocate(8, 4, 2, MemoryLocation::Cpu(Some(0)), u64::MAX)
                .unwrap(),
        );
    }
    let probe = strict
        .allocate(8, 4, 2, MemoryLocation::Cpu(Some(0)), u64::MAX)
        .unwrap();
    assert_eq!(
        probe.devices[..2],
        [0, 2],
        "probe cannot admit cross-NUMA or unavailable slots"
    );
    drop(probe);
    strict.set_available(2, false);
    let plan = strict
        .allocate(41, 8, 5, MemoryLocation::Cpu(Some(0)), u64::MAX)
        .unwrap();
    assert_eq!(plan.devices[..5], [0; 5]);
    drop(plan);
    assert_eq!(strict.state.lock().unwrap().inflight, [0; 4]);
}

#[test]
fn baseline_parents_rotate_first_tier_and_concurrent_reservations_balance() {
    let selector = selector(false, false);
    let a = selector
        .allocate(41, 8, 5, MemoryLocation::Cpu(Some(0)), u64::MAX)
        .unwrap();
    let b = selector
        .allocate(41, 8, 5, MemoryLocation::Cpu(Some(0)), u64::MAX)
        .unwrap();
    assert_eq!(a.devices[..5], [0, 2, 0, 2, 0]);
    assert_eq!(b.devices[..5], [2, 0, 2, 0, 2]);
    assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
    assert_eq!(selector.state.lock().unwrap().allocated, [41, 0, 41, 0]);
    drop((a, b));
    let barrier = Arc::new(std::sync::Barrier::new(9));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let selector = Arc::clone(&selector);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                let plan = selector
                    .allocate(41, 8, 5, MemoryLocation::Cpu(Some(0)), u64::MAX)
                    .unwrap();
                barrier.wait();
                barrier.wait();
                drop(plan);
            });
        }
        barrier.wait();
        assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
        assert_eq!(selector.state.lock().unwrap().allocated, [205, 0, 205, 0]);
        barrier.wait();
    });
    assert_eq!(selector.state.lock().unwrap().inflight, [0; 4]);
    assert_eq!(selector.state.lock().unwrap().multipath_calls, 0);
}

#[test]
fn quota_follows_exact_slices_through_the_plan_tail_and_release() {
    let base = 65536;
    let mut cases = vec![(1000, 250, 4), (1000, 300, 4), (1000, 300, 3)];
    for cap in [32, 64] {
        for length in [
            base + 1,
            8 * base + 8192,
            8 * base + base / 2,
            3 << 20,
            10 << 20,
            100 * base + 7,
        ] {
            let (block, count) =
                crate::transport::rdma::slice::plan_rdma_slices(length, base, cap).unwrap();
            cases.push((length, block, count));
        }
    }
    for (length, block, count) in cases {
        let selector = selector(true, false);
        let mut plan = selector
            .allocate(length, block, count, MemoryLocation::Cpu(None), 5)
            .unwrap();
        let totals = || {
            selector
                .stats()
                .into_iter()
                .fold((0, 0), |(live, total), s| {
                    (live + s.inflight_bytes, total + s.allocated_bytes)
                })
        };
        let mut remaining = length;
        for i in 0..count {
            assert_eq!(totals(), (remaining, length as u64));
            let lease = plan.next().unwrap();
            let expected = if i + 1 == count { remaining } else { block };
            assert_eq!(lease.length(), expected);
            remaining -= expected;
            drop(lease);
        }
        assert_eq!(remaining, 0);
        assert_eq!(totals(), (0, length as u64));
        assert!(
            selector
                .allocate(4096, 4096, 1, MemoryLocation::Cpu(None), 2)
                .is_err()
        );
        assert_eq!(totals(), (0, length as u64));
    }
}
