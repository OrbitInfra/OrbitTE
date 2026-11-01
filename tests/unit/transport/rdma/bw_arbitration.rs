use super::*;
use std::time::Duration;

#[test]
fn urgency_accounts_for_hardware_backlog_and_each_selected_flow() {
    let now = Instant::now();
    let flows = |spec: &[(u64, usize)]| {
        spec.iter()
            .enumerate()
            .map(|(index, &(ns, length))| ArbFlow {
                index,
                deadline: Some(now + Duration::from_nanos(ns)),
                length,
            })
            .collect::<Vec<_>>()
    };
    for (backlog, expected) in [(0, vec![0, 1]), (65536, vec![1, 0])] {
        let mut work = flows(&[(100_000, 4096), (30_000, 1024)]);
        order_by_urgency(&mut work, now, 1e9, backlog);
        assert_eq!(work.iter().map(|f| f.index).collect::<Vec<_>>(), expected);
    }
    let mut work = flows(&[(40_000, 100_000), (100_000, 20_000), (50_000, 5_000)]);
    order_by_urgency(&mut work, now, 1e9, 0);
    assert_eq!(work.iter().map(|f| f.index).collect::<Vec<_>>(), [0, 2, 1]);
}

#[test]
fn disabled_estimates_and_ties_preserve_fifo_across_the_greedy_tail() {
    let now = Instant::now();
    for bandwidth in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e9] {
        for deadline in [None, Some(now), Some(now + Duration::from_secs(1))] {
            let mut flows: Vec<_> = (0..129)
                .map(|index| ArbFlow {
                    index,
                    deadline,
                    length: 4096,
                })
                .collect();
            order_by_urgency(&mut flows, now, bandwidth, u64::MAX);
            assert_eq!(
                flows.iter().map(|f| f.index).collect::<Vec<_>>(),
                (0..129).collect::<Vec<_>>()
            );
        }
    }
}
