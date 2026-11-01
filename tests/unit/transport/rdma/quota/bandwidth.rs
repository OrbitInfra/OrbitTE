use super::*;
use crate::TransferOp;
use crate::transport::rdma::{qp::send_queue::SendQueue, slice::SendWork, sys};
use std::sync::Arc;

fn info() -> Arc<DeviceInfo> {
    let info = Arc::new(DeviceInfo::new(SchedulingParams {
        bandwidth_learning_rate: 0.0,
        transmit_bandwidth_learning_rate: 0.0,
        ..Default::default()
    }));
    info.set_bandwidth(Some(100_000));
    info
}
fn completed_work(info: &Arc<DeviceInfo>, start: Instant, end: Instant, length: usize) {
    let sq = SendQueue::new(2, Arc::default(), Some(Arc::clone(info))).unwrap();
    let mut work = SendWork::new(length, length, 1, sq, 1).unwrap();
    let wr = work.reserve(2).unwrap()[0];
    work.posted(
        0,
        sys::PostResult {
            accepted: 1,
            uncertain: false,
        },
        start,
    )
    .unwrap();
    work.complete(
        sys::Completion {
            id: wr.id,
            opcode: 2,
            length: wr.length,
            ..Default::default()
        },
        TransferOp::Read,
        end,
    )
    .unwrap();
}

#[test]
fn actual_payload_completions_learn_selection_and_exclude_idle_time_from_transmit() {
    let info = info();
    let t = Instant::now();
    completed_work(&info, t, t + Duration::from_millis(10), 25_000_000);
    completed_work(
        &info,
        t + Duration::from_millis(30),
        t + Duration::from_millis(40),
        25_000_000,
    );
    let stats = info.snapshot(2, "nic2", 99);
    assert_eq!(
        (
            stats.completed_bytes,
            stats.posted_bytes,
            stats.inflight_bytes
        ),
        (50_000_000, 0, 99)
    );
    assert_eq!((stats.selection_samples, stats.transmit_samples), (2, 2));
    assert_eq!(stats.busy_time, Duration::from_millis(20));
    assert_eq!(stats.ewma_bandwidth_bps, 2.5e9);
    assert_eq!(
        stats.transmit_bandwidth_bps, 2.5e9,
        "20 ms of idle time is not link work"
    );
    let sq = SendQueue::new(1, Arc::default(), Some(Arc::clone(&info))).unwrap();
    let mut bind = SendWork::control(sq, 200).unwrap();
    bind.reserve(1).unwrap();
    bind.posted(
        0,
        sys::PostResult {
            accepted: 1,
            uncertain: false,
        },
        t + Duration::from_millis(41),
    )
    .unwrap();
    bind.complete(
        sys::Completion {
            id: 200,
            opcode: 5,
            ..Default::default()
        },
        crate::transport::rdma::slice::Operation::Bind,
        t + Duration::from_millis(42),
    )
    .unwrap();
    let after = info.snapshot(2, "nic2", 99);
    assert_eq!(
        (
            after.completed_bytes,
            after.selection_samples,
            after.transmit_samples
        ),
        (50_000_000, 2, 2)
    );
}

#[test]
fn partial_uncertain_and_failed_payloads_keep_debt_until_drain_and_never_teach_rates() {
    for (code, accepted, uncertain, failed_cqe, owed) in [
        (libc::EIO, 1, false, false, 8_000_000),
        (libc::EIO, 0, true, false, 16_000_000),
        (0, 2, false, true, 8_000_000),
    ] {
        let info = info();
        let t = Instant::now();
        let sq = SendQueue::new(2, Arc::default(), Some(Arc::clone(&info))).unwrap();
        let mut work = SendWork::new(16_000_000, 8_000_000, 2, Arc::clone(&sq), 1).unwrap();
        let first = work.reserve(2).unwrap()[0];
        assert_eq!(
            work.posted(
                code,
                sys::PostResult {
                    accepted,
                    uncertain
                },
                t
            )
            .is_err(),
            code != 0
        );
        if failed_cqe {
            assert!(
                work.complete(
                    sys::Completion {
                        id: first.id,
                        status: 5,
                        ..Default::default()
                    },
                    TransferOp::Read,
                    t + Duration::from_millis(10)
                )
                .is_err()
            );
        }
        sq.fence();
        drop(work);
        let held = info.snapshot(0, "nic", 0);
        assert_eq!(held.posted_bytes, owed);
        assert_eq!((held.selection_samples, held.transmit_samples), (0, 0));
        sq.quiesce();
        assert_eq!(info.snapshot(0, "nic", 0).posted_bytes, 0);
        completed_work(
            &info,
            t + Duration::from_millis(100),
            t + Duration::from_millis(110),
            25_000_000,
        );
        assert_eq!(info.snapshot(0, "nic", 0).selection_samples, 1);
    }
    for fail_on_post in [true, false] {
        let info = info();
        let t = Instant::now();
        let sq = SendQueue::new(2, Arc::default(), Some(Arc::clone(&info))).unwrap();
        let mut payload = SendWork::new(16, 16, 1, Arc::clone(&sq), 1).unwrap();
        payload.reserve(1).unwrap();
        payload
            .posted(
                0,
                sys::PostResult {
                    accepted: 1,
                    uncertain: false,
                },
                t,
            )
            .unwrap();
        let mut control = SendWork::control(Arc::clone(&sq), 200).unwrap();
        control.reserve(2).unwrap();
        if fail_on_post {
            assert!(
                control
                    .posted(libc::EIO, sys::PostResult::default(), t)
                    .is_err()
            );
        } else {
            control
                .posted(
                    0,
                    sys::PostResult {
                        accepted: 1,
                        uncertain: false,
                    },
                    t,
                )
                .unwrap();
            assert!(
                control
                    .complete(
                        sys::Completion {
                            id: 200,
                            status: 5,
                            ..Default::default()
                        },
                        crate::transport::rdma::slice::Operation::Bind,
                        t
                    )
                    .is_err()
            );
        }
        sq.fence();
        payload
            .complete(
                sys::Completion {
                    id: 1,
                    opcode: 2,
                    length: 16,
                    ..Default::default()
                },
                TransferOp::Read,
                t + Duration::from_millis(10),
            )
            .unwrap();
        let stats = info.snapshot(0, "nic", 0);
        assert_eq!(
            (stats.selection_samples, stats.transmit_samples),
            (0, 0),
            "a failed control WR fences payload learning on the same QP"
        );
        assert_eq!((stats.posted_bytes, stats.completed_bytes), (0, 16));
        sq.quiesce();
    }
}

#[test]
fn rate_changes_reseed_once_and_reject_samples_from_an_older_link_generation() {
    let info = info();
    let t = Instant::now();
    let generation = info.posted(10, t);
    info.set_bandwidth(Some(200_000));
    info.ended(10, true, t + Duration::from_millis(10));
    info.observe(10, Duration::from_millis(10), generation);
    assert_eq!(info.bandwidth(), 25e9);
    assert_eq!(info.snapshot(0, "nic", 0).selection_samples, 0);
    completed_work(
        &info,
        t + Duration::from_millis(100),
        t + Duration::from_millis(110),
        25_000_000,
    );
    assert_eq!(info.bandwidth(), 2.5e9);
    info.set_bandwidth(Some(200_000));
    assert_eq!(
        info.bandwidth(),
        2.5e9,
        "periodic unchanged port queries retain learning"
    );
    info.set_bandwidth(None);
    assert_eq!(info.bandwidth(), 50e9);
    assert_eq!(info.snapshot(0, "nic", 0).link_speed_gbps, None);
}
