use super::*;
use crate::transport::rdma::RdmaStats;
use std::sync::Mutex;

fn usage() -> Arc<Mutex<RdmaStats>> {
    Arc::default()
}

#[test]
fn frozen_tent_slice_geometry_bounds_count_and_folds_only_short_tails() {
    for (length, base, cap, expected) in [
        (1, 64, 64, vec![1]),
        (64, 64, 64, vec![64]),
        (65, 64, 64, vec![65]),
        (79, 64, 64, vec![79]),
        (80, 64, 64, vec![64, 16]),
        (128, 64, 64, vec![64, 64]),
        (129, 64, 64, vec![64, 65]),
        (1025, 64, 4, vec![320, 320, 385]),
        (1025, 64, 2, vec![576, 449]),
        (5 << 20, 64 << 10, 5, vec![1 << 20; 5]),
        (
            (1 << 20) + 1,
            64 << 10,
            5,
            vec![256 << 10, 256 << 10, 256 << 10, (256 << 10) + 1],
        ),
        (
            (1 << 20) + (64 << 10) - 1,
            64 << 10,
            5,
            vec![256 << 10, 256 << 10, 256 << 10, (320 << 10) - 1],
        ),
        (
            (1 << 20) + (64 << 10),
            64 << 10,
            5,
            vec![256 << 10, 256 << 10, 256 << 10, 256 << 10, 64 << 10],
        ),
        (
            (256 << 10) + 1,
            64 << 10,
            64,
            vec![64 << 10, 64 << 10, 64 << 10, (64 << 10) + 1],
        ),
        (32 << 20, 64 << 10, 64, vec![512 << 10; 64]),
        (32 << 20, 64 << 10, 32, vec![1 << 20; 32]),
    ] {
        let queue =
            SendWork::new(length, base, cap, SendQueue::new(64, usage()).unwrap(), 1).unwrap();
        let (block, count) = plan_rdma_slices(length, base, cap).unwrap();
        println!("ORBITTE_RDMA_GEOMETRY {length} {base} {cap} {block} {count}");
        assert_eq!(
            queue
                .requests
                .iter()
                .map(|r| r.length as usize)
                .collect::<Vec<_>>(),
            expected,
            "{length}/{base}/{cap}"
        );
        assert_eq!(
            queue
                .requests
                .iter()
                .map(|r| r.length as usize)
                .sum::<usize>(),
            length
        );
        let mut offset = 0;
        for (i, r) in queue.requests.iter().enumerate() {
            assert_eq!((r.id, r.offset), (i as u64 + 1, offset));
            offset += r.length;
        }
    }
}

#[test]
fn valid_out_of_order_cqes_resolve_each_slot_once_and_windows_stay_bounded() {
    for limit in [1, 3, 64] {
        let usage = usage();
        let sq = SendQueue::new(limit, usage.clone()).unwrap();
        let mut queue = SendWork::new(1024, 16, 64, sq.clone(), 1).unwrap();
        while !queue.finished() {
            let requests = queue.reserve(64).unwrap().to_vec();
            assert!(queue.outstanding() <= limit);
            queue
                .posted(
                    0,
                    sys::PostResult {
                        accepted: requests.len(),
                        uncertain: false,
                    },
                )
                .unwrap();
            if requests[0].id > 1 {
                let before = *usage.lock().unwrap();
                for status in [0, 7] {
                    assert!(
                        queue
                            .complete(
                                sys::Completion {
                                    id: requests[0].id - 1,
                                    status,
                                    opcode: 2,
                                    length: 16,
                                    ..Default::default()
                                },
                                TransferOp::Read,
                            )
                            .is_err()
                    );
                    let after = *usage.lock().unwrap();
                    assert_eq!(queue.outstanding(), requests.len());
                    assert_eq!(after.work_requests, before.work_requests);
                    assert_eq!(after.completed_reads, before.completed_reads);
                    assert_eq!(after.completed_read_bytes, before.completed_read_bytes);
                    assert_eq!(after.completion_errors, before.completion_errors);
                }
            }
            for r in requests.iter().rev() {
                queue
                    .complete(
                        sys::Completion {
                            id: r.id,
                            opcode: 2,
                            length: r.length,
                            ..Default::default()
                        },
                        TransferOp::Read,
                    )
                    .unwrap();
            }
        }
        let stats = *usage.lock().unwrap();
        assert_eq!(
            (
                stats.completed_reads,
                stats.completed_read_bytes,
                stats.work_requests,
                stats.peak_work_requests,
                stats.post_calls
            ),
            (64, 1024, 0, limit, 64usize.div_ceil(limit) as u64)
        );
    }
}

#[test]
fn partial_unknown_and_inconsistent_posts_keep_only_proven_or_possible_debt_until_quiescence() {
    for (code, accepted, uncertain, owed) in [
        (libc::ENOMEM, 0, false, 0),
        (libc::ENOMEM, 1, false, 1),
        (libc::EIO, 0, true, 4),
        (0, 3, false, 4),
        (libc::EIO, 9, false, 4),
    ] {
        let usage = usage();
        let sq = SendQueue::new(4, usage.clone()).unwrap();
        let mut queue = SendWork::new(64, 16, 64, sq.clone(), 1).unwrap();
        queue.reserve(64).unwrap();
        assert!(
            queue
                .posted(
                    code,
                    sys::PostResult {
                        accepted,
                        uncertain
                    }
                )
                .is_err()
        );
        assert_eq!(queue.outstanding(), owed);
        assert_eq!(usage.lock().unwrap().work_requests, owed);
        sq.quiesce();
        queue.quiesce().unwrap();
        sq.quiesce();
        queue.quiesce().unwrap();
        assert_eq!(usage.lock().unwrap().work_requests, 0);
    }
}

#[test]
fn malformed_and_error_completions_never_invent_success_or_release_unidentified_work() {
    for (id, status, opcode, length, owed, errors) in [
        (0, 0, 2, 16, 4, 0),
        (5, 0, 2, 16, 4, 0),
        (1, 0, 1, 16, 4, 0),
        (1, 0, 2, 15, 4, 0),
        (1, 7, 0, 0, 3, 1),
    ] {
        let usage = usage();
        let sq = SendQueue::new(4, usage.clone()).unwrap();
        let mut queue = SendWork::new(64, 16, 64, sq.clone(), 1).unwrap();
        queue.reserve(64).unwrap();
        queue
            .posted(
                0,
                sys::PostResult {
                    accepted: 4,
                    uncertain: false,
                },
            )
            .unwrap();
        assert!(
            queue
                .complete(
                    sys::Completion {
                        id,
                        status,
                        opcode,
                        length,
                        vendor_error: 0,
                        qpn: 0,
                        ..Default::default()
                    },
                    TransferOp::Read
                )
                .is_err()
        );
        let stats = *usage.lock().unwrap();
        assert_eq!(
            (
                stats.completed_reads,
                stats.completed_read_bytes,
                stats.work_requests,
                stats.completion_errors
            ),
            (0, 0, owed, errors)
        );
        sq.quiesce();
        queue.quiesce().unwrap();
    }
    let usage = usage();
    let sq = SendQueue::new(4, usage.clone()).unwrap();
    let mut queue = SendWork::new(64, 16, 64, sq.clone(), 1).unwrap();
    queue.reserve(64).unwrap();
    queue
        .posted(
            0,
            sys::PostResult {
                accepted: 4,
                uncertain: false,
            },
        )
        .unwrap();
    let completion = || sys::Completion {
        id: 1,
        opcode: 2,
        length: 16,
        ..Default::default()
    };
    queue.complete(completion(), TransferOp::Read).unwrap();
    assert!(queue.complete(completion(), TransferOp::Read).is_err());
    assert_eq!(usage.lock().unwrap().work_requests, 3);
    sq.quiesce();
    queue.quiesce().unwrap();
}

#[test]
fn management_cqes_require_their_own_opcode_and_count_no_payload_bytes() {
    for (operation, opcode) in [(Operation::Bind, 5), (Operation::Invalidate, 6)] {
        let usage = usage();
        let sq = SendQueue::new(1, usage.clone()).unwrap();
        let mut queue = SendWork::control(sq, 65).unwrap();
        assert_eq!(queue.reserve(1).unwrap().len(), 1);
        queue
            .posted(
                0,
                sys::PostResult {
                    accepted: 1,
                    uncertain: false,
                },
            )
            .unwrap();
        assert!(
            queue
                .complete(
                    sys::Completion {
                        id: 65,
                        opcode: 2,
                        ..Default::default()
                    },
                    operation
                )
                .is_err()
        );
        assert_eq!(usage.lock().unwrap().work_requests, 1);
        queue
            .complete(
                sys::Completion {
                    id: 65,
                    opcode,
                    ..Default::default()
                },
                operation,
            )
            .unwrap();
        let s = *usage.lock().unwrap();
        assert_eq!(
            (
                s.work_requests,
                s.completed_reads,
                s.completed_writes,
                s.completed_read_bytes,
                s.completed_write_bytes
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(
            s.completed_window_binds + s.completed_window_invalidations,
            1
        );
    }
}

#[test]
fn concurrent_grants_share_physical_qp_depth_and_cq_capacity() {
    use std::sync::Barrier;

    let usage = usage();
    let queues = [
        SendQueue::new(5, usage.clone()).unwrap(),
        SendQueue::new(5, usage.clone()).unwrap(),
    ];
    let barrier = Barrier::new(13);
    let admitted = Mutex::new([0usize; 2]);
    std::thread::scope(|threads| {
        let mut handles = Vec::new();
        for index in 0..12 {
            let sq = queues[index % 2].clone();
            let (barrier, admitted) = (&barrier, &admitted);
            handles.push(threads.spawn(move || {
                let mut work = SendWork::new(64, 16, 4, sq, (index as u64 + 1) * 128 + 1).unwrap();
                barrier.wait();
                let submitted = (|| -> Result<Vec<sys::WorkRequest>> {
                    let requests = match work.reserve(7) {
                        Ok(requests) => requests.to_vec(),
                        Err(Error::ResourceExhausted) => return Ok(Vec::new()),
                        Err(error) => return Err(error),
                    };
                    work.posted(
                        0,
                        sys::PostResult {
                            accepted: requests.len(),
                            uncertain: false,
                        },
                    )?;
                    admitted.lock().unwrap()[index % 2] += requests.len();
                    Ok(requests)
                })();
                barrier.wait();
                barrier.wait();
                for request in submitted?.iter().rev() {
                    work.complete(
                        sys::Completion {
                            id: request.id,
                            opcode: 2,
                            length: request.length,
                            ..Default::default()
                        },
                        TransferOp::Read,
                    )?;
                }
                Ok::<_, Error>(())
            }));
        }
        barrier.wait();
        barrier.wait();
        let counts = *admitted.lock().unwrap();
        let outstanding = usage.lock().unwrap().work_requests;
        // Release the workers before asserting, so a broken limit fails instead of hanging.
        barrier.wait();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        assert!(counts.iter().all(|count| *count <= 5), "{counts:?}");
        assert_eq!(counts.iter().sum::<usize>(), 7);
        assert_eq!(outstanding, 7);
    });
    assert_eq!(usage.lock().unwrap().work_requests, 0);
    assert_eq!(usage.lock().unwrap().peak_work_requests, 7);
}

#[test]
fn sibling_grants_refill_freed_sq_slots_without_sharing_completion_authority() {
    let usage = usage();
    let sq = SendQueue::new(3, usage.clone()).unwrap();
    let mut payload = SendWork::new(32, 16, 2, sq.clone(), 129).unwrap();
    let mut bind = SendWork::control(sq.clone(), 257).unwrap();
    let mut notify = SendWork::control(sq, 385).unwrap();
    assert_eq!(payload.reserve(64).unwrap().len(), 2);
    assert_eq!(bind.reserve(64).unwrap().len(), 1);
    assert!(matches!(notify.reserve(64), Err(Error::ResourceExhausted)));
    // A CQE before the actual post receipt cannot discharge a reservation.
    assert!(
        bind.complete(
            sys::Completion {
                id: 257,
                opcode: 5,
                ..Default::default()
            },
            Operation::Bind
        )
        .is_err()
    );
    assert_eq!(usage.lock().unwrap().work_requests, 3);
    payload
        .posted(
            0,
            sys::PostResult {
                accepted: 2,
                uncertain: false,
            },
        )
        .unwrap();
    bind.posted(
        0,
        sys::PostResult {
            accepted: 1,
            uncertain: false,
        },
    )
    .unwrap();
    assert!(
        bind.posted(
            0,
            sys::PostResult {
                accepted: 1,
                uncertain: false
            }
        )
        .is_err()
    );
    assert!(
        payload
            .complete(
                sys::Completion {
                    id: 257,
                    opcode: 5,
                    ..Default::default()
                },
                Operation::Bind
            )
            .is_err()
    );
    assert_eq!(usage.lock().unwrap().work_requests, 3);
    bind.complete(
        sys::Completion {
            id: 257,
            opcode: 5,
            ..Default::default()
        },
        Operation::Bind,
    )
    .unwrap();
    assert_eq!(notify.reserve(64).unwrap().len(), 1);
    notify
        .posted(
            0,
            sys::PostResult {
                accepted: 1,
                uncertain: false,
            },
        )
        .unwrap();
    for id in [130, 129] {
        payload
            .complete(
                sys::Completion {
                    id,
                    opcode: 2,
                    length: 16,
                    ..Default::default()
                },
                TransferOp::Read,
            )
            .unwrap();
    }
    assert_eq!(usage.lock().unwrap().work_requests, 1);
    notify
        .complete(
            sys::Completion {
                id: 385,
                opcode: 0,
                ..Default::default()
            },
            Operation::Send,
        )
        .unwrap();
    assert!(bind.finished() && payload.finished() && notify.finished());
    let stats = *usage.lock().unwrap();
    assert_eq!(
        (
            stats.work_requests,
            stats.completed_reads,
            stats.completed_window_binds,
            stats.completed_sends
        ),
        (0, 2, 1, 1)
    );
}

#[test]
fn qp_drain_resolves_all_grants_but_drop_or_partial_post_cannot_return_sibling_debt() {
    let usage = usage();
    let sq = SendQueue::new(6, usage.clone()).unwrap();
    let mut first = SendWork::new(32, 16, 2, sq.clone(), 129).unwrap();
    let mut partial = SendWork::new(32, 16, 2, sq.clone(), 257).unwrap();
    let mut uncertain = SendWork::new(32, 16, 2, sq.clone(), 385).unwrap();
    for work in [&mut first, &mut partial, &mut uncertain] {
        assert_eq!(work.reserve(64).unwrap().len(), 2);
    }
    first
        .posted(
            0,
            sys::PostResult {
                accepted: 2,
                uncertain: false,
            },
        )
        .unwrap();
    assert!(
        partial
            .posted(
                libc::ENOMEM,
                sys::PostResult {
                    accepted: 1,
                    uncertain: false
                }
            )
            .is_err()
    );
    assert!(
        uncertain
            .posted(
                libc::EIO,
                sys::PostResult {
                    accepted: 0,
                    uncertain: true
                }
            )
            .is_err()
    );
    assert_eq!(usage.lock().unwrap().work_requests, 5);
    let mut rejected = SendWork::control(sq.clone(), 513).unwrap();
    assert!(matches!(rejected.reserve(64), Err(Error::Unavailable(_))));
    first
        .complete(
            sys::Completion {
                id: 129,
                opcode: 2,
                length: 16,
                ..Default::default()
            },
            TransferOp::Read,
        )
        .unwrap();
    drop(first);
    assert!(partial.quiesce().is_err());
    assert_eq!(usage.lock().unwrap().work_requests, 4);
    assert!(
        partial
            .complete(
                sys::Completion {
                    id: 258,
                    opcode: 2,
                    length: 16,
                    ..Default::default()
                },
                TransferOp::Read
            )
            .is_err()
    );
    assert_eq!(usage.lock().unwrap().work_requests, 4);
    sq.quiesce();
    partial.quiesce().unwrap();
    uncertain.quiesce().unwrap();
    sq.quiesce();
    partial.quiesce().unwrap();
    assert_eq!(usage.lock().unwrap().work_requests, 0);
    let mut next = SendWork::control(sq, 513).unwrap();
    assert!(matches!(next.reserve(64), Err(Error::Unavailable(_))));
}
