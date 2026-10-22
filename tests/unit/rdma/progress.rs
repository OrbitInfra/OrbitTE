use super::*;

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
        (32 << 20, 64 << 10, 64, vec![512 << 10; 64]),
        (32 << 20, 64 << 10, 32, vec![1 << 20; 32]),
    ] {
        let queue = SendQueue::new(length, base, cap, usage()).unwrap();
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
        let mut queue = SendQueue::new(1024, 16, 64, usage.clone()).unwrap();
        while !queue.finished() {
            let requests = queue.reserve(limit).unwrap().to_vec();
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
        let mut queue = SendQueue::new(64, 16, 64, usage.clone()).unwrap();
        queue.reserve(4).unwrap();
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
        queue.quiesce();
        queue.quiesce();
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
        let mut queue = SendQueue::new(64, 16, 64, usage.clone()).unwrap();
        queue.reserve(4).unwrap();
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
                        vendor_error: 0
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
        queue.quiesce();
    }
    let usage = usage();
    let mut queue = SendQueue::new(64, 16, 64, usage.clone()).unwrap();
    queue.reserve(4).unwrap();
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
    queue.quiesce();
}
