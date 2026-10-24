use super::*;
use std::time::Duration;

fn queue(usage: Arc<Mutex<RdmaStats>>) -> ReceiveQueue {
    ReceiveQueue::new(
        129,
        [0x42; COMPLETION_MESSAGE_SIZE],
        0x0102_0304,
        Instant::now() + Duration::from_secs(1),
        ReceiveCredits::acquire(usage, 2, 1).unwrap(),
    )
}

fn completion() -> sys::Completion {
    sys::Completion {
        id: 129,
        opcode: 128,
        length: COMPLETION_MESSAGE_SIZE as u32,
        flags: sys::WITH_INVALIDATE,
        invalidated_rkey: 0x0102_0304,
        ..Default::default()
    }
}

#[test]
fn receive_identity_flags_length_and_host_order_key_are_exact_and_never_pay_foreign_debt() {
    for case in 0..9 {
        let usage = Arc::new(Mutex::new(RdmaStats::default()));
        let mut queue = queue(Arc::clone(&usage));
        queue.reserve(1).unwrap();
        queue
            .posted(
                0,
                sys::PostResult {
                    accepted: 1,
                    uncertain: false,
                },
            )
            .unwrap();
        let mut wc = completion();
        match case {
            0 => {}
            1 => wc.id += 128,
            2 => wc.opcode = 129,
            3 => wc.length -= 1,
            4 => wc.flags = 0,
            5 => wc.flags |= 2,
            6 => wc.flags |= 0x8000_0000,
            7 => wc.invalidated_rkey = 0x0403_0201,
            8 => wc.status = 5,
            _ => unreachable!(),
        }
        let result = queue.complete(wc);
        assert_eq!(result.is_ok(), case == 0, "case {case}");
        assert_eq!(queue.pending(), case != 0 && case != 8, "case {case}");
        if case == 0 {
            assert!(queue.complete(completion()).is_err());
        }
        queue.quiesce();
        assert_eq!(usage.lock().unwrap().receive_work_requests, 0);
    }
}

#[test]
fn receive_capacity_is_independent_from_send_and_uncertain_post_keeps_its_charge() {
    let usage = Arc::new(Mutex::new(RdmaStats {
        work_requests: 1,
        ..Default::default()
    }));
    let mut first = queue(Arc::clone(&usage));
    let mut next = queue(Arc::clone(&usage));
    first.reserve(1).unwrap();
    assert!(matches!(next.reserve(1), Err(Error::ResourceExhausted)));
    assert!(
        first
            .posted(
                libc::EIO,
                sys::PostResult {
                    accepted: 0,
                    uncertain: true
                }
            )
            .is_err()
    );
    assert!(first.pending());
    assert_eq!(
        (usage.lock().unwrap().work_requests, first.pending()),
        (1, true)
    );
    first.quiesce();
    next.reserve(1).unwrap();
    assert!(
        next.posted(libc::ENOMEM, sys::PostResult::default())
            .is_err()
    );
    assert!(!next.pending());
    assert_eq!(usage.lock().unwrap().receive_work_requests, 0);
}

#[test]
fn control_bytes_have_their_own_checked_budget_before_any_native_registration() {
    let usage = Arc::new(Mutex::new(RdmaStats::default()));
    let first = MessageBuffer::new(Arc::clone(&usage), 1).unwrap();
    assert!(matches!(
        MessageBuffer::new(Arc::clone(&usage), 1),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(usage.lock().unwrap().message_bytes, COMPLETION_MESSAGE_SIZE);
    drop(first);
    assert_eq!(usage.lock().unwrap().message_bytes, 0);
    assert!(matches!(
        MessageBuffer::new(Arc::clone(&usage), usize::MAX),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(usage.lock().unwrap().message_bytes, 0);
}

#[test]
fn whole_parent_receive_admission_is_atomic_and_unused_credits_roll_back() {
    let usage = Arc::new(Mutex::new(RdmaStats::default()));
    let mut first = ReceiveCredits::acquire(Arc::clone(&usage), 7, 3).unwrap();
    let second = ReceiveCredits::acquire(Arc::clone(&usage), 7, 3).unwrap();
    assert!(matches!(
        ReceiveCredits::acquire(Arc::clone(&usage), 7, 3),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(usage.lock().unwrap().receive_credits, 6);
    let single = ReceiveCredits::acquire(Arc::clone(&usage), 7, 1).unwrap();
    assert!(matches!(
        ReceiveCredits::acquire(Arc::clone(&usage), 7, 0),
        Err(Error::InvalidRange)
    ));
    let mut queue = ReceiveQueue::new(
        129,
        [0x42; COMPLETION_MESSAGE_SIZE],
        0x0102_0304,
        Instant::now() + Duration::from_secs(1),
        first.take().unwrap(),
    );
    queue.reserve(7).unwrap();
    assert!(
        queue
            .posted(
                libc::EIO,
                sys::PostResult {
                    accepted: 0,
                    uncertain: true
                }
            )
            .is_err()
    );
    drop(first);
    drop(second);
    drop(single);
    let stats = *usage.lock().unwrap();
    assert_eq!(
        (
            stats.receive_credits,
            stats.receive_work_requests,
            stats.peak_receive_credits,
            stats.receive_admission_rejections
        ),
        (1, 1, 7, 1)
    );
    assert!(
        queue.credits.is_some(),
        "possibly posted receives retain their own admission credit"
    );
    queue.quiesce();
    let stats = *usage.lock().unwrap();
    assert_eq!((stats.receive_credits, stats.receive_work_requests), (0, 0));
    let mut all = ReceiveCredits::acquire(usage, 7, 7).unwrap();
    for _ in 0..7 {
        drop(all.take().unwrap());
    }
    assert!(all.take().is_err());
}
