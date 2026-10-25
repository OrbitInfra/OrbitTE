use super::*;

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
    let grant_credit = first.take().unwrap();
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
        (1, 0, 7, 1)
    );
    drop(grant_credit);
    let stats = *usage.lock().unwrap();
    assert_eq!((stats.receive_credits, stats.receive_work_requests), (0, 0));
    let mut all = ReceiveCredits::acquire(usage, 7, 7).unwrap();
    for _ in 0..7 {
        drop(all.take().unwrap());
    }
    assert!(all.take().is_err());
}
