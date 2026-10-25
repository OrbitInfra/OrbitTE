use super::*;

#[test]
fn parent_reservations_survive_children_and_retry_without_overbooking() {
    let usage = Arc::new(Mutex::new(RdmaStats::default()));
    let parent = GrantCredits::acquire(Arc::clone(&usage), 3, 2).unwrap();
    assert!(GrantCredits::acquire(Arc::clone(&usage), 3, 2).is_err());
    let first = parent.take().unwrap();
    let second = parent.take().unwrap();
    assert!(parent.take().is_err());
    drop(first);
    let retry = parent.take().unwrap();
    drop((parent, second));
    assert_eq!(
        usage.lock().unwrap().grant_credits,
        2,
        "retained child keeps its parent reservation"
    );
    let other = GrantCredits::acquire(Arc::clone(&usage), 3, 1).unwrap();
    assert!(GrantCredits::acquire(Arc::clone(&usage), 3, 1).is_err());
    drop(retry);
    assert_eq!(usage.lock().unwrap().grant_credits, 1);
    drop(other);
    assert_eq!(usage.lock().unwrap().grant_credits, 0);
}
