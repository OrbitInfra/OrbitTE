use super::*;

fn message(value: &str) -> Notification {
    Notification {
        name: String::new(),
        message: value.into(),
    }
}

#[test]
fn reservations_bound_pending_bodies_and_survive_unrelated_drains() {
    let inbox = NotificationInbox::new(2, 6, 6);
    inbox.push(&message("ab")).unwrap();
    let pending = inbox.reserve(1, 3).unwrap();
    assert_eq!(inbox.usage().unwrap(), (1, 2, 6));
    assert!(matches!(inbox.reserve(0, 0), Err(Error::ResourceExhausted)));
    assert_eq!(inbox.receive().unwrap(), [message("ab")]);
    assert_eq!(inbox.usage().unwrap(), (0, 1, 4));
    assert!(matches!(inbox.reserve(0, 3), Err(Error::ResourceExhausted)));
    inbox.push(&message("cd")).unwrap();
    pending
        .commit(Notification {
            name: "n".into(),
            message: "123".into(),
        })
        .unwrap();
    assert_eq!(
        inbox.receive().unwrap(),
        [
            message("cd"),
            Notification {
                name: "n".into(),
                message: "123".into()
            }
        ]
    );
    assert_eq!(inbox.usage().unwrap(), (0, 0, 0));
    assert!(inbox.receive().unwrap().is_empty());
}

#[test]
fn failed_reservations_and_dropped_bodies_restore_exact_capacity() {
    let inbox = NotificationInbox::new(2, 6, 4);
    for (name, message) in [(usize::MAX, 1), (0, 5), (5, 0)] {
        assert!(matches!(
            inbox.reserve(name, message),
            Err(Error::ResourceExhausted)
        ));
        assert_eq!(inbox.usage().unwrap(), (0, 0, 0));
    }
    let first = inbox.reserve(2, 2).unwrap();
    let second = inbox.reserve(0, 2).unwrap();
    drop(first);
    assert_eq!(inbox.usage().unwrap(), (0, 1, 2));
    inbox.push(&message("1234")).unwrap();
    drop(second);
    assert_eq!(inbox.usage().unwrap(), (1, 1, 4));
    assert_eq!(inbox.receive().unwrap(), [message("1234")]);
    assert_eq!(inbox.usage().unwrap(), (0, 0, 0));
}
