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

struct Unimplemented;
impl NotificationChannel for Unimplemented {}

struct Leg {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    behavior: &'static str,
    inbox: Option<TransferEngine>,
}
impl NotificationChannel for Leg {
    fn send(
        &self,
        peer: Target,
        notification: &Notification,
        deadline: Instant,
    ) -> Option<Delivery> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(match self.behavior {
            "rpc" => control_plane::notify(Operation::NotifyRpc, peer, notification, deadline),
            "invalid" => Err(Error::InvalidRange.into()),
            "unknown" => Err(NotificationFailure {
                error: Error::Unavailable("lost receipt"),
                effect: NotificationEffect::Unknown,
            }),
            _ => Err(Error::Unavailable(self.behavior).into()),
        })
    }
    fn drain(&self, received: &mut Vec<Notification>) -> Result<()> {
        if let Some(engine) = &self.inbox {
            received.extend(engine.receive_notifications()?);
            Ok(())
        } else {
            Err(Error::Unavailable("broken inbox"))
        }
    }
}

fn routed_engine(tcp: bool, fallback: bool) -> TransferEngine {
    TransferEngine::new(EngineConfig {
        tcp_notifications: tcp,
        notification_rpc_fallback: fallback,
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn notification_routes_try_only_unposted_unavailable_channels() {
    use std::sync::atomic::Ordering::SeqCst;
    let remote = routed_engine(false, false);
    let destination = remote.local_target();
    let until = || Instant::now() + crate::test_support::EVENTUAL_PROGRESS;
    let note = message("routes");
    for (behavior, fallback, expected) in [
        ("unavailable", true, true),
        ("invalid", true, false),
        ("unknown", true, false),
        ("unavailable", false, false),
    ] {
        let mut routes = NotificationRoutes::new(&EngineConfig {
            tcp_notifications: false,
            notification_rpc_fallback: fallback,
            ..Default::default()
        });
        routes.channels.push(Box::new(Unimplemented));
        let first = Arc::default();
        let second = Arc::default();
        routes.channels.push(Box::new(Leg {
            calls: Arc::clone(&first),
            behavior,
            inbox: None,
        }));
        routes.channels.push(Box::new(Leg {
            calls: Arc::clone(&second),
            behavior: "rpc",
            inbox: None,
        }));
        let result = routes.send(destination, &note, until());
        assert_eq!(first.load(SeqCst), 1);
        assert_eq!(second.load(SeqCst), usize::from(expected));
        match behavior {
            "invalid" => assert!(matches!(
                result.unwrap_err().outcome.error,
                Error::InvalidRange
            )),
            "unknown" => assert_eq!(
                result.unwrap_err().outcome.effect,
                NotificationEffect::Unknown
            ),
            _ => assert_eq!(result.is_ok(), expected),
        }
        assert_eq!(
            remote.receive_notifications().unwrap().len(),
            usize::from(expected)
        );
    }
    // No TCP channel installed on the receiver. A definite TCP rejection
    // reaches the explicit RPC. With neither installed, RPC still delivers.
    for tcp in [false, true] {
        let sender = routed_engine(tcp, true);
        sender.notify_target(destination, &note, until()).unwrap();
        assert_eq!(
            remote.receive_notifications().unwrap(),
            std::slice::from_ref(&note)
        );
    }
    let mut routes = NotificationRoutes::new(&EngineConfig {
        tcp_notifications: false,
        ..Default::default()
    });
    routes.channels.push(Box::new(Leg {
        calls: Arc::default(),
        behavior: "first",
        inbox: None,
    }));
    routes.channels.push(Box::new(Leg {
        calls: Arc::default(),
        behavior: "last",
        inbox: None,
    }));
    let stale = Target {
        epoch: SourceEpoch::generate().unwrap(),
        ..destination
    };
    let rejected = routes.send(stale, &note, until()).unwrap_err();
    assert!(matches!(rejected.outcome.error, Error::Unavailable("last")));
    assert_eq!(rejected.outcome.effect, NotificationEffect::None);
    assert!(!rejected.retryable);
    // RPC-only unreachable peers preserve the original socket error.
    drop(remote);
    routes.channels.clear();
    // Keep the port bound but non-listening. A freed ephemeral port could be
    // reused by a parallel test, or its old listener inherited until exec.
    let refused = tokio::net::TcpSocket::new_v4().unwrap();
    refused.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let destination = Target {
        endpoint: refused.local_addr().unwrap(),
        ..destination
    };
    assert!(matches!(
        routes
            .send(destination, &note, until())
            .unwrap_err()
            .outcome
            .error,
        Error::Io(_)
    ));
}

#[test]
fn notification_receive_keeps_draining_after_a_channel_fails() {
    let mut receiver = routed_engine(true, true);
    let sender = routed_engine(true, true);
    let add = |routes: &mut NotificationRoutes, value| {
        let target = routed_engine(true, true);
        sender
            .notify_target(
                target.local_target(),
                &message(value),
                Instant::now() + crate::test_support::EVENTUAL_PROGRESS,
            )
            .unwrap();
        routes.channels.push(Box::new(Leg {
            calls: Arc::default(),
            behavior: "rpc",
            inbox: Some(target),
        }));
    };
    add(&mut receiver.notification_routes, "one");
    receiver.notification_routes.channels.push(Box::new(Leg {
        calls: Arc::default(),
        behavior: "broken",
        inbox: None,
    }));
    add(&mut receiver.notification_routes, "two");
    receiver.send_local_notification(&message("self")).unwrap();
    assert_eq!(
        receiver.receive_notifications().unwrap(),
        [message("one"), message("two"), message("self")]
    );
    assert!(matches!(
        receiver.receive_notifications(),
        Err(Error::Unavailable("broken inbox"))
    ));
}

#[test]
fn attached_targets_retry_only_missing_peers_and_keep_self_deliverable() {
    use crate::{Permission, TransferOp, TransferRequest, Transport};
    let timeout = crate::test_support::EVENTUAL_PROGRESS;
    let requester = Arc::new(routed_engine(false, true));
    let healthy = routed_engine(false, true);
    let blocked = TransferEngine::new(EngineConfig {
        max_pending_notifications: 1,
        ..Default::default()
    })
    .unwrap();
    blocked.send_local_notification(&message("filler")).unwrap();
    let data = healthy
        .register_memory(vec![27].into(), Permission::RemoteRead)
        .unwrap();
    let local = requester
        .register_memory(vec![0].into(), Permission::LocalOnly)
        .unwrap();
    let resolve = |e: &TransferEngine| {
        requester
            .open_segment(e.endpoint(), e.source_epoch(), timeout)
            .unwrap()
    };
    let peer = resolve(&healthy);
    let note = message("done");
    let req = [TransferRequest::new(
        Transport::Tcp,
        local.base_address(),
        data.base_address(),
        1,
    )];
    let mut batch = requester.allocate_batch(1).unwrap();
    batch
        .submit_with_notification_targets(
            TransferOp::Read,
            &peer,
            &req,
            &note,
            &[
                resolve(&blocked),
                peer.clone(),
                resolve(&requester),
                peer.clone(),
            ],
            timeout,
        )
        .unwrap();
    batch.seal().unwrap();
    assert_eq!(batch.wait(timeout).unwrap().completed_tasks, 1);
    assert!(matches!(
        *batch.submissions().unwrap()[0].notification,
        NotificationStatus::Failed(_)
    ));
    assert_eq!(
        healthy.receive_notifications().unwrap(),
        std::slice::from_ref(&note)
    );
    assert_eq!(
        requester.receive_notifications().unwrap(),
        std::slice::from_ref(&note)
    );
    assert_eq!(
        blocked.receive_notifications().unwrap(),
        [message("filler")]
    );
    // Observation doesn't attempt the failed target even after it becomes eligible.
    for _ in 0..16 {
        assert!(batch.status().unwrap().is_terminal());
    }
    assert!(blocked.receive_notifications().unwrap().is_empty());
    let id = batch.id().unwrap();
    requester.progress_batch(id).unwrap();
    let until = Instant::now() + timeout;
    while !matches!(
        *batch.submissions().unwrap()[0].notification,
        NotificationStatus::Delivered
    ) {
        assert!(Instant::now() < until);
        std::thread::yield_now();
    }
    for _ in 0..16 {
        requester.progress_batch(id).unwrap();
    }
    assert_eq!(blocked.receive_notifications().unwrap(), [note]);
    assert!(healthy.receive_notifications().unwrap().is_empty());
    assert!(requester.receive_notifications().unwrap().is_empty());
    let mut byte = [0];
    local.read(0, &mut byte).unwrap();
    assert_eq!(byte, [27]);
    batch.free().unwrap();
}

#[test]
fn hook_self_delivers_while_the_remote_peer_withholds_its_receipt() {
    use std::{io::Read, net::TcpListener};
    let engine = Arc::new(routed_engine(false, true));
    let timeout = crate::test_support::EVENTUAL_PROGRESS;
    let own = engine
        .open_segment(engine.endpoint(), engine.source_epoch(), timeout)
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut remote = own.clone();
    remote.endpoint = listener.local_addr().unwrap();
    engine.bind_test_peer(&mut remote).unwrap();
    let mut hook = engine
        .notification_routes
        .hook(engine.local_target(), &[remote, own], 1)
        .unwrap();
    let (reached, wait) = std::sync::mpsc::sync_channel(0);
    let (release, hold) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut header = [0; control_plane::REQUEST_SIZE];
            stream.read_exact(&mut header).unwrap();
            reached.send(()).unwrap();
            hold.recv_timeout(timeout).unwrap();
        });
        let attempt =
            scope.spawn(|| hook.attempt(&engine, &message("x"), Instant::now() + timeout));
        wait.recv_timeout(timeout).unwrap();
        assert_eq!(engine.receive_notifications().unwrap(), [message("x")]);
        release.send(()).unwrap();
        assert!(matches!(
            *attempt.join().unwrap(),
            NotificationStatus::Failed(_)
        ));
    });
}

#[test]
fn self_needs_no_notification_channel_and_hook_capacity_is_returned() {
    let engine = TransferEngine::new(EngineConfig {
        tcp_notifications: false,
        notification_rpc_fallback: false,
        max_pending_notifications: 1,
        max_pending_notification_bytes: 3,
        max_notification_bytes: 3,
        ..Default::default()
    })
    .unwrap();
    let timeout = crate::test_support::EVENTUAL_PROGRESS;
    let own = engine
        .open_segment(engine.endpoint(), engine.source_epoch(), timeout)
        .unwrap();
    engine
        .send_notification(&own, &message("abc"), timeout)
        .unwrap();
    assert_eq!(engine.receive_notifications().unwrap(), [message("abc")]);
    let reserve = || {
        engine
            .notification_routes
            .hook(engine.local_target(), std::slice::from_ref(&own), 3)
    };
    let retained = reserve().unwrap();
    assert!(matches!(reserve(), Err(Error::ResourceExhausted)));
    drop(retained);
    assert!(reserve().is_ok());
}
