use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use orbit_te::{
    EngineConfig, Error, Notification, NotificationEffect, NotificationStatus, Permission,
    TransferEngine, TransferOp, TransferRequest,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn notification(name: &str, message: &str) -> Notification {
    Notification {
        name: name.into(),
        message: message.into(),
    }
}

#[test]
fn local_and_remote_messages_share_one_bounded_draining_inbox() {
    let source = TransferEngine::new(EngineConfig {
        max_pending_notifications: 3,
        max_pending_notification_bytes: 8,
        max_notification_bytes: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    let sender = TransferEngine::new(EngineConfig::default()).unwrap();
    let peer = sender
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let local = notification("名", "");
    let remote = notification("", "é");
    source.send_local_notification(&local).unwrap();
    sender.send_notification(&peer, &remote, TIMEOUT).unwrap();
    sender.send_notification(&peer, &remote, TIMEOUT).unwrap();
    let failure = sender
        .send_notification(&peer, &notification("", ""), TIMEOUT)
        .unwrap_err();
    assert!(matches!(failure.error, Error::ResourceExhausted));
    assert_eq!(failure.effect, NotificationEffect::None);
    let usage = source.stats().unwrap();
    assert_eq!(
        (
            usage.queued_notifications,
            usage.retained_notifications,
            usage.retained_notification_bytes
        ),
        (3, 3, 7)
    );
    assert_eq!(
        source.receive_notifications().unwrap(),
        [local, remote.clone(), remote]
    );
    assert!(source.receive_notifications().unwrap().is_empty());
    let self_peer = source
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    source
        .send_notification(&self_peer, &notification("", "12345678"), TIMEOUT)
        .unwrap();
    let failure = sender
        .send_notification(&peer, &notification("", "x"), TIMEOUT)
        .unwrap_err();
    assert!(matches!(failure.error, Error::ResourceExhausted));
    assert_eq!(failure.effect, NotificationEffect::None);
    source.close_source(TIMEOUT).unwrap();
    assert_eq!(
        source.receive_notifications().unwrap(),
        [notification("", "12345678")]
    );
    let failure = sender
        .send_notification(&peer, &notification("", ""), TIMEOUT)
        .unwrap_err();
    assert!(matches!(failure.error, Error::SourceClosed));
    assert_eq!(failure.effect, NotificationEffect::None);
    assert!(matches!(
        source.send_local_notification(&notification("", "")),
        Err(Error::SourceClosed)
    ));
    assert_eq!(source.stats().unwrap().retained_notification_bytes, 0);
}

#[test]
fn attached_delivery_and_queue_failure_preserve_exact_transfer_results() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let source = TransferEngine::new(EngineConfig {
            max_pending_notifications: 1,
            ..EngineConfig::default()
        })
        .unwrap();
        let exported = source
            .register_memory(
                vec![0x31; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let sender = TransferEngine::new(EngineConfig::default()).unwrap();
        let local = sender
            .register_memory(vec![0xa7; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = sender
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        let request = TransferRequest {
            priority: Default::default(),
            remote_staging: false,
            transport: orbit_te::Transport::Tcp,
            local_address: local.base_address(),
            remote_address: peer.regions()[0].base_address(),
            length: 8,
        };
        let done = notification("scope-1", "complete");
        let report = sender
            .submit_and_notify(
                operation,
                &peer,
                std::slice::from_ref(&request),
                &done,
                TIMEOUT,
            )
            .unwrap();
        assert!(report.is_complete());
        assert!(matches!(report.notification, NotificationStatus::Delivered));
        assert_eq!(
            (report.completed_requests, report.transferred_bytes),
            (1, 8)
        );
        assert_eq!(
            source.receive_notifications().unwrap().as_slice(),
            std::slice::from_ref(&done)
        );
        assert!(source.receive_notifications().unwrap().is_empty());
        let mut bytes = [0; 8];
        if operation == TransferOp::Read {
            local.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [0x31; 8]);
        } else {
            exported.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [0xa7; 8]);
        }
        source
            .send_local_notification(&notification("busy", "existing"))
            .unwrap();
        local.write(0, &[0xcc; 8]).unwrap();
        exported.write(0, &[0x42; 8]).unwrap();
        let report = sender
            .submit_and_notify(
                operation,
                &peer,
                std::slice::from_ref(&request),
                &done,
                TIMEOUT,
            )
            .unwrap();
        assert!(report.is_complete());
        assert_eq!(
            (report.completed_requests, report.transferred_bytes),
            (1, 8)
        );
        match report.notification {
            NotificationStatus::Failed(failure) => {
                assert!(matches!(failure.error, Error::ResourceExhausted));
                assert_eq!(failure.effect, NotificationEffect::None);
            }
            other => panic!("expected separate notification failure: {other:?}"),
        }
        if operation == TransferOp::Read {
            local.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [0x42; 8]);
        } else {
            exported.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [0xcc; 8]);
        }
        assert_eq!(
            source.receive_notifications().unwrap(),
            [notification("busy", "existing")]
        );
        let empty = sender
            .submit_and_notify(operation, &peer, &[], &done, TIMEOUT)
            .unwrap();
        assert!(empty.is_complete());
        assert!(matches!(empty.notification, NotificationStatus::Skipped));
        local.write(0, &[0x55; 8]).unwrap();
        exported.write(0, &[0x66; 8]).unwrap();
        let oversized = notification("", &"x".repeat((64 << 10) + 1));
        assert!(matches!(
            sender.submit_and_notify(
                operation,
                &peer,
                std::slice::from_ref(&request),
                &oversized,
                TIMEOUT
            ),
            Err(Error::ResourceExhausted)
        ));
        if operation == TransferOp::Read {
            local.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [0x55; 8]);
        } else {
            exported.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, [0x66; 8]);
        }
        assert!(source.receive_notifications().unwrap().is_empty());
    }
}

#[test]
fn concurrent_senders_and_receivers_deliver_each_message_once() {
    let source = Arc::new(
        TransferEngine::new(EngineConfig {
            handler_threads: 8,
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let sender = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());
    let peer = sender
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let finished = AtomicBool::new(false);
    let start = Barrier::new(4);
    let messages = thread::scope(|scope| {
        let receivers: Vec<_> = (0..2)
            .map(|_| {
                let source = &source;
                let finished = &finished;
                scope.spawn(move || {
                    let mut received = Vec::new();
                    let end = Instant::now() + TIMEOUT;
                    loop {
                        let part = source.receive_notifications().unwrap();
                        if part.is_empty() && finished.load(Ordering::Acquire) {
                            break;
                        }
                        received.extend(part);
                        assert!(Instant::now() < end, "notification senders did not finish");
                        thread::sleep(Duration::from_millis(1));
                    }
                    received
                })
            })
            .collect();
        let producers: Vec<_> = (0..4)
            .map(|producer| {
                let start = &start;
                let source = &source;
                let sender = &sender;
                let peer = &peer;
                scope.spawn(move || {
                    start.wait();
                    for index in 0..16 {
                        let message = notification("scope", &format!("{producer}-{index}"));
                        if producer % 2 == 0 {
                            source.send_local_notification(&message).unwrap();
                        } else {
                            sender.send_notification(peer, &message, TIMEOUT).unwrap();
                        }
                    }
                })
            })
            .collect();
        for producer in producers {
            producer.join().unwrap();
        }
        finished.store(true, Ordering::Release);
        receivers
            .into_iter()
            .flat_map(|receiver| receiver.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(messages.len(), 64);
    let actual: BTreeSet<_> = messages
        .into_iter()
        .map(|message| (message.name, message.message))
        .collect();
    let expected = (0..4)
        .flat_map(|producer| {
            (0..16).map(move |index| ("scope".into(), format!("{producer}-{index}")))
        })
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(source.stats().unwrap().retained_notifications, 0);
    assert_eq!(sender.stats().unwrap().inflight_bytes, 0);
}
