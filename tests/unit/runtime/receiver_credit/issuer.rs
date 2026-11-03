use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use super::super::{CreditKey, SenderCreditLedger};
use super::*;
use crate::{
    BatchState, EngineConfig, Permission, RemoteSegment, RuntimeQueueConfig, TransferEngine,
    TransferOp, TransferRequest, Transport,
};

const TIMEOUT: Duration = Duration::from_secs(3);

fn config() -> ReceiverCreditConfig {
    ReceiverCreditConfig {
        max_sessions: 3,
        max_bytes: 8,
        max_requests: 4,
        bytes_per_sender: 4,
        requests_per_sender: 2,
        ..ReceiverCreditConfig::default()
    }
}

fn source(config: ReceiverCreditConfig) -> TransferEngine {
    TransferEngine::new(EngineConfig {
        receiver_credit: Some(config),
        ..EngineConfig::default()
    })
    .unwrap()
}

fn report(sender: u8) -> CreditReport {
    CreditReport {
        sender: SourceEpoch([sender; 16]),
        epoch: 0,
        qos_class: 7,
        committed: [0; 2],
        completed: [0; 2],
    }
}

#[test]
fn issuer_windows_are_conserved_across_cumulative_retry_close_and_late_open() {
    let issuer = ReceiverCreditIssuer::new(config());
    let mut a = report(1);
    let first = issuer.synchronize(a, false, true).unwrap();
    assert_eq!(issuer.synchronize(a, false, true).unwrap(), first);
    a.epoch = first.epoch;
    let mut b = report(2);
    b.epoch = issuer.synchronize(b, false, true).unwrap().epoch;
    assert!(matches!(
        issuer.synchronize(report(3), false, true),
        Err(Error::ResourceExhausted)
    ));
    for invalid in [
        CreditReport {
            completed: [1, 0],
            ..a
        },
        CreditReport {
            committed: [5, 0],
            ..a
        },
        CreditReport { qos_class: 8, ..a },
        CreditReport {
            epoch: a.epoch + 100,
            ..a
        },
        CreditReport {
            epoch: 0,
            committed: [1, 0],
            ..a
        },
    ] {
        assert!(issuer.synchronize(invalid, false, true).is_err());
        assert_eq!(issuer.synchronize(a, false, true).unwrap(), first);
    }
    a.committed = [4, 2];
    a.completed = [4, 2];
    let renewed = issuer.synchronize(a, false, true).unwrap();
    assert_eq!((renewed.grants, renewed.sequence), ([8, 4], 2));
    assert_eq!(issuer.synchronize(a, false, true).unwrap(), renewed);
    assert_eq!(
        issuer.synchronize(report(1), false, false).unwrap(),
        renewed
    );
    assert!(matches!(
        issuer.synchronize(
            CreditReport {
                committed: [4, 2],
                ..b
            },
            true,
            false
        ),
        Err(Error::BatchBusy)
    ));
    let closed = issuer.synchronize(a, true, false).unwrap();
    assert_eq!(issuer.synchronize(a, true, false).unwrap(), closed);
    assert!(matches!(
        issuer.synchronize(report(1), false, true),
        Err(Error::Cancelled)
    ));
    assert_eq!(
        issuer.stats(),
        ReceiverCreditIssuerStats {
            active_senders: 1,
            retired_senders: 1,
            reserved_bytes: 4,
            reserved_requests: 2,
            ..ReceiverCreditIssuerStats::default()
        }
    );
    assert!(matches!(
        issuer.synchronize(report(3), false, false),
        Err(Error::SourceClosed)
    ));
    issuer.synchronize(report(3), false, true).unwrap();
    issuer.synchronize(b, true, false).unwrap();
    assert!(
        matches!(
            issuer.synchronize(report(4), false, true),
            Err(Error::ResourceExhausted)
        ),
        "tombstones remain bounded and cannot be evicted for a delayed open"
    );

    let near_overflow = ReceiverCreditIssuer::new(ReceiverCreditConfig {
        max_bytes: u64::MAX,
        bytes_per_sender: u64::MAX,
        ..config()
    });
    let initial = near_overflow.synchronize(report(1), false, true).unwrap();
    let over = CreditReport {
        epoch: initial.epoch,
        committed: [1, 1],
        completed: [1, 1],
        ..report(1)
    };
    assert!(matches!(
        near_overflow.synchronize(over, false, true),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(
        near_overflow.synchronize(report(1), false, true).unwrap(),
        initial
    );
}

fn request(local: u64, remote: u64, length: usize) -> TransferRequest {
    TransferRequest::new(Transport::Tcp, local, remote, length)
}

#[test]
fn public_auto_windows_wait_for_actual_drop_and_replenish_without_per_request_sync() {
    for queued in [false, true] {
        let source = source(ReceiverCreditConfig {
            bytes_per_sender: 2,
            requests_per_sender: 2,
            ..config()
        });
        let requester = Arc::new(
            TransferEngine::new(EngineConfig {
                max_receiver_credit_sessions: 2,
                runtime_queue: queued.then_some(RuntimeQueueConfig {
                    max_dispatch_owners: 2,
                    max_dispatch_bytes: 8,
                    deadline_aware: true,
                }),
                ..EngineConfig::default()
            })
            .unwrap(),
        );
        let remote = source
            .register_memory(vec![0; 4].into_boxed_slice(), Permission::RemoteWrite)
            .unwrap();
        let local = requester
            .register_memory(vec![42; 4].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        requester
            .connect_receiver_credit(&peer, 7, TIMEOUT)
            .unwrap();
        requester
            .connect_receiver_credit(&peer, 7, TIMEOUT)
            .unwrap();
        assert_eq!(
            source
                .receiver_credit_issuer_stats()
                .unwrap()
                .active_senders,
            1
        );
        assert!(matches!(
            requester.activate_receiver_credit(&peer, 99, 7),
            Err(Error::InvalidConfig(_))
        ));
        assert!(matches!(
            requester.deactivate_receiver_credit(&peer, 1),
            Err(Error::InvalidConfig(_))
        ));
        let make =
            |offset, length| request(local.base_address(), remote.base_address() + offset, length);
        assert!(matches!(
            requester.submit_async(TransferOp::Write, &peer, &[make(0, 3)], None, TIMEOUT),
            Err(Error::ResourceExhausted)
        ));
        let held = requester
            .prepare(
                TransferOp::Write,
                &peer,
                &[make(0, 1), make(1, 1)],
                None,
                TIMEOUT,
                None,
            )
            .unwrap();
        assert!(requester.execute(&held, None).is_complete());
        assert_eq!(
            requester
                .receiver_credit_stats(&peer)
                .unwrap()
                .completed_requests,
            0
        );
        let mut waiting = requester
            .submit_async(TransferOp::Write, &peer, &[make(2, 1)], None, TIMEOUT)
            .unwrap();
        assert_eq!(waiting.status().unwrap().state, BatchState::Pending);
        assert_eq!(requester.receiver_credit_stats(&peer).unwrap().sequence, 1);
        drop(held);
        assert_eq!(waiting.wait(TIMEOUT).unwrap().state, BatchState::Completed);
        waiting.free().unwrap();
        assert_eq!(
            requester
                .receiver_credit_stats(&peer)
                .unwrap()
                .completed_requests,
            3
        );
        assert_eq!(requester.receiver_credit_stats(&peer).unwrap().sequence, 2);
        assert!(
            requester
                .submit_and_wait(TransferOp::Write, &peer, &[make(3, 1)], TIMEOUT)
                .unwrap()
                .is_complete()
        );
        assert_eq!(
            requester.receiver_credit_stats(&peer).unwrap().sequence,
            2,
            "remaining window avoids another control exchange"
        );
        assert!(
            requester
                .submit_and_wait(TransferOp::Write, &peer, &[make(0, 1), make(1, 1)], TIMEOUT)
                .unwrap()
                .is_complete()
        );
        let stats = requester.receiver_credit_stats(&peer).unwrap();
        assert_eq!(
            (
                stats.sequence,
                stats.completed_requests,
                stats.consumed_requests,
                stats.active_owners
            ),
            (3, 6, 6, 0)
        );
        let mut bytes = [0; 4];
        remote.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [42; 4]);
        source.close_source(TIMEOUT).unwrap();
        requester
            .disconnect_receiver_credit(&peer, TIMEOUT)
            .unwrap();
        requester
            .disconnect_receiver_credit(&peer, TIMEOUT)
            .unwrap();
        assert_eq!(
            source
                .receiver_credit_issuer_stats()
                .unwrap()
                .reserved_bytes,
            0
        );
        assert!(matches!(
            requester.connect_receiver_credit(&peer, 7, TIMEOUT),
            Err(Error::Cancelled)
        ));
    }
}

// A bounded control-only relay lets the real issuer commit before its reply is
// lost or delayed. It never implements a second credit ledger or data transport.
struct Relay {
    peer: RemoteSegment,
    received: Arc<AtomicUsize>,
    hold: Arc<AtomicBool>,
    lose: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Relay {
    fn new(source: &TransferEngine) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let peer = RemoteSegment {
            metadata: Default::default(),
            endpoint: listener.local_addr().unwrap(),
            epoch: source.source_epoch(),
            regions: Vec::new(),
        };
        let endpoint = source.endpoint();
        let received = Arc::new(AtomicUsize::new(0));
        let hold = Arc::new(AtomicBool::new(false));
        let lose = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let (received, hold, lose, stop) = (
                Arc::clone(&received),
                Arc::clone(&hold),
                Arc::clone(&lose),
                Arc::clone(&stop),
            );
            thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let mut incoming = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => panic!("credit relay accept: {error}"),
                    };
                    incoming.set_read_timeout(Some(TIMEOUT)).unwrap();
                    incoming.set_write_timeout(Some(TIMEOUT)).unwrap();
                    let mut bytes = [0; 52 + 64];
                    if incoming.read_exact(&mut bytes).is_err() {
                        continue;
                    }
                    let mut upstream = TcpStream::connect_timeout(&endpoint, TIMEOUT).unwrap();
                    upstream.set_read_timeout(Some(TIMEOUT)).unwrap();
                    upstream.write_all(&bytes).unwrap();
                    let mut reply = [0; 36];
                    upstream.read_exact(&mut reply).unwrap();
                    let length = u64::from_be_bytes(reply[28..36].try_into().unwrap()) as usize;
                    assert!(length <= 72);
                    let mut payload = vec![0; length];
                    upstream.read_exact(&mut payload).unwrap();
                    received.fetch_add(1, Ordering::Release);
                    while hold.load(Ordering::Acquire) && !stop.load(Ordering::Acquire) {
                        thread::sleep(Duration::from_millis(1));
                    }
                    if lose.swap(false, Ordering::AcqRel) {
                        continue;
                    }
                    if incoming.write_all(&reply).is_ok() {
                        let _ = incoming.write_all(&payload);
                    }
                }
            })
        };
        Self {
            peer,
            received,
            hold,
            lose,
            stop,
            worker: Some(worker),
        }
    }

    fn until_received(&self, count: usize) {
        let deadline = Instant::now() + crate::test_support::EVENTUAL_PROGRESS;
        while self.received.load(Ordering::Acquire) < count {
            assert!(Instant::now() < deadline, "credit RPC was not received");
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.hold.store(false, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[test]
fn lost_open_and_close_replies_preserve_identity_and_sync_cancel_is_retryable() {
    let source = source(config());
    let relay = Relay::new(&source);
    let ledger = SenderCreditLedger::new(1, None);
    let connect = || {
        ledger.connect_automatic(
            &relay.peer,
            SourceEpoch([9; 16]),
            7,
            Instant::now() + TIMEOUT,
        )
    };
    relay.lose.store(true, Ordering::Release);
    assert!(connect().is_err());
    assert_eq!(ledger.stats(&relay.peer).unwrap().epoch, 0);
    let alias = RemoteSegment {
        metadata: Default::default(),
        endpoint: source.endpoint(),
        ..relay.peer.clone()
    };
    assert!(matches!(
        ledger.connect_automatic(&alias, SourceEpoch([9; 16]), 7, Instant::now() + TIMEOUT),
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        ledger.activate(&alias, 1, 7),
        Err(Error::InvalidConfig(_))
    ));

    assert_eq!(
        source
            .receiver_credit_issuer_stats()
            .unwrap()
            .active_senders,
        1
    );
    assert!(matches!(
        ledger.activate(&relay.peer, 1, 7),
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        ledger.disconnect_automatic(&relay.peer, Instant::now() + TIMEOUT),
        Err(Error::Unavailable(_))
    ));
    connect().unwrap();
    assert_eq!(ledger.stats(&relay.peer).unwrap().epoch, 1);
    assert!(matches!(
        ledger.apply_update(
            &relay.peer,
            &super::super::ReceiverCreditUpdateV1 {
                schema_version: 1,
                flags: 0,
                receiver_session_id: relay.peer.source_epoch(),
                qos_class: 7,
                epoch: 1,
                sequence: 2,
                freshness_ttl_ms: 0,
                grants: Vec::new(),
            }
        ),
        Err(Error::InvalidConfig(_))
    ));

    let done = ledger.prepare(&relay.peer, 4, 2).unwrap();
    assert!(done.reservation.try_reserve().unwrap());
    done.reservation.commit().unwrap();
    done.reservation.complete();
    drop(done);
    let waiting = ledger.prepare(&relay.peer, 4, 2).unwrap();
    relay.hold.store(true, Ordering::Release);
    let cancel = Arc::new(AtomicBool::new(false));
    let worker = {
        let reservation = Arc::clone(&waiting.reservation);
        let cancel = Arc::clone(&cancel);
        thread::spawn(move || {
            reservation.wait(Instant::now() + TIMEOUT, || cancel.load(Ordering::Acquire))
        })
    };
    relay.until_received(3);
    let follower = ledger.prepare(&relay.peer, 1, 1).unwrap();
    assert!(
        !follower.reservation.needs_sync().unwrap(),
        "one RPC per peer at a time"
    );
    cancel.store(true, Ordering::Release);
    ledger.wake();
    assert!(matches!(worker.join().unwrap(), Err(Error::Cancelled)));
    drop((waiting, follower));
    relay.hold.store(false, Ordering::Release);
    let retry = ledger.prepare(&relay.peer, 4, 2).unwrap();
    retry
        .reservation
        .wait(Instant::now() + TIMEOUT, || false)
        .unwrap();
    assert_eq!(ledger.stats(&relay.peer).unwrap().consumed_requests, 4);
    drop(retry); // Unstarted reservation rolls back; committed totals remain 2.
    let entry = ledger.lock();
    assert_eq!(
        entry.entries[&CreditKey::from(&relay.peer)].committed,
        [4, 2]
    );
    drop(entry);
    relay.lose.store(true, Ordering::Release);
    assert!(
        ledger
            .disconnect_automatic(&relay.peer, Instant::now() + TIMEOUT)
            .is_err()
    );
    assert_eq!(
        source
            .receiver_credit_issuer_stats()
            .unwrap()
            .reserved_bytes,
        0
    );
    ledger
        .disconnect_automatic(&relay.peer, Instant::now() + TIMEOUT)
        .unwrap();
    assert!(matches!(connect(), Err(Error::Cancelled)));

    let requester = TransferEngine::new(EngineConfig {
        max_receiver_credit_sessions: 1,
        ..EngineConfig::default()
    })
    .unwrap();
    relay.lose.store(true, Ordering::Release);
    assert!(
        requester
            .connect_receiver_credit(&relay.peer, 7, TIMEOUT)
            .is_err()
    );
    requester.close_requester(TIMEOUT).unwrap();
    requester
        .connect_receiver_credit(&relay.peer, 7, TIMEOUT)
        .unwrap();
    requester
        .disconnect_receiver_credit(&relay.peer, TIMEOUT)
        .unwrap();
    assert_eq!(
        source
            .receiver_credit_issuer_stats()
            .unwrap()
            .reserved_bytes,
        0
    );
}

#[test]
fn completion_during_single_flight_is_resynchronized_before_waiting() {
    let source = source(config());
    let relay = Relay::new(&source);
    let ledger = SenderCreditLedger::new(1, None);
    ledger
        .connect_automatic(
            &relay.peer,
            SourceEpoch([8; 16]),
            7,
            Instant::now() + TIMEOUT,
        )
        .unwrap();
    let a = ledger.prepare(&relay.peer, 2, 1).unwrap();
    let b = ledger.prepare(&relay.peer, 2, 1).unwrap();
    for owner in [&a, &b] {
        assert!(owner.reservation.try_reserve().unwrap());
        owner.reservation.commit().unwrap();
    }
    a.reservation.complete();
    drop(a);
    let waiting = ledger.prepare(&relay.peer, 4, 2).unwrap();
    relay.hold.store(true, Ordering::Release);
    let worker = {
        let reservation = Arc::clone(&waiting.reservation);
        thread::spawn(move || reservation.wait(Instant::now() + TIMEOUT, || false))
    };
    relay.until_received(2);
    b.reservation.complete();
    drop(b);
    relay.hold.store(false, Ordering::Release);
    worker.join().unwrap().unwrap();
    assert_eq!(relay.received.load(Ordering::Acquire), 3);
    drop(waiting);
    assert_eq!(ledger.stats(&relay.peer).unwrap().available_requests, 2);
}

#[cfg(feature = "cuda")]
#[test]
fn staging_credit_owners_fence_close_and_require_each_child_to_drain() {
    use crate::CreditResource::{ConsumerSlots, StagingSlots};
    use crate::runtime::receiver_credit::protocol::PinCredit;

    let issuer = ReceiverCreditIssuer::new(ReceiverCreditConfig {
        max_staging_slots: 2,
        max_consumer_slots: 1,
        staging_slots_per_sender: 2,
        consumer_slots_per_sender: 1,
        ..config()
    });
    let mut sender = report(1);
    sender.epoch = issuer.synchronize(sender, false, true).unwrap().epoch;
    let binding = PinCredit {
        sender: sender.sender,
        epoch: sender.epoch,
    };
    assert!(matches!(
        issuer.synchronize(report(2), false, true),
        Err(Error::ResourceExhausted)
    ));
    for binding in [
        None,
        Some(PinCredit {
            epoch: binding.epoch + 1,
            ..binding
        }),
    ] {
        assert!(issuer.reserve_staging(binding, 2).is_err());
    }
    assert_eq!(issuer.stats().active_staging_slots, 0);
    let reservation = issuer.reserve_staging(Some(binding), 2).unwrap().unwrap();
    assert!(matches!(
        issuer.reserve_staging(Some(binding), 1),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(issuer.stats().active_staging_slots, 2);
    assert_eq!(issuer.stats().active_consumer_slots, 1);
    assert!(matches!(
        issuer.synchronize(sender, true, false),
        Err(Error::BatchBusy)
    ));
    assert!(matches!(
        issuer.reserve_staging(Some(binding), 1),
        Err(Error::Cancelled)
    ));
    drop(reservation);
    issuer.synchronize(sender, true, false).unwrap();
    assert_eq!(issuer.stats().reserved_staging_slots, 0);
    assert_eq!(issuer.stats().reserved_consumer_slots, 0);

    let mut sender = report(2);
    sender.epoch = issuer.synchronize(sender, false, true).unwrap().epoch;
    let binding = PinCredit {
        sender: sender.sender,
        epoch: sender.epoch,
    };
    for retain in [false, true] {
        let mut reservation = issuer.reserve_staging(Some(binding), 2).unwrap().unwrap();
        let mut consumer = reservation.take(ConsumerSlots).unwrap();
        consumer.bind();
        let mut child = reservation.take(StagingSlots).unwrap();
        child.bind();
        drop(reservation); // The second stage never touched native state.
        assert_eq!(issuer.stats().active_staging_slots, 1);
        if retain {
            drop(child); // Unproven child Drop cannot silently return the session.
            assert!(matches!(consumer.release(), Err(Error::Unquiesced { .. })));
        } else {
            child.release().unwrap();
            consumer.release().unwrap();
            assert_eq!(issuer.stats().active_consumer_slots, 0);
        }
    }
    sender.committed = [4, 2];
    sender.completed = sender.committed;
    issuer.synchronize(sender, false, true).unwrap();
    let stats = issuer.stats();
    assert_eq!(
        (stats.active_staging_slots, stats.retained_staging_slots),
        (1, 1)
    );
    assert_eq!(
        (stats.active_consumer_slots, stats.retained_consumer_slots),
        (1, 1)
    );
    assert!(matches!(
        issuer.synchronize(sender, true, false),
        Err(Error::BatchBusy)
    ));
}
