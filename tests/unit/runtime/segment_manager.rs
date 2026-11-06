use super::*;
use crate::test_support::EVENTUAL_PROGRESS as WAIT;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

fn key(n: u8) -> PeerKey {
    PeerKey {
        endpoint: "127.0.0.1:1234".parse().unwrap(),
        epoch: SourceEpoch([n; 16]),
    }
}

#[test]
fn peer_retirement_fences_before_drain_and_tombstones_are_bounded() {
    let m = SegmentManager::new(2);
    let (handle, p) = m.open(key(1)).unwrap();
    let alias = p.clone();
    let calls = AtomicUsize::new(0);
    assert!(matches!(
        m.close(key(1), Instant::now(), || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
        Err(Error::Unquiesced { .. })
    ));
    assert!(matches!(m.admit(&handle, key(1)), Err(Error::PeerClosed)));
    assert_eq!(
        m.stats(),
        PeerStats {
            identities: 1,
            retired: 1,
            unquiesced: 1,
            active: 1,
            ..Default::default()
        }
    );
    drop(p);
    assert!(matches!(
        m.close(key(1), Instant::now(), || Ok(())),
        Err(Error::Unquiesced { .. })
    ));
    drop(alias);
    m.close(key(1), Instant::now() + WAIT, || {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    m.close(key(1), Instant::now(), || panic!("cleanup cannot repeat"))
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (_other, _permit) = m.open(key(2)).unwrap();
    assert!(matches!(m.open(key(3)), Err(Error::ResourceExhausted)));
    assert!(matches!(m.admit(&handle, key(1)), Err(Error::PeerClosed)));
    assert_eq!(
        m.stats(),
        PeerStats {
            identities: 2,
            live: 1,
            retired: 1,
            active: 1,
            ..Default::default()
        }
    );
}

#[test]
fn peer_retirement_concurrent_closers_share_one_sticky_native_result() {
    for failed in [false, true] {
        let m = SegmentManager::new(3);
        let (_handle, p) = m.open(key(1)).unwrap();
        let calls = AtomicUsize::new(0);
        let (entered, on_enter) = mpsc::channel();
        let (release, on_release) = mpsc::channel();
        std::thread::scope(|s| {
            let manager = &m;
            let calls = &calls;
            let cleanup = s.spawn(move || {
                manager.close(key(1), Instant::now() + WAIT, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    entered.send(()).unwrap();
                    on_release.recv_timeout(WAIT).unwrap();
                    if failed {
                        Err(Error::Unquiesced {
                            operation: "test destroy",
                            code: 9,
                        })
                    } else {
                        Ok(())
                    }
                })
            });
            drop(p);
            on_enter.recv_timeout(WAIT).unwrap();
            let other = s.spawn(|| {
                m.close(key(1), Instant::now() + WAIT, || {
                    panic!("repeated native destroy")
                })
            });
            let (_other, _permit) = m.open(key(2)).unwrap();
            release.send(()).unwrap();
            assert_eq!(cleanup.join().unwrap().is_err(), failed);
            assert_eq!(other.join().unwrap().is_err(), failed);
            let again = m.close(key(1), Instant::now() + WAIT, || {
                panic!("repeated native destroy")
            });
            assert_eq!(again.is_err(), failed);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn peer_retirement_global_close_waits_for_selective_native_cleanup() {
    let m = SegmentManager::new(2);
    let (entered, on_enter) = mpsc::channel();
    let (release, on_release) = mpsc::channel();
    std::thread::scope(|s| {
        let m = &m;
        let closer = s.spawn(move || {
            m.close(key(1), Instant::now() + WAIT, || {
                entered.send(()).unwrap();
                on_release.recv_timeout(WAIT).unwrap();
                Err(Error::Unquiesced {
                    operation: "test destroy",
                    code: 9,
                })
            })
        });
        on_enter.recv_timeout(WAIT).unwrap();
        assert!(matches!(
            m.close_cleanup(Instant::now() + Duration::from_millis(1)),
            Err(Error::Unquiesced { .. })
        ));
        release.send(()).unwrap();
        assert!(closer.join().unwrap().is_err());
        assert!(matches!(
            m.close_cleanup(Instant::now() + WAIT),
            Err(Error::Unquiesced {
                operation: "test destroy",
                code: 9
            })
        ));
    });
}

#[test]
fn peer_retirement_unreferenced_live_and_retired_entries_reclaim_but_uncertainty_stays_charged() {
    let m = SegmentManager::new(2);
    for n in 0..20 {
        let (handle, permit) = m.open(key(n)).unwrap();
        drop(permit);
        if n % 2 == 0 {
            m.close(key(n), Instant::now() + WAIT, || Ok(())).unwrap();
        }
        assert_eq!(m.stats().identities, 1);
        drop(handle);
        assert_eq!(m.stats().identities, 0);
        assert_eq!(m.stats().reclaimed, u64::from(n) + 1);
    }
    let (handle, permit) = m.open(key(21)).unwrap();
    assert!(matches!(
        m.close(key(21), Instant::now(), || panic!("not drained")),
        Err(Error::Unquiesced { .. })
    ));
    drop((handle, permit));
    // Draining was inconclusive: disappearance of the last permit is not the
    // missing native cleanup receipt. A later close can finish that cleanup.
    assert_eq!(m.stats().unquiesced, 1);
    assert_eq!(m.stats().identities, 1);
    m.close(key(21), Instant::now() + WAIT, || Ok(())).unwrap();
    assert_eq!(m.stats().identities, 0);
    assert_eq!(m.stats().reclaimed, 21);
    for n in [22, 23] {
        let (handle, permit) = m.open(key(n)).unwrap();
        drop(permit);
        assert!(
            m.close(key(n), Instant::now() + WAIT, || Err(Error::Unquiesced {
                operation: "destroy",
                code: 1
            }))
            .is_err()
        );
        drop(handle);
    }
    assert_eq!(
        m.stats(),
        PeerStats {
            identities: 2,
            retired: 2,
            unquiesced: 2,
            reclaimed: 21,
            ..Default::default()
        }
    );
    assert!(matches!(m.open(key(24)), Err(Error::ResourceExhausted)));
    assert!(matches!(m.open(key(22)), Err(Error::PeerClosed)));
    assert!(
        m.close(key(22), Instant::now() + WAIT, || panic!(
            "destroy cannot repeat"
        ))
        .is_err()
    );
}

#[test]
fn peer_retirement_reclaim_races_open_admit_and_cleanup_on_one_identity() {
    let m = SegmentManager::new(1);
    for _ in 0..20 {
        let (handle, permit) = m.open(key(1)).unwrap();
        drop(permit);
        let clone = handle.clone();
        let barrier = std::sync::Barrier::new(4);
        std::thread::scope(|s| {
            s.spawn(|| {
                barrier.wait();
                let result = m.admit(&clone, key(1));
                assert!(result.is_ok() || matches!(result, Err(Error::PeerClosed)));
            });
            s.spawn(|| {
                barrier.wait();
                let result = m.open(key(1));
                assert!(result.is_ok() || matches!(result, Err(Error::PeerClosed)));
            });
            s.spawn(|| {
                barrier.wait();
                assert_eq!(m.stats().identities, 1);
                assert!(matches!(m.open(key(2)), Err(Error::ResourceExhausted)));
            });
            barrier.wait();
            m.close(key(1), Instant::now() + WAIT, || Ok(())).unwrap();
        });
        drop(handle);
        assert!(matches!(m.admit(&clone, key(1)), Err(Error::PeerClosed)));
        drop(clone);
        // Race the last-reference sweep with an explicit authoritative reopen.
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|s| {
            let opener = s.spawn(|| {
                barrier.wait();
                m.open(key(1)).unwrap()
            });
            barrier.wait();
            m.stats();
            let (new, permit) = opener.join().unwrap();
            let shared = m.admit(&new, key(1)).unwrap();
            assert_eq!(m.stats().active, 2);
            drop((shared, permit, new));
        });
        assert_eq!(m.stats().identities, 0);
    }
    assert_eq!(m.stats().reclaimed, 40);
}

#[test]
fn peer_retirement_cleanup_itself_keeps_the_last_identity_reference() {
    let m = SegmentManager::new(1);
    let (handle, permit) = m.open(key(1)).unwrap();
    drop(permit);
    let (entered, on_enter) = mpsc::channel();
    let (release, on_release) = mpsc::channel();
    std::thread::scope(|s| {
        let m = &m;
        let closer = s.spawn(move || {
            m.close(key(1), Instant::now() + WAIT, || {
                entered.send(()).unwrap();
                on_release.recv_timeout(WAIT).unwrap();
                Ok(())
            })
        });
        on_enter.recv_timeout(WAIT).unwrap();
        drop(handle);
        assert_eq!(m.stats().identities, 1);
        assert!(matches!(m.open(key(2)), Err(Error::ResourceExhausted)));
        assert!(matches!(m.open(key(1)), Err(Error::PeerClosed)));
        release.send(()).unwrap();
        closer.join().unwrap().unwrap();
    });
    assert_eq!(m.stats().identities, 0);
    assert_eq!(m.stats().reclaimed, 1);
    drop(m.open(key(1)).unwrap());
    assert_eq!(m.stats().reclaimed, 2);
}
