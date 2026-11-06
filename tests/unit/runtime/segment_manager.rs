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
    let p = m.admit(key(1)).unwrap();
    let alias = p.clone();
    let calls = AtomicUsize::new(0);
    assert!(matches!(
        m.close(key(1), Instant::now(), || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
        Err(Error::Unquiesced { .. })
    ));
    assert!(matches!(m.admit(key(1)), Err(Error::PeerClosed)));
    assert_eq!(m.stats(), (1, 1, 1));
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
    drop(m.admit(key(2)).unwrap());
    assert!(matches!(m.admit(key(3)), Err(Error::ResourceExhausted)));
    assert!(matches!(m.admit(key(1)), Err(Error::PeerClosed)));
    assert_eq!(m.stats(), (2, 1, 0));
}

#[test]
fn peer_retirement_concurrent_closers_share_one_sticky_native_result() {
    for failed in [false, true] {
        let m = SegmentManager::new(3);
        let p = m.admit(key(1)).unwrap();
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
            drop(m.admit(key(2)).unwrap());
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
        assert!(matches!(m.admit(key(2)), Err(Error::RequesterClosed)));
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
