use super::*;
use crate::transport::rdma::{Descriptor, discovery::GidType};
fn path(id: usize, name: &str) -> NicPath {
    NicPath {
        nic_id: id,
        name: name.into(),
        domain: [id as u8 + 1; 16],
        gid_type: GidType::Ib,
        descriptor: Descriptor {
            mtu: 3,
            lid: 1,
            link_layer: 1,
            ..Default::default()
        },
    }
}
const PEER: SourceEpoch = SourceEpoch([8; 16]);
fn monitor(config: RdmaConfig) -> Arc<RailMonitor> {
    let rails = Arc::new(RailMonitor::default());
    rails.configure(&config);
    rails
}
fn finish(lease: RailLease, success: bool, now: Instant) {
    lease.done.store(true, Ordering::Release);
    lease.owner.finish(&lease, Some(success), now);
}
fn spans(rails: &RailMonitor) -> (usize, u64, u64, u64) {
    let mut stats = super::super::RdmaStats::default();
    rails.snapshot(&mut stats);
    (
        stats.open_rails,
        stats.rail_errors,
        stats.rail_probes,
        stats.rail_recoveries,
    )
}

#[test]
fn pair_mapping_is_peer_specific_stable_on_refresh_and_bounded_by_the_target_paths() {
    let now = Instant::now();
    let local = [path(0, "left"), path(1, "right")];
    let remote = [path(0, "one"), path(1, "two")];
    for (json, expected) in [
        (None, 3),
        (Some("invalid"), 3),
        (Some(r#"{"all":[],"direct":[]}"#), 0),
        (Some(r#"{"all":[{"local":"right","remote":"one"}]}"#), 2),
    ] {
        let rails = monitor(RdmaConfig {
            rail_topology: json.map(String::from),
            ..Default::default()
        });
        assert_eq!(
            rails.mask(PEER, &local, &remote[..1], now).unwrap().0,
            expected
        );
        assert_eq!(rails.mask(PEER, &local, &[], now).unwrap().0, 0);
    }
    let topology = r#"{
        "all": [{"local":"left","remote":"two"}, {"local":"right","remote":"one"}],
        "direct": [{"local":"left","remote":"two"}]
    }"#;
    let rails = monitor(RdmaConfig {
        rail_topology: Some(topology.into()),
        rail_error_threshold: 1,
        ..Default::default()
    });
    assert_eq!(rails.mask(PEER, &local, &remote, now).unwrap().0, 3);
    let (chosen, lease) = rails.admit(PEER, &local[0], &remote, now).unwrap();
    assert_eq!(chosen.nic_id, 1);
    let key = lease.key;
    finish(lease, false, now);
    let snapshot = remote.clone();
    drop(remote);
    assert!(rails.admit(PEER, &local[0], &snapshot, now).is_err());
    assert!(
        rails
            .admit(SourceEpoch([9; 16]), &local[0], &snapshot, now)
            .is_ok()
    );
    let mut replacement = snapshot;
    replacement[1].domain = [42; 16];
    assert!(rails.admit(PEER, &local[0], &replacement, now).is_ok());
    assert!(rails.table.lock().unwrap().pairs.contains_key(&key));
}

#[test]
fn an_open_pair_admits_one_probe_and_only_its_receipt_can_reopen_it() {
    let c = RdmaConfig {
        rail_error_threshold: 1,
        rail_cooldown: Duration::from_secs(10),
        rail_probe_interval: Duration::from_secs(2),
        ..Default::default()
    };
    let rails = monitor(c);
    let l = path(0, "n");
    let r = [path(1, "n")];
    let now = Instant::now();
    let take = |t| rails.admit(PEER, &l, &r, t).map(|(_, lease)| lease);
    let old = take(now).unwrap();
    finish(take(now).unwrap(), false, now);
    assert_eq!(spans(&rails), (1, 1, 0, 0));
    assert!(take(now).is_err());
    let at = |secs| now + Duration::from_secs(secs);
    for _ in 0..3 {
        assert_eq!(
            rails
                .mask(PEER, std::slice::from_ref(&l), &r, at(2))
                .unwrap(),
            (1, 0)
        );
    }
    let probe = take(at(2)).unwrap();
    assert!(take(at(15)).is_err());
    finish(old, true, at(3));
    assert_eq!(spans(&rails), (1, 1, 1, 0));
    finish(probe, false, at(4));
    let abandoned = take(at(10)).unwrap();
    drop(abandoned);
    let half = take(at(10)).unwrap();
    assert!(take(at(100)).is_err());
    finish(half, false, at(11));
    let state = rails.table.lock().unwrap();
    let pair = state.pairs.values().next().unwrap();
    assert_eq!(pair.cooldown, Duration::from_secs(20));
    assert_eq!(pair.resume, Some(at(31)));
    drop(state);
    let early = take(at(13)).unwrap();
    finish(early, true, at(14));
    assert_eq!(spans(&rails), (0, 3, 4, 1));
    finish(take(at(15)).unwrap(), false, at(15));
    assert_eq!(
        rails
            .table
            .lock()
            .unwrap()
            .pairs
            .values()
            .next()
            .unwrap()
            .cooldown,
        Duration::from_secs(10)
    );
}

#[test]
fn failures_expire_success_resets_and_an_old_generation_cannot_close_a_new_circuit() {
    let rails = monitor(RdmaConfig {
        rail_error_threshold: 2,
        rail_error_window: Duration::from_secs(5),
        ..Default::default()
    });
    let l = path(0, "n");
    let r = [path(0, "n")];
    let t = Instant::now();
    let take = |s| {
        rails
            .admit(PEER, &l, &r, t + Duration::from_secs(s))
            .unwrap()
            .1
    };
    finish(take(0), false, t);
    finish(take(6), false, t + Duration::from_secs(6));
    finish(take(7), true, t + Duration::from_secs(7));
    finish(take(8), false, t + Duration::from_secs(8));
    assert_eq!(spans(&rails).0, 0);
    let late = take(9);
    rails.port(0, 0, true, t);
    rails.port(0, 1, false, t + Duration::from_secs(9));
    finish(late, true, t + Duration::from_secs(10));
    assert_eq!(spans(&rails).0, 1);
    let until = rails
        .table
        .lock()
        .unwrap()
        .pairs
        .values()
        .next()
        .unwrap()
        .resume;
    rails.port(0, 2, false, t + Duration::from_secs(10));
    assert_eq!(
        rails
            .table
            .lock()
            .unwrap()
            .pairs
            .values()
            .next()
            .unwrap()
            .resume,
        until
    );
    rails.port(0, 2, true, t + Duration::from_secs(10));
    assert_eq!(spans(&rails).0, 1);
    let trial = take(40);
    finish(trial, true, t + Duration::from_secs(40));
    assert_eq!(spans(&rails).0, 0);
}

#[test]
fn bounded_pair_table_cannot_evict_a_live_or_unproven_rail() {
    let rails = monitor(RdmaConfig {
        max_rail_pairs: 1,
        rail_error_threshold: 1,
        ..Default::default()
    });
    let now = Instant::now();
    let l = path(0, "n");
    let r = [path(0, "n")];
    let live = rails.admit(PEER, &l, &r, now).unwrap().1;
    assert!(matches!(
        rails.admit(SourceEpoch([7; 16]), &l, &r, now),
        Err(Error::ResourceExhausted)
    ));
    finish(live, false, now);
    assert!(matches!(
        rails.admit(SourceEpoch([7; 16]), &l, &r, now),
        Err(Error::ResourceExhausted)
    ));
}

#[test]
fn half_open_failures_cap_at_five_minutes_and_prepost_abandonment_reopens_the_trial() {
    let rails = monitor(RdmaConfig {
        rail_error_threshold: 1,
        rail_cooldown: Duration::from_secs(100),
        rail_probe_interval: Duration::ZERO,
        ..Default::default()
    });
    let l = path(0, "n");
    let r = [path(0, "n")];
    let mut now = Instant::now();
    finish(rails.admit(PEER, &l, &r, now).unwrap().1, false, now);
    for cooldown in [100, 200, 300, 300] {
        now += Duration::from_secs(cooldown);
        let canceled = rails.admit(PEER, &l, &r, now).unwrap().1;
        canceled.setup_failed(&Error::Backend {
            operation: "unposted QP",
            code: 1,
        });
        drop(canceled);
        let trial = rails.admit(PEER, &l, &r, now).unwrap().1;
        assert!(rails.admit(PEER, &l, &r, now).is_err());
        finish(trial, false, now);
        let state = rails.table.lock().unwrap();
        let s = state.pairs.values().next().unwrap();
        assert_eq!(s.cooldown, Duration::from_secs((2 * cooldown).min(300)));
    }
}

#[test]
fn concurrent_workers_share_exactly_one_trial_until_its_own_receipt() {
    let rails = monitor(RdmaConfig {
        rail_error_threshold: 1,
        rail_cooldown: Duration::ZERO,
        ..Default::default()
    });
    let now = Instant::now();
    let l = path(0, "left");
    let r = [path(0, "right")];
    finish(rails.admit(PEER, &l, &r, now).unwrap().1, false, now);
    let barrier = std::sync::Barrier::new(8);
    let winner = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..8 {
            threads.push(scope.spawn(|| {
                barrier.wait();
                rails.admit(PEER, &l, &r, now).ok().map(|(_, s)| s)
            }));
        }
        let mut winners = threads.into_iter().filter_map(|h| h.join().unwrap());
        let first = winners.next().unwrap();
        assert!(winners.next().is_none());
        first
    });
    assert_eq!(spans(&rails), (1, 1, 1, 0));
    finish(winner, true, now);
    assert_eq!(spans(&rails), (0, 1, 1, 1));
}

#[test]
fn r1b_wildcard_prefers_the_mapped_peer_and_only_a_matching_flush_cancels_a_trial() {
    let json = r#"{"all":[{"local":"L","remote":"a"},{"local":"L","remote":"b"}],"direct":[{"local":"L","remote":"a"},{"local":"L","remote":"b"}]}"#;
    let rails = monitor(RdmaConfig {
        rail_topology: Some(json.into()),
        rail_error_threshold: 1,
        rail_probe_interval: Duration::from_secs(1),
        ..Default::default()
    });
    let local = path(0, "L");
    let remote = [path(1, "a"), path(2, "b")];
    let t = Instant::now();
    rails.set_time(t);
    let flush = || {
        Err(Error::Backend {
            operation: "RDMA work completion",
            code: super::super::sys::FLUSHED as i32,
        })
    };
    let (r, old) = rails.admit(PEER, &local, &remote, t).unwrap();
    assert_eq!(r.name, "b");
    let (_, root) = rails.admit(PEER, &local, &remote, t).unwrap();
    finish(root, false, t);
    assert_eq!(rails.admit(PEER, &local, &remote, t).unwrap().0.name, "a");
    let t = t + Duration::from_secs(2);
    rails.set_time(t);
    // A healthy alternative continues to outrank a half-open mapped peer.
    assert_eq!(rails.admit(PEER, &local, &remote, t).unwrap().0.name, "a");
    let (_, probe) = rails.admit(PEER, &local, &remote[1..], t).unwrap();
    old.receipt(&flush());
    assert!(rails.admit(PEER, &local, &remote[1..], t).is_err());
    probe.receipt(&flush());
    let t = t + Duration::from_secs(1);
    rails.set_time(t);
    let (_, replacement) = rails.admit(PEER, &local, &remote[1..], t).unwrap();
    replacement.receipt(&Ok(()));
    assert_eq!(spans(&rails), (0, 1, 2, 1));
    rails.port(0, 1, false, t);
    assert_eq!(
        rails
            .mask(PEER, std::slice::from_ref(&local), &remote, t)
            .unwrap(),
        (0, 0)
    );
    assert!(matches!(
        rails.admit(PEER, &local, &remote, t),
        Err(Error::Unavailable(_))
    ));
    let discovery = monitor(RdmaConfig::default());
    let reversed = [path(2, "other"), path(1, "L")];
    assert_eq!(
        discovery.admit(PEER, &local, &reversed, t).unwrap().0.name,
        "L"
    );
}
