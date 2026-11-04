use super::*;
use std::sync::{Arc, Barrier};
use std::thread;

fn config(mode: Mode) -> Config {
    Config {
        mode,
        min_window_bytes: 64,
        max_window_bytes: 256,
        ..Default::default()
    }
}
fn domain(mode: Mode) -> Arc<DomainState> {
    DomainState::new(config(mode), 1).unwrap()
}
fn path(device: &Arc<DomainState>, route: &Arc<DomainState>) -> PathHandle {
    PathHandle::new(Some(device.clone()), Some(route.clone()))
}
fn finish(permit: &mut Permit) {
    assert!(permit.complete(Outcome::Success, FailureScope::Operation));
    assert!(!permit.complete(Outcome::Success, FailureScope::Operation));
}
#[test]
fn startup_validates_each_supported_value_without_mutating_the_environment() {
    use std::ffi::OsString;
    let load = |values: &[(&str, &str)]| {
        Config::from_lookup(|key| {
            values
                .iter()
                .find(|(k, _)| *k == key.strip_prefix("MC_ADAPTIVE_CONGESTION_CONTROL_").unwrap())
                .map(|(_, v)| OsString::from(v))
        })
    };
    let defaults = load(&[]).unwrap();
    assert_eq!(
        (
            defaults.mode,
            defaults.min_window_bytes,
            defaults.max_window_bytes,
            defaults.target_drain_time_ns,
            defaults.high_pressure_epochs,
            defaults.low_pressure_epochs
        ),
        (Mode::Off, 4 << 20, 64 << 20, 2_000_000, 2, 3)
    );
    let c = load(&[
        ("MODE", "enforce"),
        ("MIN_WINDOW_BYTES", "1"),
        ("MAX_WINDOW_BYTES", "18"),
        ("TARGET_DRAIN_US", "7"),
        ("HIGH_PRESSURE_EPOCHS", "9"),
        ("LOW_PRESSURE_EPOCHS", "3"),
    ])
    .unwrap();
    assert_eq!(
        (
            c.mode,
            c.min_window_bytes,
            c.max_window_bytes,
            c.target_drain_time_ns,
            c.high_pressure_epochs,
            c.low_pressure_epochs
        ),
        (Mode::Enforce, 1, 18, 7000, 9, 3)
    );
    assert_eq!(
        load(&[("MODE", "observe"), ("QUARANTINE_MS", "broken")])
            .unwrap()
            .mode,
        Mode::Observe
    );
    for (key, val) in [
        ("MODE", "Observe"),
        ("MIN_WINDOW_BYTES", "+1"),
        ("MAX_WINDOW_BYTES", ""),
        ("MIN_WINDOW_BYTES", "4294967296"),
        ("TARGET_DRAIN_US", "18446744073709552"),
        ("HIGH_PRESSURE_EPOCHS", "0"),
        ("LOW_PRESSURE_EPOCHS", "4294967296"),
        ("HIGH_PRESSURE_EPOCHS", " 3"),
    ] {
        assert!(
            matches!(load(&[(key, val)]), Err(crate::Error::InvalidConfig(_))),
            "{key}"
        );
    }
    let mut invalid = c;
    invalid.min_window_bytes = 20;
    assert!(DomainState::new(invalid, 0).is_err());
}
#[test]
fn modes_admission_evidence_and_moved_permits_balance_device_and_route() {
    for mode in [Mode::Off, Mode::Observe, Mode::Enforce] {
        let device = domain(mode);
        let route = domain(mode);
        let p = path(&device, &route);
        let mut first = p.try_acquire(100).unwrap();
        assert_eq!(
            device.snapshot().inflight_bytes,
            if mode == Mode::Off { 0 } else { 100 }
        );
        let second = p.try_acquire(4);
        if mode == Mode::Enforce {
            let e = second.unwrap_err();
            assert_eq!(
                (e.decision, e.domain, e.reason, e.snapshot.deferred_bytes),
                (Decision::Defer, AdmissionDomain::Device, Reason::Window, 4)
            );
        } else {
            finish(&mut second.unwrap());
        }
        assert!(first.is_current());
        route.reset_generation(2);
        assert!(!first.is_current());
        finish(&mut first);
        assert_eq!(device.snapshot().inflight_bytes, 0);
        if mode == Mode::Enforce {
            let e = p.try_acquire(2).unwrap_err();
            assert_eq!(
                (e.domain, e.decision, e.reason),
                (
                    AdmissionDomain::Route,
                    Decision::Avoid,
                    Reason::StaleGeneration
                )
            );
            assert_eq!(device.snapshot().inflight_bytes, 0);
            device.reset_generation(2);
            assert_eq!(
                p.try_acquire(2).unwrap_err().domain,
                AdmissionDomain::Device
            );
        } else {
            finish(&mut p.try_acquire(2).unwrap());
        }
        if mode == Mode::Off {
            device.record_signals(
                2,
                Signals {
                    receiver_pressure: 42,
                    ..Default::default()
                },
            );
            device.control_tick();
            assert_eq!(device.snapshot().window_bytes, 64);
        }
        assert!(!first.is_current());
    }
    let device = domain(Mode::Enforce);
    let route = domain(Mode::Enforce);
    let occupied = PathHandle::new(None, Some(route.clone()));
    let p = path(&device, &route);
    let mut a = occupied.try_acquire(48).unwrap();
    let e = p.try_acquire(32).unwrap_err();
    assert_eq!(e.domain, AdmissionDomain::Route);
    assert_eq!(
        (
            e.snapshot.inflight_bytes,
            e.snapshot.deferred_bytes,
            device.snapshot().inflight_bytes
        ),
        (48, 32, 0)
    );
    let b = occupied.try_acquire(16).unwrap();
    finish(&mut a);
    a = b;
    assert_eq!(route.snapshot().inflight_bytes, 16);
    finish(&mut a);
    let unconfirmed = p.try_acquire(64).unwrap();
    drop(unconfirmed);
    assert_eq!(p.try_acquire(1).unwrap_err().reason, Reason::Window);
}
#[test]
fn feedback_ticks_apply_release_pressure_scope_and_generation_rules() {
    for mode in [Mode::Observe, Mode::Enforce] {
        let d = domain(mode);
        let r = domain(mode);
        let sibling = domain(mode);
        let p = path(&d, &r);
        let q = path(&d, &sibling);
        for _ in 0..16 {
            finish(&mut p.try_acquire(9).unwrap());
            d.control_tick();
            r.control_tick();
        }
        assert_eq!(r.snapshot().window_bytes, 256);
        r.record_signals(
            1,
            Signals {
                backlog_bytes: u64::MAX,
                delivery_rate_bytes_per_sec: 1,
                poller_stalled: true,
                ..Default::default()
            },
        );
        r.control_tick();
        assert_eq!(r.snapshot().window_bytes, 256);
        let mut credit = p.try_acquire(2).unwrap();
        credit.complete(Outcome::ReceiverPressure, FailureScope::Qp);
        r.record_signals(
            1,
            Signals {
                receiver_pressure: 9,
                hard_errors: 1,
                successful_completions: 7,
                ..Default::default()
            },
        );
        r.control_tick();
        d.control_tick();
        assert_eq!(
            (
                r.snapshot().state,
                r.snapshot().window_bytes,
                d.snapshot().state,
                d.snapshot().window_bytes
            ),
            (PathState::Congested, 128, PathState::Healthy, 256)
        );
        for scope in [
            FailureScope::Operation,
            FailureScope::Route,
            FailureScope::Qp,
            FailureScope::Cq,
            FailureScope::Device,
            FailureScope::Port,
        ] {
            d.reset_generation(1);
            r.reset_generation(1);
            sibling.reset_generation(1);
            finish(&mut p.try_acquire(1).unwrap());
            finish(&mut q.try_acquire(1).unwrap());
            let mut f = p.try_acquire(1).unwrap();
            f.complete(Outcome::RouteTimeout, scope);
            d.control_tick();
            r.control_tick();
            sibling.control_tick();
            let shared = matches!(
                scope,
                FailureScope::Cq | FailureScope::Device | FailureScope::Port
            );
            assert_eq!(
                (
                    d.snapshot().window_bytes,
                    r.snapshot().window_bytes,
                    sibling.snapshot().window_bytes
                ),
                (if shared { 64 } else { 80 }, 64, 80)
            );
            finish(&mut p.try_acquire(1).unwrap());
            r.control_tick();
            assert_eq!(r.snapshot().window_bytes, 80);
        }
        let mut old = p.try_acquire(12).unwrap();
        r.reset_generation(2);
        r.record_signals(
            1,
            Signals {
                receiver_pressure: 1,
                ..Default::default()
            },
        );
        let mut new = path(&d, &r).try_acquire(32).unwrap();
        assert!(!old.is_current());
        old.complete(Outcome::Fatal, FailureScope::Route);
        r.control_tick();
        assert_eq!(
            (
                r.snapshot().state,
                r.snapshot().inflight_bytes,
                r.snapshot().generation
            ),
            (PathState::Healthy, 32, 2)
        );
        finish(&mut new);
    }
}
#[test]
fn concurrent_oversize_reservations_and_epoch_resets_are_bounded() {
    let d = domain(Mode::Enforce);
    let r = domain(Mode::Enforce);
    let p = path(&d, &r);
    let start = Barrier::new(8);
    let held = Barrier::new(8);
    let admitted = AtomicU64::new(0);
    thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                start.wait();
                let mut permit = p.try_acquire(128).ok();
                if permit.is_some() {
                    admitted.fetch_add(1, Ordering::Relaxed);
                }
                held.wait();
                if let Some(v) = &mut permit {
                    finish(v);
                }
                for _ in 0..100 {
                    if let Ok(mut v) = p.try_acquire(16) {
                        finish(&mut v);
                    }
                }
            });
        }
    });
    assert_eq!(admitted.load(Ordering::Relaxed), 1);
    assert_eq!(
        (d.snapshot().inflight_bytes, r.snapshot().inflight_bytes),
        (0, 0)
    );
    thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                for _ in 0..300 {
                    d.record_signals(
                        1,
                        Signals {
                            successful_completions: 1,
                            poller_stalled: true,
                            ..Default::default()
                        },
                    );
                    d.control_tick();
                }
            });
        }
        d.reset_generation(2);
    });
    assert_eq!(
        (
            d.snapshot().generation,
            d.snapshot().state,
            d.snapshot().window_bytes
        ),
        (2, PathState::Healthy, 64)
    );
}

#[test]
fn verbs_errors_classify_root_failures_and_ignore_topology_events() {
    use FailureScope::*;
    use Outcome::*;
    use ffi::ibv_wc_status::*;
    use ibverbs_sys as ffi;
    for (code, outcome, scope, root) in [
        (IBV_WC_SUCCESS, Success, Operation, false),
        (IBV_WC_RNR_RETRY_EXC_ERR, ReceiverPressure, Qp, true),
        (IBV_WC_RETRY_EXC_ERR, RouteTimeout, Qp, true),
        (IBV_WC_RESP_TIMEOUT_ERR, RouteTimeout, Qp, true),
        (IBV_WC_LOC_PROT_ERR, LocalConfiguration, Operation, true),
        (IBV_WC_LOC_QP_OP_ERR, LocalConfiguration, Qp, true),
        (IBV_WC_REM_ACCESS_ERR, RemoteMetadata, Operation, true),
        (IBV_WC_WR_FLUSH_ERR, DerivedFlush, Qp, false),
        (IBV_WC_GENERAL_ERR, Fatal, Qp, true),
        (u32::MAX, Fatal, Qp, true),
    ] {
        assert_eq!(
            classify_completion(code, 17),
            Classification {
                outcome,
                scope,
                root_failure: root,
                vendor_error: 17
            }
        );
    }
    for (code, scope) in [
        (ffi::IBV_EVENT_QP_FATAL, Qp),
        (ffi::IBV_EVENT_QP_REQ_ERR, Qp),
        (ffi::IBV_EVENT_QP_ACCESS_ERR, Qp),
        (ffi::IBV_EVENT_WQ_FATAL, Qp),
        (ffi::IBV_EVENT_SRQ_ERR, Qp),
        (ffi::IBV_EVENT_CQ_ERR, Cq),
        (ffi::IBV_EVENT_PATH_MIG_ERR, Route),
        (ffi::IBV_EVENT_PORT_ERR, Port),
        (ffi::IBV_EVENT_DEVICE_FATAL, Device),
    ] {
        assert_eq!(
            classify_async_event(code),
            Some(Classification {
                outcome: Fatal,
                scope,
                root_failure: true,
                vendor_error: 0
            })
        );
    }
    for code in [
        ffi::IBV_EVENT_PORT_ACTIVE,
        ffi::IBV_EVENT_GID_CHANGE,
        ffi::IBV_EVENT_LID_CHANGE,
        ffi::IBV_EVENT_PKEY_CHANGE,
    ] {
        assert_eq!(classify_async_event(code), None);
    }
}

#[test]
fn root_errors_stall_and_legacy_signals_never_isolate_a_domain() {
    for mode in [Mode::Off, Mode::Observe, Mode::Enforce] {
        for outcome in [Outcome::Fatal, Outcome::RouteTimeout] {
            for scope in [
                FailureScope::Operation,
                FailureScope::Qp,
                FailureScope::Route,
                FailureScope::Cq,
                FailureScope::Device,
                FailureScope::Port,
            ] {
                let d = domain(mode);
                let r = domain(mode);
                let p = path(&d, &r);
                for _ in 0..8 {
                    let mut permit = p.try_acquire(8).unwrap();
                    assert!(permit.complete(outcome, scope));
                    assert!(!permit.complete(outcome, scope));
                    d.control_tick();
                    r.control_tick();
                }
                for owner in [&d, &r] {
                    owner.record_signals(
                        1,
                        Signals {
                            hard_errors: u64::MAX,
                            fatal_failures: u64::MAX,
                            route_timeouts: u64::MAX,
                            poller_stalled: true,
                            ..Default::default()
                        },
                    );
                    owner.control_tick();
                    assert_eq!(
                        (
                            owner.snapshot().state,
                            owner.snapshot().window_bytes,
                            owner.snapshot().inflight_bytes
                        ),
                        (PathState::Healthy, 64, 0)
                    );
                }
                let mut data = p.try_acquire(64).unwrap();
                assert!(data.complete(Outcome::DerivedFlush, FailureScope::Operation));
                let mut pressured = p.try_acquire(64).unwrap();
                pressured.complete(Outcome::Congestion, FailureScope::Route);
                for owner in [&d, &r] {
                    owner.control_tick();
                    assert_eq!(
                        owner.snapshot().state,
                        if mode == Mode::Off {
                            PathState::Healthy
                        } else {
                            PathState::Congested
                        }
                    );
                }
            }
        }
    }
}
