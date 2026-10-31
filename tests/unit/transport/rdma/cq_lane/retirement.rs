use super::*;

struct NativeHold(Arc<(Mutex<(bool, bool)>, Condvar)>);
impl Drop for NativeHold {
    fn drop(&mut self) {
        release(&self.0);
    }
}

#[test]
fn poll_retirement_keeps_native_owners_until_close_or_retention_receipt() {
    for failure in [false, true] {
        let runtime = configured(
            if failure {
                "qp_pause_close0"
            } else {
                "qp_pause"
            },
            3,
        );
        let mut grant = runtime.staging_endpoint(8, 0).unwrap();
        let hold = NativeHold(pause(&runtime));
        assert!(!grant.poll_close().unwrap());
        wait_blocked(&hold.0);
        let before = runtime.stats().unwrap();
        assert_eq!(
            (
                before.active_endpoints,
                before.active_grants,
                before.staging_bytes
            ),
            (1, 1, 8)
        );
        let until = Instant::now() + Duration::from_secs(1);
        for _ in 0..32 {
            assert!(!grant.poll_close().unwrap());
            assert!(Instant::now() < until, "poll joined native destroy_qp");
            assert!(grant.output.is_none());
        }
        assert!(grant.record.try_lock().unwrap().is_none());
        grant.deadline = Instant::now();
        assert!(matches!(
            grant.poll_close(),
            Err(Error::Unquiesced {
                code: libc::ETIMEDOUT,
                ..
            })
        ));
        assert_eq!(runtime.stats().unwrap().staging_bytes, 8);
        grant.deadline = deadline();
        drop(hold);
        let result = loop {
            match grant.poll_close() {
                Ok(false) => thread::yield_now(),
                result => break result,
            }
        };
        assert_eq!(matches!(result, Err(Error::Unquiesced { .. })), failure);
        assert_eq!(grant.output.is_some(), !failure);
        if failure {
            let control = grant.progress.shared.control.lock().unwrap();
            assert!(control.finished && control.detached);
            assert!(control.retained.is_some());
        }
        assert_eq!(
            events(&runtime)
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "destroy_qp")
                .count(),
            1
        );
        drop(grant);
        let _ = runtime.close(deadline());
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.active_grants, usize::from(failure));
        assert_eq!(stats.active_endpoints, usize::from(failure));
        assert_eq!(stats.retained_grants, usize::from(failure));
    }
}

#[test]
fn poll_retirement_yields_before_fencing_a_busy_store_or_sibling() {
    let runtime = configured("ok", 3);
    let mut grant = runtime.staging_endpoint(8, 0).unwrap();
    let lane = Arc::clone(&grant.progress);
    let record = Arc::clone(&grant.record);
    let sibling = lane
        .register(Box::new(Resources {
            connection: Arc::clone(&record.connection),
            grant: None,
        }))
        .unwrap();
    {
        let _request = record.lock().unwrap();
        assert!(!grant.poll_close().unwrap());
        #[cfg(feature = "cuda")]
        {
            assert!(!lane.query(&record, grant.claim).unwrap());
            assert!(!lane.query_completion(&record, grant.claim).unwrap());
            assert!(!lane.query_finish(&record, grant.claim).unwrap());
        }
    }
    {
        let _control = lane.shared.control.lock().unwrap();
        assert!(!grant.poll_close().unwrap());
    }
    {
        let _sibling = sibling.lock().unwrap();
        assert!(!grant.poll_close().unwrap());
        record.require_live().unwrap();
        assert!(!record.lock().unwrap().close_requested);
        assert_eq!(record.actions.load(Ordering::Acquire) & CLOSE, 0);
    }
    // An empty logical grant can leave without retiring the physical sibling.
    wait_until(|| {
        let ready = lane.poll_close_record(&sibling, 1, deadline()).unwrap();
        record.require_live().unwrap();
        ready
    });
    assert_eq!(runtime.stats().unwrap().closed_connections, 0);
    // A stale close does not acquire authority over the live claim.
    wait_until(|| {
        let ready = lane.poll_close_record(&record, 2, deadline()).unwrap();
        record.require_live().unwrap();
        ready
    });
    grant.close().unwrap();
    drop((grant, sibling));
    runtime.close(deadline()).unwrap();
}

#[test]
fn retained_record_poll_waits_for_the_whole_lane_receipt() {
    let runtime = configured("close0", 3);
    let mut grant = runtime.staging_endpoint(8, 0).unwrap();
    let other = unopened(&runtime);
    let other_record = Arc::clone(&other.record);
    let lock = other_record.lock().unwrap();
    grant
        .progress
        .shared
        .fail(Fault::Protocol("controlled lane failure"));
    wait_until(|| {
        grant
            .record
            .try_lock()
            .unwrap()
            .is_some_and(|s| s.phase == Phase::Retained)
    });
    for _ in 0..32 {
        assert!(!grant.poll_close().unwrap());
    }
    assert!(!grant.progress.shared.control.lock().unwrap().finished);
    assert!(grant.output.is_none());
    drop(lock);
    assert!(matches!(grant.close(), Err(Error::Unquiesced { .. })));
    let control = grant.progress.shared.control.lock().unwrap();
    assert!(control.finished && control.detached);
    drop(control);
    drop((grant, other));
    let _ = runtime.close(deadline());
}
