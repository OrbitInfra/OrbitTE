use super::*;

#[test]
fn shared_qp_records_keep_distinct_namespaces_and_one_native_retirement_owner() {
    for retained in [false, true] {
        let runtime = configured(if retained { "close0" } else { "ok" }, 2);
        let mut first = runtime.staging_endpoint(8, 0).unwrap();
        first.connect(&first.descriptor()).unwrap();
        // A second admitted record can still be waiting for its native grant.
        let second = first
            .progress
            .register(Box::new(Resources {
                connection: Arc::clone(&first.record.connection),
                grant: None,
            }))
            .unwrap();
        first.record.send_namespace.store(11, Ordering::Release);
        second.send_namespace.store(17, Ordering::Release);
        {
            let control = first.progress.shared.control.lock().unwrap();
            assert!(Arc::ptr_eq(
                &control.qps.work_owner(11, false).unwrap(),
                &first.record
            ));
            assert!(Arc::ptr_eq(
                &control.qps.work_owner(17, false).unwrap(),
                &second
            ));
            assert!(control.qps.work_owner(12, false).is_none());
            assert_eq!(control.qps.affiliates(&first.record).len(), 2);
        }
        first.progress.close_record(&second, 2, deadline()).unwrap();
        first.record.require_live().unwrap();
        second.require_live().unwrap();
        let result = first.close();
        assert_eq!(matches!(result, Err(Error::Unquiesced { .. })), retained);
        wait_until(|| {
            matches!(
                second.lock().unwrap().phase,
                Phase::Closed | Phase::Retained
            )
        });
        assert!(second.require_live().is_err());
        drop((first, second));
        let _ = runtime.close(deadline());
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.opened_connections, 1);
        assert_eq!(stats.closed_connections, u64::from(!retained));
        assert_eq!(stats.retained_endpoints, usize::from(retained));
        assert_eq!(stats.retained_grants, usize::from(retained));
        assert_eq!(stats.retained_bytes, if retained { 8 } else { 0 });
    }
}
