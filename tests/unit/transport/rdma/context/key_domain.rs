use super::*;

#[test]
fn exhausted_windows_keep_old_keys_burned_and_rotate_within_the_pd_budget() {
    for mode in [
        "window_rollover",
        "window_rollover_duplicate",
        "window_rollover_alloc",
        "window_rollover_partial",
        "window_rollover_close",
        "window_rollover_budget",
    ] {
        let mut runtime = runtime(mode);
        if mode.ends_with("budget") {
            runtime.config.max_window_key_indices = 1;
        }
        let mut keys = std::collections::HashSet::new();
        let count = if matches!(mode, "window_rollover" | "window_rollover_close") {
            300
        } else {
            255
        };
        for _ in 0..count {
            let mut endpoint = runtime.staging_endpoint(8, 1).unwrap();
            endpoint.connect(&endpoint.descriptor()).unwrap();
            endpoint.bind_window().unwrap();
            assert!(
                keys.insert(endpoint.grant().unwrap().rkey),
                "{mode}: repeated rkey"
            );
            endpoint.invalidate_window().unwrap();
            endpoint.close().unwrap();
        }
        if count == 255 {
            assert!(
                runtime.staging_endpoint(8, 1).is_err(),
                "{mode}: unsafe rollover accepted"
            );
        }
        let stats = runtime.stats().unwrap();
        assert_eq!(stats.burned_window_keys, count, "{mode}");
        assert_eq!(
            (
                stats.active_windows,
                stats.exhausted_windows,
                stats.active_endpoints
            ),
            (1, 1, 0),
            "{mode}"
        );
        assert_eq!(stats.window_key_rollovers, 1, "{mode}");
        let events = events(&runtime);
        let close = runtime.close(Instant::now() + Duration::from_secs(2));
        if mode == "window_rollover_close" {
            assert!(matches!(
                close,
                Err(Error::Unquiesced {
                    operation: "ibv_dealloc_mw exhausted index",
                    ..
                })
            ));
            let before = events.lock().unwrap().len();
            assert!(
                runtime
                    .close(Instant::now() + Duration::from_secs(2))
                    .is_err()
            );
            assert_eq!(
                events.lock().unwrap().len(),
                before,
                "retained cleanup retried"
            );
            assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
            let stats = runtime.stats().unwrap();
            assert_eq!((stats.retained_windows, stats.retained_devices), (1, 1));
        } else {
            close.unwrap();
            let stats = runtime.stats().unwrap();
            assert_eq!(
                (
                    stats.active_windows,
                    stats.exhausted_windows,
                    stats.window_key_indices,
                    stats.active_devices
                ),
                (0, 0, 0, 0),
                "{mode}"
            );
            let events = events.lock().unwrap();
            assert!(
                events.iter().rposition(|e| *e == "dealloc_window")
                    < events.iter().position(|e| *e == "dealloc_pd")
            );
        }
    }
}
