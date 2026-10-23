use super::*;

fn source(
    mode: &str,
) -> (
    RdmaTransport,
    crate::memory::MemoryRegistry,
    RegisteredMemory,
    RdmaEndpoint,
) {
    let runtime = runtime(mode);
    let registry = crate::memory::MemoryRegistry::new(SourceEpoch([1; 16]), 1, 8);
    let memory = registry
        .register(
            vec![0x42; 8].into_boxed_slice(),
            crate::Permission::RemoteReadWrite,
        )
        .unwrap();
    let mut endpoint = runtime
        .endpoint(&memory, 0, 8, 2, Instant::now() + Duration::from_secs(2))
        .unwrap();
    endpoint.connect(&endpoint.descriptor()).unwrap();
    (runtime, registry, memory, endpoint)
}

#[test]
fn windows_bind_once_invalidate_and_keep_exact_loan_until_qp_barrier() {
    let (runtime, registry, memory, mut endpoint) = source("direct:ok");
    let initial = runtime.stats().unwrap();
    assert_eq!(
        (initial.active_windows, initial.completed_window_binds),
        (1, 0)
    );
    assert_eq!(&endpoint.connection()[..12], &[0; 12]);
    assert!(Descriptor::decode_connection(&endpoint.connection()).is_ok());
    assert!(Descriptor::decode_connection(&endpoint.descriptor().encode()).is_err());
    assert!(endpoint.grant().is_err());
    endpoint.bind_window().unwrap();
    assert!(endpoint.grant().is_ok());
    assert!(endpoint.bind_window().is_err(), "one bind per source QP/MW");
    assert!(memory.read(0, &mut [0; 8]).is_err());
    endpoint.invalidate_window().unwrap();
    assert!(endpoint.grant().is_err());
    assert!(endpoint.invalidate_window().is_err());
    assert!(
        memory.read(0, &mut [0; 8]).is_err(),
        "INV is not arbitrary incoming-DMA quiescence"
    );
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.completed_window_binds,
            stats.completed_window_invalidations,
            stats.work_requests
        ),
        (1, 1, 0)
    );
    assert_eq!((stats.completed_reads, stats.completed_writes), (0, 0));
    endpoint.close().unwrap();
    drop(endpoint);
    let mut bytes = [0; 8];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0x42; 8]);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_windows,
            stats.active_endpoints,
            stats.direct_bytes
        ),
        (0, 0, 0)
    );
    registry.unregister(&memory).unwrap();
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn window_errors_abort_actual_owners_or_retain_failed_cleanup_without_retry() {
    for mode in [
        "window_bind_post",
        "window_inv_post",
        "window_unknown_post",
        "window_bind_error",
        "window_inv_error",
        "window_bind_timeout",
        "window_inv_timeout",
        "window_close",
        "window_unknown_post_close0",
        "window_inv_unknown_close0",
    ] {
        let (runtime, registry, memory, mut endpoint) = source(&format!("direct:{mode}"));
        if mode.ends_with("_timeout") {
            endpoint.deadline = Instant::now() + Duration::from_millis(100);
        }
        let retained = mode == "window_close" || mode.ends_with("_close0");
        let log = events(&runtime);
        let weak = Arc::downgrade(&memory.0);
        let bound = endpoint.bind_window();
        let result = if bound.is_ok() {
            endpoint.invalidate_window()
        } else {
            bound
        };
        if mode != "window_close" {
            assert!(result.is_err(), "{mode}");
        }
        let closed = endpoint.close();
        registry.unregister(&memory).unwrap();
        if retained {
            assert!(matches!(closed, Err(Error::Unquiesced { .. })));
            assert!(matches!(endpoint.close(), Err(Error::Unquiesced { .. })));
            assert!(memory.read(0, &mut [0; 8]).is_err());
            let stats = runtime.stats().unwrap();
            assert_eq!(
                (
                    stats.active_windows,
                    stats.retained_windows,
                    stats.retained_endpoints,
                    stats.direct_bytes,
                    stats.retained_grants
                ),
                (1, 1, usize::from(mode.ends_with("_close0")), 8, 1)
            );
        } else {
            assert!(
                !matches!(closed, Err(Error::Unquiesced { .. }))
                    || (mode.ends_with("_timeout")
                        && matches!(
                            closed,
                            Err(Error::Unquiesced {
                                code: libc::ETIMEDOUT,
                                ..
                            })
                        )),
                "{mode}: {closed:?}"
            );
        }
        drop(endpoint);
        drop(memory);
        if retained {
            assert!(weak.upgrade().is_some());
            assert!(matches!(
                runtime.close(Instant::now() + Duration::from_secs(1)),
                Err(Error::Unquiesced { .. })
            ));
            assert_eq!(
                log.lock()
                    .unwrap()
                    .iter()
                    .filter(|e| **e == "dealloc_window")
                    .count(),
                usize::from(mode == "window_close")
            );
            let stats = runtime.stats().unwrap();
            let debt = usize::from(mode.ends_with("_close0"));
            assert_eq!(
                (stats.work_requests, stats.retained_work_requests),
                (debt, debt)
            );
            assert!(!log.lock().unwrap().contains(&"deregister"));
        } else {
            runtime
                .close(Instant::now() + Duration::from_secs(1))
                .unwrap();
            assert!(weak.upgrade().is_none(), "{mode}");
            assert_eq!(
                (
                    runtime.stats().unwrap().active_windows,
                    runtime.stats().unwrap().work_requests
                ),
                (0, 0)
            );
        }
    }
    let runtime = runtime("window_unsupported");
    assert!(runtime.staging_endpoint(8, 2).is_err());
    assert_eq!(
        (
            runtime.stats().unwrap().active_windows,
            runtime.stats().unwrap().active_endpoints
        ),
        (0, 0)
    );
    assert!(!events(&runtime).lock().unwrap().contains(&"bind_window"));
    runtime
        .close(Instant::now() + Duration::from_secs(1))
        .unwrap();
}
