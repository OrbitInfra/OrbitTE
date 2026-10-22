use std::time::{Duration, Instant};

use super::*;
use crate::rdma::{Runtime, tests::FakeDevice, tests::runtime};
use std::sync::Barrier;
use std::thread;

pub(crate) fn events(runtime: &Runtime) -> Arc<Mutex<Vec<&'static str>>> {
    let cache = runtime.device.lock().unwrap();
    let owner = cache.owner.as_ref().unwrap();
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("device is not open");
    };
    Arc::clone(&unsafe { &*resources.handles.context.cast::<FakeDevice>() }.events)
}

pub(crate) fn pause(runtime: &Runtime) -> Arc<(Mutex<(bool, bool)>, std::sync::Condvar)> {
    let owner = runtime.device().unwrap();
    let state = owner.state.lock().unwrap();
    let State::Open(resources) = &*state else {
        panic!("device is not open");
    };
    Arc::clone(&unsafe { &*resources.handles.context.cast::<FakeDevice>() }.pause)
}

#[test]
fn slices_reuse_one_device_and_explicit_close_is_permanent() {
    let runtime = runtime("ok");
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    for _ in 0..16 {
        let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
        endpoint.close().unwrap();
    }
    let events = events(&runtime);
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.opened_devices,
            stats.active_devices,
            stats.active_endpoints
        ),
        (1, 1, 0)
    );
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "open_device")
            .count(),
        1
    );
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    let closed = events.lock().unwrap().clone();
    assert_eq!(&closed[closed.len() - 2..], &["dealloc_pd", "close_device"]);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert!(matches!(
        runtime.staging_endpoint(8, 0),
        Err(Error::Unavailable(_))
    ));
    drop(runtime);
    assert_eq!(*events.lock().unwrap(), closed);
}

#[test]
fn endpoint_owners_keep_device_live_after_runtime_drop() {
    let mut runtime = runtime("ok");
    runtime.config.max_endpoints = 2;
    runtime.config.max_staging_bytes = 16;
    let mut first = runtime.staging_endpoint(8, 0).unwrap();
    let mut second = runtime.staging_endpoint(8, 0).unwrap();
    let events = events(&runtime);
    let usage = Arc::clone(&runtime.usage);
    drop(runtime);
    first.close().unwrap();
    drop(first);
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    assert_eq!(usage.lock().unwrap().active_devices, 1);
    second.close().unwrap();
    drop(second);
    assert_eq!(usage.lock().unwrap().active_devices, 0);
    assert_eq!(usage.lock().unwrap().active_endpoints, 0);
    let events = events.lock().unwrap();
    assert_eq!(&events[events.len() - 2..], &["dealloc_pd", "close_device"]);
}

#[test]
fn concurrent_slices_and_closers_construct_and_destroy_device_once() {
    let mut runtime = runtime("ok");
    runtime.config.max_endpoints = 8;
    runtime.config.max_staging_bytes = 64;
    let runtime = Arc::new(runtime);
    let barrier = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let runtime = Arc::clone(&runtime);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
            barrier.wait();
            barrier.wait();
            endpoint.close().unwrap();
        }));
    }
    barrier.wait();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.opened_devices,
            stats.active_devices,
            stats.active_endpoints
        ),
        (1, 1, 8)
    );
    let events = events(&runtime);
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let mut closers = Vec::new();
    for _ in 0..8 {
        let runtime = Arc::clone(&runtime);
        closers.push(thread::spawn(move || {
            runtime.close(Instant::now() + Duration::from_secs(2))
        }));
    }
    for closer in closers {
        closer.join().unwrap().unwrap();
    }
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
    let events = events.lock().unwrap();
    assert_eq!(events.iter().filter(|e| **e == "open_device").count(), 1);
    assert_eq!(events.iter().filter(|e| **e == "dealloc_pd").count(), 1);
    assert_eq!(events.iter().filter(|e| **e == "close_device").count(), 1);
}

#[test]
fn device_teardown_failures_are_retained_without_retry_from_close_or_drop() {
    for (mode, operation) in [("close3", "ibv_dealloc_pd"), ("close4", "ibv_close_device")] {
        let runtime = Arc::new(runtime(mode));
        runtime.staging_endpoint(8, 0).unwrap().close().unwrap();
        let events = events(&runtime);
        let usage = Arc::clone(&runtime.usage);
        let mut closers = Vec::new();
        for _ in 0..4 {
            let runtime = Arc::clone(&runtime);
            closers.push(thread::spawn(move || {
                runtime.close(Instant::now() + Duration::from_secs(2))
            }));
        }
        for closer in closers {
            assert!(
                matches!(closer.join().unwrap(), Err(Error::Unquiesced { operation: op, code: 16 }) if op == operation)
            );
        }
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_devices,
                stats.retained_devices,
                stats.active_endpoints
            ),
            (1, 1, 0)
        );
        let retained = events.lock().unwrap().clone();
        assert_eq!(
            retained
                .iter()
                .filter(|e| **e
                    == if mode == "close3" {
                        "dealloc_pd"
                    } else {
                        "close_device"
                    })
                .count(),
            1
        );
        assert!(matches!(
            runtime.staging_endpoint(8, 0),
            Err(Error::Unavailable(_))
        ));
        drop(runtime);
        assert_eq!(*events.lock().unwrap(), retained);
        assert_eq!(usage.lock().unwrap().active_devices, 1);
    }
}

#[test]
fn partial_device_construction_cleans_up_or_poison_retains_its_charge() {
    for mode in [
        "device_missing",
        "device_open",
        "device_open_close",
        "device_pd_close",
    ] {
        let runtime = runtime(mode);
        let retained = mode.ends_with("close");
        let error = runtime.staging_endpoint(8, 0).err().unwrap();
        assert_eq!(
            matches!(error, Error::Unquiesced { .. }),
            retained,
            "{mode}: {error:?}"
        );
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.staging_bytes,
                stats.opened_devices
            ),
            (0, 0, 0)
        );
        assert_eq!(
            (stats.active_devices, stats.retained_devices),
            (usize::from(retained), usize::from(retained))
        );
        if retained {
            assert!(matches!(
                runtime.staging_endpoint(8, 0),
                Err(Error::Unavailable(_))
            ));
            assert!(matches!(
                runtime.close(Instant::now() + Duration::from_secs(2)),
                Err(Error::Unquiesced { .. })
            ));
        } else {
            runtime
                .close(Instant::now() + Duration::from_secs(2))
                .unwrap();
        }
    }
}

#[test]
fn close_cannot_destroy_a_device_while_any_endpoint_still_owns_it() {
    let runtime = runtime("ok");
    let mut endpoint = runtime.staging_endpoint(8, 0).unwrap();
    let events = events(&runtime);
    assert!(matches!(
        runtime.close(Instant::now() + Duration::from_secs(2)),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        runtime.staging_endpoint(1, 0),
        Err(Error::Unavailable(_)) | Err(Error::ResourceExhausted)
    ));
    assert!(!events.lock().unwrap().contains(&"dealloc_pd"));
    endpoint.close().unwrap();
    // Closed endpoints still retain their stable output and the device until drop.
    assert!(matches!(
        runtime.close(Instant::now() + Duration::from_secs(2)),
        Err(Error::Timeout)
    ));
    drop(endpoint);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
    assert_eq!(runtime.stats().unwrap().active_devices, 0);
}
