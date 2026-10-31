use super::ipc::owned;
use super::*;
use crate::memory::Allocation;
use std::sync::{Barrier, mpsc};
use std::time::Instant;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

fn count(device: &Device, name: &str) -> usize {
    device
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|&&e| e == name)
        .count()
}

#[test]
fn stream_pool_bounds_concurrent_cold_leases_without_locking_memory() {
    let device = allocation(&[0; 128]);
    let memory = owned(&device, false);
    let pool = CUDAStreamPool::new(1).unwrap();
    let start = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(2));
    let (ready_tx, ready_rx) = mpsc::channel();
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let (pool, memory, start, release, ready) = (
                pool.clone(),
                memory.clone(),
                start.clone(),
                release.clone(),
                ready_tx.clone(),
            );
            std::thread::spawn(move || {
                start.wait();
                let stream = pool.acquire(&Allocation::Cuda(memory), deadline()).unwrap();
                ready.send(()).unwrap();
                release.wait();
                drop(stream);
            })
        })
        .collect();
    start.wait();
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(ready_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert_eq!(pool.stats().unwrap(), (1, 1, 0));
    assert!(memory.allocation.try_lock().is_ok());
    assert!(matches!(
        pool.acquire(
            &Allocation::Cuda(memory.clone()),
            Instant::now() + Duration::from_millis(5)
        ),
        Err(Error::Timeout)
    ));
    assert_eq!(count(&device, "stream-create"), 1);
    release.wait();
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(pool.stats().unwrap(), (1, 1, 0));
    release.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(count(&device, "stream-create"), 1);
    assert_eq!(count(&device, "event-create"), 1);
    pool.close(deadline()).unwrap();
    assert_eq!(pool.stats().unwrap(), (0, 0, 0));
}

#[test]
fn stream_pool_matches_actual_context_reclaims_idle_and_fences_close() {
    let devices: Vec<_> = (0..2).map(|_| allocation(&[0; 128])).collect();
    let memories: Vec<_> = devices
        .iter()
        .map(|d| Allocation::Cuda(owned(d, false)))
        .collect();
    let pool = CUDAStreamPool::new(1).unwrap();
    for index in [0, 0, 1, 1, 0] {
        let mut handle = pool.acquire(&memories[index], deadline()).unwrap();
        assert_eq!(
            handle.stream().unwrap().context_key(),
            memories[index].ipc_context().unwrap()
        );
    }
    assert_eq!(count(&devices[0], "stream-create"), 2);
    assert_eq!(count(&devices[0], "stream-destroy"), 1);
    assert_eq!(count(&devices[1], "stream-create"), 1);
    assert_eq!(count(&devices[1], "stream-destroy"), 1);
    let lease = pool.acquire(&memories[0], deadline()).unwrap();
    assert!(matches!(
        pool.close(Instant::now() + Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        pool.acquire(&memories[1], deadline()),
        Err(Error::Unavailable(_))
    ));
    drop(lease);
    pool.close(deadline()).unwrap();
    pool.close(deadline()).unwrap();
    assert_eq!(pool.stats().unwrap(), (0, 0, 0));
    assert_eq!(count(&devices[0], "stream-destroy"), 2);
    assert!(STACK.with(|s| s.borrow().is_empty()));
}

#[test]
fn stream_pool_charges_uncertain_construction_and_cleanup_without_retry() {
    for fault in [
        "event-create",
        "partial-create",
        "event-destroy",
        "stream-destroy",
        "replace",
    ] {
        let device = allocation(&[0; 128]);
        let memory = Allocation::Cuda(owned(&device, false));
        let pool = CUDAStreamPool::new(1).unwrap();
        match fault {
            "event-create" => device.event_create_error.store(1, Ordering::Release),
            "partial-create" => {
                device.stream_create_error.store(1, Ordering::Release);
                device.stream_partial_create.store(true, Ordering::Release);
                device.stream_destroy_error.store(1, Ordering::Release);
            }
            _ => (),
        }
        let lease = pool.acquire(&memory, deadline());
        if fault == "event-create" {
            assert!(matches!(lease, Err(Error::Backend { .. })));
            assert_eq!(pool.stats().unwrap(), (0, 0, 0));
            device.event_create_error.store(0, Ordering::Release);
            drop(pool.acquire(&memory, deadline()).unwrap());
            pool.close(deadline()).unwrap();
            continue;
        }
        if fault == "partial-create" {
            assert!(matches!(lease, Err(Error::Unquiesced { .. })));
        } else {
            drop(lease.unwrap());
            if fault == "event-destroy" {
                device.event_destroy_error.store(1, Ordering::Release);
            } else {
                device.stream_destroy_error.store(1, Ordering::Release);
            }
        }
        if fault == "replace" {
            let other = allocation(&[0; 128]);
            assert!(matches!(
                pool.acquire(&Allocation::Cuda(owned(&other, false)), deadline()),
                Err(Error::Unquiesced { .. })
            ));
            assert_eq!(count(&other, "stream-create"), 0);
        }
        assert!(matches!(
            pool.close(deadline()),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(pool.stats().unwrap(), (1, 0, 1));
        let destroys = (
            count(&device, "event-destroy"),
            count(&device, "stream-destroy"),
        );
        device.event_destroy_error.store(0, Ordering::Release);
        device.stream_destroy_error.store(0, Ordering::Release);
        assert!(pool.close(deadline()).is_err());
        drop(pool);
        assert_eq!(
            destroys,
            (
                count(&device, "event-destroy"),
                count(&device, "stream-destroy")
            )
        );
        assert!(!device.streams.lock().unwrap().is_empty());
    }
}
