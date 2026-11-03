use super::*;
use crate::test_support::EVENTUAL_PROGRESS;
use crate::{EngineConfig, RuntimeQueueConfig, TransferEngine};
use std::time::Instant;

#[test]
fn poll_backoff_hot_exponential_capped_and_saturating() {
    for poll in 0..128 {
        assert_eq!(next_poll_delay(poll), Duration::ZERO);
    }
    for (index, delay) in [1, 2, 4, 8, 16, 32, 64, 128, 200].into_iter().enumerate() {
        assert_eq!(
            next_poll_delay(128 + index as u64),
            Duration::from_micros(delay)
        );
    }
    for poll in [256, u32::MAX as u64, u64::MAX - 1, u64::MAX] {
        assert_eq!(next_poll_delay(poll), Duration::from_micros(200));
    }
    for poll in 1..1024 {
        assert!(next_poll_delay(poll) >= next_poll_delay(poll - 1));
    }
}

#[test]
fn worker_wakes_are_bounded_coalesced_and_stop_joins() {
    let engine = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());
    assert!(engine.progress_worker.is_none());
    let ids: Vec<_> = (0..3).map(|_| engine.allocate_batch(1).unwrap()).collect();
    let wakes = Wakes {
        pending: Mutex::default(),
        ready: Condvar::new(),
        capacity: 2,
        ticks: Default::default(),
    };
    let ids = ids.iter().map(|b| b.id().unwrap()).collect::<Vec<_>>();
    for &id in &[ids[0], ids[1], ids[0], ids[2]] {
        wakes.batch(id);
        wakes.queue();
    }
    {
        let state = wakes.pending.lock().unwrap();
        assert_eq!(state.batches, &ids[..2]);
        assert!(state.queue);
    }
    wakes.discard(ids[0]);
    wakes.batch(ids[2]);
    assert_eq!(wakes.pending.lock().unwrap().batches, &ids[1..]);
    let mut engine = TransferEngine::new(EngineConfig {
        enable_progress_worker: true,
        ..Default::default()
    })
    .unwrap();
    let worker = engine.progress_worker.as_mut().unwrap();
    let owner = Arc::downgrade(&worker.wakes);
    worker.stop();
    worker.stop();
    worker.wakes.batch(ids[0]);
    assert!(worker.wakes.pending.lock().unwrap().batches.is_empty());
    assert!(worker.thread.is_none());
    drop(engine);
    assert!(owner.upgrade().is_none());
    let engine = TransferEngine::new(EngineConfig {
        enable_progress_worker: true,
        ..Default::default()
    })
    .unwrap();
    let runner = Arc::downgrade(&engine.progress_worker.as_ref().unwrap().wakes);
    drop(engine);
    assert!(
        runner.upgrade().is_none(),
        "the joined runner releases its wake set before drop returns"
    );
}

#[test]
fn fallback_tick_runs_only_while_the_queue_owns_work() {
    for interval in [Duration::from_millis(2), Duration::ZERO] {
        let queue = LocalTransferAdmissionQueue::new(RuntimeQueueConfig::default());
        let worker = ProgressWorker::start(
            Arc::new(AllocationBudget::new()),
            Some(queue.clone()),
            2,
            interval,
        )
        .unwrap();
        let ticks = &worker.wakes.ticks;
        let wait = Duration::from_millis(18);
        thread::sleep(wait);
        assert_eq!(ticks.load(std::sync::atomic::Ordering::Acquire), 0);
        let permit = queue
            .try_admit(1, Instant::now() + EVENTUAL_PROGRESS, None, None, None)
            .unwrap();
        if interval.is_zero() {
            thread::sleep(wait);
            assert_eq!(ticks.load(std::sync::atomic::Ordering::Acquire), 0);
        } else {
            let until = Instant::now() + EVENTUAL_PROGRESS;
            while ticks.load(std::sync::atomic::Ordering::Acquire) == 0 {
                assert!(Instant::now() < until);
                thread::yield_now();
            }
        }
        drop(permit);
        // One timeout can already be returning as the permit drops.
        let before = ticks.load(std::sync::atomic::Ordering::Acquire);
        thread::sleep(wait);
        assert!(ticks.load(std::sync::atomic::Ordering::Acquire) <= before + 1);
    }
}
