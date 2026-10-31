//! Gate a real cache creator after provider return, outside all production locks.
use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;

use super::*;

static GATES: OnceLock<Mutex<BTreeMap<usize, Weak<Gate>>>> = OnceLock::new();

struct Gate {
    context: usize,
    state: Mutex<Publication>,
    changed: Condvar,
    polls: AtomicUsize,
}

#[derive(Default)]
struct Publication {
    started: bool,
    entered: bool,
    released: bool,
    key: Weak<crate::memory::Registration>,
    window_bind: bool,
    creator: Option<JoinHandle<Result<()>>>,
}

fn gate(cache: &LocalBufferManager, context: &Arc<RdmaContext>) -> Option<Arc<Gate>> {
    GATES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(&(cache as *const _ as usize))
        .and_then(Weak::upgrade)
        .filter(|gate| gate.context == Arc::as_ptr(context) as usize)
}

pub(crate) fn before_acquire(
    cache: &Arc<LocalBufferManager>,
    context: &Arc<RdmaContext>,
    memory: &RegisteredMemory,
    window_bind: bool,
) {
    let Some(gate) = gate(cache, context) else {
        return;
    };
    let mut state = gate.state.lock().unwrap();
    if state.started {
        return;
    }
    state.started = true;
    state.key = Arc::downgrade(&memory.0);
    state.window_bind = window_bind;
    let creator_cache = Arc::clone(cache);
    let context = Arc::clone(context);
    let memory = memory.clone();
    state.creator = Some(std::thread::spawn(move || {
        let mut lease = creator_cache.acquire(
            context,
            &memory,
            window_bind,
            Instant::now() + Duration::from_secs(15),
        )?;
        lease.release()
    }));
    let (state, timeout) = gate
        .changed
        .wait_timeout_while(state, Duration::from_secs(3), |state| !state.entered)
        .unwrap();
    assert!(
        !timeout.timed_out() && state.entered,
        "real MR creator did not reach publication"
    );
}

pub(crate) fn before_publish(
    cache: &LocalBufferManager,
    context: &Arc<RdmaContext>,
    memory: &RegisteredMemory,
    window_bind: bool,
) {
    let Some(gate) = gate(cache, context) else {
        return;
    };
    let mut state = gate.state.lock().unwrap();
    if state.entered
        || !Weak::ptr_eq(&state.key, &Arc::downgrade(&memory.0))
        || state.window_bind != window_bind
    {
        return;
    }
    state.entered = true;
    gate.changed.notify_all();
    let (state, timeout) = gate
        .changed
        .wait_timeout_while(state, Duration::from_secs(15), |state| !state.released)
        .unwrap();
    assert!(
        !timeout.timed_out() && state.released,
        "MR publication gate was never released"
    );
}

pub(crate) fn observe_pending(
    cache: &LocalBufferManager,
    context: &Arc<RdmaContext>,
    memory: &RegisteredMemory,
    window_bind: bool,
) {
    let Some(gate) = gate(cache, context) else {
        return;
    };
    let state = gate.state.lock().unwrap();
    if state.entered
        && !state.released
        && state.window_bind == window_bind
        && Weak::ptr_eq(&state.key, &Arc::downgrade(&memory.0))
    {
        gate.polls.fetch_add(1, Ordering::Release);
    }
}

pub(crate) struct HeldPublication {
    cache: Arc<LocalBufferManager>,
    gate: Arc<Gate>,
}

impl HeldPublication {
    pub(crate) fn arm(runtime: &crate::transport::rdma::RdmaTransport, device: usize) -> Self {
        let cache = Arc::clone(&runtime.registrations);
        let context = runtime.device_at(device).unwrap();
        let gate = Arc::new(Gate {
            context: Arc::as_ptr(&context) as usize,
            state: Mutex::default(),
            changed: Condvar::new(),
            polls: AtomicUsize::new(0),
        });
        assert!(
            GATES
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(Arc::as_ptr(&cache) as usize, Arc::downgrade(&gate))
                .is_none()
        );
        Self { cache, gate }
    }

    pub(crate) fn polls(&self) -> usize {
        self.gate.polls.load(Ordering::Acquire)
    }

    pub(crate) fn assert_held(&self) {
        let state = self.gate.state.lock().unwrap();
        assert!(state.entered && !state.released);
        let key = state.key.clone();
        let window_bind = state.window_bind;
        drop(state);
        let cache = self.cache.state.lock().unwrap();
        let entries: Vec<_> = cache
            .entries
            .iter()
            .filter(|entry| {
                entry.owner.context.as_ptr() as usize == self.gate.context
                    && Weak::ptr_eq(&entry.owner.key, &key)
                    && entry.owner.window_bind == window_bind
            })
            .collect();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].creating);
        assert_eq!(
            entries[0].borrowers, 1,
            "only the creator owns this MR, not its waiter"
        );
    }

    pub(crate) fn release(&mut self) {
        let creator = {
            let mut state = self.gate.state.lock().unwrap();
            state.released = true;
            self.gate.changed.notify_all();
            state.creator.take()
        };
        if let Some(creator) = creator {
            creator.join().unwrap().unwrap();
        }
    }
}

impl Drop for HeldPublication {
    fn drop(&mut self) {
        self.release();
        GATES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(&(Arc::as_ptr(&self.cache) as usize));
    }
}

#[test]
fn pending_mr_waiter_preserves_creator_and_close_retirement() {
    for close in [false, true] {
        let registry = MemoryRegistry::new(SourceEpoch([31; 16]), 2, 32);
        let memory = registry
            .register(vec![1; 16].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let runtime = limited_runtime("direct:copy", 2, 32);
        let device = runtime.device().unwrap();
        let mut gate = HeldPublication::arm(&runtime, 0);
        let deadline = Instant::now() + Duration::from_secs(3);
        for _ in 0..32 {
            assert!(
                runtime
                    .registrations
                    .poll_acquire(Arc::clone(&device), &memory, false, deadline)
                    .unwrap()
                    .is_none()
            );
        }
        gate.assert_held();
        assert_eq!(gate.polls(), 32);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.opened_persistent_mrs,
                stats.persistent_mrs,
                stats.persistent_mr_bytes,
                stats.reused_persistent_mrs
            ),
            (1, 1, 16, 0)
        );
        assert!(matches!(
            runtime
                .registrations
                .poll_acquire(Arc::clone(&device), &memory, false, Instant::now()),
            Err(Error::Timeout)
        ));
        if close {
            assert!(matches!(
                runtime.registrations.close(Instant::now()),
                Err(Error::Timeout)
            ));
            assert!(matches!(
                runtime
                    .registrations
                    .poll_acquire(device, &memory, false, deadline),
                Err(Error::Unavailable("MR cache is closed"))
            ));
        } else {
            registry.unregister(&memory).unwrap();
            runtime.registrations.retire(&memory).unwrap();
        }
        gate.assert_held();
        gate.release();
        assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
        runtime.close(deadline).unwrap();
    }
}
