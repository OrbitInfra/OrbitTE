//! One shared poller for in-flight NVLink events. Setup and receipts stay with callers.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::cuda::ipc::{IpcCopyCompletion, IpcCopyGroup};
use crate::engine::remaining;
use crate::{Error, Result};

struct Work {
    group: IpcCopyGroup,
    deadline: Instant,
    terminal: mpsc::SyncSender<IpcCopyCompletion>,
}

struct State {
    queue: VecDeque<Work>,
    count: usize,
    open: bool,
    started: bool,
    exited: bool,
    failed: bool,
}

struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

pub(super) struct Progress {
    shared: Arc<Shared>,
    worker: Mutex<Option<JoinHandle<()>>>,
    limit: usize,
}

impl Progress {
    pub(super) fn new(limit: usize) -> Result<Self> {
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(limit)
            .map_err(|_| Error::ResourceExhausted)?;
        Ok(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    queue,
                    count: 0,
                    open: true,
                    started: false,
                    exited: false,
                    failed: false,
                }),
                changed: Condvar::new(),
            }),
            worker: Mutex::new(None),
            limit,
        })
    }

    pub(super) fn wait(&self, mut group: IpcCopyGroup, deadline: Instant) -> IpcCopyCompletion {
        // Already-ready copies avoid a completion channel and cross-thread wakeup.
        match group.query() {
            Ok(true) => return group.complete(Ok(())),
            Ok(false) if Instant::now() < deadline => (),
            Ok(false) => return group.complete(Err(Error::Timeout)),
            Err(error) => return group.complete(Err(error)),
        }
        let attempted = group.attempted;
        let mut worker = self.worker.lock().unwrap_or_else(|p| p.into_inner());
        let mut state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.open || state.count == self.limit {
            drop(state);
            return group.complete(Err(Error::ResourceExhausted));
        }
        if worker.is_none() {
            let mut pending = VecDeque::new();
            if pending.try_reserve_exact(self.limit).is_err() {
                drop(state);
                return group.complete(Err(Error::ResourceExhausted));
            }
            let shared = Arc::clone(&self.shared);
            match thread::Builder::new()
                .name("orbitte-nvlink".into())
                .spawn(move || {
                    let _exit = WorkerExit(Arc::clone(&shared));
                    // Drop in-flight owners before WorkerExit publishes termination.
                    let mut pending = pending;
                    shared.run(&mut pending);
                }) {
                Ok(handle) => {
                    *worker = Some(handle);
                    state.started = true;
                }
                Err(error) => {
                    drop(state);
                    return group.complete(Err(error.into()));
                }
            }
        }
        let (terminal, finished) = mpsc::sync_channel(1);
        state.queue.push_back(Work {
            group,
            deadline,
            terminal,
        });
        state.count += 1;
        self.shared.changed.notify_one();
        drop(state);
        drop(worker);
        // The worker owns deadline handling and physical drain/retention. A caller
        // timeout must never detach the import from a still-running event.
        finished.recv().unwrap_or(IpcCopyCompletion {
            completed: 0,
            attempted,
            result: Err(Error::Unquiesced {
                operation: "NVLink completion worker exit",
                code: -1,
            }),
        })
    }

    pub(super) fn stats(&self) -> (usize, usize) {
        let state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        (state.count, usize::from(state.started && !state.exited))
    }

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        let mut worker = self.worker.lock().unwrap_or_else(|p| p.into_inner());
        let mut state = self.shared.state.lock().unwrap_or_else(|p| p.into_inner());
        state.open = false;
        self.shared.changed.notify_all();
        while state.started && !state.exited {
            state = self
                .shared
                .changed
                .wait_timeout(state, remaining(deadline)?)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        let failed = state.failed;
        drop(state);
        if let Some(worker) = worker.take() {
            worker.join().map_err(|_| Error::Unquiesced {
                operation: "NVLink completion worker exit",
                code: -1,
            })?;
        }
        if failed {
            Err(Error::Unquiesced {
                operation: "NVLink completion worker exit",
                code: -1,
            })
        } else {
            Ok(())
        }
    }
}

impl Shared {
    fn run(&self, pending: &mut VecDeque<Work>) {
        let mut idle_rounds = 0;
        loop {
            {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                while pending.is_empty() && state.queue.is_empty() && state.open {
                    state = self.changed.wait(state).unwrap_or_else(|p| p.into_inner());
                }
                if pending.is_empty() && state.queue.is_empty() && !state.open {
                    return;
                }
                if !state.queue.is_empty() {
                    idle_rounds = 0;
                }
                pending.append(&mut state.queue);
            }
            let count = pending.len();
            for _ in 0..count {
                let Some(mut work) = pending.pop_front() else {
                    break;
                };
                let result = match work.group.query() {
                    Ok(true) => Ok(()),
                    Ok(false) if Instant::now() < work.deadline => {
                        pending.push_back(work);
                        continue;
                    }
                    Ok(false) => Err(Error::Timeout),
                    Err(error) => Err(error),
                };
                // Release admission before group Drop returns its stream slot.
                self.state.lock().unwrap_or_else(|p| p.into_inner()).count -= 1;
                let completion = work.group.complete(result);
                // Group Drop already released proven leases or retained uncertainty.
                let _ = work.terminal.send(completion);
                idle_rounds = 0;
            }
            idle_rounds += 1;
            if !pending.is_empty() {
                if idle_rounds < 16 {
                    std::hint::spin_loop();
                } else {
                    let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    if state.queue.is_empty() {
                        let _ = self.changed.wait_timeout(state, Duration::from_micros(50));
                    }
                }
            }
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .open = false;
        self.shared.changed.notify_all();
    }
}

struct WorkerExit(Arc<Shared>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        let queued = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            state.open = false;
            state.exited = true;
            state.failed = thread::panicking();
            state.count = 0;
            std::mem::take(&mut state.queue)
        };
        drop(queued);
        self.0.changed.notify_all();
    }
}
