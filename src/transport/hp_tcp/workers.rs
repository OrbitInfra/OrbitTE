use crate::{Error, Result};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use tokio::{
    runtime::{Builder, Handle},
    sync::oneshot,
};

/// One socket reactor per worker. A lane always executes on its assigned reactor.
pub(super) struct Workers {
    handles: Vec<Handle>,
    failed: Arc<AtomicBool>,
    stopping: AtomicBool,
    owners: Mutex<Vec<Worker>>,
}
struct Worker {
    stop: Option<oneshot::Sender<()>>,
    thread: thread::JoinHandle<()>,
}
impl Workers {
    pub(super) fn start(count: usize) -> Result<Self> {
        let mut workers = Self {
            handles: Vec::with_capacity(count),
            failed: Arc::default(),
            stopping: AtomicBool::new(false),
            owners: Mutex::new(Vec::with_capacity(count)),
        };
        for i in 0..count {
            let runtime = Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()?;
            let handle = runtime.handle().clone();
            let (stop, stopped) = oneshot::channel();
            let thread = thread::Builder::new()
                .name(format!("orbitte-hp-tcp-{i}"))
                .spawn(move || {
                    let _ = runtime.block_on(stopped);
                    // Dropping the reactor closes sockets and drops their loans on this worker.
                })?;
            workers.handles.push(handle);
            workers
                .owners
                .get_mut()
                .unwrap_or_else(|p| p.into_inner())
                .push(Worker {
                    thread,
                    stop: Some(stop),
                });
        }
        Ok(workers)
    }
    pub(super) fn execute(&self, index: usize, f: impl Future<Output = ()> + Send + 'static) {
        let failed = self.failed.clone();
        self.handles[index % self.handles.len()].spawn(async move {
            let mut job = std::pin::pin!(f);
            std::future::poll_fn(|cx| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    job.as_mut().poll(cx)
                })) {
                    Ok(progress) => progress,
                    Err(_) => {
                        failed.store(true, Ordering::Release);
                        log::error!("HP-TCP socket worker task failed");
                        std::task::Poll::Ready(())
                    }
                }
            })
            .await;
        });
    }
    pub(super) fn ready(&self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) || self.stopping.load(Ordering::Acquire) {
            return Err(Error::Unavailable("HP-TCP workers are stopped or failed"));
        }
        Ok(())
    }
    pub(super) fn handles(&self) -> Vec<Handle> {
        self.handles.clone()
    }
    pub(super) fn stop(&self) -> Result<()> {
        self.stopping.store(true, Ordering::Release);
        let mut owners = self.owners.lock().unwrap_or_else(|p| p.into_inner());
        for w in &mut *owners {
            if let Some(stop) = w.stop.take() {
                let _ = stop.send(());
            }
        }
        let mut result = Ok(());
        for w in owners.drain(..) {
            if w.thread.join().is_err() {
                result = Err(Error::Internal(
                    "HP-TCP socket worker failed during shutdown",
                ));
            }
        }
        result
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        if let Err(e) = self.stop() {
            log::error!("HP-TCP shutdown: {e}");
        }
    }
}
