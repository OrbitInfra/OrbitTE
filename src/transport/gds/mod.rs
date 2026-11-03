//! Caller-owned cuFile batch transport for CUDA and host memory.
mod batch;
mod registration;
mod sys;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::{Error, Result};
use batch::LiveBatch;
pub(crate) enum Quarantine {
    Batch(Box<LiveBatch>),
    BufferRelease,
}
pub(crate) use registration::{BufferLease, BufferSlot, FileHandle};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GdsMode {
    /// Effective driver configuration disables compatibility I/O.
    NativeOnly,
    CompatAllowed,
    CompatForced,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GdsStats {
    pub mode: Option<GdsMode>,
    pub available: bool,
    /// Clean cuFile buffer registration failures that left the CUDA region usable.
    pub buffer_registration_failures: u64,
    pub submitted_batches: u64,
    pub quarantined_batches: usize,
}

pub(crate) struct Driver {
    api: Arc<dyn sys::Api>,
    // PID in the upper bits. A child never waits on an inherited initializer.
    process: AtomicU64,
    max_native_slice: AtomicUsize,
}
impl Driver {
    fn new(api: Arc<dyn sys::Api>) -> Arc<Self> {
        Arc::new(Self {
            api,
            process: AtomicU64::new(0),
            max_native_slice: AtomicUsize::new(0),
        })
    }

    pub(crate) fn pid(&self) -> u32 {
        self.api.pid()
    }

    fn mode(&self) -> Result<GdsMode> {
        let generation = u64::from(self.api.pid()) << 32;
        loop {
            let state = self.process.load(Ordering::Acquire);
            if state >> 32 == generation >> 32 {
                return match state as u32 {
                    1 => {
                        std::thread::yield_now();
                        continue;
                    }
                    2 => Ok(GdsMode::NativeOnly),
                    3 => Ok(GdsMode::CompatAllowed),
                    4 => Ok(GdsMode::CompatForced),
                    _ => Err(Error::Unavailable("cuFile driver unavailable")),
                };
            }
            if self
                .process
                .compare_exchange(state, generation | 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let result = self
                .api
                .open()
                .and_then(|()| self.api.profile())
                .map(|profile| {
                    self.max_native_slice
                        .store(profile.max_native_slice, Ordering::Relaxed);
                    profile.mode
                });
            let tag = match &result {
                Ok(GdsMode::NativeOnly) => 2,
                Ok(GdsMode::CompatAllowed) => 3,
                Ok(GdsMode::CompatForced) => 4,
                Err(_) => 5,
            };
            self.process.store(generation | tag, Ordering::Release);
            return result.map_err(|_| Error::Unavailable("cuFile driver unavailable"));
        }
    }
}

pub(crate) struct GdsTransport {
    driver: Arc<Driver>,
    depth: u32,
    require_native: bool,
    buffer_registration_failures: AtomicU64,
    submitted: AtomicU64,
    quarantine: Mutex<Vec<Retained>>,
}

struct Retained {
    batch: Quarantine,
    submission: crate::engine::PreparedSubmission,
    _source: Arc<crate::engine::Source>,
    progress: Option<(Arc<crate::batch::BatchProgress>, usize)>,
}

impl GdsTransport {
    pub(crate) fn probe(config: &crate::EngineConfig) -> Option<Self> {
        // cuFile is a process service. Engines share its open and DSO lifetime;
        // a dropping engine must not close another engine's driver.
        static DRIVER: OnceLock<Option<Arc<Driver>>> = OnceLock::new();
        let driver = DRIVER
            .get_or_init(|| sys::load().ok().map(Driver::new))
            .as_ref()?
            .clone();
        driver.mode().ok()?;
        Some(Self::new(driver, config))
    }
    fn new(driver: Arc<Driver>, config: &crate::EngineConfig) -> Self {
        Self {
            driver,
            depth: config.gds_batch_depth,
            require_native: config.gds_require_native,
            buffer_registration_failures: AtomicU64::new(0),
            submitted: AtomicU64::new(0),
            quarantine: Mutex::default(),
        }
    }
    pub(crate) fn mode(&self) -> Result<GdsMode> {
        let mode = self.driver.mode()?;
        if self.require_native && mode != GdsMode::NativeOnly {
            return Err(Error::Unavailable("GDS compatibility mode is enabled"));
        }
        Ok(mode)
    }
    pub(crate) fn stats(&self) -> GdsStats {
        GdsStats {
            mode: self.driver.mode().ok(),
            available: self.mode().is_ok(),
            buffer_registration_failures: self.buffer_registration_failures.load(Ordering::Relaxed),
            submitted_batches: self.submitted.load(Ordering::Relaxed),
            quarantined_batches: self
                .quarantine
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .len(),
        }
    }

    pub(crate) fn retain(
        &self,
        batch: Quarantine,
        submission: crate::engine::PreparedSubmission,
        source: Arc<crate::engine::Source>,
        progress: Option<(Arc<crate::batch::BatchProgress>, usize)>,
        publish: impl FnOnce(),
    ) {
        let mut retained = self.quarantine.lock().unwrap_or_else(|p| p.into_inner());
        retained.push(Retained {
            batch,
            submission,
            _source: source,
            progress,
        });
        publish();
    }

    // Explicit progress probes only one recoverable owner for this batch.
    // A busy quarantine yields; an irreversible deregister/destroy is never retried.
    pub(crate) fn try_collect_for(&self, id: crate::BatchId) -> bool {
        let Ok(mut retained) = self.quarantine.try_lock() else {
            return true;
        };
        let Some(i) = retained.iter().position(|r| {
            r.recoverable() && r.progress.as_ref().is_some_and(|(p, _)| p.id() == id)
        }) else {
            return false;
        };
        let mut item = retained.remove(i);
        if item.collect() {
            item.publish_drained();
        } else {
            retained.push(item);
        }
        retained
            .iter()
            .any(|r| r.recoverable() && r.progress.as_ref().is_some_and(|(p, _)| p.id() == id))
    }

    pub(crate) fn cleanup(&self) {
        let mut retained = self.quarantine.lock().unwrap_or_else(|p| p.into_inner());
        let mut i = 0;
        while i < retained.len() {
            if retained[i].collect() {
                retained.swap_remove(i).publish_drained();
            } else {
                i += 1;
            }
        }
    }
}

impl Retained {
    fn publish_drained(mut self) {
        let progress = self.progress.take();
        drop(self);
        if let Some((progress, index)) = progress {
            progress.gds_drained(index);
        }
    }
    fn recoverable(&self) -> bool {
        matches!(&self.batch, Quarantine::Batch(batch) if !batch.permanent)
    }

    fn collect(&mut self) -> bool {
        let Quarantine::Batch(batch) = &mut self.batch else {
            return false;
        };
        if !batch.collect() {
            return false;
        }
        if !self.submission.release_gds_buffers() {
            batch.permanent = true;
            return false;
        }
        true
    }
}

impl Drop for GdsTransport {
    fn drop(&mut self) {
        self.cleanup();
        let retained = self.quarantine.get_mut().unwrap_or_else(|p| p.into_inner());
        if !retained.is_empty() {
            std::mem::forget(std::mem::take(retained));
        }
    }
}

fn uncertain(operation: &'static str, error: &Error) -> Error {
    Error::Unquiesced {
        operation,
        code: match error {
            Error::Backend { code, .. } | Error::Unquiesced { code, .. } => *code,
            Error::Io(e) => e.raw_os_error().unwrap_or(-1),
            _ => -1,
        },
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/gds.rs"]
pub(crate) mod tests;
