//! Caller-owned cuFile batch transport for CUDA and host memory.
mod batch;
mod registration;
mod sys;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::{Error, Result};
pub(crate) use batch::LiveBatch;
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
    submitted: AtomicU64,
    quarantine: Mutex<Vec<Retained>>,
}

struct Retained {
    batch: LiveBatch,
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
        batch: LiveBatch,
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

    // A later submitting or closing caller can collect late receipts. A failed
    // teardown is never tried twice. Each item retains its original admission.
    pub(crate) fn cleanup(&self) {
        let mut retained = self.quarantine.lock().unwrap_or_else(|p| p.into_inner());
        let mut i = 0;
        while i < retained.len() {
            let item = &mut retained[i];
            if !item.batch.collect() {
                i += 1;
                continue;
            }
            if !item.submission.release_gds_buffers() {
                item.batch.permanent = true;
                i += 1;
                continue;
            }
            let mut item = retained.swap_remove(i);
            let progress = item.progress.take();
            drop(item);
            if let Some((progress, index)) = progress {
                progress.gds_drained(index);
            }
        }
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
