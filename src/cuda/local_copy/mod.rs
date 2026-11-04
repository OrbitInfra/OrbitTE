//! Stream-ordered local copies between registered CUDA and pinned host ranges.
use super::{Context, driver};
use crate::{Error, RegisteredMemory, Result};
use cudarc::driver::sys;
use driver::check;
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod dma;
mod flight;
mod kernel;
mod plan;

use flight::{Inner, Pool};

/// The exact CUDA host registration shared by one or more retained allocator slots.
pub struct PinnedHostRegistration {
    base: u64,
    length: usize,
    _owner: Arc<dyn Send + Sync>,
}
impl PinnedHostRegistration {
    /// # Safety
    /// `base..base+length` must be one live CUDA host registration. The owner must
    /// keep it registered and stationary. All its suballocations must share this
    /// identity and coordinate external accesses with their OrbitTE range loans.
    pub unsafe fn from_raw_parts(
        base: NonNull<u8>,
        length: usize,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Arc<Self>> {
        let base = base.as_ptr() as usize as u64;
        if length == 0 || length > isize::MAX as usize || base.checked_add(length as u64).is_none()
        {
            return Err(Error::InvalidRange);
        }
        Ok(Arc::new(Self {
            base,
            length,
            _owner: owner,
        }))
    }
    pub(crate) fn base(&self) -> u64 {
        self.base
    }
    pub(crate) fn offset(&self, address: u64, length: usize) -> Result<usize> {
        let off = address
            .checked_sub(self.base)
            .and_then(|v| usize::try_from(v).ok())
            .ok_or(Error::InvalidRange)?;
        crate::memory::checked_range(off, length, self.length)?;
        Ok(off)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyDirection {
    HostToDevice,
    DeviceToHost,
}
impl CopyDirection {
    fn ends(self, device: u64, host: u64) -> (u64, u64) {
        match self {
            Self::HostToDevice => (device, host),
            Self::DeviceToHost => (host, device),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalCopyBackend {
    Auto,
    Dma,
    Kernel,
}

#[derive(Clone, Copy, Debug)]
pub struct LocalCopyConfig {
    pub backend: LocalCopyBackend,
    /// Minimum number of DMA operations after contiguous and strided coalescing.
    pub kernel_threshold: usize,
    /// Bounds input descriptors and expanded rows in one submission.
    pub max_descriptors: usize,
    pub max_inflight: usize,
    /// Total payload bytes plus reserved device and pinned kernel descriptor slots.
    pub max_bytes: usize,
}
impl Default for LocalCopyConfig {
    fn default() -> Self {
        Self {
            backend: LocalCopyBackend::Auto,
            kernel_threshold: 128,
            max_descriptors: 4096,
            max_inflight: 4,
            max_bytes: 256 << 20,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalCopyDesc {
    device: RegisteredMemory,
    device_offset: usize,
    host: RegisteredMemory,
    host_offset: usize,
    width: usize,
    rows: usize,
    device_pitch: usize,
    host_pitch: usize,
}
impl LocalCopyDesc {
    pub fn new(
        device: &RegisteredMemory,
        device_offset: usize,
        host: &RegisteredMemory,
        host_offset: usize,
        width: usize,
    ) -> Self {
        Self {
            device: device.clone(),
            device_offset,
            host: host.clone(),
            host_offset,
            width,
            rows: 1,
            device_pitch: width,
            host_pitch: width,
        }
    }
    /// Repeat this row with independent byte pitches in the same two views.
    #[must_use]
    pub fn strided(mut self, rows: usize, device_pitch: usize, host_pitch: usize) -> Self {
        self.rows = rows;
        self.device_pitch = device_pitch;
        self.host_pitch = host_pitch;
        self
    }
}

/// A caller-recorded CUDA event generation. Submission waits on the GPU and
/// keeps the supplied owner until the copy's own completion event retires.
#[derive(Clone)]
pub struct ProducerEvent {
    raw: usize,
    _owner: Arc<dyn Send + Sync>,
}
impl ProducerEvent {
    /// # Safety
    /// The event must be live, already recorded, and represent the intended
    /// producer generation. The owner retains the event and producer inputs.
    /// Do not re-record or destroy it while a submission may still bind its wait.
    pub unsafe fn from_raw(event: *mut c_void, owner: Arc<dyn Send + Sync>) -> Result<Self> {
        if event.is_null() {
            return Err(Error::InvalidConfig("null CUDA producer event"));
        }
        Ok(Self {
            raw: event as usize,
            _owner: owner,
        })
    }
}

pub struct LocalCopyEngine {
    inner: Arc<Inner>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LocalCopyStats {
    pub active: usize,
    pub retained: usize,
    pub payload_bytes: usize,
    pub descriptor_bytes: usize,
}

impl LocalCopyEngine {
    /// Bind one bounded copy owner to a caller stream. Kernel scratch is reserved here,
    /// outside submission. The device registration supplies the exact execution context.
    ///
    /// # Safety
    /// The stream must be valid in the device registration's context, and `owner`
    /// must keep it and its context alive. Coordinate stream capture, destruction,
    /// producer work and access by external code with OrbitTE. A per-thread default
    /// stream must be bound and submitted on its originating thread.
    pub unsafe fn bind(
        device: &RegisteredMemory,
        stream: *mut c_void,
        owner: Arc<dyn Send + Sync>,
        config: LocalCopyConfig,
    ) -> Result<Self> {
        if !(1..=65536).contains(&config.max_descriptors)
            || !(1..=64).contains(&config.max_inflight)
            || config.kernel_threshold == 0
            || config.max_bytes == 0
        {
            return Err(Error::InvalidConfig(
                "invalid local copy descriptor, slot, threshold or byte limit",
            ));
        }
        let (gpu, _) = device.cuda_range(0, device.len())?;
        let ctx = gpu
            .allocation
            .lock()
            .map_err(|_| Error::Internal("CUDA allocation lock"))?
            .context
            .clone();
        // Special handles are relative to the caller's current context.
        let mut actual = std::ptr::null_mut();
        // SAFETY: the caller retains a valid stream; resolve it before pushing a context.
        check("cuStreamGetCtx local copy", unsafe {
            (ctx.driver.stream_context)(stream.cast(), &mut actual)
        })?;
        if actual != ctx.raw.as_ptr() {
            return Err(Error::Unavailable(
                "caller stream belongs to another CUDA context",
            ));
        }
        let pitch = ctx.current(|| {
            check_capture(&ctx, stream.cast())?;
            if ctx.driver.local.copy_2d.is_some() && ctx.driver.local.attribute.is_some() {
                attribute(&ctx, sys::CUdevice_attribute::CU_DEVICE_ATTRIBUTE_MAX_PITCH)
            } else {
                Ok(0)
            }
        })?;
        let reserve = config.max_descriptors * config.max_inflight * 48;
        let want_kernel = config.backend == LocalCopyBackend::Kernel
            || (config.backend == LocalCopyBackend::Auto
                && config.kernel_threshold <= config.max_descriptors);
        let kernel = if !want_kernel {
            None
        } else if reserve >= config.max_bytes {
            if config.backend == LocalCopyBackend::Kernel {
                return Err(Error::ResourceExhausted);
            }
            None
        } else {
            match kernel::Kernel::install(&ctx, &config) {
                Ok(k) => Some(k),
                Err(Error::Unavailable(reason)) if config.backend == LocalCopyBackend::Auto => {
                    log::debug!("local CUDA DMA selected: {reason}");
                    None
                }
                Err(e) => return Err(e),
            }
        };
        let pool = Pool::new(ctx, stream as usize, owner, config, pitch, kernel, reserve);
        Ok(Self {
            inner: Arc::new(Inner(Mutex::new(pool))),
        })
    }

    /// Enqueue in caller stream order. A partial enqueue error is reported by
    /// the completion after its marker. A missing marker returns Unquiesced.
    pub fn submit(
        &self,
        direction: CopyDirection,
        descs: &[LocalCopyDesc],
        ready: Option<&ProducerEvent>,
    ) -> Result<LocalCopyCompletion> {
        let mut pool = self
            .inner
            .0
            .lock()
            .map_err(|_| Error::Internal("local copy pool lock"))?;
        let (index, signal, backend) = pool.submit(direction, descs, ready)?;
        Ok(LocalCopyCompletion {
            inner: self.inner.clone(),
            index,
            signal,
            backend,
        })
    }
    /// Poll abandoned submissions and return current ownership charges.
    pub fn progress(&self) -> Result<LocalCopyStats> {
        let mut pool = self
            .inner
            .0
            .lock()
            .map_err(|_| Error::Internal("local copy pool lock"))?;
        pool.progress();
        Ok(pool.stats())
    }
    /// Stop admission and wait up to the timeout for event receipts. A timeout
    /// retains all active loans. Teardown can be called again after a timeout.
    pub fn close(&self, timeout: Duration) -> Result<()> {
        self.inner.close(timeout)
    }
}

#[must_use = "observe the completion before reusing payload bytes"]
pub struct LocalCopyCompletion {
    inner: Arc<Inner>,
    index: usize,
    signal: Arc<flight::Signal>,
    backend: LocalCopyBackend,
}
impl LocalCopyCompletion {
    pub fn backend(&self) -> LocalCopyBackend {
        self.backend
    }
    /// Query only the event recorded immediately after this submission.
    pub fn poll(&self) -> Result<bool> {
        let mut pool = self
            .inner
            .0
            .lock()
            .map_err(|_| Error::Internal("local copy pool lock"))?;
        if self.signal.status()?.is_none() {
            pool.poll(self.index);
        }
        self.signal.status().map(|s| s.is_some())
    }
    pub fn wait(&self, timeout: Duration) -> Result<()> {
        wait_for(timeout, || self.poll())
    }
}

fn wait_for(timeout: Duration, mut step: impl FnMut() -> Result<bool>) -> Result<()> {
    let end = Instant::now()
        .checked_add(timeout)
        .ok_or(Error::InvalidConfig("local copy deadline overflow"))?;
    loop {
        if step()? {
            return Ok(());
        }
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(Error::Timeout);
        }
        std::thread::sleep(left.min(Duration::from_micros(200)));
    }
}
fn attribute(ctx: &Context, key: sys::CUdevice_attribute) -> Result<usize> {
    let f = ctx
        .driver
        .local
        .attribute
        .ok_or(Error::Unavailable("cuDeviceGetAttribute local copy"))?;
    let mut value = 0;
    // SAFETY: the device is owned by the bound context; output is writable.
    check("cuDeviceGetAttribute local copy", unsafe {
        f(&mut value, key, ctx.device)
    })?;
    usize::try_from(value)
        .ok()
        .filter(|n| *n > 0)
        .ok_or(Error::Unavailable("invalid local CUDA device limit"))
}
fn check_capture(ctx: &Context, stream: sys::CUstream) -> Result<()> {
    let mut capture = sys::CUstreamCaptureStatus::CU_STREAM_CAPTURE_STATUS_NONE;
    // SAFETY: the caller retains the verified stream, and capture is writable.
    check("cuStreamIsCapturing local copy", unsafe {
        (ctx.driver.stream_capture)(stream, &mut capture)
    })?;
    if capture != sys::CUstreamCaptureStatus::CU_STREAM_CAPTURE_STATUS_NONE {
        return Err(Error::Unavailable(
            "local CUDA copy cannot join stream capture",
        ));
    }
    Ok(())
}
fn uncertain(e: Error) -> Error {
    match e {
        Error::Backend { operation, code } | Error::Unquiesced { operation, code } => {
            Error::Unquiesced { operation, code }
        }
        _ => Error::Unquiesced {
            operation: "local CUDA completion",
            code: -1,
        },
    }
}
